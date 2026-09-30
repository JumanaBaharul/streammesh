// Package wal is StreamMesh's durability layer.
//
// Every accepted event is appended to a segmented log before the pipeline
// acknowledges it. Segments are size-bounded so old data can be removed without
// rewriting the file, and each record carries a CRC32 so a torn write from a
// crash is detected and truncated on replay instead of being replayed as truth.
//
// Appends buffer in memory and are flushed by a background syncer, which is the
// throughput/durability trade-off real systems make: with SyncEach disabled
// there is a bounded window (one flush interval) of events that a machine-level
// crash can lose, and the counter streammesh_wal_unsynced_bytes exposes exactly
// how large that window currently is.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerSize    = 12 // 8-byte sequence, 4-byte payload length
	checksumSize  = 4
	segmentPrefix = "wal-"
	segmentSuffix = ".log"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt marks a record whose checksum or length is not trustworthy.
var ErrCorrupt = errors.New("wal: corrupt record")

// Config configures the log.
type Config struct {
	// Dir is where segment files live.
	Dir string
	// SegmentBytes is the target size before rotating to a new segment.
	SegmentBytes int64
	// FsyncInterval is how often buffered appends are flushed and synced.
	FsyncInterval time.Duration
	// SyncEach forces an fsync per append, trading throughput for durability.
	SyncEach bool
	// MaxSegments keeps at most this many closed segments (0 = unlimited).
	MaxSegments int
}

// DefaultConfig returns a balanced configuration.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:           dir,
		SegmentBytes:  32 << 20,
		FsyncInterval: 200 * time.Millisecond,
	}
}

type stats struct {
	appends     int64
	syncs       int64
	rotations   int64
	segment     int64
	size        int64
	unsynced    int64
	lastSync    time.Time
	truncations int64
}

// Log is a segmented, append-only, checksummed write-ahead log.
type Log struct {
	cfg Config

	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	segment  int64
	written  int64
	unsynced int64
	seq      uint64
	size     int64
	closed   bool

	statsMu sync.Mutex
	stats   stats

	stop   chan struct{}
	done   chan struct{}
	synced chan struct{}
}

// Open creates or resumes a log in the configured directory.
func Open(cfg Config) (*Log, error) {
	if cfg.Dir == "" {
		return nil, errors.New("wal: directory is required")
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 32 << 20
	}
	if cfg.FsyncInterval <= 0 {
		cfg.FsyncInterval = 200 * time.Millisecond
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create directory: %w", err)
	}

	log := &Log{
		cfg:    cfg,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		synced: make(chan struct{}, 1),
	}
	if err := log.openLatest(); err != nil {
		return nil, err
	}
	go log.syncer()
	return log, nil
}

// openLatest finds the highest-numbered segment and resumes it, recovering the
// last sequence number.
func (l *Log) openLatest() error {
	segments, err := l.segments()
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return l.rotate(1)
	}
	last := segments[len(segments)-1]
	index, err := segmentIndex(last)
	if err != nil {
		return err
	}

	// Replay the existing segment to find the highest sequence and drop any
	// trailing torn record before appending after it.
	valid, highest, size, err := scanSegment(last)
	if err != nil {
		return err
	}
	if size < fileSize(last) {
		// A partial record was found: truncate it so the next append starts at a
		// record boundary.
		if err := os.Truncate(last, size); err != nil {
			return fmt.Errorf("wal: truncate torn record: %w", err)
		}
		l.statsMu.Lock()
		l.stats.truncations++
		l.statsMu.Unlock()
	}
	_ = valid

	file, err := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	l.file = file
	l.writer = bufio.NewWriterSize(file, 1<<20)
	l.segment = index
	l.written = size
	l.seq = highest
	l.size = size

	l.statsMu.Lock()
	l.stats.segment = index
	l.stats.size = size
	l.statsMu.Unlock()
	return nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// scanSegment returns whether the segment is fully valid, its highest sequence
// number, and the offset of the first invalid byte.
func scanSegment(path string) (bool, uint64, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, 0, 0, fmt.Errorf("wal: open segment for scan: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReaderSize(file, 1<<20)
	var offset int64
	var highest uint64
	valid := true

	for {
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			valid = false
			break
		}
		seq := binary.LittleEndian.Uint64(header[:8])
		length := binary.LittleEndian.Uint32(header[8:])
		if length == 0 || length > 64<<20 {
			valid = false
			break
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			valid = false
			break
		}
		checksum := make([]byte, checksumSize)
		if _, err := io.ReadFull(reader, checksum); err != nil {
			valid = false
			break
		}
		if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(checksum) {
			valid = false
			break
		}
		offset += int64(headerSize) + int64(length) + int64(checksumSize)
		if seq > highest {
			highest = seq
		}
	}
	return valid, highest, offset, nil
}

func (l *Log) syncer() {
	defer close(l.done)
	ticker := time.NewTicker(l.cfg.FsyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			_ = l.flush(true)
			return
		case <-ticker.C:
			_ = l.flush(true)
		}
	}
}

// Append writes a payload and returns its sequence number. Unless SyncEach is
// set, the data is buffered and becomes durable at the next flush.
func (l *Log) Append(payload []byte) (uint64, error) {
	if len(payload) == 0 {
		return 0, errors.New("wal: empty payload")
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, errors.New("wal: log is closed")
	}

	recordSize := int64(headerSize + len(payload) + checksumSize)
	if l.written > 0 && l.written+recordSize > l.cfg.SegmentBytes {
		if err := l.rotate(l.segment + 1); err != nil {
			return 0, err
		}
	}

	header := make([]byte, headerSize)
	l.seq++
	binary.LittleEndian.PutUint64(header[:8], l.seq)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(payload)))

	checksum := make([]byte, checksumSize)
	binary.LittleEndian.PutUint32(checksum, crc32.Checksum(payload, crcTable))

	if _, err := l.writer.Write(header); err != nil {
		return 0, fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := l.writer.Write(payload); err != nil {
		return 0, fmt.Errorf("wal: write payload: %w", err)
	}
	if _, err := l.writer.Write(checksum); err != nil {
		return 0, fmt.Errorf("wal: write checksum: %w", err)
	}

	l.written += recordSize
	l.unsynced += recordSize
	seq := l.seq

	l.statsMu.Lock()
	l.stats.appends++
	l.stats.size = l.written
	l.stats.unsynced = l.unsynced
	l.statsMu.Unlock()

	if l.cfg.SyncEach {
		if err := l.flushLocked(); err != nil {
			return seq, err
		}
	}
	return seq, nil
}

// Sync forces the buffered data to disk.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushLocked()
}

func (l *Log) flushLocked() error {
	if l.writer == nil {
		return nil
	}
	if err := l.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if l.unsynced > 0 {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
		l.unsynced = 0
	}
	l.statsMu.Lock()
	l.stats.syncs++
	l.stats.lastSync = time.Now()
	l.stats.unsynced = 0
	l.statsMu.Unlock()

	select {
	case l.synced <- struct{}{}:
	default:
	}
	return nil
}

// flush is the lock-taking variant used by the background syncer.
func (l *Log) flush(force bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if l.unsynced == 0 && !force {
		return nil
	}
	return l.flushLocked()
}

func (l *Log) rotate(next int64) error {
	if l.file != nil {
		if err := l.flushLocked(); err != nil {
			return err
		}
		if err := l.file.Close(); err != nil {
			return fmt.Errorf("wal: close segment: %w", err)
		}
	}
	path := filepath.Join(l.cfg.Dir, segmentName(next))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	l.file = file
	l.writer = bufio.NewWriterSize(file, 1<<20)
	l.segment = next
	l.written = 0
	l.unsynced = 0

	l.statsMu.Lock()
	l.stats.rotations++
	l.stats.segment = next
	l.stats.size = 0
	l.statsMu.Unlock()

	return l.pruneSegments()
}

// pruneSegments enforces MaxSegments by removing the oldest closed segments.
func (l *Log) pruneSegments() error {
	if l.cfg.MaxSegments <= 0 {
		return nil
	}
	segments, err := l.segments()
	if err != nil {
		return err
	}
	if len(segments) <= l.cfg.MaxSegments {
		return nil
	}
	for _, path := range segments[:len(segments)-l.cfg.MaxSegments] {
		index, err := segmentIndex(path)
		if err != nil || index == l.segment {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: prune segment: %w", err)
		}
	}
	return nil
}

func (l *Log) segments() ([]string, error) {
	entries, err := os.ReadDir(l.cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read directory: %w", err)
	}
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		paths = append(paths, filepath.Join(l.cfg.Dir, name))
	}
	sort.Slice(paths, func(i, j int) bool {
		left, errLeft := segmentIndex(paths[i])
		right, errRight := segmentIndex(paths[j])
		if errLeft != nil || errRight != nil {
			return paths[i] < paths[j]
		}
		return left < right
	})
	return paths, nil
}

func segmentName(index int64) string {
	return fmt.Sprintf("%s%08d%s", segmentPrefix, index, segmentSuffix)
}

func segmentIndex(path string) (int64, error) {
	name := filepath.Base(path)
	name = strings.TrimPrefix(name, segmentPrefix)
	name = strings.TrimSuffix(name, segmentSuffix)
	return strconv.ParseInt(name, 10, 64)
}

// Entry is one replayed record.
type Entry struct {
	Seq     uint64
	Payload []byte
}

// Truncation reports where replay stopped early, if it did.
type Truncation struct {
	Segment int64
	Offset  int64
	Err     error
}

// Reader replays the log in sequence order.
type Reader struct {
	log        *Log
	paths      []string
	index      int
	file       *os.File
	reader     *bufio.Reader
	from       uint64
	Truncation *Truncation
}

// NewReader opens a reader positioned at the first record with seq >= from.
func (l *Log) NewReader(from uint64) (*Reader, error) {
	// Replay must see everything already written, so flush first.
	if err := l.Sync(); err != nil {
		return nil, err
	}
	segments, err := l.segments()
	if err != nil {
		return nil, err
	}
	return &Reader{log: l, paths: segments, from: from}, nil
}

// Next returns the next record, or io.EOF at the end of the log.
func (r *Reader) Next() (Entry, error) {
	for {
		if r.reader == nil {
			if r.index >= len(r.paths) {
				return Entry{}, io.EOF
			}
			path := r.paths[r.index]
			index, err := segmentIndex(path)
			if err != nil {
				return Entry{}, err
			}
			file, err := os.Open(path)
			if err != nil {
				return Entry{}, fmt.Errorf("wal: open segment for replay: %w", err)
			}
			r.file = file
			r.reader = bufio.NewReaderSize(file, 1<<20)
			r.index++
			_ = index
		}

		entry, err := readRecord(r.reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if r.file != nil {
					_ = r.file.Close()
					r.file = nil
				}
				r.reader = nil
				continue
			}
			if errors.Is(err, ErrCorrupt) {
				// Stop at the first bad record rather than replaying garbage.
				r.Truncation = &Truncation{Err: err}
				if r.file != nil {
					_ = r.file.Close()
					r.file = nil
					r.reader = nil
				}
				return Entry{}, io.EOF
			}
			return Entry{}, err
		}
		if entry.Seq < r.from {
			continue
		}
		return entry, nil
	}
}

// Close releases the current segment file.
func (r *Reader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		r.reader = nil
		return err
	}
	return nil
}

func readRecord(reader *bufio.Reader) (Entry, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return Entry{}, err
	}
	seq := binary.LittleEndian.Uint64(header[:8])
	length := binary.LittleEndian.Uint32(header[8:])
	if length == 0 || length > 64<<20 {
		return Entry{}, fmt.Errorf("%w: implausible length %d", ErrCorrupt, length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Entry{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	checksum := make([]byte, checksumSize)
	if _, err := io.ReadFull(reader, checksum); err != nil {
		return Entry{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(checksum) {
		return Entry{}, fmt.Errorf("%w: checksum mismatch at seq %d", ErrCorrupt, seq)
	}
	return Entry{Seq: seq, Payload: payload}, nil
}

// Stats is a snapshot of log activity.
type Stats struct {
	Appends     int64
	Syncs       int64
	Rotations   int64
	Segment     int64
	Size        int64
	Unsynced    int64
	Truncations int64
	LastSync    time.Time
}

// Stats returns the current counters.
func (l *Log) Stats() Stats {
	l.statsMu.Lock()
	defer l.statsMu.Unlock()
	return Stats{
		Appends:     l.stats.appends,
		Syncs:       l.stats.syncs,
		Rotations:   l.stats.rotations,
		Segment:     l.stats.segment,
		Size:        l.stats.size,
		Unsynced:    l.stats.unsynced,
		Truncations: l.stats.truncations,
		LastSync:    l.stats.lastSync,
	}
}

// Close flushes, syncs and stops the background syncer.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	close(l.stop)
	<-l.done

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
		if err := l.file.Sync(); err != nil {
			return err
		}
		return l.file.Close()
	}
	return nil
}
