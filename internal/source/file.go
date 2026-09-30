package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// fileSource tails a log file and resumes from a stored byte offset, so a
// restart during a deploy does not replay or skip the window around it.
//
// Rotation and truncation are detected without relying on inode numbers, which
// Windows does not expose: a file whose size shrank below the stored offset has
// been truncated or replaced, and the reader resets to the beginning of the new
// file. A genuinely replaced file that happens to be larger than the old offset
// is only detected on the next shrink, which is documented rather than guessed.
type fileSource struct {
	Base
	mu      sync.Mutex
	state   fileState
	tailers *offsetStore
}

type fileState struct {
	Offset  int64  `json:"offset"`
	Size    int64  `json:"size"`
	Partial []byte `json:"-"`
}

type offsetStore struct {
	path   string
	mu     sync.Mutex
	states map[string]fileState
}

func newFileSource(base Base) (Source, error) {
	store := &offsetStore{
		path:   base.cfg.OffsetsFile,
		states: map[string]fileState{},
	}
	if store.path == "" {
		store.path = filepath.Join(".streammesh", "tailer-offsets.json")
	}
	return &fileSource{Base: base, tailers: store}, nil
}

func (s *fileSource) Start(ctx context.Context) error {
	if err := s.tailers.load(); err != nil {
		// A missing or unreadable offset file is not fatal: start from the
		// configured position instead.
		s.logError(err, "cannot load tailer offsets, starting fresh")
	}

	if err := s.initialize(); err != nil {
		s.logError(err, "cannot position tailer")
	}

	interval := s.cfg.PollInterval.Duration()
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	persist := time.NewTicker(2 * time.Second)
	defer persist.Stop()

	s.logInfo("tailer started", "file", s.cfg.File, "poll", interval.String(), "start_at", s.position())

	for {
		select {
		case <-ctx.Done():
			if err := s.tailers.save(); err != nil {
				s.logError(err, "cannot persist tailer offsets on shutdown")
			}
			return nil
		case <-persist.C:
			if err := s.tailers.save(); err != nil {
				s.logError(err, "cannot persist tailer offsets")
			}
		case <-ticker.C:
			if err := s.poll(); err != nil {
				s.logError(err, "tail poll failed")
			}
		}
	}
}

func (s *fileSource) position() string {
	if s.cfg.StartAt == "start" {
		return "start"
	}
	return "end"
}

// initialize resolves the starting offset: a stored offset wins so restarts are
// seamless, otherwise start_at decides.
func (s *fileSource) initialize() error {
	info, err := os.Stat(s.cfg.File)
	if err != nil {
		if os.IsNotExist(err) {
			// The file may be created later by the producer being watched.
			s.mu.Lock()
			s.state = fileState{}
			s.mu.Unlock()
			return nil
		}
		return fmt.Errorf("stat %s: %w", s.cfg.File, err)
	}

	s.tailers.mu.Lock()
	stored, found := s.tailers.states[s.cfg.File]
	s.tailers.mu.Unlock()

	offset := int64(0)
	if s.position() == "end" {
		offset = info.Size()
	}
	if found && stored.Offset <= info.Size() {
		offset = stored.Offset
	}

	s.mu.Lock()
	s.state = fileState{Offset: offset, Size: info.Size()}
	s.mu.Unlock()
	return nil
}

// poll reads whatever is new and emits complete lines. A partial trailing line
// stays buffered until its newline arrives, so a record is never split.
func (s *fileSource) poll() error {
	info, err := os.Stat(s.cfg.File)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", s.cfg.File, err)
	}

	file, err := os.Open(s.cfg.File)
	if err != nil {
		return fmt.Errorf("open %s: %w", s.cfg.File, err)
	}
	defer func() { _ = file.Close() }()

	s.mu.Lock()
	state := s.state
	if info.Size() < state.Offset {
		// Truncated in place, or rotated onto a shorter file: restart at the
		// beginning and drop the stale partial line.
		s.logInfo("log file truncated or rotated, restarting from offset 0",
			"previous_offset", state.Offset, "size", info.Size())
		state.Offset = 0
		state.Partial = nil
	}
	s.mu.Unlock()

	if info.Size() == state.Offset {
		return nil
	}
	if _, err := file.Seek(state.Offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek %s: %w", s.cfg.File, err)
	}

	reader := io.LimitReader(file, info.Size()-state.Offset)
	chunk, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read %s: %w", s.cfg.File, err)
	}

	buffer := append(state.Partial, chunk...)
	offset := state.Offset + int64(len(chunk))

	var lines [][]byte
	for {
		index := indexByte(buffer, '\n')
		if index < 0 {
			break
		}
		line := buffer[:index]
		buffer = buffer[index+1:]
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}

	// A line that exceeds the cap is emitted as a single truncated event rather
	// than growing memory without bound.
	if s.cfg.MaxLineBytes > 0 && len(buffer) > s.cfg.MaxLineBytes {
		lines = append(lines, buffer[:s.cfg.MaxLineBytes])
		buffer = nil
		s.logError(fmt.Errorf("line exceeded %d bytes", s.cfg.MaxLineBytes),
			"oversized log line truncated")
	}

	s.mu.Lock()
	s.state = fileState{Offset: offset, Size: info.Size(), Partial: buffer}
	s.mu.Unlock()

	if len(lines) == 0 {
		return nil
	}

	var events []event.Event
	for _, line := range lines {
		decoded, err := s.decode(line)
		if err != nil {
			s.logError(err, "partially rejected log line")
		}
		events = append(events, decoded...)
	}
	if err := s.deliver(events); err != nil {
		// The offset has already advanced, so the events stay in the write-ahead
		// log and can be replayed instead of being read again from the file.
		return err
	}
	s.remember()
	return nil
}

func indexByte(data []byte, target byte) int {
	return strings.IndexByte(string(data), target)
}

// Close persists offsets.
func (s *fileSource) Close() error {
	return s.tailers.save()
}

func (o *offsetStore) load() error {
	raw, err := os.ReadFile(o.path)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return json.Unmarshal(raw, &o.states)
}

func (o *offsetStore) save() error {
	o.mu.Lock()
	snapshot := make(map[string]fileState, len(o.states))
	for key, value := range o.states {
		snapshot[key] = value
	}
	o.mu.Unlock()

	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(o.path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(o.path, raw, 0o644)
}

// remember records the current offset for this file.
func (s *fileSource) remember() {
	s.mu.Lock()
	state := s.state
	s.mu.Unlock()

	s.tailers.mu.Lock()
	s.tailers.states[s.cfg.File] = fileState{Offset: state.Offset, Size: state.Size}
	s.tailers.mu.Unlock()
}
