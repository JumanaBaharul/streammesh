package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T, cfg Config) *Log {
	t.Helper()
	log, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return log
}

func readAll(t *testing.T, log *Log) []Entry {
	t.Helper()
	reader, err := log.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var entries []Entry
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func TestAppendAndReplayPreservesOrderAndPayloads(t *testing.T) {
	log := openTemp(t, Config{Dir: t.TempDir(), FsyncInterval: time.Hour})

	payloads := []string{"first", "second", "third"}
	for index, payload := range payloads {
		seq, err := log.Append([]byte(payload))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq != uint64(index+1) {
			t.Fatalf("seq = %d, want %d", seq, index+1)
		}
	}

	entries := readAll(t, log)
	if len(entries) != len(payloads) {
		t.Fatalf("replayed %d entries, want %d", len(entries), len(payloads))
	}
	for index, entry := range entries {
		if string(entry.Payload) != payloads[index] {
			t.Errorf("entry %d = %q, want %q", index, entry.Payload, payloads[index])
		}
		if entry.Seq != uint64(index+1) {
			t.Errorf("entry %d seq = %d", index, entry.Seq)
		}
	}
}

func TestReaderCanStartFromASequence(t *testing.T) {
	log := openTemp(t, Config{Dir: t.TempDir(), FsyncInterval: time.Hour})
	for i := 0; i < 5; i++ {
		if _, err := log.Append([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	reader, err := log.NewReader(3)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var seen []uint64
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		seen = append(seen, entry.Seq)
	}
	if len(seen) != 3 || seen[0] != 3 || seen[2] != 5 {
		t.Fatalf("replayed seqs %v, want [3 4 5]", seen)
	}
}

func TestSyncEachMakesDataDurableImmediately(t *testing.T) {
	dir := t.TempDir()
	log := openTemp(t, Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})

	if _, err := log.Append([]byte("durable")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	stats := log.Stats()
	if stats.Syncs == 0 {
		t.Fatal("SyncEach did not sync the log")
	}
	if stats.Unsynced != 0 {
		t.Fatalf("unsynced bytes = %d, want 0", stats.Unsynced)
	}

	// The data must be on disk before Close, since SyncEach was requested.
	raw, err := os.ReadFile(filepath.Join(dir, segmentName(1)))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(raw) != headerSize+len("durable")+checksumSize {
		t.Fatalf("segment size = %d, want one record", len(raw))
	}
}

func TestBatchedModeReportsItsUnsyncedWindow(t *testing.T) {
	log := openTemp(t, Config{Dir: t.TempDir(), FsyncInterval: time.Hour})

	if _, err := log.Append([]byte("buffered")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if unsynced := log.Stats().Unsynced; unsynced == 0 {
		t.Fatal("expected a non-zero unsynced window before the first flush")
	}

	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if unsynced := log.Stats().Unsynced; unsynced != 0 {
		t.Fatalf("unsynced bytes = %d after Sync, want 0", unsynced)
	}
}

func TestRotationCreatesNewSegmentsAndPrunesOldOnes(t *testing.T) {
	dir := t.TempDir()
	log := openTemp(t, Config{
		Dir:           dir,
		SegmentBytes:  64,
		FsyncInterval: time.Hour,
		MaxSegments:   2,
	})

	for i := 0; i < 20; i++ {
		if _, err := log.Append([]byte("0123456789012345678901234567890123456789")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	stats := log.Stats()
	if stats.Rotations == 0 {
		t.Fatal("expected the log to rotate")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	segments := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			segments++
		}
	}
	if segments > 2 {
		t.Fatalf("segment count = %d, want at most 2 after pruning", segments)
	}
}

func TestReopenResumesTheSequenceAndKeepsData(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := log.Append([]byte("payload")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	seq, err := reopened.Append([]byte("after restart"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if seq != 4 {
		t.Fatalf("seq = %d, want 4 (continuing after the existing records)", seq)
	}

	entries := readAll(t, reopened)
	if len(entries) != 4 {
		t.Fatalf("replayed %d entries, want 4", len(entries))
	}
	if string(entries[3].Payload) != "after restart" {
		t.Fatalf("last entry = %q", entries[3].Payload)
	}
}

// TestTornRecordIsTruncatedOnReopen is the crash-recovery case that matters:
// a partially written trailing record must be discarded, and the next append
// must start at a clean record boundary instead of corrupting the log.
func TestTornRecordIsTruncatedOnReopen(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := log.Append([]byte("complete record")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write by appending a partial record: a header that
	// claims more bytes than follow it.
	path := filepath.Join(dir, segmentName(1))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	partial := make([]byte, headerSize+4)
	partial[8] = 200 // claim a 200 byte payload
	if _, err := file.Write(partial); err != nil {
		t.Fatalf("write partial record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	reopened, err := Open(Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})
	if err != nil {
		t.Fatalf("reopen after a torn write: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if reopened.Stats().Truncations == 0 {
		t.Fatal("expected the torn record to be reported as truncated")
	}
	if _, err := reopened.Append([]byte("recovered")); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}

	entries := readAll(t, reopened)
	if len(entries) != 2 {
		t.Fatalf("replayed %d entries, want 2 (the complete record and the new one)", len(entries))
	}
	if string(entries[0].Payload) != "complete record" {
		t.Errorf("first entry = %q", entries[0].Payload)
	}
	if string(entries[1].Payload) != "recovered" {
		t.Errorf("second entry = %q", entries[1].Payload)
	}
}

func TestCorruptChecksumStopsReplay(t *testing.T) {
	dir := t.TempDir()
	log := openTemp(t, Config{Dir: dir, FsyncInterval: time.Hour, SyncEach: true})

	if _, err := log.Append([]byte("good one")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := log.Append([]byte("will be corrupted")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Flip a byte inside the second payload.
	path := filepath.Join(dir, segmentName(1))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	offset := headerSize + len("good one") + checksumSize + headerSize + 1
	raw[offset] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	reader, err := log.NewReader(0)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	first, err := reader.Next()
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if string(first.Payload) != "good one" {
		t.Fatalf("first entry = %q", first.Payload)
	}

	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF at the corrupted record", err)
	}
	if reader.Truncation == nil {
		t.Fatal("the corruption was not reported")
	}
}

func TestAppendRejectsEmptyPayloadsAndUseAfterClose(t *testing.T) {
	log, err := Open(Config{Dir: t.TempDir(), FsyncInterval: time.Hour})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := log.Append(nil); err == nil {
		t.Fatal("expected an error for an empty payload")
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := log.Append([]byte("after close")); err == nil {
		t.Fatal("expected an error when appending after Close")
	}
}

func TestOpenRequiresADirectory(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("expected an error when no directory is configured")
	}
}

func TestConcurrentAppendsKeepDistinctSequences(t *testing.T) {
	log := openTemp(t, Config{Dir: t.TempDir(), FsyncInterval: 10 * time.Millisecond})

	const writers = 8
	const perWriter = 200
	done := make(chan struct{}, writers)
	for writer := 0; writer < writers; writer++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perWriter; i++ {
				if _, err := log.Append([]byte("concurrent")); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}
	for writer := 0; writer < writers; writer++ {
		<-done
	}

	entries := readAll(t, log)
	if len(entries) != writers*perWriter {
		t.Fatalf("replayed %d entries, want %d", len(entries), writers*perWriter)
	}
	seen := map[uint64]bool{}
	for _, entry := range entries {
		if seen[entry.Seq] {
			t.Fatalf("duplicate sequence number %d", entry.Seq)
		}
		seen[entry.Seq] = true
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("distinct sequences = %d, want %d", len(seen), writers*perWriter)
	}
}

func BenchmarkAppendBuffered(b *testing.B) {
	log, err := Open(Config{Dir: b.TempDir(), FsyncInterval: time.Hour})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = log.Close() }()

	payload := []byte(`{"id":"benchmark","source_id":"svc","severity":"info","message":"benchmark payload"}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.Append(payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}
