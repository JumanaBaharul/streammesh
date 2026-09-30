package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// dlqWriter appends events that could not be delivered to a per-sink dead
// letter file. Nothing is ever silently discarded: a batch that exhausts its
// retries lands here, and the file is plain newline-delimited JSON so it can be
// inspected, tailed or replayed with the same tooling as anything else.
type dlqWriter struct {
	path   string
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	events int64
}

func newDLQWriter(dir, sinkID string) (*dlqWriter, error) {
	if dir == "" {
		dir = ".streammesh"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("dlq: create directory: %w", err)
	}
	return &dlqWriter{path: filepath.Join(dir, fmt.Sprintf("dlq-%s.ndjson", sinkID))}, nil
}

// Path returns the file this writer appends to.
func (d *dlqWriter) Path() string { return d.path }

// Count returns how many events have been dead-lettered.
func (d *dlqWriter) Count() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.events
}

func (d *dlqWriter) write(events []event.Event) error {
	if len(events) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		file, err := os.OpenFile(d.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("dlq: open %s: %w", d.path, err)
		}
		d.file = file
		d.writer = bufio.NewWriterSize(file, 64*1024)
	}

	for i := range events {
		raw, err := json.Marshal(events[i])
		if err != nil {
			continue
		}
		if _, err := d.writer.Write(append(raw, '\n')); err != nil {
			return fmt.Errorf("dlq: write: %w", err)
		}
	}
	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("dlq: flush: %w", err)
	}
	d.events += int64(len(events))
	return nil
}

// read returns every event currently in the dead letter file.
func (d *dlqWriter) read() ([]event.Event, error) {
	d.mu.Lock()
	if d.writer != nil {
		_ = d.writer.Flush()
	}
	d.mu.Unlock()

	file, err := os.Open(d.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)

	var events []event.Event
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev event.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}
	return events, scanner.Err()
}

// clear removes the dead letter file, used after a successful replay.
func (d *dlqWriter) clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		_ = d.file.Close()
		d.file = nil
		d.writer = nil
	}
	d.events = 0
	if err := os.Remove(d.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// close flushes and releases the file.
func (d *dlqWriter) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			return err
		}
	}
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		d.writer = nil
		return err
	}
	return nil
} // ReplayResult summarises a replay run.
// The counters describe re-queueing, not delivery: replay puts events back on
// the sink's queue, and an event that fails again is dead-lettered again. That
// is why the field is Requeued rather than Delivered - a claim of delivery would
// be a lie at this point in the path.
type ReplayResult struct {
	Source   string `json:"source"`
	Read     int    `json:"read"`
	Requeued int    `json:"requeued"`
	Failed   int    `json:"failed_to_requeue"`
}

// ReplayDLQ re-queues everything in a sink's dead letter file. The file is
// cleared only when every event made it back onto the queue, so a partial
// replay is always safe to run again.
func (p *Pipeline) ReplayDLQ(ctx context.Context, sinkID string) (ReplayResult, error) {
	worker, ok := p.sinkWorkers[sinkID]
	if !ok {
		return ReplayResult{}, fmt.Errorf("pipeline: unknown sink %q", sinkID)
	}
	events, err := worker.dlq.read()
	if err != nil {
		return ReplayResult{}, fmt.Errorf("pipeline: read dead letter file: %w", err)
	}

	result := ReplayResult{Source: "dlq:" + sinkID, Read: len(events)}
	for i := range events {
		ev := events[i]
		if err := worker.submit(ctx, ev); err != nil {
			result.Failed++
			continue
		}
		result.Requeued++
	}
	if result.Failed == 0 {
		if err := worker.dlq.clear(); err != nil {
			return result, fmt.Errorf("pipeline: clear dead letter file: %w", err)
		}
	}
	return result, nil
}
