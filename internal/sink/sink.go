// Package sink implements delivery destinations.
//
// A sink performs exactly one attempt per Write. Retries, batching and dead
// lettering belong to the pipeline's delivery workers, which keeps every
// connector small and makes delivery policy identical no matter the destination.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
)

// Sink is one destination.
type Sink interface {
	// ID is the configured identifier.
	ID() string
	// Write delivers a batch. Returning an error means the whole batch is
	// retried; sinks that can partially succeed must report failure rather than
	// silently dropping records.
	Write(ctx context.Context, events []event.Event) error
	// Close flushes anything buffered.
	Close(ctx context.Context) error
	// Health reports why the sink is currently unable to accept data.
	Health() error
	// Config exposes the sink configuration.
	Config() config.Sink
}

// Buffer is implemented by sinks that retain events in memory. It is how tests
// and the demo inspect what was actually delivered without touching disk.
type Buffer interface {
	Sink
	// Recent returns up to n of the most recently written events, oldest first.
	Recent(n int) []event.Event
	// Total returns how many events this sink has received.
	Total() int64
}

// Deps is what a sink needs from the engine.
type Deps struct {
	Registry *metrics.Registry
	Logger   *slog.Logger
}

// Build constructs a sink from its configuration.
func Build(cfg config.Sink, deps Deps) (Sink, error) {
	base := &Base{cfg: cfg, logger: deps.Logger}
	if deps.Registry != nil {
		labels := map[string]string{"sink": cfg.ID}
		base.written = deps.Registry.Counter("streammesh_sink_events_total", "Events successfully written to a sink.", labels)
		base.bytes = deps.Registry.Counter("streammesh_sink_bytes_total", "Bytes written to a sink.", labels)
		base.failures = deps.Registry.Counter("streammesh_sink_failures_total", "Sink writes that failed, before retries.", labels)
	}

	switch cfg.Type {
	case "file":
		return newFileSink(base)
	case "http":
		return newHTTPSink(base)
	case "kafka":
		return newKafkaSink(base)
	case "s3":
		return newS3Sink(base)
	case "memory":
		return newMemorySink(base)
	default:
		return nil, fmt.Errorf("sink %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

// Types lists the registered sink types.
func Types() []string {
	types := []string{"file", "http", "kafka", "s3", "memory"}
	sort.Strings(types)
	return types
}

// Base carries the bookkeeping every sink shares.
type Base struct {
	cfg    config.Sink
	logger *slog.Logger

	written  *metrics.Counter
	bytes    *metrics.Counter
	failures *metrics.Counter

	mu        sync.Mutex
	lastErr   string
	lastErrAt time.Time
}

// ID returns the sink identifier.
func (b *Base) ID() string { return b.cfg.ID }

// Config exposes the sink configuration.
func (b *Base) Config() config.Sink { return b.cfg }

// noteSuccess clears the health error and records the batch.
func (b *Base) noteSuccess(events []event.Event, size int) {
	if b.written != nil {
		b.written.Add(float64(len(events)))
	}
	if b.bytes != nil {
		b.bytes.Add(float64(size))
	}
	b.mu.Lock()
	b.lastErr = ""
	b.mu.Unlock()
}

// noteFailure records the reason the sink is unhealthy.
func (b *Base) noteFailure(err error) {
	if b.failures != nil {
		b.failures.Inc()
	}
	b.mu.Lock()
	b.lastErr = err.Error()
	b.lastErrAt = time.Now()
	b.mu.Unlock()
}

// Health reports the last recorded failure, if any.
func (b *Base) Health() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lastErr == "" {
		return nil
	}
	return fmt.Errorf("sink %q: last error at %s: %s", b.cfg.ID, b.lastErrAt.UTC().Format(time.RFC3339), b.lastErr)
}

// Close is a no-op for sinks with nothing buffered.
func (b *Base) Close(context.Context) error { return nil }

// encodeBatch renders events as newline-delimited canonical JSON.
func encodeBatch(events []event.Event) ([]byte, error) {
	var buffer []byte
	for i := range events {
		raw, err := json.Marshal(events[i])
		if err != nil {
			return nil, fmt.Errorf("sink: encode event: %w", err)
		}
		buffer = append(buffer, raw...)
		buffer = append(buffer, '\n')
	}
	return buffer, nil
}

func (b *Base) logInfo(msg string, args ...any) {
	if b.logger == nil {
		return
	}
	args = append([]any{"sink", b.cfg.ID}, args...)
	b.logger.Info(msg, args...)
}
