// Package source implements the ingest connectors.
//
// A source owns its transport and its framing, and hands canonical events to
// the pipeline through the Emit callback. Because every source shares the same
// parser registry and the same normalisation path, adding a new transport never
// means adding new parsing logic.
package source

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/parse"
)

// Source is one ingest endpoint.
type Source interface {
	// ID is the configured identifier, used as the event's source_id.
	ID() string
	// Start begins serving and blocks until ctx is cancelled or a fatal error
	// occurs. It must return promptly when ctx is cancelled.
	Start(ctx context.Context) error
	// Close releases listener resources.
	Close() error
	// Snapshot reports this source's counters for the control plane.
	Snapshot() Stats
}

// Stats is a source's observable state.
type Stats struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Received    int64  `json:"received"`
	ParseErrors int64  `json:"parse_errors"`
	Bytes       int64  `json:"bytes"`
	Dropped     int64  `json:"dropped"`
	Errors      int64  `json:"errors"`
}

// Snapshot implements Source.
func (b *Base) Snapshot() Stats {
	stats := Stats{ID: b.cfg.ID, Type: b.cfg.Type}
	if b.received != nil {
		stats.Received = int64(b.received.Value())
	}
	if b.parseErrors != nil {
		stats.ParseErrors = int64(b.parseErrors.Value())
	}
	if b.bytes != nil {
		stats.Bytes = int64(b.bytes.Value())
	}
	if b.dropped != nil {
		stats.Dropped = int64(b.dropped.Value())
	}
	if b.errors != nil {
		stats.Errors = int64(b.errors.Value())
	}
	return stats
}

// Addressed is implemented by sources that listen on a network address. Tests
// and the startup log use it to discover an ephemeral port.
type Addressed interface {
	// Addr is the bound address, or an empty string before the listener exists.
	Addr() string
}

// Deps is what a source needs from the engine.
type Deps struct {
	// Emit hands events to the pipeline. It blocks when the pipeline is full,
	// which is how backpressure reaches the edge.
	Emit func([]event.Event) error
	// Registry receives per-source metrics; may be nil.
	Registry *metrics.Registry
	// Logger is the structured logger; may be nil.
	Logger *slog.Logger
}

// Build constructs a source from its configuration.
func Build(cfg config.Source, deps Deps) (Source, error) {
	parser, err := parse.Build(cfg.Parser, cfg.ParserOptions)
	if err != nil {
		return nil, fmt.Errorf("source %q: %w", cfg.ID, err)
	}
	// Normalise the tag list once at build time, so every event from this source
	// carries a clean, deduplicated set instead of repeating the config verbatim.
	cfg.DefaultTags = splitTags(cfg.DefaultTags)
	base := Base{
		cfg:    cfg,
		parser: parser,
		logger: deps.Logger,
		emit:   deps.Emit,
	}
	if deps.Registry != nil {
		labels := map[string]string{"source": cfg.ID}
		base.received = deps.Registry.Counter("streammesh_source_events_total", "Events accepted from a source, before routing.", labels)
		base.parseErrors = deps.Registry.Counter("streammesh_source_parse_errors_total", "Payloads a source could not parse.", labels)
		base.bytes = deps.Registry.Counter("streammesh_source_bytes_total", "Bytes read from a source.", labels)
		base.dropped = deps.Registry.Counter("streammesh_source_dropped_total", "Events dropped at the source, for example by backpressure.", labels)
		base.errors = deps.Registry.Counter("streammesh_source_errors_total", "Transport errors seen by a source.", labels)
	}

	switch cfg.Type {
	case "http":
		return newHTTPSource(base)
	case "webhook":
		return newWebhookSource(base)
	case "syslog":
		return newSyslogSource(base)
	case "file":
		return newFileSource(base)
	default:
		return nil, fmt.Errorf("source %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

// Types lists the registered source types.
func Types() []string {
	types := []string{"http", "webhook", "syslog", "file"}
	sort.Strings(types)
	return types
}

// Base carries the behaviour every source shares: parsing, tagging and
// delivery, plus this source's metric handles.
type Base struct {
	cfg    config.Source
	parser parse.Parser
	logger *slog.Logger
	emit   func([]event.Event) error

	received    *metrics.Counter
	parseErrors *metrics.Counter
	bytes       *metrics.Counter
	dropped     *metrics.Counter
	errors      *metrics.Counter
}

// ID returns the source identifier.
func (b *Base) ID() string { return b.cfg.ID }

// Config exposes the source configuration.
func (b *Base) Config() config.Source { return b.cfg }

// Close is a no-op for sources that own no OS resources.
func (b *Base) Close() error { return nil }

// decode parses a payload and normalises every event it contains.
//
// A parser that recovered some records returns them together with an error, so
// the data is kept and the rejection is counted rather than thrown away.
func (b *Base) decode(data []byte) ([]event.Event, error) {
	if b.bytes != nil {
		b.bytes.Add(float64(len(data)))
	}
	events, err := b.parser.Parse(b.cfg.ID, data)
	if err != nil {
		if b.parseErrors != nil {
			b.parseErrors.Inc()
		}
	}
	if len(events) == 0 && err != nil {
		return nil, err
	}
	for i := range events {
		ev := &events[i]
		ev.Source = b.cfg.ID
		if b.cfg.Category != "" {
			ev.Category = b.cfg.Category
		}
		ev.Tags = append(ev.Tags, b.cfg.DefaultTags...)
		ev.Normalize()
	}
	return events, nil
}

// deliver passes events to the pipeline, counting what happened.
func (b *Base) deliver(events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	if b.received != nil {
		b.received.Add(float64(len(events)))
	}
	if b.emit == nil {
		return nil
	}
	if err := b.emit(events); err != nil {
		if b.dropped != nil {
			b.dropped.Add(float64(len(events)))
		}
		return err
	}
	return nil
}

// logError records a transport-level failure.
func (b *Base) logError(err error, msg string, args ...any) {
	if b.errors != nil {
		b.errors.Inc()
	}
	if b.logger == nil {
		return
	}
	args = append([]any{"source", b.cfg.ID, "error", err.Error()}, args...)
	b.logger.Warn(msg, args...)
}

func (b *Base) logInfo(msg string, args ...any) {
	if b.logger == nil {
		return
	}
	args = append([]any{"source", b.cfg.ID}, args...)
	b.logger.Info(msg, args...)
}

// splitTags normalises a tag list, dropping blanks and duplicates.
func splitTags(tags []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out
}
