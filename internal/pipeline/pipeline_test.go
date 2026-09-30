package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/rules"
	"github.com/JumanaBaharul/streammesh/internal/sink"
)

func boolPtr(value bool) *bool { return &value }

func testConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func startPipeline(t *testing.T, cfg *config.Config) *Pipeline {
	t.Helper()
	pipe, err := New(Options{Config: cfg, Registry: metrics.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = pipe.Shutdown(ctx)
	})
	return pipe
}

func makeEvent(t *testing.T, source, category string, severity event.Severity, id string) event.Event {
	t.Helper()
	ev := event.New(source)
	ev.ID = id
	ev.Category = category
	ev.Severity = severity
	ev.Host = "edge-01"
	ev.Attrs["message"] = "request completed"
	return ev
}

// waitFor polls until the condition holds, so tests do not depend on sleeps.
// The deadline is generous because `go test ./...` runs packages in parallel and
// a loaded machine can starve a delivery worker for seconds at a time.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func bufferSink(t *testing.T, pipe *Pipeline, id string) sink.Buffer {
	t.Helper()
	target, ok := pipe.Sinks()[id]
	if !ok {
		t.Fatalf("sink %q is not configured", id)
	}
	buffer, ok := target.(sink.Buffer)
	if !ok {
		t.Fatalf("sink %q is not a memory sink", id)
	}
	return buffer
}

func TestPipelineRoutesRedactsAndDrops(t *testing.T) {
	cfg := testConfig(t, `
pipeline:
  workers: 2
  queue_size: 64
  wal:
    enabled: false
  dedup:
    enabled: false
dlp:
  rule_sets:
    - name: pii
      patterns:
        - name: email
          regex: '[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}'
          replace: "[redacted:email]"
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: recent
    type: memory
    buffer_size: 64
rules:
  - name: drop-healthcheck
    match:
      field: category
      equals: healthcheck
    actions:
      - drop: true
  - name: scrub
    match:
      always: true
    actions:
      - redact: pii
  - name: default
    match:
      always: true
    actions:
      - route: [recent]
`)
	pipe := startPipeline(t, cfg)
	recent := bufferSink(t, pipe, "recent")

	normal := makeEvent(t, "svc", "api", event.SeverityInfo, "evt-normal")
	normal.Attrs["actor_email"] = "alice@example.com"
	noise := makeEvent(t, "svc", "healthcheck", event.SeverityDebug, "evt-noise")

	if err := pipe.Ingest([]event.Event{normal, noise}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitFor(t, "the routed event to be delivered", func() bool { return recent.Total() >= 1 })

	delivered := recent.Recent(0)
	if len(delivered) != 1 {
		t.Fatalf("delivered %d events, want only the non-healthcheck one", len(delivered))
	}
	if got := event.ValueString(delivered[0].Attrs["actor_email"]); got != "[redacted:email]" {
		t.Errorf("email was not redacted: %q", got)
	}
	if delivered[0].ID != "evt-normal" {
		t.Errorf("delivered id = %q", delivered[0].ID)
	}

	stats := pipe.Stats()
	if stats.RulesDropped != 1 {
		t.Errorf("rules dropped = %d, want 1", stats.RulesDropped)
	}
	if stats.Ingested != 2 {
		t.Errorf("ingested = %d, want 2", stats.Ingested)
	}
}

func TestPipelineSuppressesDuplicateIDs(t *testing.T) {
	cfg := testConfig(t, `
pipeline:
  queue_size: 64
  wal:
    enabled: false
  dedup:
    enabled: true
    ttl: 1m
    bits: 1048576
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: recent
    type: memory
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [recent]
`)
	pipe := startPipeline(t, cfg)
	recent := bufferSink(t, pipe, "recent")

	first := makeEvent(t, "svc", "api", event.SeverityInfo, "same-id")
	second := makeEvent(t, "svc", "api", event.SeverityInfo, "same-id")

	if err := pipe.Ingest([]event.Event{first, second}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// A third, distinct event proves the duplicate filter is not simply
	// discarding everything.
	third := makeEvent(t, "svc", "api", event.SeverityInfo, "other-id")
	if err := pipe.Ingest([]event.Event{third}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitFor(t, "two distinct events", func() bool { return recent.Total() >= 2 })

	stats := pipe.Stats()
	if stats.Duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", stats.Duplicates)
	}
	if recent.Total() != 2 {
		t.Errorf("delivered %d events, want 2", recent.Total())
	}
}

func TestPipelineCountsUnroutedEvents(t *testing.T) {
	cfg := testConfig(t, `
pipeline:
  queue_size: 16
  wal:
    enabled: false
  dedup:
    enabled: false
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: recent
    type: memory
rules:
  - name: only-errors
    match:
      severity: error
    actions:
      - route: [recent]
`)
	pipe := startPipeline(t, cfg)

	quiet := makeEvent(t, "svc", "api", event.SeverityInfo, "quiet")
	if err := pipe.Ingest([]event.Event{quiet}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitFor(t, "the event to be counted as unrouted", func() bool { return pipe.Stats().Unrouted == 1 })
}

func TestPipelineDefaultSinksCatchUnroutedTraffic(t *testing.T) {
	cfg := testConfig(t, `
pipeline:
  queue_size: 64
  default_sinks: [recent]
  wal:
    enabled: false
  dedup:
    enabled: false
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: recent
    type: memory
rules:
  - name: only-errors
    match:
      severity: error
    actions:
      - route: [recent]
`)
	pipe := startPipeline(t, cfg)
	recent := bufferSink(t, pipe, "recent")

	quiet := makeEvent(t, "svc", "api", event.SeverityInfo, "quiet")
	if err := pipe.Ingest([]event.Event{quiet}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitFor(t, "the default sink to receive the event", func() bool { return recent.Total() == 1 })
}

func TestPipelineDeadLettersWhenASinkNeverAccepts(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, `
pipeline:
  queue_size: 64
  shutdown_grace: 5s
  wal:
    enabled: false
    dir: `+dir+`
  dedup:
    enabled: false
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: broken
    type: http
    url: "http://127.0.0.1:1/collect"
    batch_size: 1
    flush_interval: 5ms
    retry:
      max_attempts: 2
      base: 1ms
      max: 2ms
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [broken]
`)
	pipe := startPipeline(t, cfg)

	ev := makeEvent(t, "svc", "api", event.SeverityError, "cannot-deliver")
	if err := pipe.Ingest([]event.Event{ev}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitFor(t, "the event to be dead-lettered", func() bool {
		for _, stats := range pipe.Stats().Sinks {
			if stats.DeadLettered > 0 {
				return true
			}
		}
		return false
	})

	stats := pipe.Stats()
	var deadLetterPath string
	for _, s := range stats.Sinks {
		if s.DeadLettered > 0 {
			deadLetterPath = s.DeadLetter
			if s.Health == "ok" {
				t.Error("a sink that failed every write should not report healthy")
			}
		}
	}
	if deadLetterPath == "" {
		t.Fatal("no dead letter file was recorded")
	}

	raw := readFile(t, deadLetterPath)
	if !strings.Contains(raw, "cannot-deliver") {
		t.Fatalf("dead letter file does not contain the event:\n%s", raw)
	}

	// Replaying puts the event back on the queue. The sink is still broken, so
	// the event must come back to the dead letter file rather than vanish: that
	// is the no-loss property replay exists to provide.
	result, err := pipe.ReplayDLQ(context.Background(), "broken")
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if result.Read != 1 {
		t.Fatalf("replay read %d events, want 1", result.Read)
	}
	if result.Requeued != 1 {
		t.Fatalf("replay requeued %d events, want 1", result.Requeued)
	}

	// Replay clears the file, so the event reappearing in it proves the event
	// survived the round trip rather than being dropped somewhere in between.
	waitFor(t, "the replayed event to be dead-lettered again", func() bool {
		raw, err := os.ReadFile(deadLetterPath)
		return err == nil && strings.Contains(string(raw), "cannot-deliver")
	})
}

func TestPipelineReplaysFromTheWriteAheadLog(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, `
pipeline:
  queue_size: 128
  wal:
    enabled: true
    dir: `+dir+`
    segment_bytes: 1MB
    fsync_interval: 10ms
  dedup:
    enabled: true
    ttl: 1m
    bits: 1048576
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: recent
    type: memory
    buffer_size: 128
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [recent]
`)
	pipe := startPipeline(t, cfg)
	recent := bufferSink(t, pipe, "recent")

	const count = 20
	events := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, makeEvent(t, "svc", "api", event.SeverityInfo, eventID(i)))
	}
	if err := pipe.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitFor(t, "the first pass to be delivered", func() bool { return recent.Total() >= count })

	stats := pipe.Stats()
	if !stats.WAL.Enabled {
		t.Fatal("the write-ahead log should be enabled")
	}
	if stats.WAL.Appends != count {
		t.Fatalf("wal appends = %d, want %d", stats.WAL.Appends, count)
	}

	// Replaying must bypass the duplicate filter, because these events are by
	// definition already recorded.
	result, err := pipe.ReplayWAL(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if result.Read != count {
		t.Fatalf("replay read %d records, want %d", result.Read, count)
	}
	if result.Accepted != count {
		t.Fatalf("replay accepted %d records, want %d (errors: %d)", result.Accepted, count, result.Errors)
	}
	if pipe.Stats().Replayed != count {
		t.Fatalf("replayed counter = %d, want %d", pipe.Stats().Replayed, count)
	}
}

func TestShutdownDrainsAcceptedEvents(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, `
pipeline:
  workers: 2
  queue_size: 4096
  shutdown_grace: 15s
  wal:
    enabled: true
    dir: `+dir+`
    fsync_interval: 10ms
  dedup:
    enabled: true
    ttl: 1m
    bits: 4194304
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: archive
    type: file
    file: `+dir+`/archive.ndjson
    batch_size: 50
    flush_interval: 10ms
  - id: recent
    type: memory
    buffer_size: 8192
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [archive, recent]
`)

	pipe, err := New(Options{Config: cfg, Registry: metrics.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const count = 500
	events := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, makeEvent(t, "svc", "api", event.SeverityInfo, eventID(i)))
	}
	if err := pipe.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipe.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	raw := readFile(t, dir+"/archive.ndjson")
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	if lines != count {
		t.Fatalf("archived %d events, want all %d accepted events to survive shutdown", lines, count)
	}

	if err := pipe.Ingest([]event.Event{makeEvent(t, "svc", "api", event.SeverityInfo, "after-shutdown")}); err == nil {
		t.Fatal("Ingest should fail once the pipeline is closed")
	}
}

// TestPipelineSaturationReturnsAnError proves backpressure reaches the edge: a
// destination that stops answering eventually makes Ingest refuse work instead
// of buffering without bound.
func TestPipelineSaturationReturnsAnError(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	cfg := testConfig(t, `
pipeline:
  workers: 1
  queue_size: 1
  sink_queue_size: 1
  accept_timeout: 100ms
  wal:
    enabled: false
    dir: `+t.TempDir()+`
  dedup:
    enabled: false
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: slow
    type: http
    url: `+slow.URL+`
    batch_size: 1
    flush_interval: 1ms
    timeout: 10s
    retry:
      max_attempts: 1
      base: 1ms
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [slow]
`)
	pipe := startPipeline(t, cfg)

	var lastErr error
	for i := 0; i < 50; i++ {
		lastErr = pipe.Ingest([]event.Event{makeEvent(t, "svc", "api", event.SeverityInfo, eventID(i))})
		if lastErr != nil {
			break
		}
	}
	if lastErr == nil {
		t.Fatal("expected ingest to report backpressure once the sink stalled")
	}
	if !strings.Contains(lastErr.Error(), "saturated") {
		t.Fatalf("unexpected backpressure error: %v", lastErr)
	}
	if pipe.Stats().Saturated == 0 {
		t.Fatal("the saturation counter was not incremented")
	}
}

func TestReloadRulesChangesRoutingWithoutRestart(t *testing.T) {
	cfg := testConfig(t, `
pipeline:
  queue_size: 64
  wal:
    enabled: false
  dedup:
    enabled: false
sources:
  - id: test-http
    type: http
    listen: "127.0.0.1:0"
sinks:
  - id: first
    type: memory
  - id: second
    type: memory
rules:
  - name: initial
    match:
      always: true
    actions:
      - route: [first]
`)
	pipe := startPipeline(t, cfg)

	if err := pipe.ReloadRules([]rules.Rule{{
		Name:    "swapped",
		Match:   &rules.Matcher{Always: boolPtr(true)},
		Actions: []rules.Action{{Route: []string{"second"}}},
	}}); err != nil {
		t.Fatalf("ReloadRules: %v", err)
	}

	first := bufferSink(t, pipe, "first")
	second := bufferSink(t, pipe, "second")
	if err := pipe.Ingest([]event.Event{makeEvent(t, "svc", "api", event.SeverityInfo, "after-reload")}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitFor(t, "the new route to receive the event", func() bool { return second.Total() == 1 })
	if first.Total() != 0 {
		t.Fatalf("the old route still received %d events", first.Total())
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// eventID returns a distinct id per index. Distinctness matters here: the
// pipeline deduplicates on id, so a colliding generator would make these tests
// measure the filter rather than the behaviour under test.
func eventID(index int) string {
	return fmt.Sprintf("evt-%04d", index)
}
