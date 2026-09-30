package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/pipeline"
)

func startServer(t *testing.T, body string) (*pipeline.Pipeline, string) {
	t.Helper()
	cfg, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	pipe, err := pipeline.New(pipeline.Options{Config: cfg, Registry: metrics.New()})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	control := New(pipe, cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = control.Start(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		_ = control.Close()
		<-done
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = pipe.Shutdown(shutdownCtx)
	})

	deadline := time.Now().Add(5 * time.Second)
	for control.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if control.Addr() == "" {
		t.Fatal("the control plane never bound an address")
	}
	return pipe, "http://" + control.Addr()
}

const serverConfig = `
server:
  addr: "127.0.0.1:0"
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
  - id: recent
    type: memory
rules:
  - name: default
    match:
      always: true
    actions:
      - route: [recent]
`

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	response, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

func request(t *testing.T, method, url, body string, headers map[string]string) (int, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

func TestHealthAndReadinessEndpoints(t *testing.T) {
	_, base := startServer(t, serverConfig)

	status, body := get(t, base+"/healthz")
	if status != http.StatusOK {
		t.Fatalf("healthz status = %d", status)
	}
	if !strings.Contains(body, "ok") {
		t.Errorf("healthz body = %q", body)
	}

	status, body = get(t, base+"/readyz")
	if status != http.StatusOK {
		t.Fatalf("readyz status = %d, body = %s", status, body)
	}
	if !strings.Contains(body, "ready") {
		t.Errorf("readyz body = %q", body)
	}
}

func TestMetricsExposeEngineAndRuntimeSeries(t *testing.T) {
	_, base := startServer(t, serverConfig)

	status, body := get(t, base+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("metrics status = %d", status)
	}
	for _, want := range []string{
		"# TYPE streammesh_ingested_total counter",
		"streammesh_queue_depth",
		"streammesh_processing_seconds_count",
		"streammesh_goroutines",
		"streammesh_heap_bytes",
		"streammesh_sink_delivered_total{sink=\"recent\"}",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
}

func TestStatsReportCountersAndConnectorState(t *testing.T) {
	pipe, base := startServer(t, serverConfig)

	if err := pipe.Ingest([]event.Event{newEvent("evt-1")}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	status, body := get(t, base+"/stats")
	if status != http.StatusOK {
		t.Fatalf("stats status = %d", status)
	}

	var decoded struct {
		Ingested int64 `json:"ingested"`
		Sources  []struct {
			ID       string `json:"id"`
			Received int64  `json:"received"`
		} `json:"sources"`
		Sinks []struct {
			ID     string `json:"id"`
			Health string `json:"health"`
		} `json:"sinks"`
		WAL struct {
			Enabled bool `json:"enabled"`
		} `json:"wal"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, body)
	}
	if decoded.Ingested != 1 {
		t.Errorf("ingested = %d, want 1", decoded.Ingested)
	}
	if len(decoded.Sources) != 1 || decoded.Sources[0].ID != "test-http" {
		t.Errorf("sources = %+v", decoded.Sources)
	}
	if len(decoded.Sinks) != 1 || decoded.Sinks[0].Health != "ok" {
		t.Errorf("sinks = %+v", decoded.Sinks)
	}
	if decoded.WAL.Enabled {
		t.Error("the write-ahead log should be reported as disabled in this configuration")
	}
}

func TestRulesCanBeReadAndHotReloaded(t *testing.T) {
	pipe, base := startServer(t, serverConfig)

	status, body := get(t, base+"/rules")
	if status != http.StatusOK {
		t.Fatalf("GET /rules status = %d", status)
	}
	if !strings.Contains(body, "default") {
		t.Fatalf("GET /rules body = %s", body)
	}

	replacement := `
rules:
  - name: swapped
    match:
      always: true
    actions:
      - route: [recent]
      - tag: swapped
`
	status, body = request(t, http.MethodPut, base+"/rules", replacement, nil)
	if status != http.StatusOK {
		t.Fatalf("PUT /rules status = %d, body = %s", status, body)
	}
	if active := pipe.Engine().Rules(); len(active) != 1 || active[0].Name != "swapped" {
		t.Fatalf("active rules = %+v", active)
	}

	// A broken rule set must be rejected without disturbing the running one.
	status, body = request(t, http.MethodPut, base+"/rules", `rules:
  - name: broken
    match:
      regex: "(["
    actions:
      - route: [recent]
`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an invalid rule set, body = %s", status, body)
	}
	if !strings.Contains(body, "previous rules still active") {
		t.Errorf("rejection body = %q", body)
	}
	if active := pipe.Engine().Rules(); len(active) != 1 || active[0].Name != "swapped" {
		t.Fatalf("a rejected reload changed the running rules: %+v", active)
	}
}

func TestMutatingEndpointsRequireTheToken(t *testing.T) {
	pipe, base := startServer(t, strings.Replace(serverConfig,
		`server:
  addr: "127.0.0.1:0"`,
		`server:
  addr: "127.0.0.1:0"
  api_token: "top-secret"`, 1))

	rules := `rules:
  - name: anything
    match:
      always: true
    actions:
      - route: [recent]
`
	status, _ := request(t, http.MethodPut, base+"/rules", rules, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("status without a token = %d, want 401", status)
	}

	status, _ = request(t, http.MethodPut, base+"/rules", rules, map[string]string{"Authorization": "Bearer wrong"})
	if status != http.StatusUnauthorized {
		t.Fatalf("status with a wrong token = %d, want 401", status)
	}

	status, body := request(t, http.MethodPut, base+"/rules", rules, map[string]string{"Authorization": "Bearer top-secret"})
	if status != http.StatusOK {
		t.Fatalf("status with the token = %d, body = %s", status, body)
	}

	// Probes stay open, because a load balancer has no token.
	if status, _ := get(t, base+"/healthz"); status != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200 without a token", status)
	}

	if len(pipe.Engine().Rules()) != 1 {
		t.Fatal("rules were not replaced")
	}
}

func TestReplayEndpointRedrivesTheWriteAheadLog(t *testing.T) {
	dir := t.TempDir()
	cfg := strings.Replace(serverConfig, `  wal:
    enabled: false`, `  wal:
    enabled: true
    dir: `+dir+`
    fsync_interval: 10ms`, 1)

	pipe, base := startServer(t, cfg)

	for i := 0; i < 5; i++ {
		if err := pipe.Ingest([]event.Event{newEvent("evt-" + string(rune('a'+i)))}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	status, body := request(t, http.MethodPost, base+"/replay", `{"from":0,"limit":0}`, nil)
	if status != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", status, body)
	}
	var result struct {
		Read     int `json:"read"`
		Accepted int `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Read != 5 || result.Accepted != 5 {
		t.Fatalf("replay result = %+v, want 5 read and accepted", result)
	}
}

func TestDLQReplayRequiresAKnownSink(t *testing.T) {
	_, base := startServer(t, serverConfig)

	status, body := request(t, http.MethodPost, base+"/dlq/replay", `{"sink":"ghost"}`, nil)
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409 for an unknown sink, body = %s", status, body)
	}
	if !strings.Contains(body, "unknown sink") {
		t.Errorf("body = %q", body)
	}

	status, _ = request(t, http.MethodPost, base+"/dlq/replay", `{}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 without a sink id", status)
	}
}

func TestUnsupportedMethodsAreRejected(t *testing.T) {
	_, base := startServer(t, serverConfig)

	status, _ := request(t, http.MethodDelete, base+"/rules", "", nil)
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", status)
	}
	status, _ = get(t, base+"/replay")
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("GET /replay status = %d, want 405", status)
	}
}

func newEvent(id string) event.Event {
	ev := event.New("test-http")
	ev.ID = id
	ev.Host = "edge-01"
	ev.Category = "api"
	ev.Attrs["message"] = "request completed"
	return ev
}
