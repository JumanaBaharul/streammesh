package source

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/duration"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
)

type collector struct {
	mu     sync.Mutex
	events []event.Event
	fail   error
	notify chan struct{}
}

func newCollector() *collector {
	return &collector{notify: make(chan struct{}, 64)}
}

func (c *collector) emit(events []event.Event) error {
	c.mu.Lock()
	if c.fail != nil {
		c.mu.Unlock()
		return c.fail
	}
	c.events = append(c.events, events...)
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return nil
}

func (c *collector) setFailure(err error) {
	c.mu.Lock()
	c.fail = err
	c.mu.Unlock()
}

func (c *collector) snapshot() []event.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]event.Event(nil), c.events...)
}

func (c *collector) wait(t *testing.T, want int) []event.Event {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if events := c.snapshot(); len(events) >= want {
			return events
		}
		select {
		case <-c.notify:
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %d events, have %d", want, len(c.snapshot()))
	return nil
}

func startSource(t *testing.T, cfg config.Source, deps Deps) Source {
	t.Helper()
	src, err := Build(cfg, deps)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Start(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		_ = src.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("source did not stop when its context was cancelled")
		}
	})

	if addressed, ok := src.(Addressed); ok {
		deadline := time.Now().Add(5 * time.Second)
		for addressed.Addr() == "" && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if addressed.Addr() == "" {
			t.Fatal("source never reported a bound address")
		}
	}
	return src
}

func deps(c *collector) Deps {
	return Deps{Emit: c.emit, Registry: metrics.New()}
}

func TestBuildRejectsUnknownTypesAndBadParsers(t *testing.T) {
	if _, err := Build(config.Source{ID: "x", Type: "carrier-pigeon"}, Deps{}); err == nil {
		t.Fatal("expected an unknown type to fail")
	}
	if _, err := Build(config.Source{ID: "x", Type: "http", Parser: "nonsense"}, Deps{}); err == nil {
		t.Fatal("expected an unknown parser to fail")
	}
}

func TestTypesAreRegistered(t *testing.T) {
	joined := strings.Join(Types(), ",")
	for _, want := range []string{"file", "http", "syslog", "webhook"} {
		if !strings.Contains(joined, want) {
			t.Errorf("source type %q is missing from %s", want, joined)
		}
	}
}

func TestHTTPSourceAcceptsAndParsesPayloads(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID:           "app-http",
		Type:         "http",
		Listen:       "127.0.0.1:0",
		Path:         "/ingest",
		Parser:       "json",
		MaxBodyBytes: 1024,
	}, deps(events))

	addr := src.(Addressed).Addr()
	url := "http://" + addr + "/ingest"

	body := `{"level":"error","host":"edge-01","category":"api","message":"boom"}` + "\n" +
		`{"level":"warn","host":"edge-02","category":"api","message":"slow"}`
	response := post(t, url, body, nil)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", response.StatusCode)
	}
	_ = response.Body.Close()

	got := events.wait(t, 2)
	if got[0].Source != "app-http" {
		t.Errorf("source = %q, want app-http", got[0].Source)
	}
	if got[0].Severity != event.SeverityError {
		t.Errorf("severity = %v, want error", got[0].Severity)
	}
	if got[1].Host != "edge-02" {
		t.Errorf("host = %q", got[1].Host)
	}

	// A GET is not an ingest.
	get, err := http.Get(url)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = get.Body.Close() }()
	if get.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", get.StatusCode)
	}
}

func TestHTTPSourceRejectsUnparseablePayloads(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{ID: "app-http", Type: "http", Listen: "127.0.0.1:0", Parser: "json"}, deps(events))

	response := post(t, "http://"+src.(Addressed).Addr()+"/ingest", "this is not json", nil)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
	if len(events.snapshot()) != 0 {
		t.Fatal("a rejected payload produced events")
	}
}

func TestHTTPSourceEnforcesTheBodyLimit(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID: "app-http", Type: "http", Listen: "127.0.0.1:0", Parser: "json", MaxBodyBytes: 32,
	}, deps(events))

	big := `{"message":"` + strings.Repeat("x", 200) + `"}`
	response := post(t, "http://"+src.(Addressed).Addr()+"/ingest", big, nil)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.StatusCode)
	}
}

func TestHTTPSourceRequiresABearerTokenWhenConfigured(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID: "secured", Type: "http", Listen: "127.0.0.1:0", Parser: "json", Secret: "s3cret",
	}, deps(events))

	url := "http://" + src.(Addressed).Addr() + "/ingest"

	unauthorized := post(t, url, `{"message":"hi"}`, nil)
	_ = unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without a token = %d, want 401", unauthorized.StatusCode)
	}

	wrong := post(t, url, `{"message":"hi"}`, map[string]string{"Authorization": "Bearer nope"})
	_ = wrong.Body.Close()
	if wrong.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status with a wrong token = %d, want 401", wrong.StatusCode)
	}

	authorized := post(t, url, `{"message":"hi"}`, map[string]string{"Authorization": "Bearer s3cret"})
	_ = authorized.Body.Close()
	if authorized.StatusCode != http.StatusAccepted {
		t.Fatalf("status with the right token = %d, want 202", authorized.StatusCode)
	}
	events.wait(t, 1)
}

func TestWebhookSourceVerifiesTheSignature(t *testing.T) {
	events := newCollector()
	secret := "webhook-secret"
	src := startSource(t, config.Source{
		ID: "hook", Type: "webhook", Listen: "127.0.0.1:0", Path: "/webhook", Parser: "json", Secret: secret,
	}, deps(events))

	url := "http://" + src.(Addressed).Addr() + "/webhook"
	body := `{"message":"delivered"}`

	unsigned := post(t, url, body, nil)
	_ = unsigned.Body.Close()
	if unsigned.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without a signature = %d, want 401", unsigned.StatusCode)
	}

	bad := post(t, url, body, map[string]string{"X-StreamMesh-Signature": "sha256=deadbeef"})
	_ = bad.Body.Close()
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status with a bad signature = %d, want 401", bad.StatusCode)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	signed := post(t, url, body, map[string]string{"X-StreamMesh-Signature": signature})
	_ = signed.Body.Close()
	if signed.StatusCode != http.StatusAccepted {
		t.Fatalf("status with a valid signature = %d, want 202", signed.StatusCode)
	}
	if got := events.wait(t, 1); got[0].Source != "hook" {
		t.Errorf("source = %q", got[0].Source)
	}
}

func TestHTTPSourceReportsBackpressureAsServiceUnavailable(t *testing.T) {
	events := newCollector()
	events.setFailure(errors.New("queue full"))

	src := startSource(t, config.Source{ID: "app-http", Type: "http", Listen: "127.0.0.1:0", Parser: "json"}, deps(events))

	response := post(t, "http://"+src.(Addressed).Addr()+"/ingest", `{"message":"hi"}`, nil)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 so the client retries", response.StatusCode)
	}
}

func TestFileSourceTailsNewLinesAndResumes(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	offsetsPath := filepath.Join(dir, "offsets.json")

	if err := os.WriteFile(logPath, []byte(`{"level":"info","message":"first"}`+"\n"+`{"level":"warn","message":"second"}`+"\n"), 0o644); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	events := newCollector()
	startSource(t, config.Source{
		ID:           "app-log",
		Type:         "file",
		File:         logPath,
		Parser:       "json",
		StartAt:      "start",
		PollInterval: duration.Duration(10 * time.Millisecond),
		OffsetsFile:  offsetsPath,
	}, deps(events))

	got := events.wait(t, 2)
	if got[0].Host != "" && got[1].Source != "app-log" {
		t.Fatalf("unexpected events: %+v", got)
	}

	// A partial line must stay buffered until its newline arrives.
	appendToFile(t, logPath, `{"level":"error","message":"par`)
	time.Sleep(100 * time.Millisecond)
	if len(events.snapshot()) != 2 {
		t.Fatal("a partial line was emitted before it was complete")
	}

	appendToFile(t, logPath, "tial\"}\n")
	got = events.wait(t, 3)
	if got[2].Severity != event.SeverityError {
		t.Errorf("third event severity = %v", got[2].Severity)
	}
}

func TestFileSourceHandlesTruncation(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte(`{"message":"one"}`+"\n"+`{"message":"two"}`+"\n"), 0o644); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	events := newCollector()
	startSource(t, config.Source{
		ID:           "app-log",
		Type:         "file",
		File:         logPath,
		Parser:       "json",
		StartAt:      "start",
		PollInterval: duration.Duration(10 * time.Millisecond),
		OffsetsFile:  filepath.Join(dir, "offsets.json"),
	}, deps(events))
	events.wait(t, 2)

	// Rotate onto a shorter file: the reader must restart from the beginning
	// rather than waiting for the offset to be reached again.
	if err := os.WriteFile(logPath, []byte(`{"message":"after-rotation"}`+"\n"), 0o644); err != nil {
		t.Fatalf("rotate log: %v", err)
	}
	got := events.wait(t, 3)
	if event.ValueString(got[2].Attrs["message"]) != "after-rotation" {
		t.Fatalf("third message = %v", got[2].Attrs["message"])
	}
}

func TestFileSourceWaitsForAFileThatDoesNotExistYet(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "later.log")

	events := newCollector()
	startSource(t, config.Source{
		ID:           "app-log",
		Type:         "file",
		File:         logPath,
		Parser:       "json",
		StartAt:      "start",
		PollInterval: duration.Duration(10 * time.Millisecond),
		OffsetsFile:  filepath.Join(dir, "offsets.json"),
	}, deps(events))

	time.Sleep(100 * time.Millisecond)
	if len(events.snapshot()) != 0 {
		t.Fatal("events were produced for a missing file")
	}

	appendToFile(t, logPath, `{"message":"created late"}`+"\n")
	events.wait(t, 1)
}

func TestSyslogSourceReadsUDPDatagrams(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID: "app-syslog", Type: "syslog", Listen: "127.0.0.1:0", Network: "udp", Parser: "syslog",
	}, deps(events))

	conn, err := net.Dial("udp", src.(Addressed).Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte("<34>1 2026-09-30T09:15:01Z edge-01 api 12 ID1 - request failed")); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := events.wait(t, 1)
	if got[0].Severity != event.SeverityCritical {
		t.Errorf("severity = %v, want critical", got[0].Severity)
	}
	if got[0].Host != "edge-01" {
		t.Errorf("host = %q", got[0].Host)
	}
}

func TestSyslogSourceReadsTCPStreams(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID: "app-syslog", Type: "syslog", Listen: "127.0.0.1:0", Network: "tcp", Parser: "syslog",
	}, deps(events))

	conn, err := net.Dial("tcp", src.(Addressed).Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte("<13>Sep 30 09:15:01 api-11 sshd[9]: failed password\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := events.wait(t, 1)
	if got[0].Category != "sshd" {
		t.Errorf("category = %q", got[0].Category)
	}
}

func TestSourceSnapshotReportsCounters(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{ID: "app-http", Type: "http", Listen: "127.0.0.1:0", Parser: "json"}, deps(events))

	response := post(t, "http://"+src.(Addressed).Addr()+"/ingest", `{"message":"hello"}`, nil)
	_ = response.Body.Close()
	events.wait(t, 1)

	stats := src.Snapshot()
	if stats.ID != "app-http" || stats.Type != "http" {
		t.Errorf("unexpected stats identity: %+v", stats)
	}
	if stats.Received != 1 {
		t.Errorf("received = %d, want 1", stats.Received)
	}
	if stats.Bytes == 0 {
		t.Error("bytes counter was not incremented")
	}
	if stats.ParseErrors != 0 || stats.Dropped != 0 {
		t.Errorf("unexpected error counters: %+v", stats)
	}
}

func TestDecodeAppliesTagsAndCategoryOverrides(t *testing.T) {
	events := newCollector()
	src := startSource(t, config.Source{
		ID:          "tagged",
		Type:        "http",
		Listen:      "127.0.0.1:0",
		Parser:      "json",
		Category:    "internal-api",
		DefaultTags: []string{"prod", "prod", ""},
	}, deps(events))

	response := post(t, "http://"+src.(Addressed).Addr()+"/ingest", `{"message":"hi","category":"from-payload"}`, nil)
	_ = response.Body.Close()

	got := events.wait(t, 1)
	if got[0].Category != "internal-api" {
		t.Errorf("category = %q, want the configured override", got[0].Category)
	}
	if len(got[0].Tags) != 1 || got[0].Tags[0] != "prod" {
		t.Errorf("tags = %v, want a single deduplicated tag", got[0].Tags)
	}
}

func post(t *testing.T, url, body string, headers map[string]string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	return response
}

func appendToFile(t *testing.T, path, content string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteString(content); err != nil {
		t.Fatalf("append: %v", err)
	}
}
