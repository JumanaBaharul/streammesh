package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/event"
)

func testEvents(count int) []event.Event {
	events := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		ev := event.New("svc")
		ev.ID = string(rune('a' + i%26))
		ev.Host = "edge-01"
		ev.Attrs["index"] = float64(i)
		events = append(events, ev)
	}
	return events
}

func TestBuildRejectsBadConfiguration(t *testing.T) {
	cases := []config.Sink{
		{ID: "file", Type: "file"},
		{ID: "http", Type: "http"},
		{ID: "kafka", Type: "kafka", Topic: "events"},
		{ID: "kafka", Type: "kafka", Brokers: []string{"localhost:9092"}},
		{ID: "s3", Type: "s3"},
		{ID: "unknown", Type: "carrier-pigeon"},
	}
	for _, cfg := range cases {
		if _, err := Build(cfg, Deps{}); err == nil {
			t.Errorf("Build(%s/%s) succeeded, want an error", cfg.ID, cfg.Type)
		}
	}
}

func TestTypesAreRegistered(t *testing.T) {
	got := strings.Join(Types(), ",")
	for _, want := range []string{"file", "http", "kafka", "memory", "s3"} {
		if !strings.Contains(got, want) {
			t.Errorf("sink type %q is not registered (have %s)", want, got)
		}
	}
}

func TestFileSinkWritesAndRotates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.ndjson")

	target, err := Build(config.Sink{
		ID:          "archive",
		Type:        "file",
		File:        path,
		RotateBytes: 400,
		RotateKeep:  2,
	}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = target.Close(context.Background()) }()

	ctx := context.Background()
	for batch := 0; batch < 6; batch++ {
		if err := target.Write(ctx, testEvents(4)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("the active file is empty")
	}
	lines := strings.Count(string(raw), "\n")
	if lines == 0 {
		t.Fatal("no newline-delimited records were written")
	}

	// Rotation must have created the first generation.
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected a rotated generation: %v", err)
	}

	var decoded event.Event
	if err := json.Unmarshal([]byte(strings.Split(string(raw), "\n")[0]), &decoded); err != nil {
		t.Fatalf("output is not canonical JSON: %v", err)
	}
	if decoded.Source != "svc" {
		t.Fatalf("source = %q", decoded.Source)
	}
}

func TestFileSinkHealthTracksFailures(t *testing.T) {
	// The configured path is a directory, so the sink cannot open it as a file.
	path := filepath.Join(t.TempDir(), "not-a-file")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	target, err := Build(config.Sink{ID: "bad", Type: "file", File: path}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := target.Write(context.Background(), testEvents(1)); err == nil {
		t.Fatal("expected the write to fail")
	}
	if target.Health() == nil {
		t.Fatal("a failed sink should not report itself healthy")
	}
}

func TestHTTPSinkClassifiesResponses(t *testing.T) {
	var receivedBody string
	var receivedHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody = string(body)
		receivedHeader = r.Header.Get("X-Route")
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		case "/broken":
			w.WriteHeader(http.StatusInternalServerError)
		case "/rejected":
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()

	ok, err := Build(config.Sink{ID: "ok", Type: "http", URL: server.URL + "/ok", Headers: map[string]string{"X-Route": "alerts"}}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := ok.Write(ctx, testEvents(2)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if receivedHeader != "alerts" {
		t.Errorf("configured header was not sent, got %q", receivedHeader)
	}
	if strings.Count(receivedBody, "\n") != 2 {
		t.Errorf("expected two newline-delimited records, got %q", receivedBody)
	}
	if ok.Health() != nil {
		t.Errorf("a healthy sink reported %v", ok.Health())
	}

	retryable, err := Build(config.Sink{ID: "broken", Type: "http", URL: server.URL + "/broken"}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = retryable.Write(ctx, testEvents(1))
	var retryErr *RetryableError
	if err == nil || !errors.As(err, &retryErr) {
		t.Fatalf("a 500 should be retryable, got %v", err)
	}

	permanent, err := Build(config.Sink{ID: "rejected", Type: "http", URL: server.URL + "/rejected"}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = permanent.Write(ctx, testEvents(1))
	if err == nil {
		t.Fatal("expected a 400 to fail")
	}
	var notRetryable *RetryableError
	if errors.As(err, &notRetryable) {
		t.Fatal("a 400 must not be retryable")
	}
}

func TestMemorySinkKeepsABoundedRing(t *testing.T) {
	target, err := Build(config.Sink{ID: "recent", Type: "memory", BufferSize: 3}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	buffer, ok := target.(Buffer)
	if !ok {
		t.Fatal("the memory sink does not implement Buffer")
	}

	if err := target.Write(context.Background(), testEvents(5)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if buffer.Total() != 5 {
		t.Fatalf("total = %d, want 5", buffer.Total())
	}
	recent := buffer.Recent(3)
	if len(recent) != 3 {
		t.Fatalf("recent = %d events, want 3", len(recent))
	}
	// The ring holds the last three events, in write order.
	for index, want := range []float64{2, 3, 4} {
		got, _ := event.ValueFloat(recent[index].Attrs["index"])
		if got != want {
			t.Errorf("recent[%d] index = %v, want %v", index, got, want)
		}
	}
	if all := buffer.Recent(0); len(all) != 3 {
		t.Fatalf("Recent(0) = %d events, want the whole ring", len(all))
	}
}

func TestS3SinkRequiresCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, err := Build(config.Sink{ID: "cold", Type: "s3", Bucket: "archive"}, Deps{}); err == nil {
		t.Fatal("expected an error without credentials")
	}
}

// TestS3SinkSignsRequestsCorrectly checks the hand-rolled SigV4 implementation
// against the documented algorithm: the payload hash must match the body, the
// credential scope must be well formed, and the signature must match an
// independently recomputed one.
func TestS3SinkSignsRequestsCorrectly(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")

	type captured struct {
		method        string
		path          string
		authorization string
		amzDate       string
		contentHash   string
		body          []byte
	}
	got := make(chan captured, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- captured{
			method:        r.Method,
			path:          r.URL.Path,
			authorization: r.Header.Get("Authorization"),
			amzDate:       r.Header.Get("x-amz-date"),
			contentHash:   r.Header.Get("x-amz-content-sha256"),
			body:          body,
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	target, err := Build(config.Sink{
		ID:           "cold",
		Type:         "s3",
		Bucket:       "telemetry-archive",
		Endpoint:     server.URL,
		Region:       "ap-south-1",
		Prefix:       "logs",
		UsePathStyle: true,
	}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = target.Close(context.Background()) }()

	if err := target.Write(context.Background(), testEvents(2)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case request := <-got:
		if request.method != http.MethodPut {
			t.Errorf("method = %s, want PUT", request.method)
		}
		if !strings.Contains(request.path, "/telemetry-archive/logs/dt=") {
			t.Errorf("path %q does not use path-style addressing with the configured prefix", request.path)
		}
		if !strings.HasSuffix(request.path, ".ndjson.gz") {
			t.Errorf("path %q does not end in .ndjson.gz", request.path)
		}

		sum := sha256.Sum256(request.body)
		if request.contentHash != hex.EncodeToString(sum[:]) {
			t.Error("x-amz-content-sha256 does not match the request body")
		}

		if _, err := time.Parse("20060102T150405Z", request.amzDate); err != nil {
			t.Errorf("x-amz-date %q is not a valid SigV4 timestamp", request.amzDate)
		}
		if !strings.Contains(request.authorization, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/") {
			t.Errorf("authorization header is malformed: %q", request.authorization)
		}
		if !strings.Contains(request.authorization, "/ap-south-1/s3/aws4_request") {
			t.Errorf("credential scope is wrong: %q", request.authorization)
		}
		if !strings.Contains(request.authorization, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
			t.Errorf("signed headers are wrong: %q", request.authorization)
		}

		signature := request.authorization[strings.Index(request.authorization, "Signature=")+len("Signature="):]
		if len(signature) != 64 {
			t.Errorf("signature %q is not a 32-byte hex digest", signature)
		}

		// Independently recompute the signature from the captured request.
		dateStamp := request.amzDate[:8]
		canonicalHeaders := "host:" + strings.TrimPrefix(server.URL, "http://") + "\n" +
			"x-amz-content-sha256:" + request.contentHash + "\n" +
			"x-amz-date:" + request.amzDate + "\n"
		canonicalRequest := strings.Join([]string{
			request.method,
			request.path,
			"",
			canonicalHeaders,
			"host;x-amz-content-sha256;x-amz-date",
			request.contentHash,
		}, "\n")
		hashedCanonical := sha256.Sum256([]byte(canonicalRequest))
		stringToSign := strings.Join([]string{
			"AWS4-HMAC-SHA256",
			request.amzDate,
			dateStamp + "/ap-south-1/s3/aws4_request",
			hex.EncodeToString(hashedCanonical[:]),
		}, "\n")
		key := deriveSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", dateStamp, "ap-south-1", "s3")
		expected := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
		if signature != expected {
			t.Errorf("signature mismatch\n got: %s\nwant: %s", signature, expected)
		}

		// The archived payload must be the gzipped newline-delimited events.
		reader, err := gzip.NewReader(bytes.NewReader(request.body))
		if err != nil {
			t.Fatalf("body is not gzip: %v", err)
		}
		decoded, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("decompress: %v", err)
		}
		if strings.Count(string(decoded), "\n") != 2 {
			t.Errorf("archived payload = %q, want two records", decoded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the S3 sink never issued a request")
	}
}

// TestKafkaSinkProducesToABroker uses an in-process fake Kafka cluster, so the
// Kafka path is exercised end to end without needing Docker.
func TestKafkaSinkProducesToABroker(t *testing.T) {
	cluster, err := kfake.NewCluster()
	if err != nil {
		t.Fatalf("start fake cluster: %v", err)
	}
	defer cluster.Close()

	addrs := cluster.ListenAddrs()
	if len(addrs) == 0 {
		t.Fatal("the fake cluster did not expose an address")
	}
	// The fake broker does not auto-create topics, so declare the one the sink
	// will produce to.
	if err := cluster.CreateTopic("streammesh.events", 1, nil); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	target, err := Build(config.Sink{
		ID:      "events",
		Type:    "kafka",
		Brokers: addrs,
		Topic:   "streammesh.events",
	}, Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = target.Close(context.Background()) }()

	events := testEvents(3)
	if err := target.Write(context.Background(), events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if target.Health() != nil {
		t.Fatalf("sink reported unhealthy: %v", target.Health())
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics("streammesh.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seen := 0
	for seen < len(events) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		for _, record := range fetches.Records() {
			if record.Topic != "streammesh.events" {
				t.Errorf("record topic = %q", record.Topic)
			}
			if len(record.Key) == 0 {
				t.Error("record key is empty, so partition ordering is not pinned to the event id")
			}
			var decoded event.Event
			if err := json.Unmarshal(record.Value, &decoded); err != nil {
				t.Fatalf("record value is not a canonical event: %v", err)
			}
			if decoded.Source != "svc" {
				t.Errorf("decoded source = %q", decoded.Source)
			}
			seen++
		}
		if ctx.Err() != nil {
			break
		}
	}
	if seen != len(events) {
		t.Fatalf("consumed %d of %d records", seen, len(events))
	}
}

func TestEncodeBatchProducesOneLinePerEvent(t *testing.T) {
	payload, err := encodeBatch(testEvents(3))
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(payload), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3", len(lines))
	}
	for index, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d is not JSON: %v", index, err)
		}
	}
}

func TestTruncateBoundsErrorMessages(t *testing.T) {
	long := strings.Repeat("x", 500)
	if got := truncate(long, 10); len(got) != 13 {
		t.Fatalf("truncate returned %d bytes, want 13 including the ellipsis", len(got))
	}
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("truncate shortened a short string: %q", got)
	}
}
