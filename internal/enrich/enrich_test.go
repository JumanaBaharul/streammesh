package enrich

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/rules"
)

func TestOllamaProviderReturnsTheModelAnswer(t *testing.T) {
	var calls int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		if r.URL.Path != "/api/generate" {
			t.Errorf("path = %q, want /api/generate", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if body["stream"] != false {
			t.Error("streaming should be disabled for a bounded call")
		}
		if !strings.Contains(body["prompt"].(string), "many failed logins") {
			t.Errorf("the event text was not included in the prompt: %v", body["prompt"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"response":"credential-stuffing"}`))
	}))
	defer server.Close()

	client := New(metrics.New())
	output, err := client.Enrich(context.Background(), rules.EnrichRequest{
		Provider: "ollama",
		URL:      server.URL,
		Model:    "llama3.1",
		Input:    "many failed logins",
		Timeout:  2 * time.Second,
		CacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if output != "credential-stuffing" {
		t.Fatalf("output = %q", output)
	}
	if atomic.LoadInt64(&calls) != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
}

func TestRepeatedInputsAreServedFromCache(t *testing.T) {
	var calls int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		_, _ = w.Write([]byte(`{"response":"label"}`))
	}))
	defer server.Close()

	client := New(metrics.New())
	request := rules.EnrichRequest{
		Provider: "ollama",
		URL:      server.URL,
		Input:    "identical input",
		Timeout:  2 * time.Second,
		CacheTTL: time.Minute,
	}

	for i := 0; i < 5; i++ {
		if _, err := client.Enrich(context.Background(), request); err != nil {
			t.Fatalf("Enrich: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want 1 with caching enabled", calls)
	}
	if client.CacheSize() != 1 {
		t.Fatalf("cache size = %d, want 1", client.CacheSize())
	}
}

func TestCachedEntriesExpire(t *testing.T) {
	var calls int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		_, _ = w.Write([]byte(`{"response":"label"}`))
	}))
	defer server.Close()

	client := New(metrics.New())
	request := rules.EnrichRequest{
		Provider: "ollama",
		URL:      server.URL,
		Input:    "expiring",
		Timeout:  2 * time.Second,
		CacheTTL: 20 * time.Millisecond,
	}

	if _, err := client.Enrich(context.Background(), request); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := client.Enrich(context.Background(), request); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if calls != 2 {
		t.Fatalf("provider calls = %d, want 2 after the cache entry expired", calls)
	}
}

func TestProviderErrorsAreReturnedSoTheCallerCanFallBack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("model exploded"))
	}))
	defer server.Close()

	client := New(metrics.New())
	_, err := client.Enrich(context.Background(), rules.EnrichRequest{
		Provider: "ollama",
		URL:      server.URL,
		Input:    "anything",
		Timeout:  time.Second,
	})
	if err == nil {
		t.Fatal("expected the provider failure to surface")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error %q does not carry the status code", err)
	}
}

// TestTimeoutIsEnforced proves a hanging model cannot stall the data path: the
// call returns as soon as the per-event timeout expires.
func TestTimeoutIsEnforced(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = w.Write([]byte(`{"response":"too late"}`))
	}))
	defer server.Close()
	defer close(release)

	client := New(metrics.New())
	started := time.Now()
	_, err := client.Enrich(context.Background(), rules.EnrichRequest{
		Provider: "ollama",
		URL:      server.URL,
		Input:    "slow request",
		Timeout:  100 * time.Millisecond,
	})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the timeout took %s, expected it to abort near 100ms", elapsed)
	}
}

func TestOpenAIProviderUsesChatCompletions(t *testing.T) {
	var sawPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath = r.URL.Path
		if auth := r.Header.Get("Authorization"); auth != "" && !strings.HasPrefix(auth, "Bearer ") {
			t.Errorf("authorization header = %q", auth)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"  storage-latency  "}}]}`))
	}))
	defer server.Close()

	client := New(metrics.New())
	output, err := client.Enrich(context.Background(), rules.EnrichRequest{
		Provider: "openai",
		URL:      server.URL,
		Model:    "gpt-4o-mini",
		Prompt:   "classify",
		Input:    "disk is slow",
		Timeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if sawPath != "/chat/completions" {
		t.Fatalf("path = %q, want /chat/completions", sawPath)
	}
	if output != "storage-latency" {
		t.Fatalf("output = %q, want the trimmed answer", output)
	}
}

func TestUnknownProvidersAndEmptyInputAreHandled(t *testing.T) {
	client := New(metrics.New())

	if output, err := client.Enrich(context.Background(), rules.EnrichRequest{Provider: "ollama", Input: "  "}); err != nil || output != "" {
		t.Fatalf("empty input should be a no-op, got %q / %v", output, err)
	}

	if _, err := client.Enrich(context.Background(), rules.EnrichRequest{Provider: "mystery", Input: "x"}); err == nil {
		t.Fatal("expected an unknown provider to fail")
	}
}

func TestCacheStaysBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"response":"label"}`))
	}))
	defer server.Close()

	client := New(metrics.New())
	for i := 0; i < 6000; i++ {
		_, err := client.Enrich(context.Background(), rules.EnrichRequest{
			Provider: "ollama",
			URL:      server.URL,
			Input:    string(rune('a'+i%26)) + strings.Repeat("x", i%5) + string(rune('0'+i%10)),
			Timeout:  time.Second,
			CacheTTL: time.Minute,
		})
		if err != nil {
			t.Fatalf("Enrich %d: %v", i, err)
		}
	}
	if size := client.CacheSize(); size > 4096 {
		t.Fatalf("cache grew to %d entries, want it bounded", size)
	}
}
