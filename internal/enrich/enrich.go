// Package enrich is the optional AI stage of the pipeline.
//
// It calls an LLM to classify or extract a field from an event, and it is built
// so that a slow or broken model can never stall the data path: every call has
// a hard timeout, results are cached by input, and failures fall back to a
// deterministic value. Enrichment is strictly opt-in per rule.
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/rules"
)

// DefaultURLs are used when a rule does not specify an endpoint.
var DefaultURLs = map[string]string{
	"ollama": "http://localhost:11434",
	"openai": "https://api.openai.com/v1",
}

// Client implements rules.Enricher.
type Client struct {
	http    *http.Client
	mu      sync.Mutex
	cache   map[string]cacheEntry
	maxKeys int

	calls   *metrics.Counter
	hits    *metrics.Counter
	errors  *metrics.Counter
	latency *metrics.Histogram
}

type cacheEntry struct {
	value   string
	expires time.Time
}

// New creates a client. A nil registry disables instrumentation.
func New(registry *metrics.Registry) *Client {
	client := &Client{
		http:    &http.Client{Timeout: 10 * time.Second},
		cache:   map[string]cacheEntry{},
		maxKeys: 4096,
	}
	if registry != nil {
		client.calls = registry.Counter("streammesh_enrich_provider_calls_total", "LLM enrichment provider calls.", nil)
		client.hits = registry.Counter("streammesh_enrich_cache_hits_total", "Enrichment results served from cache.", nil)
		client.errors = registry.Counter("streammesh_enrich_provider_errors_total", "LLM enrichment provider errors.", nil)
		client.latency = registry.Histogram("streammesh_enrich_latency_seconds", "Enrichment provider latency in seconds.", nil, nil)
	}
	go client.reap()
	return client
}

// Enrich implements rules.Enricher.
func (c *Client) Enrich(ctx context.Context, req rules.EnrichRequest) (string, error) {
	if strings.TrimSpace(req.Input) == "" {
		return "", nil
	}
	cacheKey := cacheKey(req)

	c.mu.Lock()
	if entry, found := c.cache[cacheKey]; found && time.Now().Before(entry.expires) {
		c.mu.Unlock()
		if c.hits != nil {
			c.hits.Inc()
		}
		return entry.value, nil
	}
	c.mu.Unlock()

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if c.calls != nil {
		c.calls.Inc()
	}
	started := time.Now()
	output, err := c.call(callCtx, req)
	if c.latency != nil {
		c.latency.Observe(time.Since(started).Seconds())
	}
	if err != nil {
		if c.errors != nil {
			c.errors.Inc()
		}
		return "", err
	}

	ttl := req.CacheTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	c.mu.Lock()
	if len(c.cache) >= c.maxKeys {
		c.evictLocked()
	}
	c.cache[cacheKey] = cacheEntry{value: output, expires: time.Now().Add(ttl)}
	c.mu.Unlock()

	return output, nil
}

func (c *Client) call(ctx context.Context, req rules.EnrichRequest) (string, error) {
	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	if provider == "" {
		provider = "ollama"
	}
	base := strings.TrimSpace(req.URL)
	if base == "" {
		base = DefaultURLs[provider]
	}
	if base == "" {
		return "", fmt.Errorf("enrich: unknown provider %q", provider)
	}
	base = strings.TrimRight(base, "/")

	prompt := strings.TrimSpace(req.Prompt)
	if prompt == "" {
		prompt = "Classify the following log line. Reply with one short label and nothing else."
	}

	switch provider {
	case "ollama":
		return c.callOllama(ctx, base, req.Model, prompt, req.Input)
	case "openai":
		return c.callOpenAI(ctx, base, req.Model, prompt, req.Input)
	default:
		return "", fmt.Errorf("enrich: unsupported provider %q", provider)
	}
}

func (c *Client) callOllama(ctx context.Context, base, model, prompt, input string) (string, error) {
	if model == "" {
		model = "llama3.1"
	}
	body := map[string]any{
		"model":  model,
		"prompt": prompt + "\n\n" + input,
		"stream": false,
		"options": map[string]any{
			"temperature": 0,
			"num_predict": 32,
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("enrich: encode request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/generate", bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("enrich: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("OLLAMA_API_KEY"); key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return "", fmt.Errorf("enrich: ollama request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("enrich: read response: %w", err)
	}
	if response.StatusCode >= 300 {
		return "", fmt.Errorf("enrich: ollama returned %d: %s", response.StatusCode, truncate(string(payload), 200))
	}

	var decoded struct {
		Response string `json:"response"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", fmt.Errorf("enrich: decode ollama response: %w", err)
	}
	return strings.TrimSpace(decoded.Response), nil
}

func (c *Client) callOpenAI(ctx context.Context, base, model, prompt, input string) (string, error) {
	if model == "" {
		model = "gpt-4o-mini"
	}
	body := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": prompt},
			{"role": "user", "content": input},
		},
		"temperature": 0,
		"max_tokens":  32,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("enrich: encode request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("enrich: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return "", fmt.Errorf("enrich: openai request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("enrich: read response: %w", err)
	}
	if response.StatusCode >= 300 {
		return "", fmt.Errorf("enrich: openai returned %d: %s", response.StatusCode, truncate(string(payload), 200))
	}

	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", fmt.Errorf("enrich: decode openai response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", fmt.Errorf("enrich: openai returned no choices")
	}
	return strings.TrimSpace(decoded.Choices[0].Message.Content), nil
}

func cacheKey(req rules.EnrichRequest) string {
	return req.Provider + "\x00" + req.Model + "\x00" + req.Prompt + "\x00" + req.OutputField + "\x00" + req.Input
}

// evictLocked drops expired entries; if none are expired it drops the oldest
// tenth of the cache so memory stays bounded under a key explosion.
func (c *Client) evictLocked() {
	now := time.Now()
	removed := 0
	for key, entry := range c.cache {
		if now.After(entry.expires) {
			delete(c.cache, key)
			removed++
		}
	}
	if removed > 0 {
		return
	}
	target := len(c.cache) / 10
	if target == 0 {
		target = 1
	}
	for key := range c.cache {
		delete(c.cache, key)
		target--
		if target <= 0 {
			break
		}
	}
}

func (c *Client) reap() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		c.evictLocked()
		c.mu.Unlock()
	}
}

// CacheSize reports how many entries are cached, for tests.
func (c *Client) CacheSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
