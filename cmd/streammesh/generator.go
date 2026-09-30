package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// syntheticGenerator produces realistic-looking operational and security events
// and posts them to an ingest endpoint. It exists so throughput and latency
// numbers in the README come from a real run rather than an estimate.
type syntheticGenerator struct {
	url       string
	rate      int
	batchSize int
	workers   int
	sources   int
	token     string
	logger    *slog.Logger
	client    *http.Client

	sent     atomic.Int64
	accepted atomic.Int64
	requests atomic.Int64
	failed   atomic.Int64

	mu   sync.Mutex
	rand *rand.Rand
}

func newSyntheticGenerator(url string, rate, batchSize, sources, workers int, seed int64, token string, logger *slog.Logger) *syntheticGenerator {
	return &syntheticGenerator{
		url:       url,
		rate:      rate,
		batchSize: batchSize,
		sources:   sources,
		workers:   workers,
		token:     token,
		logger:    logger,
		rand:      rand.New(rand.NewSource(seed)),
		client: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

var (
	severities = []string{"debug", "info", "info", "info", "notice", "warning", "error", "critical"}
	categories = []string{"auth", "api", "database", "deploy", "healthcheck", "file-transfer", "dlp"}
	hosts      = []string{"edge-01", "edge-02", "api-11", "api-12", "worker-3", "db-primary"}
	actors     = []string{"svc-ingest", "svc-billing", "alice.k", "bob.r", "ci-runner"}
	resources  = []string{"/v1/orders", "/v1/customers/42", "/v1/reports/daily", "/internal/metrics", "/v2/search"}
	messages   = []string{
		"request completed",
		"upstream timeout, retrying",
		"circuit breaker opened for peer",
		"token refresh succeeded",
		"payload rejected by schema validation",
		"checkpoint flushed",
	}
)

// nextBatch builds one batch of events.
func (g *syntheticGenerator) nextBatch(sequence int64) ([]byte, int) {
	type event struct {
		ID       string         `json:"id"`
		Source   string         `json:"source"`
		Host     string         `json:"host"`
		Severity string         `json:"severity"`
		Category string         `json:"category"`
		Actor    string         `json:"actor"`
		Resource string         `json:"resource"`
		TraceID  string         `json:"trace_id"`
		Message  string         `json:"message"`
		Email    string         `json:"contact_email"`
		Latency  float64        `json:"latency_ms"`
		Attrs    map[string]any `json:"attributes"`
	}

	g.mu.Lock()
	batch := make([]event, 0, g.batchSize)
	for i := 0; i < g.batchSize; i++ {
		index := int(sequence) + i
		category := categories[g.rand.Intn(len(categories))]
		event := event{
			ID:       fmt.Sprintf("evt-%d-%d", sequence, i),
			Source:   fmt.Sprintf("svc-%02d", g.rand.Intn(g.sources)),
			Host:     hosts[g.rand.Intn(len(hosts))],
			Severity: severities[g.rand.Intn(len(severities))],
			Category: category,
			Actor:    actors[g.rand.Intn(len(actors))],
			Resource: resources[g.rand.Intn(len(resources))],
			TraceID:  fmt.Sprintf("trace-%08x", g.rand.Uint32()),
			Message:  messages[g.rand.Intn(len(messages))],
			// Deliberate PII so the redaction stage has something to prove.
			Email:   fmt.Sprintf("user%d@example.com", index),
			Latency: float64(g.rand.Intn(9000)) / 10,
			Attrs: map[string]any{
				"region":     "ap-south-1",
				"retry":      g.rand.Intn(4),
				"status":     200 + g.rand.Intn(5)*100,
				"bytes":      g.rand.Intn(1 << 20),
				"session_id": fmt.Sprintf("sess-%06d", g.rand.Intn(1_000_000)),
			},
		}
		batch = append(batch, event)
	}
	g.mu.Unlock()

	var buffer bytes.Buffer
	for _, item := range batch {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		buffer.Write(raw)
		buffer.WriteByte('\n')
	}
	return buffer.Bytes(), len(batch)
}

// work posts batches at the configured rate until the context ends. The target
// rate is shared across the posting goroutines, so N workers do not multiply it.
func (g *syntheticGenerator) work(ctx context.Context) {
	interval := time.Duration(float64(time.Second) * float64(g.batchSize*g.workers) / float64(g.rate))
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var sequence int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		g.mu.Lock()
		sequence = g.rand.Int63n(1 << 40)
		g.mu.Unlock()

		body, count := g.nextBatch(sequence)
		g.sent.Add(int64(count))
		g.requests.Add(1)

		request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.url, bytes.NewReader(body))
		if err != nil {
			g.failed.Add(1)
			continue
		}
		request.Header.Set("Content-Type", "application/x-ndjson")
		if g.token != "" {
			request.Header.Set("Authorization", "Bearer "+g.token)
		}

		response, err := g.client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.failed.Add(1)
			if g.logger != nil {
				g.logger.Debug("generator request failed", "error", err.Error())
			}
			continue
		}
		status := response.StatusCode
		_ = response.Body.Close()

		if status == http.StatusAccepted {
			g.accepted.Add(int64(count))
			continue
		}
		g.failed.Add(1)
		if g.logger != nil {
			g.logger.Debug("generator got unexpected status", "status", status)
		}
	}
}

type generatorSummary struct {
	sent     int64
	accepted int64
	requests int64
	failed   int64
}

func (g *syntheticGenerator) summary() generatorSummary {
	return generatorSummary{
		sent:     g.sent.Load(),
		accepted: g.accepted.Load(),
		requests: g.requests.Load(),
		failed:   g.failed.Load(),
	}
}
