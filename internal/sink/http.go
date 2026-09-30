package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// httpSink posts batches as newline-delimited JSON. It treats any 5xx or 429 as
// retryable and any other non-2xx as a permanent failure, so a misconfigured URL
// shows up as a dead-letter backlog rather than an infinite retry loop.
type httpSink struct {
	*Base
	client *http.Client
	url    string
}

func newHTTPSink(base *Base) (Sink, error) {
	if base.cfg.URL == "" {
		return nil, fmt.Errorf("sink %q: url is required", base.cfg.ID)
	}
	timeout := base.cfg.Timeout.Duration()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &httpSink{
		Base: base,
		url:  base.cfg.URL,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}, nil
}

// RetryableError marks a failure that should be retried.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

func (s *httpSink) Write(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	payload, err := encodeBatch(events)
	if err != nil {
		s.noteFailure(err)
		return err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(payload))
	if err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: build request: %w", s.cfg.ID, err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("User-Agent", "streammesh")
	for key, value := range s.cfg.Headers {
		request.Header.Set(key, value)
	}

	response, err := s.client.Do(request)
	if err != nil {
		s.noteFailure(err)
		return &RetryableError{Err: fmt.Errorf("sink %q: post: %w", s.cfg.ID, err)}
	}
	defer func() { _ = response.Body.Close() }()

	// Drain a bounded amount so the connection can be reused.
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		s.noteSuccess(events, len(payload))
		return nil
	case response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests:
		err := fmt.Errorf("sink %q: status %d: %s", s.cfg.ID, response.StatusCode, truncate(string(body), 200))
		s.noteFailure(err)
		return &RetryableError{Err: err}
	default:
		err := fmt.Errorf("sink %q: status %d: %s", s.cfg.ID, response.StatusCode, truncate(string(body), 200))
		s.noteFailure(err)
		return err
	}
}

func (s *httpSink) Close(context.Context) error {
	s.client.CloseIdleConnections()
	return nil
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}
