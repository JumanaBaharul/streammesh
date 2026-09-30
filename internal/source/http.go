package source

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// httpSource accepts events over HTTP.
//
// The response status is the contract clients program against: 202 means the
// batch is durable (it is in the write-ahead log), 400 means the payload was
// unparseable, 401 means the bearer token was wrong, 413 means the body was too
// large, and 503 means the pipeline is saturated and the client should retry.
type httpSource struct {
	Base
	server  *http.Server
	webhook bool
	addr    string
	mu      sync.Mutex
}

func newHTTPSource(base Base) (Source, error) {
	return &httpSource{Base: base}, nil
}

func newWebhookSource(base Base) (Source, error) {
	return &httpSource{Base: base, webhook: true}, nil
}

func (s *httpSource) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("source %q: listen on %s: %w", s.cfg.ID, s.cfg.Listen, err)
	}

	s.mu.Lock()
	s.addr = listener.Addr().String()
	s.mu.Unlock()

	mux := http.NewServeMux()
	path := s.cfg.Path
	if path == "" {
		path = "/ingest"
	}
	mux.HandleFunc(path, s.handle)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	s.logInfo("ingest listener started", "addr", listener.Addr().String(), "path", path, "parser", s.cfg.Parser)

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("source %q: shutdown: %w", s.cfg.ID, err)
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("source %q: serve: %w", s.cfg.ID, err)
		}
		return nil
	}
}

// Addr implements Addressed, reporting the bound listen address.
func (s *httpSource) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Close releases the listener.
func (s *httpSource) Close() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *httpSource) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.cfg.Secret != "" {
		if s.webhook {
			if err := verifySignature(r, s.cfg.Secret); err != nil {
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}
		} else if !tokenMatches(r.Header.Get("Authorization"), s.cfg.Secret) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes.Or(8<<20))
	defer func() { _ = body.Close() }()

	payload, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	var events []event.Event
	if len(strings.TrimSpace(string(payload))) > 0 {
		events, err = s.decode(payload)
		if err != nil && len(events) == 0 {
			// Nothing usable came out of the payload, so the client must fix it.
			http.Error(w, "cannot parse payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err != nil {
			// Some records were malformed but the rest are good; the rejected
			// count is visible in metrics instead of failing the whole batch.
			s.logError(err, "partially rejected ingest payload")
		}
	}

	if err := s.deliver(events); err != nil {
		// The pipeline is saturated; the client should retry rather than assume
		// the data landed.
		http.Error(w, "ingest queue saturated, retry later", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "{\"accepted\":%d,\"source\":%q}\n", len(events), s.cfg.ID)
}

func tokenMatches(header, want string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return hmac.Equal([]byte(got), []byte(want))
}

// verifySignature checks an HMAC-SHA256 signature over the raw body. The
// signature may arrive as "sha256=<hex>" in X-StreamMesh-Signature or as a bare
// hex digest in X-Hub-Signature-256, so common webhook producers work as-is.
func verifySignature(r *http.Request, secret string) error {
	provided := r.Header.Get("X-StreamMesh-Signature")
	if provided == "" {
		provided = r.Header.Get("X-Hub-Signature-256")
	}
	if provided == "" {
		return errors.New("source: missing signature header")
	}
	provided = strings.TrimPrefix(provided, "sha256=")
	provided = strings.TrimSpace(provided)

	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("source: read body: %w", err)
	}
	// Restore the body for the parser.
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(strings.ToLower(provided))) {
		return errors.New("source: signature mismatch")
	}
	return nil
}
