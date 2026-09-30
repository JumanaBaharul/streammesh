// Package server exposes the control plane: health and readiness probes,
// Prometheus metrics, runtime statistics, rule reloading and replay.
//
// Ingestion endpoints are owned by their sources, not by this server, so a
// control-plane restart can never interrupt the data path and each source can
// listen wherever it is configured.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/pipeline"
	"github.com/JumanaBaharul/streammesh/internal/rules"
)

// Server is the control-plane HTTP server.
type Server struct {
	pipe   *pipeline.Pipeline
	cfg    *config.Config
	log    *slog.Logger
	runtim *metrics.RuntimeCollector
	http   *http.Server
	addr   string
	mu     sync.Mutex
}

// New creates the control-plane server. A nil logger falls back to a discard
// handler, because a control plane that panics when nobody configured logging is
// worse than one that stays quiet.
func New(pipe *pipeline.Pipeline, cfg *config.Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{
		pipe:   pipe,
		cfg:    cfg,
		log:    logger,
		runtim: metrics.NewRuntimeCollector(pipe.Registry()),
	}
}

// Start serves until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/rules", s.handleRules)
	mux.HandleFunc("/replay", s.handleReplay)
	mux.HandleFunc("/dlq/replay", s.handleDLQReplay)

	listener, err := net.Listen("tcp", s.cfg.Server.Addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Server.Addr, err)
	}

	s.http = &http.Server{
		Handler:           withLogging(s.log, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       s.cfg.Server.ReadTimeout.Or(15 * time.Second),
		WriteTimeout:      s.cfg.Server.WriteTimeout.Or(30 * time.Second),
		IdleTimeout:       60 * time.Second,
	}

	s.mu.Lock()
	s.addr = listener.Addr().String()
	s.mu.Unlock()

	s.log.Info("control plane listening", "addr", listener.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Addr reports the bound control-plane address, which is useful when the
// configuration asks for an ephemeral port.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Close shuts the server down.
func (s *Server) Close() error {
	if s.http == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{\"status\":\"ok\"}\n"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := s.pipe.Ready(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"status": "not ready", "reason": err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.runtim.Update()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := s.pipe.Registry().WriteTo(w); err != nil {
		s.log.Warn("cannot render metrics", "error", err.Error())
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, s.pipe.Stats())
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]any{"rules": s.pipe.Engine().Rules()})
	case http.MethodPut, http.MethodPost:
		if !s.authorized(w, r) {
			return
		}
		var incoming struct {
			Rules []rules.Rule `yaml:"rules" json:"rules"`
		}
		body, err := readLimited(r, 8<<20)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := decodeRules(body, &incoming); err != nil {
			http.Error(w, "cannot parse rules: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.pipe.ReloadRules(incoming.Rules); err != nil {
			// The running ruleset is untouched when compilation fails.
			http.Error(w, "rules rejected, previous rules still active: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		writeJSON(w, map[string]any{"status": "reloaded", "count": len(incoming.Rules)})
	default:
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(w, r) {
		return
	}
	body, err := readLimited(r, 1<<20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var request struct {
		From  uint64 `json:"from"`
		Limit int    `json:"limit"`
	}
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, "cannot parse request: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	result, err := s.pipe.ReplayWAL(ctx, request.From, request.Limit)
	if err != nil {
		http.Error(w, "replay failed: "+err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, result)
}

func (s *Server) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(w, r) {
		return
	}
	body, err := readLimited(r, 1<<20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var request struct {
		Sink string `json:"sink"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Sink == "" {
		http.Error(w, "a sink id is required", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	result, err := s.pipe.ReplayDLQ(ctx, request.Sink)
	if err != nil {
		http.Error(w, "replay failed: "+err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, result)
}

// authorized enforces the control-plane bearer token when one is configured.
// Mutating endpoints are the only ones protected, so probes and metrics never
// need credentials.
func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	token := s.cfg.Server.APIToken
	if token == "" {
		return true
	}
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if strings.TrimSpace(strings.TrimPrefix(header, prefix)) != token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func decodeRules(body []byte, target *struct {
	Rules []rules.Rule `yaml:"rules" json:"rules"`
}) error {
	if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
		var list []rules.Rule
		if err := json.Unmarshal(body, &list); err != nil {
			return err
		}
		target.Rules = list
		return nil
	}
	return yaml.Unmarshal(body, target)
}

func readLimited(r *http.Request, limit int64) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	buffer := make([]byte, 0, 1024)
	chunk := make([]byte, 4096)
	var total int64
	for {
		read, err := r.Body.Read(chunk)
		if read > 0 {
			total += int64(read)
			if total > limit {
				return nil, errors.New("request body too large")
			}
			buffer = append(buffer, chunk[:read]...)
		}
		if err != nil {
			break
		}
	}
	return buffer, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func withLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		if logger == nil {
			return
		}
		logger.Debug("control plane request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration", time.Since(started).String())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
