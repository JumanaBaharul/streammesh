// Package pipeline wires sources, the rule engine and sinks into one running
// engine, and owns the delivery guarantees, backpressure and dead lettering.
//
// The shape of the data path is deliberately boring:
//
//	source -> Ingest -> [dedup] -> [write-ahead log] -> ingress queue
//	       -> dispatcher workers (rule evaluation) -> per-sink queues
//	       -> sink delivery workers (batching, retry) -> sink or dead letter file
//
// Every arrow crosses a bounded channel. Nothing is unbounded, so a stall
// anywhere becomes visible queue depth instead of an out-of-memory kill.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/dedup"
	"github.com/JumanaBaharul/streammesh/internal/dlp"
	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/retry"
	"github.com/JumanaBaharul/streammesh/internal/rules"
	"github.com/JumanaBaharul/streammesh/internal/sink"
	"github.com/JumanaBaharul/streammesh/internal/source"
	"github.com/JumanaBaharul/streammesh/internal/wal"
)

// Errors returned to sources and callers.
var (
	// ErrSaturated means the ingress queue did not accept the event in time.
	ErrSaturated = errors.New("pipeline: ingest queue saturated")
	// ErrClosed means the pipeline is shutting down.
	ErrClosed = errors.New("pipeline: closed")
)

// Options configures a pipeline.
type Options struct {
	Config   *config.Config
	Logger   *slog.Logger
	Registry *metrics.Registry
	Enricher rules.Enricher
}

// Pipeline is the running engine.
type Pipeline struct {
	cfg   *config.Config
	log   *slog.Logger
	reg   *metrics.Registry
	start time.Time

	engine   *rules.Engine
	walLog   *wal.Log
	dedupSet *dedup.Set

	sources     []source.Source
	sinks       map[string]sink.Sink
	sinkWorkers map[string]*sinkWorker

	ingress   chan event.Event
	ctx       context.Context
	cancel    context.CancelFunc
	sendMu    sync.RWMutex // guards sends to ingress against shutdown close
	closed    bool
	workersWG sync.WaitGroup
	sourcesWG sync.WaitGroup

	ingested     *metrics.Counter
	duplicates   *metrics.Counter
	rulesDropped *metrics.Counter
	unrouted     *metrics.Counter
	saturated    *metrics.Counter
	walErrors    *metrics.Counter
	overflowed   *metrics.Counter
	queueDepth   *metrics.Gauge
	processTime  *metrics.Histogram
	replayed     *metrics.Counter
}

// New builds a pipeline from configuration. Every connector, rule and ruleset
// is constructed here, so a bad configuration fails at startup with a precise
// message rather than at 3am with a dropped batch.
func New(opts Options) (*Pipeline, error) {
	if opts.Config == nil {
		return nil, errors.New("pipeline: config is required")
	}
	registry := opts.Registry
	if registry == nil {
		registry = metrics.New()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	cfg := opts.Config
	library, err := dlp.NewLibrary(cfg.DLP.RuleSets)
	if err != nil {
		return nil, err
	}

	engine, err := rules.New(cfg.Rules, library, opts.Enricher, registry)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		cfg:         cfg,
		log:         logger,
		reg:         registry,
		start:       time.Now(),
		engine:      engine,
		sinks:       map[string]sink.Sink{},
		sinkWorkers: map[string]*sinkWorker{},
		ingress:     make(chan event.Event, cfg.Pipeline.QueueSize),

		ingested:     registry.Counter("streammesh_ingested_total", "Events accepted into the pipeline.", nil),
		duplicates:   registry.Counter("streammesh_duplicates_total", "Events discarded as duplicates.", nil),
		rulesDropped: registry.Counter("streammesh_rules_dropped_total", "Events discarded by a rule, before routing.", nil),
		unrouted:     registry.Counter("streammesh_unrouted_total", "Events that matched no route and no default sink.", nil),
		saturated:    registry.Counter("streammesh_saturated_total", "Ingest attempts rejected because the queue was full.", nil),
		walErrors:    registry.Counter("streammesh_wal_errors_total", "Write-ahead log failures.", nil),
		overflowed:   registry.Counter("streammesh_sink_overflow_total", "Events dead-lettered because a sink queue was full.", nil),
		queueDepth:   registry.Gauge("streammesh_queue_depth", "Events waiting in the ingress queue.", nil),
		processTime:  registry.Histogram("streammesh_processing_seconds", "Time to evaluate rules and route one event.", nil, nil),
		replayed:     registry.Counter("streammesh_replayed_total", "Events replayed from the write-ahead log.", nil),
	}

	if cfg.Pipeline.WAL.IsEnabled() {
		log, err := wal.Open(wal.Config{
			Dir:           cfg.Pipeline.WAL.Dir,
			SegmentBytes:  cfg.Pipeline.WAL.SegmentBytes.Int64(),
			FsyncInterval: cfg.Pipeline.WAL.FsyncInterval.Duration(),
			SyncEach:      cfg.Pipeline.WAL.SyncEach,
			MaxSegments:   cfg.Pipeline.WAL.MaxSegments,
		})
		if err != nil {
			return nil, err
		}
		p.walLog = log
		logger.Info("write-ahead log ready",
			"dir", cfg.Pipeline.WAL.Dir,
			"fsync_interval", cfg.Pipeline.WAL.FsyncInterval.Duration().String(),
			"sync_each", cfg.Pipeline.WAL.SyncEach)
	}

	if cfg.Pipeline.Dedup.IsEnabled() {
		p.dedupSet = dedup.New(dedup.Options{
			TTL:          cfg.Pipeline.Dedup.TTL.Duration(),
			Bits:         uint64(cfg.Pipeline.Dedup.Bits),
			Hashes:       cfg.Pipeline.Dedup.Hashes,
			DropProbable: cfg.Pipeline.Dedup.DropProbable,
		})
		logger.Info("duplicate filter ready",
			"ttl", cfg.Pipeline.Dedup.TTL.Duration().String(),
			"bits", cfg.Pipeline.Dedup.Bits,
			"drop_probable", cfg.Pipeline.Dedup.DropProbable)
	}

	defaultPolicy := retry.Policy{
		MaxAttempts: cfg.Pipeline.Retry.MaxAttempts,
		Base:        cfg.Pipeline.Retry.Base.Duration(),
		Max:         cfg.Pipeline.Retry.Max.Duration(),
		Jitter:      cfg.Pipeline.Retry.Jitter,
		MaxElapsed:  cfg.Pipeline.Retry.MaxElapsed.Duration(),
	}

	dlqDir := cfg.Pipeline.WAL.Dir
	for _, sinkCfg := range cfg.Sinks {
		if !sinkCfg.IsEnabled() {
			continue
		}
		target, err := sink.Build(sinkCfg, sink.Deps{Registry: registry, Logger: logger})
		if err != nil {
			return nil, err
		}
		policy := defaultPolicy
		if sinkCfg.Retry.MaxAttempts > 0 {
			policy.MaxAttempts = sinkCfg.Retry.MaxAttempts
		}
		if sinkCfg.Retry.Base > 0 {
			policy.Base = sinkCfg.Retry.Base.Duration()
		}
		if sinkCfg.Retry.Max > 0 {
			policy.Max = sinkCfg.Retry.Max.Duration()
		}
		if sinkCfg.Retry.MaxElapsed > 0 {
			policy.MaxElapsed = sinkCfg.Retry.MaxElapsed.Duration()
		}

		worker, err := newSinkWorker(target, sinkWorkerConfig{}, workerOptions{
			Registry:      registry,
			Logger:        logger,
			QueueSize:     cfg.Pipeline.SinkQueueSize,
			BatchSize:     sinkCfg.BatchSize,
			FlushInterval: sinkCfg.FlushInterval.Duration(),
			SubmitTimeout: time.Second,
			Policy:        policy,
			DLQDir:        dlqDir,
		})
		if err != nil {
			return nil, err
		}
		p.sinks[sinkCfg.ID] = target
		p.sinkWorkers[sinkCfg.ID] = worker
	}

	for _, sourceCfg := range cfg.Sources {
		if !sourceCfg.IsEnabled() {
			continue
		}
		src, err := source.Build(sourceCfg, source.Deps{
			Emit:     p.ingest,
			Registry: registry,
			Logger:   logger,
		})
		if err != nil {
			return nil, err
		}
		p.sources = append(p.sources, src)
	}

	return p, nil
}

// Registry exposes the metrics registry, for the /metrics endpoint.
func (p *Pipeline) Registry() *metrics.Registry { return p.reg }

// Engine exposes the rule engine, for the control plane.
func (p *Pipeline) Engine() *rules.Engine { return p.engine }

// Config exposes the loaded configuration.
func (p *Pipeline) Config() *config.Config { return p.cfg }

// Sinks lists the configured sinks.
func (p *Pipeline) Sinks() map[string]sink.Sink { return p.sinks }

// Start launches sinks, dispatchers and sources. It returns immediately; the
// context governs the lifetime of the internal goroutines.
func (p *Pipeline) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	for id, worker := range p.sinkWorkers {
		worker.start(p.ctx)
		p.log.Info("sink worker started",
			"id", id, "type", p.sinks[id].Config().Type,
			"queue", cap(worker.queue), "batch", worker.batchSize)
	}

	for i := 0; i < p.cfg.Pipeline.Workers; i++ {
		p.workersWG.Add(1)
		go func(index int) {
			defer p.workersWG.Done()
			p.dispatchLoop(index)
		}(i)
	}
	p.log.Info("dispatcher workers started", "count", p.cfg.Pipeline.Workers)

	for _, src := range p.sources {
		src := src
		p.sourcesWG.Add(1)
		go func() {
			defer p.sourcesWG.Done()
			if err := src.Start(p.ctx); err != nil {
				p.log.Error("source stopped with error", "source", src.ID(), "error", err.Error())
			}
		}()
	}
	return nil
}

// ingest is the callback sources call with freshly parsed events.
func (p *Pipeline) ingest(events []event.Event) error {
	return p.accept(events, false)
}

// Ingest accepts events from a caller, applying dedup, durability and
// backpressure. A non-nil error means the caller must retry the batch.
func (p *Pipeline) Ingest(events []event.Event) error {
	return p.accept(events, false)
}

// accept is the shared implementation; replay bypasses dedup so replayed events
// are never mistaken for duplicates of themselves.
func (p *Pipeline) accept(events []event.Event, bypassDedup bool) error {
	if len(events) == 0 {
		return nil
	}
	timeout := p.cfg.Pipeline.AcceptTimeout.Duration()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)

	for i := range events {
		ev := events[i]
		ev.EnsureID()

		if p.dedupSet != nil && !bypassDedup && p.dedupSet.Seen(ev.ID) {
			p.duplicates.Inc()
			continue
		}

		if p.walLog != nil {
			raw, err := json.Marshal(ev)
			if err != nil {
				p.walErrors.Inc()
				return fmt.Errorf("pipeline: encode for write-ahead log: %w", err)
			}
			if _, err := p.walLog.Append(raw); err != nil {
				p.walErrors.Inc()
				return fmt.Errorf("pipeline: write-ahead log: %w", err)
			}
		}

		if err := p.send(ev, time.Until(deadline)); err != nil {
			return err
		}
		p.ingested.Inc()
		if p.queueDepth != nil {
			p.queueDepth.Set(float64(len(p.ingress)))
		}
	}
	return nil
}

func (p *Pipeline) send(ev event.Event, remaining time.Duration) error {
	if remaining <= 0 {
		p.saturated.Inc()
		return ErrSaturated
	}

	// Holding the read lock guarantees shutdown cannot close the channel
	// underneath a send in progress.
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	if p.closed {
		return ErrClosed
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case p.ingress <- ev:
		return nil
	case <-p.sendDone():
		return ErrClosed
	case <-timer.C:
		p.saturated.Inc()
		return ErrSaturated
	}
}

func (p *Pipeline) sendDone() <-chan struct{} {
	if p.ctx == nil {
		return make(chan struct{})
	}
	return p.ctx.Done()
}

// dispatchLoop pulls events from the ingress queue and routes them.
func (p *Pipeline) dispatchLoop(index int) {
	_ = index
	for {
		ev, ok := <-p.ingress
		if !ok {
			return
		}
		p.dispatch(ev)
	}
}

// dispatch evaluates the rules and hands the event to every routed sink.
func (p *Pipeline) dispatch(ev event.Event) {
	started := time.Now()
	ctx := p.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	decision := p.engine.Evaluate(ctx, &ev)
	if p.processTime != nil {
		p.processTime.Observe(time.Since(started).Seconds())
	}
	if decision.Drop {
		p.rulesDropped.Inc()
		return
	}

	targets := decision.Sinks
	if len(targets) == 0 {
		targets = p.cfg.Pipeline.DefaultSinks
	}
	if len(targets) == 0 {
		p.unrouted.Inc()
		return
	}

	for _, id := range targets {
		worker, ok := p.sinkWorkers[id]
		if !ok {
			p.unrouted.Inc()
			continue
		}
		// Fan-out hands each sink its own deep copy, so one connector can never
		// observe another's mutation.
		payload := ev
		if len(targets) > 1 {
			payload = ev.Clone()
		}
		if err := worker.submit(ctx, payload); err != nil {
			// The sink is stalled. The event is preserved in the dead letter
			// file rather than dropped, and healthy sinks keep flowing.
			p.overflowed.Inc()
			if dlqErr := worker.dlq.write([]event.Event{payload}); dlqErr == nil {
				worker.deadLetters.Inc()
			}
		}
	}
}

// SinkStats is the observable state of one sink.
type SinkStats struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// Delivered counts events that reached the destination, which is the only
	// number that proves ingest throughput became delivery throughput.
	Delivered     int64  `json:"delivered"`
	Queued        int    `json:"queued"`
	QueueCapacity int    `json:"queue_capacity"`
	DeadLettered  int64  `json:"dead_lettered"`
	DeadLetter    string `json:"dead_letter_file,omitempty"`
	Health        string `json:"health"`
}

// WALStats is the observable state of the write-ahead log.
type WALStats struct {
	Enabled   bool  `json:"enabled"`
	Appends   int64 `json:"appends"`
	Syncs     int64 `json:"syncs"`
	Rotations int64 `json:"rotations"`
	Segment   int64 `json:"segment"`
	Size      int64 `json:"size_bytes"`
	Unsynced  int64 `json:"unsynced_bytes"`
} // DedupStats is the observable state of the duplicate filter.
type DedupStats struct {
	Enabled bool  `json:"enabled"`
	Tracked int64 `json:"tracked"`
	Expired int64 `json:"expired"`
	// Probable counts events the long window recognised but that were accepted
	// anyway, because a bloom hit alone is not proof of a duplicate.
	Probable int64 `json:"probable_kept"`
}

// Stats is the pipeline's full observable state, served at /stats.
type Stats struct {
	Uptime        string         `json:"uptime"`
	Ingested      int64          `json:"ingested"`
	Duplicates    int64          `json:"duplicates"`
	RulesDropped  int64          `json:"rules_dropped"`
	Unrouted      int64          `json:"unrouted"`
	Saturated     int64          `json:"saturated"`
	WALErrors     int64          `json:"wal_errors"`
	SinkOverflow  int64          `json:"sink_overflow"`
	Replayed      int64          `json:"replayed"`
	QueueDepth    int            `json:"queue_depth"`
	QueueCapacity int            `json:"queue_capacity"`
	Rules         int            `json:"rules"`
	Sources       []source.Stats `json:"sources"`
	Sinks         []SinkStats    `json:"sinks"`
	WAL           WALStats       `json:"wal"`
	Dedup         DedupStats     `json:"dedup"`
}

// Stats collects a point-in-time snapshot. Every counter here reads an atomic
// value, so a /stats scrape never blocks the data path.
func (p *Pipeline) Stats() Stats {
	stats := Stats{
		Uptime:        time.Since(p.start).Round(time.Second).String(),
		Ingested:      int64(p.ingested.Value()),
		Duplicates:    int64(p.duplicates.Value()),
		RulesDropped:  int64(p.rulesDropped.Value()),
		Unrouted:      int64(p.unrouted.Value()),
		Saturated:     int64(p.saturated.Value()),
		WALErrors:     int64(p.walErrors.Value()),
		SinkOverflow:  int64(p.overflowed.Value()),
		Replayed:      int64(p.replayed.Value()),
		QueueDepth:    len(p.ingress),
		QueueCapacity: cap(p.ingress),
		Rules:         len(p.engine.Rules()),
		Sources:       make([]source.Stats, 0, len(p.sources)),
		Sinks:         make([]SinkStats, 0, len(p.sinkWorkers)),
	}

	for _, src := range p.sources {
		stats.Sources = append(stats.Sources, src.Snapshot())
	}
	for _, worker := range p.sinkWorkers {
		stats.Sinks = append(stats.Sinks, worker.stats())
	}

	if p.walLog != nil {
		logStats := p.walLog.Stats()
		stats.WAL = WALStats{
			Enabled:   true,
			Appends:   logStats.Appends,
			Syncs:     logStats.Syncs,
			Rotations: logStats.Rotations,
			Segment:   logStats.Segment,
			Size:      logStats.Size,
			Unsynced:  logStats.Unsynced,
		}
	}
	if p.dedupSet != nil {
		_, _, expired, tracked, probable := p.dedupSet.Stats()
		stats.Dedup = DedupStats{Enabled: true, Tracked: tracked, Expired: expired, Probable: probable}
	}
	return stats
}

// Ready reports whether the pipeline can accept traffic. A sink with a recorded
// failure makes the pipeline not ready, which is what a load balancer needs to
// take it out of rotation.
func (p *Pipeline) Ready() error {
	for id, target := range p.sinks {
		if err := target.Health(); err != nil {
			return fmt.Errorf("pipeline: sink %s unhealthy: %w", id, err)
		}
	}
	if p.walLog != nil && p.walLog.Stats().Unsynced > 0 {
		// Unsynced bytes are expected between flushes; only report a problem if
		// the log has never synced at all.
		if p.walLog.Stats().Syncs == 0 && p.walLog.Stats().Appends > 0 {
			return errors.New("pipeline: write-ahead log has not synced")
		}
	}
	return nil
}

// ReloadRules swaps in a new rule set without restarting. Events already in
// flight finish under the old rules.
func (p *Pipeline) ReloadRules(cfgs []rules.Rule) error {
	if err := p.engine.Reload(cfgs); err != nil {
		return err
	}
	p.cfg.Rules = cfgs
	p.log.Info("rules reloaded", "count", len(cfgs))
	return nil
} // WALReplayResult summarises a write-ahead log replay.
type WALReplayResult struct {
	From       uint64 `json:"from"`
	Read       int    `json:"read"`
	Accepted   int    `json:"accepted"`
	Duplicates int    `json:"duplicates"`
	Errors     int    `json:"errors"`
}

// ReplayWAL re-drives records from the write-ahead log through the current
// rules. It is how a backfill after a bad rule or an outage is performed, and it
// deliberately bypasses the duplicate filter because the events being replayed
// are by definition already recorded.
func (p *Pipeline) ReplayWAL(ctx context.Context, from uint64, limit int) (WALReplayResult, error) {
	if p.walLog == nil {
		return WALReplayResult{}, errors.New("pipeline: write-ahead log is disabled")
	}
	reader, err := p.walLog.NewReader(from)
	if err != nil {
		return WALReplayResult{}, err
	}
	defer func() { _ = reader.Close() }()

	result := WALReplayResult{From: from}
	for limit <= 0 || result.Read < limit {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, err
		}
		result.Read++

		var ev event.Event
		if err := json.Unmarshal(entry.Payload, &ev); err != nil {
			result.Errors++
			continue
		}
		if err := p.accept([]event.Event{ev}, true); err != nil {
			result.Errors++
			continue
		}
		result.Accepted++
		p.replayed.Inc()
	}
	return result, nil
}

// Shutdown drains the pipeline in order: stop producing, drain the ingress
// queue, stop dispatching, let the sinks finish their batches, then close the
// log. Each step is bounded by the configured shutdown grace.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	grace := p.cfg.Pipeline.ShutdownGrace.Duration()
	if grace <= 0 {
		grace = 10 * time.Second
	}
	deadline := time.Now().Add(grace)

	p.log.Info("shutting down", "grace", grace.String())

	// 1. Stop taking new work.
	for _, src := range p.sources {
		if err := src.Close(); err != nil {
			p.log.Warn("source close failed", "source", src.ID(), "error", err.Error())
		}
	}
	waitOrTimeout(&p.sourcesWG, time.Until(deadline))

	// 2. Let the dispatchers chew through what sources already produced.
	for len(p.ingress) > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	remaining := len(p.ingress)
	if remaining > 0 {
		p.log.Warn("ingress queue not fully drained before shutdown", "remaining", remaining)
	}

	// 3. Close the ingress queue under the send lock, then wait for dispatchers.
	p.sendMu.Lock()
	p.closed = true
	close(p.ingress)
	p.sendMu.Unlock()
	p.workersWG.Wait()

	// 4. Drain and stop sink workers.
	for _, worker := range p.sinkWorkers {
		worker.stop()
	}

	// 5. Flush and close sinks, then the log.
	var firstErr error
	for id, worker := range p.sinkWorkers {
		if err := worker.close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pipeline: close sink %s: %w", id, err)
		}
	}
	if p.walLog != nil {
		if err := p.walLog.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pipeline: close write-ahead log: %w", err)
		}
	}
	if p.cancel != nil {
		p.cancel()
	}

	stats := p.Stats()
	p.log.Info("shutdown complete",
		"ingested", stats.Ingested,
		"delivered_events_are_per_sink", true,
		"dead_lettered", deadLetteredTotal(stats))
	return firstErr
}

func deadLetteredTotal(stats Stats) int64 {
	var total int64
	for _, s := range stats.Sinks {
		total += s.DeadLettered
	}
	return total
}

func waitOrTimeout(group *sync.WaitGroup, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
