package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/event"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/retry"
	"github.com/JumanaBaharul/streammesh/internal/sink"
)

// sinkWorker owns one destination: a bounded queue, a batching buffer, the
// retry policy and that sink's dead letter file.
//
// Every sink runs its own goroutine so a slow destination cannot starve a fast
// one. When a queue is full the dispatcher stops waiting and dead-letters
// instead, which keeps healthy sinks flowing while still losing nothing.
type sinkWorker struct {
	id            string
	sink          sink.Sink
	policy        retry.Policy
	batchSize     int
	flushInterval time.Duration
	submitTimeout time.Duration
	log           *slog.Logger

	queue chan event.Event
	dlq   *dlqWriter

	delivered   *metrics.Counter
	retries     *metrics.Counter
	failures    *metrics.Counter
	deadLetters *metrics.Counter
	queueDepth  *metrics.Gauge
	latency     *metrics.Histogram

	wg sync.WaitGroup
}

type workerOptions struct {
	Registry      *metrics.Registry
	Logger        *slog.Logger
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	SubmitTimeout time.Duration
	Policy        retry.Policy
	DLQDir        string
}

func newSinkWorker(target sink.Sink, cfg sinkWorkerConfig, opts workerOptions) (*sinkWorker, error) {
	dlq, err := newDLQWriter(opts.DLQDir, target.ID())
	if err != nil {
		return nil, err
	}

	worker := &sinkWorker{
		id:            target.ID(),
		sink:          target,
		policy:        opts.Policy,
		batchSize:     opts.BatchSize,
		flushInterval: opts.FlushInterval,
		submitTimeout: opts.SubmitTimeout,
		log:           opts.Logger,
		queue:         make(chan event.Event, opts.QueueSize),
		dlq:           dlq,
	}
	_ = cfg

	if opts.Registry != nil {
		labels := map[string]string{"sink": target.ID()}
		worker.delivered = opts.Registry.Counter("streammesh_sink_delivered_total", "Events delivered to a sink, including retries.", labels)
		worker.retries = opts.Registry.Counter("streammesh_sink_retries_total", "Delivery attempts retried for a sink.", labels)
		worker.failures = opts.Registry.Counter("streammesh_sink_batches_failed_total", "Batches that exhausted their retry budget.", labels)
		worker.deadLetters = opts.Registry.Counter("streammesh_dead_lettered_total", "Events written to a dead letter file, by sink.", labels)
		worker.queueDepth = opts.Registry.Gauge("streammesh_sink_queue_depth", "Events waiting for a sink, by sink.", labels)
		worker.latency = opts.Registry.Histogram("streammesh_sink_latency_seconds", "Sink delivery latency in seconds, by sink.", labels, nil)
	}
	return worker, nil
}

// sinkWorkerConfig is a placeholder for per-sink tuning that is not needed yet.
type sinkWorkerConfig struct{}

// submit queues one event, giving up after the submit timeout so a stalled sink
// cannot block the whole engine.
func (w *sinkWorker) submit(ctx context.Context, ev event.Event) error {
	timer := time.NewTimer(w.submitTimeout)
	defer timer.Stop()

	select {
	case w.queue <- ev:
		if w.queueDepth != nil {
			w.queueDepth.Set(float64(len(w.queue)))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errSinkSaturated
	}
}

var errSinkSaturated = errors.New("pipeline: sink queue saturated")

// start launches the delivery loop.
func (w *sinkWorker) start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(ctx)
	}()
}

func (w *sinkWorker) run(ctx context.Context) {
	batch := make([]event.Event, 0, w.batchSize)
	timer := time.NewTimer(w.flushInterval)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.flush(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain whatever is queued before returning, so an orderly shutdown
			// does not drop accepted events.
			for {
				select {
				case ev := <-w.queue:
					batch = append(batch, ev)
					if len(batch) >= w.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case ev, ok := <-w.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if w.queueDepth != nil {
				w.queueDepth.Set(float64(len(w.queue)))
			}
			if len(batch) >= w.batchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(w.flushInterval)
			}
		case <-timer.C:
			flush()
			timer.Reset(w.flushInterval)
		}
	}
}

// flush delivers a batch, retrying per the policy and dead-lettering on final
// failure.
func (w *sinkWorker) flush(ctx context.Context, batch []event.Event) {
	payload := make([]event.Event, len(batch))
	copy(payload, batch)

	started := time.Now()
	err := w.policy.Do(ctx, func(attemptCtx context.Context) error {
		err := w.sink.Write(attemptCtx, payload)
		if w.latency != nil {
			w.latency.Observe(time.Since(started).Seconds())
		}
		if err == nil {
			return nil
		}
		var retryable *sink.RetryableError
		if errors.As(err, &retryable) {
			if w.retries != nil {
				w.retries.Inc()
			}
			return err
		}
		// Anything else is a configuration or contract failure; retrying will
		// not help, so fail fast into the dead letter file.
		return retry.Permanent(err)
	})

	if err == nil {
		if w.delivered != nil {
			w.delivered.Add(float64(len(payload)))
		}
		return
	}

	if w.failures != nil {
		w.failures.Inc()
	}
	if w.deadLetters != nil {
		w.deadLetters.Add(float64(len(payload)))
	}
	if dlqErr := w.dlq.write(payload); dlqErr != nil && w.log != nil {
		w.log.Error("cannot write dead letter file",
			"sink", w.id, "error", dlqErr.Error(), "lost_events", len(payload))
	}
	if w.log != nil {
		w.log.Warn("batch dead-lettered after exhausting retries",
			"sink", w.id, "events", len(payload), "error", err.Error())
	}
}

// stop closes the queue and waits for the worker to finish.
func (w *sinkWorker) stop() {
	close(w.queue)
	w.wg.Wait()
}

// close releases the sink and the dead letter file.
func (w *sinkWorker) close(ctx context.Context) error {
	if err := w.sink.Close(ctx); err != nil {
		return err
	}
	return w.dlq.close()
}

// stats reports this worker's observable state.
func (w *sinkWorker) stats() SinkStats {
	stats := SinkStats{
		ID:            w.id,
		Type:          w.sink.Config().Type,
		Queued:        len(w.queue),
		QueueCapacity: cap(w.queue),
		DeadLettered:  w.dlq.Count(),
		DeadLetter:    w.dlq.Path(),
		Health:        healthString(w.sink.Health()),
	}
	if w.delivered != nil {
		stats.Delivered = int64(w.delivered.Value())
	}
	return stats
}

func healthString(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}
