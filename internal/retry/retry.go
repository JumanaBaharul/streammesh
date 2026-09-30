// Package retry provides bounded exponential backoff with jitter for sink
// delivery. Every sink shares the same policy shape so delivery behaviour is
// uniform and observable.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Policy describes how a failed operation is retried.
type Policy struct {
	// MaxAttempts is the total number of tries, including the first one.
	MaxAttempts int
	// Base is the delay before the first retry.
	Base time.Duration
	// Max caps the delay between attempts.
	Max time.Duration
	// Jitter spreads retries out; 0 disables it, 1 means full jitter.
	Jitter float64
	// MaxElapsed aborts retrying once this much time has passed (0 = unlimited).
	MaxElapsed time.Duration
	// OnRetry is called before each sleep, for logging and metrics.
	OnRetry func(attempt int, delay time.Duration, err error)
	// rand is injected in tests for determinism.
	rand *rand.Rand
}

// PermanentError marks a failure that retrying cannot fix, such as a 400 from a
// misconfigured endpoint. It is what keeps a bad sink from retrying forever.
type PermanentError struct {
	Err error
}

// Error implements error.
func (p *PermanentError) Error() string { return p.Err.Error() }

// Unwrap exposes the cause to errors.Is and errors.As.
func (p *PermanentError) Unwrap() error { return p.Err }

// Permanent wraps err so Do stops immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// DefaultPolicy is tuned for network sinks: fast first retry, then back off.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		Base:        25 * time.Millisecond,
		Max:         2 * time.Second,
		Jitter:      0.5,
		MaxElapsed:  15 * time.Second,
	}
}

// WithRand returns a copy that draws jitter from r, for deterministic tests.
func (p Policy) WithRand(r *rand.Rand) Policy {
	p.rand = r
	return p
}

// Delay returns the backoff before the given attempt (1-based).
func (p Policy) Delay(attempt int) time.Duration {
	base := p.Base
	if base <= 0 {
		base = 25 * time.Millisecond
	}
	delay := float64(base) * math.Pow(2, float64(attempt-1))
	if p.Max > 0 && delay > float64(p.Max) {
		delay = float64(p.Max)
	}
	if p.Jitter > 0 {
		r := p.rand
		if r == nil {
			r = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		spread := delay * p.Jitter
		if spread > 0 {
			delay -= r.Float64() * spread
		}
	}
	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

// Do runs fn until it succeeds, the attempts run out, the elapsed budget is
// spent, or the context is cancelled. It returns the last error seen.
func (p Policy) Do(ctx context.Context, fn func(context.Context) error) error {
	attempts := p.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	started := time.Now()
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%w: %v", err, lastErr)
			}
			return err
		}
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			return lastErr
		}
		var permanent *PermanentError
		if errors.As(lastErr, &permanent) {
			return permanent.Err
		}
		if attempt == attempts {
			break
		}
		if p.MaxElapsed > 0 && time.Since(started)+p.Delay(attempt) > p.MaxElapsed {
			break
		}
		delay := p.Delay(attempt)
		if p.OnRetry != nil {
			p.OnRetry(attempt, delay, lastErr)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
	return lastErr
}
