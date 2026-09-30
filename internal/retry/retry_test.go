package retry

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func fastPolicy() Policy {
	return Policy{MaxAttempts: 4, Base: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}
}

func TestDoSucceedsAfterTransientFailures(t *testing.T) {
	attempts := 0
	err := fastPolicy().Do(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do returned %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDoGivesUpAfterMaxAttempts(t *testing.T) {
	attempts := 0
	sentinel := errors.New("still failing")
	err := fastPolicy().Do(context.Background(), func(context.Context) error {
		attempts++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the sentinel", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4", attempts)
	}
}

func TestPermanentFailuresStopImmediately(t *testing.T) {
	attempts := 0
	sentinel := errors.New("bad request")
	err := fastPolicy().Do(context.Background(), func(context.Context) error {
		attempts++
		return Permanent(sentinel)
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the sentinel unwrapped", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 for a permanent failure", attempts)
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	policy := Policy{MaxAttempts: 100, Base: 50 * time.Millisecond, Max: time.Second}

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := policy.Do(ctx, func(context.Context) error {
		attempts++
		return errors.New("transient")
	})
	if err == nil {
		t.Fatal("expected an error after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
}

func TestDeadlineErrorsAreNotRetried(t *testing.T) {
	attempts := 0
	err := fastPolicy().Do(context.Background(), func(context.Context) error {
		attempts++
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestMaxElapsedBoundsTotalTime(t *testing.T) {
	policy := Policy{MaxAttempts: 1000, Base: 10 * time.Millisecond, Max: 10 * time.Millisecond, MaxElapsed: 30 * time.Millisecond}
	start := time.Now()
	_ = policy.Do(context.Background(), func(context.Context) error { return errors.New("transient") })
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("MaxElapsed was not respected: %s", elapsed)
	}
}

func TestDelayGrowsExponentiallyAndIsCapped(t *testing.T) {
	policy := Policy{Base: 10 * time.Millisecond, Max: 40 * time.Millisecond}
	if got := policy.Delay(1); got != 10*time.Millisecond {
		t.Fatalf("delay(1) = %s, want 10ms", got)
	}
	if got := policy.Delay(2); got != 20*time.Millisecond {
		t.Fatalf("delay(2) = %s, want 20ms", got)
	}
	if got := policy.Delay(3); got != 40*time.Millisecond {
		t.Fatalf("delay(3) = %s, want 40ms", got)
	}
	if got := policy.Delay(9); got != 40*time.Millisecond {
		t.Fatalf("delay(9) = %s, want the 40ms cap", got)
	}
}

func TestJitterStaysWithinBounds(t *testing.T) {
	policy := Policy{Base: 100 * time.Millisecond, Max: time.Second, Jitter: 0.5}.
		WithRand(rand.New(rand.NewSource(42)))

	for attempt := 1; attempt <= 5; attempt++ {
		full := time.Duration(float64(policy.Base) * float64(int(1)<<(attempt-1)))
		if full > policy.Max {
			full = policy.Max
		}
		got := policy.Delay(attempt)
		if got > full || got < full/2 {
			t.Fatalf("attempt %d: jittered delay %s outside [%s, %s]", attempt, got, full/2, full)
		}
	}
}

func TestMaxAttemptsZeroStillTriesOnce(t *testing.T) {
	attempts := 0
	policy := Policy{}
	_ = policy.Do(context.Background(), func(context.Context) error {
		attempts++
		return errors.New("nope")
	})
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestOnRetryCallbackObservesAttempts(t *testing.T) {
	var seen []int
	policy := fastPolicy()
	policy.OnRetry = func(attempt int, delay time.Duration, err error) {
		seen = append(seen, attempt)
	}
	_ = policy.Do(context.Background(), func(context.Context) error { return errors.New("transient") })
	if len(seen) != 3 {
		t.Fatalf("OnRetry called %d times, want 3", len(seen))
	}
}
