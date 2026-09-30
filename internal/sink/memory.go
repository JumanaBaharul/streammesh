package sink

import (
	"context"
	"sync"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// memorySink keeps the most recent events in a bounded ring buffer. It exists so
// tests, benchmarks and the compose demo need no external system, and it backs
// the "memory" sink type used by the demo config.
type memorySink struct {
	*Base
	mu     sync.RWMutex
	buffer []event.Event
	next   int
	full   bool
	total  int64
}

func newMemorySink(base *Base) (Sink, error) {
	size := base.cfg.BufferSize
	if size <= 0 {
		size = 1024
	}
	return &memorySink{Base: base, buffer: make([]event.Event, size)}, nil
}

func (s *memorySink) Write(_ context.Context, events []event.Event) error {
	payload, err := encodeBatch(events)
	if err != nil {
		s.noteFailure(err)
		return err
	}

	s.mu.Lock()
	for i := range events {
		s.buffer[s.next] = events[i]
		s.next = (s.next + 1) % len(s.buffer)
		if s.next == 0 {
			s.full = true
		}
	}
	s.total += int64(len(events))
	s.mu.Unlock()

	s.noteSuccess(events, len(payload))
	return nil
}

// Recent returns up to n of the most recently written events, oldest first.
func (s *memorySink) Recent(n int) []event.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	size := s.next
	if s.full {
		size = len(s.buffer)
	}
	if n <= 0 || n > size {
		n = size
	}
	out := make([]event.Event, 0, n)
	start := s.next - n
	if start < 0 {
		start += len(s.buffer)
	}
	for i := 0; i < n; i++ {
		out = append(out, s.buffer[(start+i)%len(s.buffer)])
	}
	return out
}

// Total returns how many events this sink has received.
func (s *memorySink) Total() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.total
}
