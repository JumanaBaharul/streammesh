// Package dedup suppresses duplicate events so that at-least-once delivery does
// not turn into at-least-once storage.
//
// The design is a two-tier filter:
//
//   - an exact time-to-live map, which is authoritative and answers with
//     certainly-duplicate or certainly-new for the recent window;
//   - a bloom filter covering a much longer window, which answers "definitely
//     not seen" cheaply and "probably seen" for events whose exact entry has
//     already expired.
//
// The probabilistic answer is deliberately not allowed to discard data by
// default. A bloom hit with no exact entry means the event might be a duplicate
// from an older window; dropping on that basis would silently lose genuinely new
// events when the filter saturates, which is the worst possible failure for a
// data pipeline. Instead the event is accepted and counted, and an operator who
// prefers aggressive dedup over guaranteed delivery can set drop_probable.
package dedup

import (
	"hash/fnv"
	"sync"
	"time"
)

// Set is a concurrent duplicate filter.
type Set struct {
	mu sync.Mutex

	ttl    time.Duration
	bits   []uint64
	mask   uint64
	hashes uint64

	dropProbable bool

	exact   map[uint64]int64 // hash -> unix-nano expiry
	sweepAt time.Time

	// Observability: counts are atomic-free because they are read under mu.
	inserted int64
	dupes    int64
	expired  int64
	probable int64
}

// Options configures a filter.
type Options struct {
	// TTL is the window covered by the authoritative exact map.
	TTL time.Duration
	// Bits is the bloom filter size, rounded up to a power of two.
	Bits uint64
	// Hashes is the number of bloom probes per ID.
	Hashes int
	// DropProbable makes a bloom-only hit discard the event.
	DropProbable bool
}

// New builds a filter with the given options. bits is rounded up to a power of
// two so the index modulo is a mask.
func New(opts Options) *Set {
	ttl := opts.TTL
	bits := opts.Bits
	hashes := opts.Hashes
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if bits < 64 {
		bits = 64
	}
	size := uint64(1)
	for size < bits {
		size <<= 1
	}
	if hashes < 1 {
		hashes = 3
	}
	return &Set{
		ttl:          ttl,
		bits:         make([]uint64, size/64),
		mask:         size - 1,
		hashes:       uint64(hashes),
		exact:        make(map[uint64]int64),
		sweepAt:      time.Now().Add(ttl),
		dropProbable: opts.DropProbable,
	}
}

// Seen reports whether the event ID has already been observed. New IDs are
// recorded; expired IDs are re-armed.
func (s *Set) Seen(id string) bool {
	if id == "" {
		return false
	}
	now := time.Now()
	h1, h2 := hashPair(id)

	s.mu.Lock()
	defer s.mu.Unlock()

	if now.After(s.sweepAt) {
		s.sweep(now)
	}

	// The exact map is authoritative for the recent window.
	if expiry, found := s.exact[h1]; found && expiry > now.UnixNano() {
		s.dupes++
		return true
	}

	inBloom := s.probe(h1, h2)
	s.set(h1, h2)
	s.exact[h1] = now.Add(s.ttl).UnixNano()
	s.inserted++

	if inBloom {
		// Seen before according to the long window, but the exact entry has
		// expired: a probable duplicate, not a certain one.
		s.probable++
		if s.dropProbable {
			s.dupes++
			return true
		}
	}
	return false
}

// probe reports whether every bloom index for the ID is already set.
func (s *Set) probe(h1, h2 uint64) bool {
	for i := uint64(0); i < s.hashes; i++ {
		index := (h1 + i*h2) & s.mask
		if s.bits[index>>6]&(1<<(index&63)) == 0 {
			return false
		}
	}
	return true
}

func (s *Set) set(h1, h2 uint64) {
	for i := uint64(0); i < s.hashes; i++ {
		index := (h1 + i*h2) & s.mask
		s.bits[index>>6] |= 1 << (index & 63)
	}
}

func (s *Set) sweep(now time.Time) {
	cutoff := now.UnixNano()
	for k, expiry := range s.exact {
		if expiry <= cutoff {
			delete(s.exact, k)
			s.expired++
		}
	}
	s.sweepAt = now.Add(s.ttl / 2)
}

// Stats reports filter counters for metrics and tests. probable counts events
// that a saturated or long-window filter recognised but that were accepted
// anyway because they were not certain duplicates.
func (s *Set) Stats() (inserted, duplicates, expired, tracked, probable int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inserted, s.dupes, s.expired, int64(len(s.exact)), s.probable
}

// hashPair derives two independent 64-bit hashes, which are combined
// Kirsch-Mitzenmacher style to produce k bloom indices.
func hashPair(id string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(id))
	first := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte(id))
	second := h2.Sum64()
	if second == 0 {
		second = 0x9e3779b97f4a7c15
	}
	return first, second
}
