package dedup

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSeenReportsRepeatsAndAcceptsNewIDs(t *testing.T) {
	set := New(Options{TTL: time.Minute, Bits: 1 << 16, Hashes: 3})

	if set.Seen("first") {
		t.Fatal("a brand new id was reported as a duplicate")
	}
	if !set.Seen("first") {
		t.Fatal("the second sighting of an id was not reported as a duplicate")
	}
	if set.Seen("second") {
		t.Fatal("an unrelated id was reported as a duplicate")
	}
}

func TestEmptyIDsAreNeverDuplicates(t *testing.T) {
	set := New(Options{TTL: time.Minute, Bits: 1 << 12, Hashes: 3})
	if set.Seen("") {
		t.Fatal("an empty id should never be treated as a duplicate")
	}
}

// TestSaturatedFilterDoesNotDropUniqueEvents is the regression test for the bug
// found in the first end-to-end run: an undersized bloom filter saturated, every
// unique event looked like a duplicate, and 99.9% of the stream was discarded.
// A probabilistic hit must never discard data.
func TestSaturatedFilterDoesNotDropUniqueEvents(t *testing.T) {
	// A deliberately absurd 64-bit filter, which saturates after a few dozen ids.
	set := New(Options{TTL: time.Minute, Bits: 64, Hashes: 3})

	const events = 5000
	accepted := 0
	for i := 0; i < events; i++ {
		if !set.Seen(fmt.Sprintf("unique-event-%d", i)) {
			accepted++
		}
	}
	if accepted != events {
		t.Fatalf("saturated filter discarded %d of %d unique events", events-accepted, events)
	}

	_, duplicates, _, _, probable := set.Stats()
	if duplicates != 0 {
		t.Fatalf("certain duplicates = %d, want 0", duplicates)
	}
	if probable == 0 {
		t.Fatal("expected the saturated filter to report probable duplicates")
	}
}

func TestDropProbableDiscardsProbableDuplicates(t *testing.T) {
	set := New(Options{TTL: time.Minute, Bits: 64, Hashes: 3, DropProbable: true})

	dropped := 0
	for i := 0; i < 5000; i++ {
		if set.Seen(fmt.Sprintf("unique-event-%d", i)) {
			dropped++
		}
	}
	if dropped == 0 {
		t.Fatal("drop_probable did not discard any probable duplicates")
	}
}

func TestEntriesExpireAfterTheTTL(t *testing.T) {
	set := New(Options{TTL: 20 * time.Millisecond, Bits: 1 << 16, Hashes: 3})

	if set.Seen("event") {
		t.Fatal("first sighting reported as a duplicate")
	}
	time.Sleep(30 * time.Millisecond)
	// The exact entry has expired, so this is a bloom-only hit.
	if set.Seen("event") {
		t.Fatal("an expired entry should not be reported as a certain duplicate")
	}

	_, _, expired, _, probable := set.Stats()
	if probable == 0 {
		t.Fatal("expected the expired window to be reported as probable")
	}
	_ = expired
}

func TestSweepBoundsMemory(t *testing.T) {
	set := New(Options{TTL: 15 * time.Millisecond, Bits: 1 << 16, Hashes: 3})

	for i := 0; i < 500; i++ {
		set.Seen(fmt.Sprintf("event-%d", i))
	}
	time.Sleep(40 * time.Millisecond)
	set.Seen("trigger-sweep")

	_, _, expired, tracked, _ := set.Stats()
	if expired == 0 {
		t.Fatal("expected expired entries to be reclaimed")
	}
	if tracked > 200 {
		t.Fatalf("tracked %d entries after a sweep, expected the map to shrink", tracked)
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	set := New(Options{TTL: time.Minute, Bits: 1 << 20, Hashes: 4})

	var wg sync.WaitGroup
	duplicates := make([]int, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Each worker owns a disjoint id space, so exactly zero of its ids
			// may be reported as duplicates.
			for i := 0; i < 2000; i++ {
				if set.Seen(fmt.Sprintf("worker-%d-event-%d", index, i)) {
					duplicates[index]++
				}
			}
		}(worker)
	}
	wg.Wait()

	for index, count := range duplicates {
		if count != 0 {
			t.Fatalf("worker %d saw %d false duplicates", index, count)
		}
	}
}

func BenchmarkSeenNewID(b *testing.B) {
	set := New(Options{TTL: time.Minute, Bits: 1 << 24, Hashes: 3})
	ids := make([]string, 1024)
	for i := range ids {
		ids[i] = fmt.Sprintf("benchmark-event-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Seen(ids[i%len(ids)])
	}
}
