package metrics

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCounterAccumulates(t *testing.T) {
	counter := &Counter{}
	counter.Inc()
	counter.Add(4)
	counter.Add(-1) // ignored

	if got := counter.Value(); got != 5 {
		t.Fatalf("value = %v, want 5", got)
	}
}

func TestGaugeGoesBothWays(t *testing.T) {
	gauge := &Gauge{}
	gauge.Set(10)
	gauge.Add(5)
	gauge.Sub(3)

	if got := gauge.Value(); got != 12 {
		t.Fatalf("value = %v, want 12", got)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	histogram := &Histogram{
		buckets: []float64{0.1, 0.5, 1},
		counts:  make([]atomic.Uint64, 3),
	}
	histogram.Observe(0.05)
	histogram.Observe(0.3)
	histogram.Observe(2)

	if histogram.Count() != 3 {
		t.Fatalf("count = %d, want 3", histogram.Count())
	}
	if sum := histogram.Sum(); sum < 2.34 || sum > 2.36 {
		t.Fatalf("sum = %v, want about 2.35", sum)
	}
	// 0.05 is within every bucket, 0.3 within the last two, 2 in none.
	want := []uint64{1, 2, 2}
	for index, expected := range want {
		if got := histogram.counts[index].Load(); got != expected {
			t.Errorf("bucket %d = %d, want %d", index, got, expected)
		}
	}
}

func TestRegistryReturnsTheSameHandleForTheSameLabels(t *testing.T) {
	registry := New()
	first := registry.Counter("requests_total", "help", map[string]string{"sink": "a"})
	second := registry.Counter("requests_total", "help", map[string]string{"sink": "a"})
	third := registry.Counter("requests_total", "help", map[string]string{"sink": "b"})

	first.Inc()
	if second.Value() != 1 {
		t.Fatal("the same name and labels should resolve to the same counter")
	}
	if third.Value() != 0 {
		t.Fatal("a different label set should be a separate counter")
	}
}

func TestWriteToRendersPrometheusText(t *testing.T) {
	registry := New()
	registry.Counter("events_total", "Events seen.", map[string]string{"source": "http"}).Add(3)
	registry.Gauge("queue_depth", "Queued events.", nil).Set(7)
	histogram := registry.Histogram("latency_seconds", "Latency.", nil, []float64{0.1, 1})
	histogram.Observe(0.05)
	histogram.Observe(0.5)

	var builder strings.Builder
	written, err := registry.WriteTo(&builder)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if written == 0 {
		t.Fatal("WriteTo reported zero bytes")
	}

	output := builder.String()
	expected := []string{
		"# HELP events_total Events seen.",
		"# TYPE events_total counter",
		`events_total{source="http"} 3`,
		"# TYPE queue_depth gauge",
		"queue_depth 7",
		"# TYPE latency_seconds histogram",
		`latency_seconds_bucket{le="0.1"} 1`,
		`latency_seconds_bucket{le="1"} 2`,
		`latency_seconds_bucket{le="+Inf"} 2`,
		"latency_seconds_count 2",
	}
	for _, want := range expected {
		if !strings.Contains(output, want) {
			t.Errorf("rendered metrics are missing %q\n%s", want, output)
		}
	}
}

func TestWriteToEscapesLabelValues(t *testing.T) {
	registry := New()
	registry.Counter("events_total", "Events seen.", map[string]string{"source": `weird "source"`}).Inc()

	var builder strings.Builder
	if _, err := registry.WriteTo(&builder); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if !strings.Contains(builder.String(), `source="weird \"source\""`) {
		t.Fatalf("label value was not escaped:\n%s", builder.String())
	}
}

func TestConcurrentRecordingIsRaceFree(t *testing.T) {
	registry := New()
	counter := registry.Counter("events_total", "Events.", nil)
	gauge := registry.Gauge("depth", "Depth.", nil)
	histogram := registry.Histogram("latency", "Latency.", nil, nil)

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				counter.Inc()
				gauge.Set(float64(i))
				histogram.Observe(0.001)
			}
		}()
	}
	wg.Wait()

	if counter.Value() != 4000 {
		t.Fatalf("counter = %v, want 4000", counter.Value())
	}
	if histogram.Count() != 4000 {
		t.Fatalf("histogram count = %d, want 4000", histogram.Count())
	}
}

func TestRuntimeCollectorReportsGoroutinesAndHeap(t *testing.T) {
	registry := New()
	collector := NewRuntimeCollector(registry)
	collector.Update()
	collector.Update() // a second scrape must not double-count

	var builder strings.Builder
	if _, err := registry.WriteTo(&builder); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	output := builder.String()
	for _, name := range []string{"streammesh_goroutines", "streammesh_heap_bytes", "streammesh_allocations_total"} {
		if !strings.Contains(output, name) {
			t.Errorf("runtime metric %s is missing", name)
		}
	}
}

func BenchmarkCounterAdd(b *testing.B) {
	counter := &Counter{}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Inc()
		}
	})
}
