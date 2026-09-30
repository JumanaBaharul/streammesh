// Package metrics implements the slice of Prometheus instrumentation this
// engine needs, with no third-party dependency.
//
// Counters and gauges are plain CAS loops over atomic.Uint64 bit patterns, and
// histograms keep fixed bucket arrays, so recording a sample on the hot path
// never takes a lock. Handles are resolved once at startup and reused, which is
// why lookups (the only locking operation) never appear in the data path.
package metrics

import (
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultBuckets are millisecond-scale latency buckets, measured in seconds.
var DefaultBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}

// Counter is a monotonically increasing value.
type Counter struct {
	bits atomic.Uint64
}

// Inc adds one.
func (c *Counter) Inc() { c.Add(1) }

// Add adds delta, ignoring negatives.
func (c *Counter) Add(delta float64) {
	if delta <= 0 {
		return
	}
	addFloat(&c.bits, delta)
}

// Value returns the current total.
func (c *Counter) Value() float64 { return math.Float64frombits(c.bits.Load()) }

// Gauge is a value that can go up or down.
type Gauge struct {
	bits atomic.Uint64
}

// Set replaces the value.
func (g *Gauge) Set(v float64) { storeFloat(&g.bits, v) }

// Add increases the value.
func (g *Gauge) Add(delta float64) { addFloat(&g.bits, delta) }

// Sub decreases the value.
func (g *Gauge) Sub(delta float64) { addFloat(&g.bits, -delta) }

// Value returns the current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram tracks a distribution across fixed buckets.
type Histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	sum     atomic.Uint64
	count   atomic.Uint64
}

// Observe records one sample.
func (h *Histogram) Observe(v float64) {
	for i, upper := range h.buckets {
		if v <= upper {
			h.counts[i].Add(1)
		}
	}
	addFloat(&h.sum, v)
	h.count.Add(1)
}

// Count returns how many samples were recorded.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Sum returns the sum of all recorded samples.
func (h *Histogram) Sum() float64 { return math.Float64frombits(h.sum.Load()) }

func addFloat(addr *atomic.Uint64, delta float64) {
	for {
		old := addr.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if addr.CompareAndSwap(old, next) {
			return
		}
	}
}

func storeFloat(addr *atomic.Uint64, v float64) {
	for {
		old := addr.Load()
		if addr.CompareAndSwap(old, math.Float64bits(v)) {
			return
		}
	}
}

type family struct {
	name   string
	help   string
	kind   string // counter | gauge | histogram
	labels string // already-rendered {a="b",c="d"} or ""
	h      *Histogram
	obj    any
}

// Registry is a collection of metric families that can be rendered in the
// Prometheus text exposition format.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
	order    []string
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{families: map[string]*family{}}
}

func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteString("}")
	return b.String()
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteString("}")
	return b.String()
}

// Counter returns (creating if needed) the counter for a name and label set.
func (r *Registry) Counter(name, help string, labels map[string]string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels)
	if f, exists := r.families[k]; exists {
		return f.obj.(*Counter)
	}
	c := &Counter{}
	r.families[k] = &family{name: name, help: help, kind: "counter", labels: renderLabels(labels), obj: c}
	r.order = append(r.order, k)
	return c
}

// Gauge returns (creating if needed) the gauge for a name and label set.
func (r *Registry) Gauge(name, help string, labels map[string]string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels)
	if f, exists := r.families[k]; exists {
		return f.obj.(*Gauge)
	}
	g := &Gauge{}
	r.families[k] = &family{name: name, help: help, kind: "gauge", labels: renderLabels(labels), obj: g}
	r.order = append(r.order, k)
	return g
}

// Histogram returns (creating if needed) the histogram for a name and label set.
func (r *Registry) Histogram(name, help string, labels map[string]string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels)
	if f, exists := r.families[k]; exists {
		return f.h
	}
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	h := &Histogram{buckets: sorted, counts: make([]atomic.Uint64, len(sorted))}
	r.families[k] = &family{name: name, help: help, kind: "histogram", labels: renderLabels(labels), h: h}
	r.order = append(r.order, k)
	return h
}

// WriteTo renders every metric in the Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.RLock()
	keys := append([]string(nil), r.order...)
	families := make(map[string]*family, len(r.families))
	for k, f := range r.families {
		families[k] = f
	}
	r.mu.RUnlock()

	sort.Strings(keys)
	seenHelp := map[string]bool{}
	var written int64
	for _, k := range keys {
		f := families[k]
		if !seenHelp[f.name] {
			seenHelp[f.name] = true
			n, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
			written += int64(n)
			if err != nil {
				return written, err
			}
		}
		switch f.kind {
		case "counter":
			n, err := fmt.Fprintf(w, "%s%s %g\n", f.name, f.labels, f.obj.(*Counter).Value())
			written += int64(n)
			if err != nil {
				return written, err
			}
		case "gauge":
			n, err := fmt.Fprintf(w, "%s%s %g\n", f.name, f.labels, f.obj.(*Gauge).Value())
			written += int64(n)
			if err != nil {
				return written, err
			}
		case "histogram":
			labels := strings.TrimSuffix(strings.TrimPrefix(f.labels, "{"), "}")
			prefix := ""
			if labels != "" {
				prefix = labels + ","
			}
			for i, upper := range f.h.buckets {
				n, err := fmt.Fprintf(w, "%s_bucket{%sle=%q} %d\n", f.name, prefix, fmt.Sprintf("%g", upper), f.h.counts[i].Load())
				written += int64(n)
				if err != nil {
					return written, err
				}
			}
			n, err := fmt.Fprintf(w, "%s_bucket{%sle=\"+Inf\"} %d\n", f.name, prefix, f.h.count.Load())
			written += int64(n)
			if err != nil {
				return written, err
			}
			n, err = fmt.Fprintf(w, "%s_sum%s %g\n%s_count%s %d\n", f.name, f.labels, f.h.Sum(), f.name, f.labels, f.h.count.Load())
			written += int64(n)
			if err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// RuntimeCollector mirrors the Go runtime's own statistics into metric handles
// resolved once at startup, so a single /metrics scrape is enough to see
// goroutines, heap size, GC cycles and allocation churn next to engine metrics.
// Cumulative runtime counters are monotonically increasing, so only deltas are
// added - a rescrape must never double-count.
type RuntimeCollector struct {
	mu sync.Mutex

	numGoroutine *Gauge
	heapAlloc    *Gauge
	heapObjects  *Gauge
	gcCycles     *Counter
	mallocs      *Counter
	frees        *Counter

	lastGC      uint32
	lastMallocs uint64
	lastFrees   uint64
}

// NewRuntimeCollector creates and registers the runtime metric handles.
func NewRuntimeCollector(r *Registry) *RuntimeCollector {
	return &RuntimeCollector{
		numGoroutine: r.Gauge("streammesh_goroutines", "Number of goroutines in the process.", nil),
		heapAlloc:    r.Gauge("streammesh_heap_bytes", "Heap memory currently allocated, in bytes.", nil),
		heapObjects:  r.Gauge("streammesh_heap_objects", "Objects currently allocated on the heap.", nil),
		gcCycles:     r.Counter("streammesh_gc_cycles_total", "Garbage collection cycles completed since start.", nil),
		mallocs:      r.Counter("streammesh_allocations_total", "Heap allocations observed since start.", nil),
		frees:        r.Counter("streammesh_frees_total", "Heap frees observed since start.", nil),
	}
}

// Update refreshes every runtime metric from a fresh MemStats read.
func (c *RuntimeCollector) Update() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.numGoroutine.Set(float64(runtime.NumGoroutine()))
	c.heapAlloc.Set(float64(stats.HeapAlloc))
	c.heapObjects.Set(float64(stats.HeapObjects))

	if stats.NumGC > c.lastGC {
		c.gcCycles.Add(float64(stats.NumGC - c.lastGC))
		c.lastGC = stats.NumGC
	}
	if stats.Mallocs > c.lastMallocs {
		c.mallocs.Add(float64(stats.Mallocs - c.lastMallocs))
		c.lastMallocs = stats.Mallocs
	}
	if stats.Frees > c.lastFrees {
		c.frees.Add(float64(stats.Frees - c.lastFrees))
		c.lastFrees = stats.Frees
	}
}
