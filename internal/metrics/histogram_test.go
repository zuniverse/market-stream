package metrics_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/metrics"
)

func bounds() []time.Duration {
	return []time.Duration{time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second}
}

func TestHistogramCountsAndSum(t *testing.T) {
	h := metrics.NewHistogram("test_latency_seconds", "help", bounds())
	for _, d := range []time.Duration{
		500 * time.Microsecond, // bucket 1ms
		5 * time.Millisecond,   // bucket 10ms
		50 * time.Millisecond,  // bucket 100ms
		2 * time.Second,        // +Inf
	} {
		h.Observe(d)
	}
	if got := h.Count(); got != 4 {
		t.Errorf("Count() = %d, want 4", got)
	}
	want := (500*time.Microsecond + 5*time.Millisecond + 50*time.Millisecond + 2*time.Second) / 4
	if got := h.Mean(); got != want {
		t.Errorf("Mean() = %v, want %v", got, want)
	}
}

// TestHistogramBoundIsInclusive pins which side of a bound a value falls on,
// because Prometheus buckets are "less than or equal" and getting it backwards
// is invisible until someone reads a dashboard.
func TestHistogramBoundIsInclusive(t *testing.T) {
	h := metrics.NewHistogram("t", "h", []time.Duration{time.Millisecond})
	h.Observe(time.Millisecond)
	var buf bytes.Buffer
	h.WriteProm(&buf, "")
	if !strings.Contains(buf.String(), `t_bucket{le="0.001"} 1`) {
		t.Errorf("a value equal to the bound did not land in it:\n%s", buf.String())
	}
}

func TestHistogramQuantile(t *testing.T) {
	h := metrics.NewHistogram("t", "h", bounds())
	if got := h.Quantile(0.5); got != 0 {
		t.Errorf("Quantile on an empty histogram = %v, want 0", got)
	}
	// 100 observations, all in the 1ms to 10ms bucket.
	for range 100 {
		h.Observe(5 * time.Millisecond)
	}
	p50 := h.Quantile(0.5)
	if p50 < time.Millisecond || p50 > 10*time.Millisecond {
		t.Errorf("p50 = %v, want it inside the bucket the data is in", p50)
	}

	// A quantile that falls in the +Inf bucket cannot be interpolated and
	// comes back as the largest bound, which understates it.
	tail := metrics.NewHistogram("t", "h", bounds())
	for range 99 {
		tail.Observe(time.Millisecond)
	}
	tail.Observe(time.Hour)
	if got, want := tail.Quantile(1), time.Second; got != want {
		t.Errorf("p100 with an overflow observation = %v, want the largest bound %v", got, want)
	}
	if got := tail.Quantile(0.5); got > time.Millisecond {
		t.Errorf("p50 = %v, want it at or below 1ms", got)
	}
}

// TestHistogramNegativeIsNotClamped covers clock skew: a tick-to-book of less
// than nothing is evidence about the clocks and must not become a fast p50.
func TestHistogramNegativeIsNotClamped(t *testing.T) {
	h := metrics.NewHistogram("t", "h", bounds())
	h.Observe(-5 * time.Millisecond)
	h.Observe(50 * time.Millisecond)

	if got := h.Negative(); got != 1 {
		t.Errorf("Negative() = %d, want 1", got)
	}
	if got := h.Count(); got != 1 {
		t.Errorf("Count() = %d, want 1: a negative observation is not one of the timings", got)
	}
	if got := h.Mean(); got != 50*time.Millisecond {
		t.Errorf("Mean() = %v, want the one real observation", got)
	}
	var buf bytes.Buffer
	h.WriteProm(&buf, "")
	if !strings.Contains(buf.String(), "t_negative_total 1") {
		t.Errorf("the negative count is not exposed:\n%s", buf.String())
	}
}

func TestHistogramWriteProm(t *testing.T) {
	h := metrics.NewHistogram("ms_latency_seconds", "Tick to book.", bounds())
	h.Observe(500 * time.Microsecond)
	h.Observe(50 * time.Millisecond)

	var buf bytes.Buffer
	h.WriteProm(&buf, `stage="book"`)
	out := buf.String()

	for _, want := range []string{
		"# TYPE ms_latency_seconds histogram",
		`ms_latency_seconds_bucket{stage="book",le="0.001"} 1`,
		`ms_latency_seconds_bucket{stage="book",le="0.01"} 1`,
		`ms_latency_seconds_bucket{stage="book",le="0.1"} 2`,
		`ms_latency_seconds_bucket{stage="book",le="+Inf"} 2`,
		`ms_latency_seconds_count{stage="book"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Buckets are cumulative, so each line must be at least the one above it.
	var prev int
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "_bucket{") {
			continue
		}
		var n int
		if _, err := fmtSscan(line, &n); err != nil {
			t.Fatalf("cannot read the count from %q", line)
		}
		if n < prev {
			t.Errorf("bucket counts are not cumulative at %q", line)
		}
		prev = n
	}
}

func fmtSscan(line string, n *int) (int, error) {
	fields := strings.Fields(line)
	var err error
	var v int
	v, err = atoi(fields[len(fields)-1])
	*n = v
	return 1, err
}

func atoi(s string) (int, error) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errNotANumber
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

var errNotANumber = errString("not a number")

type errString string

func (e errString) Error() string { return string(e) }

// TestHistogramConcurrent is the property that lets it sit on the path every
// event takes: observing from many goroutines at once loses nothing.
func TestHistogramConcurrent(t *testing.T) {
	h := metrics.NewHistogram("t", "h", bounds())
	const goroutines, each = 8, 1000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				h.Observe(time.Duration(i%20) * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	if got, want := h.Count(), uint64(goroutines*each); got != want {
		t.Errorf("Count() = %d, want %d", got, want)
	}
}

func TestHistogramBoundsAreSortedAndUnique(t *testing.T) {
	h := metrics.NewHistogram("t", "h", []time.Duration{
		time.Second, time.Millisecond, time.Second, 10 * time.Millisecond,
	})
	h.Observe(500 * time.Microsecond)
	var buf bytes.Buffer
	h.WriteProm(&buf, "")
	out := buf.String()
	if strings.Count(out, "_bucket{") != 4 { // 3 bounds plus +Inf
		t.Errorf("duplicate or unsorted bounds survived:\n%s", out)
	}
	want := []string{`le="0.001"`, `le="0.01"`, `le="1"`, `le="+Inf"`}
	var at int
	for _, w := range want {
		i := strings.Index(out, w)
		if i < at {
			t.Errorf("bound %s is out of order:\n%s", w, out)
		}
		at = i
	}
}
