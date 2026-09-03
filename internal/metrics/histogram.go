package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// Histogram counts observed durations into fixed buckets.
//
// It is safe for concurrent use and lock free: every field is an atomic, and
// an observation is one binary search and two adds. A reader can see the
// bucket counts and the total disagree by an observation or two, which is the
// normal property of any counter read while it is moving and is not worth a
// lock on the path that records every event in the system.
type Histogram struct {
	name   string
	help   string
	bounds []time.Duration // upper bounds, ascending, exclusive of +Inf
	counts []atomic.Uint64 // one per bound, plus one for +Inf
	sum    atomic.Int64    // nanoseconds
	total  atomic.Uint64

	// negative counts observations below zero, which for a tick-to-book
	// measurement means the exchange clock is ahead of the local one. They
	// are counted rather than clamped into the first bucket: a latency of
	// less than nothing is evidence about the clocks, not about the
	// pipeline, and burying it in the p50 would hide both (D43).
	negative atomic.Uint64
}

// NewHistogram returns a histogram over the given upper bounds, which are
// sorted and deduplicated. An implicit +Inf bucket is always added.
func NewHistogram(name, help string, bounds []time.Duration) *Histogram {
	sorted := append([]time.Duration(nil), bounds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	uniq := sorted[:0]
	for i, b := range sorted {
		if i == 0 || b != sorted[i-1] {
			uniq = append(uniq, b)
		}
	}
	return &Histogram{
		name:   name,
		help:   help,
		bounds: uniq,
		counts: make([]atomic.Uint64, len(uniq)+1),
	}
}

// Observe records one duration.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		h.negative.Add(1)
		return
	}
	i := sort.Search(len(h.bounds), func(i int) bool { return d <= h.bounds[i] })
	h.counts[i].Add(1)
	h.sum.Add(int64(d))
	h.total.Add(1)
}

// Count returns how many durations have been observed, not counting negative
// ones.
func (h *Histogram) Count() uint64 { return h.total.Load() }

// Negative returns how many observations were below zero.
func (h *Histogram) Negative() uint64 { return h.negative.Load() }

// Mean returns the average observation, or zero if there are none.
func (h *Histogram) Mean() time.Duration {
	n := h.total.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sum.Load() / int64(n))
}

// Quantile returns an estimate of the q-th quantile, q in [0,1].
//
// The estimate is linear interpolation inside the bucket the quantile falls
// in, so its accuracy is the width of that bucket and no better. An
// observation in the +Inf bucket cannot be interpolated at all and comes back
// as the largest bound, which understates it. Both are stated wherever the
// number is printed: a p99 quoted without its bucket width is a number
// pretending to a precision it does not have.
func (h *Histogram) Quantile(q float64) time.Duration {
	n := h.total.Load()
	if n == 0 {
		return 0
	}
	switch {
	case q <= 0:
		q = 0
	case q >= 1:
		q = 1
	}
	want := q * float64(n)

	var cum, prevCum float64
	var lower time.Duration
	for i := range h.counts {
		cum += float64(h.counts[i].Load())
		if cum < want {
			prevCum = cum
			if i < len(h.bounds) {
				lower = h.bounds[i]
			}
			continue
		}
		if i >= len(h.bounds) {
			// The +Inf bucket: there is no upper bound to interpolate to.
			return h.bounds[len(h.bounds)-1]
		}
		upper := h.bounds[i]
		inBucket := cum - prevCum
		if inBucket <= 0 {
			return upper
		}
		frac := (want - prevCum) / inBucket
		return lower + time.Duration(float64(upper-lower)*frac)
	}
	return h.bounds[len(h.bounds)-1]
}

// WriteProm renders the histogram in the Prometheus text exposition format.
// labels, if given, are rendered on every series; they must already be
// escaped.
func (h *Histogram) WriteProm(w io.Writer, labels string) {
	sep := ""
	if labels != "" {
		sep = ","
	}
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)

	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{%s%sle=\"%s\"} %d\n", h.name, labels, sep, secondsLabel(b), cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", h.name, labels, sep, cum)

	open, close := "", ""
	if labels != "" {
		open, close = "{"+labels+"}", ""
	}
	fmt.Fprintf(w, "%s_sum%s%s %g\n", h.name, open, close, time.Duration(h.sum.Load()).Seconds())
	fmt.Fprintf(w, "%s_count%s%s %d\n", h.name, open, close, cum)
	if n := h.negative.Load(); n > 0 || labels == "" {
		fmt.Fprintf(w, "# HELP %s_negative_total Observations below zero, which mean the clocks disagree.\n", h.name)
		fmt.Fprintf(w, "# TYPE %s_negative_total counter\n%s_negative_total%s%s %d\n",
			h.name, h.name, open, close, n)
	}
}

// secondsLabel renders a bucket bound as Prometheus wants it, in seconds.
func secondsLabel(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'g', -1, 64)
}
