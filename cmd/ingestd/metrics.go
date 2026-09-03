package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/zuniverse/market-stream/internal/pipeline"
)

// serveMetrics runs the HTTP server that exposes /metrics and /healthz until
// ctx is cancelled.
//
// The metrics are rendered in the Prometheus text format by hand rather than
// through the client library. The set exposed here is counters and one gauge,
// which the format expresses in three lines each, so the library would buy
// nothing today beyond a dependency. M6 is where the histograms arrive, and a
// histogram is where writing the format by hand stops being reasonable.
//
// Putting an http.Server in the process now is also what architecture.md
// asks for: the future query API attaches to a server that already exists
// rather than bringing its own.
func serveMetrics(ctx context.Context, logger *slog.Logger, addr string, router *pipeline.Router, pub *pipeline.Publisher, cnt *counters) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writeMetrics(w, router, pub, cnt)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// The listener is created here rather than inside ListenAndServe so that
	// the address actually bound can be reported. That matters for a port of
	// 0, which is what a test asks for and what a container sometimes gets.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "metrics listen",
			slog.String("addr", addr), slog.String("err", err.Error()))
		return
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "metrics listening",
		slog.String("addr", ln.Addr().String()))

	done := make(chan struct{})
	// Owner: serveMetrics. Exit: done closed, after ListenAndServe returns.
	go func() {
		select {
		case <-ctx.Done():
			// Shutdown needs a context of its own: ctx is already cancelled.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				logger.LogAttrs(ctx, slog.LevelWarn, "metrics shutdown", slog.String("err", err.Error()))
			}
		case <-done:
		}
	}()
	defer close(done)

	// A metrics endpoint that fails is worth reporting loudly and is not
	// worth stopping ingestion for.
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		logger.LogAttrs(ctx, slog.LevelError, "metrics serve",
			slog.String("addr", ln.Addr().String()), slog.String("err", err.Error()))
	}
}

// writeMetrics renders the current counters in the Prometheus text format.
func writeMetrics(w io.Writer, router *pipeline.Router, pub *pipeline.Publisher, cnt *counters) {
	st := router.Stats()
	counter := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}

	counter("market_stream_frames_total", "Websocket frames received.", cnt.frames.Load())
	counter("market_stream_decode_errors_total", "Frames that failed to decode.", cnt.decodeErrs.Load())
	counter("market_stream_route_errors_total", "Events that could not be handed to a shard.", cnt.routeErrs.Load())
	counter("market_stream_events_total", "Events handled by the book stage.", st.Events)
	counter("market_stream_deltas_applied_total", "Deltas applied to a book.", st.Applied)
	counter("market_stream_deltas_discarded_total", "Events already reflected in a book.", st.Discarded)
	counter("market_stream_deltas_buffered_total", "Deltas held during a resync.", st.Buffered)
	counter("market_stream_gaps_total", "Holes detected in an update id sequence.", st.Gaps)
	counter("market_stream_snapshots_total", "Snapshots that anchored a book.", st.Snapshots)
	counter("market_stream_refetches_total", "Snapshots that landed too old to anchor a book.", st.Refetches)
	counter("market_stream_fetch_failures_total", "Snapshot requests that returned an error.", st.FetchFails)
	counter("market_stream_book_errors_total", "Events a book refused.", st.Errors)
	counter("market_stream_queries_total", "Book reads answered.", st.Queries)
	counter("market_stream_checks_total", "Books compared against a fresh snapshot.", cnt.checks.Load())
	counter("market_stream_check_divergences_total", "Comparisons that found a divergence.", cnt.divergences.Load())

	fmt.Fprintf(w, "# HELP market_stream_shard_queue_depth Events waiting in a shard queue.\n")
	fmt.Fprintf(w, "# TYPE market_stream_shard_queue_depth gauge\n")
	for i, depth := range router.QueueDepth() {
		fmt.Fprintf(w, "market_stream_shard_queue_depth{shard=\"%d\"} %d\n", i, depth)
	}

	dropped := pub.Dropped()
	names := make([]string, 0, len(dropped))
	for name := range dropped {
		names = append(names, name)
	}
	sort.Strings(names) // stable output, so a diff between two scrapes is readable
	fmt.Fprintf(w, "# HELP market_stream_subscriber_dropped_total Events dropped for a slow subscriber.\n")
	fmt.Fprintf(w, "# TYPE market_stream_subscriber_dropped_total counter\n")
	for _, name := range names {
		// %q escapes the quote, the backslash and the newline exactly as the
		// exposition format requires for a label value.
		fmt.Fprintf(w, "market_stream_subscriber_dropped_total{subscriber=%q} %d\n", name, dropped[name])
	}
}
