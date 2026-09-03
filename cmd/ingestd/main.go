// Command ingestd ingests Binance market data, maintains order books, and
// exposes what it is doing on a metrics endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/metrics"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
	"github.com/zuniverse/market-stream/internal/record"
)

// Channel capacities. Every channel in the process has an explicit bound and
// an explicit policy for being full, so these are constants rather than
// flags: they are sized once against the observed rate, and an operator who
// needs to change one needs a profile, not a flag (D19 keeps the flag surface
// to what an operator actually decides).
const (
	// frameCap holds raw frames between the socket reader and the decoder.
	// Full means the decoder has fallen behind, and the transport blocks,
	// which is correct on the lossless path.
	frameCap = 4096

	// shardQueueCap holds decoded events between the decoder and one shard.
	// Full means Route blocks, which is the backpressure D29 describes.
	shardQueueCap = 4096

	// subCap holds events for one subscriber. Full means the oldest queued
	// event is dropped and counted, which is the lossy path.
	subCap = 1024
)

// config holds the parsed flag values. The four names D19 commits to are
// -symbols, -endpoint, -shards and -metrics-addr, and they will not be
// renamed without a deprecation notice.
type config struct {
	symbols      symbolList
	endpoint     string
	restEndpoint string
	shards       int
	metricsAddr  string
	summaryEvery time.Duration
	checkEvery   time.Duration
	recordDir    string
}

// symbolList collects the repeatable -symbols flag. Each value is a
// normalised BASE-QUOTE pair; the exchange's own spelling never appears in
// the configuration of this binary (D16).
type symbolList []model.Symbol

func (l *symbolList) String() string {
	names := make([]string, len(*l))
	for i, s := range *l {
		names[i] = string(s)
	}
	return strings.Join(names, ",")
}

func (l *symbolList) Set(v string) error {
	sym := model.Symbol(strings.ToUpper(strings.TrimSpace(v)))
	base, quote, ok := strings.Cut(string(sym), "-")
	if !ok || base == "" || quote == "" || strings.Contains(quote, "-") {
		return fmt.Errorf("%q is not a BASE-QUOTE pair, for example BTC-USDT", v)
	}
	if slices.Contains(*l, sym) {
		return fmt.Errorf("%s given twice", sym)
	}
	*l = append(*l, sym)
	return nil
}

// instruments are the figures the main loop owns and the metrics handler
// reads, which is why the counters are atomics and the histograms are safe
// for concurrent use.
type instruments struct {
	started time.Time

	frames      atomic.Uint64
	decodeErrs  atomic.Uint64
	routeErrs   atomic.Uint64
	checks      atomic.Uint64
	divergences atomic.Uint64
	skewed      atomic.Uint64

	// tickToBook is the primary health indicator: the venue's event time to
	// the moment a book applies the event. It crosses two clocks, so it
	// carries their skew, which is why the histogram counts observations
	// below zero separately instead of clamping them (D43).
	tickToBook *metrics.Histogram
	decode     *metrics.Histogram
	resync     *metrics.Histogram
}

// Bucket bounds. Wide enough at the top to show a stall, fine enough at the
// bottom that a healthy run does not collapse into the first bucket, and few
// enough that the exposition stays readable.
var (
	tickToBookBounds = []time.Duration{
		time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
		10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
		100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		time.Second, 2 * time.Second, 5 * time.Second,
	}
	decodeBounds = []time.Duration{
		time.Microsecond, 2 * time.Microsecond, 5 * time.Microsecond,
		10 * time.Microsecond, 25 * time.Microsecond, 50 * time.Microsecond,
		100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond,
		time.Millisecond, 5 * time.Millisecond,
	}
	resyncBounds = []time.Duration{
		10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
		100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		time.Second, 2500 * time.Millisecond, 5 * time.Second, 10 * time.Second,
	}
)

func newInstruments() *instruments {
	return &instruments{
		started: time.Now(),
		tickToBook: metrics.NewHistogram("market_stream_tick_to_book_seconds",
			"Venue event time to the moment a book applied the event.", tickToBookBounds),
		decode: metrics.NewHistogram("market_stream_decode_seconds",
			"Time to decode one frame into an event.", decodeBounds),
		resync: metrics.NewHistogram("market_stream_resync_seconds",
			"Time from a book asking for a snapshot to one anchoring it.", resyncBounds),
	}
}

func main() {
	var cfg config
	flag.Var(&cfg.symbols, "symbols", "symbol to ingest as a normalised BASE-QUOTE pair; repeat for more (default BTC-USDT)")
	flag.StringVar(&cfg.endpoint, "endpoint", binance.DefaultWSEndpoint, "Binance websocket endpoint")
	flag.StringVar(&cfg.restEndpoint, "rest-endpoint", binance.DefaultRESTEndpoint, "Binance REST endpoint, for instrument metadata and depth snapshots")
	flag.IntVar(&cfg.shards, "shards", runtime.NumCPU(), "number of book shard goroutines")
	flag.StringVar(&cfg.metricsAddr, "metrics-addr", "127.0.0.1:9090", "listen address for /metrics and /healthz")
	flag.DurationVar(&cfg.summaryEvery, "summary-interval", 30*time.Second, "how often to log a run summary")
	flag.DurationVar(&cfg.checkEvery, "check-interval", 5*time.Minute, "how often to check each book against a fresh snapshot; 0 disables")
	flag.StringVar(&cfg.recordDir, "record-dir", "", "directory for hourly recordings; empty disables recording")
	flag.Parse()

	if len(cfg.symbols) == 0 {
		cfg.symbols = symbolList{"BTC-USDT"}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(ctx, cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// run owns the process lifetime. It returns context.Canceled on a clean
// shutdown, which main treats as success.
func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	inst := newInstruments()

	// Fetched once at startup. The cache serves symbol normalisation in the
	// decoder, decimal exponents in the log subscriber, and the reverse
	// lookup that builds the stream URL and the depth requests (D16).
	httpClient := &http.Client{Timeout: 10 * time.Second}
	metaBody, err := binance.FetchExchangeInfoRaw(ctx, httpClient, cfg.restEndpoint, cfg.symbols)
	if err != nil {
		return err
	}
	cache, err := binance.ParseExchangeInfo(metaBody)
	if err != nil {
		return err
	}
	streamURL, err := binance.CombinedStreamURL(cfg.endpoint, cache, cfg.symbols)
	if err != nil {
		return err
	}
	depth := binance.NewDepthClient(httpClient, cfg.restEndpoint, cache, 0)

	// The recorder sits beside the pipeline rather than in it: it sees every
	// frame and every snapshot, and nothing waits for it (D37). A recording
	// needs the snapshots as much as the frames, since a replayed book is
	// anchored on one (D35).
	var recorder *record.Recorder
	snapshots := pipeline.Snapshotter(depth)
	if cfg.recordDir != "" {
		recorder, err = record.NewRecorder(cfg.recordDir, metaBody, 0, logger)
		if err != nil {
			return err
		}
		if err := recorder.Start(ctx); err != nil {
			return err
		}
		defer recorder.Close()
		snapshots = record.RecordSnapshots(recorder, depth)
	}

	// Every normalised event leaves this process through the publisher. The
	// binary must not write pipeline events directly, or the fan-out seam
	// goes untested until the milestone that makes changing it expensive
	// (D17).
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(pipeline.NewLogSubscriber("stdout-log", logger, cache), subCap); err != nil {
		return err
	}
	if err := pub.Start(ctx); err != nil {
		return err
	}
	defer pub.Close()

	router, err := pipeline.NewRouter(pipeline.RouterConfig{
		Shards:     cfg.shards,
		QueueCap:   shardQueueCap,
		Snapshots:  snapshots,
		Log:        logger,
		TickToBook: inst.tickToBook,
		Resync:     inst.resync,
	})
	if err != nil {
		return err
	}
	if err := router.Start(ctx); err != nil {
		return err
	}
	defer router.Close()

	frames := make(chan model.Frame, frameCap)
	transport := binance.NewTransport(streamURL, frames)

	// Background workers stop before the stages they read from. The deferred
	// calls run last-registered-first, so the order here is: workers stop,
	// then the router drains, then the publisher drains.
	workers, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		stopWorkers()
		wg.Wait()
	}()

	// Owner: run. Exit: workers cancelled, then Shutdown returns.
	wg.Add(1)
	go func() {
		defer wg.Done()
		serveMetrics(workers, logger, cfg.metricsAddr, router, pub, recorder, transport, inst)
	}()

	if cfg.checkEvery > 0 {
		// Owner: run. Exit: workers cancelled.
		wg.Add(1)
		go func() {
			defer wg.Done()
			runChecker(workers, logger, router, depth, cfg.symbols, cfg.checkEvery, inst)
		}()
	}

	// Owner: run. Exit: ctx cancelled, which is the only way Transport.Run
	// returns. The buffer of 1 keeps the send from blocking after run returns.
	errc := make(chan error, 1)
	go func() { errc <- transport.Run(ctx) }()

	// The end-of-run summary is the point of the instruments: a process that
	// ran for six hours should say what it did on the way out, not leave it
	// to whoever was scraping.
	defer func() { logFinal(logger, router, pub, recorder, transport, inst) }()

	logger.LogAttrs(ctx, slog.LevelInfo, "started",
		slog.String("symbols", cfg.symbols.String()),
		slog.Int("shards", router.Shards()),
		slog.Int("depth_limit", depth.Limit()),
		slog.String("metrics_addr", cfg.metricsAddr),
		slog.String("record_dir", cfg.recordDir))

	dec := binance.NewDecoder(cache)
	ticker := time.NewTicker(cfg.summaryEvery)
	defer ticker.Stop()

	for {
		select {
		case f := <-frames:
			inst.frames.Add(1)
			if recorder != nil {
				// Before decoding: what is recorded is what arrived, so that
				// the decoder stays inside the loop a profile measures (D6).
				recorder.Frame(f)
			}
			decodeStart := time.Now()
			ev, err := dec.Decode(f)
			inst.decode.Observe(time.Since(decodeStart))
			if err != nil {
				inst.decodeErrs.Add(1)
				logger.LogAttrs(ctx, slog.LevelWarn, "decode", slog.String("err", err.Error()))
				continue
			}
			// The book stage first: it is upstream of the subscribers, and it
			// is the one that may block. Publishing first would show a
			// subscriber an event the books have not accepted yet.
			if err := router.Route(ctx, ev); err != nil {
				inst.routeErrs.Add(1)
				return err
			}
			pub.Publish(ev)

		case <-ticker.C:
			logSummary(ctx, logger, router, pub, recorder, inst)

		case err := <-errc:
			return err
		}
	}
}

// logSummary writes one line describing the run so far. M6 replaces the
// counters here with the histograms that give the same picture with
// percentiles rather than totals.
func logSummary(ctx context.Context, logger *slog.Logger, router *pipeline.Router, pub *pipeline.Publisher, rec *record.Recorder, inst *instruments) {
	st := router.Stats()
	logger.LogAttrs(ctx, slog.LevelInfo, "summary",
		slog.Uint64("frames", inst.frames.Load()),
		slog.Uint64("decode_errors", inst.decodeErrs.Load()),
		slog.Group("book",
			slog.Uint64("applied", st.Applied),
			slog.Uint64("discarded", st.Discarded),
			slog.Uint64("buffered", st.Buffered),
			slog.Uint64("gaps", st.Gaps),
			slog.Uint64("snapshots", st.Snapshots),
			slog.Uint64("refetches", st.Refetches),
			slog.Uint64("fetch_failures", st.FetchFails),
			slog.Uint64("errors", st.Errors)),
		slog.Group("checks",
			slog.Uint64("run", inst.checks.Load()),
			slog.Uint64("diverged", inst.divergences.Load()),
			slog.Uint64("skewed", inst.skewed.Load())),
		slog.Any("queue_depth", router.QueueDepth()),
		recordAttr(rec),
		droppedAttr(pub))
}

// recordAttr renders the recorder's counters, or an empty group when nothing
// is being recorded.
func recordAttr(rec *record.Recorder) slog.Attr {
	if rec == nil {
		return slog.Group("recording")
	}
	st := rec.Stats()
	return slog.Group("recording",
		slog.Uint64("written", st.Written),
		slog.Uint64("dropped", st.Dropped),
		slog.Uint64("files", st.Files))
}

// droppedAttr renders the publisher's per-subscriber drop counters as a log
// group.
func droppedAttr(pub *pipeline.Publisher) slog.Attr {
	counts := pub.Dropped()
	attrs := make([]any, 0, len(counts))
	for name, n := range counts {
		attrs = append(attrs, slog.Uint64(name, n))
	}
	return slog.Group("dropped", attrs...)
}

// logFinal writes the end-of-run summary. It is the one place the run reports
// what it did rather than what it is doing, and it is deliberately made of
// measured figures: how many messages, how fast, how late, how much was
// dropped, how often a book had to be rebuilt.
//
// The percentiles come from bucketed histograms, so each is only as precise
// as the bucket it lands in. That is stated in the line itself rather than in
// a document nobody reads next to the number.
func logFinal(logger *slog.Logger, router *pipeline.Router, pub *pipeline.Publisher, rec *record.Recorder, tr *binance.Transport, inst *instruments) {
	elapsed := time.Since(inst.started)
	frames := inst.frames.Load()
	var rate float64
	if elapsed > 0 {
		rate = float64(frames) / elapsed.Seconds()
	}
	st := router.Stats()

	logger.LogAttrs(context.Background(), slog.LevelInfo, "run summary",
		slog.Duration("elapsed", elapsed.Round(time.Second)),
		slog.Uint64("frames", frames),
		slog.Float64("frames_per_second", round1(rate)),
		slog.Uint64("decode_errors", inst.decodeErrs.Load()),
		slog.Uint64("route_errors", inst.routeErrs.Load()),
		latencyAttr("tick_to_book", inst.tickToBook),
		latencyAttr("decode", inst.decode),
		latencyAttr("resync", inst.resync),
		slog.Group("book",
			slog.Uint64("applied", st.Applied),
			slog.Uint64("gaps", st.Gaps),
			slog.Uint64("resyncs", st.Snapshots),
			slog.Uint64("refetches", st.Refetches),
			slog.Uint64("errors", st.Errors)),
		slog.Group("checks",
			slog.Uint64("run", inst.checks.Load()),
			slog.Uint64("diverged", inst.divergences.Load()),
			slog.Uint64("skewed", inst.skewed.Load())),
		slog.Uint64("reconnects", tr.Reconnects()),
		recordAttr(rec),
		droppedAttr(pub),
		slog.String("percentiles", "interpolated from bucket bounds; accurate to the width of the bucket"))
}

// latencyAttr renders one histogram as a group, or an empty one when nothing
// was observed. An empty group is more honest than a p99 of zero.
func latencyAttr(name string, h *metrics.Histogram) slog.Attr {
	if h.Count() == 0 {
		return slog.Group(name, slog.String("observations", "none"))
	}
	return slog.Group(name,
		slog.Uint64("count", h.Count()),
		slog.Duration("mean", h.Mean()),
		slog.Duration("p50", h.Quantile(0.50)),
		slog.Duration("p95", h.Quantile(0.95)),
		slog.Duration("p99", h.Quantile(0.99)),
		// A negative tick-to-book means the venue's clock is ahead of this
		// machine's. It says nothing about the pipeline and would drag the
		// percentiles down if it were counted as a fast observation.
		slog.Uint64("negative", h.Negative()))
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }
