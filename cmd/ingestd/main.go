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

// counters are the figures the main loop owns and the metrics handler reads,
// which is why they are atomics.
type counters struct {
	frames      atomic.Uint64
	decodeErrs  atomic.Uint64
	routeErrs   atomic.Uint64
	checks      atomic.Uint64
	divergences atomic.Uint64
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
	var cnt counters

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
		Shards:    cfg.shards,
		QueueCap:  shardQueueCap,
		Snapshots: snapshots,
		Log:       logger,
	})
	if err != nil {
		return err
	}
	if err := router.Start(ctx); err != nil {
		return err
	}
	defer router.Close()

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
		serveMetrics(workers, logger, cfg.metricsAddr, router, pub, recorder, &cnt)
	}()

	if cfg.checkEvery > 0 {
		// Owner: run. Exit: workers cancelled.
		wg.Add(1)
		go func() {
			defer wg.Done()
			runChecker(workers, logger, router, depth, cfg.symbols, cfg.checkEvery, &cnt)
		}()
	}

	frames := make(chan model.Frame, frameCap)
	transport := binance.NewTransport(streamURL, frames)

	// Owner: run. Exit: ctx cancelled, which is the only way Transport.Run
	// returns. The buffer of 1 keeps the send from blocking after run returns.
	errc := make(chan error, 1)
	go func() { errc <- transport.Run(ctx) }()

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
			cnt.frames.Add(1)
			if recorder != nil {
				// Before decoding: what is recorded is what arrived, so that
				// the decoder stays inside the loop a profile measures (D6).
				recorder.Frame(f)
			}
			ev, err := dec.Decode(f)
			if err != nil {
				cnt.decodeErrs.Add(1)
				logger.LogAttrs(ctx, slog.LevelWarn, "decode", slog.String("err", err.Error()))
				continue
			}
			// The book stage first: it is upstream of the subscribers, and it
			// is the one that may block. Publishing first would show a
			// subscriber an event the books have not accepted yet.
			if err := router.Route(ctx, ev); err != nil {
				cnt.routeErrs.Add(1)
				return err
			}
			pub.Publish(ev)

		case <-ticker.C:
			logSummary(ctx, logger, router, pub, recorder, &cnt)

		case err := <-errc:
			return err
		}
	}
}

// logSummary writes one line describing the run so far. M6 replaces the
// counters here with the histograms that give the same picture with
// percentiles rather than totals.
func logSummary(ctx context.Context, logger *slog.Logger, router *pipeline.Router, pub *pipeline.Publisher, rec *record.Recorder, cnt *counters) {
	st := router.Stats()
	logger.LogAttrs(ctx, slog.LevelInfo, "summary",
		slog.Uint64("frames", cnt.frames.Load()),
		slog.Uint64("decode_errors", cnt.decodeErrs.Load()),
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
			slog.Uint64("run", cnt.checks.Load()),
			slog.Uint64("diverged", cnt.divergences.Load())),
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
