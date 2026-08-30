package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/pipeline"
)

const (
	wsBase      = "wss://stream.binance.com:9443/ws"
	restBaseURL = "https://api.binance.com"
)

// config holds the parsed flag values.
//
// These names are provisional and do not yet match the surface committed in
// D19 (-symbols, -endpoint, -shards, -metrics-addr). Reconciling them is a
// separate change, since D19 treats a flag name as a public commitment.
type config struct {
	stream   string
	interval time.Duration
	chanCap  int
	subCap   int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.stream, "stream", "btcusdt@aggTrade", "Binance stream name")
	flag.DurationVar(&cfg.interval, "interval", 5*time.Second, "reporting interval")
	flag.IntVar(&cfg.chanCap, "chan-cap", 1024, "raw frame channel capacity")
	flag.IntVar(&cfg.subCap, "sub-cap", 1024, "per-subscriber channel capacity")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// run owns the process lifetime. It returns context.Canceled on a clean
// shutdown, which main treats as success.
func run(ctx context.Context, cfg config) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Fetched once at startup. The cache serves both symbol normalisation in
	// the decoder and decimal exponents in the log subscriber (D16).
	httpClient := &http.Client{Timeout: 10 * time.Second}
	cache, err := binance.FetchExchangeInfo(ctx, httpClient, restBaseURL)
	if err != nil {
		return err
	}
	dec := binance.NewDecoder(cache)

	// Every normalised event leaves this process through the publisher. The
	// binary must not write pipeline events directly, or the fan-out seam goes
	// untested until the milestone that makes changing it expensive (D17).
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(pipeline.NewLogSubscriber("stdout-log", logger, cache), cfg.subCap); err != nil {
		return err
	}
	if err := pub.Start(ctx); err != nil {
		return err
	}
	defer pub.Close()

	frames := make(chan binance.Frame, cfg.chanCap)
	tr := binance.NewTransport(wsBase+"/"+cfg.stream, frames)

	// Owner: run. Exit: ctx cancelled, which is the only way Transport.Run
	// returns. The buffer of 1 keeps the send from blocking after run returns.
	errc := make(chan error, 1)
	go func() { errc <- tr.Run(ctx) }()

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	var window, total, decodeErrs int64
	for {
		select {
		case f := <-frames:
			window++
			total++
			ev, err := dec.Decode(f)
			if err != nil {
				decodeErrs++
				logger.LogAttrs(ctx, slog.LevelWarn, "decode", slog.String("err", err.Error()))
				continue
			}
			pub.Publish(ev)

		case <-ticker.C:
			logger.LogAttrs(ctx, slog.LevelInfo, "summary",
				slog.Int64("frames", window),
				slog.Int64("total", total),
				slog.Int64("decode_errors", decodeErrs),
				droppedAttr(pub),
			)
			window = 0

		case err := <-errc:
			return err
		}
	}
}

// droppedAttr renders the publisher's per-subscriber drop counters as a log
// group. M6 replaces this with a Prometheus counter.
func droppedAttr(pub *pipeline.Publisher) slog.Attr {
	counts := pub.Dropped()
	attrs := make([]any, 0, len(counts))
	for name, n := range counts {
		attrs = append(attrs, slog.Uint64(name, n))
	}
	return slog.Group("dropped", attrs...)
}
