// Command replay runs a recording through the same pipeline the live daemon
// uses, and writes the resulting book state.
//
// The point is reproducibility: two runs of one recording must produce the
// same bytes. That is what makes a before-and-after comparison of an
// optimisation meaningful, and it is why the recorder was built before any
// profiling work rather than after it (D5).
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
	"github.com/zuniverse/market-stream/internal/record"
)

const (
	frameCap      = 4096
	shardQueueCap = 4096

	// settleTimeout bounds the wait for the book stage to go quiet after the
	// last frame. A book that never anchors, because the recording ran out of
	// snapshots for it, would otherwise hold the run open forever.
	settleTimeout = 30 * time.Second
)

type config struct {
	files  []string
	out    string
	speed  float64
	shards int
	depth  int
	quiet  bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.out, "out", "-", "write the book dump here; - is stdout")
	flag.Float64Var(&cfg.speed, "speed", 0, "replay speed multiplier; 0 replays as fast as possible")
	flag.IntVar(&cfg.shards, "shards", runtime.NumCPU(), "number of book shard goroutines")
	flag.IntVar(&cfg.depth, "depth", 0, "levels per side in the dump; 0 dumps the whole book")
	flag.BoolVar(&cfg.quiet, "quiet", false, "suppress the progress log")
	flag.Parse()
	cfg.files = flag.Args()

	if len(cfg.files) == 0 {
		fmt.Fprintln(os.Stderr, "usage: replay [flags] file.msr.zst [file.msr.zst ...]")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, os.Stderr); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, cfg config, logTo io.Writer) error {
	level := slog.LevelInfo
	if cfg.quiet {
		level = slog.LevelError
	}
	logger := slog.New(slog.NewTextHandler(logTo, &slog.HandlerOptions{Level: level}))

	frames := make(chan model.Frame, frameCap)
	src, err := record.NewReplaySource(cfg.files, frames, cfg.speed)
	if err != nil {
		return err
	}

	// The metadata comes out of the recording, not off the network. A replay
	// that needed the venue to still be listing a symbol would not be an
	// archive (D35).
	cache, err := binance.ParseExchangeInfo(src.Meta())
	if err != nil {
		return fmt.Errorf("replay: instrument metadata: %w", err)
	}
	dec := binance.NewDecoder(cache)

	router, err := pipeline.NewRouter(pipeline.RouterConfig{
		Shards:    cfg.shards,
		QueueCap:  shardQueueCap,
		Snapshots: src.Snapshots(),
		Log:       logger,
	})
	if err != nil {
		return err
	}
	if err := router.Start(ctx); err != nil {
		return err
	}
	defer router.Close()

	logger.LogAttrs(ctx, slog.LevelInfo, "replaying",
		slog.Int("files", len(cfg.files)),
		slog.Int("shards", router.Shards()),
		slog.Int("snapshots", src.Snapshots().Len()),
		slog.Uint64("recorded_drops", src.Drops()))

	// Owner: run. Exit: the recording is exhausted or ctx is cancelled.
	errc := make(chan error, 1)
	go func() { errc <- src.Run(ctx) }()

	symbols := make(map[model.Symbol]struct{})
	var decodeErrs, routed uint64

	// Route until the source stops, then drain whatever it left queued. The
	// drain is what makes the run deterministic: every frame the recording
	// held has reached a shard before anything is read back.
	srcErr := func() error {
		for {
			select {
			case f := <-frames:
				if err := handle(ctx, dec, router, f, symbols, &decodeErrs, &routed); err != nil {
					return err
				}
			case err := <-errc:
				for {
					select {
					case f := <-frames:
						if err := handle(ctx, dec, router, f, symbols, &decodeErrs, &routed); err != nil {
							return err
						}
					default:
						return err
					}
				}
			}
		}
	}()
	if srcErr != nil {
		return srcErr
	}

	settled := waitSettled(ctx, router, symbols)
	st := router.Stats()
	logger.LogAttrs(ctx, slog.LevelInfo, "replayed",
		slog.Uint64("frames", src.Frames()),
		slog.Uint64("decode_errors", decodeErrs),
		slog.Uint64("routed", routed),
		slog.Uint64("applied", st.Applied),
		slog.Uint64("gaps", st.Gaps),
		slog.Uint64("snapshots_used", src.Snapshots().Used()),
		slog.Bool("settled", settled))

	views, err := dumpViews(ctx, router, symbols, cfg.depth)
	if err != nil {
		return err
	}
	return writeDump(cfg.out, views)
}

// handle decodes one frame and hands it to the book stage.
func handle(
	ctx context.Context,
	dec *binance.Decoder,
	router *pipeline.Router,
	f model.Frame,
	symbols map[model.Symbol]struct{},
	decodeErrs, routed *uint64,
) error {
	ev, err := dec.Decode(f)
	if err != nil {
		*decodeErrs++
		return nil
	}
	if err := router.Route(ctx, ev); err != nil {
		return err
	}
	*routed++
	switch ev.Kind {
	case model.KindBookDelta:
		symbols[ev.BookDelta.Symbol] = struct{}{}
	case model.KindSnapshot:
		symbols[ev.Snapshot.Symbol] = struct{}{}
	}
	return nil
}

// waitSettled blocks until the book stage has nothing left to do: every queue
// empty, every book anchored, and no counter moving.
//
// A book that is live has no snapshot request outstanding, since a request is
// only started for a book that needs one, so the three conditions together
// mean the stage is idle and its state is final. It reports whether it got
// there before the timeout.
func waitSettled(ctx context.Context, router *pipeline.Router, symbols map[model.Symbol]struct{}) bool {
	deadline := time.Now().Add(settleTimeout)
	var last pipeline.Stats
	for time.Now().Before(deadline) {
		st := router.Stats()
		if st == last && queuesEmpty(router) && allLive(ctx, router, symbols) {
			return true
		}
		last = st
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return false
		}
	}
	return false
}

func queuesEmpty(router *pipeline.Router) bool {
	for _, n := range router.QueueDepth() {
		if n != 0 {
			return false
		}
	}
	return true
}

func allLive(ctx context.Context, router *pipeline.Router, symbols map[model.Symbol]struct{}) bool {
	for sym := range symbols {
		view, err := router.Query(ctx, sym, 0)
		if err != nil || !view.Live {
			return false
		}
	}
	return true
}

// dumpViews reads every book, in symbol order.
func dumpViews(ctx context.Context, router *pipeline.Router, symbols map[model.Symbol]struct{}, depth int) ([]pipeline.BookView, error) {
	sorted := make([]model.Symbol, 0, len(symbols))
	for sym := range symbols {
		sorted = append(sorted, sym)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	out := make([]pipeline.BookView, 0, len(sorted))
	for _, sym := range sorted {
		want := depth
		if want <= 0 {
			want = int(^uint(0) >> 1) // the whole book
		}
		view, err := router.Query(ctx, sym, want)
		if err != nil {
			return nil, fmt.Errorf("replay: read %s: %w", sym, err)
		}
		out = append(out, view)
	}
	return out, nil
}

// writeDump renders the book state in a form two runs can be compared with
// byte for byte. Nothing about the machine, the clock or the shard count
// appears in it: a book is its symbol, its position in the stream, and its
// levels.
func writeDump(path string, views []pipeline.BookView) error {
	var w io.Writer = os.Stdout
	if path != "-" {
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("replay: %w", err)
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriter(w)

	fmt.Fprintf(bw, "market-stream replay dump v1\n")
	for _, v := range views {
		fmt.Fprintf(bw, "symbol %s live=%t last_id=%d crossed=%t bids=%d asks=%d\n",
			v.Symbol, v.Live, v.LastID, v.Crossed, v.BidDepth, v.AskDepth)
		for _, l := range v.Bids {
			fmt.Fprintf(bw, "B %d %d\n", l.Price, l.Qty)
		}
		for _, l := range v.Asks {
			fmt.Fprintf(bw, "A %d %d\n", l.Price, l.Qty)
		}
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("replay: write dump: %w", err)
	}
	return nil
}
