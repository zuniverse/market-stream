package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
)

// runChecker is the correctness harness. Every interval it fetches a fresh
// snapshot for each symbol and asks the shard that owns the book to compare
// the two, reporting any divergence.
//
// This is the only thing in the process that can tell a correct book from a
// plausible one. A book maintained from deltas for six hours does not crash
// when it is wrong, it lies, and nothing else here would notice.
//
// The cost is deliberate and bounded: one full-depth request per symbol per
// interval, which on Binance spot weighs 250 against a per-IP budget of 6000
// per minute (D25). At the default of five minutes and a handful of symbols
// that is a rounding error, and -check-interval 0 turns it off entirely.
//
// Owner: run. Exit: ctx cancelled.
func runChecker(
	ctx context.Context,
	logger *slog.Logger,
	router *pipeline.Router,
	depth *binance.DepthClient,
	symbols []model.Symbol,
	every time.Duration,
	cnt *counters,
) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, sym := range symbols {
			if ctx.Err() != nil {
				return
			}
			checkOne(ctx, logger, router, depth, sym, cnt)
		}
	}
}

// checkOne fetches a snapshot for sym and compares it against the book.
func checkOne(
	ctx context.Context,
	logger *slog.Logger,
	router *pipeline.Router,
	depth *binance.DepthClient,
	sym model.Symbol,
	cnt *counters,
) {
	snap, err := depth.Snapshot(ctx, sym)
	if err != nil {
		if ctx.Err() == nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "check fetch",
				slog.String("symbol", string(sym)), slog.String("err", err.Error()))
		}
		return
	}
	diff, err := router.Check(ctx, snap)
	if err != nil {
		if ctx.Err() == nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "check",
				slog.String("symbol", string(sym)), slog.String("err", err.Error()))
		}
		return
	}
	if !diff.Live {
		// A book waiting on a resync is missing updates by definition.
		// Comparing it would report the whole book as divergent and say
		// nothing that Live does not already say.
		logger.LogAttrs(ctx, slog.LevelInfo, "check skipped",
			slog.String("symbol", string(sym)), slog.String("reason", "book not live"))
		return
	}

	cnt.checks.Add(1)
	if diff.OK() {
		logger.LogAttrs(ctx, slog.LevelInfo, "check ok",
			slog.String("symbol", string(sym)),
			slog.Int("compared", diff.Compared),
			slog.Int64("id_skew", diff.IDSkew()))
		return
	}

	// A divergence at a small id skew, on levels near the touch, is the
	// snapshot and the stream describing the same book a few updates apart.
	// One at a skew of zero, or deep in the book, is a real defect. Both are
	// reported with the skew attached so the reader can tell them apart, and
	// neither is smoothed over here (D28).
	cnt.divergences.Add(1)
	logger.LogAttrs(ctx, slog.LevelWarn, "check diverged",
		slog.String("symbol", string(sym)),
		slog.Int("levels", len(diff.Levels)),
		slog.Int("compared", diff.Compared),
		slog.Int64("id_skew", diff.IDSkew()),
		slog.String("first", diff.Levels[0].String()))
}
