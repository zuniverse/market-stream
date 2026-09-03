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
	inst *instruments,
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
			checkOne(ctx, logger, router, depth, sym, inst)
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
	inst *instruments,
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

	inst.checks.Add(1)
	if diff.OK() {
		logger.LogAttrs(ctx, slog.LevelInfo, "check ok",
			slog.String("symbol", string(sym)),
			slog.Int("compared", diff.Compared),
			slog.Int64("id_skew", diff.IDSkew()))
		return
	}

	// The two views describe the same book at two moments. When the skew is
	// not zero the snapshot and the stream are a few updates apart, so a
	// handful of levels differing is arithmetic rather than evidence: it is
	// precisely what D28 says to expect, and reporting it as a fault teaches
	// an operator to ignore the one report that matters.
	//
	// The check that proves something is the one at a skew of zero. Those are
	// counted separately, and any divergence in one is a defect: the same
	// book, at the same update id, disagreeing with the venue.
	if diff.IDSkew() != 0 {
		inst.skewed.Add(1)
		logger.LogAttrs(ctx, slog.LevelInfo, "check skewed",
			slog.String("symbol", string(sym)),
			slog.Int("levels", len(diff.Levels)),
			slog.Int("compared", diff.Compared),
			slog.Int64("id_skew", diff.IDSkew()),
			slog.String("first", diff.Levels[0].String()))
		return
	}

	inst.divergences.Add(1)
	logger.LogAttrs(ctx, slog.LevelWarn, "check diverged",
		slog.String("symbol", string(sym)),
		slog.Int("levels", len(diff.Levels)),
		slog.Int("compared", diff.Compared),
		slog.Int64("id_skew", 0),
		slog.String("first", diff.Levels[0].String()))
}
