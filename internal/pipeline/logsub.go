package pipeline

import (
	"context"
	"log/slog"

	"github.com/zuniverse/market-stream/internal/model"
)

// Instruments resolves a normalised symbol to its instrument metadata, which
// carries the decimal exponents needed to render a fixed-point value. Under
// D14 the exponent is not stored in the value itself, so anything that formats
// a Price or a Qty needs this lookup.
//
// The interface is declared here, on the consumer side, so that a subscriber
// never imports an exchange package. *binance.InstrumentCache satisfies it
// without knowing this package exists.
type Instruments interface {
	LookupByNormalized(model.Symbol) (model.Instrument, bool)
}

// LogSubscriber writes each event it receives to a structured logger.
//
// It is the first subscriber, and it exists so that the publisher fan-out seam
// is exercised by the first working binary rather than bypassed (D17).
type LogSubscriber struct {
	name  string
	log   *slog.Logger
	insts Instruments
}

// NewLogSubscriber returns a LogSubscriber that logs to log and resolves
// decimal exponents through insts.
func NewLogSubscriber(name string, log *slog.Logger, insts Instruments) *LogSubscriber {
	return &LogSubscriber{name: name, log: log, insts: insts}
}

// Name implements Subscriber.
func (s *LogSubscriber) Name() string { return s.name }

// Consume implements Subscriber. It logs one line per event, at Info level for
// known kinds and at Warn for an unrecognised one.
func (s *LogSubscriber) Consume(ctx context.Context, ev model.Event) {
	switch ev.Kind {
	case model.KindTrade:
		t := ev.Trade
		priceDec, qtyDec := s.decimals(t.Symbol)
		side := "sell"
		if t.IsBuy {
			side = "buy"
		}
		s.log.LogAttrs(ctx, slog.LevelInfo, "trade",
			slog.String("symbol", string(t.Symbol)),
			slog.String("price", model.FormatFixed(int64(t.Price), priceDec)),
			slog.String("qty", model.FormatFixed(int64(t.Qty), qtyDec)),
			slog.String("side", side),
		)

	case model.KindBookDelta:
		d := ev.BookDelta
		s.log.LogAttrs(ctx, slog.LevelInfo, "book_delta",
			slog.String("symbol", string(d.Symbol)),
			slog.Int64("first_id", d.FirstID),
			slog.Int64("last_id", d.LastID),
			slog.Int("bids", len(d.Bids)),
			slog.Int("asks", len(d.Asks)),
		)

	case model.KindSnapshot:
		snap := ev.Snapshot
		s.log.LogAttrs(ctx, slog.LevelInfo, "snapshot",
			slog.String("symbol", string(snap.Symbol)),
			slog.Int64("last_id", snap.LastID),
			slog.Int("bids", len(snap.Bids)),
			slog.Int("asks", len(snap.Asks)),
		)

	default:
		s.log.LogAttrs(ctx, slog.LevelWarn, "unknown event kind",
			slog.Int("kind", int(ev.Kind)),
		)
	}
}

// decimals returns the price and quantity exponents for sym. An unknown symbol
// yields zeroes, so the line is logged unscaled rather than dropped: losing an
// observation to a metadata miss is worse than logging a raw integer.
func (s *LogSubscriber) decimals(sym model.Symbol) (priceDec, qtyDec int) {
	inst, ok := s.insts.LookupByNormalized(sym)
	if !ok {
		return 0, 0
	}
	return inst.PriceDecimals, inst.QtyDecimals
}
