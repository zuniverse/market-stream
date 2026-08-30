package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
)

// fakeInstruments is a fixed instrument table. The real implementation is
// *binance.InstrumentCache, which this package must not import.
type fakeInstruments map[model.Symbol]model.Instrument

func (f fakeInstruments) LookupByNormalized(sym model.Symbol) (model.Instrument, bool) {
	inst, ok := f[sym]
	return inst, ok
}

func newLogBuffer() (*bytes.Buffer, *slog.Logger) {
	var buf bytes.Buffer
	return &buf, slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// decodeLine parses the single JSON log record written to buf. UseNumber keeps
// numeric fields as json.Number rather than float64, which this project does
// not use anywhere, tests included.
func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	dec.UseNumber()
	var rec map[string]any
	if err := dec.Decode(&rec); err != nil {
		t.Fatalf("log output is not one JSON object: %v (%q)", err, buf.String())
	}
	return rec
}

func TestLogSubscriberFormatsTradeWithInstrumentDecimals(t *testing.T) {
	insts := fakeInstruments{
		"BTC-USDT": {Symbol: "BTC-USDT", PriceDecimals: 2, QtyDecimals: 8},
	}
	buf, logger := newLogBuffer()
	sub := pipeline.NewLogSubscriber("test", logger, insts)

	// 5000000 at 2 decimals is 50000.00; 10000000 at 8 decimals is 0.10000000.
	sub.Consume(context.Background(), model.Event{
		Kind: model.KindTrade,
		Trade: model.Trade{
			Symbol: "BTC-USDT",
			Price:  model.Price(5_000_000),
			Qty:    model.Qty(10_000_000),
			IsBuy:  true,
		},
	})

	rec := decodeLine(t, buf)
	want := map[string]any{
		"msg":    "trade",
		"symbol": "BTC-USDT",
		"price":  "50000.00",
		"qty":    "0.10000000",
		"side":   "buy",
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("field %q = %v, want %v", k, rec[k], v)
		}
	}
}

func TestLogSubscriberSellSide(t *testing.T) {
	insts := fakeInstruments{"BTC-USDT": {Symbol: "BTC-USDT", PriceDecimals: 2, QtyDecimals: 8}}
	buf, logger := newLogBuffer()
	sub := pipeline.NewLogSubscriber("test", logger, insts)

	sub.Consume(context.Background(), model.Event{
		Kind:  model.KindTrade,
		Trade: model.Trade{Symbol: "BTC-USDT", Price: 1, Qty: 1, IsBuy: false},
	})

	if got := decodeLine(t, buf)["side"]; got != "sell" {
		t.Errorf("side = %v, want sell", got)
	}
}

// TestLogSubscriberUnknownSymbolStillLogs pins the degradation policy: a
// metadata miss must not silently discard the observation.
func TestLogSubscriberUnknownSymbolStillLogs(t *testing.T) {
	buf, logger := newLogBuffer()
	sub := pipeline.NewLogSubscriber("test", logger, fakeInstruments{})

	sub.Consume(context.Background(), model.Event{
		Kind:  model.KindTrade,
		Trade: model.Trade{Symbol: "XYZ-USDT", Price: model.Price(12345), Qty: model.Qty(7)},
	})

	rec := decodeLine(t, buf)
	if rec["msg"] != "trade" {
		t.Fatalf("expected a trade record, got %v", rec)
	}
	// No instrument means no exponent, so the raw fixed-point integer is shown.
	if rec["price"] != "12345" {
		t.Errorf("price = %v, want unscaled 12345", rec["price"])
	}
}

func TestLogSubscriberBookDelta(t *testing.T) {
	buf, logger := newLogBuffer()
	sub := pipeline.NewLogSubscriber("test", logger, fakeInstruments{})

	sub.Consume(context.Background(), model.Event{
		Kind: model.KindBookDelta,
		BookDelta: model.BookDelta{
			Symbol:  "BTC-USDT",
			FirstID: 10,
			LastID:  12,
			Bids:    []model.Level{{Price: 1, Qty: 1}, {Price: 2, Qty: 2}},
			Asks:    []model.Level{{Price: 3, Qty: 3}},
		},
	})

	rec := decodeLine(t, buf)
	want := map[string]any{
		"msg":      "book_delta",
		"symbol":   "BTC-USDT",
		"first_id": json.Number("10"),
		"last_id":  json.Number("12"),
		"bids":     json.Number("2"),
		"asks":     json.Number("1"),
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("field %q = %v, want %v", k, rec[k], v)
		}
	}
}

func TestLogSubscriberUnknownKindWarns(t *testing.T) {
	buf, logger := newLogBuffer()
	sub := pipeline.NewLogSubscriber("test", logger, fakeInstruments{})

	sub.Consume(context.Background(), model.Event{Kind: model.EventKind(99)})

	rec := decodeLine(t, buf)
	if rec["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", rec["level"])
	}
}

// TestLogSubscriberSatisfiesSubscriber is a compile-time check that the
// concrete type still fits the interface the Publisher registers.
func TestLogSubscriberSatisfiesSubscriber(t *testing.T) {
	var _ pipeline.Subscriber = pipeline.NewLogSubscriber("x", slog.Default(), fakeInstruments{})
}
