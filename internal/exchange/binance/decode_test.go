package binance_test

import (
	"testing"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
)

func loadCache(t *testing.T) *binance.InstrumentCache {
	t.Helper()
	cache, err := binance.ParseExchangeInfo(fixture(t, "exchangeinfo.json"))
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestDecode(t *testing.T) {
	dec := binance.NewDecoder(loadCache(t))

	tests := []struct {
		name  string
		file  string
		kind  model.EventKind
		check func(*testing.T, model.Event)
	}{
		{
			name: "aggTrade",
			file: "agg_trade.json",
			kind: model.KindTrade,
			check: func(t *testing.T, ev model.Event) {
				// "50000.01" with priceDecimals=2 -> 5000001
				// "0.12345"  with qtyDecimals=5   -> 12345
				// m=false -> IsBuy=true
				if ev.Trade.Symbol != "BTC-USDT" {
					t.Errorf("Symbol = %q, want BTC-USDT", ev.Trade.Symbol)
				}
				if ev.Trade.Price != model.Price(5000001) {
					t.Errorf("Price = %d, want 5000001", ev.Trade.Price)
				}
				if ev.Trade.Qty != model.Qty(12345) {
					t.Errorf("Qty = %d, want 12345", ev.Trade.Qty)
				}
				if !ev.Trade.IsBuy {
					t.Error("IsBuy = false, want true")
				}
			},
		},
		{
			name: "depthUpdate",
			file: "depth_update.json",
			kind: model.KindBookDelta,
			check: func(t *testing.T, ev model.Event) {
				d := ev.BookDelta
				if d.Symbol != "BTC-USDT" {
					t.Errorf("Symbol = %q, want BTC-USDT", d.Symbol)
				}
				if d.FirstID != 1000 || d.LastID != 1005 {
					t.Errorf("IDs = %d/%d, want 1000/1005", d.FirstID, d.LastID)
				}
				if len(d.Bids) != 2 || len(d.Asks) != 2 {
					t.Fatalf("bids=%d asks=%d, want 2/2", len(d.Bids), len(d.Asks))
				}
				// "50000.00" -> 5000000, "1.23456" -> 123456
				if d.Bids[0].Price != model.Price(5000000) || d.Bids[0].Qty != model.Qty(123456) {
					t.Errorf("Bids[0] = %+v, want {5000000 123456}", d.Bids[0])
				}
				// "49999.99" -> 4999999, "0.00000" -> 0
				if d.Bids[1].Price != model.Price(4999999) || d.Bids[1].Qty != model.Qty(0) {
					t.Errorf("Bids[1] = %+v, want {4999999 0}", d.Bids[1])
				}
				// "50000.01" -> 5000001, "2.34567" -> 234567
				if d.Asks[0].Price != model.Price(5000001) || d.Asks[0].Qty != model.Qty(234567) {
					t.Errorf("Asks[0] = %+v, want {5000001 234567}", d.Asks[0])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := dec.Decode(binance.Frame{Data: fixture(t, tc.file)})
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if ev.Kind != tc.kind {
				t.Fatalf("Kind = %v, want %v", ev.Kind, tc.kind)
			}
			tc.check(t, ev)
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	dec := binance.NewDecoder(loadCache(t))

	tests := []struct {
		name string
		data []byte
	}{
		{"unknown type", []byte(`{"e":"ticker","s":"BTCUSDT"}`)},
		{"unknown symbol", []byte(`{"e":"aggTrade","s":"XYZABC","p":"1.00","q":"1.00","m":false}`)},
		{"invalid json", []byte(`not json`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dec.Decode(binance.Frame{Data: tc.data})
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}
