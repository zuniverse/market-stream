package binance_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
)

func TestCombinedStreamURL(t *testing.T) {
	cache := loadCache(t)

	got, err := binance.CombinedStreamURL("wss://stream.binance.com:9443", cache,
		[]model.Symbol{"BTC-USDT", "USDT-TRY"})
	if err != nil {
		t.Fatalf("CombinedStreamURL: %v", err)
	}
	want := "wss://stream.binance.com:9443/stream?streams=" +
		"btcusdt@aggTrade/btcusdt@depth@100ms/usdttry@aggTrade/usdttry@depth@100ms"
	if got != want {
		t.Errorf("URL:\n got %s\nwant %s", got, want)
	}
	// The separators must reach the endpoint as written. An escaped "/" or
	// "@" is not a form Binance documents accepting.
	if strings.ContainsAny(got, "%") {
		t.Errorf("URL contains an escape: %s", got)
	}

	if _, err := binance.CombinedStreamURL("wss://x", cache, nil); err == nil {
		t.Error("CombinedStreamURL accepted an empty symbol list")
	}
	if _, err := binance.CombinedStreamURL("wss://x", cache, []model.Symbol{"NOPE-USDT"}); err == nil {
		t.Error("CombinedStreamURL accepted an unknown symbol")
	}

	// A trailing slash on the endpoint must not change the result.
	withSlash, err := binance.CombinedStreamURL("wss://stream.binance.com:9443/", cache, []model.Symbol{"BTC-USDT"})
	if err != nil {
		t.Fatalf("CombinedStreamURL: %v", err)
	}
	withoutSlash, err := binance.CombinedStreamURL("wss://stream.binance.com:9443", cache, []model.Symbol{"BTC-USDT"})
	if err != nil {
		t.Fatalf("CombinedStreamURL: %v", err)
	}
	if withSlash != withoutSlash {
		t.Errorf("trailing slash changed the URL:\n got %s\nwant %s", withSlash, withoutSlash)
	}
}

// TestDecodeCombinedStreamEnvelope feeds every captured delta twice, once as
// it arrived on a single-stream connection and once wrapped the way the
// combined endpoint wraps it, and requires the two to decode identically.
//
// The payloads are the captured bytes. The wrapper is written here from the
// documented shape rather than captured, which is the honest position: the
// part that could be wrong about the wire is the payload, and that part is
// evidence.
func TestDecodeCombinedStreamEnvelope(t *testing.T) {
	dec := binance.NewDecoder(loadCache(t))
	frames := capturedFrames(t)
	if len(frames) == 0 {
		t.Fatal("no captured frames")
	}

	for i, payload := range frames {
		plain, err := dec.Decode(binance.Frame{Data: payload})
		if err != nil {
			t.Fatalf("frame %d: decode unwrapped: %v", i, err)
		}
		wrapped := []byte(fmt.Sprintf(`{"stream":"btcusdt@depth@100ms","data":%s}`, payload))
		got, err := dec.Decode(binance.Frame{Data: wrapped})
		if err != nil {
			t.Fatalf("frame %d: decode wrapped: %v", i, err)
		}
		if got.Kind != plain.Kind ||
			got.BookDelta.Symbol != plain.BookDelta.Symbol ||
			got.BookDelta.FirstID != plain.BookDelta.FirstID ||
			got.BookDelta.LastID != plain.BookDelta.LastID ||
			len(got.BookDelta.Bids) != len(plain.BookDelta.Bids) ||
			len(got.BookDelta.Asks) != len(plain.BookDelta.Asks) {
			t.Fatalf("frame %d: wrapped decoded differently:\n got %+v\nwant %+v", i, got.BookDelta, plain.BookDelta)
		}
	}
}

// TestDecodeCombinedStreamErrors covers the two ways a wrapped frame can be
// wrong without the wrapper itself being malformed.
func TestDecodeCombinedStreamErrors(t *testing.T) {
	dec := binance.NewDecoder(loadCache(t))

	for _, tc := range []struct {
		name string
		data string
	}{
		{"payload is not an object", `{"stream":"btcusdt@depth","data":"nope"}`},
		{"payload has no event type", `{"stream":"btcusdt@depth","data":{"s":"BTCUSDT"}}`},
		{"payload is wrapped twice", `{"stream":"a","data":{"stream":"b","data":{"e":"aggTrade","s":"BTCUSDT","p":"1.00000000","q":"1.00000000"}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := dec.Decode(binance.Frame{Data: []byte(tc.data)}); err == nil {
				t.Errorf("Decode accepted %s", tc.data)
			}
		})
	}
}
