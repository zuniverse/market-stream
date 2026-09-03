package binance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseExchangeInfo(t *testing.T) {
	cache, err := binance.ParseExchangeInfo(fixture(t, "exchangeinfo.json"))
	if err != nil {
		t.Fatalf("ParseExchangeInfo: %v", err)
	}

	tests := []struct {
		rawSymbol string
		want      model.Instrument
	}{
		{
			"BTCUSDT",
			model.Instrument{Symbol: "BTC-USDT", PriceDecimals: 2, QtyDecimals: 5},
		},
		{
			"USDTTRY",
			model.Instrument{Symbol: "USDT-TRY", PriceDecimals: 3, QtyDecimals: 0},
		},
	}

	for _, tc := range tests {
		inst, ok := cache.Lookup(tc.rawSymbol)
		if !ok {
			t.Errorf("Lookup(%q): not found", tc.rawSymbol)
			continue
		}
		if inst != tc.want {
			t.Errorf("Lookup(%q) = %+v, want %+v", tc.rawSymbol, inst, tc.want)
		}
	}
}

func TestParseExchangeInfo_SkipsNonTrading(t *testing.T) {
	cache, err := binance.ParseExchangeInfo(fixture(t, "exchangeinfo.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Lookup("BNBBTC"); ok {
		t.Error("BNBBTC (status BREAK) should not be in the cache")
	}
}

func TestParseExchangeInfo_InvalidJSON(t *testing.T) {
	_, err := binance.ParseExchangeInfo([]byte(`not json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseExchangeInfo_MissingFilter(t *testing.T) {
	data := []byte(`{"symbols":[{
		"symbol":"ETHUSDT","status":"TRADING",
		"baseAsset":"ETH","quoteAsset":"USDT",
		"filters":[{"filterType":"LOT_SIZE","stepSize":"0.00010000"}]
	}]}`)
	_, err := binance.ParseExchangeInfo(data)
	if err == nil {
		t.Error("expected error when PRICE_FILTER is absent")
	}
}

// TestFetchExchangeInfoFiltersSymbols pins the request the client makes. The
// unfiltered response on Binance spot is over seventeen megabytes, which is
// past the recorder's per-record limit and goes into every hourly file, so
// asking for only the instruments being ingested is not an optimisation.
func TestFetchExchangeInfoFiltersSymbols(t *testing.T) {
	body := fixture(t, "exchangeinfo.json")

	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/exchangeInfo" {
			t.Errorf("path = %q", r.URL.Path)
		}
		gotQuery = r.URL.Query().Get("symbols")
		w.Write(body)
	}))
	defer srv.Close()

	ctx := context.Background()
	cache, err := binance.FetchExchangeInfo(ctx, srv.Client(), srv.URL,
		[]model.Symbol{"BTC-USDT", "USDT-TRY"})
	if err != nil {
		t.Fatalf("FetchExchangeInfo: %v", err)
	}
	if want := `["BTCUSDT","USDTTRY"]`; gotQuery != want {
		t.Errorf("symbols = %q, want %q", gotQuery, want)
	}
	if _, ok := cache.LookupByNormalized("BTC-USDT"); !ok {
		t.Error("the parsed cache does not hold BTC-USDT")
	}

	// No symbols means no parameter, which is the whole venue.
	if _, err := binance.FetchExchangeInfo(ctx, srv.Client(), srv.URL, nil); err != nil {
		t.Fatalf("FetchExchangeInfo: %v", err)
	}
	if gotQuery != "" {
		t.Errorf("symbols = %q on an unfiltered request, want it absent", gotQuery)
	}
}
