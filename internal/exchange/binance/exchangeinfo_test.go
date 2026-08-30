package binance_test

import (
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
