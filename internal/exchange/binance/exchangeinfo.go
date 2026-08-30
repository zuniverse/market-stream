package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/zuniverse/market-stream/internal/model"
)

// InstrumentCache maps raw Binance symbol strings to normalised Instrument values.
type InstrumentCache struct {
	m map[string]model.Instrument
}

// Lookup returns the Instrument for the given raw Binance symbol (e.g. "BTCUSDT").
func (c *InstrumentCache) Lookup(rawSymbol string) (model.Instrument, bool) {
	inst, ok := c.m[rawSymbol]
	return inst, ok
}

// ParseExchangeInfo parses the JSON body of a Binance /api/v3/exchangeInfo
// response and returns a cache of normalised instrument metadata. Symbols
// with status other than "TRADING" are silently skipped.
func ParseExchangeInfo(data []byte) (*InstrumentCache, error) {
	var resp struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Filters    []struct {
				FilterType string `json:"filterType"`
				TickSize   string `json:"tickSize"`
				StepSize   string `json:"stepSize"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("binance: parse exchangeInfo: %w", err)
	}

	cache := &InstrumentCache{m: make(map[string]model.Instrument, len(resp.Symbols))}

	for _, sym := range resp.Symbols {
		if sym.Status != "TRADING" {
			continue
		}

		var tickSize, stepSize string
		for _, f := range sym.Filters {
			switch f.FilterType {
			case "PRICE_FILTER":
				tickSize = f.TickSize
			case "LOT_SIZE":
				stepSize = f.StepSize
			}
		}
		if tickSize == "" {
			return nil, fmt.Errorf("binance: symbol %s: missing PRICE_FILTER tickSize", sym.Symbol)
		}
		if stepSize == "" {
			return nil, fmt.Errorf("binance: symbol %s: missing LOT_SIZE stepSize", sym.Symbol)
		}

		priceDec, err := sizeDecimals(tickSize)
		if err != nil {
			return nil, fmt.Errorf("binance: symbol %s tickSize %q: %w", sym.Symbol, tickSize, err)
		}
		qtyDec, err := sizeDecimals(stepSize)
		if err != nil {
			return nil, fmt.Errorf("binance: symbol %s stepSize %q: %w", sym.Symbol, stepSize, err)
		}

		cache.m[sym.Symbol] = model.Instrument{
			Symbol:        model.Symbol(sym.BaseAsset + "-" + sym.QuoteAsset),
			PriceDecimals: priceDec,
			QtyDecimals:   qtyDec,
		}
	}

	return cache, nil
}

// FetchExchangeInfo fetches /api/v3/exchangeInfo from baseURL and returns a
// parsed InstrumentCache. baseURL should not include a trailing slash.
func FetchExchangeInfo(ctx context.Context, client *http.Client, baseURL string) (*InstrumentCache, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v3/exchangeInfo", nil)
	if err != nil {
		return nil, fmt.Errorf("binance: build exchangeInfo request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance: fetch exchangeInfo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance: exchangeInfo: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("binance: read exchangeInfo body: %w", err)
	}
	return ParseExchangeInfo(data)
}

// sizeDecimals converts a Binance size string such as "0.01000000" to the
// number of significant fractional digits (2 in that example).
func sizeDecimals(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}
	_, frac, hasDot := strings.Cut(s, ".")
	if !hasDot {
		return 0, nil
	}
	return len(strings.TrimRight(frac, "0")), nil
}
