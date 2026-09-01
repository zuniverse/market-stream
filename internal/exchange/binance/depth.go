package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/zuniverse/market-stream/internal/model"
)

// MaxDepthLimit is the deepest snapshot /api/v3/depth will return. Binance
// caps the response there whatever larger limit is asked for, so asking for
// more only wastes request weight.
const MaxDepthLimit = 5000

// maxErrBody bounds how much of an error response is quoted back in the
// returned error. Binance answers a rejected request with a small JSON body
// carrying a code and a message, which is worth surfacing to an operator; a
// proxy or a captive portal in the way may answer with a whole HTML page,
// which is not.
const maxErrBody = 512

// DepthClient fetches order book snapshots from the Binance REST API.
//
// It is the REST half of the depth resync procedure (M2.4): the LastID of a
// returned snapshot is the update id from which buffered deltas are replayed.
// The client is stateless beyond its configuration and is safe for concurrent
// use, since *http.Client is.
type DepthClient struct {
	http    *http.Client
	baseURL string
	cache   *InstrumentCache
	limit   int
}

// NewDepthClient returns a client fetching limit levels per side from baseURL,
// which must not end in a slash. cache must be non-nil; it resolves the
// normalised symbol back to the exchange's own form.
//
// A limit of zero or less selects MaxDepthLimit, and a larger one is clamped
// to it, matching what the endpoint does with the parameter. Depth is not
// free: on Binance spot a request weighs 5 for up to 100 levels and 250 for
// the maximum, against a per-IP budget, so a resync-heavy run at full depth
// is the case worth watching (D25).
func NewDepthClient(hc *http.Client, baseURL string, cache *InstrumentCache, limit int) *DepthClient {
	if limit <= 0 || limit > MaxDepthLimit {
		limit = MaxDepthLimit
	}
	return &DepthClient{http: hc, baseURL: baseURL, cache: cache, limit: limit}
}

// Limit returns the number of levels per side this client requests.
func (c *DepthClient) Limit() int { return c.limit }

// Snapshot fetches the current order book for sym, which is a normalised
// BASE-QUOTE symbol.
//
// The response body carries no symbol of its own, so the returned Snapshot is
// labelled from the request. Prices and quantities are parsed at the
// instrument's decimal exponents, the same ones the websocket decoder uses,
// so a snapshot and a delta for one symbol are directly comparable (D14).
func (c *DepthClient) Snapshot(ctx context.Context, sym model.Symbol) (model.Snapshot, error) {
	raw, inst, ok := c.cache.RawSymbol(sym)
	if !ok {
		return model.Snapshot{}, fmt.Errorf("binance: depth: unknown symbol %q", sym)
	}

	q := url.Values{}
	q.Set("symbol", raw)
	q.Set("limit", strconv.Itoa(c.limit))
	endpoint := c.baseURL + "/api/v3/depth?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: build depth request for %s: %w", sym, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: fetch depth for %s: %w", sym, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
		return model.Snapshot{}, fmt.Errorf("binance: depth for %s: HTTP %d: %s",
			sym, resp.StatusCode, bytes.TrimSpace(body))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: read depth body for %s: %w", sym, err)
	}
	return ParseDepth(data, inst, c.limit)
}

// ParseDepth parses the JSON body of a Binance /api/v3/depth response for the
// given instrument. limit is the number of levels per side that was
// requested; it is what decides whether the result is marked Truncated, since
// the body itself gives no indication.
func ParseDepth(data []byte, inst model.Instrument, limit int) (model.Snapshot, error) {
	var raw struct {
		LastUpdateID int64       `json:"lastUpdateId"`
		Bids         [][2]string `json:"bids"`
		Asks         [][2]string `json:"asks"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: decode depth for %s: %w", inst.Symbol, err)
	}
	// A missing lastUpdateId decodes to zero and would otherwise become the
	// baseline every subsequent delta is sequenced against, turning a
	// malformed response into a book that is quietly wrong rather than a
	// resync that failed.
	if raw.LastUpdateID <= 0 {
		return model.Snapshot{}, fmt.Errorf("binance: depth for %s: lastUpdateId %d is not a valid update id",
			inst.Symbol, raw.LastUpdateID)
	}

	bids, err := parseLevels(raw.Bids, inst.PriceDecimals, inst.QtyDecimals)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: depth for %s: bids: %w", inst.Symbol, err)
	}
	asks, err := parseLevels(raw.Asks, inst.PriceDecimals, inst.QtyDecimals)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("binance: depth for %s: asks: %w", inst.Symbol, err)
	}

	return model.Snapshot{
		Symbol: inst.Symbol,
		LastID: raw.LastUpdateID,
		Bids:   bids,
		Asks:   asks,
		// A side filled exactly to the limit is the only evidence available
		// that the exchange had more to send. Fewer levels than the limit
		// means the whole side arrived.
		Truncated: limit > 0 && (len(bids) >= limit || len(asks) >= limit),
	}, nil
}
