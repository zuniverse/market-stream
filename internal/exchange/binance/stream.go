package binance

import (
	"fmt"
	"strings"

	"github.com/zuniverse/market-stream/internal/model"
)

// DefaultWSEndpoint is the Binance spot websocket host.
const DefaultWSEndpoint = "wss://stream.binance.com:9443"

// DefaultRESTEndpoint is the Binance spot REST host.
const DefaultRESTEndpoint = "https://api.binance.com"

// streamSuffixes are the streams subscribed to for every symbol.
//
// The depth stream is the 100ms variant rather than the 1000ms default. It is
// ten times the delta rate for the same book, which is the point: the
// sequencing and resync machinery is what this project is about, and the
// faster stream is what puts it under load. Both variants carry the same
// contiguity guarantee on update ids.
var streamSuffixes = []string{"@aggTrade", "@depth@100ms"}

// CombinedStreamURL builds the combined stream URL subscribing to every
// stream of every symbol on one connection.
//
// The symbols are normalised BASE-QUOTE values; the exchange's own form is
// resolved through the cache, so no caller outside this package ever writes
// "btcusdt@depth". base is a websocket host such as DefaultWSEndpoint, with
// no trailing slash.
//
// The combined endpoint wraps each event in {"stream":...,"data":...}, which
// Decode unwraps. The alternative, one connection per stream, multiplies the
// reconnection state by the symbol count for no gain.
func CombinedStreamURL(base string, cache *InstrumentCache, symbols []model.Symbol) (string, error) {
	if len(symbols) == 0 {
		return "", fmt.Errorf("binance: combined stream: no symbols")
	}
	names := make([]string, 0, len(symbols)*len(streamSuffixes))
	for _, sym := range symbols {
		raw, _, ok := cache.RawSymbol(sym)
		if !ok {
			return "", fmt.Errorf("binance: combined stream: unknown symbol %q", sym)
		}
		for _, suffix := range streamSuffixes {
			names = append(names, strings.ToLower(raw)+suffix)
		}
	}
	// The separators go on the wire as written. Binance documents the list
	// with literal "/" and "@", and every name is lower-case alphanumerics
	// plus those, so there is nothing here that needs escaping and escaping
	// it would produce a URL the endpoint does not document accepting.
	return strings.TrimSuffix(base, "/") + "/stream?streams=" + strings.Join(names, "/"), nil
}
