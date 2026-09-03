package binance

import (
	"encoding/json"
	"fmt"

	"github.com/zuniverse/market-stream/internal/model"
)

// Decoder converts raw Binance websocket frames into model.Event values
// using encoding/json per D8.
type Decoder struct {
	cache *InstrumentCache
}

// NewDecoder returns a Decoder backed by cache for symbol lookup.
func NewDecoder(cache *InstrumentCache) *Decoder {
	return &Decoder{cache: cache}
}

// envelope is the outer shape of any frame. A connection subscribed to a
// single stream sends the event itself, so Type is set. The combined stream
// endpoint wraps each event, so Stream and Data are set instead and the event
// is one level down.
type envelope struct {
	Type      string `json:"e"`
	EventTime int64  `json:"E"` // exact match prevents "E" from colliding with "e" via case-insensitive fallback

	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// Decode decodes f into a model.Event, unwrapping the combined stream
// envelope when there is one. Returns an error for unknown event types or
// unknown symbols.
func (d *Decoder) Decode(f Frame) (model.Event, error) {
	payload := f.Data
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return model.Event{}, fmt.Errorf("binance: decode envelope: %w", err)
	}
	if len(env.Data) > 0 {
		// Combined stream. Unwrap exactly once: a second wrapper is not
		// something the endpoint produces, and recursing on the payload would
		// turn a malformed frame into unbounded work.
		stream := env.Stream
		payload = env.Data
		env = envelope{}
		if err := json.Unmarshal(payload, &env); err != nil {
			return model.Event{}, fmt.Errorf("binance: decode %s payload: %w", stream, err)
		}
	}
	switch env.Type {
	case "aggTrade":
		return d.decodeAggTrade(payload)
	case "depthUpdate":
		return d.decodeDepthUpdate(payload)
	default:
		return model.Event{}, fmt.Errorf("binance: unknown event type %q", env.Type)
	}
}

func (d *Decoder) decodeAggTrade(data []byte) (model.Event, error) {
	var raw struct {
		Symbol  string `json:"s"`
		Price   string `json:"p"`
		Qty     string `json:"q"`
		Maker   bool   `json:"m"` // true = buyer is maker = sell trade
		Discard bool   `json:"M"` // always true; explicit to prevent overwriting m via case-insensitive fallback
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return model.Event{}, fmt.Errorf("binance: decode aggTrade: %w", err)
	}
	inst, ok := d.cache.Lookup(raw.Symbol)
	if !ok {
		return model.Event{}, fmt.Errorf("binance: aggTrade: unknown symbol %q", raw.Symbol)
	}
	price, err := model.ParsePrice(raw.Price, inst.PriceDecimals)
	if err != nil {
		return model.Event{}, fmt.Errorf("binance: aggTrade price %q: %w", raw.Price, err)
	}
	qty, err := model.ParseQty(raw.Qty, inst.QtyDecimals)
	if err != nil {
		return model.Event{}, fmt.Errorf("binance: aggTrade qty %q: %w", raw.Qty, err)
	}
	return model.Event{
		Kind: model.KindTrade,
		Trade: model.Trade{
			Symbol: inst.Symbol,
			Price:  price,
			Qty:    qty,
			IsBuy:  !raw.Maker,
		},
	}, nil
}

func (d *Decoder) decodeDepthUpdate(data []byte) (model.Event, error) {
	var raw struct {
		Symbol  string      `json:"s"`
		FirstID int64       `json:"U"`
		LastID  int64       `json:"u"`
		Bids    [][2]string `json:"b"`
		Asks    [][2]string `json:"a"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return model.Event{}, fmt.Errorf("binance: decode depthUpdate: %w", err)
	}
	inst, ok := d.cache.Lookup(raw.Symbol)
	if !ok {
		return model.Event{}, fmt.Errorf("binance: depthUpdate: unknown symbol %q", raw.Symbol)
	}
	bids, err := parseLevels(raw.Bids, inst.PriceDecimals, inst.QtyDecimals)
	if err != nil {
		return model.Event{}, fmt.Errorf("binance: depthUpdate bids: %w", err)
	}
	asks, err := parseLevels(raw.Asks, inst.PriceDecimals, inst.QtyDecimals)
	if err != nil {
		return model.Event{}, fmt.Errorf("binance: depthUpdate asks: %w", err)
	}
	return model.Event{
		Kind: model.KindBookDelta,
		BookDelta: model.BookDelta{
			Symbol:  inst.Symbol,
			FirstID: raw.FirstID,
			LastID:  raw.LastID,
			Bids:    bids,
			Asks:    asks,
		},
	}, nil
}

func parseLevels(raw [][2]string, priceDec, qtyDec int) ([]model.Level, error) {
	levels := make([]model.Level, 0, len(raw))
	for _, pair := range raw {
		price, err := model.ParsePrice(pair[0], priceDec)
		if err != nil {
			return nil, fmt.Errorf("price %q: %w", pair[0], err)
		}
		qty, err := model.ParseQty(pair[1], qtyDec)
		if err != nil {
			return nil, fmt.Errorf("qty %q: %w", pair[1], err)
		}
		levels = append(levels, model.Level{Price: price, Qty: qty})
	}
	return levels, nil
}
