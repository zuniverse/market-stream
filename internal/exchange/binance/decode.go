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

// Decode decodes f into a model.Event, unwrapping the combined stream
// envelope when there is one. Returns an error for unknown event types or
// unknown symbols.
//
// The two fields that decide what to do, the combined-stream "data" wrapper
// and the event type "e", are located by scanning rather than by unmarshalling
// (see jsonscan.go). Reading them with encoding/json meant validating and
// walking the whole frame twice before the payload was decoded a third time,
// which the M4 profile showed was more than half the cost of decoding (D42).
//
// The payload itself still goes through encoding/json, so anything the
// scanner got wrong fails here rather than becoming a wrong event.
func (d *Decoder) Decode(f model.Frame) (model.Event, error) {
	payload := f.Data
	var stream string
	if data, ok := fieldValue(payload, "data"); ok {
		// Combined stream. Unwrap exactly once: a second wrapper is not
		// something the endpoint produces, and recursing on the payload would
		// turn a malformed frame into unbounded work.
		stream, _ = stringField(payload, "stream")
		payload = data
	}
	typ, ok := stringField(payload, "e")
	if !ok {
		if stream != "" {
			return model.Event{}, fmt.Errorf("binance: %s payload carries no event type", stream)
		}
		return model.Event{}, fmt.Errorf("binance: frame carries no event type")
	}
	switch typ {
	case "aggTrade":
		return d.decodeAggTrade(payload)
	case "depthUpdate":
		return d.decodeDepthUpdate(payload)
	default:
		return model.Event{}, fmt.Errorf("binance: unknown event type %q", typ)
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
