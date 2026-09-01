package model

// Price is a decimal fixed-point market price stored as int64.
// The decimal exponent is an attribute of the instrument, not stored here.
// Prices from different symbols must not be compared directly (D14).
type Price int64

// Qty is a decimal fixed-point order quantity stored as int64.
// The decimal exponent is an attribute of the instrument, not stored here.
type Qty int64

// Symbol is the normalised trading pair in BASE-QUOTE form (e.g. "BTC-USDT").
// All exchange-specific symbol formats are converted to this form at the
// decode boundary (D16).
type Symbol string

// EventKind is the discriminator for the Event tagged struct (D15).
type EventKind uint8

const (
	KindTrade     EventKind = iota + 1 // public execution event
	KindBookDelta                      // incremental order book update
	KindSnapshot                       // full order book snapshot
)

// Level is one price level in an order book.
type Level struct {
	Price Price
	Qty   Qty
}

// Trade is a public execution event.
type Trade struct {
	Symbol Symbol
	Price  Price
	Qty    Qty
	IsBuy  bool
}

// BookDelta carries incremental order book changes for one symbol.
type BookDelta struct {
	Symbol  Symbol
	FirstID int64
	LastID  int64
	Bids    []Level
	Asks    []Level
}

// Snapshot is a full order book snapshot for one symbol.
type Snapshot struct {
	Symbol Symbol
	LastID int64
	Bids   []Level
	Asks   []Level

	// Truncated reports that the source returned only the top of the book
	// because it was asked for a bounded number of levels, so this snapshot
	// is authoritative only down to the last level held on each side. Beyond
	// that, a price being absent means unknown, not empty: the exchange may
	// well hold depth there that was never sent.
	//
	// A consumer that compares a locally maintained book against a fresh
	// snapshot must restrict the comparison to the price range the snapshot
	// covers, or every level below the cut will look like a divergence.
	Truncated bool
}

// Event is the canonical pipeline message type.
// Kind selects which payload field is populated; the others are zero (D15).
type Event struct {
	Kind      EventKind
	Trade     Trade
	BookDelta BookDelta
	Snapshot  Snapshot
}
