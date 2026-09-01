package book

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/zuniverse/market-stream/internal/model"
)

// Sentinel errors returned by Apply and Reset. Every one of them means the
// caller handed the book a level that cannot exist on an exchange, so the
// book refuses the whole update rather than storing a state that never was.
var (
	ErrInvalidPrice   = errors.New("book: level price must be positive")
	ErrNegativeQty    = errors.New("book: level quantity must not be negative")
	ErrDuplicatePrice = errors.New("book: duplicate price in snapshot side")
)

// side is one half of a book. Levels are kept sorted so that index 0 is
// always the top of book: bids descend by price, asks ascend. Both sides
// therefore read the same way, and the hot operations (best price, first N
// levels) are a slice prefix rather than a search (D23).
type side struct {
	levels []model.Level
	// desc reports whether this side sorts by descending price, which is
	// true for bids and false for asks.
	desc bool
}

// compare orders a stored level against a target price in this side's
// direction. It is the single definition of the ordering, shared by the
// binary search and the sort in reset so the two cannot drift apart.
func (s *side) compare(l model.Level, p model.Price) int {
	if s.desc {
		return cmp.Compare(p, l.Price)
	}
	return cmp.Compare(l.Price, p)
}

// apply inserts, updates, or removes the level at l.Price. A zero quantity
// removes the level: that is how exchanges signal a deletion.
func (s *side) apply(l model.Level) {
	i, found := s.search(l.Price)
	switch {
	case l.Qty != 0 && found:
		s.levels[i].Qty = l.Qty
	case l.Qty != 0:
		s.levels = slices.Insert(s.levels, i, l)
	case found:
		s.levels = slices.Delete(s.levels, i, i+1)
	default:
		// Deleting a level the book does not hold. This is normal rather than
		// an error: a REST snapshot is truncated to a depth limit, so levels
		// past the last one returned are unknown, not empty, and a delta may
		// legitimately remove one of them (M2.2).
	}
}

// search returns the index at which p sits or would be inserted, and
// whether a level at exactly p is already present.
func (s *side) search(p model.Price) (int, bool) {
	return slices.BinarySearchFunc(s.levels, p, s.compare)
}

// top returns a copy of at most n levels from the top of book.
func (s *side) top(n int) []model.Level {
	if n <= 0 || len(s.levels) == 0 {
		return nil
	}
	n = min(n, len(s.levels))
	out := make([]model.Level, n)
	copy(out, s.levels[:n])
	return out
}

// sorted returns levels ordered for this side, without touching the current
// contents. Zero quantities are skipped: a snapshot states what is on the
// book, so a level with no quantity is simply a level that is not there.
func (s *side) sorted(levels []model.Level) ([]model.Level, error) {
	out := make([]model.Level, 0, len(levels))
	for _, l := range levels {
		if l.Qty == 0 {
			continue
		}
		out = append(out, l)
	}
	slices.SortFunc(out, func(a, b model.Level) int {
		return s.compare(a, b.Price)
	})
	// A duplicate price would leave two entries the binary search can reach
	// only one of, so the other would be invisible to every later update and
	// would sit in the book forever. Detect it here, where it is one
	// comparison per level on an already sorted slice.
	for i := 1; i < len(out); i++ {
		if out[i].Price == out[i-1].Price {
			return nil, fmt.Errorf("price %d: %w", out[i].Price, ErrDuplicatePrice)
		}
	}
	return out, nil
}

// Book is the order book for one symbol.
//
// It is a pure container: it applies level updates and answers reads, and
// knows nothing about sequence numbers, snapshots over the wire, or
// concurrency. Sequencing is added in M2.3 and ownership in M2.5.
//
// A Book is not safe for concurrent use. It is owned by exactly one
// goroutine, which is what lets the book state carry no mutex at all (D3).
type Book struct {
	symbol model.Symbol
	bids   side
	asks   side
}

// New returns an empty book for symbol.
func New(symbol model.Symbol) *Book {
	return &Book{
		symbol: symbol,
		bids:   side{desc: true},
		asks:   side{desc: false},
	}
}

// Symbol returns the symbol this book tracks.
func (b *Book) Symbol() model.Symbol { return b.symbol }

// Apply applies one incremental update to the book. Each level either sets
// the quantity at its price or, when the quantity is zero, removes that price
// from the side. Levels within one call are applied in order, so a repeated
// price resolves to its last occurrence.
//
// Apply is all or nothing: every level is validated before any is applied. A
// delta that failed halfway through would leave the book in a state that
// never existed on the exchange, and nothing downstream could tell that from
// a correct one.
func (b *Book) Apply(bids, asks []model.Level) error {
	if err := validate(bids); err != nil {
		return fmt.Errorf("book %s: bid: %w", b.symbol, err)
	}
	if err := validate(asks); err != nil {
		return fmt.Errorf("book %s: ask: %w", b.symbol, err)
	}
	for _, l := range bids {
		b.bids.apply(l)
	}
	for _, l := range asks {
		b.asks.apply(l)
	}
	return nil
}

// Reset discards the current state and replaces it with the given levels,
// which need not be sorted. It is how a full snapshot is loaded (M2.2).
//
// Like Apply, Reset validates before it mutates, so a rejected snapshot
// leaves the previous book untouched.
func (b *Book) Reset(bids, asks []model.Level) error {
	if err := validate(bids); err != nil {
		return fmt.Errorf("book %s: bid: %w", b.symbol, err)
	}
	if err := validate(asks); err != nil {
		return fmt.Errorf("book %s: ask: %w", b.symbol, err)
	}
	newBids, err := b.bids.sorted(bids)
	if err != nil {
		return fmt.Errorf("book %s: bid: %w", b.symbol, err)
	}
	newAsks, err := b.asks.sorted(asks)
	if err != nil {
		return fmt.Errorf("book %s: ask: %w", b.symbol, err)
	}
	b.bids.levels, b.asks.levels = newBids, newAsks
	return nil
}

// BestBid returns the highest bid, and false if the bid side is empty.
func (b *Book) BestBid() (model.Level, bool) {
	if len(b.bids.levels) == 0 {
		return model.Level{}, false
	}
	return b.bids.levels[0], true
}

// BestAsk returns the lowest ask, and false if the ask side is empty.
func (b *Book) BestAsk() (model.Level, bool) {
	if len(b.asks.levels) == 0 {
		return model.Level{}, false
	}
	return b.asks.levels[0], true
}

// Bids returns at most n levels from the top of the bid side, in descending
// price order. n of zero or less returns nil; use BidDepth for the whole
// side.
//
// The result is a copy. Reads exist to be answered on the shard query path,
// where the value is sent to another goroutine while this one keeps applying
// deltas (D4), so handing out a view of the internal slice would be a data
// race by construction.
func (b *Book) Bids(n int) []model.Level { return b.bids.top(n) }

// Asks returns at most n levels from the top of the ask side, in ascending
// price order. It copies, for the reason given on Bids.
func (b *Book) Asks(n int) []model.Level { return b.asks.top(n) }

// BidDepth returns the number of price levels held on the bid side.
func (b *Book) BidDepth() int { return len(b.bids.levels) }

// AskDepth returns the number of price levels held on the ask side.
func (b *Book) AskDepth() int { return len(b.asks.levels) }

// Crossed reports whether the best bid is at or above the best ask. An empty
// side is never crossed.
//
// A crossed book is not a state any exchange publishes: it means a delta was
// lost, applied out of sequence, or applied to the wrong side. The book
// records what it was told and reports the condition rather than rejecting
// the update, because the response to it is to resync the symbol (M2.4), and
// that decision belongs to the sequencing layer rather than to the container
// (D24).
func (b *Book) Crossed() bool {
	if len(b.bids.levels) == 0 || len(b.asks.levels) == 0 {
		return false
	}
	return b.bids.levels[0].Price >= b.asks.levels[0].Price
}

// validate rejects levels that cannot exist on an exchange. A price of zero
// or less is meaningless, and a negative quantity is not a deletion but a
// decoding bug that would otherwise be stored and served as real depth.
func validate(levels []model.Level) error {
	for _, l := range levels {
		if l.Price <= 0 {
			return fmt.Errorf("price %d: %w", l.Price, ErrInvalidPrice)
		}
		if l.Qty < 0 {
			return fmt.Errorf("price %d qty %d: %w", l.Price, l.Qty, ErrNegativeQty)
		}
	}
	return nil
}
