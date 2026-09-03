package book

import (
	"fmt"

	"github.com/zuniverse/market-stream/internal/model"
)

// Side identifies one half of a book.
type Side uint8

const (
	Bid Side = iota + 1
	Ask
)

// String returns the side name, for logs and test failure messages.
func (s Side) String() string {
	switch s {
	case Bid:
		return "bid"
	case Ask:
		return "ask"
	default:
		return "invalid"
	}
}

// LevelDiff is one price at which a locally maintained book and a freshly
// fetched snapshot disagree. A quantity of zero means the level is absent
// from that side of the comparison, since a level with no quantity is a level
// that is not there.
type LevelDiff struct {
	Side   Side
	Price  model.Price
	Local  model.Qty
	Remote model.Qty
}

func (d LevelDiff) String() string {
	return fmt.Sprintf("%s %d: local %d, snapshot %d", d.Side, d.Price, d.Local, d.Remote)
}

// Diff is the result of comparing a tracked book against a fresh snapshot of
// the same symbol. It is the output of the correctness harness: the only way
// to know a book is right rather than merely plausible.
type Diff struct {
	Symbol     model.Symbol
	Live       bool  // the book claimed to be anchored and current
	LocalID    int64 // last update id applied locally
	SnapshotID int64 // last update id the snapshot reflects
	Compared   int   // distinct prices examined across both sides
	Levels     []LevelDiff

	// BidFloor and AskCeil are the edges of the compared range. Prices past
	// them were skipped because one of the two books does not claim to know
	// what is there. Zero means the side was compared in full.
	BidFloor model.Price
	AskCeil  model.Price
}

// OK reports whether the book and the snapshot agree everywhere they were
// compared.
func (d Diff) OK() bool { return len(d.Levels) == 0 }

// IDSkew is how far the snapshot is ahead of the book, in update ids. It is
// never zero in practice: the snapshot is fetched over the network while the
// stream keeps running, so the two describe the same book at two moments. A
// divergence at a small skew, on levels near the touch, is the expected
// consequence of that; a divergence at a skew of zero, or deep in the book,
// is not.
func (d Diff) IDSkew() int64 { return d.SnapshotID - d.LocalID }

func (d Diff) String() string {
	state := "stale"
	if d.Live {
		state = "live"
	}
	if d.OK() {
		return fmt.Sprintf("%s: %s, %d levels match, skew %d",
			d.Symbol, state, d.Compared, d.IDSkew())
	}
	return fmt.Sprintf("%s: %s, %d of %d levels diverge, skew %d, first %v",
		d.Symbol, state, len(d.Levels), d.Compared, d.IDSkew(), d.Levels[0])
}

// Compare checks the tracked book against a freshly fetched snapshot and
// reports every price at which they disagree.
//
// The comparison is restricted to the price range both sides claim to know.
// A snapshot fetched with a depth limit describes the top of the book and
// nothing below its deepest level (D25), and a book seeded from such a
// snapshot is complete only down to the level that seeded it. Comparing past
// either edge reports absence as divergence, when absence there means
// unknown. The range is therefore bounded by whichever of the two is
// shallower, and the bounds used are reported in the Diff.
//
// Compare does not fetch. It is given the snapshot, so the caller decides how
// often to spend a full-depth request on a check, and no book-owning
// goroutine ever blocks on the network.
func (t *Tracker) Compare(s model.Snapshot) (Diff, error) {
	if s.Symbol != t.book.Symbol() {
		return Diff{}, fmt.Errorf("book %s: snapshot for symbol %s", t.book.Symbol(), s.Symbol)
	}
	// sorted puts the snapshot in the same top-first order as the book and
	// rejects a duplicate price, which would otherwise make the merge below
	// report a phantom divergence on whichever copy it did not match.
	bids, err := t.book.bids.sorted(s.Bids)
	if err != nil {
		return Diff{}, fmt.Errorf("book %s: snapshot bid: %w", t.book.Symbol(), err)
	}
	asks, err := t.book.asks.sorted(s.Asks)
	if err != nil {
		return Diff{}, fmt.Errorf("book %s: snapshot ask: %w", t.book.Symbol(), err)
	}

	d := Diff{
		Symbol:     t.book.Symbol(),
		Live:       t.Live(),
		LocalID:    t.seq.LastID(),
		SnapshotID: s.LastID,
	}
	d.BidFloor, d.AskCeil = t.bidFloor, t.askCeil
	if s.Truncated {
		if n := len(bids); n > 0 && bids[n-1].Price > d.BidFloor {
			d.BidFloor = bids[n-1].Price
		}
		if n := len(asks); n > 0 && (d.AskCeil == 0 || asks[n-1].Price < d.AskCeil) {
			d.AskCeil = asks[n-1].Price
		}
	}

	var n int
	d.Levels, n = compareSide(Bid, true, t.book.bids.levels, bids, d.BidFloor, d.Levels)
	d.Compared += n
	d.Levels, n = compareSide(Ask, false, t.book.asks.levels, asks, d.AskCeil, d.Levels)
	d.Compared += n
	return d, nil
}

// compareSide merges two slices already sorted top-first and appends a
// LevelDiff for every price they do not agree on. bound is the last price
// covered on this side, or zero for no bound. It returns the appended
// divergences and the number of distinct prices examined.
func compareSide(sd Side, desc bool, local, remote []model.Level, bound model.Price, out []LevelDiff) ([]LevelDiff, int) {
	covers := func(p model.Price) bool {
		switch {
		case bound == 0:
			return true
		case desc:
			return p >= bound
		default:
			return p <= bound
		}
	}
	// nearer reports whether a sits closer to the top of book than b.
	nearer := func(a, b model.Price) bool {
		if desc {
			return a > b
		}
		return a < b
	}

	var i, j, n int
	for {
		// Both slices are sorted, so the first price out of range means every
		// price after it is out of range too.
		hasL := i < len(local) && covers(local[i].Price)
		hasR := j < len(remote) && covers(remote[j].Price)
		switch {
		case !hasL && !hasR:
			return out, n
		case hasL && hasR && local[i].Price == remote[j].Price:
			if local[i].Qty != remote[j].Qty {
				out = append(out, LevelDiff{Side: sd, Price: local[i].Price, Local: local[i].Qty, Remote: remote[j].Qty})
			}
			i, j = i+1, j+1
		case hasL && (!hasR || nearer(local[i].Price, remote[j].Price)):
			out = append(out, LevelDiff{Side: sd, Price: local[i].Price, Local: local[i].Qty})
			i++
		default:
			out = append(out, LevelDiff{Side: sd, Price: remote[j].Price, Remote: remote[j].Qty})
			j++
		}
		n++
	}
}
