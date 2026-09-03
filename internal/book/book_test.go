package book

import (
	"errors"
	"math/rand"
	"slices"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

// lv is a shorthand for a level, keeping the table cases readable. Prices and
// quantities are fixed-point int64 throughout; no float appears in this
// package or its tests (D2).
func lv(price, qty int64) model.Level {
	return model.Level{Price: model.Price(price), Qty: model.Qty(qty)}
}

func levels(pairs ...int64) []model.Level {
	if len(pairs)%2 != 0 {
		panic("levels: odd number of arguments")
	}
	out := make([]model.Level, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, lv(pairs[i], pairs[i+1]))
	}
	return out
}

// checkInvariants asserts the two properties every other test depends on:
// each side is sorted with the top of book at index 0, and no price appears
// twice on a side. A duplicate would be unreachable through the binary search
// and would never be updated or removed again.
func checkInvariants(t *testing.T, b *Book) {
	t.Helper()
	for _, s := range []struct {
		name   string
		levels []model.Level
		desc   bool
	}{
		{"bid", b.bids.levels, true},
		{"ask", b.asks.levels, false},
	} {
		for i := 1; i < len(s.levels); i++ {
			prev, cur := s.levels[i-1].Price, s.levels[i].Price
			if prev == cur {
				t.Errorf("%s side: duplicate price %d at index %d", s.name, cur, i)
				continue
			}
			if s.desc && cur > prev {
				t.Errorf("%s side: not descending at index %d: %d then %d", s.name, i, prev, cur)
			}
			if !s.desc && cur < prev {
				t.Errorf("%s side: not ascending at index %d: %d then %d", s.name, i, prev, cur)
			}
		}
		for _, l := range s.levels {
			if l.Qty == 0 {
				t.Errorf("%s side: zero quantity retained at price %d", s.name, l.Price)
			}
		}
	}
}

func TestApplyOrdering(t *testing.T) {
	tests := []struct {
		name     string
		bids     []model.Level
		asks     []model.Level
		wantBids []model.Level
		wantAsks []model.Level
	}{
		{
			name:     "insert out of order sorts both sides",
			bids:     levels(100, 1, 102, 2, 101, 3),
			asks:     levels(105, 1, 103, 2, 104, 3),
			wantBids: levels(102, 2, 101, 3, 100, 1),
			wantAsks: levels(103, 2, 104, 3, 105, 1),
		},
		{
			name:     "update existing price keeps position",
			bids:     levels(100, 1, 101, 2, 100, 7),
			wantBids: levels(101, 2, 100, 7),
		},
		{
			name:     "zero quantity removes the level",
			bids:     levels(100, 1, 101, 2, 102, 3, 101, 0),
			wantBids: levels(102, 3, 100, 1),
		},
		{
			name:     "removing the top of book promotes the next level",
			asks:     levels(103, 1, 104, 2, 103, 0),
			wantAsks: levels(104, 2),
		},
		{
			name: "removing an unheld level is a no-op",
			// Levels beyond a truncated snapshot are unknown rather than
			// absent, so a delete for one of them must not be an error.
			bids:     levels(100, 1, 99, 0),
			wantBids: levels(100, 1),
		},
		{
			name:     "repeated price within one delta resolves to the last",
			asks:     levels(103, 1, 103, 5, 103, 2),
			wantAsks: levels(103, 2),
		},
		{
			name: "insert then remove leaves the side empty",
			bids: levels(100, 1, 100, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New("BTC-USDT")
			if err := b.Apply(tt.bids, tt.asks); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			checkInvariants(t, b)

			gotBids := b.Bids(b.BidDepth())
			if !slices.Equal(gotBids, tt.wantBids) {
				t.Errorf("bids = %v, want %v", gotBids, tt.wantBids)
			}
			gotAsks := b.Asks(b.AskDepth())
			if !slices.Equal(gotAsks, tt.wantAsks) {
				t.Errorf("asks = %v, want %v", gotAsks, tt.wantAsks)
			}
		})
	}
}

func TestBestBidAsk(t *testing.T) {
	b := New("BTC-USDT")
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid on an empty book returned ok")
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("BestAsk on an empty book returned ok")
	}

	if err := b.Apply(levels(100, 1, 102, 2), levels(105, 1, 103, 2)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, ok := b.BestBid(); !ok || got != lv(102, 2) {
		t.Errorf("BestBid = %v, %v, want %v, true", got, ok, lv(102, 2))
	}
	if got, ok := b.BestAsk(); !ok || got != lv(103, 2) {
		t.Errorf("BestAsk = %v, %v, want %v, true", got, ok, lv(103, 2))
	}

	// Emptying a side must return to the not-present answer rather than
	// leaving a stale best price behind.
	if err := b.Apply(levels(100, 0, 102, 0), nil); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid after emptying the bid side returned ok")
	}
}

func TestTopNLevels(t *testing.T) {
	b := New("BTC-USDT")
	if err := b.Apply(levels(100, 1, 101, 2, 102, 3), levels(103, 4, 104, 5)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := b.Bids(2); !slices.Equal(got, levels(102, 3, 101, 2)) {
		t.Errorf("Bids(2) = %v", got)
	}
	if got := b.Asks(1); !slices.Equal(got, levels(103, 4)) {
		t.Errorf("Asks(1) = %v", got)
	}
	if got := b.Bids(99); len(got) != 3 {
		t.Errorf("Bids(99) returned %d levels, want the whole side (3)", len(got))
	}
	if got := b.Bids(0); got != nil {
		t.Errorf("Bids(0) = %v, want nil", got)
	}
	if got := b.Bids(-1); got != nil {
		t.Errorf("Bids(-1) = %v, want nil", got)
	}

	// The result must be a copy: on the shard query path it is read by a
	// goroutine other than the one that owns the book (D4).
	got := b.Bids(1)
	got[0].Qty = 999
	if best, _ := b.BestBid(); best.Qty != 3 {
		t.Errorf("mutating the result of Bids changed the book: best bid qty = %d", best.Qty)
	}
}

func TestCrossedBook(t *testing.T) {
	tests := []struct {
		name string
		bids []model.Level
		asks []model.Level
		want bool
	}{
		{name: "empty book"},
		{name: "bids only", bids: levels(100, 1)},
		{name: "asks only", asks: levels(103, 1)},
		{name: "normal spread", bids: levels(100, 1), asks: levels(101, 1)},
		{name: "touching at the same price", bids: levels(100, 1), asks: levels(100, 1), want: true},
		{name: "bid above ask", bids: levels(102, 1), asks: levels(100, 1), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New("BTC-USDT")
			if err := b.Apply(tt.bids, tt.asks); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if got := b.Crossed(); got != tt.want {
				t.Errorf("Crossed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCrossedIsReportedNotRejected(t *testing.T) {
	// A cross is a signal for the sequencing layer to resync (M2.4), so the
	// book must store it and report it rather than refusing the update: a
	// silently dropped delta is exactly the failure this milestone guards.
	b := New("BTC-USDT")
	if err := b.Apply(levels(100, 1), levels(101, 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := b.Apply(levels(105, 2), nil); err != nil {
		t.Fatalf("Apply of a crossing level returned an error: %v", err)
	}
	if !b.Crossed() {
		t.Fatal("Crossed() = false after applying a bid above the best ask")
	}
	if best, _ := b.BestBid(); best != lv(105, 2) {
		t.Errorf("BestBid = %v, want the crossing level to have been stored", best)
	}

	// And it must clear once the offending level goes away.
	if err := b.Apply(levels(105, 0), nil); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if b.Crossed() {
		t.Error("Crossed() = true after the crossing level was removed")
	}
}

func TestApplyRejectsInvalidLevels(t *testing.T) {
	tests := []struct {
		name string
		bids []model.Level
		asks []model.Level
		want error
	}{
		{name: "zero price", bids: levels(0, 1), want: ErrInvalidPrice},
		{name: "negative price", asks: levels(-1, 1), want: ErrInvalidPrice},
		{name: "negative quantity", bids: levels(100, -1), want: ErrNegativeQty},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New("BTC-USDT")
			if err := b.Apply(levels(100, 1), levels(101, 1)); err != nil {
				t.Fatalf("Apply of the initial state: %v", err)
			}
			err := b.Apply(tt.bids, tt.asks)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Apply = %v, want %v", err, tt.want)
			}
			// All or nothing: the book must be exactly as it was.
			if !slices.Equal(b.Bids(9), levels(100, 1)) || !slices.Equal(b.Asks(9), levels(101, 1)) {
				t.Errorf("rejected Apply mutated the book: bids %v asks %v", b.Bids(9), b.Asks(9))
			}
		})
	}
}

func TestApplyIsAllOrNothing(t *testing.T) {
	// The invalid level sits after two valid ones. If validation were
	// interleaved with application, the first two would already be stored.
	b := New("BTC-USDT")
	err := b.Apply(levels(100, 1, 101, 2, 102, -3), nil)
	if !errors.Is(err, ErrNegativeQty) {
		t.Fatalf("Apply = %v, want %v", err, ErrNegativeQty)
	}
	if b.BidDepth() != 0 {
		t.Errorf("BidDepth = %d after a rejected Apply, want 0: %v", b.BidDepth(), b.Bids(9))
	}
}

func TestReset(t *testing.T) {
	b := New("BTC-USDT")
	if err := b.Apply(levels(90, 1, 91, 2), levels(200, 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Unsorted input, with a zero quantity that a snapshot should simply not
	// contain as a level.
	err := b.Reset(levels(100, 1, 102, 2, 101, 3, 99, 0), levels(105, 1, 103, 2))
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	checkInvariants(t, b)

	if got := b.Bids(9); !slices.Equal(got, levels(102, 2, 101, 3, 100, 1)) {
		t.Errorf("bids after Reset = %v", got)
	}
	if got := b.Asks(9); !slices.Equal(got, levels(103, 2, 105, 1)) {
		t.Errorf("asks after Reset = %v", got)
	}

	// The previous state must be gone, not merged.
	if _, found := b.bids.search(90); found {
		t.Error("a level from before Reset survived it")
	}

	// A later delta must still find its way into the reset side, which is the
	// property a resorted-but-corrupt slice would break.
	if err := b.Apply(levels(101, 0, 104, 7), nil); err != nil {
		t.Fatalf("Apply after Reset: %v", err)
	}
	if got := b.Bids(9); !slices.Equal(got, levels(104, 7, 102, 2, 100, 1)) {
		t.Errorf("bids after Apply following Reset = %v", got)
	}
}

func TestResetRejectsDuplicateAndInvalidLevels(t *testing.T) {
	tests := []struct {
		name string
		bids []model.Level
		asks []model.Level
		want error
	}{
		{name: "duplicate bid price", bids: levels(100, 1, 100, 2), want: ErrDuplicatePrice},
		{name: "duplicate ask price", asks: levels(103, 1, 103, 2), want: ErrDuplicatePrice},
		{name: "negative quantity", bids: levels(100, -1), want: ErrNegativeQty},
		{name: "zero price", asks: levels(0, 1), want: ErrInvalidPrice},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New("BTC-USDT")
			if err := b.Apply(levels(100, 1), levels(101, 1)); err != nil {
				t.Fatalf("Apply of the initial state: %v", err)
			}
			if err := b.Reset(tt.bids, tt.asks); !errors.Is(err, tt.want) {
				t.Fatalf("Reset = %v, want %v", err, tt.want)
			}
			if !slices.Equal(b.Bids(9), levels(100, 1)) || !slices.Equal(b.Asks(9), levels(101, 1)) {
				t.Errorf("rejected Reset mutated the book: bids %v asks %v", b.Bids(9), b.Asks(9))
			}
		})
	}
}

func TestSymbol(t *testing.T) {
	if got := New("ETH-USDT").Symbol(); got != model.Symbol("ETH-USDT") {
		t.Errorf("Symbol() = %q", got)
	}
}

// TestApplyAgainstReferenceModel drives a long random sequence of updates
// through the book and through a naive map, then compares. The map is
// obviously correct and obviously too slow to ship; it is here to catch the
// cases the hand-written tables did not think of, in the one component of
// this pipeline where a mistake produces a plausible answer instead of a
// crash.
func TestApplyAgainstReferenceModel(t *testing.T) {
	const (
		steps      = 20000
		priceRange = 200
	)
	rng := rand.New(rand.NewSource(1))
	b := New("BTC-USDT")
	refBids := map[model.Price]model.Qty{}
	refAsks := map[model.Price]model.Qty{}

	for i := 0; i < steps; i++ {
		l := lv(int64(1+rng.Intn(priceRange)), 0)
		// Zero a third of the time, so deletions of both held and unheld
		// levels are exercised.
		if rng.Intn(3) != 0 {
			l.Qty = model.Qty(1 + rng.Intn(1000))
		}

		ref := refAsks
		var bids, asks []model.Level
		if rng.Intn(2) == 0 {
			ref, bids = refBids, []model.Level{l}
		} else {
			asks = []model.Level{l}
		}
		if err := b.Apply(bids, asks); err != nil {
			t.Fatalf("step %d: Apply: %v", i, err)
		}
		if l.Qty == 0 {
			delete(ref, l.Price)
		} else {
			ref[l.Price] = l.Qty
		}
	}
	checkInvariants(t, b)

	assertSideMatchesRef(t, "bid", b.Bids(b.BidDepth()), refBids, true)
	assertSideMatchesRef(t, "ask", b.Asks(b.AskDepth()), refAsks, false)
}

func assertSideMatchesRef(t *testing.T, name string, got []model.Level, ref map[model.Price]model.Qty, desc bool) {
	t.Helper()
	want := make([]model.Level, 0, len(ref))
	for p, q := range ref {
		want = append(want, model.Level{Price: p, Qty: q})
	}
	slices.SortFunc(want, func(a, b model.Level) int {
		if desc {
			return int(b.Price - a.Price)
		}
		return int(a.Price - b.Price)
	})
	if !slices.Equal(got, want) {
		t.Errorf("%s side diverged from the reference model:\n got %v\nwant %v", name, got, want)
	}
}
