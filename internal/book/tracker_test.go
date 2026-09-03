package book

import (
	"errors"
	"math/rand"
	"slices"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

const testSymbol model.Symbol = "BTC-USDT"

// exchange is a minimal stand-in for the venue: it holds the authoritative
// book, emits deltas that describe every change it makes, and can be asked
// for a snapshot of its current state at any point. It is what makes the M2.4
// criterion testable, since the criterion compares a resynced book against a
// freshly fetched snapshot of the same book.
type exchange struct {
	bids   map[model.Price]model.Qty
	asks   map[model.Price]model.Qty
	lastID int64
	rng    *rand.Rand
}

func newExchange(seed int64) *exchange {
	e := &exchange{
		bids:   make(map[model.Price]model.Qty),
		asks:   make(map[model.Price]model.Qty),
		lastID: 1000,
		rng:    rand.New(rand.NewSource(seed)),
	}
	for p := model.Price(900); p < 1000; p += 3 {
		e.bids[p] = model.Qty(1 + e.rng.Intn(9))
	}
	for p := model.Price(1000); p < 1100; p += 3 {
		e.asks[p] = model.Qty(1 + e.rng.Intn(9))
	}
	return e
}

// next mutates the book and returns the delta describing the mutation. Bids
// stay below 1000 and asks at or above it, so the generated stream never
// crosses and a crossed book in a test means the book got it wrong.
func (e *exchange) next() model.BookDelta {
	d := model.BookDelta{Symbol: testSymbol, FirstID: e.lastID + 1}
	for n := 1 + e.rng.Intn(3); n > 0; n-- {
		qty := model.Qty(e.rng.Intn(6)) // zero deletes the level
		if e.rng.Intn(2) == 0 {
			p := model.Price(900 + e.rng.Intn(100))
			e.set(e.bids, p, qty)
			d.Bids = append(d.Bids, model.Level{Price: p, Qty: qty})
		} else {
			p := model.Price(1000 + e.rng.Intn(100))
			e.set(e.asks, p, qty)
			d.Asks = append(d.Asks, model.Level{Price: p, Qty: qty})
		}
	}
	e.lastID += int64(1 + e.rng.Intn(3))
	d.LastID = e.lastID
	return d
}

func (e *exchange) set(side map[model.Price]model.Qty, p model.Price, q model.Qty) {
	if q == 0 {
		delete(side, p)
		return
	}
	side[p] = q
}

func (e *exchange) snapshot() model.Snapshot {
	s := model.Snapshot{Symbol: testSymbol, LastID: e.lastID}
	for p, q := range e.bids {
		s.Bids = append(s.Bids, model.Level{Price: p, Qty: q})
	}
	for p, q := range e.asks {
		s.Asks = append(s.Asks, model.Level{Price: p, Qty: q})
	}
	// Map iteration order is random, which is useful here: Reset must sort.
	return s
}

// assertMatches compares every level of the book against the snapshot. Both
// sides are compared in full, so an extra local level is caught as surely as
// a missing one.
func assertMatches(t *testing.T, b *Book, s model.Snapshot) {
	t.Helper()
	want := func(levels []model.Level, desc bool) []model.Level {
		out := slices.Clone(levels)
		slices.SortFunc(out, func(a, b model.Level) int {
			if desc {
				return int(b.Price - a.Price)
			}
			return int(a.Price - b.Price)
		})
		return out
	}
	if got, w := b.Bids(b.BidDepth()), want(s.Bids, true); !slices.Equal(got, w) {
		t.Errorf("bid side:\n got %v\nwant %v", got, w)
	}
	if got, w := b.Asks(b.AskDepth()), want(s.Asks, false); !slices.Equal(got, w) {
		t.Errorf("ask side:\n got %v\nwant %v", got, w)
	}
}

// TestTrackerStartup covers the path every symbol takes before it has a book:
// deltas are held, the first snapshot anchors them, and the ones it already
// covers are dropped rather than applied twice.
func TestTrackerStartup(t *testing.T) {
	tr := NewTracker(testSymbol, 0)
	if tr.Live() {
		t.Fatal("a tracker with no snapshot reports Live")
	}
	if !tr.BeginFetch() {
		t.Fatal("BeginFetch is false on a tracker that has never had a snapshot")
	}
	if tr.BeginFetch() {
		t.Error("BeginFetch is true while a fetch is already outstanding")
	}

	e := newExchange(1)
	var buffered []model.BookDelta
	for range 5 {
		d := e.next()
		buffered = append(buffered, d)
		if st, err := tr.Apply(d); err != nil || st != StatusBuffered {
			t.Fatalf("Apply during fetch = %v, %v, want %v", st, err, StatusBuffered)
		}
	}
	if tr.Buffered() != len(buffered) {
		t.Fatalf("Buffered() = %d, want %d", tr.Buffered(), len(buffered))
	}
	// The snapshot is taken after those five deltas, so replay must discard
	// all of them as stale.
	snap := e.snapshot()

	if err := tr.Load(snap); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !tr.Live() {
		t.Fatal("not Live after loading a snapshot newer than every buffered delta")
	}
	if tr.Buffered() != 0 {
		t.Errorf("Buffered() = %d after a successful load, want 0", tr.Buffered())
	}
	if tr.LastID() != snap.LastID {
		t.Errorf("LastID() = %d, want the snapshot id %d", tr.LastID(), snap.LastID)
	}
	assertMatches(t, tr.Book(), snap)

	for range 20 {
		d := e.next()
		if st, err := tr.Apply(d); err != nil || st != StatusApplied {
			t.Fatalf("Apply after load = %v, %v, want %v", st, err, StatusApplied)
		}
	}
	assertMatches(t, tr.Book(), e.snapshot())
	if tr.Book().Crossed() {
		t.Error("book is crossed after a clean stream")
	}
	if st := tr.Stats(); st.Resyncs != 1 || st.Applied != 20 || st.Buffered != 5 || st.Gaps != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// TestTrackerDroppedDelta is the M2.4 done criterion: a deliberately dropped
// delta produces a resync that restores a book identical to a freshly fetched
// snapshot.
func TestTrackerDroppedDelta(t *testing.T) {
	e := newExchange(7)
	tr := NewTracker(testSymbol, 0)
	tr.BeginFetch()
	if err := tr.Load(e.snapshot()); err != nil {
		t.Fatalf("initial Load: %v", err)
	}

	for range 30 {
		if st, err := tr.Apply(e.next()); err != nil || st != StatusApplied {
			t.Fatalf("Apply = %v, %v", st, err)
		}
	}

	e.next() // produced by the exchange, never delivered to the tracker

	st, err := tr.Apply(e.next())
	if err != nil || st != StatusGapped {
		t.Fatalf("Apply after a dropped delta = %v, %v, want %v", st, err, StatusGapped)
	}
	if tr.Live() {
		t.Fatal("Live is true after a gap")
	}

	// The stream keeps running while the snapshot request is in flight.
	if !tr.BeginFetch() {
		t.Fatal("BeginFetch is false after a gap")
	}
	for range 4 {
		if st, err := tr.Apply(e.next()); err != nil || st != StatusBuffered {
			t.Fatalf("Apply during resync = %v, %v", st, err)
		}
	}
	snap := e.snapshot() // fetched here, mid-stream
	for range 4 {
		if st, err := tr.Apply(e.next()); err != nil || st != StatusBuffered {
			t.Fatalf("Apply after the fetch = %v, %v", st, err)
		}
	}

	if err := tr.Load(snap); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !tr.Live() {
		t.Fatal("not Live after a resync that should have closed the gap")
	}
	assertMatches(t, tr.Book(), e.snapshot())

	for range 30 {
		if st, err := tr.Apply(e.next()); err != nil || st != StatusApplied {
			t.Fatalf("Apply after resync = %v, %v", st, err)
		}
	}
	assertMatches(t, tr.Book(), e.snapshot())
	if s := tr.Stats(); s.Gaps != 1 || s.Resyncs != 2 {
		t.Errorf("stats = %+v, want 1 gap and 2 resyncs", s)
	}
}

// TestTrackerSnapshotTooOld covers the case the Binance procedure answers with
// "go back and fetch again": the stream ran past the buffer's oldest held
// delta while the request was in flight, so the snapshot cannot be joined to
// what is held.
func TestTrackerSnapshotTooOld(t *testing.T) {
	e := newExchange(11)
	tr := NewTracker(testSymbol, 0)
	tr.BeginFetch()
	if err := tr.Load(e.snapshot()); err != nil {
		t.Fatalf("initial Load: %v", err)
	}

	stale := e.snapshot() // fetched now, delivered much later
	for range 10 {
		e.next() // dropped in transit: everything below is a gap
	}
	if st, _ := tr.Apply(e.next()); st != StatusGapped {
		t.Fatalf("Apply = %v, want %v", st, StatusGapped)
	}
	tr.BeginFetch()
	for range 3 {
		tr.Apply(e.next())
	}
	held := tr.Buffered()

	if err := tr.Load(stale); err != nil {
		t.Fatalf("Load of an old snapshot returned an error, want a quiet refetch: %v", err)
	}
	if tr.Live() {
		t.Error("Live is true after loading a snapshot that predates the buffer")
	}
	if tr.Buffered() != held {
		t.Errorf("Buffered() = %d after a failed load, want the %d held deltas kept", tr.Buffered(), held)
	}
	if s := tr.Stats(); s.Refetches != 1 {
		t.Errorf("Refetches = %d, want 1", s.Refetches)
	}
	if !tr.BeginFetch() {
		t.Fatal("BeginFetch is false after a snapshot that failed to anchor the book")
	}

	// A snapshot taken now covers the held deltas, so the resync completes.
	if err := tr.Load(e.snapshot()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !tr.Live() {
		t.Fatal("not Live after a snapshot newer than the buffer")
	}
	assertMatches(t, tr.Book(), e.snapshot())
}

// TestTrackerBufferOverflow asserts the bound and its policy: the buffer never
// grows past its capacity, the oldest delta goes, and the drop is counted.
func TestTrackerBufferOverflow(t *testing.T) {
	e := newExchange(13)
	tr := NewTracker(testSymbol, 4)
	tr.BeginFetch()

	var last []model.BookDelta
	for range 10 {
		d := e.next()
		tr.Apply(d)
		last = append(last, d)
		if tr.Buffered() > 4 {
			t.Fatalf("Buffered() = %d, over the capacity of 4", tr.Buffered())
		}
	}
	if got := tr.Stats().Dropped; got != 6 {
		t.Errorf("Dropped = %d, want 6", got)
	}
	// The four newest are the ones kept: dropping the oldest is what keeps
	// the held deltas contiguous with each other.
	if got, want := tr.buf, last[len(last)-4:]; !slices.EqualFunc(got, want, func(a, b model.BookDelta) bool {
		return a.FirstID == b.FirstID && a.LastID == b.LastID
	}) {
		t.Errorf("buffer holds %v, want the last four deltas %v", ids(got), ids(want))
	}

	if err := tr.Load(e.snapshot()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !tr.Live() {
		t.Fatal("not Live: a snapshot newer than every held delta must anchor the book")
	}
	assertMatches(t, tr.Book(), e.snapshot())
}

func ids(ds []model.BookDelta) [][2]int64 {
	out := make([][2]int64, len(ds))
	for i, d := range ds {
		out[i] = [2]int64{d.FirstID, d.LastID}
	}
	return out
}

func TestTrackerRejectsForeignSymbol(t *testing.T) {
	tr := NewTracker(testSymbol, 0)
	tr.BeginFetch()
	if err := tr.Load(model.Snapshot{Symbol: "ETH-USDT", LastID: 10}); err == nil {
		t.Error("Load accepted a snapshot for another symbol")
	}
	if err := tr.Load(model.Snapshot{Symbol: testSymbol, LastID: 10}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := tr.Apply(model.BookDelta{Symbol: "ETH-USDT", FirstID: 11, LastID: 11}); err == nil {
		t.Error("Apply accepted a delta for another symbol")
	}
	if !tr.Live() {
		t.Error("a misrouted delta invalidated the book")
	}
}

// TestTrackerInvalidLevels covers the two places a level that cannot exist can
// enter, and asserts that neither leaves the book quietly wrong.
func TestTrackerInvalidLevels(t *testing.T) {
	t.Run("in a snapshot", func(t *testing.T) {
		tr := NewTracker(testSymbol, 0)
		tr.BeginFetch()
		err := tr.Load(model.Snapshot{
			Symbol: testSymbol, LastID: 10,
			Bids: levels(100, 1, 100, 2), // duplicate price
		})
		if !errors.Is(err, ErrDuplicatePrice) {
			t.Fatalf("Load = %v, want %v", err, ErrDuplicatePrice)
		}
		if tr.Live() {
			t.Error("Live after a rejected snapshot")
		}
		if tr.Book().BidDepth() != 0 {
			t.Error("a rejected snapshot reached the book")
		}
		if !tr.BeginFetch() {
			t.Error("BeginFetch is false after a rejected snapshot")
		}
	})

	t.Run("in a delta", func(t *testing.T) {
		tr := NewTracker(testSymbol, 0)
		tr.BeginFetch()
		if err := tr.Load(model.Snapshot{Symbol: testSymbol, LastID: 10, Bids: levels(100, 1)}); err != nil {
			t.Fatalf("Load: %v", err)
		}
		_, err := tr.Apply(model.BookDelta{
			Symbol: testSymbol, FirstID: 11, LastID: 11,
			Bids: levels(99, -1), // negative quantity
		})
		if !errors.Is(err, ErrNegativeQty) {
			t.Fatalf("Apply = %v, want %v", err, ErrNegativeQty)
		}
		// The sequencer moved past the delta, so the book is missing an
		// update whatever happens next. It must not claim to be current.
		if tr.Live() {
			t.Error("Live after a delta that advanced the sequence but never reached the book")
		}
		if tr.Book().BidDepth() != 1 {
			t.Error("a rejected delta reached the book")
		}
	})
}

func TestStatusString(t *testing.T) {
	for _, tt := range []struct {
		s    Status
		want string
	}{
		{StatusApplied, "applied"},
		{StatusDiscarded, "discarded"},
		{StatusBuffered, "buffered"},
		{StatusGapped, "gapped"},
		{Status(0), "invalid"},
	} {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("Status(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
