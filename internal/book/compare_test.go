package book

import (
	"slices"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

// tracked returns a tracker anchored on s, so a comparison has something to
// compare against.
func tracked(t *testing.T, s model.Snapshot) *Tracker {
	t.Helper()
	tr := NewTracker(s.Symbol, 0)
	tr.BeginFetch()
	if err := tr.Load(s); err != nil {
		t.Fatalf("Load: %v", err)
	}
	return tr
}

func snap(lastID int64, truncated bool, bids, asks []model.Level) model.Snapshot {
	return model.Snapshot{
		Symbol: testSymbol, LastID: lastID, Truncated: truncated,
		Bids: bids, Asks: asks,
	}
}

func TestCompareAgrees(t *testing.T) {
	s := snap(100, false, levels(99, 5, 98, 3, 97, 1), levels(101, 4, 102, 2))
	tr := tracked(t, s)

	d, err := tr.Compare(s)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if !d.OK() {
		t.Errorf("book diverges from the snapshot it was built from: %v", d.Levels)
	}
	if d.Compared != 5 {
		t.Errorf("Compared = %d, want 5", d.Compared)
	}
	if !d.Live || d.LocalID != 100 || d.SnapshotID != 100 || d.IDSkew() != 0 {
		t.Errorf("diff = %+v", d)
	}
	if d.BidFloor != 0 || d.AskCeil != 0 {
		t.Errorf("untruncated snapshots bounded the comparison: floor %d ceil %d", d.BidFloor, d.AskCeil)
	}
}

// TestCompareDiverges covers the three shapes a divergence takes: a level only
// the book has, a level only the snapshot has, and a level both have at
// different quantities.
func TestCompareDiverges(t *testing.T) {
	tr := tracked(t, snap(100, false, levels(99, 5, 98, 3), levels(101, 4, 103, 9)))

	fresh := snap(100, false,
		levels(99, 7 /* quantity differs */, 98, 3),
		levels(101, 4, 102, 6 /* only the snapshot has it */),
		// 103 is only in the book
	)
	d, err := tr.Compare(fresh)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	want := []LevelDiff{
		{Side: Bid, Price: 99, Local: 5, Remote: 7},
		{Side: Ask, Price: 102, Remote: 6},
		{Side: Ask, Price: 103, Local: 9},
	}
	if !slices.Equal(d.Levels, want) {
		t.Errorf("divergences:\n got %v\nwant %v", d.Levels, want)
	}
	if d.OK() {
		t.Error("OK reports agreement on a diverging book")
	}
	if d.Compared != 5 {
		t.Errorf("Compared = %d, want 5", d.Compared)
	}
}

// TestCompareRespectsSnapshotTruncation is the false positive D25 exists to
// prevent: a fresh snapshot that reaches deeper than the book was ever told
// about must not report every level below that edge as missing.
func TestCompareRespectsSnapshotTruncation(t *testing.T) {
	// The book was seeded from a truncated snapshot reaching down to 98 on
	// the bid side and up to 102 on the ask side.
	tr := tracked(t, snap(100, true, levels(99, 5, 98, 3), levels(101, 4, 102, 2)))

	// The fresh snapshot reaches further on both sides. The extra levels are
	// real, and the book never had any way to know about them.
	fresh := snap(100, true,
		levels(99, 5, 98, 3, 97, 8, 96, 1),
		levels(101, 4, 102, 2, 103, 7),
	)
	d, err := tr.Compare(fresh)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if !d.OK() {
		t.Errorf("levels below the book's knowledge reported as divergence: %v", d.Levels)
	}
	if d.BidFloor != 98 || d.AskCeil != 102 {
		t.Errorf("bounds = floor %d ceil %d, want the book's own edges 98 and 102", d.BidFloor, d.AskCeil)
	}
	if d.Compared != 4 {
		t.Errorf("Compared = %d, want the 4 levels inside the bounds", d.Compared)
	}
}

// TestCompareRespectsBookDepth is the mirror case: the book knows deeper than
// the fresh snapshot reports, so the snapshot's edge is the bound.
func TestCompareRespectsBookDepth(t *testing.T) {
	tr := tracked(t, snap(100, false, levels(99, 5, 98, 3, 97, 8), levels(101, 4, 102, 2, 103, 7)))

	fresh := snap(100, true, levels(99, 5), levels(101, 4))
	d, err := tr.Compare(fresh)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if !d.OK() {
		t.Errorf("levels below the snapshot's cut reported as divergence: %v", d.Levels)
	}
	if d.BidFloor != 99 || d.AskCeil != 101 {
		t.Errorf("bounds = floor %d ceil %d, want the snapshot's edges 99 and 101", d.BidFloor, d.AskCeil)
	}
	if d.Compared != 2 {
		t.Errorf("Compared = %d, want 2", d.Compared)
	}
}

// TestCompareInsideBoundsStillReported guards the obvious way the bounds could
// be got wrong: silencing divergences that fall inside the compared range.
func TestCompareInsideBoundsStillReported(t *testing.T) {
	tr := tracked(t, snap(100, true, levels(99, 5, 98, 3), levels(101, 4, 102, 2)))

	fresh := snap(100, true,
		levels(99, 5, 98, 3, 97, 1), // 98 agrees, 97 is out of bounds
		levels(101, 4, 102, 6),      // 102 is at the bound and disagrees
	)
	d, err := tr.Compare(fresh)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	want := []LevelDiff{{Side: Ask, Price: 102, Local: 2, Remote: 6}}
	if !slices.Equal(d.Levels, want) {
		t.Errorf("divergences:\n got %v\nwant %v", d.Levels, want)
	}
}

// TestCompareAfterLiveUpdates is the harness doing its actual job: a book kept
// current by deltas is checked against the venue and found to agree, and the
// same check catches a book that was quietly corrupted.
func TestCompareAfterLiveUpdates(t *testing.T) {
	e := newExchange(23)
	tr := tracked(t, e.snapshot())
	for range 200 {
		if st, err := tr.Apply(e.next()); err != nil || st != StatusApplied {
			t.Fatalf("Apply = %v, %v", st, err)
		}
	}

	d, err := tr.Compare(e.snapshot())
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if !d.OK() {
		t.Errorf("a correctly maintained book diverged: %v", d)
	}
	if d.Compared < 50 {
		t.Errorf("Compared = %d, too few levels for the comparison to mean anything", d.Compared)
	}

	// Corrupt one level behind the tracker's back, exactly as a lost delta
	// would, and confirm the harness sees it.
	tr.book.bids.levels[3].Qty += 1
	corrupted := tr.book.bids.levels[3]
	d, err = tr.Compare(e.snapshot())
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	want := []LevelDiff{{Side: Bid, Price: corrupted.Price, Local: corrupted.Qty, Remote: corrupted.Qty - 1}}
	if !slices.Equal(d.Levels, want) {
		t.Errorf("divergences:\n got %v\nwant %v", d.Levels, want)
	}
}

func TestCompareRejectsBadSnapshot(t *testing.T) {
	tr := tracked(t, snap(100, false, levels(99, 5), levels(101, 4)))

	if _, err := tr.Compare(model.Snapshot{Symbol: "ETH-USDT"}); err == nil {
		t.Error("Compare accepted a snapshot for another symbol")
	}
	if _, err := tr.Compare(snap(100, false, levels(99, 5, 99, 6), nil)); err == nil {
		t.Error("Compare accepted a snapshot with a duplicate price")
	}
}

func TestCompareReportsStaleBook(t *testing.T) {
	tr := NewTracker(testSymbol, 0)
	d, err := tr.Compare(snap(100, false, levels(99, 5), levels(101, 4)))
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if d.Live {
		t.Error("a book with no snapshot reported Live")
	}
	if d.OK() {
		t.Error("an empty book agreed with a non-empty snapshot")
	}
}

func TestSideString(t *testing.T) {
	for _, tt := range []struct {
		s    Side
		want string
	}{{Bid, "bid"}, {Ask, "ask"}, {Side(0), "invalid"}} {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("Side(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
