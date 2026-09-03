package book_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zuniverse/market-stream/internal/book"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

// referenceSnapshot returns the first snapshot in the committed capture: a
// real book, five thousand levels a side, which is the shape the cost of
// every operation below depends on. A book built from a handful of synthetic
// levels would make the binary search look free and the insert memmove
// invisible, which are the two things D23 said M4 should measure.
func referenceSnapshot(tb testing.TB) model.Snapshot {
	tb.Helper()
	path := filepath.Join("..", "..", "docs", "baseline", "reference.msr.zst")
	f, err := os.Open(path)
	if err != nil {
		tb.Skipf("reference recording unavailable: %v", err)
	}
	defer f.Close()

	r, err := record.NewReader(f)
	if err != nil {
		tb.Fatalf("read reference: %v", err)
	}
	defer r.Close()

	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			tb.Fatal("the reference recording holds no snapshot")
		}
		if err != nil {
			tb.Fatalf("read reference: %v", err)
		}
		if rec.Kind == record.KindSnapshot {
			return rec.Snapshot
		}
	}
}

// loadedBook returns a book holding the reference snapshot.
func loadedBook(tb testing.TB) (*book.Book, model.Snapshot) {
	tb.Helper()
	snap := referenceSnapshot(tb)
	b := book.New(snap.Symbol)
	if err := b.Reset(snap.Bids, snap.Asks); err != nil {
		tb.Fatalf("Reset: %v", err)
	}
	return b, snap
}

// sortedBids returns the bid side in book order, top first, so a benchmark
// can pick a level by its distance from the touch.
func sortedBids(tb testing.TB, b *book.Book) []model.Level {
	tb.Helper()
	levels := b.Bids(b.BidDepth())
	if len(levels) < 4500 {
		tb.Fatalf("the reference book holds %d bid levels, too few to measure depth with", len(levels))
	}
	return levels
}

func BenchmarkReset(b *testing.B) {
	snap := referenceSnapshot(b)
	bk := book.New(snap.Symbol)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := bk.Reset(snap.Bids, snap.Asks); err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkUpdate changes the quantity at an existing level, which is the
// commonest delta and the one that moves no memory: a binary search and a
// store.
func benchmarkUpdate(b *testing.B, depth int) {
	bk, _ := loadedBook(b)
	levels := sortedBids(b, bk)
	target := levels[depth]
	upd := []model.Level{{Price: target.Price, Qty: target.Qty}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		upd[0].Qty = target.Qty + model.Qty(i%2)
		if err := bk.Apply(upd, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateTopOfBook(b *testing.B) { benchmarkUpdate(b, 0) }
func BenchmarkUpdateNearTouch(b *testing.B) { benchmarkUpdate(b, 20) }
func BenchmarkUpdateDeep(b *testing.B)      { benchmarkUpdate(b, 4000) }

// benchmarkDeleteInsert removes an existing level and puts it back, so the
// book returns to its original size every iteration. The figure covers both
// operations, and both pay the slice memmove whose cost is what D23 said this
// milestone should measure: how much the shifting costs, and whether it
// depends on distance from the touch.
func benchmarkDeleteInsert(b *testing.B, depth int) {
	bk, _ := loadedBook(b)
	levels := sortedBids(b, bk)
	target := levels[depth]
	del := []model.Level{{Price: target.Price, Qty: 0}}
	add := []model.Level{{Price: target.Price, Qty: target.Qty}}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := bk.Apply(del, nil); err != nil {
			b.Fatal(err)
		}
		if err := bk.Apply(add, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeleteInsertTopOfBook(b *testing.B) { benchmarkDeleteInsert(b, 0) }
func BenchmarkDeleteInsertNearTouch(b *testing.B) { benchmarkDeleteInsert(b, 20) }
func BenchmarkDeleteInsertDeep(b *testing.B)      { benchmarkDeleteInsert(b, 4000) }

// BenchmarkBids20 is the hot read: the first twenty levels of a side, which
// on a sorted slice is a prefix and needs no search at all (D23).
func BenchmarkBids20(b *testing.B) {
	bk, _ := loadedBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = bk.Bids(20)
	}
}

func BenchmarkBestBid(b *testing.B) {
	bk, _ := loadedBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := bk.BestBid(); !ok {
			b.Fatal("empty book")
		}
	}
}

func BenchmarkCrossed(b *testing.B) {
	bk, _ := loadedBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = bk.Crossed()
	}
}

// BenchmarkSequencerNext measures the classification every delta pays before
// it reaches a book.
func BenchmarkSequencerNext(b *testing.B) {
	var s book.Sequencer
	s.Reset(1000)
	d := model.BookDelta{Symbol: "BTC-USDT"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		d.FirstID = int64(1001 + i)
		d.LastID = d.FirstID
		if got := s.Next(d); got != book.Contiguous {
			b.Fatalf("classification = %v", got)
		}
	}
}
