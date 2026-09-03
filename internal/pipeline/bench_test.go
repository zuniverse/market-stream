package pipeline_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
	"github.com/zuniverse/market-stream/internal/record"
)

// referenceSnapshot returns the first snapshot in the committed capture, so
// the benchmarks below run against a real five thousand level book rather
// than a toy one.
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

// fixedSnapshotter answers every request with the same snapshot.
type fixedSnapshotter struct{ snap model.Snapshot }

func (s fixedSnapshotter) Snapshot(context.Context, model.Symbol) (model.Snapshot, error) {
	return s.snap, nil
}

// benchRouter returns a started router whose book for snap.Symbol is anchored
// and live, and the delta id to continue from.
func benchRouter(tb testing.TB, shards int, snap model.Snapshot) (*pipeline.Router, int64) {
	tb.Helper()
	r, err := pipeline.NewRouter(pipeline.RouterConfig{
		Shards:    shards,
		QueueCap:  1024,
		Snapshots: fixedSnapshotter{snap},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		tb.Fatalf("NewRouter: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		tb.Fatalf("Start: %v", err)
	}

	// One delta creates the book and triggers the anchoring fetch.
	first := model.Event{Kind: model.KindBookDelta, BookDelta: model.BookDelta{
		Symbol: snap.Symbol, FirstID: snap.LastID + 1, LastID: snap.LastID + 1,
	}}
	if err := r.Route(context.Background(), first); err != nil {
		tb.Fatalf("Route: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.Stats().Snapshots == 0 {
		if time.Now().After(deadline) {
			r.Close()
			tb.Fatal("the book never anchored")
		}
		time.Sleep(time.Millisecond)
	}
	return r, snap.LastID + 1
}

// waitApplied blocks until the shards have applied n deltas.
func waitApplied(tb testing.TB, r *pipeline.Router, n uint64) {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for r.Stats().Applied < n {
		if time.Now().After(deadline) {
			tb.Fatalf("only %d of %d deltas were applied", r.Stats().Applied, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// benchmarkRouteApply measures one delta all the way through the book stage:
// the channel hop to the owning shard, the sequence classification, and the
// book update. It is the pipeline cost of a delta once it has been decoded,
// so the difference between this and the decoder benchmark is the whole
// argument for where an optimisation should go.
func benchmarkRouteApply(b *testing.B, shards, depth int) {
	snap := referenceSnapshot(b)
	r, nextID := benchRouter(b, shards, snap)
	defer r.Close()

	// A level that exists, so every delta is an update rather than an
	// insertion: the shape of the commonest delta on the wire.
	bids := snap.Bids
	if len(bids) <= depth {
		b.Skipf("the reference book holds %d bid levels", len(bids))
	}
	target := bids[depth]

	ctx := context.Background()
	before := r.Stats().Applied
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		id := nextID + int64(i) + 1
		ev := model.Event{Kind: model.KindBookDelta, BookDelta: model.BookDelta{
			Symbol:  snap.Symbol,
			FirstID: id,
			LastID:  id,
			Bids:    []model.Level{{Price: target.Price, Qty: target.Qty + model.Qty(i%2)}},
		}}
		if err := r.Route(ctx, ev); err != nil {
			b.Fatal(err)
		}
	}
	waitApplied(b, r, before+uint64(b.N))
	b.StopTimer()
}

// The reference snapshot is sorted top first, so index 0 is the touch. The
// depth matters: a book update near the touch shifts more of the slice than
// one deep in it, which is the finding this milestone recorded.
func BenchmarkRouteApplyTopOfBook(b *testing.B) { benchmarkRouteApply(b, 1, 0) }
func BenchmarkRouteApplyDeep(b *testing.B)      { benchmarkRouteApply(b, 1, 4000) }

// BenchmarkRouteApplyShards4 is the same work with four shard goroutines and
// one symbol, which cannot use them: sharding parallelises across symbols,
// never within one (D3). The figure is here to show what the extra goroutines
// cost when they cannot help.
func BenchmarkRouteApplyShards4(b *testing.B) { benchmarkRouteApply(b, 4, 20) }

// BenchmarkQuery measures a book read through the shard that owns it: the
// request, the copy of twenty levels, and the reply (D31).
func BenchmarkQuery(b *testing.B) {
	snap := referenceSnapshot(b)
	r, _ := benchRouter(b, 1, snap)
	defer r.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := r.Query(ctx, snap.Symbol, 20); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublish measures the lossy fan-out: one event to one subscriber
// that does nothing. It is the floor on what the publisher costs, and it is
// deliberately compared against the book stage rather than against the
// decoder, since the two are the halves of the backpressure argument (D29).
func BenchmarkPublish(b *testing.B) {
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(nopSubscriber{}, 1024); err != nil {
		b.Fatal(err)
	}
	if err := pub.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer pub.Close()

	ev := model.Event{Kind: model.KindTrade, Trade: model.Trade{Symbol: "BTC-USDT", Price: 1, Qty: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pub.Publish(ev)
	}
}

type nopSubscriber struct{}

func (nopSubscriber) Name() string                         { return "bench-nop" }
func (nopSubscriber) Consume(context.Context, model.Event) {}
