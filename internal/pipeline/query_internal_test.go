package pipeline

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/metrics"
	"github.com/zuniverse/market-stream/internal/model"
)

// checkView asserts the properties every answer must have, whatever the book
// was doing when it was read. A view is a copy taken between two deltas, so it
// is a state the book actually held, never a half-applied one.
func checkView(t *testing.T, v BookView, depth int) {
	t.Helper()
	if len(v.Bids) > depth || len(v.Asks) > depth {
		t.Errorf("%s: view holds %d bids and %d asks, over the requested depth %d",
			v.Symbol, len(v.Bids), len(v.Asks), depth)
	}
	if len(v.Bids) > v.BidDepth || len(v.Asks) > v.AskDepth {
		t.Errorf("%s: view holds more levels than the side has: %d of %d bids, %d of %d asks",
			v.Symbol, len(v.Bids), v.BidDepth, len(v.Asks), v.AskDepth)
	}
	for i := 1; i < len(v.Bids); i++ {
		if v.Bids[i].Price >= v.Bids[i-1].Price {
			t.Errorf("%s: bids not descending at %d: %v", v.Symbol, i, v.Bids)
			break
		}
	}
	for i := 1; i < len(v.Asks); i++ {
		if v.Asks[i].Price <= v.Asks[i-1].Price {
			t.Errorf("%s: asks not ascending at %d: %v", v.Symbol, i, v.Asks)
			break
		}
	}
	for _, l := range slices.Concat(v.Bids, v.Asks) {
		if l.Qty == 0 {
			t.Errorf("%s: view holds an empty level at %d", v.Symbol, l.Price)
		}
	}
	if v.Live && v.Crossed {
		t.Errorf("%s: a live book is crossed: %v / %v", v.Symbol, v.Bids, v.Asks)
	}
	if v.Live && v.Buffered != 0 {
		t.Errorf("%s: a live book holds %d buffered deltas", v.Symbol, v.Buffered)
	}
}

func TestQueryReadsBook(t *testing.T) {
	syms := testSymbols(4)
	v := newVenue(syms, 41)
	r := newTestRouter(t, RouterConfig{Shards: 2, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewSource(41))
	for range 20 {
		for _, sym := range syms {
			if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
				t.Fatalf("Route: %v", err)
			}
		}
	}
	waitFor(t, "the books to sync", func() bool { return r.Stats().Snapshots == uint64(len(syms)) })

	for _, sym := range syms {
		view, err := r.Query(ctx, sym, 5)
		if err != nil {
			t.Fatalf("Query %s: %v", sym, err)
		}
		checkView(t, view, 5)
		if view.Symbol != sym {
			t.Errorf("view is for %s, want %s", view.Symbol, sym)
		}
		if !view.Live || view.LastID == 0 {
			t.Errorf("%s: view = %+v, want a live book with an applied id", sym, view)
		}
		if view.BidDepth == 0 || view.AskDepth == 0 {
			t.Errorf("%s: view reports an empty side: %+v", sym, view)
		}
		if view.Stats.Applied == 0 && view.Stats.Discarded == 0 {
			t.Errorf("%s: per-symbol stats are empty: %+v", sym, view.Stats)
		}
	}

	// depth of zero is the health check form: state, no levels.
	view, err := r.Query(ctx, syms[0], 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(view.Bids) != 0 || len(view.Asks) != 0 {
		t.Errorf("depth 0 returned %d bids and %d asks", len(view.Bids), len(view.Asks))
	}
	if view.BidDepth == 0 {
		t.Error("depth 0 dropped the side counts too")
	}
}

// TestQueryReturnsCopies is the property that lets a read cross a goroutine
// boundary at all: the caller gets a copy, so nothing it does can reach the
// book (D4).
func TestQueryReturnsCopies(t *testing.T) {
	syms := testSymbols(1)
	sym := syms[0]
	v := newVenue(syms, 43)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewSource(43))
	if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitFor(t, "the book to sync", func() bool { return r.Stats().Snapshots == 1 })

	first, err := r.Query(ctx, sym, 10)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := slices.Clone(first.Bids)
	for i := range first.Bids {
		first.Bids[i].Qty = 999999
	}

	second, err := r.Query(ctx, sym, 10)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !slices.Equal(second.Bids, want) {
		t.Errorf("mutating a view changed the book:\n got %v\nwant %v", second.Bids, want)
	}
}

func TestQueryUnknownSymbol(t *testing.T) {
	v := newVenue(testSymbols(1), 47)
	r := newTestRouter(t, RouterConfig{Shards: 2, QueueCap: 4, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.Query(ctx, "NOPE-USDT", 5); !errors.Is(err, ErrUnknownSymbol) {
		t.Errorf("Query for an untracked symbol = %v, want %v", err, ErrUnknownSymbol)
	}
	// A read must not start tracking anything.
	if st := r.Stats(); st.Events != 0 {
		t.Errorf("a query created work: %+v", st)
	}
}

// TestQueryRespectsCancellation is the requirement that a caller which gives
// up never blocks the shard: no shard is running here, so the request can
// never be taken, and Query must still return.
func TestQueryRespectsCancellation(t *testing.T) {
	v := newVenue(testSymbols(1), 53)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 4, Snapshots: v})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Query(ctx, testSymbols(1)[0], 5)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Query returned %v before the shard could take it, want it to wait", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Query = %v, want context.Canceled", err)
	}
	r.Close()
}

func TestQueryAfterClose(t *testing.T) {
	syms := testSymbols(1)
	v := newVenue(syms, 59)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 4, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Close()

	if _, err := r.Query(ctx, syms[0], 5); !errors.Is(err, ErrRouterClosed) {
		t.Errorf("Query after Close = %v, want %v", err, ErrRouterClosed)
	}
}

// TestQueryDuringLiveUpdates is the M2.6 done criterion: concurrent readers
// against books that are being updated the whole time, under the race
// detector, with no lock anywhere on book state.
func TestQueryDuringLiveUpdates(t *testing.T) {
	const (
		producers = 3
		readers   = 6
		perSymbol = 200
	)
	syms := testSymbols(12)
	v := newVenue(syms, 61)
	r := newTestRouter(t, RouterConfig{Shards: 3, QueueCap: 16, Snapshots: v})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var writers sync.WaitGroup
	for p := range producers {
		writers.Add(1)
		go func() {
			defer writers.Done()
			rng := rand.New(rand.NewSource(int64(200 + p)))
			mine := syms[p*len(syms)/producers : (p+1)*len(syms)/producers]
			for i := range perSymbol {
				for _, sym := range mine {
					d := v.next(sym, rng)
					if i%53 == 52 {
						continue // dropped, so a resync runs while queries are in flight
					}
					if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: d}); err != nil {
						t.Errorf("Route: %v", err)
						return
					}
				}
			}
		}()
	}

	// Readers run until the producers are done, then one final sweep each so
	// that the last state is read too.
	stop := make(chan struct{})
	var reading sync.WaitGroup
	var answered atomic.Int64
	for range readers {
		reading.Add(1)
		go func() {
			defer reading.Done()
			for {
				for _, sym := range syms {
					view, err := r.Query(ctx, sym, 8)
					if errors.Is(err, ErrUnknownSymbol) {
						continue // not seen by its shard yet
					}
					if err != nil {
						t.Errorf("Query %s: %v", sym, err)
						return
					}
					checkView(t, view, 8)
					answered.Add(1)
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	reading.Wait()

	waitFor(t, "the shards to go quiet", func() bool {
		before := r.Stats()
		time.Sleep(10 * time.Millisecond)
		return before == r.Stats() && queuesEmpty(r)
	})
	r.Close()

	assertBooksMatch(t, r, v)
	st := r.Stats()
	if st.Queries == 0 {
		t.Error("no query was answered")
	}
	if uint64(answered.Load()) > st.Queries {
		t.Errorf("Queries = %d, fewer than the %d answers the readers received", st.Queries, answered.Load())
	}
	if st.Gaps == 0 {
		t.Error("no gap was detected, so no query ran against a resyncing book")
	}
	t.Logf("stats: %+v, views checked: %d", st, answered.Load())
}

// TestCheckComparesAgainstVenue drives the correctness harness the way ingestd
// does: fetch a fresh snapshot, hand it to the shard that owns the book, and
// read back the divergence report.
func TestCheckComparesAgainstVenue(t *testing.T) {
	syms := testSymbols(2)
	v := newVenue(syms, 67)
	r := newTestRouter(t, RouterConfig{Shards: 2, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewSource(67))
	const perSymbol = 50
	for range perSymbol {
		for _, sym := range syms {
			if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
				t.Fatalf("Route: %v", err)
			}
		}
	}
	// Both conditions are needed. A snapshot anchors the book, but a delta
	// still queued is a delta the venue has already applied, and comparing
	// then reports a divergence at a skew of one that is the test's own
	// timing rather than a defect.
	waitFor(t, "the books to sync and the queues to drain", func() bool {
		st := r.Stats()
		return st.Snapshots == uint64(len(syms)) && st.Events == uint64(perSymbol*len(syms))
	})

	for _, sym := range syms {
		diff, err := r.Check(ctx, v.state(sym))
		if err != nil {
			t.Fatalf("Check %s: %v", sym, err)
		}
		if !diff.OK() {
			t.Errorf("%s: %v", sym, diff)
		}
		if !diff.Live || diff.Compared == 0 {
			t.Errorf("%s: nothing was actually compared: %+v", sym, diff)
		}
	}

	// A snapshot that has moved on must be reported rather than smoothed over.
	sym := syms[0]
	for range 5 {
		v.next(sym, rng) // the venue advances, the book is not told
	}
	diff, err := r.Check(ctx, v.state(sym))
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if diff.OK() {
		t.Error("Check agreed with a book that had missed five updates")
	}
	if diff.IDSkew() <= 0 {
		t.Errorf("IDSkew() = %d, want the snapshot ahead of the book", diff.IDSkew())
	}

	if _, err := r.Check(ctx, model.Snapshot{Symbol: "NOPE-USDT"}); !errors.Is(err, ErrUnknownSymbol) {
		t.Errorf("Check for an untracked symbol = %v, want %v", err, ErrUnknownSymbol)
	}
}

// TestRouterObservesLatencies covers the two measurements the book stage is
// the only place that can take: how old a delta is when it reaches a book,
// and how long a resync took (D43).
func TestRouterObservesLatencies(t *testing.T) {
	tickToBook := metrics.NewHistogram("tick", "h", []time.Duration{
		time.Millisecond, 10 * time.Millisecond, time.Second, time.Hour,
	})
	resync := metrics.NewHistogram("resync", "h", []time.Duration{
		time.Millisecond, 10 * time.Millisecond, time.Second,
	})

	syms := testSymbols(1)
	sym := syms[0]
	v := newVenue(syms, 71)
	r := newTestRouter(t, RouterConfig{
		Shards: 1, QueueCap: 8, Snapshots: v,
		TickToBook: tickToBook, Resync: resync,
	})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	rng := rand.New(rand.NewSource(71))
	now := time.Now()
	for i := range 30 {
		d := v.next(sym, rng)
		ev := model.Event{
			Kind:         model.KindBookDelta,
			BookDelta:    d,
			ExchangeTime: now.Add(-time.Duration(i) * time.Millisecond).UnixNano(),
		}
		if err := r.Route(ctx, ev); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}
	waitFor(t, "the deltas to be applied", func() bool {
		return r.Stats().Applied > 0 && r.Stats().Snapshots == 1
	})
	r.Close()

	if got := tickToBook.Count(); got == 0 {
		t.Error("no tick-to-book observation was taken")
	}
	if got := tickToBook.Negative(); got != 0 {
		t.Errorf("%d observations were negative, but every event was stamped in the past", got)
	}
	if got := resync.Count(); got != 1 {
		t.Errorf("resync observations = %d, want 1 for the initial anchoring", got)
	}

	// An event with no exchange time must not be observed as an enormous
	// latency measured from the Unix epoch.
	if p99 := tickToBook.Quantile(0.99); p99 > time.Second {
		t.Errorf("p99 = %v, want it in the millisecond range", p99)
	}
}

// TestRouterWithoutHistogramsIsUnaffected covers the nil case, which is what
// cmd/replay uses: measuring latency against a recorded timestamp would
// report how long ago the recording was made.
func TestRouterWithoutHistogramsIsUnaffected(t *testing.T) {
	syms := testSymbols(1)
	v := newVenue(syms, 73)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rng := rand.New(rand.NewSource(73))
	for range 5 {
		ev := model.Event{Kind: model.KindBookDelta, BookDelta: v.next(syms[0], rng), ExchangeTime: 1}
		if err := r.Route(ctx, ev); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}
	waitFor(t, "the book to sync", func() bool { return r.Stats().Snapshots == 1 })
	r.Close()
	assertBooksMatch(t, r, v)
}
