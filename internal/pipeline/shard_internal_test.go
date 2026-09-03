package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/book"
	"github.com/zuniverse/market-stream/internal/model"
)

// These tests are in package pipeline rather than pipeline_test because they
// read the books after Close. That is safe and it is the only read there is
// until M2.6 builds the query path: Close waits for every shard goroutine to
// return, which orders their writes before the test's reads.

// venue is a stand-in exchange. It holds the authoritative books, hands out
// the deltas describing every change it makes, and serves snapshots of its
// current state. Its lock is the network boundary, not book state: the
// snapshot calls arrive on the shards' fetch goroutines.
type venue struct {
	mu    sync.Mutex
	books map[model.Symbol]*venueBook

	// failuresLeft makes the next N snapshot requests fail, for the retry
	// path. It is decremented from the fetch goroutines.
	failuresLeft atomic.Int64
	calls        atomic.Uint64
}

type venueBook struct {
	bids, asks map[model.Price]model.Qty
	lastID     int64
}

func newVenue(symbols []model.Symbol, seed int64) *venue {
	rng := rand.New(rand.NewSource(seed))
	v := &venue{books: make(map[model.Symbol]*venueBook, len(symbols))}
	for _, sym := range symbols {
		b := &venueBook{
			bids:   make(map[model.Price]model.Qty),
			asks:   make(map[model.Price]model.Qty),
			lastID: 1000,
		}
		for p := model.Price(900); p < 1000; p += 5 {
			b.bids[p] = model.Qty(1 + rng.Intn(9))
		}
		for p := model.Price(1000); p < 1100; p += 5 {
			b.asks[p] = model.Qty(1 + rng.Intn(9))
		}
		v.books[sym] = b
	}
	return v
}

// next advances one symbol and returns the delta describing the change. It
// must be called from a single goroutine per symbol, so that the order deltas
// are generated in is the order they are routed in.
func (v *venue) next(sym model.Symbol, rng *rand.Rand) model.BookDelta {
	v.mu.Lock()
	defer v.mu.Unlock()
	b := v.books[sym]
	d := model.BookDelta{Symbol: sym, FirstID: b.lastID + 1}
	for n := 1 + rng.Intn(2); n > 0; n-- {
		qty := model.Qty(rng.Intn(5)) // zero deletes the level
		if rng.Intn(2) == 0 {
			p := model.Price(900 + rng.Intn(100))
			setLevel(b.bids, p, qty)
			d.Bids = append(d.Bids, model.Level{Price: p, Qty: qty})
		} else {
			p := model.Price(1000 + rng.Intn(100))
			setLevel(b.asks, p, qty)
			d.Asks = append(d.Asks, model.Level{Price: p, Qty: qty})
		}
	}
	b.lastID += int64(1 + rng.Intn(2))
	d.LastID = b.lastID
	return d
}

func setLevel(side map[model.Price]model.Qty, p model.Price, q model.Qty) {
	if q == 0 {
		delete(side, p)
		return
	}
	side[p] = q
}

// Snapshot implements Snapshotter.
func (v *venue) Snapshot(ctx context.Context, sym model.Symbol) (model.Snapshot, error) {
	v.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return model.Snapshot{}, err
	}
	if v.failuresLeft.Add(-1) >= 0 {
		return model.Snapshot{}, errors.New("venue: snapshot unavailable")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snapshotLocked(sym), nil
}

func (v *venue) state(sym model.Symbol) model.Snapshot {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snapshotLocked(sym)
}

func (v *venue) snapshotLocked(sym model.Symbol) model.Snapshot {
	b, ok := v.books[sym]
	if !ok {
		return model.Snapshot{Symbol: sym}
	}
	s := model.Snapshot{Symbol: sym, LastID: b.lastID}
	for p, q := range b.bids {
		s.Bids = append(s.Bids, model.Level{Price: p, Qty: q})
	}
	for p, q := range b.asks {
		s.Asks = append(s.Asks, model.Level{Price: p, Qty: q})
	}
	return s
}

func testSymbols(n int) []model.Symbol {
	out := make([]model.Symbol, n)
	for i := range out {
		out[i] = model.Symbol(fmt.Sprintf("SYM%02d-USDT", i))
	}
	return out
}

func newTestRouter(t *testing.T, cfg RouterConfig) *Router {
	t.Helper()
	if cfg.QueueCap == 0 {
		cfg.QueueCap = 64
	}
	r, err := NewRouter(cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// waitFor polls cond until it holds or the deadline passes. The book stage is
// asynchronous by construction, since a snapshot fetch runs on its own
// goroutine, so there is no state to synchronise on other than the counters.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// assertBooksMatch checks every book the router holds against the venue. It
// must be called after Close.
func assertBooksMatch(t *testing.T, r *Router, v *venue) {
	t.Helper()
	var checked int
	for _, s := range r.shards {
		for sym, tr := range s.books {
			checked++
			if !tr.Live() {
				t.Errorf("%s: book is not live at shutdown", sym)
				continue
			}
			d, err := tr.Compare(v.state(sym))
			if err != nil {
				t.Fatalf("%s: Compare: %v", sym, err)
			}
			if !d.OK() {
				t.Errorf("%s diverged from the venue: %v", sym, d)
			}
		}
	}
	if checked != len(v.books) {
		t.Errorf("router holds %d books, want %d", checked, len(v.books))
	}
}

func TestShardIndexIsStableAndSpread(t *testing.T) {
	// Fixed values: the assignment must not change between runs, or two
	// replays of one recording would not be comparable (D5).
	for _, tc := range []struct {
		sym  model.Symbol
		n    int
		want int
	}{
		{"BTC-USDT", 4, shardIndex("BTC-USDT", 4)},
		{"BTC-USDT", 1, 0},
		{"ETH-USDT", 1, 0},
	} {
		if got := shardIndex(tc.sym, tc.n); got != tc.want {
			t.Errorf("shardIndex(%q, %d) = %d, want %d", tc.sym, tc.n, got, tc.want)
		}
	}
	for range 100 {
		if a, b := shardIndex("BTC-USDT", 8), shardIndex("BTC-USDT", 8); a != b {
			t.Fatalf("shardIndex is not deterministic: %d then %d", a, b)
		}
	}

	// Every shard must get work, or sharding buys nothing.
	const n = 8
	seen := make(map[int]int)
	for _, sym := range testSymbols(64) {
		seen[shardIndex(sym, n)]++
	}
	if len(seen) != n {
		t.Errorf("64 symbols reached %d of %d shards: %v", len(seen), n, seen)
	}
}

func TestNewRouterValidation(t *testing.T) {
	v := newVenue(nil, 1)
	if _, err := NewRouter(RouterConfig{QueueCap: 1}); err == nil {
		t.Error("NewRouter accepted a nil Snapshotter")
	}
	if _, err := NewRouter(RouterConfig{Snapshots: v}); err == nil {
		t.Error("NewRouter accepted an unbounded queue")
	}
	r, err := NewRouter(RouterConfig{Snapshots: v, QueueCap: 1})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if r.Shards() != runtime.NumCPU() {
		t.Errorf("Shards() = %d, want runtime.NumCPU() = %d (D18)", r.Shards(), runtime.NumCPU())
	}
}

// TestRouterAnchorsAndMaintainsBooks is the ordinary path: a delta for an
// unseen symbol creates a book, triggers the snapshot that anchors it, and the
// deltas that arrive meanwhile are replayed over it.
func TestRouterAnchorsAndMaintainsBooks(t *testing.T) {
	syms := testSymbols(6)
	v := newVenue(syms, 3)
	r := newTestRouter(t, RouterConfig{Shards: 3, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	rng := rand.New(rand.NewSource(3))
	const perSymbol = 40
	for range perSymbol {
		for _, sym := range syms {
			if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
				t.Fatalf("Route: %v", err)
			}
		}
	}

	waitFor(t, "every book to be anchored", func() bool {
		st := r.Stats()
		return st.Events == uint64(perSymbol*len(syms)) && st.Snapshots == uint64(len(syms))
	})
	r.Close()

	assertBooksMatch(t, r, v)
	st := r.Stats()
	if st.Gaps != 0 {
		t.Errorf("Gaps = %d on a stream with no dropped delta", st.Gaps)
	}
	if st.Applied == 0 {
		t.Error("no delta was ever applied: the books were rebuilt from snapshots alone")
	}
	if st.Errors != 0 {
		t.Errorf("Errors = %d", st.Errors)
	}
}

// TestRouterResyncsAfterGap is the sharded form of the M2.4 criterion: a
// deliberately dropped delta must produce a second snapshot and a book that
// still matches the venue.
func TestRouterResyncsAfterGap(t *testing.T) {
	syms := testSymbols(1)
	sym := syms[0]
	v := newVenue(syms, 5)
	r := newTestRouter(t, RouterConfig{Shards: 2, QueueCap: 8, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	rng := rand.New(rand.NewSource(5))
	route := func(d model.BookDelta) {
		t.Helper()
		if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: d}); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}
	for range 20 {
		route(v.next(sym, rng))
	}
	waitFor(t, "the first snapshot", func() bool { return r.Stats().Snapshots == 1 })

	v.next(sym, rng) // generated by the venue, never routed

	for range 20 {
		route(v.next(sym, rng))
	}
	waitFor(t, "the resync", func() bool {
		st := r.Stats()
		return st.Gaps == 1 && st.Snapshots == 2
	})
	r.Close()

	assertBooksMatch(t, r, v)
}

// TestRouterRetriesFailedFetch covers the path where the venue is unreachable:
// the shard must keep asking rather than leaving the book stale forever, and
// must not spin on the endpoint while doing so.
func TestRouterRetriesFailedFetch(t *testing.T) {
	syms := testSymbols(1)
	sym := syms[0]
	v := newVenue(syms, 7)
	v.failuresLeft.Store(2)

	r := newTestRouter(t, RouterConfig{
		Shards: 1, QueueCap: 8, Snapshots: v, RetryDelay: 5 * time.Millisecond,
	})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	rng := rand.New(rand.NewSource(7))
	for range 10 {
		if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}
	waitFor(t, "the retried snapshot", func() bool {
		st := r.Stats()
		return st.FetchFails == 2 && st.Snapshots == 1
	})
	r.Close()

	assertBooksMatch(t, r, v)
	if calls := v.calls.Load(); calls != 3 {
		t.Errorf("venue saw %d snapshot requests, want 3: two failures and one success", calls)
	}
}

// TestRouterUnderLoad is the done criterion for the milestone: many symbols,
// several producers, gaps injected throughout, run under the race detector.
func TestRouterUnderLoad(t *testing.T) {
	const (
		producers = 4
		perSymbol = 300
	)
	syms := testSymbols(32)
	v := newVenue(syms, 11)
	r := newTestRouter(t, RouterConfig{Shards: 4, QueueCap: 16, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Each producer owns a disjoint set of symbols, so a symbol's deltas are
	// generated in the order they are routed in. Sharing a symbol between
	// producers would reorder its stream, which is the venue's guarantee to
	// break, not this stage's.
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(100 + p)))
			mine := syms[p*len(syms)/producers : (p+1)*len(syms)/producers]
			for i := range perSymbol {
				for _, sym := range mine {
					d := v.next(sym, rng)
					if i%97 == 96 {
						continue // dropped in transit, so the shard must resync
					}
					if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: d}); err != nil {
						t.Errorf("Route: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	// Quiescence: every event handled, and no counter moving. A resync in
	// flight is still work in progress even once the last event is in.
	waitFor(t, "the shards to go quiet", func() bool {
		before := r.Stats()
		time.Sleep(10 * time.Millisecond)
		after := r.Stats()
		return before == after && after.Events > 0 && queuesEmpty(r)
	})
	r.Close()

	assertBooksMatch(t, r, v)
	st := r.Stats()
	if st.Gaps == 0 {
		t.Error("no gap was detected, so the resync path was never exercised")
	}
	if st.Errors != 0 {
		t.Errorf("Errors = %d", st.Errors)
	}
	t.Logf("stats: %+v", st)
}

// TestRouteBlocksAndRespectsContext is the lossless policy: a full queue
// blocks the producer rather than dropping, and a producer that gives up
// learns that its event was not delivered.
// queuesEmpty reports whether every shard queue has been drained.
func queuesEmpty(r *Router) bool {
	for _, n := range r.QueueDepth() {
		if n != 0 {
			return false
		}
	}
	return true
}

func TestRouteBlocksAndRespectsContext(t *testing.T) {
	v := newVenue(testSymbols(1), 13)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 2, Snapshots: v})
	// Deliberately not started: nothing drains the queue.

	ctx, cancel := context.WithCancel(context.Background())
	ev := model.Event{Kind: model.KindBookDelta, BookDelta: model.BookDelta{Symbol: testSymbols(1)[0]}}
	for range 2 {
		if err := r.Route(ctx, ev); err != nil {
			t.Fatalf("Route into a queue with room: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- r.Route(ctx, ev) }()
	select {
	case err := <-done:
		t.Fatalf("Route returned %v on a full queue, want it to block", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Route = %v, want context.Canceled", err)
	}
	r.Close() // never started: must be a no-op rather than a panic
}

func TestRouteRejectsEventWithoutSymbol(t *testing.T) {
	v := newVenue(nil, 17)
	r := newTestRouter(t, RouterConfig{Shards: 1, Snapshots: v})
	if err := r.Route(context.Background(), model.Event{}); err == nil {
		t.Error("Route accepted an event with no symbol")
	}
}

func TestRouterLifecycle(t *testing.T) {
	v := newVenue(nil, 19)
	r := newTestRouter(t, RouterConfig{Shards: 2, Snapshots: v})
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := r.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("second Start = %v, want %v", err, ErrAlreadyStarted)
	}
	r.Close()
	r.Close() // idempotent
}

// TestShardIgnoresSnapshotBehindTheBook covers the case where a snapshot
// arrives on the stream after the book has already moved past it.
func TestShardIgnoresSnapshotBehindTheBook(t *testing.T) {
	syms := testSymbols(1)
	sym := syms[0]
	v := newVenue(syms, 23)
	r := newTestRouter(t, RouterConfig{Shards: 1, QueueCap: 8, Snapshots: v})
	ctx := context.Background()

	old := v.state(sym) // taken before anything is applied
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rng := rand.New(rand.NewSource(23))
	for range 20 {
		if err := r.Route(ctx, model.Event{Kind: model.KindBookDelta, BookDelta: v.next(sym, rng)}); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}
	waitFor(t, "the book to sync", func() bool { return r.Stats().Snapshots == 1 })

	before := r.Stats().Discarded
	if err := r.Route(ctx, model.Event{Kind: model.KindSnapshot, Snapshot: old}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitFor(t, "the stale snapshot to be discarded", func() bool { return r.Stats().Discarded > before })
	r.Close()

	assertBooksMatch(t, r, v)
	if st := r.Stats(); st.Errors != 0 {
		t.Errorf("Errors = %d: a snapshot behind the book is not a fault", st.Errors)
	}
}

// TestTrackerCompareUsedHere is a guard on the test helper itself: if
// assertBooksMatch could not detect a wrong book, every test above would pass
// for the wrong reason.
func TestAssertBooksMatchDetectsDivergence(t *testing.T) {
	syms := testSymbols(1)
	v := newVenue(syms, 29)
	tr := book.NewTracker(syms[0], 0)
	tr.BeginFetch()
	if err := tr.Load(v.state(syms[0])); err != nil {
		t.Fatalf("Load: %v", err)
	}
	d, err := tr.Compare(v.state(syms[0]))
	if err != nil || !d.OK() {
		t.Fatalf("a book loaded from the venue diverges from it: %v, %v", d, err)
	}

	rng := rand.New(rand.NewSource(29))
	v.next(syms[0], rng) // the venue moves, the book does not
	d, err = tr.Compare(v.state(syms[0]))
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if d.OK() {
		t.Error("Compare agreed with a book that had missed an update")
	}
}
