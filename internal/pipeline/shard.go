package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuniverse/market-stream/internal/book"
	"github.com/zuniverse/market-stream/internal/model"
)

// DefaultRetryDelay is how long a shard waits before asking for a snapshot
// again after a failed request. It is deliberately blunt: a tracker allows one
// outstanding fetch per symbol, so the request rate a shard can produce is
// already bounded by its symbol count divided by this delay.
const DefaultRetryDelay = time.Second

// Snapshotter fetches a full order book snapshot for one symbol.
//
// The interface is declared here, on the consumer side, so that the pipeline
// never imports an exchange package. *binance.DepthClient satisfies it without
// knowing this package exists (D21).
type Snapshotter interface {
	Snapshot(ctx context.Context, symbol model.Symbol) (model.Snapshot, error)
}

// RouterConfig configures a Router.
type RouterConfig struct {
	// Shards is the number of shard goroutines, N in hash(symbol) % N. Zero
	// or less uses runtime.NumCPU(), which is the flag default in D18.
	Shards int

	// QueueCap is the capacity of each shard's input channel. It must be
	// positive: this is the lossless path, so the policy when the queue is
	// full is to block the sender, and a queue with no bound has no policy
	// at all.
	QueueCap int

	// BufferCap is the per-symbol delta buffer held during a resync. Zero or
	// less uses book.DefaultBufferCap.
	BufferCap int

	// Snapshots fetches the depth snapshots that anchor and re-anchor books.
	// Required: without it a book can never leave its initial stale state.
	Snapshots Snapshotter

	// RetryDelay is the wait after a failed snapshot request. Zero or less
	// uses DefaultRetryDelay.
	RetryDelay time.Duration

	// Log receives one line per event that a book could not absorb. Nil
	// discards them.
	Log *slog.Logger
}

// Stats counts what the shards have done since Start. The values are read
// with atomics and are individually consistent but not a consistent set: a
// reader can see Applied incremented before the Events that carried it.
type Stats struct {
	Events     uint64 // events accepted by Route and handled by a shard
	Applied    uint64 // deltas applied to a book
	Discarded  uint64 // events already reflected in a book
	Buffered   uint64 // deltas held during a resync
	Gaps       uint64 // holes detected in an id sequence
	Snapshots  uint64 // snapshots that anchored a book
	Refetches  uint64 // snapshots that landed too old to anchor one
	FetchFails uint64 // snapshot requests that returned an error
	Errors     uint64 // events a book refused
	Queries    uint64 // book reads answered
}

// Router owns the book stage: it routes each event to the shard that owns
// that symbol's book, and each shard is the exclusive owner of every book
// assigned to it.
//
// Routing is hash(symbol) % N rather than a worker pool because book deltas
// must be applied in strict per-symbol sequence, and a pool destroys that
// ordering. Single ownership then gives the ordering guarantee, no locking on
// book state, and parallelism across symbols (D3).
//
// Route blocks when a shard queue is full. That is the opposite of
// Publisher.Publish, and the contrast is the point: this is the lossless path,
// where dropping a delta corrupts a book, so overload propagates backwards as
// backpressure. Downstream of the publisher, where freshness beats
// completeness, the same situation drops the oldest event instead.
//
// The lifecycle is Start -> Route* -> Close. Route is safe to call from
// several goroutines at once, so one router can serve several connections.
type Router struct {
	shards []*shard
	log    *slog.Logger

	mu        sync.Mutex
	started   bool
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewRouter returns a Router configured by cfg, with no goroutines running.
func NewRouter(cfg RouterConfig) (*Router, error) {
	if cfg.Snapshots == nil {
		return nil, fmt.Errorf("pipeline: router: a Snapshotter is required")
	}
	if cfg.QueueCap <= 0 {
		return nil, fmt.Errorf("pipeline: router: queue capacity must be positive, got %d", cfg.QueueCap)
	}
	n := cfg.Shards
	if n <= 0 {
		n = runtime.NumCPU()
	}
	delay := cfg.RetryDelay
	if delay <= 0 {
		delay = DefaultRetryDelay
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	r := &Router{shards: make([]*shard, n), log: log}
	for i := range r.shards {
		r.shards[i] = &shard{
			id:        i,
			in:        make(chan model.Event, cfg.QueueCap),
			results:   make(chan fetchResult),
			queries:   make(chan query),
			quit:      make(chan struct{}),
			books:     make(map[model.Symbol]*book.Tracker),
			bufferCap: cfg.BufferCap,
			snapshots: cfg.Snapshots,
			retry:     delay,
			log:       log,
			wg:        &r.wg,
		}
	}
	return r, nil
}

// Start launches one goroutine per shard. ctx is passed to snapshot requests
// so that an in-flight fetch is abandoned when the process is shutting down.
//
// Owner of each shard goroutine: the Router. Exit: its input channel is
// closed by Close.
//
// The shard loop deliberately does not select on ctx.Done. A select with both
// cases ready picks at random, so a cancelled context would discard an
// arbitrary prefix of a queue that is bounded and therefore cheap to drain.
// The same reasoning as the publisher's delivery loops (D20).
func (r *Router) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return ErrAlreadyStarted
	}
	r.started = true

	for _, s := range r.shards {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			s.run(ctx)
		}()
	}
	return nil
}

// Route hands ev to the shard that owns its symbol, blocking until the shard
// accepts it or ctx is cancelled. It returns ctx.Err() in the second case, so
// a caller that gives up knows the event was not delivered.
//
// Route is valid only between Start and Close.
func (r *Router) Route(ctx context.Context, ev model.Event) error {
	sym, ok := eventSymbol(ev)
	if !ok {
		return fmt.Errorf("pipeline: route: event kind %d carries no symbol", ev.Kind)
	}
	s := r.shards[shardIndex(sym, len(r.shards))]
	select {
	case s.in <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the shards and waits for them to drain their queues and for
// every outstanding snapshot request to return. Draining is bounded work
// because every queue is bounded.
//
// Close must not overlap with Route: a send on a closed channel panics.
// Repeated calls after the first do nothing.
func (r *Router) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		started := r.started
		r.mu.Unlock()
		if !started {
			return
		}
		for _, s := range r.shards {
			close(s.in)
		}
		r.wg.Wait()
	})
}

// Shards returns the number of shard goroutines.
func (r *Router) Shards() int { return len(r.shards) }

// QueueDepth returns the number of events waiting in each shard queue, in
// shard order. M6 exposes these as the per-stage queue depth gauge.
func (r *Router) QueueDepth() []int {
	out := make([]int, len(r.shards))
	for i, s := range r.shards {
		out[i] = len(s.in)
	}
	return out
}

// Stats returns the shard counters, summed across shards.
func (r *Router) Stats() Stats {
	var total Stats
	for _, s := range r.shards {
		total.Events += s.events.Load()
		total.Applied += s.applied.Load()
		total.Discarded += s.discarded.Load()
		total.Buffered += s.buffered.Load()
		total.Gaps += s.gaps.Load()
		total.Snapshots += s.anchored.Load()
		total.Refetches += s.refetches.Load()
		total.FetchFails += s.fetchFails.Load()
		total.Errors += s.errors.Load()
		total.Queries += s.answered.Load()
	}
	return total
}

// shardIndex maps a symbol to a shard. The hash is FNV-1a written out rather
// than taken from hash/maphash, because maphash is seeded per process and the
// assignment must be the same on every run for two replays of one recording to
// be comparable (D5).
func shardIndex(sym model.Symbol, n int) int {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(sym); i++ {
		h ^= uint64(sym[i])
		h *= prime64
	}
	return int(h % uint64(n))
}

// eventSymbol returns the symbol an event concerns.
func eventSymbol(ev model.Event) (model.Symbol, bool) {
	switch ev.Kind {
	case model.KindTrade:
		return ev.Trade.Symbol, true
	case model.KindBookDelta:
		return ev.BookDelta.Symbol, true
	case model.KindSnapshot:
		return ev.Snapshot.Symbol, true
	default:
		return "", false
	}
}

// fetchResult is one completed snapshot request on its way back to the shard
// that asked for it.
type fetchResult struct {
	symbol   model.Symbol
	snapshot model.Snapshot
	err      error
}

// shard owns every book assigned to it by shardIndex. Everything below this
// point runs on the shard's own goroutine unless a comment says otherwise, so
// the books, the trackers and the map that holds them need no locking (D3).
type shard struct {
	id  int
	in  chan model.Event
	log *slog.Logger

	// results carries completed snapshot requests back from the fetch
	// goroutines. Unbuffered: a fetch that has finished can afford to wait,
	// since it holds nothing the shard needs.
	results chan fetchResult

	// queries carries book reads, each with the channel to answer on.
	// Unbuffered, so there is no queue to size and no queued request left
	// unanswered at shutdown: a caller waits exactly until the shard takes
	// its request, or until it gives up (D31).
	queries chan query

	// quit is closed by the shard goroutine as it returns, so a fetch still
	// in flight at shutdown does not block forever on a send to results.
	quit chan struct{}

	books     map[model.Symbol]*book.Tracker
	bufferCap int
	snapshots Snapshotter
	retry     time.Duration

	// wg is the Router's, and counts this loop plus every fetch goroutine it
	// starts. Adds happen on this goroutine while the loop still holds its
	// own count, so the counter cannot reach zero between them.
	wg *sync.WaitGroup

	// Counters are read by Router.Stats from another goroutine, which is why
	// they are atomics. They are not book state.
	events     atomic.Uint64
	applied    atomic.Uint64
	discarded  atomic.Uint64
	buffered   atomic.Uint64
	gaps       atomic.Uint64
	anchored   atomic.Uint64
	refetches  atomic.Uint64
	fetchFails atomic.Uint64
	errors     atomic.Uint64
	answered   atomic.Uint64
}

// run is the shard loop. It exits when its input channel is closed, having
// drained whatever was queued.
func (s *shard) run(ctx context.Context) {
	// Closing quit releases every caller still parked: a fetch goroutine with
	// nowhere to deliver its result, and a query whose request was never
	// taken. Because the query channel is unbuffered, a request that was
	// taken is one this loop is already answering, so nothing can be left
	// waiting for an answer that will not come.
	defer close(s.quit)

	for {
		select {
		case ev, ok := <-s.in:
			if !ok {
				return
			}
			s.handle(ctx, ev)
		case res := <-s.results:
			s.complete(ctx, res)
		case q := <-s.queries:
			s.answer(q)
		}
	}
}

// handle applies one event to the book it concerns.
func (s *shard) handle(ctx context.Context, ev model.Event) {
	s.events.Add(1)
	switch ev.Kind {
	case model.KindBookDelta:
		d := ev.BookDelta
		t := s.tracker(ctx, d.Symbol)
		st, err := t.Apply(d)
		if err != nil {
			s.errors.Add(1)
			s.log.LogAttrs(ctx, slog.LevelWarn, "book delta rejected",
				slog.String("symbol", string(d.Symbol)), slog.String("err", err.Error()))
		}
		switch st {
		case book.StatusApplied:
			s.applied.Add(1)
		case book.StatusDiscarded:
			s.discarded.Add(1)
		case book.StatusBuffered:
			s.buffered.Add(1)
		case book.StatusGapped:
			s.gaps.Add(1)
			s.log.LogAttrs(ctx, slog.LevelWarn, "sequence gap",
				slog.String("symbol", string(d.Symbol)),
				slog.Int64("last_applied", t.LastID()),
				slog.Int64("delta_first", d.FirstID))
		}
		s.fetch(ctx, t)

	case model.KindSnapshot:
		t := s.tracker(ctx, ev.Snapshot.Symbol)
		s.load(ctx, t, ev.Snapshot)

	case model.KindTrade:
		// Trades do not touch the book. They are routed here anyway so that
		// one symbol's events keep their arrival order through the stage,
		// which the aggregation stage will depend on.

	default:
		s.errors.Add(1)
	}
}

// tracker returns the tracker for sym, creating it on first sight and asking
// for the snapshot that will anchor it.
func (s *shard) tracker(ctx context.Context, sym model.Symbol) *book.Tracker {
	t, ok := s.books[sym]
	if !ok {
		t = book.NewTracker(sym, s.bufferCap)
		s.books[sym] = t
		s.fetch(ctx, t)
	}
	return t
}

// fetch starts a snapshot request if the tracker needs one and none is
// already in flight for it.
//
// Owner of the fetch goroutine: this shard. Exit: the request returns and the
// result is either delivered or abandoned because the shard has stopped.
func (s *shard) fetch(ctx context.Context, t *book.Tracker) {
	if !t.BeginFetch() {
		return
	}
	sym := t.Symbol()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		snap, err := s.snapshots.Snapshot(ctx, sym)
		if err != nil {
			// Hold the failure for the retry delay rather than reporting it
			// straight back. The tracker counts a fetch as outstanding until
			// the result lands, so waiting here is what keeps a symbol whose
			// snapshots keep failing from spinning on the endpoint, and it
			// costs only this goroutine.
			select {
			case <-time.After(s.retry):
			case <-ctx.Done():
			case <-s.quit:
			}
		}
		select {
		case s.results <- fetchResult{symbol: sym, snapshot: snap, err: err}:
		case <-s.quit:
		}
	}()
}

// complete takes a finished snapshot request back into the book.
func (s *shard) complete(ctx context.Context, res fetchResult) {
	t, ok := s.books[res.symbol]
	if !ok {
		return // cannot happen: a fetch is only started for a tracked symbol
	}
	if res.err != nil {
		s.fetchFails.Add(1)
		s.log.LogAttrs(ctx, slog.LevelWarn, "snapshot fetch failed",
			slog.String("symbol", string(res.symbol)), slog.String("err", res.err.Error()))
		t.FetchFailed()
		s.fetch(ctx, t)
		return
	}
	s.load(ctx, t, res.snapshot)
}

// load re-anchors a book on a snapshot and starts another fetch if the
// snapshot turned out to be too old to bridge to the buffered deltas.
func (s *shard) load(ctx context.Context, t *book.Tracker, snap model.Snapshot) {
	live := t.Live()
	err := t.Load(snap)
	switch {
	case errors.Is(err, book.ErrSnapshotBehind):
		// Two snapshots were in flight at once and the older one landed
		// second. The book is already past it, so there is nothing to do.
		s.discarded.Add(1)
	case err != nil:
		s.errors.Add(1)
		s.log.LogAttrs(ctx, slog.LevelWarn, "snapshot rejected",
			slog.String("symbol", string(snap.Symbol)), slog.String("err", err.Error()))
	case t.Live():
		s.anchored.Add(1)
		if !live {
			s.log.LogAttrs(ctx, slog.LevelInfo, "book synced",
				slog.String("symbol", string(snap.Symbol)),
				slog.Int64("last_id", t.LastID()))
		}
	default:
		// The snapshot predates the oldest buffered delta, so it cannot be
		// joined to what is held. Ask for a newer one.
		s.refetches.Add(1)
	}
	s.fetch(ctx, t)
}
