package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/zuniverse/market-stream/internal/book"
	"github.com/zuniverse/market-stream/internal/model"
)

// Errors returned by Query.
var (
	// ErrUnknownSymbol means no book is held for that symbol. A query never
	// creates one: a read must not change what the stage is tracking.
	ErrUnknownSymbol = errors.New("pipeline: no book for symbol")

	// ErrRouterClosed means the shard that owns the symbol has stopped, so
	// the query can never be answered. It is returned instead of waiting for
	// the caller's context to expire.
	ErrRouterClosed = errors.New("pipeline: router closed")
)

// BookView is the answer to a query: a copy of what one book held at the
// moment the owning shard read it, between two deltas.
//
// The levels are copies, so the caller may keep or modify them freely. That
// is what makes reading a book safe without a lock: the value crosses the
// goroutine boundary, the book itself never does (D4).
type BookView struct {
	Symbol model.Symbol

	// Live reports that the book was anchored and current. A view of a book
	// that is not live is a view of a book missing updates, and the levels in
	// it are the last good state rather than the state now.
	Live bool

	// LastID is the last update id applied.
	LastID int64

	// Crossed reports the best bid at or above the best ask, which no
	// exchange publishes and which therefore means the book is wrong.
	Crossed bool

	// Bids and Asks hold at most the requested depth, top of book first.
	Bids []model.Level
	Asks []model.Level

	// BidDepth and AskDepth are the level counts of the whole side, not of
	// the slices above.
	BidDepth int
	AskDepth int

	// Buffered is how many deltas are held pending a resync, which is zero
	// for a live book.
	Buffered int

	// Stats are the per-symbol counters, which nothing outside the owning
	// shard can reach except through this path.
	Stats book.Stats
}

// query is one book read on its way to the shard that owns the symbol.
type query struct {
	symbol model.Symbol
	depth  int

	// check, when set, asks for a comparison against this snapshot instead of
	// a view. The comparison has to run on the owning goroutine: it reads the
	// book, and it needs the edges of what that book knows, which live in the
	// tracker and are not part of any view (D28).
	check *model.Snapshot

	// reply has capacity 1 and exactly one value is ever sent on it, so the
	// shard's send cannot block even when the caller has already given up
	// and stopped listening.
	reply chan queryResult
}

type queryResult struct {
	view BookView
	diff book.Diff
	err  error
}

// Query reads one book through the goroutine that owns it. The request
// carries the channel it is answered on, and the owning shard answers it
// between two deltas, so no lock is ever taken on book state (D4).
//
// depth is the number of levels wanted per side, top of book first. Zero or
// less returns the view without any levels, which is what a health check
// wants.
//
// A caller that gives up costs the shard nothing: cancelling ctx returns
// immediately, and the answer, if one was already on its way, is discarded
// into a buffered channel rather than blocking the shard on a send.
//
// Query is valid between Start and Close, and is safe to call from any number
// of goroutines at once.
func (r *Router) Query(ctx context.Context, symbol model.Symbol, depth int) (BookView, error) {
	res, err := r.ask(ctx, query{symbol: symbol, depth: depth})
	return res.view, err
}

// Check compares the book against snap and reports every price at which they
// disagree, together with the update id of each (D28). It is the correctness
// harness: a fresh snapshot is the only external truth about a book that has
// been maintained from deltas for hours.
//
// The comparison runs on the goroutine that owns the book, for the same
// reason a read does, and the caller fetches the snapshot itself so that the
// cost of a full-depth request stays a decision the caller makes.
func (r *Router) Check(ctx context.Context, snap model.Snapshot) (book.Diff, error) {
	res, err := r.ask(ctx, query{symbol: snap.Symbol, check: &snap})
	return res.diff, err
}

// ask sends q to the shard that owns its symbol and waits for the answer.
func (r *Router) ask(ctx context.Context, q query) (queryResult, error) {
	q.reply = make(chan queryResult, 1)
	s := r.shards[shardIndex(q.symbol, len(r.shards))]

	select {
	case s.queries <- q:
	case <-s.quit:
		return queryResult{}, fmt.Errorf("query %s: %w", q.symbol, ErrRouterClosed)
	case <-ctx.Done():
		return queryResult{}, fmt.Errorf("query %s: %w", q.symbol, ctx.Err())
	}

	select {
	case res := <-q.reply:
		return res, res.err
	case <-ctx.Done():
		return queryResult{}, fmt.Errorf("query %s: %w", q.symbol, ctx.Err())
	}
}

// answer runs on the shard goroutine, between two deltas.
func (s *shard) answer(q query) {
	s.answered.Add(1)
	t, ok := s.books[q.symbol]
	if !ok {
		q.reply <- queryResult{err: fmt.Errorf("shard %d: %s: %w", s.id, q.symbol, ErrUnknownSymbol)}
		return
	}
	if q.check != nil {
		diff, err := t.Compare(*q.check)
		q.reply <- queryResult{diff: diff, err: err}
		return
	}
	b := t.Book()
	q.reply <- queryResult{view: BookView{
		Symbol:   q.symbol,
		Live:     t.Live(),
		LastID:   t.LastID(),
		Crossed:  b.Crossed(),
		Bids:     b.Bids(q.depth),
		Asks:     b.Asks(q.depth),
		BidDepth: b.BidDepth(),
		AskDepth: b.AskDepth(),
		Buffered: t.Buffered(),
		Stats:    t.Stats(),
	}}
}
