package book

import "github.com/zuniverse/market-stream/internal/model"

// Class is the result of sequencing one delta against the ids already
// applied to a book. The zero value is not a valid class.
type Class uint8

const (
	// Stale means every id the delta carries has already been applied. The
	// delta is redundant and must be discarded.
	Stale Class = iota + 1

	// Contiguous means the delta continues from the last applied id with no
	// missing update in between, so it can be applied to the book.
	Contiguous

	// Gapped means the delta cannot be placed in sequence: at least one
	// update between the last applied id and this one was never seen. The
	// delta must not be applied, and the book is stale until a fresh
	// snapshot re-anchors it.
	Gapped
)

// String returns the class name, for logs and test failure messages.
func (c Class) String() string {
	switch c {
	case Stale:
		return "stale"
	case Contiguous:
		return "contiguous"
	case Gapped:
		return "gapped"
	default:
		return "invalid"
	}
}

// Sequencer tracks the last update id applied to one symbol's book and
// classifies each incoming delta against it.
//
// It holds no book state and performs no recovery: it decides only whether a
// delta may be applied, and reports when a resync is required. Acting on that
// decision (fetching a snapshot, buffering deltas during the fetch, replaying
// the buffer) belongs to the layer above, in M2.4.
//
// The zero value is usable and needs a resync: a sequencer that has never
// been given a snapshot id has nothing to sequence against, so it classifies
// every delta as Gapped until Reset is called.
//
// A Sequencer is not safe for concurrent use. Like the Book it sequences for,
// it is owned by exactly one goroutine (D3).
type Sequencer struct {
	last   int64
	synced bool
}

// Reset anchors the sequencer at the last update id of a snapshot, which is
// the id of the last delta already reflected in that snapshot's contents. It
// clears the stale flag, so deltas are classified again from the next call.
//
// Reset does not check that id is newer than what has already been applied.
// A snapshot that arrives older than the current position is a resync
// decision, not a sequencing one: the caller either refetches or accepts the
// rewind, and it is the caller that knows which.
func (s *Sequencer) Reset(id int64) {
	s.last = id
	s.synced = true
}

// Next classifies d and advances the sequencer.
//
// It is not a pure predicate: a Contiguous result moves the last applied id
// forward, and a Gapped result marks the sequencer stale. Classifying and
// advancing in one call is what makes it impossible to apply a delta to the
// book without recording that it was applied.
//
// The rule is the Binance depth specification, in one comparison per bound:
// a delta is Stale when its last id is at or below the last applied id, and
// Contiguous when its first id is at or below the next expected id. Anything
// else leaves a hole in the id space and is Gapped.
//
// The same rule covers the first delta after a snapshot and every delta after
// that. Binance states the boundary case separately (U <= lastUpdateId+1 and
// u >= lastUpdateId+1) but it is the general rule with the Stale test already
// applied, so it needs no special case here.
//
// A delta that overlaps what has already been applied, rather than starting
// exactly at the next id, is Contiguous rather than an error. Depth levels
// carry absolute quantities, not increments, so a level restated over a range
// that was already applied resolves to the same value. Refusing the overlap
// would force a resync to reach a state the book is already in.
func (s *Sequencer) Next(d model.BookDelta) Class {
	if !s.synced {
		return Gapped
	}
	switch {
	case d.LastID <= s.last:
		return Stale
	case d.FirstID <= s.last+1:
		s.last = d.LastID
		return Contiguous
	default:
		s.synced = false
		return Gapped
	}
}

// Invalidate marks the sequencer as needing a resync without a gap having
// been observed. It exists for the case where a delta was classified
// Contiguous and then failed to reach the book: the position has advanced
// past an update the book never saw, which is the same hole a lost delta
// leaves, and only the caller knows it happened.
func (s *Sequencer) Invalidate() { s.synced = false }

// LastID returns the highest update id applied, or the snapshot id given to
// Reset if no delta has been applied since. It is zero before the first
// Reset. After a gap it holds the position the stream was at when the gap was
// detected, which is what a log line about the gap needs.
func (s *Sequencer) LastID() int64 { return s.last }

// NeedsResync reports whether the book this sequencer tracks must be
// re-anchored on a fresh snapshot before any further delta can be applied. It
// is true before the first Reset and after any gap.
func (s *Sequencer) NeedsResync() bool { return !s.synced }
