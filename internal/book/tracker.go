package book

import (
	"fmt"
	"slices"

	"github.com/zuniverse/market-stream/internal/model"
)

// DefaultBufferCap is the buffered delta capacity a Tracker uses when none is
// given. At the Binance depth stream rate of one delta per 100ms per symbol,
// it holds several minutes of updates, which is far more than any snapshot
// fetch should take. It is large because the cost of one entry is a slice
// header and the levels the decoder already allocated, and small enough that
// a symbol wedged in resync cannot grow without bound.
const DefaultBufferCap = 2048

// Status is the disposition of one delta handed to a Tracker.
type Status uint8

const (
	// Applied means the delta was in sequence and is now in the book.
	StatusApplied Status = iota + 1

	// Discarded means every id in the delta was already applied. Redundant
	// traffic, not a fault: it is the normal outcome for the deltas that
	// arrive while a snapshot is in flight and turn out to predate it.
	StatusDiscarded

	// Buffered means a resync is under way and the delta is being held for
	// replay once the snapshot lands.
	StatusBuffered

	// Gapped means this delta opened a hole in the id sequence. It was not
	// applied, it is buffered, and the book is stale until a snapshot
	// re-anchors it.
	StatusGapped
)

// String returns the status name, for logs and test failure messages.
func (s Status) String() string {
	switch s {
	case StatusApplied:
		return "applied"
	case StatusDiscarded:
		return "discarded"
	case StatusBuffered:
		return "buffered"
	case StatusGapped:
		return "gapped"
	default:
		return "invalid"
	}
}

// Stats counts the events a Tracker has seen since it was created. They are
// the per-symbol figures the metrics in M6 report.
type Stats struct {
	Applied   uint64 // deltas applied to the book
	Discarded uint64 // deltas already reflected in the book
	Buffered  uint64 // deltas held during a resync
	Gaps      uint64 // holes detected in the id sequence
	Dropped   uint64 // buffered deltas discarded because the buffer was full
	Resyncs   uint64 // snapshots that successfully re-anchored the book
	Refetches uint64 // snapshots that landed too old to re-anchor it
}

// Tracker maintains one symbol's book across its whole lifecycle: the initial
// snapshot, live deltas, gap detection, and resync. It is the Binance depth
// resync procedure, minus the fetch itself.
//
// The fetch is deliberately the caller's. It blocks on the network, and the
// goroutine that owns a Tracker owns other symbols' books too (M2.5), so a
// tracker that fetched for itself would stall every book beside it. The
// division is: the Tracker says when a snapshot is needed and what to do with
// it, the caller decides how and where to go and get one.
//
// The cycle is:
//
//	for each delta:      Apply
//	when it says so:     BeginFetch, then Load or FetchFailed
//
// A Tracker performs no I/O, starts no goroutines, and is not safe for
// concurrent use. Like the Book inside it, it is owned by exactly one
// goroutine (D3).
type Tracker struct {
	book *Book
	seq  Sequencer

	// buf holds deltas received while the book has no valid anchor. It is
	// bounded: on overflow the oldest entry is dropped and counted. Dropping
	// the oldest cannot make a hole in what remains, and the hole it does
	// leave, between the snapshot and the new head, is caught by the
	// sequencer during replay and answered with another fetch.
	buf    []model.BookDelta
	bufCap int

	// fetching records that the caller has a snapshot request outstanding,
	// so a book that is stale for a thousand consecutive deltas asks for one
	// snapshot rather than a thousand.
	fetching bool

	// bidFloor and askCeil are the edges of what this book knows, taken from
	// the truncated snapshot that seeded it: complete at and above bidFloor,
	// complete at and below askCeil, unknown past them. Zero means the
	// snapshot covered the whole side and there is no bound (D25).
	bidFloor model.Price
	askCeil  model.Price

	stats Stats
}

// NewTracker returns a tracker for symbol, holding at most bufCap deltas
// while a snapshot is in flight. A bufCap of zero or less uses
// DefaultBufferCap.
//
// The returned tracker has no book contents and needs a snapshot, so the
// first call to BeginFetch returns true. Startup and recovery are the same
// path: there is no separate "first snapshot" case anywhere below.
func NewTracker(symbol model.Symbol, bufCap int) *Tracker {
	if bufCap <= 0 {
		bufCap = DefaultBufferCap
	}
	return &Tracker{book: New(symbol), bufCap: bufCap}
}

// Symbol returns the symbol this tracker maintains.
func (t *Tracker) Symbol() model.Symbol { return t.book.Symbol() }

// Book returns the book, for reading. Callers must not mutate it: the
// tracker's sequence position describes exactly the updates in it, and an
// edit from outside makes that description a lie. The read methods on Book
// return copies, so answering a query from it is safe.
func (t *Tracker) Book() *Book { return t.book }

// Live reports whether the book is anchored and current, meaning every delta
// up to LastID has been applied and none is missing. It is false before the
// first snapshot and between a gap and the resync that closes it.
func (t *Tracker) Live() bool { return !t.seq.NeedsResync() }

// LastID returns the last update id applied to the book.
func (t *Tracker) LastID() int64 { return t.seq.LastID() }

// Buffered returns the number of deltas currently held for replay.
func (t *Tracker) Buffered() int { return len(t.buf) }

// Stats returns a copy of the counters.
func (t *Tracker) Stats() Stats { return t.stats }

// Apply offers one delta to the tracker and reports what became of it.
//
// The error is separate from the Status and much rarer: it means the delta
// could not belong to this book at all, either because it names another
// symbol or because it carries levels that cannot exist. Neither is a
// sequencing outcome, and both leave the book stale and awaiting a resync.
func (t *Tracker) Apply(d model.BookDelta) (Status, error) {
	if d.Symbol != t.book.Symbol() {
		return 0, fmt.Errorf("book %s: delta for symbol %s", t.book.Symbol(), d.Symbol)
	}
	if t.seq.NeedsResync() {
		t.buffer(d)
		t.stats.Buffered++
		return StatusBuffered, nil
	}
	switch t.seq.Next(d) {
	case Stale:
		t.stats.Discarded++
		return StatusDiscarded, nil
	case Contiguous:
		if err := t.book.Apply(d.Bids, d.Asks); err != nil {
			// The sequencer has already moved past this delta, so the book is
			// now missing an update whatever the caller does next. Say so.
			t.seq.Invalidate()
			t.buffer(d)
			return 0, fmt.Errorf("book %s: apply delta %d-%d: %w", t.book.Symbol(), d.FirstID, d.LastID, err)
		}
		t.stats.Applied++
		return StatusApplied, nil
	default:
		t.stats.Gaps++
		t.buffer(d)
		return StatusGapped, nil
	}
}

// BeginFetch reports whether a snapshot must be fetched, and marks a fetch as
// outstanding when it returns true. The caller must then call exactly one of
// Load or FetchFailed, or the tracker will never ask again.
func (t *Tracker) BeginFetch() bool {
	if t.fetching || !t.seq.NeedsResync() {
		return false
	}
	t.fetching = true
	return true
}

// FetchFailed clears the outstanding fetch after a snapshot request that
// produced no snapshot, so the next BeginFetch asks for another. Retry policy
// (how soon, how many times, with what backoff) is the caller's, since it is
// the caller that knows what failed.
func (t *Tracker) FetchFailed() { t.fetching = false }

// Load re-anchors the book on a fresh snapshot and replays the buffered
// deltas over it, which is the whole of the resync procedure:
//
//   - the snapshot replaces the book contents outright,
//   - buffered deltas the snapshot already reflects are discarded,
//   - the rest are applied in order,
//   - live application resumes.
//
// The snapshot may land too old to be usable, when the stream ran further
// ahead than the buffer's oldest held delta while the request was in flight.
// That is not an error: Load keeps the deltas it could not place, leaves the
// book needing a resync, and the next BeginFetch asks for a newer snapshot.
// Check Live to tell the two outcomes apart.
func (t *Tracker) Load(s model.Snapshot) error {
	t.fetching = false
	if s.Symbol != t.book.Symbol() {
		return fmt.Errorf("book %s: snapshot for symbol %s", t.book.Symbol(), s.Symbol)
	}
	if err := t.book.Reset(s.Bids, s.Asks); err != nil {
		// Reset leaves the book untouched on failure, and the tracker still
		// needs a resync, so a rejected snapshot costs only this attempt.
		return fmt.Errorf("book %s: load snapshot %d: %w", t.book.Symbol(), s.LastID, err)
	}
	t.seq.Reset(s.LastID)
	t.setBounds(s)

	for i, d := range t.buf {
		switch t.seq.Next(d) {
		case Stale:
			continue
		case Contiguous:
			if err := t.book.Apply(d.Bids, d.Asks); err != nil {
				t.seq.Invalidate()
				t.keepBufferFrom(i)
				return fmt.Errorf("book %s: replay delta %d-%d: %w", t.book.Symbol(), d.FirstID, d.LastID, err)
			}
		default:
			// The snapshot predates the oldest delta still held, so the
			// updates between them are gone. Keep this delta and everything
			// after it: a newer snapshot will cover the hole.
			t.seq.Invalidate()
			t.keepBufferFrom(i)
			t.stats.Refetches++
			return nil
		}
	}
	t.clearBuffer()
	t.stats.Resyncs++
	return nil
}

// buffer appends d, dropping the oldest held delta when the buffer is full.
func (t *Tracker) buffer(d model.BookDelta) {
	if len(t.buf) >= t.bufCap {
		t.buf = slices.Delete(t.buf, 0, len(t.buf)-t.bufCap+1)
		t.stats.Dropped++
	}
	t.buf = append(t.buf, d)
}

// keepBufferFrom discards the first i held deltas, which have been applied or
// were already reflected in the snapshot.
func (t *Tracker) keepBufferFrom(i int) {
	t.buf = slices.Delete(t.buf, 0, i)
}

// clearBuffer empties the buffer and releases the levels it held. The
// capacity is kept: a tracker that resynced once will resync again, and the
// slice is small.
func (t *Tracker) clearBuffer() {
	clear(t.buf)
	t.buf = t.buf[:0]
}

// setBounds records how deep the snapshot's knowledge goes, per D25. An
// untruncated snapshot describes the whole book, so both bounds are cleared.
//
// Truncation is reported for the snapshot as a whole rather than per side, so
// a side that happened to come back complete is still bounded at its deepest
// level. The cost is a comparison that stops one level short of where it
// could have gone; the alternative is reporting every level below the cut as
// a divergence.
func (t *Tracker) setBounds(s model.Snapshot) {
	t.bidFloor, t.askCeil = 0, 0
	if !s.Truncated {
		return
	}
	for i, l := range s.Bids {
		if i == 0 || l.Price < t.bidFloor {
			t.bidFloor = l.Price
		}
	}
	for i, l := range s.Asks {
		if i == 0 || l.Price > t.askCeil {
			t.askCeil = l.Price
		}
	}
}
