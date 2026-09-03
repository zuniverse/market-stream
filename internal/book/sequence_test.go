package book

import (
	"math/rand"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

// delta builds a book delta carrying only the ids. The sequencer never looks
// at the levels, so leaving them empty keeps the tables about sequencing.
func delta(first, last int64) model.BookDelta {
	return model.BookDelta{Symbol: "BTC-USDT", FirstID: first, LastID: last}
}

// step is one delta pushed through the sequencer and the state expected
// afterwards. Asserting the position after every step is the point: a
// classification that is right while the last applied id has silently drifted
// would pass a test that only compared classes.
type step struct {
	d      model.BookDelta
	want   Class
	last   int64
	resync bool
}

func TestSequencerClassify(t *testing.T) {
	tests := []struct {
		name string
		// snapshot is the id given to Reset before the steps run. A negative
		// value means Reset is never called, so the sequencer stays at its
		// zero value.
		snapshot int64
		steps    []step
	}{
		{
			name:     "zero value needs a snapshot before anything",
			snapshot: -1,
			steps: []step{
				{delta(1, 2), Gapped, 0, true},
				{delta(3, 4), Gapped, 0, true},
			},
		},
		{
			name:     "first delta starting exactly after the snapshot",
			snapshot: 100,
			steps:    []step{{delta(101, 105), Contiguous, 105, false}},
		},
		{
			name:     "first delta straddling the snapshot id",
			snapshot: 100,
			// U <= lastUpdateId+1 <= u: the delta covers updates the snapshot
			// already reflects and updates it does not. Binance names this
			// case explicitly as the one to apply after a resync.
			steps: []step{{delta(98, 103), Contiguous, 103, false}},
		},
		{
			name:     "first delta ending exactly at the snapshot id",
			snapshot: 100,
			steps:    []step{{delta(96, 100), Stale, 100, false}},
		},
		{
			name:     "first delta entirely before the snapshot",
			snapshot: 100,
			steps:    []step{{delta(90, 95), Stale, 100, false}},
		},
		{
			name:     "first delta leaving a hole after the snapshot",
			snapshot: 100,
			// The snapshot was fetched too late, or an update was lost while
			// it was in flight. Either way update 101 is gone.
			steps: []step{{delta(102, 110), Gapped, 100, true}},
		},
		{
			name:     "contiguous chain advances one delta at a time",
			snapshot: 100,
			steps: []step{
				{delta(101, 101), Contiguous, 101, false},
				{delta(102, 108), Contiguous, 108, false},
				{delta(109, 109), Contiguous, 109, false},
			},
		},
		{
			name:     "a repeated delta mid-stream is stale, not a gap",
			snapshot: 100,
			steps: []step{
				{delta(101, 110), Contiguous, 110, false},
				{delta(101, 110), Stale, 110, false},
				{delta(111, 112), Contiguous, 112, false},
			},
		},
		{
			name:     "an overlapping delta mid-stream is applied",
			snapshot: 100,
			steps: []step{
				{delta(101, 110), Contiguous, 110, false},
				// Restates part of what was applied and extends past it.
				// Absolute quantities make the overlap a no-op, so forcing a
				// resync here would buy nothing.
				{delta(105, 120), Contiguous, 120, false},
			},
		},
		{
			name:     "a gap latches until a new snapshot arrives",
			snapshot: 100,
			steps: []step{
				{delta(101, 110), Contiguous, 110, false},
				{delta(112, 115), Gapped, 110, true},
				// Contiguous with the delta that caused the gap, but the book
				// is missing update 111 and cannot be trusted again until it
				// is re-anchored.
				{delta(116, 120), Gapped, 110, true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s Sequencer
			if tt.snapshot >= 0 {
				s.Reset(tt.snapshot)
				if s.NeedsResync() {
					t.Fatalf("NeedsResync() is true right after Reset(%d)", tt.snapshot)
				}
				if got := s.LastID(); got != tt.snapshot {
					t.Fatalf("LastID() = %d after Reset(%d)", got, tt.snapshot)
				}
			} else if !s.NeedsResync() {
				t.Fatalf("NeedsResync() is false on a sequencer that has never been Reset")
			}
			for i, st := range tt.steps {
				got := s.Next(st.d)
				if got != st.want {
					t.Errorf("step %d: Next(U=%d,u=%d) = %v, want %v",
						i, st.d.FirstID, st.d.LastID, got, st.want)
				}
				if last := s.LastID(); last != st.last {
					t.Errorf("step %d: LastID() = %d, want %d", i, last, st.last)
				}
				if resync := s.NeedsResync(); resync != st.resync {
					t.Errorf("step %d: NeedsResync() = %v, want %v", i, resync, st.resync)
				}
			}
		})
	}
}

// TestSequencerResetClearsGap covers the transition M2.4 depends on: once a
// fresh snapshot has been loaded, the sequencer classifies again from the new
// anchor and the buffered deltas either side of it fall out correctly.
func TestSequencerResetClearsGap(t *testing.T) {
	var s Sequencer
	s.Reset(100)
	if got := s.Next(delta(105, 110)); got != Gapped {
		t.Fatalf("Next after a hole = %v, want %v", got, Gapped)
	}

	s.Reset(150)
	if s.NeedsResync() {
		t.Fatal("NeedsResync() is still true after a fresh snapshot")
	}
	// Deltas buffered during the fetch: those the snapshot already covers are
	// discarded, and the first one past it resumes the stream.
	if got := s.Next(delta(140, 149)); got != Stale {
		t.Errorf("delta below the new snapshot id = %v, want %v", got, Stale)
	}
	if got := s.Next(delta(148, 152)); got != Contiguous {
		t.Errorf("delta straddling the new snapshot id = %v, want %v", got, Contiguous)
	}
	if got := s.LastID(); got != 152 {
		t.Errorf("LastID() = %d, want 152", got)
	}
}

// TestSequencerDroppedDelta drives a long contiguous stream with exactly one
// delta removed and asserts the sequencer reports exactly one gap, at the
// removal point. It is the classification half of the M2.4 done criterion,
// before there is any recovery to test.
func TestSequencerDroppedDelta(t *testing.T) {
	rng := rand.New(rand.NewSource(2))

	const snapshotID = 1000
	var stream []model.BookDelta
	next := int64(snapshotID + 1)
	for range 200 {
		span := int64(rng.Intn(4)) // deltas cover one to four updates
		stream = append(stream, delta(next, next+span))
		next += span + 1
	}
	drop := 137

	var s Sequencer
	s.Reset(snapshotID)
	for i, d := range stream {
		if i == drop {
			continue
		}
		got := s.Next(d)
		switch {
		case i < drop && got != Contiguous:
			t.Fatalf("delta %d before the drop = %v, want %v", i, got, Contiguous)
		case i > drop && got != Gapped:
			t.Fatalf("delta %d after the drop = %v, want %v", i, got, Gapped)
		}
	}
	if !s.NeedsResync() {
		t.Error("NeedsResync() is false after a dropped delta")
	}
	if want := stream[drop-1].LastID; s.LastID() != want {
		t.Errorf("LastID() = %d, want %d, the last id applied before the drop", s.LastID(), want)
	}
}

func TestClassString(t *testing.T) {
	for _, tt := range []struct {
		c    Class
		want string
	}{
		{Stale, "stale"},
		{Contiguous, "contiguous"},
		{Gapped, "gapped"},
		{Class(0), "invalid"},
		{Class(9), "invalid"},
	} {
		if got := tt.c.String(); got != tt.want {
			t.Errorf("Class(%d).String() = %q, want %q", tt.c, got, tt.want)
		}
	}
}
