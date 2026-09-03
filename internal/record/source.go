package record

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuniverse/market-stream/internal/model"
)

// ErrNoSnapshot is returned by a replay's Snapshotter when the recording
// holds no further snapshot for that symbol.
var ErrNoSnapshot = errors.New("record: recording holds no further snapshot for this symbol")

// Source is the entry point of the pipeline: something that produces frames
// until it is done or its context is cancelled.
//
// The interface is one method on purpose. Everything a source has to agree
// with the rest of the process, where the frames go and how many can be in
// flight, is settled by the bounded channel it is given at construction, so
// there is nothing left for the interface to say. *binance.Transport
// satisfies it as written, which is the test of whether the seam was put in
// the right place: the live source needed no wrapper and no edit.
//
// The point of the seam is that everything downstream is identical for a live
// run and a replay, so the code path a profile measures is the production
// code path (D5).
type Source interface {
	Run(ctx context.Context) error
}

// ReplaySource replays recorded frames from one or more files.
//
// It also carries the two things a replayed pipeline needs and cannot fetch:
// the instrument metadata, through Meta, and the recorded snapshots, through
// Snapshots (D35).
type ReplaySource struct {
	files []string
	out   chan<- model.Frame
	speed float64
	meta  []byte
	store *SnapshotStore

	frames atomic.Uint64
	drops  atomic.Uint64
}

// NewReplaySource opens the recording, reads its metadata and its snapshots,
// and returns a source that will replay the frames.
//
// speed scales the recorded inter-frame delays: 1 replays in real time, 2
// twice as fast, and zero or less replays as fast as the pipeline will take
// them. Accelerated replay compresses time and therefore flattens the burst
// structure of the original feed: it measures maximum throughput and
// saturation behaviour, not a realistic load shape. That limitation is worth
// stating rather than hiding, since every number produced from an accelerated
// run inherits it.
//
// The snapshots and the metadata are read in a first pass over the files, so
// a replay decompresses them twice. That is deliberate: the second pass is
// the one a profile measures, and it does nothing but read frames and hand
// them on, exactly as a live socket does.
func NewReplaySource(files []string, out chan<- model.Frame, speed float64) (*ReplaySource, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("record: replay: no files")
	}
	s := &ReplaySource{
		files: files,
		out:   out,
		speed: speed,
		store: newSnapshotStore(),
	}
	if err := s.scan(); err != nil {
		return nil, err
	}
	if len(s.meta) == 0 {
		return nil, fmt.Errorf("record: replay: %s holds no instrument metadata", files[0])
	}
	return s, nil
}

// Meta returns the exchange metadata response recorded at the head of the
// first file. The caller parses it with whichever exchange package wrote it,
// which is why it comes back as bytes: a recording must carry no type that
// belongs to an exchange package (D35).
func (s *ReplaySource) Meta() []byte { return s.meta }

// Snapshots returns the recorded snapshots, as a Snapshotter the book stage
// can use in place of a REST client.
func (s *ReplaySource) Snapshots() *SnapshotStore { return s.store }

// Frames returns how many frames have been replayed.
func (s *ReplaySource) Frames() uint64 { return s.frames.Load() }

// Drops returns how many frames the recording says it lost when it was
// written. They are holes in the data, not in the replay, and a sequence gap
// at one of them is the recorder's doing rather than the venue's (D37).
func (s *ReplaySource) Drops() uint64 { return s.drops.Load() }

// scan reads the recording once, keeping the metadata and the snapshots.
func (s *ReplaySource) scan() error {
	return s.each(func(rec Record) error {
		switch rec.Kind {
		case KindMeta:
			if s.meta == nil {
				s.meta = rec.Payload
			}
		case KindSnapshot:
			s.store.add(rec.Snapshot)
		case KindDrop:
			s.drops.Add(rec.Dropped)
		}
		return nil
	})
}

// Run replays every frame in order, pacing them by the recorded timestamps.
// It returns nil when the recording is exhausted, or ctx.Err() if cancelled.
//
// Owner: the goroutine that calls Run. Exit: the files are exhausted or ctx
// is cancelled.
func (s *ReplaySource) Run(ctx context.Context) error {
	var last time.Time
	err := s.each(func(rec Record) error {
		if rec.Kind != KindFrame {
			return nil
		}
		if s.speed > 0 && !last.IsZero() {
			gap := time.Duration(float64(rec.ReceivedAt.Sub(last)) / s.speed)
			if gap > 0 {
				select {
				case <-time.After(gap):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		last = rec.ReceivedAt

		select {
		case s.out <- rec.Frame():
			s.frames.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return err
}

// each calls fn for every record in every file, in order.
func (s *ReplaySource) each(fn func(Record) error) error {
	for _, name := range s.files {
		if err := s.eachInFile(name, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *ReplaySource) eachInFile(name string, fn func(Record) error) error {
	f, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("record: replay: %w", err)
	}
	defer f.Close()

	r, err := NewReader(f)
	if err != nil {
		return fmt.Errorf("record: replay %s: %w", name, err)
	}
	defer r.Close()

	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("record: replay %s: %w", name, err)
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// SnapshotStore serves the snapshots a recording holds, in the order they
// were recorded, one per request per symbol.
//
// Serving them in order rather than by timestamp is what makes a replay
// reproducible. A book asks for a snapshot when it has no anchor, and the
// answer only has to be a snapshot the stream later catches up with: the
// deltas the snapshot already covers are discarded on replay and the rest are
// applied, so the book converges on the same state whenever the answer
// arrives (M2.4). Matching a request to a recorded timestamp instead would
// tie the result to how fast the replay happened to run.
type SnapshotStore struct {
	mu   sync.Mutex
	all  map[model.Symbol][]model.Snapshot
	next map[model.Symbol]int
	used atomic.Uint64
}

func newSnapshotStore() *SnapshotStore {
	return &SnapshotStore{
		all:  make(map[model.Symbol][]model.Snapshot),
		next: make(map[model.Symbol]int),
	}
}

func (s *SnapshotStore) add(snap model.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.all[snap.Symbol] = append(s.all[snap.Symbol], snap)
}

// Snapshot returns the next unserved snapshot for symbol. Once they are
// exhausted it returns ErrNoSnapshot, rather than repeating the last one: a
// book that has run out is a book the recording cannot anchor, and saying so
// is more useful than serving a snapshot the stream has already passed.
func (s *SnapshotStore) Snapshot(ctx context.Context, symbol model.Symbol) (model.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	i := s.next[symbol]
	if i >= len(s.all[symbol]) {
		return model.Snapshot{}, fmt.Errorf("%s: %w", symbol, ErrNoSnapshot)
	}
	s.next[symbol] = i + 1
	s.used.Add(1)
	return s.all[symbol][i], nil
}

// Symbols returns the symbols the recording holds snapshots for, sorted.
func (s *SnapshotStore) Symbols() []model.Symbol {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Symbol, 0, len(s.all))
	for sym := range s.all {
		out = append(out, sym)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Len returns how many snapshots the recording holds in total.
func (s *SnapshotStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, list := range s.all {
		n += len(list)
	}
	return n
}

// Used returns how many snapshots have been served.
func (s *SnapshotStore) Used() uint64 { return s.used.Load() }
