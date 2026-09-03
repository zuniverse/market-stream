package record

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuniverse/market-stream/internal/model"
)

// DefaultQueueCap is the recorder's queue when none is given. At a few
// hundred frames a second it is tens of seconds of buffer, which is far more
// than a local disk needs and enough to ride out a stall.
const DefaultQueueCap = 8192

// FileSuffix is appended to the hour a file covers.
const FileSuffix = ".msr.zst"

// RecorderStats counts what a recorder has done.
type RecorderStats struct {
	Written uint64 // records written to disk
	Dropped uint64 // frames lost because the queue was full
	Files   uint64 // files opened
}

// Recorder writes frames, snapshots and metadata to hourly files.
//
// It is a side consumer, not a pipeline stage: it hangs off the frame stream
// and off the snapshot client, and nothing downstream waits for it. That is
// why a full queue drops rather than blocks. Blocking would push disk latency
// back through the decoder into the socket reader, and a websocket that stops
// being read is a websocket the venue disconnects, which costs a reconnect
// and a resync to save a few frames of recording (D37).
//
// The lifecycle is Start -> Frame/Snapshot* -> Close. The recording methods
// are safe to call from several goroutines: snapshots arrive on the shards'
// fetch goroutines while frames arrive on the ingest loop.
type Recorder struct {
	dir      string
	meta     []byte
	log      *slog.Logger
	in       chan entry
	queueCap int

	mu        sync.Mutex
	started   bool
	closeOnce sync.Once
	wg        sync.WaitGroup

	written atomic.Uint64
	dropped atomic.Uint64
	files   atomic.Uint64
}

// entry is one pending record on its way to the writer goroutine.
type entry struct {
	kind     Kind
	at       time.Time
	payload  []byte
	snapshot model.Snapshot
}

// NewRecorder returns a Recorder writing into dir, which is created if it
// does not exist. meta is the exchange's instrument metadata response and is
// written at the head of every file, so that any one file replays on its own.
// A queueCap of zero or less uses DefaultQueueCap.
func NewRecorder(dir string, meta []byte, queueCap int, log *slog.Logger) (*Recorder, error) {
	if len(meta) == 0 {
		return nil, fmt.Errorf("record: recorder: instrument metadata is required")
	}
	if queueCap <= 0 {
		queueCap = DefaultQueueCap
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("record: create %s: %w", dir, err)
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Recorder{
		dir:      dir,
		meta:     meta,
		log:      log,
		in:       make(chan entry, queueCap),
		queueCap: queueCap,
	}, nil
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// Start launches the writer goroutine.
//
// Owner: the Recorder. Exit: the queue is closed by Close. ctx is not an exit
// condition, for the reason D20 gives: a select over the queue and a
// cancelled context discards an arbitrary prefix of a bounded queue that is
// cheap to drain, and here the discarded prefix is data nobody can recover.
func (r *Recorder) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("record: recorder already started")
	}
	r.started = true

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.run(ctx)
	}()
	return nil
}

// Frame queues one raw payload. It never blocks: a full queue drops the frame
// and counts it, and the count is written into the recording as a marker so
// that the file says what is missing from it.
func (r *Recorder) Frame(f model.Frame) {
	r.enqueue(entry{kind: KindFrame, at: f.ReceivedAt, payload: f.Data})
}

// Snapshot queues one order book snapshot. Losing a snapshot costs more than
// losing a frame, since it is what anchors a book on replay, so it is counted
// like any other drop and shows up in the same marker.
func (r *Recorder) Snapshot(at time.Time, s model.Snapshot) {
	r.enqueue(entry{kind: KindSnapshot, at: at, snapshot: s})
}

// Stats returns the counters.
func (r *Recorder) Stats() RecorderStats {
	return RecorderStats{
		Written: r.written.Load(),
		Dropped: r.dropped.Load(),
		Files:   r.files.Load(),
	}
}

// Close stops the writer, flushes the current file and waits for it to be
// closed. Repeated calls after the first do nothing.
func (r *Recorder) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		started := r.started
		r.mu.Unlock()
		if !started {
			return
		}
		close(r.in)
		r.wg.Wait()
	})
	return nil
}

func (r *Recorder) enqueue(e entry) {
	select {
	case r.in <- e:
	default:
		// The queue is full, which means the disk has stalled. Nothing
		// downstream depends on this record, so it goes.
		if r.dropped.Add(1) == 1 {
			r.log.Warn("recorder queue full, dropping records",
				slog.Int("queue_cap", r.queueCap))
		}
	}
}

// run is the writer loop. Everything below runs on its goroutine, so the open
// file and the hour it covers need no synchronisation.
func (r *Recorder) run(ctx context.Context) {
	var (
		w           *Writer
		f           *os.File
		hour        time.Time
		lastDropped uint64
	)
	closeFile := func() {
		if w == nil {
			return
		}
		if err := w.Close(); err != nil {
			r.log.LogAttrs(ctx, slog.LevelError, "recorder flush", slog.String("err", err.Error()))
		}
		if err := f.Close(); err != nil {
			r.log.LogAttrs(ctx, slog.LevelError, "recorder close", slog.String("err", err.Error()))
		}
		w, f = nil, nil
	}
	defer closeFile()

	for e := range r.in {
		// Rotate on the hour the record belongs to, in UTC. Local time would
		// produce two files named the same hour twice a year and a missing
		// hour once, which is not a property a data archive should have.
		if at := e.at.UTC().Truncate(time.Hour); w == nil || !at.Equal(hour) {
			closeFile()
			nf, nw, err := r.openFile(ctx, at)
			if err != nil {
				r.log.LogAttrs(ctx, slog.LevelError, "recorder open", slog.String("err", err.Error()))
				continue
			}
			f, w, hour = nf, nw, at
		}

		// Report the drops before the record that follows them, so the
		// marker sits where the hole is rather than at the end of the file.
		if n := r.dropped.Load(); n > lastDropped {
			if err := w.WriteDropped(e.at, n-lastDropped); err != nil {
				r.log.LogAttrs(ctx, slog.LevelError, "recorder write", slog.String("err", err.Error()))
			}
			lastDropped = n
			r.written.Add(1)
		}

		var err error
		switch e.kind {
		case KindFrame:
			err = w.WriteFrame(model.Frame{Data: e.payload, ReceivedAt: e.at})
		case KindSnapshot:
			err = w.WriteSnapshot(e.at, e.snapshot)
		default:
			err = fmt.Errorf("record: recorder: %s: %w", e.kind, ErrUnknownRecKind)
		}
		if err != nil {
			r.log.LogAttrs(ctx, slog.LevelError, "recorder write", slog.String("err", err.Error()))
			continue
		}
		r.written.Add(1)
	}
}

// openFile creates the file for one hour and writes its header and metadata.
func (r *Recorder) openFile(ctx context.Context, hour time.Time) (*os.File, *Writer, error) {
	name := filepath.Join(r.dir, hour.Format("2006-01-02T15")+FileSuffix)
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("record: create %s: %w", name, err)
	}
	w, err := NewWriter(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if err := w.WriteMeta(hour, r.meta); err != nil {
		w.Close()
		f.Close()
		return nil, nil, err
	}
	r.written.Add(1)
	r.files.Add(1)
	r.log.LogAttrs(ctx, slog.LevelInfo, "recording", slog.String("file", name))
	return f, w, nil
}

// Snapshotter fetches a full order book snapshot for one symbol.
//
// The interface is declared here, on the consumer side, so that the recorder
// can wrap one without importing the pipeline or an exchange package.
type Snapshotter interface {
	Snapshot(ctx context.Context, symbol model.Symbol) (model.Snapshot, error)
}

// RecordSnapshots returns a Snapshotter that records every snapshot inner
// returns before handing it back.
//
// The book stage fetches snapshots on its own schedule, in response to gaps
// nobody can predict, so this is the only place they can be captured: there
// is no separate moment at which the right snapshot could be requested for
// the recording. A failed fetch records nothing, since a replay reproduces
// failures by running out of snapshots rather than by replaying an error.
func RecordSnapshots(r *Recorder, inner Snapshotter) Snapshotter {
	return &snapshotTee{inner: inner, rec: r}
}

type snapshotTee struct {
	inner Snapshotter
	rec   *Recorder
}

func (t *snapshotTee) Snapshot(ctx context.Context, symbol model.Symbol) (model.Snapshot, error) {
	snap, err := t.inner.Snapshot(ctx, symbol)
	if err != nil {
		return snap, err
	}
	t.rec.Snapshot(time.Now(), snap)
	return snap, nil
}
