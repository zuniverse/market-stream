package record_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

// recordingWith writes one file and returns its path.
func recordingWith(t *testing.T, dir string, fn func(*record.Recorder)) string {
	t.Helper()
	rec, err := record.NewRecorder(dir, testMeta, 0, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	fn(rec)
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	names, err := filepath.Glob(filepath.Join(dir, "*"+record.FileSuffix))
	if err != nil || len(names) == 0 {
		t.Fatalf("no recording written: %v", err)
	}
	return names[0]
}

// drain reads every frame a source produces.
func drain(t *testing.T, files []string, speed float64) ([]model.Frame, *record.ReplaySource) {
	t.Helper()
	out := make(chan model.Frame, 256)
	src, err := record.NewReplaySource(files, out, speed)
	if err != nil {
		t.Fatalf("NewReplaySource: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background()) }()

	var got []model.Frame
	for {
		select {
		case f := <-out:
			got = append(got, f)
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			// Whatever is still queued belongs to this replay.
			for {
				select {
				case f := <-out:
					got = append(got, f)
				default:
					return got, src
				}
			}
		}
	}
}

func TestReplaySourceReadsFramesInOrder(t *testing.T) {
	base := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	file := recordingWith(t, t.TempDir(), func(rec *record.Recorder) {
		rec.Frame(model.Frame{Data: []byte("one"), ReceivedAt: base})
		rec.Snapshot(base.Add(time.Millisecond), model.Snapshot{Symbol: "BTC-USDT", LastID: 11})
		rec.Frame(model.Frame{Data: []byte("two"), ReceivedAt: base.Add(2 * time.Millisecond)})
	})

	frames, src := drain(t, []string{file}, 0)
	if len(frames) != 2 {
		t.Fatalf("replayed %d frames, want 2", len(frames))
	}
	if string(frames[0].Data) != "one" || string(frames[1].Data) != "two" {
		t.Errorf("frames came back as %q and %q", frames[0].Data, frames[1].Data)
	}
	if !frames[0].ReceivedAt.Equal(base) {
		t.Errorf("timestamp = %v, want %v", frames[0].ReceivedAt, base)
	}
	if got := src.Frames(); got != 2 {
		t.Errorf("Frames() = %d, want 2", got)
	}
	if string(src.Meta()) != string(testMeta) {
		t.Errorf("Meta() = %q", src.Meta())
	}
	if got := src.Snapshots().Len(); got != 1 {
		t.Errorf("the store holds %d snapshots, want 1", got)
	}
}

// TestReplaySourceSpansFiles covers an hour boundary: a replay takes the
// files in the order it is given them and produces one stream.
func TestReplaySourceSpansFiles(t *testing.T) {
	dir := t.TempDir()
	hour := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	recordingWith(t, dir, func(rec *record.Recorder) {
		rec.Frame(model.Frame{Data: []byte("a"), ReceivedAt: hour})
		rec.Frame(model.Frame{Data: []byte("b"), ReceivedAt: hour.Add(time.Hour)})
	})
	names, _ := filepath.Glob(filepath.Join(dir, "*"+record.FileSuffix))
	if len(names) != 2 {
		t.Fatalf("%d files, want 2", len(names))
	}

	frames, _ := drain(t, names, 0)
	if len(frames) != 2 || string(frames[0].Data) != "a" || string(frames[1].Data) != "b" {
		t.Fatalf("replayed %v", frames)
	}
}

// TestReplaySourcePaces checks that a speed multiplier is honoured. The
// recorded gap is 200ms; at speed 4 the replay must take appreciably less
// than that and still more than nothing.
func TestReplaySourcePaces(t *testing.T) {
	base := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	file := recordingWith(t, t.TempDir(), func(rec *record.Recorder) {
		rec.Frame(model.Frame{Data: []byte("a"), ReceivedAt: base})
		rec.Frame(model.Frame{Data: []byte("b"), ReceivedAt: base.Add(200 * time.Millisecond)})
	})

	start := time.Now()
	frames, _ := drain(t, []string{file}, 4)
	elapsed := time.Since(start)
	if len(frames) != 2 {
		t.Fatalf("replayed %d frames, want 2", len(frames))
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("replay took %v, too fast to have paced a 200ms gap at speed 4", elapsed)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("replay took %v, closer to real time than to speed 4", elapsed)
	}
}

func TestReplaySourceRespectsCancellation(t *testing.T) {
	base := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	file := recordingWith(t, t.TempDir(), func(rec *record.Recorder) {
		for i := range 5 {
			rec.Frame(model.Frame{Data: []byte("x"), ReceivedAt: base.Add(time.Duration(i) * time.Hour)})
		}
	})

	// An unbuffered channel nobody reads, so Run blocks on the first send.
	src, err := record.NewReplaySource([]string{file}, make(chan model.Frame), 0)
	if err != nil {
		t.Fatalf("NewReplaySource: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Run returned %v with nobody reading, want it to block", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
}

func TestReplaySourceRejectsBadInput(t *testing.T) {
	t.Run("no files", func(t *testing.T) {
		if _, err := record.NewReplaySource(nil, make(chan model.Frame, 1), 0); err == nil {
			t.Error("NewReplaySource accepted an empty file list")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := record.NewReplaySource([]string{"/nonexistent.zst"}, make(chan model.Frame, 1), 0); err == nil {
			t.Error("NewReplaySource accepted a file that does not exist")
		}
	})

	t.Run("no metadata", func(t *testing.T) {
		dir := t.TempDir()
		name := filepath.Join(dir, "bare"+record.FileSuffix)
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		w, err := record.NewWriter(f)
		if err != nil {
			t.Fatal(err)
		}
		w.WriteFrame(model.Frame{Data: []byte("x"), ReceivedAt: time.Unix(0, 1)})
		w.Close()
		f.Close()

		if _, err := record.NewReplaySource([]string{name}, make(chan model.Frame, 1), 0); err == nil {
			t.Error("NewReplaySource accepted a recording with no instrument metadata")
		}
	})
}

// TestSnapshotStoreServesInOrder covers the rule a reproducible replay rests
// on: each request gets the next recorded snapshot for that symbol, and
// running out is an error rather than a repeat.
func TestSnapshotStoreServesInOrder(t *testing.T) {
	base := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	file := recordingWith(t, t.TempDir(), func(rec *record.Recorder) {
		rec.Snapshot(base, model.Snapshot{Symbol: "BTC-USDT", LastID: 1})
		rec.Snapshot(base, model.Snapshot{Symbol: "ETH-USDT", LastID: 100})
		rec.Snapshot(base, model.Snapshot{Symbol: "BTC-USDT", LastID: 2})
		rec.Frame(model.Frame{Data: []byte("x"), ReceivedAt: base})
	})
	src, err := record.NewReplaySource([]string{file}, make(chan model.Frame, 4), 0)
	if err != nil {
		t.Fatalf("NewReplaySource: %v", err)
	}
	store := src.Snapshots()
	ctx := context.Background()

	if got, want := store.Symbols(), []model.Symbol{"BTC-USDT", "ETH-USDT"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Symbols() = %v, want %v", got, want)
	}
	if got := store.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3", got)
	}

	for _, want := range []int64{1, 2} {
		snap, err := store.Snapshot(ctx, "BTC-USDT")
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if snap.LastID != want {
			t.Errorf("LastID = %d, want %d", snap.LastID, want)
		}
	}
	if _, err := store.Snapshot(ctx, "BTC-USDT"); !errors.Is(err, record.ErrNoSnapshot) {
		t.Errorf("a third request = %v, want %v", err, record.ErrNoSnapshot)
	}
	// One symbol running out must not affect another.
	if snap, err := store.Snapshot(ctx, "ETH-USDT"); err != nil || snap.LastID != 100 {
		t.Errorf("ETH-USDT = %+v, %v", snap, err)
	}
	if _, err := store.Snapshot(ctx, "SOL-USDT"); !errors.Is(err, record.ErrNoSnapshot) {
		t.Errorf("an untracked symbol = %v, want %v", err, record.ErrNoSnapshot)
	}
	if got := store.Used(); got != 3 {
		t.Errorf("Used() = %d, want 3", got)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Snapshot(cancelled, "ETH-USDT"); !errors.Is(err, context.Canceled) {
		t.Errorf("Snapshot with a cancelled context = %v", err)
	}
}

// TestReplaySourceReportsRecordedDrops checks that a hole the recorder made
// travels with the recording, so a replay can say whose fault a gap is (D37).
func TestReplaySourceReportsRecordedDrops(t *testing.T) {
	dir := t.TempDir()
	rec, err := record.NewRecorder(dir, testMeta, 1, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	now := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	for range 4 { // one fits, three are dropped
		rec.Frame(model.Frame{Data: []byte("x"), ReceivedAt: now})
	}
	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	names, _ := filepath.Glob(filepath.Join(dir, "*"+record.FileSuffix))

	_, src := drain(t, names, 0)
	if got := src.Drops(); got != 3 {
		t.Errorf("Drops() = %d, want the 3 the recorder lost", got)
	}
}
