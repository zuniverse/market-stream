package record_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

var testMeta = []byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`)

// readFile reads back one recording file whole.
func readFile(t *testing.T, name string) []record.Record {
	t.Helper()
	f, err := os.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer f.Close()

	r, err := record.NewReader(f)
	if err != nil {
		t.Fatalf("NewReader %s: %v", name, err)
	}
	defer r.Close()

	var out []record.Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("%s: Next after %d records: %v", name, len(out), err)
		}
		out = append(out, rec)
	}
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*"+record.FileSuffix))
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestRecorderWritesAndRotates(t *testing.T) {
	dir := t.TempDir()
	rec, err := record.NewRecorder(dir, testMeta, 0, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	hour := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	rec.Frame(model.Frame{Data: []byte("a"), ReceivedAt: hour.Add(time.Minute)})
	rec.Frame(model.Frame{Data: []byte("b"), ReceivedAt: hour.Add(59 * time.Minute)})
	rec.Snapshot(hour.Add(30*time.Minute), model.Snapshot{Symbol: "BTC-USDT", LastID: 7})
	// Past the hour: a new file.
	rec.Frame(model.Frame{Data: []byte("c"), ReceivedAt: hour.Add(time.Hour + time.Minute)})
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	names := files(t, dir)
	if len(names) != 2 {
		t.Fatalf("%d files, want 2: %v", len(names), names)
	}
	if got, want := filepath.Base(names[0]), "2026-09-03T13"+record.FileSuffix; got != want {
		t.Errorf("first file = %s, want %s", got, want)
	}
	if got, want := filepath.Base(names[1]), "2026-09-03T14"+record.FileSuffix; got != want {
		t.Errorf("second file = %s, want %s", got, want)
	}

	first := readFile(t, names[0])
	wantKinds := []record.Kind{record.KindMeta, record.KindFrame, record.KindFrame, record.KindSnapshot}
	if len(first) != len(wantKinds) {
		t.Fatalf("first file has %d records, want %d", len(first), len(wantKinds))
	}
	for i, want := range wantKinds {
		if first[i].Kind != want {
			t.Errorf("first file record %d = %v, want %v", i, first[i].Kind, want)
		}
	}
	if string(first[1].Payload) != "a" || string(first[2].Payload) != "b" {
		t.Errorf("frames came back as %q and %q", first[1].Payload, first[2].Payload)
	}
	if first[3].Snapshot.LastID != 7 {
		t.Errorf("snapshot = %+v", first[3].Snapshot)
	}

	// Every file carries its own metadata, so one hour replays on its own.
	second := readFile(t, names[1])
	if len(second) != 2 || second[0].Kind != record.KindMeta || second[1].Kind != record.KindFrame {
		t.Fatalf("second file = %v", second)
	}
	if string(second[0].Payload) != string(testMeta) {
		t.Errorf("second file metadata = %q", second[0].Payload)
	}

	if st := rec.Stats(); st.Written != 6 || st.Files != 2 || st.Dropped != 0 {
		t.Errorf("stats = %+v, want 6 written, 2 files, 0 dropped", st)
	}
}

// TestRecorderDropsAndMarks fills the queue before the writer goroutine is
// running, which makes the drop deterministic, and then checks that the
// recording says so.
func TestRecorderDropsAndMarks(t *testing.T) {
	dir := t.TempDir()
	rec, err := record.NewRecorder(dir, testMeta, 2, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	now := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	for i := range 5 { // two fit, three are dropped
		rec.Frame(model.Frame{Data: []byte{byte('0' + i)}, ReceivedAt: now})
	}
	if got := rec.Stats().Dropped; got != 3 {
		t.Fatalf("Dropped = %d, want 3", got)
	}

	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	names := files(t, dir)
	if len(names) != 1 {
		t.Fatalf("%d files, want 1", len(names))
	}
	recs := readFile(t, names[0])
	var marker *record.Record
	var frames int
	for i := range recs {
		switch recs[i].Kind {
		case record.KindDrop:
			marker = &recs[i]
		case record.KindFrame:
			frames++
		}
	}
	if frames != 2 {
		t.Errorf("%d frames survived, want the 2 that fit in the queue", frames)
	}
	if marker == nil {
		t.Fatal("the recording does not say that anything was dropped")
	}
	if marker.Dropped != 3 {
		t.Errorf("drop marker reports %d, want 3", marker.Dropped)
	}
}

func TestRecorderValidation(t *testing.T) {
	if _, err := record.NewRecorder(t.TempDir(), nil, 0, nil); err == nil {
		t.Error("NewRecorder accepted a recording with no instrument metadata")
	}
	// Close without Start must not hang or panic.
	rec, err := record.NewRecorder(t.TempDir(), testMeta, 0, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Errorf("Close before Start: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// stubSnapshotter stands in for the depth client.
type stubSnapshotter struct {
	snap model.Snapshot
	err  error
	n    int
}

func (s *stubSnapshotter) Snapshot(context.Context, model.Symbol) (model.Snapshot, error) {
	s.n++
	return s.snap, s.err
}

func TestRecordSnapshots(t *testing.T) {
	dir := t.TempDir()
	rec, err := record.NewRecorder(dir, testMeta, 0, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	inner := &stubSnapshotter{snap: model.Snapshot{Symbol: "BTC-USDT", LastID: 42}}
	tee := record.RecordSnapshots(rec, inner)

	got, err := tee.Snapshot(context.Background(), "BTC-USDT")
	if err != nil || got.LastID != 42 {
		t.Fatalf("Snapshot = %+v, %v", got, err)
	}

	// A failed fetch records nothing: a replay reproduces a failure by
	// running out of snapshots, not by replaying an error.
	inner.err = errors.New("unavailable")
	if _, err := tee.Snapshot(context.Background(), "BTC-USDT"); err == nil {
		t.Error("the tee swallowed the error")
	}
	if inner.n != 2 {
		t.Errorf("inner was called %d times, want 2", inner.n)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var snapshots int
	for _, r := range readFile(t, files(t, dir)[0]) {
		if r.Kind == record.KindSnapshot {
			snapshots++
			if r.Snapshot.LastID != 42 {
				t.Errorf("recorded snapshot = %+v", r.Snapshot)
			}
		}
	}
	if snapshots != 1 {
		t.Errorf("%d snapshots recorded, want 1", snapshots)
	}
}

// TestRecorderRejectsOversizedMeta covers what a live run found and no test
// could: Binance's unfiltered exchangeInfo response is over seventeen
// megabytes, past the per-record limit. Refusing it here means the operator
// learns at startup rather than from an error per frame for the whole run.
func TestRecorderRejectsOversizedMeta(t *testing.T) {
	_, err := record.NewRecorder(t.TempDir(), make([]byte, record.MaxPayloadSize+1), 0, nil)
	if !errors.Is(err, record.ErrPayloadTooBig) {
		t.Errorf("NewRecorder = %v, want %v", err, record.ErrPayloadTooBig)
	}
}
