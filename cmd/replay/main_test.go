package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fixture reads a captured file from the exchange package. Those bytes are
// wire evidence whose provenance is documented in one place; a copy here
// would be a second file that nobody recaptures.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "exchange", "binance", "testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

// buildRecording writes a recording from the paired capture, in the order the
// capture was taken: the websocket was connected and buffering before the
// snapshot was fetched, and four frames ended at or below the snapshot id.
// Reproducing that order is what makes the replay exercise the resync
// procedure rather than a book that was anchored before anything arrived.
func buildRecording(t *testing.T) string {
	t.Helper()
	meta := fixture(t, "exchangeinfo.json")
	cache, err := binance.ParseExchangeInfo(meta)
	if err != nil {
		t.Fatalf("ParseExchangeInfo: %v", err)
	}
	inst, ok := cache.LookupByNormalized("BTC-USDT")
	if !ok {
		t.Fatal("the metadata fixture does not list BTC-USDT")
	}
	snap, err := binance.ParseDepth(fixture(t, "depth_snapshot.json"), inst, 20)
	if err != nil {
		t.Fatalf("ParseDepth: %v", err)
	}

	var frames [][]byte
	for _, line := range bytes.Split(fixture(t, "depth_stream.ndjson"), []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			frames = append(frames, line)
		}
	}
	// The capture holds 13 frames: four the snapshot already covers, one
	// that opens at exactly lastUpdateId+1, and eight after it.
	if len(frames) != 13 {
		t.Fatalf("%d captured frames, want the 13 the capture holds", len(frames))
	}

	dir := t.TempDir()
	rec, err := record.NewRecorder(dir, meta, 0, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if err := rec.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	const beforeSnapshot = 4
	for i, payload := range frames {
		if i == beforeSnapshot {
			rec.Snapshot(base.Add(time.Duration(i)*time.Millisecond), snap)
		}
		rec.Frame(model.Frame{Data: payload, ReceivedAt: base.Add(time.Duration(i) * time.Millisecond)})
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	names, err := filepath.Glob(filepath.Join(dir, "*"+record.FileSuffix))
	if err != nil || len(names) != 1 {
		t.Fatalf("expected one recording file, got %v (%v)", names, err)
	}
	return names[0]
}

// runReplay runs the whole binary over the recording and returns the dump.
func runReplay(t *testing.T, file string, shards int) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "dump.txt")
	cfg := config{files: []string{file}, out: out, speed: 0, shards: shards, quiet: true}
	if err := run(context.Background(), cfg, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	return string(data)
}

// TestReplayIsReproducible is the M3 done criterion: two runs of one
// recording produce byte-identical book state.
func TestReplayIsReproducible(t *testing.T) {
	file := buildRecording(t)

	first := runReplay(t, file, 4)
	second := runReplay(t, file, 4)
	if first != second {
		t.Errorf("two replays of one recording differ:\n--- first ---\n%s\n--- second ---\n%s",
			head(first), head(second))
	}

	// The dump has to be worth comparing. An empty book compares equal to
	// another empty book and proves nothing.
	if !strings.Contains(first, "symbol BTC-USDT live=true") {
		t.Fatalf("the replayed book is not live:\n%s", head(first))
	}
	if strings.Contains(first, "crossed=true") {
		t.Errorf("the replayed book is crossed:\n%s", head(first))
	}
	var bids, asks int
	for _, line := range strings.Split(first, "\n") {
		switch {
		case strings.HasPrefix(line, "B "):
			bids++
		case strings.HasPrefix(line, "A "):
			asks++
		}
	}
	if bids < 10 || asks < 10 {
		t.Errorf("dump holds %d bids and %d asks, too few to mean anything", bids, asks)
	}
}

// TestReplayIsIndependentOfShardCount checks that the sharding is a routing
// decision and nothing more: the same recording through one shard and through
// eight must leave the same books.
func TestReplayIsIndependentOfShardCount(t *testing.T) {
	file := buildRecording(t)
	if one, eight := runReplay(t, file, 1), runReplay(t, file, 8); one != eight {
		t.Errorf("shard count changed the result:\n--- 1 shard ---\n%s\n--- 8 shards ---\n%s",
			head(one), head(eight))
	}
}

// TestReplayRejectsBadInput covers the two ways a run cannot start.
func TestReplayRejectsBadInput(t *testing.T) {
	t.Run("no files", func(t *testing.T) {
		err := run(context.Background(), config{quiet: true}, io.Discard)
		if err == nil {
			t.Error("run accepted an empty file list")
		}
	})

	t.Run("recording without metadata", func(t *testing.T) {
		// A file written by hand, with frames but no metadata record: a
		// replay cannot parse a price without the instrument exponents.
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
		w.WriteFrame(model.Frame{Data: []byte(`{"e":"aggTrade"}`), ReceivedAt: time.Unix(0, 1)})
		w.Close()
		f.Close()

		err = run(context.Background(), config{files: []string{name}, quiet: true}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "instrument metadata") {
			t.Errorf("run = %v, want a complaint about the missing metadata", err)
		}
	})
}

func head(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) > 12 {
		lines = append(lines[:12], "...")
	}
	return strings.Join(lines, "\n")
}

// TestReplayRepeatAndProfiles covers the profiling harness: repeating the
// replay must produce the same books every time, which run asserts for
// itself, and the profile files must actually be written.
func TestReplayRepeatAndProfiles(t *testing.T) {
	file := buildRecording(t)
	dir := t.TempDir()
	cfg := config{
		files:      []string{file},
		out:        filepath.Join(dir, "dump.txt"),
		shards:     2,
		repeat:     3,
		cpuProfile: filepath.Join(dir, "cpu.pprof"),
		memProfile: filepath.Join(dir, "heap.pprof"),
		quiet:      true,
	}
	if err := run(context.Background(), cfg, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, name := range []string{"dump.txt", "cpu.pprof", "heap.pprof"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%s is empty", name)
		}
	}

	// The dump from a repeated run must match a single run's.
	if got, want := readFileString(t, cfg.out), runReplay(t, file, 2); got != want {
		t.Error("repeating the replay changed the result")
	}
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
