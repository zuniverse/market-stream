package record_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/goleak"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

// TestMain asserts that no test in this package leaves a goroutine behind.
// It earns its keep here: the zstd encoder and decoder both hold goroutines,
// so a missed Close shows up as a leak rather than as a wrong answer.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func at(ns int64) time.Time { return time.Unix(0, ns).UTC() }

func lv(price, qty int64) model.Level {
	return model.Level{Price: model.Price(price), Qty: model.Qty(qty)}
}

// write builds a recording from a sequence of writes and returns the bytes.
func write(t *testing.T, fn func(*record.Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := record.NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	fn(w)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// readAll reads a whole recording.
func readAll(t *testing.T, data []byte) []record.Record {
	t.Helper()
	r, err := record.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	var out []record.Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next after %d records: %v", len(out), err)
		}
		out = append(out, rec)
	}
}

func TestRoundTrip(t *testing.T) {
	snap := model.Snapshot{
		Symbol: "BTC-USDT", LastID: 99617563276, Truncated: true,
		Bids: []model.Level{lv(7873726, 12345), lv(7873725, 1)},
		Asks: []model.Level{lv(7873727, 900), lv(7873728, 2)},
	}
	meta := []byte(`{"symbols":[{"symbol":"BTCUSDT"}]}`)
	frames := []model.Frame{
		{Data: []byte(`{"e":"depthUpdate","U":1,"u":3}`), ReceivedAt: at(1_700_000_000_000_000_001)},
		{Data: []byte(`{"e":"aggTrade","p":"1.00000000"}`), ReceivedAt: at(1_700_000_000_500_000_002)},
		{Data: nil, ReceivedAt: at(1_700_000_001_000_000_003)}, // an empty payload is still a record
	}

	data := write(t, func(w *record.Writer) {
		if err := w.WriteMeta(at(1_700_000_000_000_000_000), meta); err != nil {
			t.Fatalf("WriteMeta: %v", err)
		}
		for _, f := range frames {
			if err := w.WriteFrame(f); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
		}
		if err := w.WriteSnapshot(at(1_700_000_002_000_000_004), snap); err != nil {
			t.Fatalf("WriteSnapshot: %v", err)
		}
		if got := w.Records(); got != 5 {
			t.Errorf("Records() = %d, want 5", got)
		}
	})

	recs := readAll(t, data)
	if len(recs) != 5 {
		t.Fatalf("read %d records, want 5", len(recs))
	}

	if recs[0].Kind != record.KindMeta || !bytes.Equal(recs[0].Payload, meta) {
		t.Errorf("meta record = %+v", recs[0])
	}
	for i, want := range frames {
		got := recs[1+i]
		if got.Kind != record.KindFrame {
			t.Errorf("record %d kind = %v, want %v", 1+i, got.Kind, record.KindFrame)
		}
		if !got.ReceivedAt.Equal(want.ReceivedAt) {
			t.Errorf("record %d time = %v, want %v", 1+i, got.ReceivedAt, want.ReceivedAt)
		}
		if !bytes.Equal(got.Frame().Data, want.Data) {
			t.Errorf("record %d payload = %q, want %q", 1+i, got.Frame().Data, want.Data)
		}
	}

	last := recs[4]
	if last.Kind != record.KindSnapshot {
		t.Fatalf("last record kind = %v, want %v", last.Kind, record.KindSnapshot)
	}
	got := last.Snapshot
	if got.Symbol != snap.Symbol || got.LastID != snap.LastID || got.Truncated != snap.Truncated {
		t.Errorf("snapshot header = %+v, want %+v", got, snap)
	}
	for side, pair := range map[string][2][]model.Level{
		"bid": {got.Bids, snap.Bids},
		"ask": {got.Asks, snap.Asks},
	} {
		if len(pair[0]) != len(pair[1]) {
			t.Errorf("%s side: %d levels, want %d", side, len(pair[0]), len(pair[1]))
			continue
		}
		for i := range pair[0] {
			if pair[0][i] != pair[1][i] {
				t.Errorf("%s level %d = %+v, want %+v", side, i, pair[0][i], pair[1][i])
			}
		}
	}
}

// TestEmptyRecording covers a run that recorded nothing: the file is still a
// valid recording, and reading it ends cleanly rather than reporting damage.
func TestEmptyRecording(t *testing.T) {
	data := write(t, func(*record.Writer) {})
	if recs := readAll(t, data); len(recs) != 0 {
		t.Errorf("read %d records from an empty recording", len(recs))
	}
}

// TestWriteIsDeterministic is what the byte-identical replay claim rests on
// one level down: the same records must compress to the same bytes.
func TestWriteIsDeterministic(t *testing.T) {
	build := func(w *record.Writer) {
		for i := range 200 {
			w.WriteFrame(model.Frame{
				Data:       []byte(`{"e":"depthUpdate","U":1,"u":3,"b":[["1.00","2.00"]]}`),
				ReceivedAt: at(int64(1_700_000_000_000_000_000 + i)),
			})
		}
	}
	first, second := write(t, build), write(t, build)
	if !bytes.Equal(first, second) {
		t.Errorf("two writes of the same records differ: %d and %d bytes", len(first), len(second))
	}
}

func TestReaderRejectsBadHeader(t *testing.T) {
	valid := write(t, func(w *record.Writer) {
		w.WriteFrame(model.Frame{Data: []byte("x"), ReceivedAt: at(1)})
	})

	t.Run("not a recording", func(t *testing.T) {
		var buf bytes.Buffer
		z, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
		z.Write([]byte("NOTAREC\x00\x00"))
		z.Close()
		if _, err := record.NewReader(bytes.NewReader(buf.Bytes())); !errors.Is(err, record.ErrBadMagic) {
			t.Errorf("NewReader = %v, want %v", err, record.ErrBadMagic)
		}
	})

	t.Run("wrong version", func(t *testing.T) {
		var buf bytes.Buffer
		z, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
		header := []byte("MSREC\x00\x00\x00")
		binary.LittleEndian.PutUint16(header[6:], 99)
		z.Write(header)
		z.Close()
		if _, err := record.NewReader(bytes.NewReader(buf.Bytes())); !errors.Is(err, record.ErrBadVersion) {
			t.Errorf("NewReader = %v, want %v", err, record.ErrBadVersion)
		}
	})

	t.Run("not compressed", func(t *testing.T) {
		if _, err := record.NewReader(bytes.NewReader([]byte("MSREC\x00\x01\x00"))); err == nil {
			t.Error("NewReader accepted an uncompressed file")
		}
	})

	t.Run("header truncated", func(t *testing.T) {
		var buf bytes.Buffer
		z, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
		z.Write([]byte("MSR"))
		z.Close()
		if _, err := record.NewReader(bytes.NewReader(buf.Bytes())); !errors.Is(err, record.ErrCorruptRecord) {
			t.Errorf("NewReader = %v, want %v", err, record.ErrCorruptRecord)
		}
	})

	// The valid file must still read, or the cases above prove nothing.
	if recs := readAll(t, valid); len(recs) != 1 {
		t.Errorf("the control recording read %d records, want 1", len(recs))
	}
}

// TestReaderRejectsCorruptRecord covers a file that ends inside a record,
// which is what a process killed mid-write leaves behind.
func TestReaderRejectsCorruptRecord(t *testing.T) {
	var buf bytes.Buffer
	z, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	header := []byte("MSREC\x00\x00\x00")
	binary.LittleEndian.PutUint16(header[6:], 1)
	z.Write(header)
	// A record head claiming 100 bytes, followed by 3.
	head := make([]byte, 13)
	head[0] = byte(record.KindFrame)
	binary.LittleEndian.PutUint64(head[1:], 42)
	binary.LittleEndian.PutUint32(head[9:], 100)
	z.Write(head)
	z.Write([]byte("abc"))
	z.Close()

	r, err := record.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if _, err := r.Next(); !errors.Is(err, record.ErrCorruptRecord) {
		t.Errorf("Next = %v, want %v", err, record.ErrCorruptRecord)
	}
}

func TestReaderRejectsUnknownKind(t *testing.T) {
	var buf bytes.Buffer
	z, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	header := []byte("MSREC\x00\x00\x00")
	binary.LittleEndian.PutUint16(header[6:], 1)
	z.Write(header)
	head := make([]byte, 13)
	head[0] = 99
	z.Write(head)
	z.Close()

	r, err := record.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if _, err := r.Next(); !errors.Is(err, record.ErrUnknownRecKind) {
		t.Errorf("Next = %v, want %v", err, record.ErrUnknownRecKind)
	}
}

func TestWriterRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	w, err := record.NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()
	err = w.WriteFrame(model.Frame{Data: make([]byte, record.MaxPayloadSize+1)})
	if !errors.Is(err, record.ErrPayloadTooBig) {
		t.Errorf("WriteFrame = %v, want %v", err, record.ErrPayloadTooBig)
	}
}

func TestKindString(t *testing.T) {
	for _, tt := range []struct {
		k    record.Kind
		want string
	}{
		{record.KindFrame, "frame"},
		{record.KindSnapshot, "snapshot"},
		{record.KindMeta, "meta"},
		{record.Kind(0), "invalid"},
	} {
		if got := tt.k.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", tt.k, got, tt.want)
		}
	}
}
