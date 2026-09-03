package record

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/zuniverse/market-stream/internal/model"
)

// Writer serialises records into a zstd stream.
//
// It is not safe for concurrent use: one Recorder goroutine owns one Writer,
// which is what keeps the record order in the file the order the process saw
// (D35).
type Writer struct {
	z    *zstd.Encoder
	head [recordHead]byte
	n    uint64
}

// NewWriter writes the file header to w and returns a Writer for the rest.
// The caller keeps ownership of w and closes it after closing the Writer.
func NewWriter(w io.Writer) (*Writer, error) {
	// Concurrency of one, deliberately. A multi-goroutine encoder splits the
	// input into blocks whose boundaries depend on scheduling, so the same
	// records can compress to different bytes on two runs. The replay
	// determinism this package exists for is easier to trust when the file
	// itself is reproducible, and one goroutine is ample for a feed that
	// produces a few hundred kilobytes a second.
	z, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("record: open encoder: %w", err)
	}
	header := make([]byte, headerSize)
	copy(header, magic)
	binary.LittleEndian.PutUint16(header[len(magic):], formatVersion)
	if _, err := z.Write(header); err != nil {
		z.Close()
		return nil, fmt.Errorf("record: write header: %w", err)
	}
	return &Writer{z: z}, nil
}

// WriteFrame records one raw exchange payload.
func (w *Writer) WriteFrame(f model.Frame) error {
	return w.write(KindFrame, f.ReceivedAt, f.Data)
}

// WriteMeta records the exchange's instrument metadata response, raw.
func (w *Writer) WriteMeta(at time.Time, body []byte) error {
	return w.write(KindMeta, at, body)
}

// WriteSnapshot records one order book snapshot.
//
// This is the one record that is not the bytes that arrived. Encoding the
// normalised value is what lets a replay rebuild books with no exchange
// package, no instrument metadata and no network (D35).
func (w *Writer) WriteSnapshot(at time.Time, s model.Snapshot) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("record: encode snapshot %s: %w", s.Symbol, err)
	}
	return w.write(KindSnapshot, at, payload)
}

// WriteDropped records that n frames were lost before this point because the
// recorder could not keep up.
func (w *Writer) WriteDropped(at time.Time, n uint64) error {
	var payload [8]byte
	binary.LittleEndian.PutUint64(payload[:], n)
	return w.write(KindDrop, at, payload[:])
}

// Records returns how many records have been written.
func (w *Writer) Records() uint64 { return w.n }

func (w *Writer) write(kind Kind, at time.Time, payload []byte) error {
	if len(payload) > MaxPayloadSize {
		return fmt.Errorf("record: %s of %d bytes: %w", kind, len(payload), ErrPayloadTooBig)
	}
	w.head[0] = byte(kind)
	binary.LittleEndian.PutUint64(w.head[1:], uint64(at.UnixNano()))
	binary.LittleEndian.PutUint32(w.head[9:], uint32(len(payload)))
	if _, err := w.z.Write(w.head[:]); err != nil {
		return fmt.Errorf("record: write %s header: %w", kind, err)
	}
	if _, err := w.z.Write(payload); err != nil {
		return fmt.Errorf("record: write %s payload: %w", kind, err)
	}
	w.n++
	return nil
}

// Close flushes the compressed stream. It does not close the underlying
// writer, which the caller owns.
func (w *Writer) Close() error {
	if err := w.z.Close(); err != nil {
		return fmt.Errorf("record: close encoder: %w", err)
	}
	return nil
}
