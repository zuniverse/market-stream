package record

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/zuniverse/market-stream/internal/model"
)

// Record is one entry read back from a recording. Which field carries the
// content depends on Kind: Payload for a frame or metadata, Snapshot for a
// snapshot.
type Record struct {
	Kind       Kind
	ReceivedAt time.Time
	Payload    []byte
	Snapshot   model.Snapshot
}

// Frame returns the record as a model.Frame. It is only meaningful for
// KindFrame.
func (r Record) Frame() model.Frame {
	return model.Frame{Data: r.Payload, ReceivedAt: r.ReceivedAt}
}

// Reader reads records back out of a zstd stream.
//
// It is not safe for concurrent use.
type Reader struct {
	z    *zstd.Decoder
	buf  *bufio.Reader
	head [recordHead]byte
	n    uint64
}

// NewReader validates the file header and returns a Reader for the records
// after it. Close must be called: the decoder holds goroutines.
func NewReader(r io.Reader) (*Reader, error) {
	z, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("record: open decoder: %w", err)
	}
	rd := &Reader{z: z, buf: bufio.NewReaderSize(z, 64<<10)}

	header := make([]byte, headerSize)
	if _, err := io.ReadFull(rd.buf, header); err != nil {
		z.Close()
		return nil, fmt.Errorf("record: read header: %w", errUnexpectedEOF(err))
	}
	if string(header[:len(magic)]) != magic {
		z.Close()
		return nil, ErrBadMagic
	}
	if v := binary.LittleEndian.Uint16(header[len(magic):]); v != formatVersion {
		z.Close()
		return nil, fmt.Errorf("record: version %d: %w", v, ErrBadVersion)
	}
	return rd, nil
}

// Next returns the next record, or io.EOF at the end of the file.
//
// A file that ends mid-record is reported as corrupt rather than as a clean
// end: a recording is written until the process stops, so a truncated tail is
// a fact about the run and hiding it would make a short replay look complete.
func (r *Reader) Next() (Record, error) {
	if _, err := io.ReadFull(r.buf, r.head[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		return Record{}, fmt.Errorf("record %d: read header: %w", r.n, errUnexpectedEOF(err))
	}
	kind := Kind(r.head[0])
	if err := validKind(kind); err != nil {
		return Record{}, fmt.Errorf("record %d: %w", r.n, err)
	}
	length := binary.LittleEndian.Uint32(r.head[9:])
	if length > MaxPayloadSize {
		return Record{}, fmt.Errorf("record %d: %d bytes: %w", r.n, length, ErrPayloadTooBig)
	}

	rec := Record{
		Kind:       kind,
		ReceivedAt: time.Unix(0, int64(binary.LittleEndian.Uint64(r.head[1:]))).UTC(),
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.buf, payload); err != nil {
		return Record{}, fmt.Errorf("record %d: read payload: %w", r.n, errUnexpectedEOF(err))
	}
	r.n++

	if kind == KindSnapshot {
		if err := json.Unmarshal(payload, &rec.Snapshot); err != nil {
			return Record{}, fmt.Errorf("record %d: decode snapshot: %w", r.n-1, err)
		}
		return rec, nil
	}
	rec.Payload = payload
	return rec, nil
}

// Records returns how many records have been read.
func (r *Reader) Records() uint64 { return r.n }

// Close releases the decoder, which holds goroutines of its own.
func (r *Reader) Close() error {
	r.z.Close()
	return nil
}

// errUnexpectedEOF turns a truncation into ErrCorruptRecord, so that a caller
// can tell a file that ends between records from one that ends inside one.
func errUnexpectedEOF(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: file ends mid-record", ErrCorruptRecord)
	}
	return err
}
