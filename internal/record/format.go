package record

import (
	"errors"
	"fmt"
)

// Kind discriminates what a record holds. A recording is not only websocket
// frames: a book rebuilt from deltas alone is anchored on a REST snapshot and
// parsed with instrument metadata, so a replay that fetched either from the
// network would not be reproducible, and reproducibility is the whole point
// of recording (D5, D35).
type Kind uint8

const (
	// KindFrame is one raw websocket payload, exactly as received.
	KindFrame Kind = iota + 1

	// KindSnapshot is one order book snapshot, normalised. It is the one
	// record that is not raw bytes, for the reason given in D35.
	KindSnapshot

	// KindMeta is the exchange's instrument metadata response, raw. One is
	// written at the head of every file, so that any single file of a
	// recording can be replayed on its own.
	KindMeta

	// KindDrop reports that the recorder could not keep up and lost frames.
	// Its payload is the number lost. It exists so that a recording says what
	// is missing from it: without the marker, a replay sees a sequence gap
	// and cannot tell the venue's fault from the recorder's (D37).
	KindDrop
)

// String returns the kind name, for logs and test failure messages.
func (k Kind) String() string {
	switch k {
	case KindFrame:
		return "frame"
	case KindSnapshot:
		return "snapshot"
	case KindMeta:
		return "meta"
	case KindDrop:
		return "drop"
	default:
		return "invalid"
	}
}

// File layout, little-endian throughout:
//
//	header:  magic [6]byte "MSREC\x00", version uint16
//	record:  kind uint8, receivedAt int64 (Unix nanoseconds), length uint32,
//	         payload [length]byte
//
// The whole file, header included, is then zstd compressed, so a truncated
// file loses its tail rather than becoming unreadable.
//
// Little-endian rather than network byte order: every platform this runs on
// is little-endian, so it is the encoding that costs no byte swapping, and
// portability comes from encoding/binary specifying the order rather than
// from which order was chosen.
const (
	magic         = "MSREC\x00"
	formatVersion = 1

	headerSize = len(magic) + 2
	recordHead = 1 + 8 + 4

	// MaxPayloadSize bounds one record. A corrupt or truncated file must not
	// be able to ask for an arbitrary allocation, and no exchange payload
	// comes close: a 5000-level depth snapshot is a few hundred kilobytes.
	MaxPayloadSize = 16 << 20
)

// Errors a reader returns for a file it cannot trust.
var (
	ErrBadMagic       = errors.New("record: not a recording")
	ErrBadVersion     = errors.New("record: unsupported format version")
	ErrPayloadTooBig  = errors.New("record: payload over the size limit")
	ErrCorruptRecord  = errors.New("record: malformed record")
	ErrUnknownRecKind = errors.New("record: unknown record kind")
)

func validKind(k Kind) error {
	switch k {
	case KindFrame, KindSnapshot, KindMeta, KindDrop:
		return nil
	default:
		return fmt.Errorf("%d: %w", k, ErrUnknownRecKind)
	}
}
