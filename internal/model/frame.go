package model

import "time"

// Frame is one raw message received from an exchange, with the time it
// arrived, before any parsing.
//
// It lives here rather than in an exchange package because it is not
// exchange-specific: a frame is bytes and a timestamp whatever produced it,
// and the recorder, the replayer and the Source seam all handle frames
// without knowing which venue they came from.
//
// Recording captures frames exactly as received rather than the events they
// decode into, so that the decoder stays inside the loop a profile measures
// (D6). Data is the payload as it came off the wire and must not be modified
// by a consumer: the recorder may still hold it.
type Frame struct {
	Data       []byte
	ReceivedAt time.Time
}
