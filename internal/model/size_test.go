package model_test

import (
	"testing"
	"unsafe"

	"github.com/zuniverse/market-stream/internal/model"
)

// TestEventSize pins what a tagged struct costs. It went from 208 to 224
// bytes at M6, when the two timestamps the tick-to-book histogram needs were
// added, which is exactly the kind of growth this test exists to make
// deliberate.
//
// Original note: D15 chose it over an
// interface to keep the hot path free of boxing, and the price is that an
// Event is as wide as all of its payloads together. That width is multiplied
// by every bounded queue in the process, so it is a number worth knowing and
// worth noticing if it grows.
func TestEventSize(t *testing.T) {
	got := unsafe.Sizeof(model.Event{})
	const want = 224
	if got != want {
		t.Errorf("unsafe.Sizeof(model.Event{}) = %d, want %d.\n"+
			"A wider Event multiplies through every queue: at the ingestd defaults "+
			"that is 8 shards times 4096 slots. Update the figure in "+
			"docs/baseline.md along with this test.", got, want)
	}
	t.Logf("Event %d, Trade %d, BookDelta %d, Snapshot %d, Level %d bytes",
		unsafe.Sizeof(model.Event{}), unsafe.Sizeof(model.Trade{}),
		unsafe.Sizeof(model.BookDelta{}), unsafe.Sizeof(model.Snapshot{}),
		unsafe.Sizeof(model.Level{}))
}
