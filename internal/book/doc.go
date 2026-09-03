// Package book implements the per-symbol order book and its sequencing.
//
// Book is a pure in-memory structure: it applies level updates, removes a
// level when its quantity reaches zero, answers top-of-book and
// first-N-levels reads, and reports whether the book is crossed. Each side is
// a slice sorted so that index 0 is the top of book (D23).
//
// Sequencer sits beside it rather than inside it, and decides whether a delta
// may be applied at all: stale (already reflected in the book), contiguous
// (apply it), or gapped (an update was lost, so the book is stale until a
// fresh snapshot re-anchors it). It holds no book state and performs no
// recovery.
//
// The resync procedure that acts on a gap is not built yet (M2.4): mark the
// book stale, fetch a REST snapshot, buffer incoming deltas during the fetch,
// then replay the buffer from the snapshot id forward. The Binance depth
// resync specification is the reference.
//
// Neither type is safe for concurrent use. Both are owned exclusively by a
// shard goroutine in internal/pipeline, and a book is read via the shard
// query path, never by locking (D3, D4).
package book
