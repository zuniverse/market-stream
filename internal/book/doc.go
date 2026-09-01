// Package book implements the per-symbol order book.
//
// The Book type is a pure in-memory structure: it applies level updates,
// removes a level when its quantity reaches zero, answers top-of-book and
// first-N-levels reads, and reports whether the book is crossed. Each side is
// a slice sorted so that index 0 is the top of book (D23).
//
// Sequencing is layered on top of it and is not built yet: gap detection
// against update ids (M2.3) and the resync procedure, where the book is
// marked stale, a REST snapshot is fetched, incoming deltas are buffered
// during the fetch, and the buffer is replayed from the snapshot id forward
// (M2.4). The Binance depth resync specification is the reference.
//
// A Book is not safe for concurrent use. It is owned exclusively by a shard
// goroutine in internal/pipeline and is read via the shard query path, never
// by locking (D3, D4).
package book
