// Package book implements the per-symbol order book.
//
// It handles delta application in strict sequence, sequence gap detection,
// and the resync procedure: on a gap the book is marked stale, a REST
// snapshot is fetched, incoming deltas are buffered during the fetch, and the
// buffer is replayed from the snapshot sequence id forward. The Binance depth
// resync specification is the reference implementation.
//
// A book is not safe for concurrent use. It is owned exclusively by a shard
// goroutine in internal/pipeline and is read via the shard query path, never
// by locking (D3, D4).
package book
