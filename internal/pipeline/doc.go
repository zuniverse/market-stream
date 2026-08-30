// Package pipeline assembles the processing stages between the exchange
// source and the subscribers.
//
// The shard router distributes events to N shard goroutines by
// hash(symbol) % N. N is set at startup via the -shards flag (D18). Each
// shard goroutine is the exclusive owner of its assigned books and processes
// updates and snapshot queries in strict arrival order with no locking on book
// state (D3).
//
// The publisher fans out to registered subscribers over individual bounded
// channels. When a subscriber channel is full, the oldest message is dropped
// and a per-subscriber counter is incremented. A slow subscriber never applies
// backpressure to the shard stage. This is the boundary between the lossless
// path (transport to book) and the lossy path (book to subscribers).
package pipeline
