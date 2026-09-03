// Package pipeline assembles the processing stages between the exchange
// source and the subscribers.
//
// The Router is the book stage. It sends each event to the shard that owns
// that symbol, by hash(symbol) % N, and each shard goroutine is the exclusive
// owner of every book assigned to it: no mutex on book state, and deltas for
// one symbol applied in strict arrival order (D3). N defaults to
// runtime.NumCPU(), which is what the -shards flag will pass in once ingestd
// builds a router (D18). Each shard also
// drives the resync procedure for its symbols, issuing snapshot requests on
// separate goroutines so that a book waiting on the network never stops the
// books beside it (D30).
//
// The Publisher fans out to registered subscribers over individual bounded
// channels, dropping the oldest queued event when one is full and counting
// the drop per subscriber.
//
// The two stages have opposite policies when a queue fills, and that is the
// point. Router.Route blocks: this is the lossless path, where a dropped
// delta corrupts a book, so overload propagates backwards as backpressure and
// the correct response is a resync, which is bounded work. Publisher.Publish
// never blocks: past the book stage freshness beats completeness, so a slow
// subscriber loses events rather than slowing the pipeline. Not all data in
// this pipeline is worth the same, and the difference is meant to be visible
// in the code rather than only in the architecture document.
//
// Router.Query reads a book without taking a lock: the request carries the
// channel it is answered on, and the owning shard answers it between two
// deltas, handing back a copy (D4, D31). Nothing in the binaries calls it
// yet. It exists first so that the seam is there before the pressure to take
// the shortcut is.
package pipeline
