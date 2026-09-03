// Package book maintains order books from an exchange's depth stream.
//
// Three layers, each usable and testable on its own:
//
//   - Book is a pure container. It applies level updates, removes a level
//     when its quantity reaches zero, answers top-of-book and first-N-levels
//     reads, and reports whether it is crossed. Each side is a slice sorted
//     so that index 0 is the top of book (D23).
//
//   - Sequencer decides what may reach a book, classifying each delta against
//     the last update id applied as stale, contiguous, or gapped (D26).
//
//   - Tracker puts the two together and runs the resync procedure: buffer
//     deltas while no anchor is held, re-anchor on a snapshot, discard the
//     buffered deltas the snapshot already covers, replay the rest, resume.
//     Startup and gap recovery are the same path (D27).
//
// Tracker.Compare is the correctness harness: it checks a maintained book
// against a freshly fetched snapshot and reports every price at which they
// disagree, restricted to the range both claim to know (D28). What is not
// here is the loop that decides when to run it, which needs a goroutine owner
// and belongs with the shard stage in M2.5.
//
// Nothing in this package performs I/O or starts a goroutine. The snapshot
// fetch is the caller's, because it blocks on the network and the goroutine
// that owns a Tracker owns other symbols' books beside it.
//
// None of these types is safe for concurrent use. Each is owned by exactly
// one shard goroutine, and a book is read via the shard query path, never by
// locking (D3, D4).
package book
