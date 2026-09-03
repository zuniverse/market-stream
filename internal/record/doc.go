// Package record captures raw exchange traffic and replays it.
//
// Recorder writes what a live run received into hourly zstd files: the raw
// websocket frames, the depth snapshots the book stage fetched, and the
// instrument metadata, which every file repeats so that one hour replays on
// its own (D35, D37). It drops rather than blocks when its queue fills, and
// writes a marker saying how many records it lost, so that a file says what
// is missing from it.
//
// ReplaySource reads those files back and satisfies Source, the one-method
// interface the pipeline is fed through. *binance.Transport satisfies it as
// written, so a live run and a replay differ in nothing downstream: the code
// path a profile measures is the production code path, which is the whole
// reason the recorder was built before any optimisation work (D5).
//
// Nothing here imports an exchange package, and a recording holds no type
// that belongs to one. A replay parses frames with whichever decoder wrote
// them, using the metadata out of the file, and needs no network at all.
//
// The accelerated replay caveat is worth repeating wherever its numbers are:
// compressing time flattens the burst structure of the original feed, so an
// accelerated run measures maximum throughput and saturation behaviour rather
// than a realistic load shape.
package record
