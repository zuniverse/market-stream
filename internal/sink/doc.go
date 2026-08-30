// Package sink is DEFERRED (v0.1 backlog).
//
// It will implement the Parquet storage writer: zstd-compressed, partitioned
// by symbol and date, written in non-blocking batches with hourly rotation.
// The sink is a subscriber of the publisher, not a pipeline stage; it must
// not block the book stage under any write latency (D9).
package sink
