// Package aggregate is DEFERRED (v0.1 backlog).
//
// It will implement OHLCV bar construction and streaming indicators,
// consuming normalised events from the shard stage and producing derived
// events for the publisher. It slots between internal/pipeline and the
// publisher without changing either.
package aggregate
