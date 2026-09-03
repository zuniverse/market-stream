// Package metrics holds the small instruments the pipeline reports with.
//
// It is deliberately not a metrics library. There is one type here, a
// duration histogram with fixed bucket bounds, because that is what the
// observability set in architecture.md needs beyond the counters the stages
// already keep. It renders itself in the Prometheus text format and answers
// approximate quantiles for the end-of-run summary.
//
// D34 expected the client library to arrive with the histograms. Having
// written one, the trade did not hold: a bucketed histogram is a bounds
// slice, an atomic per bucket, and a loop over cumulative counts to render.
// See D43.
package metrics
