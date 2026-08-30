# Roadmap

## v0.1: first usable release

One exchange (Binance), a small set of symbols, correct book state, and a
measured performance baseline. The goal of this release is a component that
runs unattended and whose behaviour under load is known rather than assumed.

**In scope:** transport with reconnection, trade stream, order book with full
resync and gap detection, recorder and replayer, profiling baseline, one
measured optimisation pass, metrics endpoint, documentation with figures.

**Out of scope, see the backlog below:** multiple exchanges, Parquet sink,
OHLCV aggregation, dashboards, fault injection tooling, container packaging,
CI.

Sharding is in scope despite bringing no measurable gain at this symbol count.
It is a structural decision, and retrofitting it means reworking the entire
book stage.

### M1. Skeleton and transport

Module, package layout, `Event` and fixed-point price types, websocket
connection to Binance, trade decoding, output. Reconnection with exponential
backoff and jitter belongs here, not bolted on afterwards.

Done when: trades stream continuously and the process recovers from a
forcibly closed connection without operator action.

### M2. Order book

The most delicate component. REST snapshot, delta application, sequence gap
detection, resync procedure, sharded ownership.

Done when: a book tracks live for an extended period without divergence from
a reference snapshot, and a deliberately dropped delta triggers a resync that
restores correct state.

### M3. Recorder and replayer

`Source` interface with live and replay implementations. Raw frame recording
with receive timestamps. Correctness test: replay a file and compare resulting
book state against a reference snapshot.

Done when: `cmd/replay` produces byte-identical book state across two runs of
the same input file.

### M4. Measurement baseline

Replay at accelerated speed, capture CPU and heap profiles, write a Go
benchmark covering the hot loop. Commit the profiles as reference artefacts.
No optimisation at this stage: record what dominates before acting on it.

Done when: a committed baseline exists and the dominant cost is documented.

### M5. Optimisation pass

Act on what the profile identified, most likely allocation rate in decoding.
Measure with `benchstat` before and after. Record the change and its measured
effect in `docs/decisions.md`.

Done when: one optimisation is justified by a profile and quantified by a
before-and-after comparison.

### M6. Observability and documentation

Queue depth gauges, tick-to-book latency histogram, `/metrics` endpoint, end
of run summary (messages processed, sustained throughput, p50/p95/p99, dropped
messages, resyncs triggered). A `goleak` test, a `-race` run, then the README
and operational notes.

Done when: documented behaviour under load rests on measured figures rather
than adjectives.

### Reduced-scope fallback

If book correctness cannot be established at M2, ship v0.1 as a trades-only
pipeline and move the book to v0.2. A correct narrow component is usable; a
book that is subtly wrong is worse than an absent one, because downstream
consumers will trust it.

## Backlog, in suggested order

Each item is scoped to be independently shippable.

1. **Second exchange (Kraken or Coinbase).** Validates the `Exchange`
   abstraction. Success criterion: no file outside `internal/exchange/` is
   modified.

2. **Parquet sink.** Introduces a genuinely slow consumer, and with it real
   backpressure from the consumer side rather than from an accelerated
   producer. Batch accumulation, threshold and time-based flushing,
   non-blocking writes.

3. **`cmd/loadgen`.** Fault injection: forced sequence gaps, disconnection
   mid-burst, a single symbol pushed to a rate no real market produces, to
   verify that sharding does not help in that case and that backpressure takes
   over cleanly.

4. **OHLCV aggregation and streaming indicators.** Slots between shards and
   publisher. Enables validation queries over the stored data.

5. **Query API.** REST or websocket, as a subscriber of the publisher, using
   the book query path built in v0.1. No locks added.

6. **Web dashboard.** Live book and trade tape. Another subscriber, on the
   lossy path, with visible drop counters.

7. **Container packaging, CI, and dashboards.** Turns the existing metrics and
   binaries into something deployable by someone who did not write the code.

8. **SBE binary feed support.** Binance publishes binary-encoded streams with
   smaller payloads and lower latency. Comparing the optimised JSON decoder
   against the binary format determines whether it is worth supporting, and is
   only meaningful once the JSON path is fully profiled.

9. **Rust micro-benchmark of the hot loop.** See D1. Settles the language
   question with numbers.
