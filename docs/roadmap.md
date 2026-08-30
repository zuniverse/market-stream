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

Split into six steps. Each step is a single short session with a small file
footprint. Commit between steps.

#### M1.1 Core types

`internal/model` only: `Price`, `Qty`, `Symbol`, `EventKind`, the tagged
`Event` struct, and string to fixed-point conversion. No I/O, no dependencies.
Unit tests on parsing, boundary values, and overflow of the price times
quantity product.

Done when: tests pass and nothing else exists yet.

#### M1.2 Instrument metadata

REST client for `exchangeInfo`, extraction of `baseAsset`, `quoteAsset`,
`tickSize` and `stepSize`, in-memory cache, symbol normalisation. Tested
against a recorded JSON fixture, with no network access in tests.

Done when: `BTCUSDT` and `USDTTRY` both split correctly into base and quote.

#### M1.3 Raw transport

Websocket connection, frame reading, raw bytes forwarded with a receive
timestamp on a bounded channel. No decoding at this step. A minimal
`cmd/ingestd` that counts frames and prints a periodic total.

Done when: the stream runs for several minutes and the counter increases.

#### M1.4 Decoding

Raw bytes to normalised `Event` values using `encoding/json` per D8. Table
driven tests over real payloads captured during M1.3 and saved as fixtures.

Done when: trades print with correct prices and quantities, verifiable by eye
against the exchange's own display.

#### M1.5 Reconnection

Exponential backoff with jitter, `serverShutdown` handling, context
cancellation propagation, clean shutdown on SIGINT. A test that simulates a
dropped connection and asserts recovery.

Done when: killing the connection by hand leaves the process running and
reconnected, with no operator action.

#### M1.6 Publisher and first subscriber

`internal/pipeline`: publisher, subscriber registration, one bounded channel
per subscriber, drop-oldest policy with a per-subscriber dropped counter. A
structured logging subscriber. Wired into `ingestd`.

Done when: the full M1 done criterion is met, and the publisher seam is
exercised rather than bypassed.

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
