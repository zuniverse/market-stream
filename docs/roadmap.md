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

Split into six steps. This is the milestone where an error is silent: a wrong
book does not crash, it lies. Each step must be verifiable on its own before
the next is built on top of it.

#### M2.1 Book data structure

`internal/book`: the per-symbol book as a pure in-memory structure. Apply a
level update, remove a level when quantity reaches zero, read top of book,
read the first N levels per side. No sequencing, no network, no concurrency.

Choose and record the representation: sorted slice with binary search, or map
plus sorted key slice. Sorted slice is the default choice, since the hot
operation is reading the top of the book and updates cluster near it.

Done when: unit tests cover insertion, update, removal, and level ordering on
both sides, including the crossed-book case (best bid at or above best ask),
which must be detectable rather than silently accepted.

#### M2.2 Snapshot fetch

REST client for `/api/v3/depth` with the maximum depth limit. Parse into the
structure from M2.1. Handle the documented limitation that levels beyond the
returned depth are unknown rather than absent.

Done when: a fetched snapshot loads into a book and its top of book matches
the exchange's own display.

#### M2.3 Sequence tracking

Track the last applied update id per symbol. Classify each incoming delta as
stale (already applied, discard), contiguous (apply), or gapped (do not
apply, mark the book stale). No recovery yet, only correct classification.

Done when: a table driven test covers all three classifications, including the
boundary conditions at the first delta after a snapshot.

#### M2.4 Resync procedure

The full Binance depth resync: buffer deltas from connection, fetch the
snapshot, discard buffered deltas older than the snapshot id, apply the
remainder in order, then resume live application. Triggered on startup and on
any gap detected in M2.3.

Done when: a deliberately dropped delta produces a resync that restores a
book identical to a freshly fetched snapshot.

#### M2.5 Sharded ownership

`internal/pipeline`: route events by `hash(symbol) % N` to N goroutines, each
the exclusive owner of its books. No mutex on book state. Shard count is a
flag defaulting to `runtime.NumCPU()`.

Done when: the race detector is clean under load, and a `goleak` test shows no
leaked goroutines after shutdown.

#### M2.6 Query path

Add the second channel to the shard loop: snapshot requests carrying a reply
channel, answered by the owning goroutine between deltas. Nothing calls it
yet. Requests must respect context cancellation so a caller that gives up
never blocks the shard.

Done when: a test issues concurrent queries during live updates, the race
detector stays clean, and no lock has been introduced on book state.

#### Correctness harness

Not a step, but a requirement that spans the milestone: a long-running check
that periodically fetches a fresh REST snapshot and compares it against the
locally maintained book, reporting any divergence. This is the only way to
know the book is right rather than merely plausible, and it should exist by
M2.4 rather than at the end.

### M3. Recorder and replayer

Split into four steps. Smaller than M2, but it combines a file format, an
abstraction, and a correctness harness, which are three separate concerns.

#### M3.1 Frame format and writer

Define the on-disk format for raw frames: length prefix, receive timestamp,
payload, per D6. Hourly files, zstd compressed, named so that ordering is
lexicographic. Write a `Recorder` that consumes raw frames and writes them,
with buffering so writes never block the transport stage.

Recording is a subscriber concern, not a transport concern: it must not sit on
the lossless path.

Done when: a recording session produces readable files, and killing the
process mid-write leaves a file that the reader can still consume up to the
last complete frame.

#### M3.2 Reader and `Source` interface

Define `Source` as the pipeline entry point, with `LiveSource` wrapping the
existing transport and `ReplaySource` reading recorded files. Everything
downstream must compile unchanged against either.

Replay honours original inter-frame timing at x1, and supports a speed
multiplier including "as fast as possible". Record the limitation from the
architecture document: accelerated replay flattens burst structure, so it
measures throughput and saturation, not realistic load shape.

Done when: `cmd/replay` runs a recorded file through the full pipeline and
produces the same log output as the live run did.

#### M3.3 Determinism

Replay of the same file must produce identical final book state across runs.
This requires eliminating nondeterminism from the replay path: no wall-clock
dependence in book logic, no map iteration order affecting output, and a
fixed shard count so symbol routing is stable.

Done when: two runs over the same input produce byte-identical book state
dumps, verified by a test rather than by hand.

#### M3.4 Correctness fixtures

Promote the harness from M2 into a repeatable test: a committed short
recording, plus a reference snapshot captured at a known point in it, with a
test that replays and asserts the book matches. Keep the recording small
enough to live in the repository, and note in the README where a larger
corpus can be regenerated.

Done when: `go test ./...` verifies book correctness against real captured
data, with no network access.

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
