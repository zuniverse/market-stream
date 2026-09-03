# market-stream

Real-time crypto market data pipeline in Go. Ingests public websocket feeds,
reconstructs order books, and is built as a study in concurrency design and
profiling under sustained load.

> **Status:** under active development toward v0.1. Not yet suitable for
> production use or as a library dependency.

## What it does

- Connects to the Binance public websocket feed (one exchange for v0.1)
- Decodes and normalises trade and order book events
- Reconstructs per-symbol order books with gap detection and automatic resync
- Checks each book against a fresh exchange snapshot on a timer, and reports
  any divergence
- Records raw frames, snapshots and instrument metadata for deterministic
  replay with no network
- Exposes Prometheus metrics on `/metrics`

Not built yet: the measurement baseline and its profiles (M4), the
optimisation pass (M5), latency histograms (M6), and everything in the
backlog.

## Design notes

Prices and quantities are fixed-point `int64`, never `float64`. Books are
sharded by symbol; each shard is owned exclusively by one goroutine with no
locks on book state. Every channel is bounded with an explicit overflow policy.

See [docs/architecture.md](docs/architecture.md) for the full pipeline design
and [docs/decisions.md](docs/decisions.md) for every design decision and its
rejected alternatives.

## Usage

```sh
go build -o bin/ingestd ./cmd/ingestd
./bin/ingestd -symbols BTC-USDT -symbols ETH-USDT
```

Run `./bin/ingestd -help` for the full flag list.

## Recording and replay

```sh
./bin/ingestd -symbols BTC-USDT -record-dir ./recordings
```

Recordings are hourly zstd files. Each one carries the instrument metadata and
the depth snapshots as well as the raw frames, so a replay needs no network
and no venue.

```sh
go build -o bin/replay ./cmd/replay
./bin/replay -out books.txt ./recordings/2026-09-03T13.msr.zst
```

`replay` runs the recording through the same pipeline the daemon uses and
writes the resulting book state. Two runs of one file produce the same bytes,
which is what makes a before-and-after comparison of an optimisation mean
anything. `-speed 1` replays in real time; the default replays as fast as the
pipeline will take the frames, which measures maximum throughput and
saturation rather than a realistic load shape.

## Observability

Prometheus metrics are available on `http://127.0.0.1:9090/metrics` by default
(`-metrics-addr` to override), alongside `/healthz`. Key indicators:

| Metric                                  | What it shows                          |
| --------------------------------------- | -------------------------------------- |
| `market_stream_gaps_total`              | Holes detected in an update id sequence |
| `market_stream_snapshots_total`         | Snapshots that anchored a book          |
| `market_stream_check_divergences_total` | Checks that found a book to be wrong    |
| `market_stream_shard_queue_depth`       | Saturation per shard                    |
| `market_stream_subscriber_dropped_total`| Events dropped per slow subscriber      |

The tick-to-book latency histogram, which is the primary health indicator, is
M6 and is not exposed yet.

## Project layout

```
cmd/ingestd      live ingestion daemon
cmd/replay       replay recorded data through the pipeline

internal/exchange/binance    Binance transport, decoding, normalisation
internal/model               normalised event types, fixed-point Price
internal/book                order book, gap detection, resync
internal/pipeline            sharding, backpressure, publisher
internal/record              Source interface, frame recorder, replayer
```

## Roadmap

See [docs/roadmap.md](docs/roadmap.md).
