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
- Records raw frames for deterministic replay and profiling
- Exposes Prometheus metrics on `/metrics`

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

## Replay

```sh
go build -o bin/replay ./cmd/replay
./bin/replay -file path/to/recording.zst
```

## Observability

Prometheus metrics are available on `http://localhost:9090/metrics` by default
(`-metrics-addr` to override). Key indicators:

| Metric                     | What it shows                                   |
| -------------------------- | ----------------------------------------------- |
| `tick_to_book_latency`     | Cumulative pipeline lag from exchange timestamp |
| `pipeline_queue_depth`     | Saturation per stage                            |
| `book_resync_total`        | Gap detections triggering a resync              |
| `subscriber_dropped_total` | Messages dropped per slow subscriber            |

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
