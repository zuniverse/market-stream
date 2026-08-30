# Architecture

Target architecture. Items marked DEFERRED are not built in the first
iteration but the code must not preclude them. Each seam that exists purely to
make a deferred item cheap later is called out explicitly.

## Pipeline stages

```
  exchange ws  ->  decode  ->  book shards  ->  aggregate  ->  publisher  ->  subscribers
   (transport)   (normalise)   (per symbol)    (DEFERRED)                    (metrics, API,
                                                                              storage, UI)
```

### 1. Transport

One goroutine per websocket connection. Reads frames and forwards raw bytes
plus a receive timestamp. Does no parsing. Owns reconnection: exponential
backoff with jitter, and a full resnapshot on every reconnect.

Binance sends a `serverShutdown` event before planned restarts. Handle it as a
first-class reconnect trigger rather than waiting for the connection to drop.

### 2. Decode and normalise

Converts exchange-specific payloads into internal events: `Trade`,
`BookDelta`, `Snapshot`. Symbol names are normalised at this boundary
(`BTCUSDT` becomes `BTC-USDT`). This is the only stage that knows an exchange
exists.

Prices and quantities become fixed-point `int64` here. Parsing starts with
`encoding/json` to establish an honest baseline, and is replaced only once a
profile identifies it as the bottleneck.

### 3. Book shards

Order book deltas must be applied in strict sequence per symbol. A generic
worker pool breaks that ordering, so routing is `hash(symbol) % N` to N
goroutines, each the exclusive owner of its assigned books. No mutex on book
state.

Each shard loop handles two message kinds:

- updates (deltas and snapshots), applied in order
- queries (snapshot requests), answered by writing to a reply channel carried
  in the request

The query path exists from day one even when nothing queries yet. It is the
seam that lets an HTTP handler read a book later without introducing a lock
and destroying the single-owner property.

Sequence gaps are detected by comparing update ids against the last applied
id. On a gap: mark the book stale, fetch a fresh REST snapshot, buffer deltas
during the fetch, and replay the buffer from the snapshot id forward. The
Binance depth resync procedure is the reference implementation.

### 4. Aggregation (DEFERRED)

OHLCV bar construction and streaming indicators. Consumes normalised events,
produces derived events. Slots between shards and publisher without changing
either.

### 5. Publisher and subscribers

The pipeline terminates in a publisher that fans out to registered
subscribers, not in a single hardcoded consumer. In the first iteration there
is exactly one subscriber (metrics and run summary). This seam is what makes
adding an API, a web UI, and a storage sink additive rather than a rewrite.

Each subscriber has its own bounded channel. A slow subscriber must never
apply backpressure to the book stage. Policy: drop oldest, increment a
per-subscriber dropped-message counter, expose it as a metric.

## Backpressure policy

Two classes of data with different value, and therefore different policies.

**Lossless path** (transport to decode to book): dropping a delta corrupts
book state. Never drop. On overload the correct response is to detect the
sequence gap and resync, which is bounded work with a known cost.

**Lossy path** (publisher to subscribers, metrics, UI): freshness matters more
than completeness. Bounded channel, drop oldest, count the drops.

Stating that not all data in the pipeline is worth the same is the core
architectural claim of this project. It should be visible in the code, not
just in this document.

## Source abstraction

The pipeline entry point is a `Source` interface with two implementations:

- `LiveSource`: real websocket connection
- `ReplaySource`: reads recorded frames from disk, at x1 or accelerated

Everything downstream is identical in both cases. This is what makes the
profiling numbers meaningful: the measured code path is the production code
path.

Recording captures raw frames with receive timestamps, not normalised events.
Recording post-decode would move the decoder out of the measured loop, and the
decoder is exactly what the first profile will implicate. Format: length,
timestamp, payload, in hourly files compressed with zstd.

Accelerated replay compresses time and therefore flattens the real burst
structure. It measures maximum throughput and saturation behaviour, not
realistic load shape. Document this limitation rather than hiding it.

## Exchange abstraction

```go
type Exchange interface {
    Name() string
    Subscribe(ctx context.Context, symbols []string) (<-chan Event, error)
    Snapshot(ctx context.Context, symbol string) (Snapshot, error)
}
```

Binance first, because its depth resync procedure is precisely specified.
Kraken and Coinbase are DEFERRED, but the abstraction is validated by a single
test: adding an exchange must not require editing any file outside
`internal/exchange/`. If it does, the abstraction has leaked.

## Exposure layer (DEFERRED)

A REST or websocket API and a web dashboard are subscribers of the publisher,
nothing more. The transport choice (REST, gRPC, SSE, websocket fan-out) does
not constrain any code written now and is deliberately left open.

The `/metrics` endpoint already puts an `http.Server` in the process, so the
future API attaches without new infrastructure.

## Storage (DEFERRED)

Parquet with zstd compression, partitioned by symbol and date, written in
batches with hourly rotation. Batch accumulation and flushing must not block
the pipeline. DuckDB is the query layer over those files, an analysis tool
rather than a pipeline component.

Note the consequence of deferring this: without a slow sink there is no
natural source of backpressure in the first iteration. Accelerated replay
substitutes for it by making the producer too fast instead of the consumer too
slow. The saturation behaviour observed is real, it just enters from the other
end of the pipe.

## Package layout

```
cmd/ingestd      live ingestion daemon
cmd/replay       replay recorded data through the pipeline
cmd/loadgen      fault injection and synthetic overload (DEFERRED)

internal/exchange/binance    exchange-specific decoding, nothing leaks out
internal/model               normalised event types, fixed-point price type
internal/book                order book, resync, gap detection
internal/pipeline            stages, sharding, backpressure, publisher
internal/record              frame recording and replay
internal/aggregate           bars and indicators (DEFERRED)
internal/sink                storage writers (DEFERRED)
```

## Observability

Prometheus, exposed on `/metrics`. Minimum viable set:

- queue depth per stage, as a gauge
- latency histogram per stage
- tick-to-book latency: exchange event timestamp to local application. This is
  the primary health indicator, since it captures cumulative pipeline lag
  rather than per-stage cost
- dropped messages per subscriber
- resync count and duration per symbol
- reconnect count per exchange
