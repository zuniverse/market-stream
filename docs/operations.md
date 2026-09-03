# Operating market-stream

What the binaries do, what they expose, and how to tell a healthy run from an
unhealthy one. Every figure quoted here was measured; where a number is an
estimate or carries a caveat, the caveat is next to it.

## Running the daemon

```sh
go build -o bin/ingestd ./cmd/ingestd
bin/ingestd -symbols BTC-USDT -symbols ETH-USDT
```

| Flag | Default | |
| --- | --- | --- |
| `-symbols` | `BTC-USDT` | repeatable, one normalised `BASE-QUOTE` pair each |
| `-endpoint` | Binance spot websocket | |
| `-rest-endpoint` | Binance spot REST | instrument metadata and depth snapshots |
| `-shards` | `runtime.NumCPU()` | book shard goroutines |
| `-metrics-addr` | `127.0.0.1:9090` | `/metrics` and `/healthz` |
| `-summary-interval` | `30s` | how often to log progress |
| `-check-interval` | `5m` | book against a fresh snapshot; `0` disables |
| `-record-dir` | none | hourly recordings; empty disables |

The first four names are a commitment and will not be renamed without a
deprecation notice (D19).

It shuts down on SIGINT or SIGTERM, drains every bounded queue, and writes an
end-of-run summary.

## What to watch

The daemon exposes Prometheus text on `/metrics` and a plain `/healthz`.

### The one indicator that matters

`market_stream_check_divergences_total` **should always be zero.** It counts
comparisons where the maintained book disagreed with a freshly fetched
snapshot *at the same update id*. There is no benign reading of that: the same
book, at the same point in the stream, differing from the venue means the book
is wrong, and a wrong book does not crash, it lies.

`market_stream_check_skewed_total` is its harmless sibling and will not be
zero. It counts comparisons where the two disagreed while they were a few
updates apart, which is arithmetic rather than evidence: a snapshot fetched
over the network describes the book a moment before or after the stream does.
A measured run of three symbols over seventy seconds: six checks, four at a
skew of zero and all four in exact agreement across about 9950 levels each,
two skewed, one of them by 162 updates and 60 levels.

### The rest

| Metric | Healthy | |
| --- | --- | --- |
| `market_stream_gaps_total` | rare | a hole in the update ids, answered by a resync |
| `market_stream_refetches_total` | near zero | snapshots that landed too old to bridge |
| `market_stream_fetch_failures_total` | zero | the venue refused a snapshot |
| `market_stream_book_errors_total` | zero | an event a book refused |
| `market_stream_decode_errors_total` | zero | a frame that would not parse |
| `market_stream_shard_queue_depth` | zero | anything sustained means the decoder is behind |
| `market_stream_subscriber_dropped_total` | zero | a slow subscriber losing events |
| `market_stream_record_dropped_total` | zero | the recorder losing frames to disk latency |
| `market_stream_reconnects_total` | flat | a rising one means the venue is unhappy |

Three histograms: `market_stream_tick_to_book_seconds`,
`market_stream_decode_seconds`, `market_stream_resync_seconds`. Each also
exposes a `_negative_total`, counting observations below zero, which for
tick-to-book means the venue's clock is ahead of this machine's.

## Measured behaviour

Three symbols, `@aggTrade` and `@depth@100ms` each, seventy seconds on the
live Binance spot feed, on an i7-7700HQ:

```
frames 3747 at 53.5/s   decode_errors 0   route_errors 0
tick_to_book  p50 175ms   p95 243ms   p99 249ms   (1917 observations)
decode        p50  62µs   p95 283µs   p99 789µs   (3747 observations)
resync        p50 438ms   p95 925ms                (3 observations)
book: applied 1917, gaps 0, resyncs 3, refetches 0, errors 0
checks: 6 run, 0 diverged, 2 skewed        reconnects 0
```

**Read the tick-to-book number carefully.** It is the venue's own event
timestamp to the moment a book applied the event, so it contains, in order:
the depth stream's 100ms aggregation window, the wire time from the venue,
this process's queueing and decoding, and whatever the two clocks disagree
about. Decoding is 62µs of the 175ms. The measurement is a health indicator,
useful because it moves when something is wrong, and it is **not** a measure
of what this process costs. For that, use the replay figures below, which have
no network and no second clock in them.

Percentiles come from bucketed histograms and are interpolated inside the
bucket the quantile lands in, so each is accurate to the width of its bucket
and no better. A value in the last bucket cannot be interpolated at all and is
reported as the largest bound, which understates it.

### What the pipeline itself costs

From replaying the committed reference recording, which has no network in it:

```
8764 frames, 4 symbols, ~40k levels: 293 ms, about 29,000 frames/s
```

Roughly 550 times the rate the live feed delivered. Nothing here is under
pressure, which is the honest way to say that the saturation numbers will come
from `cmd/loadgen`, which is not built. See `baseline.md` for where the time
goes and D42 for the one optimisation made so far.

## Recording and replay

```sh
bin/ingestd -symbols BTC-USDT -record-dir ./recordings
bin/replay -out books.txt ./recordings/2026-09-03T15.msr.zst
```

Recordings are hourly zstd files. Each carries the instrument metadata and the
depth snapshots as well as the raw frames, so a replay needs no network and no
venue (D35). Ninety seconds of four symbols at full depth is about 1.1 MB.

Two runs of one recording produce the same bytes, and so does the same
recording through one shard or eight. `-repeat N` replays N times and refuses
to continue if any two differ.

`-speed 1` replays in real time. The default replays as fast as the pipeline
will take the frames, which measures maximum throughput and saturation
behaviour rather than a realistic load shape: compressing time flattens the
burst structure of the original feed.

## When something goes wrong

**Gaps rising.** The venue is dropping updates or the decoder is behind. Check
`market_stream_shard_queue_depth`: a queue that is not empty means the book
stage is the bottleneck; an empty one means the frames never arrived.

**Refetches rising.** Snapshots are landing older than the buffered deltas,
which means the REST endpoint is lagging the stream. It self-corrects by
asking again; sustained, it means the venue is unwell.

**A book that will not go live.** The log says `check skipped, book not live`.
Its snapshot requests are failing; `market_stream_fetch_failures_total` says
how often. The per-IP request budget is the usual cause: a full-depth snapshot
weighs 250 against 6000 per minute, so a resync storm across many symbols can
exhaust it (D25).

**Recorder drops.** Disk latency. The recording will carry a marker saying how
many frames are missing at that point, so a later replay can tell the
recorder's fault from the venue's (D37).

**Divergences at a skew of zero.** Stop and investigate. Keep the recording if
one was running: it is the only way to reproduce what happened.

## What is not built

`cmd/loadgen`, the Parquet sink, aggregation, the query API, the dashboard,
container packaging and CI are all backlog items, listed in order in
`roadmap.md`. The book query path exists and nothing calls it yet: it is the
seam an API would attach to.
