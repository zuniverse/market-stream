# Baseline artefacts

Reference data and measurements for M4. Everything here is committed so that
a later optimisation can be compared against it rather than against a memory
of how fast things used to be (D5).

## `reference.msr.zst`

One recording, captured from the live Binance spot feed.

| | |
| --- | --- |
| Captured | 2026-09-03, 90 seconds |
| Symbols | `BTC-USDT`, `ETH-USDT`, `SOL-USDT`, `BNB-USDT` |
| Streams | `@aggTrade` and `@depth@100ms` per symbol |
| Snapshot depth | 5000 levels per side, the endpoint maximum |
| Records | 8450: 1 metadata, 4 snapshots, 8445 frames |
| Recorder drops | 0 |
| Compressed size | 1.1 MB |

Reproduced with:

```sh
ingestd -symbols BTC-USDT -symbols ETH-USDT -symbols SOL-USDT -symbols BNB-USDT \
        -check-interval 0 -record-dir ./recordings
```

Recapturing produces a different file: different ids, different prices, a
different number of frames. The numbers in `../baseline.md` are tied to this
file, so replacing it invalidates them.

Replaying it is deterministic, which is what makes it usable as a bench:

```
frames=8764  decode_errors=0  routed=8764  applied=3604  gaps=0  snapshots_used=4
BNB-USDT  bids=5043 asks=4983      BTC-USDT  bids=5394 asks=5084
ETH-USDT  bids=5144 asks=4998      SOL-USDT  bids=5004 asks=5000
```

The frame count is higher than the recorder's, because the recorder's summary
was logged five seconds before the run ended.

## Profiles

`cpu.pprof` and `heap.pprof` come from `cmd/replay` over this file. See
`../baseline.md` for how they were taken and what they say.

```sh
go tool pprof -http=: docs/baseline/cpu.pprof
```

## `bench.txt`

`go test -run xxx -bench . -count=6 ./internal/...`, in the format `benchstat`
reads. An optimisation is justified by a profile and quantified by a
`benchstat` comparison against this file:

```sh
go test -run xxx -bench . -count=6 ./internal/... > after.txt
benchstat docs/baseline/bench.txt after.txt
```

Six runs rather than one, because a single measurement has no variance to
report and `benchstat` will not tell you whether a change is a change.
