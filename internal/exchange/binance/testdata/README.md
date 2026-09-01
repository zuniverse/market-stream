# Fixtures

D22 records what happens when these are written by hand: the M1.4 fixtures
were shaped to what the decoder expected, so they confirmed that expectation
and nothing else, and the decoder rejected one hundred percent of live trades
while every test passed.

So: **fixtures here are wire bytes, unedited**. If a fixture needs different
values, capture it again rather than editing it. A fixture you can edit into
agreement with the code is not evidence.

## Provenance

| File | Origin |
| --- | --- |
| `depth_snapshot.json` | `GET /api/v3/depth?symbol=BTCUSDT&limit=20`, 2026-09-01 |
| `depth_stream.ndjson` | `btcusdt@depth@100ms`, same run, one raw payload per line |
| `exchangeinfo.json` | hand-written, trimmed to three symbols |
| `agg_trade.json`, `depth_update.json` | hand-written to the wire format (D22) |

`depth_snapshot.json` and `depth_stream.ndjson` are a **paired capture**: the
websocket was connected and buffering before the snapshot was fetched, which
is the order the resync procedure requires. A snapshot fetched first would
leave a hole between the two that no capture can show. The pair is what makes
the M2.3 classification testable against something other than my reading of
the documentation.

The three hand-written fixtures predate this and are not a paired capture.
Their format is confirmed correct against the captured bytes: the same keys,
the same eight-digit decimal padding, the same `"0.00000000"` for a deletion.

## Reproducing

The capture tool is kept alongside as `capture_tool.go.txt`, with a `.txt`
suffix so the Go build ignores it. It is a throwaway; the real recorder is M3.

```sh
mkdir -p /tmp/capture && cp capture_tool.go.txt /tmp/capture/main.go
cd /tmp/capture
cat > go.mod <<'GOMOD'
module capture

go 1.22

require github.com/gorilla/websocket v1.5.3
GOMOD
GOFLAGS=-mod=mod go run . -symbol BTCUSDT -limit 20 -seconds 10 -snapshot-after 3 -out .
```

The snapshot alone, which is all `depth_snapshot.json` needs:

```sh
curl -s 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20'
```

Recapturing changes every id and price, so the expectations in `depth_test.go`
have to be recomputed. That is the cost of fixtures that are evidence.

## What the capture showed

Observations from 102 consecutive frames, which the tests in `depth_test.go`
pin and which M2.3 is built on:

- **Update ids come in ranges, not one per frame.** Each frame covers `U` to
  `u` inclusive, and in this capture that spanned 6 to 1081 ids. Sequencing
  against a single incrementing counter would be wrong from the first frame.
- **An uninterrupted stream is exactly contiguous**: `U == previous u + 1`,
  with no exceptions across all 102 frames. So a gap is `U > lastApplied + 1`
  and a repeat is `u <= lastApplied`.
- **The snapshot lands inside the stream.** `lastUpdateId` was 99521168819;
  four buffered frames ended at or below it and are already folded into the
  snapshot, and the next frame opened at exactly `lastUpdateId + 1`.
- That last alignment is luck, not contract. The general condition is
  `U <= lastUpdateId + 1 <= u`, which also admits a frame that straddles the
  snapshot id. This capture does not contain a straddling frame, so that
  branch is covered by a synthetic case rather than by evidence.
- **Deletions arrive as `"0.00000000"`**, on both sides, confirming that a
  zero quantity is the removal signal rather than a level with no size.
- **20 levels came back for a `limit=20` request**, so the snapshot is
  `Truncated` and its deepest level bounds what it knows (D25).
