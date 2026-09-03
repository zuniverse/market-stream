# Measurement baseline

> **This describes the code as of M4, before any optimisation.** M5 acted on
> the first item at the bottom of this page and the shape changed: decoding
> fell from 56.6% of the profile to 42.5% and end-to-end throughput went from
> ~18,000 to ~29,000 frames per second. The numbers below are kept as written
> because they are the baseline every later change is measured against. See
> D42 for what changed and `baseline/cpu-after-m5.pprof` for the profile that
> replaced this one.

What the pipeline costs before anything has been optimised, and where the
cost is. Nothing here is acted on: M4 records, M5 acts, and an optimisation
that cannot point at a line of this document has no evidence behind it.

Everything below is reproducible from the artefacts in `baseline/`. See
`baseline/README.md` for what the recording is and how it was captured.

## Method

```sh
# throughput and profiles
go build -o bin/replay ./cmd/replay
bin/replay -quiet -repeat 25 \
  -cpuprofile docs/baseline/cpu.pprof \
  -memprofile docs/baseline/heap.pprof \
  -out /dev/null docs/baseline/reference.msr.zst

# benchmarks
go test -run xxx -bench . -count=6 ./internal/... > docs/baseline/bench.txt
benchstat docs/baseline/bench.txt
```

`-repeat` runs the whole replay N times, each with a fresh pipeline, because
one pass over the reference takes half a second and a CPU profile of half a
second is fifty samples. Each repeat is an independent replay of the same
input, not a second pass over books that are already current, and `run`
refuses to continue if two repeats produce different book state, so the
profiling run doubles as a determinism check.

Machine: Intel i7-7700HQ, 8 hardware threads, Go 1.25, Linux. The absolute
numbers belong to that machine. The proportions are what the next milestone
argues from.

**Two caveats that every figure here inherits.** Accelerated replay compresses
time and therefore flattens the burst structure of the real feed: this
measures maximum throughput and saturation behaviour, not a realistic load
shape. And a replay reads the recording twice, once to collect the metadata
and snapshots and once to replay the frames (D38), so the profile contains
work a live run never does. That work is named below so it can be subtracted.

## Throughput

| | |
| --- | --- |
| Reference | 8764 frames, 4 symbols, ~40k book levels |
| Wall clock per replay | 472 to 504 ms |
| End to end | **~18,000 frames/s**, including the replay harness |

For scale, the live feed that produced the recording delivered about 95
frames per second. The pipeline is roughly 190 times faster than the load it
was built for, which is the honest way to say that nothing here is under
pressure yet and that the interesting number will come from `cmd/loadgen`.

## Where the CPU goes

From `baseline/cpu.pprof`, 18.02s of samples over 25 replays. Cumulative
shares of the whole profile:

| Stage | Share | |
| --- | --- | --- |
| `binance.(*Decoder).Decode` | **56.6%** | decode and normalise |
| `record` file reading | 16.8% | replay harness, not a production cost |
| `pipeline.(*shard).run` | 15.6% | the book stage |
| everything else | 11.0% | scheduler, GC, channels |

Excluding the harness, the production path is roughly **68% decoding, 19%
book maintenance, 13% everything else**. D8 predicted that `encoding/json`
would be the first bottleneck. It is, and by a wider margin than the guess
implied.

### The decoder scans every frame twice

The interesting part is inside those 56.6%:

| Inside `Decode` | Share of `Decode` |
| --- | --- |
| envelope `json.Unmarshal` | **55.4%** |
| `decodeDepthUpdate` | 37.7% |
| `decodeAggTrade` | 6.7% |

The envelope parse costs more than the payload parse it exists to route to.
It reads exactly two fields, `"e"` and `"data"`, and to do that
`encoding/json.Unmarshal` runs `checkValid` over the entire payload first,
then walks it again to find those two keys. The payload decode then repeats
both steps on the same bytes.

`encoding/json.checkValid` alone is **25.0%** of the whole profile: a quarter
of the pipeline's CPU is spent proving that JSON the venue just sent is
syntactically valid, twice per frame.

That is the dominant cost, and it is the one M5 should act on. What follows
from it is a question for M5 with a `benchstat` comparison attached, not a
decision to take here.

### The book stage

| | Share of profile |
| --- | --- |
| `book.(*side).apply` | 14.5% |
| ... `slices.Delete` + `slices.Insert` memmove | **10.2%** |
| ... `slices.BinarySearchFunc` | 3.9% |

The search is cheap and the shifting is not, which is what D23 said this
milestone should find out.

## A finding that contradicts D23

D23 chose a sorted slice partly on this reasoning:

> The hot write clusters near the top of book, where the memmove after an
> insertion is short.

That is backwards. A slice insertion shifts everything **after** the index, so
a write at the touch moves the whole side and a write deep in the book moves
almost nothing. The benchmark, on a real 5000-level book:

| Delete and re-insert one level | |
| --- | --- |
| at the touch | 2.513 µs |
| 20 levels down | 2.478 µs |
| 4000 levels down | **535.1 ns** |

A write at the top of book is 4.7 times the cost of one deep in it, in the
opposite direction to the sentence above. Updating a level in place, which is
the commonest delta and moves no memory, is ~90 ns wherever it lands.

The conclusion D23 reached still stands: reads are a slice prefix, the
structure is contiguous, and the alternatives lose for the reasons recorded
there. The *argument* for it was wrong in one of its clauses, and that clause
was load-bearing for dismissing a tree. Whether it changes the answer is an
M5 question with a measurement attached.

## A suspicion the profile dismisses

`reflection.md` recorded after M1.6:

> `ParseFixed` allocates twice per call, in `strings.Repeat` and in the
> `intPart+fracPart` concatenation. Noted, not touched.

It does not, on the values the wire actually carries. Binance pads every
decimal to eight fractional digits (D22), which takes the path that neither
repeats nor concatenates:

| | sec/op | allocs/op |
| --- | --- | --- |
| `ParsePrice("78737.26000000", 2)` | 49.32 ns | **0** |
| `ParseQty("0.01234000", 5)` | 46.72 ns | 0 |
| `ParseQty("0.00000000", 5)` | 45.50 ns | 0 |

`model.ParseFixed` is 2.9% of the profile. This is what the rule about
profiling before optimising is for: the allocation was real in the code and
absent from the workload.

## Allocation

From `baseline/heap.pprof`, `alloc_space`:

| | Share |
| --- | --- |
| `zstd` history buffers | 23.8% (harness) |
| `record.(*Reader).Next` payloads | 20.1% (harness) |
| `reflect.growslice` | 17.3% |
| `pipeline.NewRouter` queues | 8.8% (once per repeat) |
| `json` literals and raw messages | 12.4% |

Per frame, from the benchmarks:

| | B/op | allocs/op |
| --- | --- | --- |
| `DecodeMixed` | 3.35 KiB | **54** |
| `DecodeAggTrade` | 1.01 KiB | 21 |
| `DecodeDepthUpdate` | 6.69 KiB | 101.5 |
| `RouteApply` | 16 B | 1 |
| `Publish` | 0 B | 0 |

Fifty-four allocations to decode one frame, against one for the entire book
stage. `reflect.growslice` is the `[][2]string` the depth decoder unmarshals
into: two strings per level, each a fresh allocation, immediately parsed into
two `int64` and thrown away. D1 committed to driving allocations per message
down and verifying the effect on p99 rather than assuming it; this is where
that work has to happen.

## Component costs

Full output in `baseline/bench.txt`, via `benchstat`.

| | sec/op |
| --- | --- |
| `DecodeMixed` | 24.56 µs |
| `DecodeDepthUpdate` | 46.17 µs |
| `DecodeAggTrade` | 8.491 µs |
| `ParseDepth` (5000-level snapshot) | 28.51 µs |
| `book.Reset` (load a snapshot) | 137.1 µs |
| `RouteApply` (one delta through a shard) | 518.5 ns |
| `Query` (read 20 levels through the owning shard) | 1.492 µs |
| `Publish` (fan out to one subscriber) | 144.9 ns |
| `Sequencer.Next` | 7.463 ns |
| `BestBid` | 0.63 ns |
| `record` read of the whole reference | 48.96 ms |

Decoding one frame costs about **50 times** what routing and applying it
costs. Every proportion above says the same thing from a different angle.

`RouteApplyShards4` (504.8 ns) is within noise of the single-shard figure:
four goroutines neither help nor hurt on one symbol, which is D3's stated
consequence measured rather than assumed.

## Two numbers worth watching

**`model.Event` is 208 bytes.** The tagged struct of D15 is as wide as all
its payloads together, and that width multiplies through every bounded queue:
at the `ingestd` defaults, 8 shards of 4096 slots is 6.5 MiB of queue sitting
there whether or not anything is in it, and it is the largest live allocation
in the heap profile. `TestEventSize` pins the figure so that a new payload
field cannot widen it quietly.

**The replay harness is 16.8% of its own profile.** Reading the reference
costs 48.96 ms and 18.95 MiB per pass, and a replay makes two passes. That is
the price of a self-contained recording (D35) and of collecting snapshots
before replaying frames (D38). It is not a production cost, and it is large
enough that leaving it in a quoted figure would be misleading.

## What M5 should look at, in order

1. **The double scan in `Decode`.** The envelope parse is 55% of decode cost
   for two fields, and `checkValid` is a quarter of the whole profile. This
   is the largest single item and the least controversial to change.
2. **Allocations in `decodeDepthUpdate`.** 101 allocations per depth frame,
   most of them strings that live long enough to be parsed into an `int64`.
3. **The top-of-book memmove**, at 10.2% of the profile and with the
   reasoning in D23 now known to be wrong in one clause.

Each needs a `benchstat` comparison against `baseline/bench.txt` before it
counts as done, and each needs the profile above to have named it first.
