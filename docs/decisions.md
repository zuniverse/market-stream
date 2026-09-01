# Decisions

Each entry records what was chosen, what was rejected, and why. The rejected
alternatives matter as much as the choices: without them, a later reader (or a
later coding session) will keep proposing the option that was already
considered and dismissed.

Add an entry whenever a design decision is made. Do not delete entries when a
decision is reversed. Supersede them, and keep the history.

---

## D1. Go rather than Rust

**Chosen:** Go for the entire pipeline.

**Rejected:** Rust, on the argument that high-volume ingestion belongs to a
language without garbage collection.

**Why:** On public crypto websocket feeds the dominant cost is JSON decoding
and the allocations it produces, not the language runtime. The volume regime
where a GC becomes disqualifying is binary exchange feeds with microsecond
tick-to-trade budgets, which is not this project.

The only serious argument for Rust is GC jitter at high allocation rates.
That is a real cost, and it is managed directly: allocations per message are
driven down, `GOGC` and `GOMEMLIMIT` are set deliberately rather than left at
defaults, and the effect is verified on p99 rather than assumed. Go also buys
faster iteration, a simpler deployment story, and an ecosystem where most of
the surrounding infrastructure already lives.

**Open follow-up:** a micro-benchmark of the hot loop in Rust, compared
against the optimised Go version, would settle the question with numbers
rather than assumption. Bounded scope, not required.

---

## D2. Fixed-point `int64`, never `float64`

**Chosen:** prices and quantities as `int64` with a decimal exponent.

**Rejected:** `float64`, which is more convenient to parse and to arithmetic.

**Why:** binary floating point cannot represent decimal prices exactly.
Accumulated error in book state and in aggregation produces quantities that
fail to net to zero and prices that drift from the exchange's own values. This
is a standard disqualifying error in financial systems.

---

## D3. Sharding by symbol, not a worker pool

**Chosen:** `hash(symbol) % N` routing to N goroutines, each the exclusive
owner of its books.

**Rejected:** a generic worker pool consuming from a shared queue. Also
rejected: a single book map guarded by `sync.RWMutex`.

**Why:** book deltas must be applied in strict per-symbol sequence. A worker
pool destroys that ordering. A mutex preserves correctness but serialises the
hottest path in the system and makes contention the bottleneck.

Single ownership gives ordering guarantees, zero locking on book state, and
parallelism across symbols. Its cost is that a single very hot symbol cannot
be parallelised, which is accepted: that case is handled by backpressure, not
by more workers.

---

## D4. Read access to books by message, not by lock

**Chosen:** book snapshot requests are sent on the shard channel with a reply
channel inside, and answered by the owning goroutine between deltas.

**Rejected:** a `sync.RWMutex` on book state so HTTP handlers can read
directly.

**Why:** the mutex is the natural reflex when an API is added later, and it
silently destroys the property established in D3. Building the query path from
the start means the seam exists before the pressure to take the shortcut does.

---

## D5. Recorder and replayer built first, not last

**Chosen:** raw frame recording and replay implemented before any
optimisation work.

**Rejected:** profiling against the live feed and adding replay later if
needed.

**Why:** a live feed is not reproducible, so before-and-after comparisons
against it are meaningless. Replay gives a deterministic bench, real
integration test data, and the ability to saturate the system deliberately.
Everything in the measurement dossier depends on it, so it cannot be an
afterthought.

---

## D6. Record raw frames, not normalised events

**Chosen:** recording captures the websocket payload exactly as received, with
a receive timestamp.

**Rejected:** recording decoded and normalised events, which would produce
smaller files and simpler replay.

**Why:** recording post-decode moves the decoder outside the measured loop.
The decoder is the most likely first bottleneck, so excluding it defeats the
purpose of the bench.

---

## D7. Real captured data, not a synthetic generator

**Chosen:** replay of recorded live market data as the primary test corpus.

**Rejected:** a synthetic market data generator as the primary source.

**Why:** a generator only produces what was modelled. Captured data contains
what was not anticipated: open-hour bursts, sequence gaps, books emptying at
once, occasionally absent fields, dead pairs alongside saturated ones. That is
the material that breaks pipelines.

Synthetic generation is still legitimate for `cmd/loadgen`, whose role is
different: fault injection and pathological cases that will not appear in an
hour of capture. That is fault injection, not market simulation.

---

## D8. Baseline with `encoding/json` before optimising

**Chosen:** first implementation uses the standard library decoder.

**Rejected:** starting directly with a hand-written scanner or a fast JSON
library.

**Why:** a hand-rolled decoder is harder to maintain and easier to get subtly
wrong than the standard library. It is worth that cost only where a profile
shows it pays, and only with a measured figure attached. Starting optimised
means carrying the maintenance burden without evidence that it was warranted,
and losing the reference point needed to verify any later regression.

---

## D9. Parquet as the storage sink, not TimescaleDB

**Chosen (deferred implementation):** Parquet with zstd, DuckDB as the query
layer.

**Rejected:** TimescaleDB or PostgreSQL.

**Why:** Timescale imposes an operational dependency on every adopter: a
running database, schema migrations, backups, and capacity planning. For a
component meant to be dropped into an existing system, that is a significant
integration cost, and it duplicates infrastructure most adopters already run
in their own preferred form.

Parquet gives the same I/O characteristics with no operational dependency, in
the standard format for financial data lakes, and produces files that any
downstream consumer can read without going through this project. A database
sink remains implementable as an additional subscriber for users who want one,
without becoming a requirement for those who do not.

---

## D10. Project name: market-stream

**Chosen:** `market-stream`.

**Rejected:** `go-realtime-market-ingest` (the `go-` prefix is a library
convention and GitHub already displays the language; four words is long in a
module path). `feedhandler` and `tickpipe` (stronger domain signal, less
immediately readable to a non-specialist). Any mythological or invented name
(works for a component whose context is supplied by its surrounding system,
not for a dependency that has to explain itself at the point of adoption).

**Why:** "stream" states the mechanism without making a performance claim that
invites challenge, unlike "realtime", which in finance implies bounded
deterministic latency. The realtime characteristic belongs in the README
description where it arrives accompanied by measured numbers.

---

## D11. Everything stays under `internal/` for now

**Chosen:** all packages under `internal/`. The supported reuse surface is the
`cmd/` binaries, their configuration, and the files and metrics they produce.

**Rejected:** promoting packages to importable paths now, so the project can
be taken as a library dependency from the start.

**Why:** Go forbids importing `internal/` from outside the module, and that
restriction is the point. While signatures are still moving, which they will
be throughout the first release, `internal/` means a rename or a type change
is a local edit rather than a breaking change for an unknown consumer. There
are no external consumers yet, so the restriction costs nothing today.

The asymmetry favours waiting. Promoting a package out of `internal/` later is
painless for users, since nobody could depend on it before, and costs only an
import rename inside this repository. Making a public package private again
breaks every consumer that had adopted it.

**Follow-up when demand appears:** promote `model` first (event types,
fixed-point price). It is the smallest package, the most stable, and the one a
third party actually needs in order to read the produced data. Promotion turns
its signature into a compatibility commitment, so it should happen once the
type has stopped changing, not before.

---

## D12. MIT license

**Chosen:** MIT.

**Rejected:** Apache 2.0, which additionally grants patent rights and is
sometimes required by corporate legal review before taking a dependency. Also
rejected: no license, which under default copyright makes the code legally
unusable by anyone else regardless of the repository being public.

**Why:** MIT is short, universally recognised, and imposes no obligation
beyond attribution. The patent grant in Apache 2.0 protects against a risk
that does not apply here, since no patentable technique is involved. Simplicity
wins.

---

## D13. Module path

**Chosen:** `github.com/zuniverse/market-stream`.

**Rejected:** A vanity domain path (requires hosting infrastructure). A plain
`market-stream` without a host prefix (not a valid Go module path). A
`go-market-stream` form (the `go-` prefix is a library naming convention that
does not apply to a binary-first project; see D10).

**Why:** The module path must be rooted to avoid collisions in the Go module
proxy. Using the canonical GitHub repository path is the simplest choice that
works without additional infrastructure.

---

## D14. `Price` type representation

**Chosen:** `type Price int64`. The decimal exponent is tracked as an attribute
of the instrument, not stored inside the value.

**Rejected:** `type Price struct { Value int64; Exp int8 }` -- self-describing
but 12 bytes wide (padded to 16 on most platforms), and every arithmetic
operation requires a helper. `type Price int64` with the exponent carried
alongside each individual value in message structs -- redundant; forces every
downstream stage to carry the exponent even when it never changes.

**Why:** Within a given symbol all prices share the same exponent; the
instrument config is the natural and non-redundant home for it. A single
`int64` copies and compares at zero overhead. Prices are never mixed across
symbols in the book or aggregation stages, so "only comparable within the same
symbol" is enforced by the data flow rather than by the type system.

---

## D15. `Event` polymorphism

**Chosen:** A tagged struct with a `Kind` discriminator and embedded value
fields for each payload type (`Trade`, `BookDelta`, `Snapshot`).

**Rejected:** An `Event` interface with a `Kind() EventKind` method -- requires
type assertions in every consumer and boxes the payload onto the heap on every
construction; significant cost on the hot path. Separate typed channels per
event kind -- forces the pipeline to maintain parallel channel sets and
complicates routing.

**Why:** The tagged struct keeps the hot path allocation-free (no interface
boxing, no heap escapes from the payload) and makes exhaustive switches
statically checkable. The cost -- that adding a new kind touches the struct
definition -- is acceptable; `Event` kinds are few and stable.

---

## D16. Symbol normalisation

**Chosen:** Fetch the Binance `exchangeInfo` REST endpoint at startup to obtain
the canonical `baseAsset`/`quoteAsset` split for every symbol.

**Rejected:** A hardcoded ranked list of known quote assets (`USDT`, `BTC`,
`ETH`, ...) with longest-suffix matching -- brittle; any new quote asset breaks
it silently. Requiring the caller to supply pre-split `BASE-QUOTE` pairs in
config -- pushes exchange-specific knowledge to the operator.

**Why:** `exchangeInfo` is the authoritative source of instrument metadata and
is already required for the resync procedure. One HTTP request at startup
removes all ambiguity. Hardcoded suffix tables accumulate maintenance debt when
the exchange introduces new quote assets without notice.

---

## D17. M1 first subscriber

**Chosen:** A stdout structured-log subscriber wired through the publisher, so
the publisher to subscriber fan-out seam is exercised from the first working
binary.

**Rejected:** Having the transport stage write directly to the logger, bypassing
the publisher for M1 -- simpler for a single milestone but leaves the publisher
seam untested, and the pressure to keep the shortcut grows with every milestone
that ships without removing it.

**Why:** The publisher seam is what makes adding an API, storage sink, or web
UI additive rather than a rewrite. Discovering whether the design is sound
requires running it under real load. Building the seam after the surrounding
components stabilise is the same mistake as adding the recorder last (D5).

---

## D18. Shard count N

**Chosen:** A `-shards` flag with a default of `runtime.NumCPU()` evaluated at
startup.

**Rejected:** A hardcoded constant -- inflexible across machines and prevents
replay runs from fixing the shard count for reproducibility. Automatic scaling
-- adds complexity with no benefit at M1 symbol counts; the correct response to
a single hot symbol is backpressure, not more shards (D3).

**Why:** `runtime.NumCPU()` is a sensible default on any machine without
operator action. The flag allows benchmarks to sweep shard count without
recompiling and allows replay runs to fix the count so that two runs are
directly comparable.

---

## D19. `ingestd` configuration surface (answers Q1)

**Chosen:** Flags only for v0.1, using the Go standard library `flag` package.
No config file, no environment variable mapping, no external dependency.
Committed flags:

- `-symbols` -- repeatable; each value is a normalised `BASE-QUOTE` pair.
- `-endpoint` -- Binance websocket URL; a sensible default is provided.
- `-shards` -- see D18.
- `-metrics-addr` -- HTTP listen address for `/metrics`; a default is provided.

Flag names are a public commitment under D11 and will not be renamed without a
deprecation notice. A YAML layer may be added on top later without removing
the flags.

**Rejected:** A YAML or TOML config file from the start -- adds a parsing
dependency and a file-location convention before the flag surface is stable.
Environment variable mapping -- redundant with flags for an operator-run
process; can be added on top later. A config struct loaded from multiple
sources -- premature abstraction for a binary with four flags.

**Why:** Flags require no dependencies and survive being copied verbatim from
documentation into a shell. The asymmetry is the same as D11: adding a config
file layer later is painless; removing a flag that adopters have scripted is a
breaking change.

---

## D20. Publisher owns the subscriber goroutines; shutdown drains

**Chosen:** `Subscriber` is an interface with `Name() string` and
`Consume(ctx, model.Event)`. The `Publisher` owns one delivery goroutine per
subscriber, and that goroutine's sole exit condition is its channel being
closed by `Publisher.Close()`. The lifecycle is `Subscribe* -> Start ->
Publish* -> Close`, with `Subscribe` failing once started and `Publish`
required to come from a single goroutine.

**Rejected:** handing each subscriber a `<-chan Event` and letting it own its
goroutine. Goroutine ownership diffuses across packages, every subscriber
reimplements the same receive loop, and the drop accounting can no longer be
enforced centrally because the publisher stops being the only writer.

**Rejected:** exiting the delivery goroutines on `ctx.Done()`, via a `select`
over the channel and the context. A `select` with both cases ready picks at
random, so a cancelled context discards an arbitrary prefix of a non-empty
queue. Every shutdown would then lose the tail of the stream for no benefit:
the channel is bounded, so draining it is bounded work with a known cost.
`ctx` is still passed into `Consume` so that a subscriber doing real work can
abandon it.

**Why:** the single-owner property that D3 establishes for book shards is
worth having at the publisher too. One goroutine per subscriber means a
`Consume` implementation needs no internal locking and sees events in
publication order, which is the same guarantee the shard loops give. Fixing
the subscriber set at `Start` is what lets `Publish` read the slice on the hot
path without synchronisation.

The consequence to accept: `Close` waits for the in-flight `Consume` call, so
a subscriber that blocks forever hangs shutdown. That is stated in the
interface documentation rather than defended against, because the alternative
is a timeout whose correct value is unknowable from inside the publisher.

---

## D21. `log/slog` for the log subscriber, with a consumer-side instrument interface

**Chosen:** the standard library `log/slog` with a JSON handler. The
subscriber resolves decimal exponents through an `Instruments` interface
declared in `internal/pipeline`, which `*binance.InstrumentCache` satisfies
without knowing that package exists.

**Rejected:** `zerolog` or `zap`. The only argument for them is throughput on
the logging path, and that path is explicitly the lossy one: a slow subscriber
costs itself dropped events and nothing else. Paying a dependency for
performance the architecture already says is not needed is the wrong trade.

**Rejected:** passing `*binance.InstrumentCache` into the subscriber directly.
It compiles and it is what the wiring in `ingestd` has to hand, but it puts an
exchange-specific type in a signature outside `internal/exchange/`, which the
constraint in `CLAUDE.md` forbids. The point of that rule is that a second
exchange must not require edits elsewhere, and a concrete cache type in
`pipeline` would guarantee exactly that edit.

**Why:** D14 keeps the decimal exponent out of the `Price` value, so anything
that renders a price needs a lookup. Declaring the lookup as a one-method
interface at the point of use is the ordinary Go answer, and it makes the
architectural rule mechanical rather than a matter of discipline: `pipeline`
has no import of `internal/exchange` for a compiler to permit in the first
place.

---

## D22. `ErrTooManyDecimals` means "not representable", not "longer than expected"

**Chosen:** `ParseFixed` accepts fractional digits beyond the instrument
exponent when all of them are zeros, and truncates them. It returns
`ErrTooManyDecimals` only when a non-zero digit would be lost.

**Rejected:** rejecting any string with more fractional digits than the
exponent, which is what the M1.1 implementation did. Binance pads every
decimal to eight fractional digits regardless of the symbol's `tickSize`, so
`BTCUSDT`, whose `tickSize` is `0.01`, arrives as `"78737.26000000"`. Under
the strict rule the decoder rejected one hundred percent of live trades while
the unit tests passed, because the fixtures had been hand-written to the
expected shape rather than captured from the wire.

**Rejected:** giving every instrument eight decimals and ignoring `tickSize`.
It parses everything, but it discards the tick size information that the book
stage needs, and it inflates every stored magnitude by up to a million, which
costs headroom in the `Notional` product for no gain.

**Rejected:** trimming trailing zeros in the Binance decoder before calling
`ParseFixed`. It pushes a numeric-representation concern into an exchange
package, scans the string twice, and would have to be repeated in the next
exchange added.

**Why:** the exponent describes what the instrument can represent, not how the
exchange chose to format the value. `"78737.26000000"` and `"78737.26"` are
the same number, and a parser that accepts one and rejects the other is
enforcing a formatting convention while claiming to enforce a precision
constraint. The genuine constraint, that no significant digit may be silently
discarded, is preserved exactly.

**Consequence:** the fixtures in `internal/exchange/binance/testdata/` are now
required to match the wire format byte for byte. See the note in
`reflection.md` on why hand-written fixtures defeated the test.

---

## D23. Book side representation: sorted slice with binary search

**Chosen:** each side of a book is a `[]model.Level` kept sorted so that index
0 is the top of book. Bids descend by price, asks ascend. Lookup, insertion
point, and removal point all come from one binary search.

**Rejected:** `map[Price]Qty` plus a separately maintained sorted key slice.
O(1) point updates, but the two structures have to be kept consistent by hand,
and every read of the top of book still pays for the slice.

**Rejected:** a balanced tree or skip list. Asymptotically better for
insertion far from the top of book, and worse in practice at this size: a few
thousand levels of 16 bytes each, scanned linearly by hardware that likes
contiguous memory, against a pointer chase per comparison.

**Why:** the access pattern is not uniform. The hot read is the top of book
and the first N levels, which on a sorted slice is a prefix and needs no
search at all. The hot write clusters near the top of book, where the memmove
after an insertion is short. The levels that would make insertion expensive
are deep in the book, where updates are rare.

Both sides ordering top-first, rather than both ascending by price, means the
same index means the same thing on either side and every read path is written
once.

**Consequence:** a duplicate price on a side is unreachable through the binary
search, so it would never be updated or removed again. `Reset` rejects
duplicate prices in a snapshot rather than storing them, and the invariant is
asserted directly in the tests.

**Follow-up:** this is the structure M4 profiles. If insertion memmove shows
up, that is the profile that would motivate revisiting it, per the
no-optimisation-without-a-profile rule.

---

## D24. A crossed book is reported, not rejected

**Chosen:** `Book.Crossed()` reports whether the best bid is at or above the
best ask. The book applies the update that produced the cross and stores it.

**Rejected:** having `Apply` return an error when an update would cross the
book, and leaving the book unchanged. It looks safer and is worse: refusing
the delta means the book silently diverges from the exchange from that point
on, which is the exact failure mode this milestone exists to prevent. The
crossed state is evidence; discarding the evidence does not uncross anything.

**Rejected:** having the book mark itself stale and trigger its own resync. It
would put network I/O and sequencing policy inside a pure container, and the
book would then need to know about REST clients, contexts, and retry.

**Why:** a crossed book is never a state an exchange publishes, so it means a
delta was lost, applied out of sequence, or applied to the wrong side. The
response is to resync the symbol, which is a decision for the sequencing layer
(M2.3, M2.4) that has the update ids, the snapshot client, and the buffer. The
container's job is to make the condition detectable in O(1), which is what
separates "detectable" from "silently accepted" in the M2.1 done criterion.

**Consequence:** nothing calls `Crossed()` until M2.3. It is checked by tests
in the meantime, and the correctness harness described under M2 is where it
becomes an operational signal.

---

## D25. Depth snapshots are fetched at maximum depth, and truncation is carried explicitly

**Chosen:** `DepthClient` requests `/api/v3/depth` at `limit=5000`, the deepest
the endpoint serves, by default. The result carries `Snapshot.Truncated`, set
when either side came back filled exactly to the requested limit. The deepest
level present on each side is then the boundary of what the snapshot knows.

**Rejected:** a shallow limit such as 100 or 500. It weighs 5 or 25 against
the per-IP budget instead of 250, which matters when many symbols resync at
once, and it buys that with a book whose known region ends a few ticks from
the touch. Depth beyond the cut still receives deltas and still accumulates,
so a shallow snapshot does not make the book smaller, only less anchored.

**Rejected:** treating a snapshot as the whole book, so that any locally held
level the snapshot does not mention is deleted. It is the reading that makes
`Reset` simplest, and it is wrong: those levels are outside the window the
exchange was asked about, not gone. It would also make the M2.4 correctness
harness report divergence on every run.

**Rejected:** leaving truncation implicit, on the argument that a caller can
compare `len(Bids)` against the limit it asked for. That works only where the
limit travels alongside the snapshot, which is exactly what stops being true
once a snapshot is handed to a book shard or to the harness.

**Why:** the endpoint's contract is "the top N levels", not "the book", and
the difference is invisible in the payload: a truncated response and a
complete one are the same JSON. Absence below the cut means unknown, and the
only place with enough information to say so is the code that issued the
request. Recording it there costs one bool and removes a whole class of false
divergence downstream.

The complementary half is in the book itself (M2.1): removing a level it does
not hold is a no-op rather than an error, because a delta may legitimately
delete a price that was below the snapshot cut and was therefore never seen.

**Consequence:** a comparison between a locally maintained book and a fresh
snapshot must be restricted to the price range the snapshot covers whenever
`Truncated` is set. That constraint belongs to the harness described under M2,
and is why the flag exists before anything reads it.

**Weight, for the record:** Binance spot charges request weight by limit tier,
5 for up to 100 levels and 250 for 5000, against a per-IP budget of 6000 per
minute. Full-depth snapshots are therefore a bounded resource, and a resync
storm across many symbols is the case to watch. The `limit` parameter on
`NewDepthClient` exists so that this can be traded off without a code change.
