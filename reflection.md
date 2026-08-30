# Reflection

A running log of design analysis, observations, and open questions as the
project develops. Not a changelog -- `docs/decisions.md` records what was
decided and why. This file records what was noticed, what was hard, and what
to watch for.

---

## Session 1 -- bootstrapping (2026-08-30)

### Reading the architecture before touching a file

The first thing I did was read `CLAUDE.md`, `docs/architecture.md`,
`docs/decisions.md`, and `docs/roadmap.md` in full before forming any plan.
This turned out to be essential, not optional. Several constraints that look
like implementation details are actually architectural load-bearing elements:
the lossless/lossy split, the shard ownership model, the recorder-first
principle. Touching them casually would silently break properties that the
project exists to demonstrate.

### What was underspecified for M1

Before writing a single file I identified seven things the architecture and
roadmap did not settle:

1. **Module path.** Genuinely blocks `go.mod`. Nothing else could start.

2. **`Price` type shape.** Three credible options with different performance
   and ergonomics trade-offs. The architecture stated the constraint (int64,
   decimal exponent) but not the Go type.

3. **`Event` polymorphism.** Interface, tagged struct, or separate channels.
   Each propagates into the `Exchange.Subscribe` signature and into how shard
   loops switch on message kind.

4. **Symbol normalisation algorithm.** The architecture says `BTCUSDT` becomes
   `BTC-USDT` but does not say how the split is found. For most symbols the
   answer is obvious; for `BNBETH`, `ETHBTC`, and anything with a non-obvious
   quote asset it is not.

5. **M1 output.** The roadmap says "output" without specifying what. The
   publisher exists in the architecture but the metrics endpoint is M6. The
   question was whether the M1 binary bypasses the publisher or exercises it.

6. **Shard count N.** Named and described in the architecture, value
   unspecified.

7. **Configuration surface.** Identified as an open question (Q1) in
   `decisions.md` itself. The right answer has consequences for adopters that
   are disproportionate to the apparent simplicity of the choice.

The lesson: architecture documents describe invariants and structure, not
concrete type signatures and configuration interfaces. Both are necessary and
the gap between them is always larger than it looks from the architecture side.

### On the `Price` decision (D14)

The rejected option -- `type Price struct { Value int64; Exp int8 }` -- looks
more correct at first glance because it is self-describing. But it is subtly
wrong for this use case: prices are only meaningful within the context of a
symbol, and the symbol always carries the exponent. Embedding the exponent in
every `Price` value means copying it millions of times redundantly, widening
a hot-path value from 8 to at least 12 bytes (padded to 16), and adding
indirection to every arithmetic operation.

The chosen form -- plain `int64` with exponent in instrument config -- looks
like it loses information, but it doesn't: the information was never in the
price value to begin with, it was always in the instrument. The type reflects
that reality.

The thing to watch in M1: wherever a `Price` appears in a function signature,
make sure the exponent is always reachable without an extra lookup. If it
isn't, the abstraction boundary is drawn in the wrong place.

### On `Event` polymorphism (D15)

The tagged struct is the right choice for this project, but it carries a
cost that must be understood consciously: it is a closed type. Adding a new
event kind requires editing the struct definition, which is a change to a
shared foundation type. The interface alternative would have been open --
any package could add a new kind by implementing the interface.

The reason the closed form is correct here is that `Event` kinds are
determined by the exchange protocol, not by application logic. Binance sends
trades, book deltas, and snapshots. That set does not change based on what
downstream consumers want. The openness of an interface would be false
flexibility: in practice it would just mean type-asserting on a fixed set of
known types everywhere, which is worse than a switch on a discriminator.

Watch for: if a future exchange introduces an event kind that does not map
cleanly to `Trade`, `BookDelta`, or `Snapshot`, that is the signal to revisit.
Until then the tagged struct is correct and cheaper.

### On symbol normalisation (D16)

The hardcoded-suffix-list option was tempting because it avoids a network call
at startup. But it is the kind of simplification that fails exactly when you
are not watching: Binance has introduced new quote assets (FDUSD, TUSD) without
much notice, and any hardcoded list will silently mismatch them.

Using `exchangeInfo` is already necessary for the resync procedure. Reusing it
for symbol normalisation adds no new dependency and eliminates an entire class
of latent bugs. The right question is not "can we avoid the network call" but
"are we already making the network call for another reason" -- and we are.

One thing to verify in M1: the `exchangeInfo` response is large (hundreds of
symbols). Make sure it is fetched once, stored, and not re-fetched per symbol
or per reconnect.

### On the M1 subscriber question (D17)

The temptation to bypass the publisher for M1 and write directly to the logger
is the exact kind of shortcut that accumulates into rewrites. The publisher
seam is specifically designed to prevent a later HTTP API from requiring
structural changes. If the seam is not exercised from the first binary, there
is no feedback loop to reveal whether its design is sound -- until the API
milestone, when changing it is expensive.

This is the same principle as D5 (recorder before optimisation): build the
measurement and isolation infrastructure before you need it under pressure.
The subscriber seam is the same pattern applied to output.

The practical consequence: `ingestd` in M1 is slightly more complex than it
would be with a direct `log.Printf`. That complexity is the correct
representation of the architecture, not an excess.

### On flag names as a public commitment (D19)

This was the most conceptually interesting question of the session. Flag names
look like an implementation detail -- they are, after all, just strings in a
`flag.String()` call. But for a tool that other people script against, flag
names are as stable as exported function signatures. Renaming `-symbols` to
`-symbol` after a user has wrapped the binary in a cron job is a breaking
change in exactly the same way as renaming an exported function.

D11 says everything stays under `internal/` because the signatures are still
moving. D19 is the complement: once a flag is named, it is committed. The
flag set therefore deserves the same care as an API design, not the same care
as a variable name.

The four committed flags (`-symbols`, `-endpoint`, `-shards`, `-metrics-addr`)
were chosen to be minimal and orthogonal. Each controls one independent axis.
There is no interaction between them. This is deliberate -- interaction
creates combinatorial complexity in documentation and in user mental models.

### On the skeleton itself

The `doc.go` approach forces a one-paragraph answer to "what is this package
for" before any implementation exists. This is harder than it sounds: it is
easy to write a package whose purpose can only be explained by listing its
contents. If the doc.go is vague or circular, that is a signal the package
boundary is wrong, not that the doc is hard to write.

A few observations from writing the seven doc.go files:

- `internal/model` was easy to describe: it holds the canonical shared types.
  The boundary is clear.
- `internal/pipeline` was harder: it contains both the shard router and the
  publisher, which are distinct responsibilities. They are together because
  they share the backpressure policy. If they become hard to describe jointly
  in one paragraph, split them.
- `internal/record` describes the `Source` interface, not just the recording
  mechanism. The interface is the important thing -- it is what makes live and
  replay code paths identical.
- The deferred packages (`aggregate`, `sink`, `loadgen`) were easy to describe
  because their responsibility is narrow and unambiguous. Deferred does not
  mean vague.

### What to verify at the end of M1

The done-when for M1 is: "trades stream continuously and the process recovers
from a forcibly closed connection without operator action."

Beyond the literal criterion, these are the properties worth verifying
explicitly:

1. The publisher → subscriber path is exercised by the log subscriber, not
   bypassed. A grep for `log.Printf` outside the subscriber implementation
   would be a red flag.

2. `exchangeInfo` is fetched exactly once at startup and the result is used
   for both symbol normalisation and subsequent REST calls.

3. The reconnection loop is tested by a forced close, not just by network
   conditions. There should be a test or a documented manual procedure for
   this.

4. No `float64` anywhere in the trade path. A single `grep -r 'float64'
internal/` should return empty after M1.

5. Every goroutine started in `cmd/ingestd` has a documented owner and a
   documented exit condition (stated in CLAUDE.md as non-negotiable).

---

## Session 1 continued -- M1 readiness check (2026-08-30)

### Is M1 the right next step?

Yes, and the answer was easy: the skeleton covers the "module and package
layout" component of the M1 spec, so the remaining work is well-scoped. The
useful part of the check was identifying what is still unresolved before
starting, rather than discovering it mid-implementation.

### Two decisions surfaced before M1 implementation begins

**1. Where does the `Exchange` interface live?**

The architecture shows the interface but names no package for it. The two
credible options are a `internal/exchange` parent package (making `binance` a
sub-package that implements it) or `internal/pipeline` (the package that
consumes it). A third option -- `internal/model` -- is wrong: model holds data
types, not behavioural interfaces.

The parent-package option feels natural because it co-locates the interface
with its implementations and makes the "adding a second exchange must not touch
files outside `internal/exchange/`" rule (D11) mechanically enforceable. If
the interface lives in `internal/pipeline`, a new exchange implementation
would import `internal/pipeline` to satisfy the interface, which is an
inverted dependency.

Pending decision from the project owner.

**2. Logging library for the M1 structured-log subscriber.**

`log/slog` (Go standard library since 1.21) is the obvious candidate: no
external dependency, structured key-value output, level filtering, JSON handler
available for log aggregation pipelines. The only reason to reach for an
external library would be performance on the subscriber hot path -- but the
log subscriber is on the lossy path and is explicitly allowed to be slow
relative to the book stage. `log/slog` is sufficient.

Pending confirmation from the project owner.

### Convention established: update this file after every important prompt

From this point, a reflection entry is added after any prompt that involves
a design analysis, a decision, a blocked question, or a notable observation
about the implementation. Prompts that are purely mechanical (commit messages,
formatting, file moves) do not require an entry.

---

## Session 2 -- M1.6 publisher and first subscriber (2026-08-30)

### The fixtures were fiction, and the tests were happy about it

The most important thing this session produced was not the publisher. It was
the discovery, in the first twenty seconds of running the binary against the
live feed, that the decoder had been rejecting every single trade since M1.4.

`sizeDecimals` derives the exponent from `tickSize`, so `BTCUSDT` gets two
decimals. Binance pads every decimal string to eight fractional digits
regardless of tick size, so the wire carries `"78737.26000000"`. `ParseFixed`
compared lengths and returned `ErrTooManyDecimals`. Decode error rate: one
hundred percent.

The unit tests passed throughout. The reason is the part worth keeping: the
fixtures in `testdata/` were hand-written to `"p": "50000.01"`, the shape the
implementer expected, rather than captured from the wire. A fixture written
from an assumption tests that the code matches the assumption. It cannot
detect that the assumption is wrong, and it produces a green suite while doing
it, which is worse than no test because it actively creates confidence.

The roadmap already said the right thing. M1.4 reads "table driven tests over
real payloads **captured during M1.3** and saved as fixtures". M1.3 built a
frame counter that printed totals and threw the frames away, so there was
nothing to capture from, and the fixtures got invented instead. The
instruction was followed in form and inverted in substance.

This is D7 ("real captured data, not a synthetic generator") applying one
level lower than where it was written. D7 argues about the profiling corpus.
The same argument holds for a four-line JSON fixture, and for exactly the same
reason: a generator, or a developer, only produces what was modelled. The
eight-digit padding is precisely the kind of detail nobody models.

Two consequences worth carrying forward:

1. M3 should backfill the fixtures from real recorded frames, since by then
   the recorder exists and there is no excuse. Until then, any new fixture
   gets pasted from an actual captured frame, not typed.

2. A milestone whose done-criterion is observational ("verifiable by eye
   against the exchange's own display") is not met by a passing test suite.
   M1.4 was marked done without that observation being made. The criterion was
   right; it just was not executed.

### The bug was found by running the thing, in twelve seconds

Worth stating plainly because it is easy to forget under a green suite: the
entire cost of finding a one-hundred-percent failure was building the binary
and letting it print for twelve seconds. No test that existed would ever have
found it, and no amount of additional unit testing against the same fixtures
would have either.

### On the publisher shutdown semantics (D20)

The interesting decision was the delivery goroutine's exit condition, and the
appealing answer was wrong. The reflex is `select` over the event channel and
`ctx.Done()`, because `CLAUDE.md` says every long-running goroutine takes a
context. But `select` with both cases ready chooses at random, so cancelling
the context discards an arbitrary prefix of whatever is still queued.

The channel is bounded. That is the whole point of the backpressure policy,
and it means draining on shutdown is bounded work with a known cost. So the
exit condition is channel close, and `ctx` goes to `Consume` for the
subscriber's own use.

The general shape: "thread a context through every goroutine" is about
cancellation reaching the goroutine, not about the context being the exit
condition. Those are different claims and only the first is in `CLAUDE.md`.

### Refining the M1 verification checklist from Session 1

Item 1 said a `grep` for `log.Printf` outside the subscriber would be a red
flag. That needs a distinction it did not make. `ingestd` legitimately logs
operator diagnostics directly: decode failures and the periodic summary. What
must not bypass the publisher is normalised **events**. The check is not
"does the binary log" but "does any `model.Event` reach an output without
passing through `Publish`". Currently none does.

Item 4 said `grep -r float64 internal/` should return empty. It earned its
keep immediately, on the test code rather than the production code: unmarshal
a JSON log record into `map[string]any` and every number becomes a `float64`.
Fixed with `Decoder.UseNumber`. Small, but it is the constraint working as
intended, and the test is more precise for it.

### Still open after this session

- The `ingestd` flag surface still does not match D19. `-stream` also takes a
  raw Binance stream name, which leaks an exchange-specific format into
  `cmd/`. Reconciling it is its own change because D19 makes flag names a
  commitment.
- `internal/exchange/binance/conn.go` is dead. `Transport` replaced it in
  M1.5 and nothing references `Conn`.
- The reconnect backoff never resets after a connection that succeeded. A
  socket that lives six hours and then drops resumes at the last backoff
  value rather than at 500ms.
- Where the `Exchange` interface lives is still undecided. M1.6 did not need
  it. M2 or M3 will.
- `ParseFixed` allocates twice per call, in `strings.Repeat` and in the
  `intPart+fracPart` concatenation. Noted, not touched: D8 and the "no
  optimisation without a profile" rule both say the profile comes first, and
  this is exactly the kind of thing M4 exists to either confirm or dismiss.

---

_This file is updated as the project develops. Entries are appended, not
edited. If an observation here turns out to be wrong, note the correction in
a new entry rather than revising the original._
