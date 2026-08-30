# market-stream

Real-time crypto market data pipeline in Go. Ingests public websocket feeds,
reconstructs order books, and is built as a study in concurrency design and
profiling under sustained load.

The project targets a narrow, well-executed scope: correct book
reconstruction, predictable behaviour under load, and observable internals.
When a choice exists between broader functionality and a narrower component
that is correct and measurable, choose the latter. Feature breadth is a
liability in an ingestion component that other systems depend on.

## Scope boundary

Read-only. Public market data only. No API keys, no account access, no order
placement, no trading logic anywhere in this repository. "Trade" in this
codebase always means a public execution event received from an exchange,
never an action performed by this system.

## Non-negotiable constraints

These are settled decisions. Do not propose reversing them without being
asked. Rationale for each is in `docs/decisions.md`.

- Prices and quantities are fixed-point `int64` with a decimal exponent.
  Never `float64`, anywhere, including tests and examples.
- No unbounded channels. Every channel has an explicit capacity and an
  explicit policy for what happens when it is full.
- Order books are sharded by symbol, each shard owned exclusively by one
  goroutine. No mutex on book state. Reads happen by sending a request on the
  shard channel, never by locking.
- Exchange-specific types never leave `internal/exchange/`. Adding a new
  exchange must not require touching a file outside that package.
- All packages stay under `internal/`. The supported surface is the `cmd/`
  binaries and their configuration. Do not promote packages to importable
  paths without being asked.
- The recorder and replayer exist from day one. All profiling and benchmarking
  runs against recorded data, never against a live feed.
- No optimisation without a profile that motivated it and a `benchstat`
  comparison that measures it.

## Working conventions

- Go 1.22+, standard project layout with `cmd/` and `internal/`.
- Errors wrapped with context, never swallowed. `context.Context` threaded
  through every long-running goroutine for cancellation.
- Every goroutine has a documented owner and a documented exit condition.
- Tests run with `-race`. Goroutine leaks are checked with `goleak`.
- Never use em dashes in code comments, documentation, or commit messages.
  Use a simple hyphen or rephrase.
- When a design decision is made during a session, add it to
  `docs/decisions.md` with its rejected alternatives before moving on.

## Context

@docs/architecture.md
@docs/decisions.md
@docs/roadmap.md
