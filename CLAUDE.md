# Working notes for waltz

## The first rule

**Data once acknowledged is never lost.** Refusing a write is acceptable. Taking
the process down is acceptable. Losing a write that was acked is not, and it is
not made acceptable by the log or the cold store having been configured badly.
A warning followed by a successful write is a lie the caller acts on.

This outranks throughput, availability and every measured number here. The whole
of what this library does — acknowledge into a log, fold many mutations into one
cold-store transaction — is an optimisation bought against that rule, so any
change that trades it away has misunderstood what is being optimised.

[`DURABILITY.md`](DURABILITY.md) is the standing list of every known way the rule
can break, each marked closed, open, accepted or **unknown** — and unknown means
nobody established it, which is to be treated as open. A closed entry names the
mechanism and a test that fails without it; an accepted one names what is being
accepted and what a deployment owes in its place, and the three of those are the
page's floor. A new way found belongs there whether or not it is closed the same
day.

**It is the queue, not the report**, and its last section says how it is worked:
a session takes named entries and ends each one closed, refuted or accepted, and
changes nothing else. Read that section before starting a hardening pass — going
looking for something to fix instead is what keeps the list from converging,
since every edit is new surface. What has already been established as impossible
is recorded there too, so that a later pass does not derive it again.

## What this is

waltz puts a write-ahead log in front of a Temporal history shard's cold store.
Both seams are the caller's in production, and each has **one** implementation
here, running in this process:

* the log is a `wal.Log`; `wal/memwal` is the contract in memory, and
  `wal/waltest` is the conformance suite an author of a real one runs.
* the cold store is a `cold.Store`, an applier plus a watermarker; `cold/memcold`
  is Temporal's own SQL persistence over an in-process SQLite database, embedded
  rather than written, with the folded window's transaction added beside the 28
  inherited methods. Temporal's own four persistence suites judge it.
  `internal/verify/coldtest` is still the double, for suites that need to vary
  a drain's outcome.

That pair is why a Temporal server boots over waltz in `go test` with nothing
installed (`internal/verify/e2e`), and it is not a durability claim: both die
with the process.

It is consumed the way Temporal's own custom-persistence option is:

```go
temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base))
```

where `base` is the plugin whose stores hold the cold data. That method is the
door, and `Layer.Options()` with `wrapper.NewAbstractDataStoreFactory` is the
same composition one level down, for a caller who is already building the
wrapper's stores itself: a layer composed and never handed to a factory is a
node running passthrough under a configuration that says otherwise, and nothing
reports it.

## Layout

The layer packages sit at the **module root** and `internal/verify/` holds what
judges them
([ADR 0009](docs/adr/0009-the-tree-separates-the-layer-from-what-judges-it.md)).
`cold/memcold` is the one thing at the root that is neither: it is a *store*,
sitting under the cold seam, so nothing of the layer may import it and what it
may import is the vocabulary the seam is stated in and nothing above it
([`.claude/rules/cold.md`](.claude/rules/cold.md)).
The root package `waltz` is the front door — `Compose`, the `wal` configuration
section, the dynamic-config settings, `AbstractFactory` — and nothing of the
layer may import it. The direction the layer reads in is
`fold → cycle → apply → wrapper → waltz`, which nesting cannot express and the
handbook's [03-components.md](docs/handbook/03-components.md) carries.

## Running it

Everything runs in process, with nothing installed — no cluster, no container,
no build tag:

```sh
make test            # go test ./... -count=1; the default target
make lint            # golangci-lint plus gopls's modernize, both pinned there
make check           # both
go test ./wal/...    # the contract and its conformance suite; milliseconds
```

There is no `-p 1` and its absence is deliberate: every backend is in this
process and every database is keyed by a name minted per store, so the packages
share nothing. `WAL_ACCEPTANCE_MUTATIONS` shortens or lengthens
`internal/verify/acceptance`'s stream. `internal/verify/e2e` is the longest
thing in the run — it starts four Temporal services twice — and
`go test ./internal/verify/e2e/` is how to run it alone.

Three things a green run does **not** say, and they are worth having before you
quote one:

* **it is not "the server is production-ready".** A server *does* boot here and
  complete a workflow over the layer, which is more than the tree could say
  before — but over a database that dies with the process, with nothing killed
  and one workflow of load. The wider claim is upstream's own functional suites
  over a composition, which `patches/README.md` makes reachable and a deployment
  runs.
* **it says nothing about cost.** Whether a log answers sooner than the cold
  store would is a property of that log, measured on the cluster it runs on. A
  number produced here would describe a laptop.
* **it says nothing about a fold against a store that was not folded for.**
  `cold/memcold` is a real store and the acceptance lands real batches in it, so
  a batch that contradicts the schema is caught, and the differential oracle —
  one stream applied twice, sequentially and folded, the two required to end
  identical — runs here too. Both its arms are `cold/memcold`, so a defect the
  two share cancels, and nothing in it speaks for the schema, the row layouts or
  the condition failures of a store a deployment would run.

The handbook's
[15-the-limits-of-the-evidence.md](docs/handbook/15-the-limits-of-the-evidence.md)
is the long form of all three.

## Two rules that are not about any one package

Both load from `.claude/rules/` when you touch code, and both are easily walked
past because nothing fails when you break them.

**Comments say what only the code cannot.** No provenance, no ticket numbers as
citation, no re-tellings of the ADRs or the handbook, no sentences restating the
line below. The tree sits at ~23% of non-blank lines and a new file well over
that is the signal to re-read
[`.claude/rules/comments.md`](.claude/rules/comments.md), which also has the
token-stream check that proves a compression changed nothing else.

**A test asserts behaviour, never shape.** No parsing Go source, no assertions
over the import graph, no parsing the handbook or a build file. Seven `_test.go`
files did those and all seven are gone — they are lint rules in a test's
clothing, and one is on record having been green while the invariant it existed
for was violable. [`.claude/rules/no-lint-in-tests.md`](.claude/rules/no-lint-in-tests.md)
has the three places such a rule may live instead, in the order to try them.

## Where the rest of it is

* [`README.md`](README.md) — the shape of the thing for somebody arriving: the
  composition, the two seams and what an implementer of each owes.
* [`CONTEXT.md`](CONTEXT.md) — the glossary. One name, one thing; the second
  half of it is the list of names that turned out to have been two.
* [`.claude/rules/`](.claude/rules/) — one file per subsystem, each scoped by a
  `paths:` header so it loads when you touch that directory and costs nothing
  otherwise. This is where "what to know before changing this" lives, beside the
  code it is about rather than here.
* [`docs/adr/`](docs/adr/) — the eight decisions somebody will otherwise try to
  reverse: the log contract, in-process, the configuration's home, the log's
  boundary, the tree, one entry per append, one shipped implementation at each
  seam, and the cold store embedding Temporal's own persistence;
* [`docs/handbook/`](docs/handbook/README.md) — the book. 01–11 are the
  reference (components, contracts, the paths, the keys, the series, the
  suites); 12–15 are the deep dives (what a write cost before the layer, the
  designs that were refused, where each default came from, what the green suite
  does not claim). The rule for adding to it: **the reference says what is true,
  the deep dives say why it was made that way and what it is not.**

The handbook names identifiers, defaults and tag values, so a rename lands
there. A cheap sweep is to extract every `package.Symbol` from the chapters and
grep the tree for its declaration — that is how a typo in a `dynamicconfig` key
was found, which is a `WARN unregistered key` and the default standing silently.
