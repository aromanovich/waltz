---
paths:
  - "*.go"
  - "wal/**"
  - "mutation/**"
  - "fold/**"
  - "apply/**"
  - "cycle/**"
  - "wrapper/**"
  - "walmetrics/**"
  - "baserow/**"
  - "verify/**"
---

# This repo: what each package may import

These were a table in a guard test until the amendment to
[ADR 0009](../../docs/adr/0009-the-tree-separates-the-layer-from-what-judges-it.md), which deleted
it along with every other lint rule wearing a test's clothes
([no-lint-in-tests.md](no-lint-in-tests.md)). The rules themselves did not
change; they are read here now, when you open the package, instead of failing a
run after the import is written. Each one keeps a decision from being quietly
reversed, so the *why* is the load-bearing half — a rule nobody can read is a
rule someone deletes.

Two words, used exactly: **may not depend on** covers the transitive set, and
**may not import** is about this package's own files whatever its dependencies
pull in. The second is the rule for a package that legitimately sits above one
that does the forbidden thing.

**"A cold store" below means any persistence implementation** — a Temporal
persistence plugin, a database driver, a client of one. This library implements
none, and the whole of how it reaches one is `cycle.Applier`,
`cycle.Watermarker` and the base store the wrapper decorates, all three the
caller's. So the ban is not layering hygiene: an import of one anywhere here is
this library growing a store of its own, which is the one thing it says it does
not have.

## The tree rules (ADR 0009)

* **nothing outside `verify/` may import `verify/**`** — in non-test files.
  Nothing stops a layer package from reaching for a corpus generator or an
  in-memory cold store; both are in this module and both are useful, and the
  first such import puts test scaffolding into the binary an operator runs. Test
  files are exempt and must be: fold's and cycle's own tests legitimately fold a
  generated stream from `verify/mutgen`.
* **the root package is the front door and nothing else** — `Compose`, the `wal`
  section, the dynamic-config settings, `AbstractFactory`. It may import the
  whole layer and **nothing of the layer may import it**, which is what keeps
  the composition a leaf: a package reaching back for `waltz.Config` would make
  the configuration a dependency of the thing it configures.

## The layer

| package | may not | why |
|---|---|---|
| `wal` | the Temporal server, a cold store | the contract is backend-independent (ADR 0002): a backend author gets the log, not Temporal |
| `wal/memwal` | the Temporal server, a cold store | the shipped backend is a log in memory, and it knows Temporal exactly as little as one over a real cluster must |
| `wal/waltest` | the Temporal server, a cold store, **every backend** | the conformance suite tests the contract, so it may not know any backend |
| `baserow` | everything else of this layer, a cold store | the pre-window pair is named once for three packages that may not name each other's: what it may import is Temporal's persistence, and the moment it imports more, one of the three stops being able to reach it |
| `mutation` | a cold store | the record format mirrors Temporal's requests; what eventually writes them is the caller's |
| `fold` | a cold store, `apply` | fold folds what it is handed: no cold store, no log |
| `apply` | a cold store | it says what a drain's outcome *means* and writes nothing; the write path is behind `cycle.Applier` |
| `wrapper` | a cold store | wrap, don't fork: the decorator is defined over upstream's interface, and composing it with a store is the caller's job |
| `walmetrics` | `wal`, `fold`, `apply`, `cycle`, `wrapper`, `mutation` | the metric names are the layer's vocabulary: nothing that can be measured may be imported here |
| `cycle` | a cold store | the cycle drives the seam; it does not open one beside it |
| `cycle/tailstate` | a cold store, `fold` | the tail is arithmetic over what the loop acked: not the log those seqnos index, and not the window they outlive |
| `cycle/window` | a cold store, `fold`, `walmetrics` | the window is arithmetic over what the loop folded: it counts, it does not fold, and it publishes nothing |
| `cycle/trim` | a cold store, `fold`, `cycle/tailstate` | the trim is a cadence over a watermark it is handed: it holds the log, and may reach neither the thing that moves that watermark nor the thing that folds |

Where the one-line *why* is not the whole reason:

* **`waltest` names every backend, not just the one that ships.** A rule listing
  `memwal` is satisfied by a suite that imports it to special-case it. It is
  stated as "every backend" rather than as a list because the suite's whole
  audience is the author of a backend that is not in this repository.
* **`walmetrics` is named by both ends of the layer** — the wrapper counts what
  crosses it, the cycle counts what the accumulator did — so it has to be
  reachable from both, which is exactly why it may reach neither. The hazard is
  specific: a metric is easiest to add where the number already is, so an import
  of `fold` here would put "just read Stats" one line away, and the emitter would
  end up holding the component under test.
* **`wrapper`'s ban is transitive and therefore also a ban on `apply` and
  `cycle` reaching a store**: it is why the wrapper talks to the layer through
  `wrapper.ShardLayer`, and why translating a cycle's answer into the store's
  error types lives in `cycle/write.go` rather than here.
* **`cycle`'s is a direct ban and can only be one** — it defines the seam a cold
  store arrives at. The cycle holds the log, the accumulator and the write path
  at once, which is exactly why it must not hold a store as well: one such import
  and the shard has a second write path beside the one the drain takes, and the
  cycle is where a "just this once" write (a delete, a repair, a readback) is
  most tempting to add.
* **`tailstate` and `window` need entries of their own** — a rule stated over
  `cycle` covers neither. What they protect is not a store but the reach
  of two types meant to have none. `applied` is exactly the watermark a trim goes
  to, so `wal.Log` here would put "trim to the watermark" on the type where the
  watermark moves — which is where `publish` went, and which would take trim off
  the goroutine beside the loop that a stuck trim must not block a drain from.
  `fold` is the other half: the window's bytes and the tail's bytes are two
  numbers on purpose, and either type that could see the accumulator is one merge
  away from bounding the wrong one. **`cycle/window` is deliberately absent
  from `tailstate`'s list**: `Settle` takes a `window.Taken`, the typed edge
  between those two numbers and the reason a settle cannot state a byte count of
  its own. The rule is about reaching the thing that folds, not the token it
  hands over.
* **four packages may import `wal` and may not name `wal.Log` or
  `wal.Entry`** — `fold`, `apply`, `tailstate`, `window`. They fold, judge and
  count what they are handed; pacing and backpressure are the apply cycle's
  policy. **`cycle/trim` is the one sub-package that may name `wal.Log`**,
  and that is the same rule from the other side: the trim's whole job is the
  log, and giving it a package is what keeps `wal.Log` off the type where the
  watermark moves — while making the cycle's no-`go`-with-a-`*state` rule
  structural, since the goroutine now lives where no `*state` can reach. A
  package that can reach the log is a package where those land early, one
  convenience at a time. This one is a fact about identifiers rather than
  about the import graph, so it is stated on each of the four package doc
  comments as well, which is where it is read at the moment it could be broken.

## The judges

| package | may not | why |
|---|---|---|
| `verify/mutgen` | a cold store, `fold`, `apply`, `wal`, `verify/drive` | the corpus states what Temporal's write path produces: not what folds, not what one store accepts, and not what drives it |
| `verify/mutbuild` | a cold store, `fold`, `apply`, `cycle`, `wal` | `mutgen`'s rule at the granularity of one mutation: what a well-formed request is, is Temporal's answer and not the layer's |
| `verify/coldtest` | a cold store, `apply` | the cold store's double may not reach a cold store |
| `verify/basetest` | a cold store, everything of this layer but `baserow` | the pre-window rows' double stands at one seam and may know only it |
| `verify/checker` | a cold store, `apply`, `cycle`, `fold`, `wrapper`, the root package | the checker judges the layer from outside it: the log through the contract, everything else handed in |
| `verify/witness` | `verify/drive`, anything that runs a run | the module that catches a silent pass must be judgeable with no run at all |
| `verify/drive` | *(import)* `testing`, testify **in a non-test file** | the driving half is shared with callers that are not tests: it returns errors and knows nothing about testing |

Where the one-line *why* is not the whole reason:

* **`mutgen`**: a corpus that could see the accumulator would end up tuned to it.
  The generator's job is to state what Temporal's own requests are, and what
  folds is fold's answer to give. A store is out from the other side for the
  same reason — validity here is what Temporal's exported validators say, never
  an import of something that would accept the stream. `verify/drive` is the
  same rule from the other side: `drive.Stream` is the one verb that drives a
  generated stream, so the edge between the two points one way, and a generator
  that could reach it would be tuned to what drives it.
* **`mutbuild`** is that rule one mutation at a time, and `wal` is on its list
  for the reason `mutgen` has no shard type of its own either: it takes an
  `int32`, so the log's own types stay out of a package that states what
  Temporal's write path produces. It exists because `mutgen` cannot be asked for
  *one* create — every shape there is a method on the rand walk — so three test
  packages had built their own and disagreed on whether a create carries
  `ExecutionStateBlob`. `fold`'s fixtures are deliberately not moved to
  it: theirs are legible rather than valid, and would fail every validator
  `mutbuild` runs.
* **`checker`**: each name closes one road back in. A store would give it reads
  of its own and make every assertion a statement about one deployment rather
  than about the contract (ADR 0002); `apply` and `cycle` would give it the
  layer's own beliefs, which is the reason for refusing assertions compiled into
  the layer — they see what the layer *thinks* and die with it under `kill -9`.
  The watermark and the cold-store read arrive as functions the caller passes in,
  which is not indirection for its own sake: it is what makes them the *store's*
  reads rather than the layer's.
* **`coldtest`**: the double exists because the seam's only other adapter is a
  real store, so a package that could reach one here would be the thing it stands
  in for. `apply` is named for that reason and not for layering — this package
  answers what a drain did without driving one, and an import of the outcome
  vocabulary is how it would start interpreting.
* **`basetest`** is the same argument at the other cold-store seam, and it
  differs from `coldtest` in one way worth stating: it **does** name
  `baserow`, where `coldtest` satisfies `cycle.Applier` by shape and names
  nothing. That is the difference between the two seams rather than an
  inconsistency — the seam `coldtest` stands at is `cycle`'s, so naming it would
  give the double reach into the thing it is a double for, while `baserow` is a
  leaf whose whole purpose is to be reachable from the three packages that read
  through it. What the rule buys is the same in both: `cycle`, `apply`
  and the guards had a pair of maps and an absence rule apiece, and absence is
  the rule a delegated assertion is stated over — a double answering it its own
  way leaves the suite green and the rule unjudged.
* **`witness`**: everything in it is a pure function of values a table test can
  build, because the module whose job is to catch a silent pass has to be
  judgeable itself. One import of something that drives a run ends that in a way
  nothing else would notice: **the tests would still pass.** Whatever the layer
  it is a statement about reaches is the layer's business; what must not happen
  is this package naming it itself.
* **`drive`**: a `*testing.T` anywhere in it is a signature a non-test caller
  cannot call, and a caller that cannot call it keeps a second switch of its own
  — which drifts silently, since a kind missing from one only shows up in a
  stream that contains it. Direct-only, because
  `go.temporal.io/server/common/persistence` reaches `testing` and testify
  transitively. The ban is on the *importable* surface, so an external
  `drive_test` package is fine and is where `drive.Recorder` — the recorded call,
  two fsynced lines with a deadline between them — is judged: the order A9 rests
  on, the outcome line always written, the fenced note beside it, the deadline
  released per call. What the recorder does **not** own is the stop rule: a
  driver stops at its first non-acked call and a probe retries until a definite
  answer, and both are their callers'.
