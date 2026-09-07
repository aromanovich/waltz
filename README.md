# waltz

waltz puts a write-ahead log in front of a Temporal history shard's cold store, so that many
mutable-state writes are acknowledged into the log and folded into one cold-store transaction. It
ships as a persistence decorator: you compose it over the plugin that owns your cold data and hand
the result to `temporal.WithCustomDataStoreFactory`, the same door a custom persistence backend
already goes through.

It has two seams — the log and the cold store — and each ships one implementation that runs in this
process: `wal/memwal` and `cold/memcold`. A deployment replaces both. What the two shipped ones buy
is that a Temporal server composed over waltz boots, serves and runs a workflow with nothing
installed: no cluster, no container, no port, no cgo.

The book is [`docs/handbook`](docs/handbook). This page is the shape of the thing; the handbook is
what it promises and why.

## The first rule

**Data once acknowledged is never lost.** Refusing a write is acceptable. Taking the server down is
acceptable. Losing a write that was acked is not — and it is not made acceptable by the storage
underneath having been configured badly: an engine that cannot keep the promise under the settings
it finds must refuse to take the data rather than warn and take it anyway. A warning followed by a
successful write is a lie the caller acts on.

This outranks throughput and availability, and it is what the two seams below are shaped by. An
early acknowledgement is easy; keeping it honest across a crash is the whole design. That is why
the log's guarantees are stated as five and not as "it stores things", why one drain is one
transaction with the watermark inside it, and why an ambiguous drain is resolved by reading the
watermark back rather than by re-folding.

## The composition

```go
package main

import (
	"context"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/temporal"

	"github.com/aromanovich/waltz"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wal/memwal"
)

func main() {
	// The cold store. memcold is the one shipped here — Temporal's own SQL
	// persistence over a database in this process — and a deployment puts its
	// own cold.Applier and cold.Watermarker here instead.
	store, release, err := memcold.New("active")
	if err != nil {
		panic(err)
	}
	defer release()

	layer, err := waltz.Compose(
		waltz.Backends{
			Log:       memwal.New(), // your wal.Log; memwal is the one shipped here
			Writer:    store,        // cold.Applier
			Recoverer: store,        // cold.Watermarker
		},
		cycle.Fixed(cycle.Defaults()),
		waltz.DefaultTaskCategories(),
		log.NewCLILogger(),
		nil, // the server's metrics handler arrives later, through the factory
	)
	if err != nil {
		panic(err)
	}

	// base is the persistence factory that owns the cold data — memcold's here,
	// your plugin's otherwise.
	var base client.AbstractDataStoreFactory = memcold.NewAbstractDataStoreFactory(store)

	s, err := temporal.NewServer(
		// The server's own config must name a custom datastore in
		// Persistence.DataStores; that naming is the whole of how the factory
		// below enters its persistence graph.
		temporal.WithConfig(cfg),
		temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base)),
	)
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	defer func() {
		_ = s.Stop()
		// After the server has stopped: the shutdown drain still needs a store
		// to write to. `defer release()` above runs after this one, which is the
		// order that leaves the drain a database.
		layer.Shutdown(context.Background(), 30*time.Second)
	}()
}
```

`Compose` opens nothing, reaches nothing and takes no context — everything that talks to storage
happens while the backends are built, and whatever they hold stays yours and must outlive the
layer. `Backends` is a parameter and not something `Compose` builds, and that stays true now that
two of the three have a shipped implementation: `memcold.New` is called by the caller, above, and a
composition that reached for it itself would be a second configuration of the store with nothing to
reconcile it against the one the server was given.

The lifecycle brackets the server's. Composing first means a policy whose tail budget does not add
up stops the binary rather than a node. Shutting down last means the drain of every window still
open has somewhere to land; `Layer.Shutdown` puts its budget on a context detached from the
caller's cancellation, because a shutdown drain runs exactly where a context has just been
cancelled, and one that inherited it would return at once and leave a tail behind that nothing
reports.

A layer composed but never handed to a factory is a node running passthrough under a configuration
that says otherwise, which is why `Layer.AbstractFactory` exists as a method: it pairs the
composition with the factory that carries it. `Layer.Options()` is the same thing one level down,
for a caller building `wrapper.NewExecutionStore` or `wrapper.NewShardStore` directly.

## The two seams

waltz sits between two things it does not own: the log an acknowledgement lands in, and the cold
store a drain lands on. Both are interfaces, both have exactly one implementation in this tree, and
both are meant to be replaced.

| | the contract | shipped here | what judges an implementation of it |
|---|---|---|---|
| the log | `wal.Log` | `wal/memwal` — the contract in process memory | `wal/waltest`, this repository's own conformance suite: eighteen cases you run against your backend |
| the cold store | `cold.Applier`, `cold.Watermarker` | `cold/memcold` — Temporal's own SQL persistence over a database in this process | Temporal's four exported persistence suites, which `memcold` runs unmodified |

The rows are not quite mirror images, and the difference is worth having before reading either. The
log's contract is waltz's own invention, so waltz owes it a suite and ships one. The cold store's is
Temporal's `ExecutionStore` plus one method waltz invented, so what a cold store owes is mostly
Temporal's to state — and Temporal states it, as four suites it exports. waltz therefore ships no
conformance suite at this seam, and that is a real gap rather than a symmetry: **nothing exported
from here judges somebody else's `cold.Applier`.** What is written down instead is the four
obligations below, and the worked example of all four is `cold/memcold`.

### The WAL backend: `wal.Log`

`wal.Log` is one append-only, fenced, gap-free sequence of entries per shard: `Fence`, `Append`,
`ReadFrom`, `Trim`, `Close`. A payload is opaque bytes — no Temporal types reach a backend.
Everything above it depends on five guarantees and on nothing else, which is what makes the log
replaceable:

1. **Total order per shard.** The writer, single by virtue of epoch fencing, assigns seqnos itself.
2. **`Fence` atomically cuts off appends of all lower epochs**, so a zombie ex-owner cannot slip an
   append past a completed fence.
3. **Cumulative ack.** A successful `Append` up to seqno *n* means every entry ≤ *n* is durable.
4. **Gap-freedom.** An append never skips a seqno, so a shard's log is one unbroken run and replay
   needs no hole tracking.
5. **Readback.** `ReadFrom` returns every entry a completed append acked and no trim has removed,
   in seqno order.

Three errors an append refuses with are matched by `errors.Is` and are part of the contract:
`wal.ErrFenced`, `wal.ErrAlreadyWritten`, `wal.ErrGap`. None of them writes anything, and where
more than one applies `ErrFenced` wins — `ErrAlreadyWritten` is an ack, and a fenced-out writer
must not take it as one.

**The only implementation shipped here is `wal/memwal`, in memory.** It is a backend and not a test
double, and what says so is that it passes the conformance suite.

**That suite is `wal/waltest`, and it is what you run your backend against.** One call:

```go
func TestMyBackendKeepsTheContract(t *testing.T) {
	waltest.RunContractSuite(t, myBackend())
}
```

Eighteen cases over the five guarantees, the three errors, shard independence, epoch renewal and
two writers contending for one shard. It asserts external behaviour of `wal.Log` only and imports
no backend. Beside it is `waltest.NewFaulty`, which wraps a log so that a chosen call fails or
blocks instead of reaching it — for driving what the layer above does when a log misbehaves.

### The cold store: `cold.Applier` and `cold.Watermarker`

```go
type Applier interface {
	Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error
}

type Watermarker interface {
	Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error)
}
```

**The implementation shipped here is `cold/memcold`, and it is Temporal's own store.** It runs
Temporal's SQL persistence over a SQLite database that lives in this process and dies with it —
`modernc.org/sqlite`, pure Go, so no cgo, no container, no port and no file. `memcold.Store` embeds
the `persistence.ExecutionStore` that `sql.NewFactory` vends, so the 28 methods, the schema they
were written against, the row layouts and the error classes are upstream's, unmodified. It shadows
none of them. Beside them it adds exactly what waltz needs and Temporal has no method for:

* `Apply` — the folded window's single transaction. `persistence.ExecutionStore` has nowhere to
  declare a transaction spanning many workflows, so it is opened on the `sqlplugin.DB` handle the
  store keeps beside the embedded interface. That handle is the fact the whole design rests on.
* `Watermark` and `SetWatermark` — over one table of waltz's own, `waltz_watermarks`, created
  beside the plugin's schema rather than borrowed from a column the server also writes.
* `GetCurrentExecutionWithLastWriteVersion` — the versioned current-row read described under "One
  thing the base store owes" below, which upstream's `GetCurrentExecution` computes and then
  discards for want of a field to hold it in.

**The decision behind that is embedding rather than reimplementation, and it is the one to
understand before proposing anything else here.** A history shard's store is the hardest thing in
this tree to get right and the easiest to get plausibly wrong. Writing 28 correct methods in order
to obtain one new one is a cost with no payer: the new method is the only part waltz has an opinion
about, and the other 28 would be a second, worse copy of code that already exists, needing its own
schema, its own suites and its own version bumps. Embedding buys them for free and keeps them
upstream's across a Temporal bump.

**What judges it is Temporal's own suites, and no suite of ours.** `cold/memcold/conformance_test.go`
runs `tests.NewShardSuite`, `tests.NewExecutionMutableStateSuite`,
`tests.NewExecutionMutableStateTaskSuite` and `tests.NewHistoryEventsSuite` from
`go.temporal.io/server/common/persistence/tests` — 75 subtests — exactly as they judge a plugin. A
suite written here would be this repository's opinion of what a store owes; those are the server's.

They judge the inherited surface and not `Apply`, which is not a method they know about. What judges
`Apply` is `cold/memcold/apply_test.go` — seven cases over the transaction's ordering, its refusals,
its attribution and its rollback — and `verify/acceptance`'s both-seams-real run, which drives a
generated stream through `memwal` and `memcold` at the shipped window and then asks the database
what it holds.

`verify/coldtest` is still here and is still a double: one value satisfying both interfaces, which
records what a drain carried and interprets nothing. It is what a suite uses when it needs to *vary*
a drain's outcome: `coldtest.Refusing(err)` fails every drain with the error a test chose, which
is how a suite reaches the refused, the shard-lost and the ambiguous classes without a store that
can be asked to misbehave.

What an implementer owes the contract:

* **One drain is one transaction.** Everything `fold.Batch` carries — the merged request per dirty
  workflow, the history-task work, the range completions — commits together or not at all. There is
  no partial drain.
* **The watermark moves inside it.** `batch.Watermark()` is the seqno that transaction acks, and it
  is written by the same transaction behind the same gate as everything else. That is what makes
  "did this drain commit?" a question the `Watermarker` can answer after a crash, and what stops a
  replay from re-applying rows that are already there. `Watermark` returns the seqno, whether the
  shard has one at all, and an error.
* **The epoch is asserted, and asserted first.** `Apply` is handed the epoch; the transaction
  compare-and-sets the shard's ownership on it before any other assertion, so ownership loss
  shadows a version failure rather than being reported as one. This is the cold-store half of
  end-to-end fencing — the log half is `Log.Fence`, and nothing in this repository judges the half
  that is yours.
* **It answers in `apply`'s vocabulary.** The cycle branches on `apply.Classify(err)`, which sorts
  an `Apply` error into five classes, and an implementation speaks back through it:

  | Class | What the cycle does |
  |---|---|
  | `ClassCommitted` (a nil error) | advances the watermark, empties the window, trims on cadence |
  | `ClassRefused` | nothing reached the cold store; there is an input to fix and no outcome to recover. Raise it with `apply.Refuse` |
  | `ClassShardLost` (a `*persistence.ShardOwnershipLostError`) | the epoch CAS failed. Stop writing under this epoch; do not retry |
  | `ClassInvariantViolated` (a condition-failure error) | under fencing this layer is the shard's only writer, so it is a broken invariant and not contention. Halt the shard. `apply.Attribute` turns a bare condition failure into the rows it was about |
  | `ClassUnknownOutcome` (anything else) | the transaction may or may not have committed. Read the watermark before anything else — re-folding by version instead corrupts |

  The default matters more than the four named cases: anything `Classify` cannot *prove* refused,
  fenced or condition-failed is an unknown outcome, because calling a commit a failure is how a
  batch gets applied twice.

### One thing the base store owes

Intercept mode stands delegated assertions on the pre-window rows, and one of them needs a current
row's `last_write_version` — which `persistence.InternalGetCurrentExecutionResponse` has nowhere to
hold. So the `ExecutionStore` waltz decorates must also answer
`GetCurrentExecutionWithLastWriteVersion` (`baserow.Store`). A store that cannot is refused at
construction with `baserow.ErrNoVersionedRead`, where the server is still starting and can be told
what is missing, rather than serving a mode it can confirm a condition under but never refuse it.
Passthrough does not need it. `memcold` answers it; a plugin that does not is a plugin to extend,
and the extension is one `SELECT` that keeps a column upstream already reads.

## A real server, over both seams, with nothing installed

`verify/e2e` boots a Temporal server — frontend, history, matching and worker, all four services in
the test process — over `memwal` and `memcold`, registers a namespace through the frontend, and runs
a real workflow with a real activity through the SDK. It needs no cluster, no container, no fixed
port, no cgo and no build tag: the ports come from the OS, the databases are in memory, and the
whole thing is `go test ./verify/e2e/`.

It runs twice. `TestAWorkflowRunsThroughTheLayer` hands the server the layer's factory;
`TestAWorkflowRunsWithTheLayerOutOfThePath` hands it the same store bare. Both compose a layer, and
that is what makes the control worth having — the passthrough arm's claim is that the layer it
composed saw *nothing*, which is a claim a run with no layer at all could not make. The green
workflow is the weaker half of both: a layer that quietly fell out of the path completes the same
workflow just as fast. So each arm ends in a `verify/witness` claim over the layer's own counters —
shards acquired through the layer, mutations acked, drains committed, history tasks written, task
pages merged, mutation kinds seen — and the intercept arm then reads each shard's watermark out of
the database, so a run claiming a drain committed and a store holding nothing cannot both be
believed.

**What it does not prove.** The database is in memory and dies with the process, so nothing here is
a durability claim. Nothing is killed, so nothing is a crash-recovery claim — replay is exercised
over an in-process log by `cycle`'s own tests and by no handover between two processes. One
workflow on four shards is not load, and no number this suite produces is a performance claim about
anything.

## Configuration

The layer's configuration is a `wal` section inside the custom datastore's own options
(`waltz.SectionKey`, `waltz.Parse`). An absent section is passthrough; a malformed one is a refusal
to start, because a misspelt key would otherwise be a server silently running the other mode.

Everything a cycle decides by is `cycle.Config`, read through a `cycle.Policy` at each decision
rather than at the acquire. `cycle.Fixed` is the policy that does not move; `waltz.NewPolicy` binds
the watermarks and the trim cadence to the server's dynamic config so they change under a running
node. `cycle.Defaults()` is the shipped configuration, and `Config.CheckBudget` is the arithmetic that
refuses a node before it boots: the per-shard tail bound times the shards this node may own must
fit the node's tail budget. [Chapter 08](docs/handbook/08-configuration.md) is every key.

## The packages

| Package | What it is |
|---|---|
| `.` (`waltz`) | `Compose`, `Backends`, `Layer`, and the `wal` section with its dynamic-config bindings. The only composition; the only door out |
| `wrapper` | the decorator over the plugin's `DataStoreFactory`. Its `ExecutionStore` answers eleven of its 28 methods differently in intercept mode and refuses a twelfth; its `ShardStore` reports the acquire. Passthrough changes none |
| `wal` | the log contract and its errors |
| `wal/memwal` | the log in process memory — the one backend shipped |
| `wal/waltest` | the conformance suite for the contract, and `Faulty` |
| `cold` | the cold store contract: `Applier`, `Watermarker`, and the four things an implementation owes |
| `cold/memcold` | that contract over Temporal's own SQL persistence, on a database in this process — the one cold store shipped |
| `cycle` | the state machine: one goroutine per (shard, epoch) owning the window, the drain, the apply, the replay, the trim and the three reads, so "who is touching this shard" has one answer |
| `cycle/window`, `cycle/tailstate`, `cycle/trim` | the window's size and age; the tail's arithmetic, which is what the hard bounds bound; the lazy deletion of entries the cold store already holds |
| `fold` | the compaction: per dirty workflow, one merged request plus the assertions it stands on |
| `mutation` | the record format — one `ExecutionStore` write request to the opaque bytes an entry carries, and back |
| `apply` | what a drain's outcome means: the five classes, and the errors that carry them. It writes nothing |
| `baserow` | the cold store's two pre-window mutable-state reads, as three packages that may not name each other need them |
| `walmetrics` | the layer's numbers, emitted through the server's own `metrics.Handler` — which arrives after the layer is composed, hence the late handover |
| `verify/...` | what judges the layer: the acceptance suites, the end-to-end server run (`e2e`), the guards, the witness, the generators, and the in-memory doubles (`coldtest`, `basetest`, `coldtasks`) |

The split is one-way: nothing outside `verify/` imports it in a non-test file, so what the library
ships and what judges it cannot be confused for each other.

## Status

This is a materialisation of a long research prototype, and it is worth being exact about what came
across and what has been built since. The layer's logic came across whole, with its test suites, and
both seams now have an implementation in the tree: `go test ./...` is green with nothing installed —
no cluster, no container, no port, no cgo, no build tag — and that run includes a Temporal server
booting over waltz and completing a workflow.

What is established, in order of how much it says:

* a real server, composed over waltz the production way, acquires its shards through the layer,
  writes its mutable state into the log, drains it into a database and completes a workflow — with
  a passthrough control run beside it and a witness over the layer's own counters
  (`verify/e2e`);
* a folded window, driven at volume over both real seams, leaves a real Temporal schema holding
  exactly what the batches said it should, including when the shard's epoch moves out from under a
  running cycle (`verify/acceptance`);
* the cold store passes Temporal's own four persistence suites (`cold/memcold`);
* the log satisfies the five guarantees, and `wal/waltest` would say the same of any implementation
  a deployment hands it;
* the fold folds a hundred thousand generated mutations without losing one, with the control that
  makes its collapse ratio a measurement rather than a number.

What is **not** established, and cannot be from inside this repository:

* **nothing here is durable.** Both shipped backends live in one process's memory and die with it.
  No fsync, no network and no quorum has ever been in the path of anything in this tree;
* **no process has ever been killed.** `verify/checker` is the judge written for a run under faults
  and it has no harness here, because a harness needs processes and processes need storage. Replay
  is exercised in process; a real handover between two owners on two machines is not;
* **no backend has been run under load**, and no number produced here is a performance claim.
  Whether a log answers sooner than a cold store would is a property of that pair, measured on the
  cluster it runs on;
* **nothing judges a fold against a store that was not folded for.** That needs a differential
  oracle — one stream applied twice, sequentially and folded, the two stores required to end
  identical — and an oracle needs two real cold stores.

[Chapter 15](docs/handbook/15-the-limits-of-the-evidence.md) is that boundary in full, entry by
entry, with which entries are measurements nobody has taken and which are the shape of a decision.
[`patches/`](patches/README.md) holds the fifteen-line patch that puts a composition over waltz under
upstream Temporal's own functional suites against a deployment's real store — wider coverage than
`verify/e2e`, at the price of a patched checkout, and the strongest evidence available from outside
a production cluster.

Start with the handbook: [`docs/handbook`](docs/handbook) is the reference (01–11) and the
reasoning (12–15). [`docs/adr`](docs/adr) holds the decisions somebody will otherwise try to
reverse, and [`CONTEXT.md`](CONTEXT.md) is the vocabulary every one of them is written in.

## Licence

MIT. See [LICENSE](LICENSE).
