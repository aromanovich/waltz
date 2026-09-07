# waltz

waltz puts a write-ahead log in front of a Temporal history shard's cold store, so that many
mutable-state writes are acknowledged into the log and folded into one cold-store transaction. It
ships as a persistence decorator: you compose it over the plugin that owns your cold data and hand
the result to `temporal.WithCustomDataStoreFactory`, the same door a custom persistence backend
already goes through. It implements no persistence itself — the log and the cold store are both the
caller's, and the only log shipped here is `wal/memwal`, in memory.

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
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wal/memwal"
)

func main() {
	// Your cold store, behind the two interfaces waltz reaches it through.
	cold := newColdStore() // cycle.Applier and cycle.Watermarker

	layer, err := waltz.Compose(
		waltz.Backends{
			Log:       memwal.New(), // your wal.Log; memwal is the only one shipped
			Writer:    cold,
			Recoverer: cold,
		},
		cycle.Fixed(cycle.Defaults()),
		waltz.DefaultTaskCategories(),
		log.NewCLILogger(),
		nil, // the server's metrics handler arrives later, through the factory
	)
	if err != nil {
		panic(err)
	}

	// base is the persistence plugin that owns the cold data.
	var base client.AbstractDataStoreFactory = newPlugin()

	s, err := temporal.NewServer(
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
		// to write to.
		layer.Shutdown(context.Background(), 30*time.Second)
	}()
}
```

`Compose` opens nothing, reaches nothing and takes no context — everything that talks to storage
happens while the backends are built, and whatever they hold stays yours and must outlive the
layer. `Backends` is a parameter and not something `Compose` builds, which is the point: this
library implements none of the three.

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

### The cold store: `cycle.Applier` and `cycle.Watermarker`

```go
type Applier interface {
	Apply(ctx context.Context, shard wal.ShardID, epoch wal.Epoch, batch fold.Batch) error
}

type Watermarker interface {
	Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error)
}
```

**waltz ships no production implementation of either.** `verify/coldtest` is the in-memory double
the suites here compose against: it records what a drain carried and what watermark it moved, and
interprets nothing, because what a folded batch *means* has no specification apart from the
incumbent store's behaviour.

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
Passthrough does not need it.

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
| `cycle` | the state machine: one goroutine per (shard, epoch) owning the window, the drain, the apply, the replay, the trim and the three reads, so "who is touching this shard" has one answer |
| `cycle/window`, `cycle/tailstate`, `cycle/trim` | the window's size and age; the tail's arithmetic, which is what the hard bounds bound; the lazy deletion of entries the cold store already holds |
| `fold` | the compaction: per dirty workflow, one merged request plus the assertions it stands on |
| `mutation` | the record format — one `ExecutionStore` write request to the opaque bytes an entry carries, and back |
| `apply` | what a drain's outcome means: the five classes, and the errors that carry them. It writes nothing |
| `baserow` | the cold store's two pre-window mutable-state reads, as three packages that may not name each other need them |
| `walmetrics` | the layer's numbers, emitted through the server's own `metrics.Handler` — which arrives after the layer is composed, hence the late handover |
| `verify/...` | what judges the layer: the acceptance suites, the guards, the witness, the generators, and the in-memory doubles (`coldtest`, `basetest`, `coldtasks`) |

The split is one-way: nothing outside `verify/` imports it in a non-test file, so what the library
ships and what judges it cannot be confused for each other.

## Status

This is a materialisation of a long research prototype, and it is worth being exact about what came
across. The layer's logic came across whole, with its test suites: `go test ./...` is green with
nothing installed — no cluster, no container, no build tag.

What did not come across is every backend that needed one. The prototype ran the `wal.Log` contract
against five real logs and the cold store against a real Temporal persistence plugin; none of them
is here, because none of them is this library. What is here in their place is the contract, the
conformance suite that judges an implementation of it, and one in-memory backend that passes it.

So the evidence this repository can produce for itself is the evidence a library can: the contracts
hold, the layer above them does what the suites say, and the seams are answerable without a
cluster. The evidence for a *deployment* is a deployment's:
[chapter 15](docs/handbook/15-the-limits-of-the-evidence.md) is the honest boundary of what the
green suites claim, and [`patches/`](patches/README.md) holds the fifteen-line patch that puts a
composition over waltz under upstream Temporal's own functional suites — real frontend, history and
matching, real workflows — which is the strongest evidence available from outside a production
cluster.

Start with the handbook: [`docs/handbook`](docs/handbook) is the reference (01–11) and the
reasoning (12–15). [`docs/adr`](docs/adr) holds the decisions somebody will otherwise try to
reverse, and [`CONTEXT.md`](CONTEXT.md) is the vocabulary every one of them is written in.

## Licence

MIT. See [LICENSE](LICENSE).
