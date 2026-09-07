# The layer at a glance

## A workflow writes more history than it keeps

Temporal's history service is write-heavy by construction. A single workflow can move through
hundreds of state transitions. Each transition produces an `ExecutionStore` write that the
persistence layer must make durable before reporting success to the caller. Against a persistence
implementation that is already well built, each of those writes is one immediate transaction
touching the workflow's rows plus a row per task it creates — nothing about it is wasteful on its
own, which is the point [chapter 12](12-the-write-before-the-layer.md#one-transaction-holds-the-whole-world-of-a-shard)
makes at length. The cost that grows is therefore not the cost of *one* write; it is the number of
writes. A workflow that
lives for a few seconds and dies writes and rewrites the same mutable-state rows dozens of times,
and creates task rows that are consumed and deleted long before anybody would have wanted them
stored. That profile — many thousands of short workflows, each moving through hundreds of
transitions in seconds — is the deployment this design targets, and it is an assumption rather than
a measurement; [chapter 14](14-where-the-defaults-came-from.md#the-premise-under-all-of-them) says
what rests on it.

Suppose those transitions belong to one workflow on one history shard. The first update writes a
new mutable-state row and creates a timer task. The next update rewrites the same state. Before the
workflow finishes, the timer is consumed and its task range is completed. The cold store has paid
for every version of the row and for a task whose entire lifetime fitted between two nearby
transitions.

`ExecutionStore` holds two things with different shapes, and only one of them is worth a window.
**Event history** is what a workflow replays against: a transition *appends* batches of events to
their own tables and never rewrites a batch it already wrote. **Mutable state** is the current state
of one run — which activities are started and how often they were retried, which timers are set,
which children are outstanding, where the history ends and what version all of it is at — and a
transition rewrites the run's row **whole** while adding and deleting the rows of its collections one
at a time. `InternalWorkflowMutation` carries exactly that shape: the entire `ExecutionInfoBlob` even
for a delta, plus a per-member upsert and delete map for each collection. The asymmetry is why the
layer intercepts mutable state and lets history transit — an appended batch is not rewritten by the
next transition, so holding it saves nothing, while a run row rewritten dozens of times per short
workflow is where the collapse lives.

The tempting solution is to batch those writes. An ordinary batch, however, delays the caller until
the batch commits and turns an efficiency mechanism into a latency queue; and a buffer that holds
writes without answering reads breaks the server on the next call, because the server reads back what
it just wrote across the same interface
([chapter 13](13-designs-that-were-rejected.md#a-buffer-inside-the-history-service)). To answer
earlier, the system needs another way to make the write durable first.

The shape of that answer is not new. A durable append-only log in front of the real store, an
acknowledgement once the log record is on a quorum, and aggregated updates travelling to the store
later, is how storage engines are built internally; by published description it is also how Temporal
Cloud's own custom persistence layer works — the
[post describing it](https://temporal.io/blog/higher-throughput-and-lower-latency-temporal-clouds-custom-persistence-layer)
gives the semantics and not the implementation, so everything here is an independent design that
lands on the same trade. What is not standard is doing it under a server that must not know: Temporal
goes on believing that it writes to a database, reads from a database, and that what it read is true.
Every mechanism in this book sits at one place where that belief breaks.

## An early acknowledgement creates a second truth

waltz puts a durable per-shard log in front of the cold store. Every intercepted write becomes one
log entry. In the shipped windowed configuration described by this chapter, once the append is
durable the cycle folds the mutation into an in-memory accumulator and normally returns to the
caller. The workflow's mutable-state rows in the cold store have not changed yet. The diagnostic
`sync` setting deliberately changes this order by making the caller wait for the drain; chapter 08
names that exception once rather than teaching it as an operating mode.

That separation is the design's useful trick and the source of nearly everything else in this
book. A confirmed write now exists in the log and the window, but not in the cold store. As a result:

* the stored row is missing writes the caller has already been told are safe, so a read is answered
  from the window as well as from the cold store;
* a process that dies can leave confirmed work behind, so its successor must replay the log;
* a former owner must not keep writing after a successor appears, so both the log append and the
  later cold-store transaction are fenced by the same epoch;
* the amount of confirmed-but-unsettled work must be bounded, because acknowledgement has turned it
  into a promise the system may not discard.

That last quantity has a name used throughout the book: the **tail** is the stretch of log entries
that are acknowledged and not yet settled in the cold store. The window is therefore more than a
batch buffer — it is the in-memory, folded front of the tail. Starting a drain clears that window,
but the corresponding entries remain charged to the tail until the drain's outcome is known;
[chapter 02](02-concepts-and-invariants.md#three-positions-not-two) defines both positions
precisely. Repeated writes to one workflow collapse into one merged
request, and a task creation can cancel against a later range deletion before either reaches the
cold store. When a mutation, byte or age trigger fires, one drain puts the folded batch, epoch check
and applied-seqno watermark into a single transaction.

## One write, and the interval it opens

1. The history service calls `UpdateWorkflowExecution` on `wrapper.ExecutionStore`.
2. The wrapper encodes the call as a `mutation.Mutation` and hands it to that shard's cycle.
3. The cycle checks the caller's condition and its own tail bound before writing anything.
4. It appends the record at the next seqno under the shard's current epoch. The log now contains a
   durable, ordered promise.
5. The cycle charges that promise to the tail and folds it into the window. If this write fires no
   drain trigger, it now returns to the caller: the append makes that answer safe. A write that fills
   the window waits while its own call performs the resulting drain.
6. A read during this interval combines the old cold row with the window. If the process disappears,
   the next owner reconstructs the same interval by replaying the log.
7. Eventually, a size, age, read, replay or shutdown trigger starts a drain. One transaction writes
   the folded requests and advances
   `applied_seqno`. A later trim may remove the log entries that transaction covered.

The irreversible boundary is step 4. Before the append, an error still belongs to this caller and
the operation can be refused without consuming a seqno. After the append, the record is durable, so
it must do one of two things: enter the tail and fold, or stay available for recovery. Returning to
the caller still waits until step 5. A later batch failure cannot take back the success earlier
callers were already given, and cannot honestly be blamed on any one of them. [Chapter 05](05-write-path.md) follows that distinction through every
failure class.

## The system map

This diagram shows one shard's write path, from the history service at the top to the log and cold
store at the bottom.

```mermaid
graph TD
  HS(("Temporal history service"))
  ES(("wrapper.ExecutionStore"))
  SS(("wrapper.ShardStore"))
  CY(("cycle: one goroutine per shard and epoch"))
  ACC(("fold.Accumulator: the window"))
  LOG(("wal.Log contract"))
  MW(("memwal: the in-process log"))
  AP(("cycle.Applier: one drain, one transaction"))
  CS(("the cold store"))
  MET(("walmetrics.Emitter"))

  HS -->|writes and reads| ES
  HS -->|updates shard| SS
  SS -->|fences| CY
  SS -->|transits| CS
  ES -->|hands mutations| CY
  ES -->|reads through overlay| CY
  ES -->|transits the rest| CS
  CY -->|appends| LOG
  CY -->|folds| ACC
  CY -->|drains| AP
  CY -->|trims| LOG
  LOG -->|stores entries| MW
  ACC -->|merged requests| AP
  AP -->|commits| CS
  CY -->|emits| MET
  ES -->|emits| MET
```

How to read this. Nothing crosses the shard boundary: every arrow out of `wrapper.ExecutionStore`
that is not a transit goes to *that shard's* cycle goroutine, which is the only thing that touches
that shard's accumulator, log and drain. Three kinds of path reach the cold store and only two
of them are drawn — the transits, which are the calls the layer has no shape for, and the applier,
which is the layer's only door to the base store's own transactions. The third is the base store the
layer uses for itself: an intercepted write puts its event slots down through it before the append,
and a read the window cannot answer, or an assertion the window hands on, falls through to it as
well. The arrows to `walmetrics.Emitter` are one-way: the emitter may not import anything it
measures.

Two of the boxes are seams rather than code this library ships. `wal.Log` is the log's contract,
and `memwal` is the only implementation in the tree; a deployment supplies its own. `cycle.Applier`
is the cold store's contract, and there is no implementation in the tree at all beyond the in-memory
double the suites run against; a deployment supplies that too. Everything between the two seams is
what waltz is.

The diagram has five conceptual roles: `wrapper` decides which persistence calls enter the layer;
`cycle` owns one shard's ordered decisions; `wal` makes them durable; `fold` keeps their readable,
collapsed form; and the applier moves that form into the cold store. The remaining packages support
or compose those roles. Their exact inventory belongs to [chapter 03](03-components.md); it is
included below as a reference for readers moving from the diagram into the tree.

### Reference: packages in the write path

| Package | What it is |
|---|---|
| `wal/` | the log's contract: one fenced, gap-free, totally ordered sequence of entries per shard, with `Fence`, `Append`, `ReadFrom` and `Trim`, and nothing about Temporal in it |
| `wal/memwal/` | the one shipped implementation of that contract, in process memory, so everything above the log can be tested without a cluster |
| `wal/waltest/` | the conformance suite an implementation runs to find out whether it is one |
| `mutation/` | what one entry *is*: the protobuf record of one persistence call, plus the record kinds |
| `fold/` | the accumulator: folds a window of mutations into one merged request per dirty workflow, preserves the assertions that request stands on, answers reads through the overlay, and merges task pages |
| `baserow/` | the cold store's two mutable-state reads as the write path needs them — one run's row, and the current-execution row with `last_write_version` beside it — shared because wrapper and cycle may not name each other's copy |
| `apply/` | what a drain's outcome demands of its caller: the five classes an error sorts into, and the attribution a violated invariant carries |
| `cycle/` | one goroutine per (shard, epoch) owning the accumulator, the drain, the trim, the reads and replay — the layer's state machine, and the two interfaces the cold store is reached through |
| `wrapper/` | the seam into a running server: a decorator over a base data store factory, whose `ExecutionStore` and `ShardStore` the history service talks to |
| `node/` | the composition a server builds: the `wal` section of the datastore options, and the components it names |
| `walmetrics/` | the layer's metric definitions and the emitter, on the server's own metrics handler |

[Chapter 03](03-components.md#the-packages-in-dependency-order) takes each of these apart
in the same order, naming the import ban that holds each in place and what owns what at run time;
[chapter 04](04-contracts.md) states what they promise each other, seam by seam.

## What one write costs, with and without the layer

Without the layer, one `UpdateWorkflowExecution` is one immediate transaction that rewrites the
workflow's mutable-state rows and inserts one row per task it carries. N transitions of one hot
workflow cost N such transactions and N sets of rows.

With the layer, that same call is:

* one **append** — which a log implementation is expected to make one immediate write over adjacent
  keys of its own storage. That expectation is invariant
  [I9](02-concepts-and-invariants.md#the-invariants); it is what keeps an append cheaper than the
  cold-store transaction it replaces, and nothing in this tree can check it for a backend it has
  never seen;
* plus a **share** of one later apply transaction. At the shipped watermarks that transaction
  carries up to 256 mutations.

The layer therefore still performs one durability write per call: it replaces each cold-store
transaction with a log append and amortises only the later cold-store work. Whether that is a win
depends entirely on the log being cheaper than the store, which is a property of the pair a
deployment chooses and not of this library.

Event history stands on both sides of that comparison, and outside the mechanism on both.
`wrapper.ExecutionStore.appendEvents` puts each of the mutation's event slots down through the base
store's `AppendHistoryNodes` before the mutation is acked — the same stage the incumbent pays, in a
different shape, which [chapter 12](12-the-write-before-the-layer.md#event-history-rides-separately-and-first)
takes apart. What matters here is that history rows are append-only, were never amplified, and are
therefore a share of a deployment's write volume the layer cannot address at all.
That share is the ceiling on the whole construction
([chapter 15](15-the-limits-of-the-evidence.md#event-history-stays-outside-the-log)).

Two consequences follow, and they are the project's actual goals:

* **write amplification against the cold store falls.** The rows a hot workflow rewrites N times
  are written once per drain, not once per transition. By what factor is unmeasured: the claim is
  structural, and
  [chapter 15](15-the-limits-of-the-evidence.md#write-amplification-against-the-incumbent-has-never-been-measured)
  says what the instruments that come close do and do not establish.
* **tasks can die in the window.** A range deletion folded into the window removes the tasks that
  window is already holding, so a task created and consumed inside one window is never written to
  the cold store at all. How large that share is depends on the ratio of drains to queue checkpoints. The
  layer reports both dropped and written rows so a deployment can measure the share for its own
  workload. The guarantee that makes the saving safe is invariant
  [I7](02-concepts-and-invariants.md#the-invariants); the percentage itself is workload-dependent.

The saving is not free. The system now has two durable positions to reconcile, an in-memory view
that reads must consult, a replay gate on a new owner, and a bounded tail of work whose callers have
already received success. A drain also makes one unlucky caller or timer pay for the whole batch.
The layer trades simpler, repeated cold-store transactions for a more complicated interval between
durability and application.

**Latency is explicitly not a goal.** A mutation was already one immediate transaction before the
layer existed, so a log append into the same class of storage costs about the same. The benefit is
fewer and smaller later writes to the cold store, not a promise that the original call returns
faster. A latency win would require a log that is cheaper to append to than the cold store is to
commit to; that possibility is why `wal.Log` is a contract rather than a fixed choice.

## What "mode" names here

Two settings decide how the layer behaves, and it is worth being clear from the start that they are
**independent axes** rather than one dial with four positions:

* **what the wrapper does** — passthrough or intercept, chosen by whether the node's config has a
  `wal` section (`wrapper.Options.Layer` nil or not). It decides whether writes go into the log at
  all.
* **what window the cycle keeps** — sync or windowed, chosen by `cycle.Config.Sync`, the `sync` key
  of that same section. It decides how many mutations one apply transaction carries.

Intercept says nothing about the window, and the window means nothing without intercept.

**Passthrough vs intercept.** The switch is exactly one field, `wrapper.Options.Layer`: nil is
passthrough, non-nil is intercept. In passthrough every call goes through to the base store
untouched and the wrapper makes no observations at all, metrics included; there is no cycle, so
there is no window to size. In intercept the wrapper takes **eleven** of `ExecutionStore`'s 28
methods into the layer, **refuses a twelfth**, and transits the rest:

* **eight writes become log records** — `CreateWorkflowExecution`, `UpdateWorkflowExecution`,
  `ConflictResolveWorkflowExecution`, `SetWorkflowExecution`, `DeleteWorkflowExecution`,
  `DeleteCurrentWorkflowExecution`, `AddHistoryTasks`, `RangeCompleteHistoryTasks`;
* **three reads are answered by the layer** — `GetWorkflowExecution` and `GetCurrentExecution`
  through the overlay, `GetHistoryTasks` through the task merge;
* **one is refused** — `CompleteHistoryTask`, with `wrapper.ErrCompleteHistoryTaskUnsupported`,
  because the log's deletion record is a range per category and has no shape for a single key;
* **the other sixteen transit**, exactly as they do in passthrough.

[Chapter 04](04-contracts.md#wrapperexecutionstore--28-methods) has the method table;
[chapter 07](07-read-path.md) has the three reads; and why the two task calls are records
while shard writes and event history are not is the **task record** entry of
[chapter 02](02-concepts-and-invariants.md#the-glossary-in-reading-order).

Two things about the shipped windowed configuration are worth having early. **A caller's condition
is judged before the append**, from the window itself plus a read of the pre-window rows — the ones
the window has nothing to say about. The caller is told with the store's own error, and no caller's
failed condition costs a shard: `wal_answered_condition_failures` stays at zero under a window,
because nothing reaches a drain to be answered. Sync is the exception the series exists for:
there the pre-window read is skipped, the ack is provisional, and a condition that fails inside the
drain is answered to the one caller in its window and counted in that series.

A drain can still meet a failed assertion — a condition the layer itself vouched for, failing
inside the transaction. Under fencing the layer is the shard's only writer, so nothing legitimate
can move a row the accumulator stood behind — this is the self-audit firing, not an operating
condition. It halts the shard because every writer in the batch has already been acked and there is
nobody left to tell; nothing is lost when it fires, the log keeps its entries and the trim stops.
[Path 6 of chapter 05](05-write-path.md#6-failed-drain--an-invariant-was-violated) is the whole
story; halts and replay in general are
[chapter 06](06-shard-lifecycle.md#5-halts-the-two-classes), and what an operator does about a halt
is [chapter 09](09-operations.md#b-a-shard-halted--and-which-of-the-two-classes).

## What this is not

* **Not a persistence implementation.** waltz stores nothing. The log is whatever satisfies
  `wal.Log`, the cold store is whatever satisfies `cycle.Applier` and `cycle.Watermarker`, and the
  drain hands the applier a folded batch rather than rows: the cold store's schema stays the base
  implementation's and nothing in this tree ever names a column.
* **Not a cross-shard log.** The unit is one shard's log with one writer, and the writer is made
  single by epoch fencing. There is no multi-writer shard and no cross-cluster story. The limit
  belongs to the layer's interface and not to the store: nothing here offers an operation that
  atomically changes two shards, whatever the store underneath is capable of — the layer simply
  never asks for one.
* **Not a general-purpose queue.** The log carries `ExecutionStore` mutations and history-task
  calls only. Shard writes (`GetOrCreateShard`, `UpdateShard`, `AssertShardOwnership`) stay
  immediate, because rangeID is both the fencing token and the task-id allocator; event history
  stays immediate too, and matching, visibility and cluster metadata never enter the layer at all.
* **Not a sidecar.** The layer is a library inside a custom `temporal-server` main, reached through
  the standard data store factory extension point. Process death is therefore an ordinary
  history-node failure.
* **Not a migration tool.** Shadow mode, canaries and online migration are out of scope by design.
* **Not visible to anything reading the cold store directly.** Between an ack and the drain that
  settles it, the truth about a shard is the accumulated window in one process's memory. A direct
  query against the store's tables, a backup, a dump or a second service reading those rows sees the
  shard as of the last committed drain — behind by up to a whole window (256 mutations, 256 KiB or
  5 s at the shipped watermarks), and by the whole tail while an applier is stalled. Nothing marks
  those rows as stale; they answer confidently. A consumer that must not miss acknowledged work goes
  through the layer's own read path, which is
  [chapter 07](07-read-path.md#1-route-only-reads-whose-answer-can-be-split).

### The boundaries of what has been demonstrated

Two boundaries belong with the design and not with the suites. **No performance claim is made**:
this library ships no storage, so there is no configuration of it whose latency or throughput could
be quoted, and the numbers in these pages that came off a running cluster came off the research
prototype this library was extracted from — they are named as such wherever they appear. And **the
workload the design is aimed at is an assumption**, so every saving named above is conditional on it.

[Chapter 15](15-the-limits-of-the-evidence.md) is the collected boundary — what each green target
does and does not establish, which limits are measurements nobody has taken and which are the shape
of a decision. [Chapter 11](11-verification.md) states each claim with the instrument that produced
it. Running a server and reading its numbers is [chapter 09](09-operations.md); every knob named
above is [chapter 08](08-configuration.md).

## Where this lives in the code

* [`../../baserow/baserow.go`](../../baserow/baserow.go) — the two cold-store reads the
  wrapper and the cycle both need, and why neither may hold its own copy.
* [`../../wal/wal.go`](../../wal/wal.go) — the log contract: `Fence`, `Append`,
  `ReadFrom`, `Trim`, and the guarantees stated on each.
* [`../../wal/memwal/memwal.go`](../../wal/memwal/memwal.go) — that contract in process memory,
  which is the only implementation this library ships.
* [`../../fold/histtasks.go`](../../fold/histtasks.go) — I7 inside the window: the range
  deletions, and which tasks they drop before a drain ever sees them.
* [`../../wrapper/execution_store.go`](../../wrapper/execution_store.go) — the
  eleven-of-28 partition and the refused twelfth, method by method.
* [`../../wrapper/shard_store.go`](../../wrapper/shard_store.go) — the one window onto
  shard ownership, and how an acquire is told from a heartbeat.
* [`../../cycle/cycle.go`](../../cycle/cycle.go) — the state machine, `cycle.Defaults()`'s
  shipped watermarks, and `Applier` and `Watermarker`, which are the whole of what the layer asks
  of a cold store.
* [`../../apply/failure.go`](../../apply/failure.go) — the five classes a drain's outcome sorts
  into, and which of them may be retried.
* [`../../fold/merge.go`](../../fold/merge.go) — what "rewrites the run's row whole"
  means to the fold: the last blob wins, and the collections merge member by member.
* [`../../config.go`](../../config.go) — the `wal` section: `sync`,
  `drain_on_read`, and what an absent or malformed section means.
