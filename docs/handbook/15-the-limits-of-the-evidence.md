# The limits of the evidence

This chapter reads the green targets together: what they assert, and what they leave open. Most of
the answer follows from one fact: everything here lives in one process's memory and dies with
it. Both seams have an implementation, `wal/memwal` for the log and `cold/memcold` for the cold store,
so a suite can append, commit, boot a Temporal server and read rows back. No suite can fsync an
acknowledged write, cross a network, wait on a quorum, hand a shard to another machine, or be killed.
Every claim that depends on storage outliving its process is somebody else's to make.

## Which numbers survive a release

Constants and test bounds follow from the code. Measurements (percentages, one run's counters, bytes
on one machine) describe a machine at a moment. This book prints only the first kind as fact: the
shipped triggers (256 mutations, 256 KiB, 5 s, a trim every 16 drains or 60 s), the per-shard bounds
of 8192 entries and 8 MiB and the node budget of 256 shards and 2 GiB are all `cycle.Defaults()`. The
few measurements carried anyway (the collapse knee behind the drain trigger, the resident cost of a
byte of tail, the rejection of a two-mutation window) are attributed to the research prototype this
library was extracted from, and [chapter 14](14-where-the-defaults-came-from.md) gives each one's
provenance.

## Write amplification against the incumbent has never been measured

[Chapter 01](01-overview.md#what-one-write-costs-with-and-without-the-layer) states a goal: write
amplification against the cold store falls, because the rows a hot workflow rewrites N times are
written once per drain, not once per transition. The composition of one write is derived from the
code: one append, plus a share of one later apply transaction. Its magnitude is unknown. Two
instruments come close, and neither measures it:

* The fold ratio, `foldrun.Run.CollapseRatio` (mutations folded in over merged requests out), which
  `TestAcceptanceFoldNoCluster` asserts above 1.5. A control run shows only that the generator's own
  stream ratio moves with its reuse knob, `mutgen.Config.WorkflowReuse`
  ([chapter 11](11-verification.md#the-control)). None of it measures a workload.
* The two I7 counters, `wal_dropped_tasks` and `wal_written_tasks`, give a deployment the share of
  task rows a drain skipped because a queue had already completed past them
  ([I7, chapter 02](02-concepts-and-invariants.md#the-invariants)). That share is a workload
  measurement, not a constant of the implementation
  ([chapter 07](07-read-path.md#why-the-metric-is-two-counters-and-not-a-ratio)).

The direct measurement, how many cold-store rows one state transition rewrites under the incumbent
and by what factor the layer reduces that, needs a deployment's incumbent under its workload. So
chapter 01 gives no multipliers, and "write amplification falls" is a derivation, not a result.

## The shipped window has never been run against a store that has to plan

On the research prototype, the drain's statement was built by concatenation and grew with every
row, so no compiled plan was ever reused, and a window of 32 timed out while compiling. A timeout is
an unknown outcome, so the layer halted the shard rather than retry
([chapter 13](13-designs-that-were-rejected.md#a-folded-window-as-a-concatenation-of-the-stores-own-queries)).
The shipped query's shape depends on the assertion kinds and delete families a batch carries, not on
its row count. That is a property of a deployment's own applier, so
[chapter 11](11-verification.md#guards-a-deployment-must-build) lists it as a guard a deployment
rebuilds rather than inherits.

Half of this gap is closed. `TestBothSeamsRealNoServer` folds 6,000 mutations at `cycle.Defaults()`
into `cold/memcold`, one transaction per window, against Temporal's own schema and statements, so a
merged request the schema will not take is caught. Planning cost is untested: in-process SQLite
compiles a statement in about a map lookup's time, and no run has sent a 256-mutation drain across a
network to a store with a query planner under contention.

## The price of moving deferred work into the log is not measured

`AddHistoryTasks` and `RangeCompleteHistoryTasks` are two of the eight writes that become log
records. So the goroutine that drains is the one that writes task work and completes ranges, and a
queue processor's call waits behind whatever that goroutine is doing, a whole drain included. Every
write waits this way ([chapter 05](05-write-path.md#2-the-drain-itself)); the queues are most
exposed because they run on their own timers and deadlines. The shape was accepted in advance; its
price has not been measured. An operator meets it from the other side: when task drops climb,
runbook [(e)](09-operations.md#e-task-drops-are-climbing) reaches first for the incumbent's `history.*ProcessorUpdateAckInterval`, which changes how often a queue's
call meets a drain.

## Nothing is claimed about latency

Latency depends on the pair a deployment chooses: the log it appends to and the store it would
otherwise commit to. On the same class of storage the append costs about what the replaced write
cost, and the gain is fewer, smaller cold-store writes later
([chapter 12](12-the-write-before-the-layer.md#what-follows-a-log-on-the-same-database-buys-no-latency)).
A log cheaper than the store would win latency, and nothing here, the contract suite included,
measures it.

## The only log here is an in-memory one

`wal/memwal` is a real implementation: it fences, keeps seqnos gapless, survives a trim the way a
durable log must, and passes the same twenty-one contract cases as any other. But it is a map in
one process: nothing below it fsyncs, and no fence has had to reach another process.

A deployment's log, where the interesting failures live, is judged against its own storage by
`waltest.RunContractSuite`, `waltest.CheckReopen` and `waltest.CheckRetention`, plus a two-process
failover test the deployment writes. `memwal` cannot pass `CheckReopen` (a map has nothing to
reopen), and only a deployment can spend `CheckRetention`'s window.
[Chapter 11](11-verification.md#what-the-contract-suite-cannot-see) describes both checks and their
blind spots.

## The cold store is real, and it is in memory

`cold/memcold` is Temporal's own SQL persistence over a SQLite database in this process. Its 28
execution-store methods are upstream's, embedded, and Temporal's four exported persistence suites
judge them as they judge a plugin, so a folded window meets real statements and error classes.
Where a suite needs a drain refused or failing ambiguously, it uses a double instead,
`internal/verify/coldtest`.

Two limits remain. First, the database has no file, no fsync and no second reader, so it says what a
batch does to a schema and nothing about durability. The layer's half of recovery, a successor
replaying a superseded owner's tail, is covered
([chapter 11](11-verification.md#recovery-the-same-stream-a-different-set-of-windows)). What is
missing is the kill, not the replay.

Second, both arms of the oracle that says folding is transparent run on `cold/memcold`.
`TestFoldingChangesNothingButTheNumberOfTransactions` diffs one stream at the shipped window against
the same stream at a window of one mutation
([chapter 11](11-verification.md#the-fold-against-not-folding)). A defect both arms share cancels.
A rendering defect is one: a conflict-resolve's current row once landed with a NULL `start_time`,
and only a comparison against an unwrapped store's own write path, which does not ship, could see it
([chapter 13](13-designs-that-were-rejected.md#a-unit-test-per-fold-rule)). Nor does anything in the
oracle speak for the schema, row layouts or condition failures of a deployment's store.
Rebuilding the comparison over that store is the deployment's job, in place of a unit test per fold
rule ([chapter 13](13-designs-that-were-rejected.md#a-unit-test-per-fold-rule)).

The folded path knowingly answers differently from upstream's sequential path in three places, each
written down beside its code:

| the difference | recorded in |
|---|---|
| upstream's `dbRecordVersion == 0` fallback, which compares `next_event_id` against the request's condition, has no analogue: a run assertion here is always `DBRecordVersion − 1` | `cold/memcold/rows.go` |
| a create's current-row assertion is compared against `current_executions.last_write_version`, where upstream joins and compares `executions.last_write_version` | `cold/memcold/rows.go` (`lockCurrent`), with the reason at `applyCurrentRow` in `apply.go` |
| a row count other than one on an execution-row write is a condition failure here rather than upstream's `NotFound` | `cold/memcold/rows.go` |

All three follow from the write being acknowledged already: what fold checked before the ack and what
the drain asserts must be the same question. In the second row, reading upstream's column would let
the layer ack against one value and refuse against another. A deployment's applier meets the same
three questions.

## Where event history lands is the cold store's, and neither path is measured

If the cold store's applier declares `cold.HistoryApplier`, an intercepted write's event batches ride
the record into the drain. If not, they go through the store below before the append. Neither path
changes the number of history rows: history is append-only, so a window has nothing to merge.

So the fold collapses only the mutable-state half of the cost: a deployment dominated by event
history gains less than its collapse ratio suggests. That bound is a decision, not a gap.

The gap is the difference between the two paths. Carrying batches on the record saves a foreground
round trip per batch and lets a drain write a window's nodes at once. It also spends the record's
byte budget (the byte drain trigger, and the per-shard tail bound of
[I10](02-concepts-and-invariants.md#the-invariants)) on event blobs, so a window holds fewer mutations
and drains sooner. No run measures either side. Every green target exercises the batch-carrying path,
since `cold/memcold` declares `cold.HistoryApplier`, and the corpus that
[chapter 14](14-where-the-defaults-came-from.md#the-drain-triggers-256-mutations-and-256-kib) uses to
check the byte trigger carries no event batches, so that trigger has never been checked against
records that hold history.

## No partition between layer nodes is staged

The layer's nodes do not talk to each other: no gossip, no peer protocol. Two owners of a shard
interact only through the log and the epoch, so a partition between layer nodes has no channel to
cut.

Two nodes are two `cycle.Manager`s over one log and one store, so a two-owner interleaving can be
staged in one process, and one is
([chapter 11](11-verification.md#two-live-owners-and-the-two-fences)). A second
process would add only a real transport that hangs, a real kill, and storage that survives either.

Unstaged is everything on the other side of the two seams: a failure of the log or the cold store, a
split of either's storage, a slow replica. Neither the harness that would stage those nor the judge
that would read one back exists: `internal/verify/checker` records a driver's calls and outcomes,
and the judge that would read that record is not here.

## The saving on deferred work is not observable from outside

Invariant [I7](02-concepts-and-invariants.md#the-invariants) makes a dropped task row correct: a
committed drain need not write task rows whose range a queue had already completed past. The range
deletion is visible: `mutation.KindRangeCompleteTasks` is a log entry, folded and replayed like any
other. The drop is not. A row the drain did not write exists nowhere, so
a judge reading the log and the cold store finds the range record and no row, and cannot tell a
correct drop from a loss without reproducing the fold, which such a judge may not import.

So the mechanism's own tests hold the rule: `fold/tasks_test.go`, `fold/histtasks_test.go` and
`cycle/tasks_test.go` pin the pagination, the ordering, the dedup and the deletion rule, each at its
smallest. The merged read in `cycle/tasks_test.go` runs over `internal/verify/coldtasks`, a model of
a base store's two paginations, not a store.

## Nothing cross-cluster

One shard's log has one writer, made single by epoch fencing. There is no multi-writer shard and no
cross-cluster story. Upstream's cross-cluster and version-history suites are out of scope, and nothing
here has ever replicated between clusters.

## What a green run means

A green `go test ./...` says this and no more:

* the fold acceptance folded a hundred thousand generated mutations without losing one, and the
  windows collapsed at a fold ratio above 1.5, on a stream whose own ratio a control run shows to
  follow the generator's knob;
* the same fold, at the shipped window, executed against Temporal's own schema and left the rows and
  the watermark the batches said it would; when the shard's epoch moved under a running cycle, it
  left exactly the drains that had committed and nothing after them;
* the same stream, folded at the shipped window and applied at one mutation per transaction, left two
  databases with identical rows: every run row, every current row, and the task IDs of all four task
  categories;
* a shard handed to successor after successor mid-window, each replaying the tail it inherited, left
  the database an uninterrupted run of the same stream leaves; and a node parked inside its applier
  while another took the shard was not told its write succeeded on the other's watermark;
* the contract suite says `memwal` satisfies the five guarantees, and would say the same of any
  implementation a deployment passes it;
* Temporal's own four persistence suites say `cold/memcold` is a store a server can be run on;
* a Temporal server, composed the production way, acquired its shards through the layer and
  completed a workflow over it, and a passthrough control arm saw nothing;
* the guards say a set of decisions has not been reverted;
* the unit tests say each package does what its own rules say.

Together: a claim about this library and two in-process backends, on one generated workload and one
workflow. A green run is not, and cannot be:

* a claim about storage that outlives a process: durability, recovery, or a fence reaching another
  machine;
* a claim about a deployment's own log or cold store, whichever pair it chooses;
* a claim that a composition over this library survives upstream's full functional coverage. The
  strongest form of that is upstream's own suites against a real store, which
  [`patches/README.md`](../../patches/README.md) makes reachable and which nothing here runs;
* a performance claim of any kind.

## This is not a roadmap

These limits read like a backlog and are not one: no entry carries an intention to close it. Each is
one of three kinds:

1. A measurement against a real deployment: a cluster and an afternoon, not a design.
2. Work nobody has started, such as a harness that kills processes or the oracle rebuilt over a
   deployment's store: a deployment first, then a project.
3. Not closable: the boundary of a decision that was taken.

| limit | kind |
|---|---|
| the incumbent's write amplification is unmeasured | 1 — a measurement |
| the shipped window against a store far enough away to plan | 1 — a measurement |
| the price of deferred work waiting behind a drain | 1 for the number, 3 for the shape |
| nothing is claimed about latency | 1 for a given pair, 3 for the claim in general |
| the only log here is in memory | 2 — a real log and a two-process failover test |
| the cold store here dies with the process | 2 — a durable store, and a harness that kills something |
| the fold is judged against the sequential path over one store only | 2 — the same oracle over the store a deployment runs |
| no failure of the log or the store is staged | 2 — a harness with processes to kill |
| nothing cross-cluster | 2 — a project |
| no partition between layer nodes | 3 — the nodes speak only to the log and the store |
| the number of history rows is untouched | 3 — history is append-only; there is nothing to fold |
| neither history path is measured against the other | 1 — a measurement |
| the saving on deferred work is unobservable from outside | 3 — a row a drain never wrote exists nowhere |

## Summary

Everything in this repository runs in one process's memory. Both seams have real implementations,
`memwal` and `memcold`, so the fold executes against Temporal's own schema, agrees with not folding,
survives handover and replay, and carries a Temporal server through a workflow. None of it fsyncs,
crosses a network or survives a kill.

Only numbers derived from the code are printed as fact. The measurements that would turn the
structural claims into magnitudes (write amplification, a window against a store that must plan,
task work behind a drain, latency, the two history paths) have not been made, and most cannot be
made here. Each limit is a measurement a deployment can take, a project nobody has started, or the
boundary of a decision, and the last table says which.

## Where this lives in the code

* [`../../internal/verify/checker/checker.go`](../../internal/verify/checker/checker.go) — the three
  outcome classes a driver can report, and why the third, the call whose outcome nobody knows, has to
  exist.
* [`../../internal/verify/coldtest/coldtest.go`](../../internal/verify/coldtest/coldtest.go) — the
  double that interprets nothing, with the reason at the top.
* [`../../cold/memcold/memcold.go`](../../cold/memcold/memcold.go) — the store that is not a double,
  and what the embedding covers and does not; [`apply.go`](../../cold/memcold/apply.go) is the
  drain's transaction statement by statement, and [`rows.go`](../../cold/memcold/rows.go) carries the
  three divergences from upstream's sequential path.
* [`../../internal/verify/coldtasks/coldtasks.go`](../../internal/verify/coldtasks/coldtasks.go) — the
  two paginations it models, and the paragraph headed "what can make it a lie".
* [`../../wal/memwal/memwal.go`](../../wal/memwal/memwal.go) — the one log here: a map of shards
  under one mutex, and why its next seqno is state rather than derived from the rows around it.
* [`../../internal/verify/foldrun/foldrun.go`](../../internal/verify/foldrun/foldrun.go) —
  `Run.CollapseRatio`, the fold ratio every acceptance run prints.
* [`../../fold/fold.go`](../../fold/fold.go) — `Stats.CollapseRatio`, the layer's own counters, which
  the cycle reports as `wal_drained_mutations` and `wal_drained_workflows`.
* [`../../walmetrics/walmetrics.go`](../../walmetrics/walmetrics.go) — `wal_dropped_tasks` and
  `wal_written_tasks`, tagged by task category.
* [`../../wrapper/execution_store.go`](../../wrapper/execution_store.go) — the method partition:
  which calls become log records, and which stay on the incumbent path.
* [`../../cycle/cycle.go`](../../cycle/cycle.go) — `Defaults()`, the shipped triggers and bounds.
* [`../../patches/README.md`](../../patches/README.md) — the evidence a composition can produce that
  this repository cannot, and the fifteen lines that make it reachable.

[Chapter 11](11-verification.md#the-levels-of-evidence) describes each instrument in full, and
[chapter 09](09-operations.md) shows how to run one.
