# Every way an acked write can be lost

The promise this file is about is [CLAUDE.md](CLAUDE.md)'s first rule: **data once
acknowledged is never lost.** Refusing a write is acceptable. Halting a shard is
acceptable. Taking the process down is acceptable. Losing a write that was acked
is not, and it is not made acceptable by the log or the cold store having been
configured badly — a warning followed by a successful write is a lie the caller
acts on.

**Two acks count, and they are different promises.**

1. `wal.Log.Append` returning nil: every entry up to that seqno is durable, and
   stays readable until a `Trim` passes it.
2. The layer answering a Temporal persistence call successfully: that mutation
   will reach the cold store, and until it does it is in the log.

A mutation acked at (2) that never reaches the cold store is exactly as much a
violation as an entry lost at (1). Most of this file is about (2), because (1) is
the caller's log to supply and this library's contribution there is the contract
and the suite that judges one.

Each entry is marked:

* **closed** — a mechanism in this repository prevents it, and the entry names
  the mechanism and a test that fails when it is removed;
* **open** — it can happen today, and the entry says what would close it;
* **accepted** — it can happen, nobody here will close it, and the entry says
  what is being accepted, why, and what a deployment owes in its place. A
  finished entry rather than a deferred one;
* **unknown** — nobody has established which. Treat these as open.

Severity is what the caller sees: **silent** (gone, no error), **loud** (gone,
with a named error), **unavailable** (present, unreadable).

**The most important thing on this page:** the two implementations that ship —
`wal/memwal` and `cold/memcold` — both die with the process. They are what lets a
Temporal server boot over this library in `go test` with nothing installed, and
they are not a durability claim of any kind. Everything below that is closed is
closed *in the layer*; a deployment's durability is the durability of the log and
the store it supplies. That is the first of the **accepted** entries below, which
are this page's floor: what cannot be established from inside this repository,
signed rather than left open.

---

## Closed

### The log

**An append whose outcome the contract cannot name.** Every transport failure
arrives as an error `wal` has no sentinel for, and whether the entry landed is
then not established. Reading it as "wrote nothing" and handing the seqno to the
next mutation is a race the backend settles and the caller was told both answers
of. The log is the witness: `Cycle.settleAppend` reads that one seqno back, and
an entry that is this cycle's own payload is a **success** the caller is told
about, nothing there is a seqno still free, and anything else halts the shard
holding the log as evidence. `TestAnAmbiguousAppend` (`cycle/cycle_test.go`).

**A log whose first entry is above where the replay resumes** (rung 4). Not a
backend breaking gap-freedom, but the shape a **cold store restored on its own**
leaves behind: a watermark that moved backwards — a database restored from a
backup, a replica promoted behind the leader, a watermark row rebuilt by hand —
below a trim that was legal when it ran. The entries in between are acked and gone.
Folding the log's first available entry as the next one applies a tail with a hole
in it, and the drain behind it commits a watermark saying the missing entries
arrived, after which the trim takes the rest. Replay confirms every entry's seqno
is the one it is waiting for and halts on a gap, which is all that is left to do.
`TestAReplayRefusesALogTrimmedPastItsWatermark` (`cycle/replay_test.go`).

Its isolation is the part worth keeping: on a tail that trips no watermark
mid-loop, the end-of-log confirmation catches the same hole one seqno later, so a
test with a short tail passes with the check deleted and judges nothing. It drains
per entry for that reason — measured, the first version of it was green against the
mutation it exists for.

**A tail a sync-mode node cannot replay** (rung 4). The page a replay reads with is
the window's own size, so a node in sync mode — window of one by construction —
reads its inherited tail one entry per page, and every such page is *full* and ends
exactly where the read began. `wal.Entries`' livelock guard has to admit `last ==
from` while refusing `last < from`, and nothing drove that boundary: the comparison
could be moved and sync mode's whole recovery would stop at the first entry with
"the reads are not advancing". A shard that cannot come up, on a mode this
repository ships and defaults away from rather than forbids.
`TestATailIsReplayedAPageAtATime` (`cycle/replay_test.go`), and — since a later
pass found that closure standing on a coincidence —
`TestAFullPageThatEndsWhereItBeganStillAdvances` (`wal/read_test.go`). The
coincidence is worth keeping: every case in the package that *owns* the guard
pages at 64, where a full page always ends far above its start, so the boundary
was reached only through a caller whose page size happened to equal a window of
one. Raising `max(cfg.Mutations, 1)` to a floor of 2 left the whole of
`go test ./...` green with the comparison still movable. It is held at the owner
now, and the cycle's case is what says sync mode reaches it.

**A replay that stops short of the log's end.** A page shorter than the one asked
for is the contract's "the log ends here", and a backend whose real limit is a
response size answers short for the size. A replay that believed it would come up
having folded a prefix of the tail and then serve reads and task pages missing
everything above the cut. The end is confirmed with a one-entry read rather than
inferred, and an entry found there halts the shard — charged to the tail where it
is this cycle's to account for, and reported as a failover where its epoch says a
successor wrote it.
`TestAReplayDoesNotTakeAShortPageForTheEndOfTheLog` and
`TestAConfirmationThatMeetsASuccessorReportsAFailover` (`cycle/replay_test.go`); the
backend-side obligation is `APageEndsAtItsLimitAndNotAtAByteBudget` in the
conformance suite.

**A trim that is not isolated from a concurrent append.** The trimmer runs on a
goroutine of its own so a slow trim cannot stop a shard from acking, so a trim is
always in flight while the log is being appended to. A backend whose trim is a
read-modify-write over the region the appends land in loses the entry acked while
it ran — no error, a log that simply ends lower, and the seqno handed out twice.
`TrimRunsBesideAppends` in the conformance suite; proved by giving `memwal`'s trim
a snapshot taken before a yield, which leaves every other case green.

**A seqno below a trim handed out again.** It would put a hole in a log everything
above reads as gap-free. `AppendBelowATrimIsRefused` in the conformance suite.

**A payload that aliases the log's memory, or the caller's.**
`PayloadsAreNobodyElsesMemory` in the conformance suite.

**A stale acquire replacing the owner that holds the shard** (rung 4). The server
hands out a strictly greater rangeID per acquire, so an acquire *below* the epoch a
cycle already holds is two observations delivered out of order. Installing the stale
cycle would retire the live one, and what the live one was holding is acked entries
no drain of the stale cycle can carry — fenced below the log's own epoch, every
write and every drain of it is refused, and the shard needs a third acquire before
anybody can apply them. Refused, and by two mechanisms: the registry compares the
epochs, and behind that the log's own `Fence` refuses it too, which is why deleting
the comparison leaves the test green. The comparison stays for the answer without a
round trip and for naming both epochs, the only evidence that the acquires arrived
out of order rather than that this node lost the shard.
`TestAnAcquireBelowTheHeldEpochIsRefused` (`cycle/cycle_test.go`) pins the
behaviour rather than either mechanism, and says so.

**A zombie writer appending after a fence.** `Fence` atomically cuts off every
lower epoch, and the drain asserts the epoch again as a compare-and-set before it
writes a row. `FenceCutsOffLowerEpochs` and `TwoWritersContendForOneShard` in the
conformance suite; `assertEpoch` in `cold/memcold/apply.go`.

### The drain

**A batch this store cannot write, written anyway** (rung 4). `memcold`'s drain
opens with five refusals — a zero epoch, a batch carrying nothing, a batch folded
for a different shard than the call writes, a request kind that reaches no write
path, a current-row assertion kind nothing evaluates — and all five were driven by
nothing: deleting the check left `go test ./...` green. Two of them are not
hygiene. **An empty batch answers nil**, so the drain reports success and commits
watermark zero, which tells the store every entry below it is applied and the trim
behind it takes them out of the log. **A batch folded for another shard** lands one
shard's rows under another's id, and both are wrong afterwards with nothing in
either saying so. `TestTheDrainRefusesWhatItCannotWrite`
(`cold/memcold/apply_test.go`), red in all three of its cases.

**A reset's second and third runs, acked and never written** (rung 4). A
conflict-resolve carries up to three runs — the run being reset, the run that was
current until now, and the new run the reset starts — each written in its own arm
of the applier. The last two were reachable from no fixture in the tree:
`mutgen.emitConflictResolve` emits the reset snapshot and `nil` for the other two,
`internal/verify/mutbuild` had no builder for the shape at all, and both arms of
the differential oracle run through this same applier, so a dropped arm cancels.
Deleting either left everything green. What it costs is a whole run's state: the
new run a reset starts is the run the workflow continues as, so losing it leaves
the current row naming a run with no execution row.
`TestEveryPartOfAResetReachesTheDatabase` (`cold/memcold/apply_test.go`), red for
each arm separately, over `mutbuild.Builder.ConflictResolve` — added for it, which
is the caller that package's doc said the shape was waiting for.

**A batch that lands half-applied.** One drain is one transaction, and a batch
that landed in pieces would leave rows no replay can reconstruct — the mutations
behind it were acked, folded and collapsed. `cold.Applier`'s first obligation;
`cold/memcold/apply.go` opens one transaction and commits once.

**A current-row assertion the drain never evaluates** (rung 4). What the layer
confirmed before the ack and what the drain asserts are the same question asked
twice, and the second asking runs inside the transaction that writes — between the
two the row can only have moved if the shard changed hands, which is what makes a
failure here a divergence rather than contention. Nothing drove it: the run-row
assertions have a test, fold's predicate has its own table, and no run put a
*current-row* assertion through a real drain against a row that does not satisfy
it. Unasserted, the window's write lands anyway — the current row stops naming the
run it named, and nothing above learns the workflow's pointer moved.
`TestADrainAssertsTheCurrentRowInsideItsTransaction` (`cold/memcold/apply_test.go`),
which answers `nil` with the evaluation deleted.

Its boundary was a shape away, and a later pass found it by negating the guard
rather than deleting it. The drain's work on that row is skipped when the record
carries **neither** the assertion nor the window's write, and those two facts are
independent: widening the skip to "either is missing" left the whole of
`go test ./...` green. What that reaches is the one kind that asserts the row
and writes it not at all — a **bypass-current** update or conflict-resolve,
whose whole meaning is "the current row names some other run". Every fixture in
the tree drove a kind that does both, `mutbuild` had no builder for the mode,
and the case above is a create, so the guard was proved against a defect that
cannot touch the shape it matters for. Unasserted, a write claiming its run is
not current lands while the row names exactly that run, acked.
`TestADrainAssertsACurrentRowItWillNotWrite` (`cold/memcold/apply_test.go`),
over `mutbuild.Builder.UpdateBypassingCurrent` — added for it — and driving both
sides, since a case that only refuses is green wherever something else refuses too.

**A timer written to the wrong table** (rung 4). Category is the one property of a
task that decides which table it goes to: a scheduled category is written by fire
time, and the timer category has `timer_tasks`, which the timer queue is the only
reader of. With the branch that picks it gone the rows go to the generic scheduled
table — the drain commits, the watermark moves, the log is trimmed, and the queue
reads its own table and finds nothing. An acked timer that never fires has nothing
behind it: no retry, and the workflow waits for ever. Invisible to the oracle for
the usual reason — both arms send the row to the same wrong table.
`TestATimerTaskLandsInTheTimerTable` (`cold/memcold/apply_test.go`).

**The same failure at the other five homes, and at the delete as well as the
write** (rung 4). The entry above is one arm of one of two switches. The fan-out
is a table per category — `insertImmediateTasks` special-cases transfer,
visibility and replication with a generic arm behind them, `insertScheduledTasks`
special-cases the timer, and `rangeDeleteTasks` repeats the whole shape for the
delete — and **transfer was the only category anything read back**. So
replication rows could be sent to the generic immediate table with the tree
green, and either *generic* range delete could have its bounds inverted, which
makes the `DELETE` match no row while the drain reports the range applied: the
queue that asked for it acks past rows that are still there, and nothing reads
them again. The generic arms are the ones that matter longest, being where every
category upstream adds next will land.
`TestEveryCategorysTasksLandWhereItsQueueReads` (`cold/memcold/apply_test.go`)
drives all six homes in both directions through the store's own per-category
read — which is what makes a row in the wrong table as invisible to the test as
it is to the queue — and asserts beside each that a second category's rows were
neither written into nor swept. Red for seven staged defects.

Its residue is named rather than left: `insertedAll`'s count check is an
**untested error path**. It exists for a driver that inserts fewer rows than it
was handed without saying so, and SQLite cannot be made to do that — a duplicate
key errors rather than under-inserting — so relaxing the comparison is green,
and staging the defect it exists for needs a fake transaction this store has no
seam for.

**A range delete that sweeps the task rows the same drain's requests carried**
(rung 4). The applier's second ordering rule — the range deletes before any task
row this drain writes — was held by a case staging its task through
`AddHistoryTasks`, which lands in the shard-level home written *last*. That is
not where most task rows are: a mutable-state write carries its own, they are
written inside the request loop, and `fold`'s `taskRows()` names that home first
for exactly that reason. So the deletes could be moved to after the request loop
with the whole of `go test ./...` green, and every task a drain's own requests
carried was swept by a range the same drain applied — for a scheduled category a
timer that never fires, with nothing behind it. The existing case stays green
under that move, which is what makes this a boundary rather than a duplicate:
each guard was proved red against a defect far from the boundary it claims.
`TestARangeDeleteActsBeforeTheTaskRowsItsRequestsCarry`
(`cold/memcold/apply_test.go`). Found by moving a statement rather than deleting
one, which is the class that reaches an ordering at all.

**A watermark written beside the transaction rather than inside it.** A shard
that either replays what it applied or trims what it did not. `SetWatermark` takes
the transaction rather than opening one.

**A drain whose outcome nobody could read.** Rounding an ambiguous code down to a
failure is how a committed batch is applied twice; rounding it up is how entries
are trimmed that never landed. It is its own class (`apply.ClassUnknownOutcome`),
answered by reading the watermark and nothing else, and until it is answered the
tail carries a floor that refuses every write and both reads.
`TestAnUnresolvedDrainStaysInTheTail` (`cycle/backpressure_test.go`).

**A collection of a run the drain acknowledged and never wrote.** Seven
collections are named in two literals, and one missing from either is rows
acked, folded and dropped with the watermark committed beside them. Nothing above
catches it: dropping two lines left the whole of `go test ./...` green.
`TestEveryCollectionOfARunReachesTheDatabase` (`cold/memcold/apply_test.go`).

**A run of a three-run request read out of another run's state** (rung 4). A
conflict-resolve names the run being reset, the run that was current and the new
run the reset starts, and each is adopted into a *part* of one pending request —
the part being what decides which snapshot a reader of that run is answered
from. Adopting the new run into the reset's own part answers every read of it
with the reset run's state, and folds a later delta of the new run onto the
reset's snapshot: the reset run's state destroyed and the new run's write lost,
both acked. Nothing drove it — no fixture reads or folds the new run of a reset
inside the window that created it — so the adopt could be moved with the whole
of `go test ./...` green, and so could the two arms that answer it.
`TestEachRunOfAResetIsReadOutOfItsOwnPart` (`fold/overlay_test.go`), which
asserts the read of each run and then that a write to the new run reaches the
new run — the half a read alone cannot see. Red for four crossings.

**A window folded out of a stream no single writer could have produced** (rung 4).
Three refusals exist for a log that is already corrupt — a create of a run the
window holds live, a continue-as-new into one, a mutation on a run it tombstoned —
and folding such a stream is how a corrupt log becomes a corrupt store: two pending
requests writing one run's rows, or a tombstoned run's state written back under it.
Every existing case drives streams that are valid, so each guard was deletable with
the tree green. `TestAStreamNoSingleWriterCouldHaveProducedIsRefused`
(`fold/fold_test.go`), which also pins the one shape that is *not* refused — a
create behind a tombstone, which is the run's next life.

**A drain that folds to nothing settling nothing.** A window can ack entries and
produce an empty batch, and leaving those entries unsettled strands them.
`TestADrainOfAnEmptyWindowSettlesNothing` (`cycle/tail_test.go`).

**A buffered batch filed under the wrong run of the same workflow** (rung 4). The
half the entry below could not reach: a row whose run comes off the batch and whose
workflow comes off the emitted request is wrong in a way no single-run fixture sees.
It needs one request owning two runs, which a continue-as-new is — the update
carries the closing run's delta and the new run's whole snapshot, and each run
accumulates the batches of its own mutations. Buffered events are what a signal
became while a workflow task was in flight, so a misfiled batch is a signal that
reaches the wrong incarnation's history: in the database, acked to its caller, and
flushed into a run it was never sent to.
`TestBufferedBatchesLandUnderTheirOwnRun` (`cold/memcold/apply_test.go`), red with
every batch filed under the first one's run.

**A delete of a run's collection the drain acknowledged and never applied**
(rung 4). The entry above enumerates the `Upsert*` fields of a delta, so the
delete half of the same seven collections was driven by nothing here — and by
almost nothing anywhere: `mutgen` removes sub-entity keys from **activities and
timers only**, so the differential oracle exercises two of the seven and the other
five had no guard at all. Dropping `children`, `requestCancels`, `signals`,
`signalsWanted` or `chasm` from the applier's `deletions` literal left the whole
of `go test ./...` green; dropping `activities` or `timers` was caught by the
oracle. What it costs is not the mirror of a dropped upsert, which is why it has
its own guard: the row *stays*, and a row that stays is state the sequential path
does not have — a signal id still in the requested set is a signal the next one
deduplicates against and drops, and a child or a cancel still present is a run
tracking something it has finished with. The write that removed it was
acknowledged. `TestEveryCollectionsDeletesReachTheDatabase`
(`cold/memcold/apply_test.go`), enumerated off the type in both directions so a
collection added later fails by name, and red for all seven lines.

**A snapshot-bearing write that does not clear what the run held before it**
(rung 4). The third literal of seven, after the applier's upserts and its deletes:
the clears a snapshot runs before writing whole state. A snapshot *replaces* a
run's tables rather than amending them, so a collection missing from
`clearCollections` leaves rows from before it in place — state the sequential path
does not have, answered to the next reader and written back by the next
snapshot-bearing write. The same five were unguarded here as in the deletes above,
for the same reason, and the same two were caught by the oracle.
`TestASnapshotClearsWhatTheRunHeldBefore` (`cold/memcold/apply_test.go`), red for
all seven clears.

**A deleted run's collections and buffered events, kept for ever** (rung 4). That
literal's second caller is `deleteRun`, and a later sweep found its whole
`clearCollections` call and its `deleteBufferedEvents` unguarded as well — both
deletable with `go test ./...` green. This is accumulation rather than loss: the
rows are unreachable once the execution row is gone, run ids being uuids that are
never handed out twice, so nothing reads them again to notice. What it costs is
that every workflow a namespace ever completes leaves its activities, timers,
signals and buffered batches in those tables permanently.
`TestADeletedRunLeavesNoneOfItsRowsBehind` (`cold/memcold/apply_test.go`) makes it
observable by asking for the same run id back, which is a probe and not a shape
Temporal produces — the tables are not reachable from outside the package, and
what is being pinned is the store's obligation that a delete removes the run's
rows. An earlier pass recorded this as deliberately unguarded on the grounds that
a leak is not a loss; the leak is unbounded, which is enough.

**A buffered-event batch the drain acknowledged and never wrote** (rung 4). The
entry above covers the seven collections named in two literals, and a buffered
batch is none of them: batches never merge, so the fold strips each onto
`fold.Emitted.BufferedBatches` with its run and the applier writes one row per
batch. Nothing enumerated off a request shape reaches them, so the guard above
was blind to the same failure it exists for — deleting the applier's loop over
them left the whole of `go test ./...` green, 22 packages, the differential oracle
and the e2e server included. What is lost is not a stale answer: a buffered batch
is what a signal became after its caller was told it had landed, and the run's
history flushes without it.
`TestEveryBufferedBatchReachesTheDatabase` (`cold/memcold/apply_test.go`), which
drives two batches of one run through a real drain and reads them back through
the store's own read. Its residue is named at the test: a batch filed under the
wrong run *of the same workflow* is still unguarded, that needing a request which
carries two runs at once — a continue-as-new or a conflict-resolve — which
`internal/verify/mutbuild` does not build.

**A snapshot's `Condition` dropped on replay** (rung 4). The codec carries it and
the decoder reads it back, and for the snapshot shape that line was driven by
nothing: the round-trip fixture set `Condition: 0`, and a zero cannot tell a field
that is carried from one that is dropped. For this repository's store the field is
inert — `memcold` asserts `DBRecordVersion` — but it is upstream's conditional-write
guard on a Cassandra-shaped plugin, so a deployment on one would have every
replayed create, set, reset and continue-as-new arrive **unconditional**: the write
lands without asserting what it was written against. Closed by giving the fixture a
non-zero condition, which `TestRoundTripUpdate` and `TestRoundTripEveryKind` then
hold (`mutation/mutation_test.go`).

**A field whose meaning moved between two binaries** (rung 4). `Payload.format`
is the only version there is and there is no migration path, so a node that
restarts on a new build replays a tail the old one wrote — an ordinary rolling
restart. The codec is a mirror with a second copy facing it, and the round-trip
cases drive both halves of one build: a slot swapped on **both** sides
round-trips perfectly. Swapping next-event-id with db-record-version in the
encoder and the decoder together left the whole of `go test ./...` green, and so
did swapping the child executions with the request cancels — five of the
mutation's scalars are `int64`, four of its upsert collections are
`map[int64]*DataBlob` and its delete sets share two key types, so each line can
be crossed with its neighbours and still compile. The blind spot is the
oracle's, one component over: both arms share the defect, so it cancels.
Only a record this build did not write can tell.
`TestARecordedEntryStillMeansWhatItsWriterMeant` (`mutation/record_format_test.go`)
decodes bytes recorded from a build that read them the way it asserts, and names
every slot two same-typed fields could have swapped. Red for three symmetric
crossings. The bytes are not a golden of what this build writes — re-recording
them from a changed encoder would assert nothing.

**A request the write path accepts and the replay path cannot fold** (rung 4).
The record carries a run's execution info and state as blobs and rebuilds the
structs from them, deriving nil where a blob is absent — so a request holding a
struct *without* its blob folds where it is written, is acked, and comes back out
of the log as neither. The two halves fail differently and neither is visible
from above: the fold dereferences the state, which panics every owner that
replays the entry rather than halting one, and the applier writes the info's
blob, which commits a row with the field simply missing. `Encode` refuses both
(`mutation.ErrUncarriedProto`), and it is the last place that can — past the
append every owner inherits the entry, while the refusal sits before the append
and before the fold, so it consumes no seqno and leaves the accumulator exactly
as it was. `TestARequestThatCannotRoundTripIsRefused`
(`mutation/mutation_test.go`), red in all eight of its cases with the two calls
removed. Not hypothetical: two fixtures in this repository were building the
shape, and both are now built through `internal/verify/mutbuild`.

**Every event batch of a slot but the first, acked and never written** (rung 4).
An intercepted write puts its own new history events down through the base store
before the mutation naming them is acked, and the refuted entry below says the
order cannot be got wrong. The *completeness* of that walk was a different
matter: a slot is a list — upstream's `ExecutionManager` serialises one
`InternalAppendHistoryNodesRequest` per `WorkflowEvents` it was handed, so a
transaction writing several batches to one run is an ordinary shape — and
`TestTheEventsGoDownBeforeTheMutation` drives every kind and every slot with
exactly one batch in each. It therefore pins which slots a shape has and never
that a slot is walked to its end, so stopping the inner loop after the first
append left the whole of `go test ./...` green, all eight of its own kinds
included. What that costs is the failure that case exists for, one dimension
over: a mutable state acked pointing at history nodes nobody wrote, durable and
correct-looking, which no functional suite sees.
`TestEverySlotsEventsGoDownAndNotJustItsFirst` (`wrapper/intercept_test.go`).

**A caller's deadline deciding the fate of an entry that is already durable**
(rung 4). The commonest thing that goes wrong in production is not a crash: it
is a request deadline expiring while the log is being written to, which a slow
log, a GC pause or a busy node all produce, and by then the entry may be down.
Three lines exist for it and **none was judged**. `Cycle.settleAppend` reads the
seqno back on a detached context, because the deadline is the commonest reason
the outcome became unreadable and a read on that clock could not answer in the
one case it exists for — without the detach the read fails, and a failed read
there is "an outcome nobody could read", which **halts the shard**. So every
expiring deadline would stop a shard. `Cycle.drain` detaches for the causes
whose window holds work whose callers were acked and have gone
([`drainCause.detached`]), and flipping the mutations watermark or the
drain-and-retry to keep the caller's clock strands exactly those entries in a
transaction abandoned on one writer's deadline. All four moves left the whole of
`go test ./...` green.
`TestACallersClockCannotDecideADurableEntrysFate` (`cycle/cycle_test.go`)
stages the deadline *inside* the append, through the log's own fault seam, and
reads the drain's view of its context off the applier. The third cause,
`drainWatermarkAge`, is the one that cannot be judged: its only call site is the
timer, which drains on a context of its own, so its flag is inert and a case
over it passes with the flag flipped — said at the cause.

**A trim past what the cold store holds.** The trim goes to `applied`, which only
a committed drain moves — never to what the window acked.

**A shutdown calling a shard clean that it never looked at.** A cycle replays
lazily, on the first request to reach it, so one installed by an acquire and then
left alone has never read its watermark and never seen the log — and what it
inherited is a dead owner's acked entries. Draining its empty window reported
nothing held, and `Layer.Shutdown` answered **nil**, which the operations runbook
reads as permission to remove the `wal` section: passthrough composes no log, so
those entries are never replayed by anyone. A shutdown now starts a cycle that
has not started before draining it, and a close that could not establish what its
shard holds is a residue of its own rather than a zero.
`TestAShutdownSeesATailNoRequestEverMadeItLookAt` (`waltz_test.go`).

**The same zero, in the two reads a caller has.** `RetireShard` is how a harness
stages what a killed process leaves behind, and the cycle it stops stays the
shard's — so `ShardStats` and `Totals` answer for it with no loop left to count,
and answered a zero tail while the entries sat in the log. The counters do die
with the goroutine; the tail does not, and both now read it off the mirror, as
the residue already did.
`TestARetiredShardStillReportsWhatItHolds` (`waltz_test.go`).

### The read

**A task page answered out of neither source.** The window empties when a drain
starts and the tail only when its transaction commits, so a read served anywhere
but the cycle's own goroutine can fall into the interval where a mutation is in
neither. Every read is a job on the loop.

**A task page answered without the window.** A page short a key is worse than a
stale row: its one caller completes the range it read and acks past what was
missing. A task page is therefore answered by a **running cycle or not at all** —
refused when the registry holds no cycle, when the cycle is retired, and at
either halt. `TestATaskPageIsAnsweredByARunningCycleOrNotAtAll`
(`cycle/tasks_test.go`).

**A pagination that crosses between the two token spaces.** A page the cold store
answered alone carries that store's own token, which a merging cycle cannot read;
continuing on the base alone drops the window out of every remaining page, and
the range the reader completes at the end deletes the acked rows that were in it.
No route hands out a foreign token any more, and one that arrives is refused
(`fold.ErrForeignPageToken`). The premise is not hypothetical: upstream's
`renewRangeLocked` (v1.29.6) drains in-flight task requests, bumps the range id
and updates the task key manager, and unloads nothing — the shard context and its
queue readers carry on, while `UpdateShard` with a moved range id is exactly what
hands this layer a new epoch.

**A cold store page larger than the batch it was asked for.** Where the window
alone overflows a page the ask is one row, and emitting that row is what advances
the base's cursor — so a row sent unasked is one the cursor passes unemitted.
Refused (`fold.ErrBasePageTooLarge`), rather than written down and trusted,
because this is the obligation whose breach the merge would carry out itself.

**A cold store that pages differently from `fold.BasePage`'s first three
requirements** (rung 4) — a row outside the range asked for, a page that does not
ascend or that descends below what the pagination has passed, an empty page beside
a token claiming more. All three were stated and trusted, on the ground that their
cost lands in a queue rather than in this package and that refusing would turn a
store's defect into a read that fails. The third is what reverses that: the merge
reads an empty page as the end of the pagination, so the queue completes its range
over rows it was never shown and **deletes acked task rows** — against which a
failing read is the cheap outcome. The other two are then free, the page being
walked anyway, and they name the store instead of panicking in `queues/slice.go`
or being skipped in silence by `queues/iterator.go`. Refused at the merge
(`fold.ErrBaseRowOutsideRange`, `fold.ErrBasePageNotAscending`,
`fold.ErrBasePageEmptyBesideAToken`), each against bounds the merge already holds
— the request's range, and the key this pagination last emitted, which no
conforming store can answer below.
`TestABaseThatBreaksTheRequirementsIsRefused` (`fold/taskpage_minimal_test.go`),
red in all three cases without the check; it is the test that used to assert what
each breach *cost*, over the same three staged breaches.

**A range delete that sweeps a row its reader was never shown.** The merge
subtracts the window's undrained ranges from the cold store's page and from
nothing else, at the store's own per-category predicate and resolution.
`TestAScheduledRangeComparesAtTheStoresResolution` and
`TestARangeReachesEveryHomeTheReadReaches` (`fold/histtasks_test.go`).

**A field of a read answer nothing fills.** A field that comes back zero is read
as a collection the run does not have, and a snapshot-bearing write then clears
the run's tables — an acked write deleted rather than an answer merely stale.
`TestEveryFieldOfAReadAnswerIsFilled` (`fold/overlay_test.go`).

**A delegated conflict that invents a start time** (rung 4). A current-row
conflict carries the row's start time so the start path above can run its
workflow-id reuse check, and a state with none must come back with none: upstream
reads an absent start time as a run that began at the zero time, so every interval
measured against it is enormous and the minimal-interval refusal never fires. A
start the namespace's policy forbids is then admitted — by this layer, where the
sequential path reading the same row would have refused it. The nil check in
`startTimeOf` was deletable with everything green, and what it would answer instead
is a pointer to 1970. `TestACurrentRowConflictCarriesTheStartTimeOrNothing`
(`fold/check_test.go`). Found by a sweep that deletes a guard clause rather than a
write — the mutation for an assertion being to make its condition always pass.

**A refusal the caller cannot act on.** Not a loss of data, and in this file
because the effect on a caller is the same: a current-row conflict carrying no run
id is one the history service declines to resolve, so a retried start that
collides with a run the layer already acked is answered with an opaque failure
instead of that run. The run comes off the response's own field and the request
ids out of the row's serialised state.
`TestTheDelegatedCurrentRowConflictNamesTheRunItCollidedWith`
(`fold/check_test.go`) and `TestTheCurrentRowReadCarriesTheRunAndItsRequestIDs`
(`cold/memcold/current_test.go`).

Both hold what the conflict *carries*; a later pass found that whether the
caller gets that conflict at all rests on an ordering nothing drove. One
request's two assertions are placed in one order — the workflow's current row,
then the run rows — and the store reports the first that fails. A retried start
fails both, so putting the run assertions first answers a bare
`WorkflowConditionFailedError` where the caller needed the current-row conflict,
with every row in the database identical and the write refused either way.
Swapping the two statements left the whole of `go test ./...` green.
`TestARetriedStartIsRefusedWithTheConflictItCanActOn`
(`cold/memcold/apply_test.go`). Found by moving a statement rather than
deleting one.

**A condition read that fails, answered as anything but a refusal** (rung 4). An
assertion the window does not determine is settled against the pre-window row, so
an unreachable cold store leaves the layer unable to answer — and there are three
wrong answers. Acking is the first rule's own violation: the caller is told a
conditional write happened with the condition never evaluated. Halting is the
second, a read that failed being no answer and a shard lost to a blip healing
nowhere. Reading it as a *condition failure* is the third, that being a divergence
this process owns and the next owner inherits. The write is refused with the
store's own error, the shard keeps running, and nothing is appended.
`TestAConditionReadThatFailsRefusesAndKeepsTheShard` (`cycle/cycle_test.go`), red
with either arm's error swallowed. `basetest.Store.FailAll` exists for this and was
called by nothing — the double could answer every read successfully with the whole
tree green.

**A write whose delegated assertion nobody could settle** (rung 4). An assertion
the window does not determine is settled against the pre-window row, so the caller
hands the write path the store's own two reads; a caller that brings none has
nothing to settle it with. The only two answers are to refuse the write or to ack
it with the condition unevaluated — a conditional write acknowledged by nobody
having checked the condition — and both arms of the delegated walk therefore ask,
so the refusal names the row the store would have judged first. Neither was driven:
deleting either check left the whole tree green, and what each does instead is
dereference the nil, which is the panic the refusal exists in place of.
`TestAWriteBringingNoBaseRowsIsRefused` (`cycle/cycle_test.go`), which isolates the
two arms with a `Set` for the run one — the current row is settled first wherever a
request asserts one at all.

**A shard-scoped read answered for a shard nobody holds** (rung 4). `ShardStats`
and `RetireShard` both answer a shard this node does not hold, and the answer is
the second return rather than a zero a caller could read as "held and empty". Every
existing case asks about a shard it has just acquired, so both not-held arms were
reachable from nothing, and without them each dereferences the nil the registry
hands back. Not a durability entry on its own; it is here because the shutdown's
residue is read through these, and a panic in the caller that is checking whether
the layer is safe to remove is the wrong failure.
`TestTheNarrowReadsAnswerForAShardNobodyHolds` (`waltz_test.go`).

**Backpressure reported as a possibly-committed write.** A refusal checked before
the append provably wrote nothing, and one `%w` around it turns that into a
self-inflicted failover. `TestTheBackpressureRefusalIsDefinitelyNotCommitted`
(`internal/verify/guard/`).

---

## Open

Nothing today, and the second time this page has been able to say so. Read it as
"the queue is worked", never as "the tree is clean": the entries below were found by
deleting a write, or a condition, and watching nothing fail, and the section that
says what this file is not says how to find the next one.

---

## Accepted

**Nothing in this section is held by a mechanism — that is what accepted means —
so every entry here stands at rung 5, and the bottom row of the rung table is the
whole of what carries it.** Accepting them is what gives this list a floor, and a
later pass that derives one again is finding the signature rather than a gap.

They fall into two kinds, and the difference is what a deployment can do about
them. The first three are one shortage seen from three sides: nothing here has
storage outside one process's memory, so there is nothing to restart, nothing two
writers can share, and nothing a killed run could be judged from afterwards. The
next two are obligations that live at a seam this repository cannot reach across —
an instrument exists for one of them and nothing but a sentence for the other, and
in both cases what is missing is a deployment's own storage rather than a
mechanism. The last is neither: it can be closed, and every way of closing it
changes what this layer tells a server about a failover, which is the one thing
the first three make unjudgeable here.

**Both shipped implementations die with the process.** `wal/memwal` holds every
shard's log in a map; `cold/memcold` is Temporal's own SQL persistence over a
SQLite database opened `mode=memory`, one per store. Neither has a file, an
fsync or a second reader, so nothing here has ever been restarted and no suite
judges storage that outlives a process.

*What is accepted* is not only that a restart loses everything — it is that
**every entry under Closed above is closed in the layer and nowhere else.** Each
one holds *given* a log and a cold store that keep their contracts, and this page
establishes neither. A deployment's durability is the durability of the two it
supplies.

*Why it stays accepted:* closing it means shipping durable storage, which is the
decision [ADR 0011](docs/adr/0011-each-seam-ships-one-implementation.md) took the
other way — one implementation per seam, in this process, so a Temporal server
boots over the library in `go test` with nothing installed. A second
implementation a deployment could run would be a different library, and a suite
over it would judge that library's storage rather than this layer.

*What a deployment owes in its place:* `waltest.RunContractSuite` and
`waltest.CheckRetention` against its own log, Temporal's four persistence suites
against its own store, and the folded-against-sequential comparison rebuilt over
that store rather than over `cold/memcold`.

**A backend whose `Fence` never reaches storage.** `RunContractSuite` drives one
`wal.Log` value in one process, so a displaced owner is refused by the same
in-process object its successor has just fenced, and a backend that records the
owning epoch in a process-local field passes every fencing case here — the
contention test included. Severity: silent, and the worst shape on this page —
two writers at one seqno, each told its append is durable.

*What is accepted* is that a green contract suite is a statement about a log's
**logic** and not about whether its fence reaches another machine, and that
nothing here instruments the difference — where the suite's other blind spot,
time, has `waltest.CheckRetention`.

*Why it stays accepted:* it needs two writers that share no memory. Two processes
is the honest form and there is none here. The narrower form — one process, two
independently constructed handles over one storage — would catch a process-local
epoch, and cannot be had either: `memwal.New` makes its own map, so the only
backend in this tree cannot supply the second handle, and a case added for it
would be skipped by the one backend that could ever watch it go red. A case no
implementation here can fail is the shape this file's procedure exists to refuse.

*What a deployment owes in its place:* stage the displaced owner against the
storage the log actually runs on — fence at a higher epoch from a second process,
then append from the first — and read the outcome off the log rather than off
either writer. Said at the instrument, on `waltest.RunContractSuite`, because the
carrier of this one is whoever writes the backend.

**Nothing is staged between two layer *processes*, and nothing at all between
clusters.** Two *owners* are staged, and the distinction is narrower than it
sounds: nothing in this layer speaks to another node, so two `cycle.Manager`s
over one log and one store are two nodes.
`TestASyncWriterIsNotToldItSucceededByAnotherNodesWatermark` parks one inside its
applier while the other takes the shard, replays its entry and drains over it,
and `TestARecoveredShardHoldsWhatAnUninterruptedOneDoes` supersedes an owner five
times without a drain and requires the successor's database to match an
uninterrupted run's. A partition is not unstaged either: there is no channel to
cut, every interaction between two owners going through the log and the epoch.

*What is accepted* is the absence of four things: a transport that hangs, a
`kill -9` between a call and its outcome, storage that survives either, and a
judge that reads a killed run's record back from outside the layer. The last is
the one no amount of care inside the layer substitutes for — an assertion
compiled into it sees what the layer *believes* and dies with it, which is why
the two runs named above establish the replay and never the kill.
`internal/verify/checker` is half of what such a run needs and says so — a call
line fsynced before the store is touched, an outcome line after, and no judgement
of either.

*Why it stays accepted:* the other half is a harness, and its subject is a
deployment rather than this library, which runs in process by decision
([ADR 0003](docs/adr/0003-wal-layer-runs-in-process.md)).

*What a deployment owes in its place:* that harness over its own log and store,
with `checker` as the record and the log as the arbiter.
[Chapter 15](docs/handbook/15-the-limits-of-the-evidence.md) is the long form of
what a green run here does not claim.

**A log whose storage expires entries.** Guarantee 5 excuses a trim and nothing
else, so a retention window, a TTL on the log's table or a compaction that drops
old records each break it — and each breaks it silently, taking acked entries the
cold store does not hold, which is the whole reason they were in the log.

*What is accepted* is that no run in this repository can see it. The conformance
suite finishes in milliseconds and cannot age an entry, so a backend whose storage
expires rows passes every case of it. This one differs from the three above in
having an instrument rather than only a boundary, and the instrument is not a
suite case for a reason no design can remove: the check costs the window it is
given in wall-clock time, and the length worth testing is the deployment's own.

*Why it stays accepted:* `waltest.CheckRetention` is as far as a library can take
it. It is proved non-vacuous here — `TestTheRetentionCheckIsNotVacuous` runs it
green against `memwal` and red against `waltest.Expiring` — so what remains is
somebody running it, which is not something this repository can do on another
deployment's storage.

*What a deployment owes in its place:* run it against a **deliberately shortened**
policy, because a pass says the entries outlived *that* window and never that the
backend has no retention. What is worth establishing is whether expiry exists as a
mechanism at all: a policy nobody applied to this table today is one somebody
applies to it next quarter.

**A store whose execution state omits the request ids.** The conflict a refused
write carries is built from what `baserow.Current` answered, so such a store gives
a retried start nothing to deduplicate against — an opaque failure where the layer
could have named the run it collided with. Not a loss of acked data; it is here
for the reason the delegated-conflict entry above is, that the effect on a caller
is the same.

*What is accepted* is that the obligation is checked for the one store in this
repository and unverifiable for any other. `memcold` is held to it
(`TestTheCurrentRowReadCarriesTheRunAndItsRequestIDs`), and the rule is stated
where an implementer meets it, on `baserow.Rows.Current`.

It was held on **one of the two arms**, which a later pass found by negating the
guard that chooses between them. `executionStateOf` reads the state out of the
row's blob where there is one and rebuilds it from the columns beside it where
there is not — upstream's own order, and the row without a blob is exactly the
record written before that column existed, which is the case the paragraph below
says a check could not tell from a breach. Every fixture here writes the row
through a real write, which always fills the blob, so the columns arm was
reachable from nothing: dropping its run id, its status or its create request id,
and forcing every read down it, each left the whole of `go test ./...` green —
and so did trusting a blob with only one of its two columns present.
`TestTheCurrentRowsStateIsReadBlobFirstThenColumns`
(`cold/memcold/current_internal_test.go`), internal because no exported path can
stage a blob-less row, and red for thirteen staged defects. It is the precedence
that is the claim rather than either arm: the blob carries every request id the
run has accumulated where the columns carry the create's alone.

*Why it stays accepted:* the layer cannot detect the breach, and this is the
uncommon case where that is provable rather than merely hard. An execution state
carrying no request ids is legitimate — upstream back-fills them for records
written before the field existed — so "no request ids" cannot be told apart from
"a row old enough not to have any". A check here would refuse valid rows.

*What a deployment owes in its place:* one read-back assertion over its own store,
that a current row's execution state carries the ids the create was issued with.

**A windowed write whose drain loses the shard is told it definitely did not
commit.** In a windowed mode the entry is appended and acked into the log before
the watermark trips the drain, so when that drain's `Apply` answers
`*p.ShardOwnershipLostError` the error travels out to the writer whose mutation
tripped it — and upstream reads that class as *guaranteed to have failed*,
dropping the request's task keys from its tracker and skipping its notifications.
The entry is in the log, above the watermark, and the successor applies it.

*What is accepted* is a false claim in the direction that matters, with no acked
entry behind it: the caller is told nothing happened about a mutation that will.
The blast radius is bounded by the same error class that causes it — ownership-lost
unloads the shard, which discards the tracker and the cache the claim would
otherwise have misled.

*Why it stays accepted:* every way out is a change to the failover signal, and
this repository cannot judge one. Two were worked out and neither is a local fix.
Answering something in upstream's **possibly-succeeded** set means answering an
error the shard's switch does not recognise, which reaches the default arm and a
background re-acquire — possibly-succeeded and a re-acquire at once, which is what
is wanted, but it gives up the unload that bounds the blast radius today.
Answering **nil** is defensible on this layer's own terms, the entry being durable
and the mutation certain to be applied, and it is the more honest of the two: what
failed is the drain, not the write. It also tells a node that has lost its shard
that its write succeeded, and leaves the ownership loss to be discovered by the
next call. Choosing between them needs a failover between two processes to judge
the result, which is the accepted entry three above this one.

*What a deployment owes in its place:* nothing it can do in configuration. What
it can do is know that on a windowed node, an ownership-lost answer to a write is
not evidence that the write did not happen — the log is, and the successor's
replay settles it.

---

## Unknown

Nothing today. That is a statement about this list and not about the layer: an
entry arrives here whenever a pass cannot establish which side of the line
something falls on, and the section being empty means only that none of the
entries above is in that state right now.

---

## Refuted

Established as impossible, or as not a loss, with the argument — so that a later
pass does not derive it again. This section is what makes the list converge:
without it every sweep re-checks what the last one cleared, and the re-checking
is most of the cost.

An entry here carries **how** it was established, because that is what a reader
has to weigh: `read` is one reader's derivation from the code, `measured` is a
staged defect or a probe, `structural` is a mechanism that makes the shape
unrepresentable.

**The trim never passes what the cold store holds** (measured). `Trimmer.Drained`
is reached only on the settle-forward path, after `Tail.Settle(..., MoveWatermark)`,
so the watermark it is handed is the seqno the committing transaction wrote. A
halted cycle does not trim at all: the log is the next owner's evidence. Handing
that call `Tail.Commit()` — the acked position — instead of `Tail.Applied()` is
red, so the derivation is no longer the only thing holding it.

**The write path cannot ack into a cycle that has not replayed** (measured).
`Cycle.add` calls `Cycle.start` before it reads the policy, takes a seqno or
appends anything. `Cycle.Close` was the one door that skipped it, and that is the
shutdown entry above. Moving the `start` call past the append is red.

**No intercepted write acks before its events are down** (measured). All eight go
through one `ExecutionStore.write`, which calls `appendEvents` before
`layer.Write`; a kind with no interception row is refused rather than transited.
There is no second door to keep in step. Swapping the two calls is red — which
says only that the *order* is held: how much of each slot goes down was a
separate question, and is the closed entry above about a slot's second batch.

**Event history staying outside the log is not a loss** (read). It was on the
open list on its own terms — a crash between the events and the ack leaves events
no mutable state points at — and the entry above is why that is the only order it
can happen in: the events are down *first*, so what a crash strands is unreachable
history nodes and never a mutable state pointing at events nobody wrote. Nothing
acked is missing, and the class of garbage produced is one upstream produces
itself and has a collector for: its own deletion path leaves a history branch
behind whenever stage 3 commits and stage 4 fails, "won't be accessible (because
mutable state is deleted) and special garbage collection workflow will delete it
eventually" (`service/history/shard/context_impl.go`, v1.29.6). So this is a
storage leak on a path upstream already leaks on, and
[ADR 0008](docs/adr/0008-the-log-carries-history-tasks-and-not-shard-or-event-writes.md)
holds the boundary. It stays worth knowing, which is what
[chapter 15](docs/handbook/15-the-limits-of-the-evidence.md)'s bound on the
saving is about — it is not a durability entry.

**No error is swallowed on the layer's write paths** (measured, by sweep). One
discarded error exists — the age tick's drain, which has no caller to answer and
whose outcome is on the state already.

**The registry cannot deadlock a node through a cycle's loop** (read). Every
`held` method finishes its map arithmetic and returns without calling into a
`*Cycle`, so no lock is held across the goroutine that a stuck cold store would
block.

**A sparse record cannot lose a field silently** (structural). `mutation`'s
field-set guard walks every request struct of every kind and fails by name on a
field that is neither carried nor recorded as deliberately dropped, and
`TestTheGuardCatchesAnUpgrade` is what says the guard is not vacuous.

**Sync mode's window really is one** (read). `Sync` is a section key read once
when the policy is built, not a dynamic setting, so no shard changes mode under a
live cycle and "a window of one by construction" holds wherever it is relied on.

**A queue reader does survive a range id renewal** (read, against upstream
v1.29.6). `renewRangeLocked` drains in-flight task requests, bumps the range id,
updates the task key manager and unloads nothing. This is a premise rather than a
hazard: it is what makes the task-page routing entry above reachable.

**The two `last_write_version` columns cannot drift** (read). The layer's condition
authority reads `current_executions.last_write_version` where upstream joins and
reads the executions row's, so the two copies must agree or every later condition
is judged against a stale one — the shape of an inconsistency that compounds rather
than surfaces. They cannot: each shape's current-row write takes the version from
the same struct that supplies the run row it names — `currentWriteOfSnapshot` from
the snapshot being written, `currentWriteOfUpdate` from the mutation or, on a
continue-as-new, from the new run's snapshot, `currentWriteOfConflictResolve` from
the new run's when there is one and the reset's otherwise — and a window records
the *last* writer, which is the mutation whose values the merged request carries.

Worth knowing about this one: it has no behavioural test and cannot have one from
outside the store. `p.InternalWorkflowMutableState` carries no `LastWriteVersion`,
so the executions row's copy is not readable through `p.ExecutionStore` at all, and
an assertion over it would need a table read. That is why this is recorded here
rather than closed.

**`memcold` writes two columns it never reads back**, and this is the second: the
current row's `start_time`. `currentRowResponse` builds its answer from the row's
state blob and `last_write_version` and touches the column not at all, so whether a
state with no start time writes NULL or 1970 is invisible to every read this
repository has — while for a deployment it is the value upstream's workflow-id
reuse check measures against, the same hazard the delegated-conflict entry above is
closed for. Both columns are therefore prose here by necessity, not by choice: an
assertion over either needs a reader this store does not expose.

**The oracle comparing the current row by its run alone was hiding nothing**
(measured). It diffs run rows and task rows whole and compared the current row by
the run it names, which leaves the row's own content — state, status,
last-write-version — outside every comparison the layer makes. That content is what
later conditions on the workflow are judged against, so a drift in it compounds
rather than surfaces, and the gap looked real. It is not, for a reason worth
recording: both arms derive that content through the same `currentWriteOf*`, so the
only part that can differ with the window is *which* mutation's write wins, and a
window that keeps the wrong one breaks the next assertion standing on the row. Made
to keep the first write instead of the last, the folded arm fails its own drain
("state 1 must be equal to 3") long before any comparison runs.

The comparison is widened to the whole row anyway, across all four places that make
it — the oracle, both recovery runs and the handover. It costs nothing and covers
the residue the argument leaves: a stream in which nothing ever asserts on that row
again. Recorded as measured rather than closed, because no defect available to stage
reaches it.

**The corpus reaching two of the seven collections costs no unguarded mechanism**
(read). `mutgen` upserts and deletes sub-entity keys for **activities and timers
only** — not their deletes alone, as an earlier pass wrote: it never touches
children, request cancels, signals, signal-requested ids or CHASM nodes in a delta
at all, so the differential oracle has never compared one. What that would add is
nothing, and the reason is that the per-collection surface is now enumerated in both
directions while the rest is generic. Every line that names a collection is held to
the request type by a test that fails on a new one by name — the applier's upserts,
its deletes and a snapshot's clears, and the seven `mergeItems` calls through
`TestEveryCollectionReachesBothFolds` — and everything else those five would drive
is `mergeItems` and `applyDelta`, one generic implementation each, which activities
and timers already drive on every run.

So the gap is real and its cost is coverage of shapes whose mechanisms are
enumerated, not of mechanisms. What it would still buy is volume in those
collections specifically, which no line distinguishes.

**An unpaired `DeleteWorkflowExecution` occurs, and the fold does not depend on
the pair** (read, against upstream v1.29.6). Two facts, and the first is what was
unknown. The unpaired shape *is* reachable: `ContextImpl.DeleteWorkflowExecution`
runs the deletion in four stages, marks each processed on the task itself
(`DeleteExecutionTask.ProcessStage`) and returns at the first failure — so a task
whose stage 3 failed is retried with stages 1 and 2 already marked and issues
`DeleteWorkflowExecution` with no `DeleteCurrentWorkflowExecution` beside it. What
makes that harmless is the order: stage 2 is marked only after it *succeeded*, so
a lone stage 3 is always preceded in the same stream by an acked delete of the
current row — in an earlier window, perhaps, which is all this layer needs.

And it needs less than that. `Accumulator.addDelete` collapses the run and
touches the current row not at all: what a window says about that row is derived
from `workflowAcc.cur`, which no tombstone drops (`drop` removes a pending request
and nothing else), so a lone delete can neither remove a row the sequential path
keeps nor discard a current-row write an earlier mutation of the same window
recorded. A second delete of a run already tombstoned is an explicit idempotent
no-op.

What is not established is the same conclusion by measurement:
`mutgen.emitDeletePair` always emits the pair, deliberately, so no differential
run has ever driven the lone shape. A knob there, off by default, plus one oracle
arm, is what would move this from read to measured.

---

## What this file is not

It is not a proof that the list is complete. An entry absent from it is one
nobody has written down, not one that cannot happen — which is why **unknown is
treated as open**. When a new way is found, it belongs here whether or not it is
closed the same day, and a fix that closes one belongs beside it with the test
that holds it.

**The cheapest way to find one is to delete a write and see what fails.** Every
entry here is a claim that some acked thing reaches storage, and a guard for it is
worth exactly what its absence costs: comment out the line that writes, run
`go test ./...`, and a green run names an unguarded path. Nineteen such deletions
found **eleven** unguarded lines in one sitting — the buffered batches, five of the
seven collections' deletes, and five of the same seven's clears — in a file whose
neighbouring entry already existed for exactly that failure. The same sweep
confirmed the orphaned tasks, the buffered clear, both upsert controls and all
twenty-four of `fold/merge.go`'s field carries were held, which is what makes the
negative results worth as much as the positive: the unguarded surface was the
**applier**, and the reason is structural. Both arms of the differential oracle
run through `cold/memcold`, so a defect in the applier shows up only where the
fold has made the two arms present it *different requests* — which is why the two
collections the corpus deletes from were caught and the five it does not touch
were not. An oracle cannot guard the completeness of a component both its arms
share; only a read-back can.

**Two more blind spots, measured rather than argued.** Fourteen mutations the
whole of `go test ./...` catches were re-run with the oracle as the only judge,
and it caught **seven**. The five it missed that matter name two limits beside
the one above.

The first is the **generator's reachable shapes**. One shape `mutgen` cannot
produce is the one fold's history-task keep-rule exists for: every task key comes
off a single monotonic counter — `taskID`, with a scheduled task's fire time
derived from it — so all keys ascend, while `emitRangeComplete` cuts each range
at `nextAbove` the last key *already written*. No generated task can fall inside
a range emitted before it, so "a task arriving after a range that covers it" has
never occurred in a stream either arm was driven with. That is how the
range-delete ordering entry under *The drain* above sat unguarded at one of its
three homes with every run green: moving the deletes past the request loop
leaves the oracle alone green, where the buffered-batch ordering and the reset's
clear beside it turn it red. The same measurement confirms what the collections
entry below says by reading — swapping the applier's signal and request-cancel
*deletes* is invisible to it, those being two of the five the corpus never
deletes from.

The second is that **the oracle compares what was written and never what is read
back**. Both arms are read through the store's own reads at the end, so a defect
in the merged task page — its cut, its token, its cursor — reaches no
comparison at all: setting a page's next cursor from its first key instead of
its last leaves the oracle green, and is caught only by `fold`'s own tests.

What would close the first is a knob in `mutgen` cutting a range above the last
written key, off by default, plus one oracle arm — the same shape the
unpaired-delete entry above is waiting for. **The rule to take from all three: an
oracle is bounded by what its generator reaches, by what its arms share, and by
what it reads back, and those are worth enumerating separately from the code's
branches.**

**A fourth class: cross two same-typed things.** The three above all remove
something — a write, a condition, a bound. This one leaves everything present
and doing the wrong job: a field assigned from its neighbour, a case label on
the arm beside it, an argument handed to the parameter next to it. It is what
finds a **hand-filled mirror**, and this tree is full of them — the read
answer's three, the fold's two merge functions, the applier's delta literals,
the codec's two facing each other. The reason the other three classes walk past
it is the reason a guard does: a crossed field is *present* and *non-zero*, so
every check of the form "is this filled" passes. Nine crossings in the read
answer's mirrors left the whole of `go test ./...` green, against a guard
written to enumerate that very answer off Temporal's type.

Two things sharpen it. **Look for the repeated type**: four of the six
collections are `map[int64]*DataBlob` and five of the merge's scalars are
`int64`, and those counts are exactly how many ways each line can be wrong while
compiling. And **cross a fan-out's arms, not only its fields** — routing a
category to the table beside it is the same defect one level up, which is how
the task fan-out turned out to be driven at one category of six.

Its own blind spot is worth naming, because it is the oracle's: a crossing
applied to *both* halves of a mirror pair cancels. The codec's encoder and
decoder face each other, so swapping two fields' slots in both round-trips
perfectly — and nothing else in the tree reads a record it did not just write.

**A third class: move a comparison to its adjacent form.** Deleting a write finds
what never reaches storage; deleting a guard finds a condition that always passes;
flipping `<` to `<=` finds the off-by-one, which neither of the others can see — the
line is present and does the wrong thing by one. For a layer whose correctness is
ranges, seqnos, page cuts and watermarks that is where the defects live, and it is
the class that caught **two of the guards written the day before**: each had been
proved red against a defect far from its boundary, which says nothing about the
boundary. A row at 20 in a range ending at 10 is not the same test as a row at 10.
Fifty-one comparisons over the applier, the merge, the condition authority, the
cycle, the decisions module and the log left three greens that mattered — those two
and the sync-mode page above — and the rest were equivalences worth naming: an
adjacent range that merges or does not cover the same keys either way, an assignment
of an equal value, a switch arm the case above it already matched.

**Not every green is a hole, and telling them apart is the work.** A sweep of the
second kind returns three sorts of green. A *hole* is a condition whose absence
changes what the store holds — the two entries above, and the conflict that invents
a start time. An *equivalent* mutation changes nothing observable: a fast path
whose guarded branch the following code would no-op through anyway
(`fold`'s check on an empty assertion set, the tombstone's two state nils), or a
refusal a second check downstream catches regardless (the codec's two, which the
encoder's own comment already explains). An *untested error path* is a
`if err != nil` whose call never fails in any fixture; that is coverage rather than
a defect, and chasing it means writing a fault injector per deserialiser. Read each
green before writing a test, and record the equivalents where the next sweep will
meet them — otherwise every pass re-triages the same forty lines.

It is not a suite and should not become one: a mutation run is a thing a session
does, and a target that had to stay green would be a second copy of the applier.
The mechanical half is `tools/mutation-run.py` — a runner, with no make target
and no checked-in list of mutations, for that same reason: a committed manifest
reads as coverage and leaves the next session re-running the last one's list
instead of inventing the mutations it did not think of. What a run found belongs
here; what it found and dismissed belongs beside the code.

It is also not a substitute for the reasoning. Each entry is a pointer: the
mechanisms live in `.claude/rules/`, beside the code, and the argument for each
lives in [the handbook](docs/handbook/README.md).

---

## How this file is worked

This list is the **queue**, not the report. A session that opens it takes named
entries and closes them; one that goes looking for something to fix instead is
how the list stops converging, because every edit is new surface and roughly one
defect in three found this way is the previous session's own.

**Each entry ends in exactly one of three states, and nothing else counts as
progress.**

1. **Closed** — a mechanism prevents it, with a test that fails when the
   mechanism is removed. Not a test that passes: one watched to go red.
2. **Refuted** — established as impossible or as not a loss, recorded above with
   the argument and how it was established.
3. **Accepted** — it can happen, nobody will close it, and the owner has said so
   in the entry with the reason. An accepted risk is a finished entry.

**Say which rung a closure stands on.** They are not equal, and "there is a test"
hides the difference:

| rung | what it is | what it rules out |
|---|---|---|
| 1 | a compile error — unexported fields of an exported type in a package of its own | the shape, structurally |
| 2 | exhaustive enumeration of a finite domain (`cycle/decide_test.go`'s tables) | everything in that domain |
| 3 | a differential or property run over a generated stream | what the generator reaches |
| 4 | one test, proved by staging the defect it exists for | that scenario |
| 5 | prose in `.claude/rules/` | nothing mechanically |

Rung 4 is where most of this file sits. Convergence means moving what can move to
1–3 and knowing, entry by entry, what is only held by 4 and 5.

**Two session shapes, never mixed.** A *hardening* session may change nothing
that does not close a named entry — no refactors, no simplifications, no
opportunistic tidying, however obviously right. Anything else is an *ordinary*
session, and its diff is worked as hardening afterwards, because a change that
improves the code still moves what every claim here was written against.

**The list is done when two consecutive adversarial passes, each on a context
that does not remember the last, produce nothing but stale documentation.** The
fresh context is not ceremony: a reader who remembers concluding something is
checking their own answer.

**The floor is signed, and Open is empty.** Every entry anybody has written down
is now closed, refuted or accepted. Three of the acceptances are the harness this
library does not build — a durable log, two processes, a kill, and a judge outside
all three — and the change that would reopen all three at once is durable storage
shipping here, which
[ADR 0011](docs/adr/0011-each-seam-ships-one-implementation.md) refuses. Two more
are obligations at a seam, one with an instrument and one with a sentence. The
sixth is the only one that can be closed from inside and has not been, because
every way of closing it changes the failover signal and the first three are why
that cannot be judged here.

**That is the convergence condition and not the end.** What it buys is that a pass
opening this file has nothing to pick up — which is exactly when the two
adversarial passes are worth running, each on a context that does not remember the
last. A pass that finds a named entry again should add nothing but a pointer; a
pass that finds something *not* named here has found the thing this file admits it
cannot rule out, and that entry goes in Open whether or not it is closed the same
day.
