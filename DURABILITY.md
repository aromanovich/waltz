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
* **refuted** — established as impossible, or as not a loss, and recorded with
  the argument and how it was established, so that a later pass does not derive
  it again;
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

Its isolation is the part worth keeping: on a tail that trips no trigger
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
(`cold/memcold/apply_test.go`), red in all three of its cases; the other two refusals
are default arms over values no fold can produce, unreachable from outside the
package, and the test says so.

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

**A run recreated behind its own tombstone, asserted twice** (rung 4). Not a loss
and in this file for the reason the refusals above are: a halted shard is the
outcome, and the shard halts over a stream `fold` admits by design, with every row
in the database exactly where the stream put it and nothing for a replay to fix.

A window that updates a run, deletes it and creates it again is the only one that
emits two requests naming one run. The head-of-window run assertion is the
*window's* claim about the pre-window row, and it rode both: at the Delete it holds
(the row is there at v2), and at the Create it is judged against the row that
delete removed a statement earlier inside the same transaction, so it answers
*must exist* and the drain is classified `ClassInvariantViolated`. `Drain` places
it at the first emitted request naming the run now, on the rule the workflow
record's own assertions already had.
`TestARunRecreatedBehindItsOwnTombstoneIsAssertedOnce`
(`cold/memcold/apply_test.go`) is the drain half and
`TestCreateBehindTombstone` (`fold/fold_test.go`) the shape half — that one existed
and asserted the defect, with the reason beside it: "the Create keeps the
head-of-window run assertion, because at apply time the pre-window row is still
there", which is true of the delete's placement and false of the create's. The same
sentence stood in `.claude/rules/fold.md`.

*Where it came from.* This is the Unknown entry that read "the assertion a create
behind a tombstone registers", and it is worth recording that neither side of the
line that entry named was the answer. It predicted the drain would plausibly pass
and the pre-append check would plausibly refuse; the check passes (`decideRun`
judges the create's own must-not-exist against the window's tombstone, not against
the pre-window row) and the drain is the half that fails. An entry marked unknown
is a question worth driving rather than a guess worth refining.

**An entry that appends, acks, and decodes for nobody** (rung 4). The closed entry
*A request the write path accepts and the replay path cannot fold*, one field along
and found the same way — by asking which of a package's refusals no valid stream
reaches. `Decode` parses exactly two of the blobs a record carries, the execution
info's and the state's, and it admits **proto3 alone**: any other encoding is
`unexpected blob encoding`. `Encode` took any encoding at all.

So a mutation whose state blob arrived in another encoding was appended, acked and
durable, and then failed to decode for **every owner that inherited it**: each
reads the tail, fails at that blob, leaves the cycle unstarted, and the next
request retries it. Severity: **unavailable**, for good, on a shard whose log and
cold store are both healthy — which the first rule admits only where nothing was
acked, and here the caller has been told the write succeeded.

The refusal is `Encode`'s now (`mutation.ErrBlobEncoding`), for exactly
[ErrUncarriedProto]'s reason: this is the last place that can refuse, and before
the append refusing writes nothing. The decoder's own refusal stays where it is,
for a record some older binary wrote.
`TestABlobEncodingDecodeCannotParseIsRefusedBeforeTheAppend`
(`mutation/mutation_test.go`), red in both of its cases with the check removed.

*Reachable or not, and why it did not matter.* Temporal's own `ProtoEncode`
produces proto3, so no stream this layer sees today carries anything else — which
is the same claim that was made about the parsed-struct-without-a-blob shape in
that entry, where two fixtures in this repository were found building it. The
codec already refuses a Cassandra-shaped CHASM blob at encode for the identical
reason; this was the one blob field where the refusal sat on the far side of the
ack.

[ErrUncarriedProto]: mutation/mutation.go

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
tail carries a floor that refuses every write and every read.
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
half the buffered-batch entry below could not reach: a row whose run comes off the batch and whose
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
(rung 4). *A collection of a run the drain acknowledged and never wrote*
enumerates the `Upsert*` fields of a delta, so the
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
entries above cover the seven collections named in the literals, and a buffered
batch is none of them: batches never merge, so the fold strips each onto
`fold.Emitted.BufferedBatches` with its run and the applier writes one row per
batch. Nothing enumerated off a request shape reaches them, so the guard above
was blind to the same failure it exists for — deleting the applier's loop over
them left the whole of `go test ./...` green, the differential oracle
and the e2e server included. What is lost is not a stale answer: a buffered batch
is what a signal became after its caller was told it had landed, and the run's
history flushes without it.
`TestEveryBufferedBatchReachesTheDatabase` (`cold/memcold/apply_test.go`), which
drives two batches of one run through a real drain and reads them back through
the store's own read. It cannot see a batch filed under the wrong run *of the
same workflow*, which needs a request carrying two runs at once; that half is
*A buffered batch filed under the wrong run of the same workflow* above.

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
Over a cold store that does not declare `cold.HistoryApplier`, an intercepted
write puts its own new history events down through the base store before the
mutation naming them is acked (`appendEvents`; over one that does, the record
carries them and the drain writes them), and the refuted entry below says the
order cannot be got wrong. The *completeness* of that walk was a different
matter: a slot is a list — upstream's `ExecutionManager` serialises one
`InternalAppendHistoryNodesRequest` per `WorkflowEvents` it was handed, so a
transaction writing several batches to one run is an ordinary shape — and
`TestTheEventsGoDownBeforeTheMutation` drives every kind and every slot with
exactly one batch in each. It therefore pins which slots a shape has and never
that a slot is walked to its end, so stopping the inner loop after the first
append left the whole of `go test ./...` green, all eight of its own kinds
included. What that costs is the failure that case exists for, one dimension
over: a mutable state acked pointing at history rows nobody wrote, durable and
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
([`drainCause.detached`]), and flipping the mutations trigger or the
drain-and-retry to keep the caller's clock strands exactly those entries in a
transaction abandoned on one writer's deadline. All three moves left the whole of
`go test ./...` green.
`TestACallersClockCannotDecideADurableEntrysFate` (`cycle/cycle_test.go`)
stages the deadline *inside* the append, through the log's own fault seam, and
reads the drain's view of its context off the applier. The third cause,
`drainWatermarkAge`, is the one that cannot be judged: its only call site is the
timer, which drains on a context of its own, so its flag is inert and a case
over it passes with the flag flipped — said at the cause. The bytes trigger and
the storage-pressure drain detach on the same flag, and no case here flips
either: for those two the flag is held by the table it sits in and nothing
else.

**A trim past what the cold store holds.** The trim goes to `applied`, which only
a committed drain moves — never to what the window acked. A forced trim under
storage pressure takes the same field at each of its three sites — the
settle-forward path, an acquire that has just read the watermark, and the age
tick — and the follow-up coalesced behind a running trim takes the highest of
values each of which was that field once, so bypassing the cadence bypasses no
part of the bound.

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
`renewRangeLocked` (v1.29.6) drains in-flight task requests, bumps the rangeID
and updates the task key manager, and unloads nothing — the shard context and its
queue readers carry on, while `UpdateShard` with a moved rangeID is exactly what
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
red for all three breaches without the check, across its five cases; it is the
test that used to assert what each breach *cost*, over the same three breaches.

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

**A window's conflict that invents a start time** (rung 4). The entry above one
house along, and the commoner of the two here: a layer that has acked a start into
its log answers the retry out of the *window*, the row the delegated side would
read not existing yet. That conflict is built from the blob the window will write
(`fold.currentConflict`), and it carried no start time at all — so the reuse check
above read the zero time and the minimal-interval refusal never fired, for the
whole of a window's life and again at every window that writes the row.
`TestTheWindowsCurrentRowConflictCarriesTheStartTimeOrNothing` (`fold/check_test.go`),
red without the field. It was not found by a sweep: it was written down as a known
shortfall in the comment above that function, which is where a defect goes to be
read past.

**A current row written without the start time the policy above it measures**
(rung 4). What the two entries above are about, in the row rather than in the
error, and it outlives both: `currentWriteOfConflictResolve` rendered a
conflict-resolve's current row from four fields — run id, create request id, state
and status — where both upstream plugins pass the snapshot's own execution-state
blob through. `memcold`'s `writeCurrentRow` recovers the row's columns from exactly
that blob, so `start_time` landed NULL and the reduced state landed in `data`.
Durably, and nothing back-fills a start time: every later reader, the sequential
path included, measures against a run that began at the zero time, so
`WorkflowIdReuseMinimalInterval` never fires again for that workflow — the reuse
arm skips its *Too many starts* refusal and the terminate arm terminates a live run
instead of answering `ResourceExhausted`. The request-id half is narrower and real:
`WorkflowExecutionStateFromBlob` back-fills `RequestIds[CreateRequestId]`, so an
ordinary retried start still deduplicates, while every id `AttachRequestID`
accumulated — an attached start, an update-with-start — is gone, and `FAIL` then
raises a false *already started* where `TERMINATE_EXISTING` terminates a live run.

The rendering now collapses to the snapshot upstream's own arms pick (`newWorkflow`
when there is one, else `resetWorkflow`) handed to `currentWriteOfSnapshot`, which
is *less* code than the four-field build.
`TestCurrentWriteTracksTheLastWriter/a conflict-resolve keeps the start time and
every request id` (`fold/currentwrite_test.go`) is the blob half, and
`TestTheCurrentRowsColumnsComeFromItsBlob` (`cold/memcold/currentrow_internal_test.go`)
is the column half — a separate test because that column is derived from the blob
by a line of its own, and every read in this repository answers blob-first, so the
line can be deleted with the whole of `go test ./...` green.

*Why nothing here caught it, and what changed.* The oracle that ships drives one
stream folded and unfolded, and a window of one still renders the row through the
same function, so both arms carried the reduced blob and it cancelled — this file's
standing caveat about that instrument. Beside it sat a fixture gap: `mutbuild`'s
`runningState` carried no start time, where upstream's mutable state fills one at
creation and never unsets it, so no hand-built fixture could tell a rendering that
carries the field from one that drops it. It carries one now.

*What this cost to establish, and it is the entry's point.* The divergence was
recorded as **deliberate** in three documents — ADR 0012, handbook 13, handbook 15
— and none of the three said what it bought; the ADR gave its address ("that is
what fold hands the applier") where a reason should have been. Handbook 15 even
listed it as the one of its four differences that did *not* follow from the layer
having already acked the write. A list of deliberate divergences is worth
re-reading for exactly that shape: a row that cannot say what it buys is a defect
that has been written down. All three documents now say so, and
`fold/currentwrite_test.go`'s assertion that upstream "writes run, create request,
state and status — nothing else" is gone, it having been false about both plugins.

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
the `false` each returns rather than a zero a caller could read as "held and empty". Every
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

**A tail whose records carry event batches, replayed over a store that never
said it writes them.** Severity: silent — the mutable state lands, the events it
points at do not, and the drain reports a commit. A record carries its request's
event batches only where the store the writer was composed with declares
`cold.HistoryApplier` (`wrapper/execution_store.go`, `Manager.WritesHistory`).
Replay does not ask: `fold.Accumulator.addHistory` takes whatever the record held,
so the batch the replay drain hands `cold.Applier.Apply` carries
`fold.Batch.History` whatever the successor's store declares. A successor composed
over a store that does not declare the marker — a deployment moving off
`cold/memcold`, or two builds of one deployment disagreeing about their store —
meets history on a path nothing else ever hands it, and nothing refuses it. The
contract is unambiguous — the package doc's first obligation owes `Batch.History`
from every applier handed it, declared or not, and `HistoryApplier`'s doc now says
so — but no suite hands a history-carrying batch to an applier without the marker,
so one written against the live path alone, which never sees the field, passes
everything here and drops the rows on its first replay of such a tail.

*What would close it:* replay refusing — halting the shard as an invariant
violation — an entry that carries event batches when `Manager.WritesHistory` is
false, or writing those batches through the base store before the replay drain,
the way a live write over such a store does; with a test that replays a
history-carrying tail over an applier without the marker and is watched to go red
without the refusal.

Found by a documentation pass reading ADR 0014's paragraph on changing stores
against `fold/history.go`: the paragraph then said such a tail replays correctly
anyway, and `addHistory` is where that stopped being true.

**A field Temporal adds to an event batch, dropped from the record with every
suite green.** Severity: silent, and only on the path where the record is the
batch's one copy — a store that declares `cold.HistoryApplier`, `cold/memcold`
included. `mutation/history.go` mirrors `InternalAppendHistoryNodesRequest`,
`InternalHistoryNode`, `HistoryBranch` and `HistoryBranchRange` field by field,
and the field-set guard that makes a new field of every other mirrored struct a
named failure (`mutation/fieldset_test.go`'s `mirroredStructs`) walks none of
them. A `go.temporal.io/server` bump that adds a field there compiles, encodes the
batch without it, acks, and drains the batch as the record held it.

*What would close it:* the four structs as rows of `mirroredStructs`, each field
recorded as carried or derived with the reason, so a new one fails the guard the
way a new field of the mutable-state requests does — watched to go red by adding a
field to a copy of one of them.

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

*What a deployment owes in its place:* `waltest.RunContractSuite`,
`waltest.CheckReopen` and `waltest.CheckRetention` against its own log, Temporal's
four persistence suites against its own store, and the folded-against-sequential
comparison rebuilt over that store rather than over `cold/memcold`.

**A backend whose `Fence` never reaches storage** — *narrowed, and the narrowing
is worth reading before the rest of this entry.* `RunContractSuite` drives one
`wal.Log` value in one process, so a displaced owner is refused by the same
in-process object its successor has just fenced, and a backend that records the
owning epoch in a process-local field passes every fencing case here — the
contention test included. Severity: silent, and the worst shape on this page —
two writers at one seqno, each told its append is durable.

*What was wrongly accepted, and the argument that did it.* This entry used to say
the narrower form — one process, a second handle over the same storage — "cannot be
had either: `memwal.New` makes its own map, so the only backend in this tree cannot
supply the second handle, and a case added for it would be skipped by the one
backend that could ever watch it go red." Both halves are true and the conclusion
does not follow: the same two facts are true of *time*, and the answer there was not
a suite case but a **function a deployment calls, proved in-tree against a
decorator** — `CheckRetention` against `Expiring`. This page names that
instrument, in the entry on expiring storage below, as what that blind spot has.
So the shape was sitting in the file and was read past.

`waltest.CheckReopen` is that shape applied here. It takes a way of *opening* a log
rather than a log: fence, append a run, close, open the storage again, and ask the
fresh value for the entries, for who owns the shard, and for the position to
continue at. `waltest.Unfenced` — a log whose epoch lives in this process while its
entries are the wrapped log's — is the double it is proved against, and
`TestTheReopenCheckIsNotVacuous` watches both halves go red, the ownership half
against exactly the backend this entry describes. Its *other* half closes something
this page had not named at all: guarantee 3 says an acked append is durable, every
case in the suite reads back through the value that appended, so a backend acking
into memory it never gets out of the process passed all 21 — and nothing anywhere
asked.

*What stays accepted* is the part that genuinely needs two processes: a fence
**racing** a displaced owner's append. A reopen asks a quiescent question — who owns
this shard now — and cannot ask whether the fence and the append are ordered against
each other under contention. Two writers sharing no memory is the honest form of
that and there is none here.

*What is accepted* is therefore narrower than it was: a green contract suite plus a
green reopen says the log's logic is right *and* that its entries and its epoch are
in storage, and still says nothing about the ordering of a fence against a
concurrent append on another machine.

*What a deployment owes in its place:* stage the displaced owner against the
storage the log actually runs on — fence at a higher epoch from a second process,
then append from the first — and read the outcome off the log rather than off
either writer. Said at the instrument, on `waltest.RunContractSuite`, because the
carrier of this one is whoever writes the backend.

*What stands in for it today, and what that costs to give up.* The delegated reads
`Cycle.checkDelegated` takes before an append are the only thing in the tree that
notices this failure while the caller is still on the line: a second writer that
has been writing has moved the versions those reads sample, so the write is
refused **before** the ack and the caller retries. They are not a barrier and must
not be read as one — nothing is locked between the read and the append, so against
a truly simultaneous writer this is a race, and the real barrier is the epoch
assertion inside the drain's transaction, which runs *after* the ack. What they
buy is therefore narrower and worth naming exactly: a violation that persists is
detected at a **repeatable request** instead of at a halted shard with acked
entries in the log that no replay will apply.

That is the standing argument against every proposal to drop those reads — a cache
of versions this layer wrote itself, or trusting the `DBRecordVersion - 1` the
request already carries. Both are sound under the assumption the whole design
already makes (the writers of these rows are exactly this layer), both are
genuinely cheaper, and both move this entry from *accepted and detectable* to
*accepted and silent*. Neither is a configuration flag; either one is an edit to
this page first. The assumption stops being an assumption only if the cold store
itself refuses a row write that carries no current epoch, which nothing in either
seam asks for today.

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
having an instrument for the whole of what it accepts rather than only a boundary,
and the instrument is not a
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
write carries is built from what `baserow.Rows.Current` answered, so such a store gives
a retried start nothing to deduplicate against — an opaque failure where the layer
could have named the run it collided with. Not a loss of acked data; it is here
for the reason *a refusal the caller cannot act on* above is, that the effect on a
caller is the same.

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
a trigger trips the drain, so when that drain's `Apply` answers
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

**Nothing.** The one entry here — the assertion a create behind a tombstone
registers — was settled by the run it asked for, and neither side of the line it
named was the answer. It is closed above, under *a run recreated behind its own
tombstone, asserted twice*; what the driving found was a third thing, which is
what such an entry is for.

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
red, so the derivation is no longer the only thing holding it. `Trimmer.Force`
(read) added two reach paths beside that one and both hand the same field: an
acquire calls it after `Tail.Floor` has planted the cold store's own watermark
and before anything appends, and the age tick calls it on the loop, where only
a committed drain has ever moved `applied`. The follow-up a Force queues behind
a running trim coalesces to the highest of values each of which was `applied`
at its own request, and `applied` never moves down, so the coalesced watermark
is one the cold store held.

**The write path cannot ack into a cycle that has not replayed** (measured).
`Cycle.add` calls `Cycle.start` before it reads the policy, takes a seqno or
appends anything. `Cycle.Close` was the one door that skipped it, and that is the
shutdown entry above. Moving the `start` call past the append is red.

**No intercepted write acks before its events are down** (measured). All eight go
through one `ExecutionStore.write`, which — over a store that does not declare
`cold.HistoryApplier` — calls `appendEvents` before `layer.Write`, and otherwise
leaves the batches on the record for the drain (the entry below); a kind with no
interception row is refused rather than transited.
There is no second door to keep in step. Swapping the two calls is red — which
says only that the *order* is held: how much of each slot goes down was a
separate question, and is the closed entry above about a slot's second batch.

**Event history cannot be published behind the state that names it, whichever
writer puts it down** (read). It was on the open list on its own terms — a crash
between the events and the ack leaves events no mutable state points at — and it
closes the same way on both of the two paths a cold store can choose between
(ADR 0014), because the *order* is what is pinned and the mechanism is not.

Where the store does not declare `cold.HistoryApplier`, the wrapper puts the
events down through it *first*, so a crash strands unreachable history rows and
never a mutable state pointing at events nobody wrote. Where the store does
declare it, the events and the state are one record with one ack, so there is no
interval between them at all; what can then strand is a drain that made the
history rows durable and failed before its transaction, which leaves the same
unreachable nodes. **The forbidden order is unreachable in both**: the contract on
`cold.Applier` is that every history row the batch carried is durable no later
than the transaction publishing the state — inside it or before it opens — and
`memcold` keeps it by putting them inside that transaction.

The class of garbage is the same in both and is one upstream produces itself and
has a collector for: its own deletion path leaves a history branch behind
whenever stage 3 commits and stage 4 fails, "won't be accessible (because mutable
state is deleted) and special garbage collection workflow will delete it
eventually" (`service/history/shard/context_impl.go`, v1.29.6). So this is a
storage leak on a path upstream already leaks on. Nothing acked is missing on
either path, which is why this stays a read rather than an open entry.

What is *not* closed by it and is named rather than counted: the three history
methods that transit past a window which may hold their rows — `DeleteHistoryNodes`,
`DeleteHistoryBranch`, `GetHistoryTreeContainingBranch` (named as an exposure
in [ADR
0014](docs/adr/0014-a-record-may-carry-the-event-batches-its-own-request-produced.md)).
A `DeleteHistoryNodes` from `TrimHistoryBranch` finds no row and the drain writes
that row after it; a `DeleteHistoryBranch` cannot see a branch whose tree row is
still in the window. Both leave rows behind rather than taking acked ones away — a
leak on the same collector's path — and both are left as they are because what a
deletion aimed at an undrained node *should* do depends on where a deployment put
its history, which is not this library's to decide.

**No error is swallowed on the layer's write paths** (measured, by sweep). Two
discarded errors exist, both on the age tick — its age drain and its
storage-pressure drain — which have no caller to answer and whose outcome is on
the state already.

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

**A queue reader does survive a rangeID renewal** (read, against upstream
v1.29.6). `renewRangeLocked` drains in-flight task requests, bumps the rangeID,
updates the task key manager and unloads nothing. This is a premise rather than a
hazard: it is what makes the two-token-spaces entry above reachable.

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
state blob and `last_write_version`, and reads the column only for a row with no
blob, which neither this store nor the upstream code it embeds writes; so whether a
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
entry under *Refuted* says by reading — swapping the applier's signal and request-cancel
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

**A fourth class: cross two same-typed things.** The other three all remove
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

Two things sharpen it. **Look for the repeated type**: four of the seven
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

**Run exhaustively it is a different instrument, and the second run says so.** The fifty-one were
chosen; `tools/mutation-sweep.py` enumerates the class instead, and **111** comparisons over the
whole layer — `fold`, `cycle` and its three sub-packages, `apply`, `wrapper`, `baserow`, `mutation`,
`wal`, `walmetrics` and the root — plus the two shipped implementations, `memwal` and `memcold`,
left **28** green, of which **seven** were boundaries nothing drove: the window's byte trigger and
its age trigger, the trim cadence's time half, a task range's inclusive minimum, the task page's own
range on both halves, the history page's strict ascent, and the length check in front of
`basePage[0]` — the last one a panic rather than a wrong answer. A chosen list cannot make that
claim, which is the argument for generating a class rather than writing one down.

**The dismissals were confirmed, and one of them was wrong.** Re-run with every
package in the judge, 8 of the 28 are caught: seven by the tests this branch added,
which is what a fix landing looks like, and one — the conflict-resolve refusal's
`parts > 1` — by `cycle`'s own recovery test, a package the narrow judge had
dropped. So that one was guarded all along and the *argument* for dismissing it was
the thing that was wrong, which is the failure mode a confirm run exists to catch:
a plausible reason is not evidence. `fold/histtasks.go`'s inclusive minimum is worth
a footnote the other way — the differential oracle catches it too, so the boundary
test this branch added names the bound rather than being the only thing holding it.

The **twenty** that are still green under the whole set are recorded here so the
next pass does not re-triage them, in the four shapes they came in. **A `len()` compared against zero
or against a magic prefix's length**, where the adjacent form is a tautology or
names a value no encoder produces — both page tokens, `bounded`'s second conjunct,
`mutation/encode.go`'s pre-allocation. **A switch arm the case above already
matched** — both merges' `c < 0` behind a `c == 0`. **A minimum-picking idiom**,
where assigning on equality assigns the same value — the attribution's cut seqno
and its workflow slices. **And a difference a later line absorbs**: `memwal`'s read
offset and trim length both end in an empty slice either way, the history merge's
reach filter is deduplicated by the merge itself, `Manager.ShardAcquired`'s epoch
comparison is preceded by the equal case returning, and the history page's `cut` is
capped by `min(cut, pageSize)` — that last one provable from the branch's own
arithmetic, since the branch implies an ask of one, hence a single base row, hence
a first key equal to the last.

**The second class, run the same way, and it does not stop where the first did.**
`guard` over the same set is a partial run in two segments — 151 of 331 through
`cycle`, its three sub-packages and most of `fold`, then 10 of the 195 that were
left, which reached the rest of `fold`'s task page — with **25** greens, and the two
halves of it read differently enough to be worth separating.

Over `cycle` it found nothing that was a hole, which is a result rather than an
absence: a state check a second check downstream repeats (`Cycle.startForRead`,
`Manager.ShardAcquired`'s epoch, `trim.Trimmer.Force`'s `CheckTrim`, which `start`'s
own `upTo <= doneUpTo` refuses anyway), a fast path whose own comment already
predicts the green (`fold`'s `want.empty()`), and three where the guard is
load-bearing and the caller happens to check too — `Cycle.halt`, `Cycle.refold` and
`tailstate.Tail.Resolve`. The last is the one to read: resolving an unstalled tail
assigns zero to `applied` **and** `resolved`, which is the watermark going backwards
and the whole log back under the tail, so I10 refuses every write on the shard. All
three now say at the guard what its absence costs.

**Over `fold` it found six, and the fold's refusals are where this class earns its
keep.** `ErrAfterTombstone` is raised at four sites and exactly one had a test; the
other three each hide a nil dereference, because a delete leaves the run's state
with no owner and a handler that folds onto it walks into that — a panic on the
shard's own goroutine, which takes the process rather than halting one shard. Two
more are `ErrRefused` sites, where the stream is legal and this accumulator cannot
express it: a conflict-resolve's current mutation landing on a run the window holds
as a snapshot or as an update that continued-as-new, and **a Set over a
continued-as-new pair, which is the one that loses data rather than panicking** —
dropping the pending request takes the new run's snapshot with it while the
window's entry still points at it, so the drain emits the Set and never the
creation. An acked start gone, with nothing that says so. The sixth is on the read
path: a delete-current over a workflow the cold store has no current row for, whose
guard would otherwise be compared against a row that is not there.

Two things to take from the split. **A refusal is a guard**, and a tree whose
decision paths are well driven can still have every one of its "this cannot be
expressed" arms unreached — they are the arms no valid stream produces, which is
exactly why nothing drives them and exactly why `Add` needs them, being where a
replay arrives with no `Check` in front of it. And **a panic ends the test binary**,
so the subtests after it never run: a sweep whose count looks one short may be
reporting one mutation's blast radius rather than a miscount. **The second segment's
one finding is the same shape at a seam rather than in the fold.** A foreign page
token is refused by the frame this layer puts on its own — and the test for it used
the base store's token, which fails the frame *and* the parse, so it said nothing
about which did the work. Removing the frame check left everything green. What the
frame buys over the parse is a token whose body happens to unmarshal into this
layer's own: four bytes of somebody else's followed by valid JSON is adopted as ours
at whatever cursor it decodes to, which restarts the pagination inside the window,
leaves the base's cursor behind, and hands the range the reader completes the acked
rows that were in it. That is the closed entry above about the two token spaces,
proved at last against the thing it is actually about.

**The third segment took the advice and is the whole of the cheap end**: the
shipped store, the shipped log, the wrapper, the root package and the emitter —
**94 mutations in six minutes**, against two minutes *each* in `fold`, because a
mutation high in the import graph rebuilds almost nothing. Sweep from the top down
when the budget is short; the number is that stark.

Twenty-seven candidates, and `--confirm` settles them: **six are caught** and
twenty-one are still green under the whole set. Of the six, three were already
guarded by the packages a narrow judge drops — two by panics inside the drain's
transaction (`applyCurrentRow`'s absent row, `applyHistory`'s tree row for a branch
that is not new) and one by the in-process server (`applyTasks` handed an empty
list). The other three are the tests this pass added: the store's own
`startTimeOf`, which is `fold`'s twin and had no test because every fixture fills
the field, and two **contract** claims that were missing from the conformance suite
rather than from a backend — a trim below the lower end a previous trim left, where
`upTo - base + 1` underflows and takes the whole log, and a read well past the end,
where indexing from the lower end is a slice bound. Both now sit in
`RunContractSuite`, so every backend meets them.

Of the twenty-one still green, eighteen are `if err != nil` on a call no fixture
can fail — the untested-error-path class this file already names as coverage rather
than a defect — and three are `len(x) == 0` or nil early returns the next line
no-ops through. `memwal`'s `CheckTrim` is the one worth naming: its absence is
covered by the `upTo < base` check two lines later, which is the guard that does
the work and is now driven.

**The fourth segment is measured rather than finished, and the measurement is the
useful part.** `apply`, `baserow`, `mutation`, `wal` and what was left of `fold` are
92 mutations, and they ran in **207 seconds** — against 85 seconds *each* under a
judge of ten packages. The difference is entirely in what gets linked: a mutation in
a package everything imports rebuilds that package either way, and the ten test
binaries above it are the cost. But the judge that bought the speed was narrowed to
`./mutation/ ./fold/`, and for these files that is **not a filter at all**: what
guards `apply`, `wal` and `baserow` is each one's *own* tests, which were the ones
dropped. It returned 40 greens and the first three confirmed all came back caught.

So the rule for a package low in the import graph is narrower than "narrow the
judge": narrow it to **that package's own tests and its direct consumers**, never to
two arbitrary ones. And know the price of getting it wrong — a `--confirm` over the
whole set costs about ten minutes *per candidate* down there, so forty candidates is
a session of its own rather than a step at the end of one.

**Re-run that way, `apply`, `baserow` and `wal` come back with nothing**: their 14
mutations, judged by their own tests plus `cycle`, `wrapper`, `memcold` and `fold`,
are all caught — where the same 14 under the two-package judge had returned greens.
So those three are swept and closed, with no candidates to confirm, and the pair of
runs is the cleanest statement of the rule there is: **the judge decides the
finding, so a green is about the judge until the judge contains whatever guards the
line.** What is left unswept is `mutation` alone, 72 mutations, and its consumers are
`fold`, `cycle`, `apply` and `memcold`.

On the evidence of every segment, the interesting question in what remains is the
same one: **which of its refusals no valid stream reaches.**

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
The mechanical half is two scripts and the split between them is the point.
`tools/mutation-run.py` applies mutations a session thought of, one manifest entry
at a time — with no make target and no checked-in manifest, because a committed
list reads as coverage and leaves the next session re-running the last one's.
`tools/mutation-sweep.py` *generates* a class instead and leaves nothing in it out,
which is the half a manifest cannot be: "every adjacent comparison under `fold/`"
means the same thing after the code moves, and a green from it is a line nothing
drives rather than a line nobody thought of.

**A narrowed judge is a candidate filter and a dismissal made on one is unsound.**
Dropping packages from the inner loop turns red into green and never the other way,
so a green found that way may be a green the dropped package would have caught —
which is fine for deciding what to look at and wrong for deciding what to ignore.
Shorten the acceptance stream by volume rather than by dropping its package, and
re-run the greens with everything in the judge before writing any of them down:
that is what `--confirm` is for. The first exhaustive run above did not, and going
back to do it cost one of its twenty-one dismissals: the argument was wrong and the
line was guarded by a package the narrow judge had dropped. Nothing it *acted* on
moved, every green there having been proved red against a test in the whole set —
so the price of a narrowed judge is paid in false dismissals rather than in false
fixes, which is the shape to expect. What a run found belongs
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

**What a green run means got wider, and every rung below rests on it.** Until the
`race` target existed, nothing in this repository had ever run the detector — so
every "a test holds this" above was a claim made by a run that could not see a
data race, in a layer that is a goroutine per shard, two mirrors published for
readers off it and a trim beside the loop. It comes back clean, which is the
reassuring half; the half worth keeping is that it was *unasked* for as long as
the closures were being written. A sweep that reports a mutant green should say
which targets it ran, and `make check` is now four of them — the tests, the
detector, the linters and the advisories.

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

**The floor is signed. Open went empty, took an entry back, emptied, and holds two again.**
One adversarial pass on a fresh context put an entry there — a current row written
without its start time — which is what the paragraph below says such a pass is for,
and the first time it had happened rather than been anticipated. It is closed now,
along with a second one the same reading turned up beside it, and what closing it
took is on the entry: a reverse of a decision three documents recorded as
deliberate, none of which said what the divergence bought. The floor itself is
unchanged: every entry that was closed, refuted or accepted still is. Three of the acceptances are
the harness this
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
