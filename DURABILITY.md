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
the store it supplies. That is the first of the three **accepted** entries below,
which are this page's floor: what cannot be established from inside this
repository, signed rather than left open.

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

**A zombie writer appending after a fence.** `Fence` atomically cuts off every
lower epoch, and the drain asserts the epoch again as a compare-and-set before it
writes a row. `FenceCutsOffLowerEpochs` and `TwoWritersContendForOneShard` in the
conformance suite; `assertEpoch` in `cold/memcold/apply.go`.

### The drain

**A batch that lands half-applied.** One drain is one transaction, and a batch
that landed in pieces would leave rows no replay can reconstruct — the mutations
behind it were acked, folded and collapsed. `cold.Applier`'s first obligation;
`cold/memcold/apply.go` opens one transaction and commits once.

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

**A drain that folds to nothing settling nothing.** A window can ack entries and
produce an empty batch, and leaving those entries unsettled strands them.
`TestADrainOfAnEmptyWindowSettlesNothing` (`cycle/tail_test.go`).

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

**A refusal the caller cannot act on.** Not a loss of data, and in this file
because the effect on a caller is the same: a current-row conflict carrying no run
id is one the history service declines to resolve, so a retried start that
collides with a run the layer already acked is answered with an opaque failure
instead of that run. The run comes off the response's own field and the request
ids out of the row's serialised state.
`TestTheDelegatedCurrentRowConflictNamesTheRunItCollidedWith`
(`fold/check_test.go`) and `TestTheCurrentRowReadCarriesTheRunAndItsRequestIDs`
(`cold/memcold/current_test.go`).

**Backpressure reported as a possibly-committed write.** A refusal checked before
the append provably wrote nothing, and one `%w` around it turns that into a
self-inflicted failover. `TestTheBackpressureRefusalIsDefinitelyNotCommitted`
(`internal/verify/guard/`).

---

## Open

**A log whose storage expires entries.** Guarantee 5 excuses a trim and nothing
else, so a retention window, a TTL on a table or a compaction that drops old
records each break it silently, and the suite runs in milliseconds.
`waltest.CheckRetention` is that obligation as a function a deployment runs
against its own storage, pointed at a deliberately shortened policy.

**The store's obligation to carry request ids is stated and not checked.** The
conflict a refused write carries is built from what `baserow.Current` answered, so
a store whose execution state omits the request ids gives a retried start nothing
to deduplicate against. Stated on `baserow.Rows.Current`; no test can hold a store
that is not in this repository.

**Event history stays outside the log.** An intercepted write puts its own new
events down through the base store before the mutation is acked, so a crash
between the two leaves events no mutable state points at — garbage rather than
loss, and [ADR 0008](docs/adr/0008-the-log-carries-history-tasks-and-not-shard-or-event-writes.md)
holds the boundary.

**A windowed write whose drain loses the shard is told it definitely did not
commit.** In a windowed mode the entry is appended and acked into the log before
the watermark trips the drain, so when that drain's `Apply` answers
`*p.ShardOwnershipLostError` the error travels out to the writer whose mutation
tripped it — and upstream reads that class as *guaranteed to have failed*,
dropping the request's task keys from its tracker and skipping its
notifications. The entry is in the log, above the watermark, and the successor
applies it. No acked entry is lost, and the caller's own claim is still false in
the direction that matters: it is told nothing happened about a mutation that
will. The blast radius is bounded because that class also unloads the shard,
which discards the tracker and the cache it would have misled. Closing it means
answering the caller something in upstream's possibly-succeeded set while still
telling the server to re-acquire, which is a change to the failover signal and
not a local fix.

---

## Accepted

**Nothing in this section is held by a mechanism — that is what accepted means —
so every entry here stands at rung 5, and the bottom row of the rung table is the
whole of what carries it.** The three are one shortage seen from three sides:
nothing here has storage outside one process's memory, so there is nothing to
restart, nothing two writers can share, and nothing a killed run could be judged
from afterwards. Accepting them is what gives this list a floor, and a later pass
that derives one again is finding the signature rather than a gap.

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

---

## Unknown

**Whether any acked stream produces an unpaired `DeleteWorkflowExecution`.** The
fold collapses a deletion into a tombstone on the assumption that Temporal's
deletion flow always pairs it with `DeleteCurrentWorkflowExecution`; an unpaired
one would leave the current row holding pre-window content where the sequential
path updated it. A differential run against the sequential path is what would
judge it.

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

**The trim never passes what the cold store holds** (read). `Trimmer.Drained` is
reached only on the settle-forward path, after `Tail.Settle(..., MoveWatermark)`,
so the watermark it is handed is the seqno the committing transaction wrote. A
halted cycle does not trim at all: the log is the next owner's evidence.

**The write path cannot ack into a cycle that has not replayed** (read).
`Cycle.add` calls `Cycle.start` before it reads the policy, takes a seqno or
appends anything. `Cycle.Close` was the one door that skipped it, and that is the
shutdown entry above.

**No intercepted write acks before its events are down** (read). All eight go
through one `ExecutionStore.write`, which calls `appendEvents` before
`layer.Write`; a kind with no interception row is refused rather than transited.
There is no second door to keep in step.

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

---

## What this file is not

It is not a proof that the list is complete. An entry absent from it is one
nobody has written down, not one that cannot happen — which is why **unknown is
treated as open**. When a new way is found, it belongs here whether or not it is
closed the same day, and a fix that closes one belongs beside it with the test
that holds it.

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

**The floor is signed.** The three entries that cannot be closed from inside this
repository are in Accepted rather than Open: both shipped implementations die with
the process, a backend whose `Fence` never reaches storage passes every fencing
case in the suite, and no failover between two processes is staged. They were one
harness and a decision — a durable log, two processes, a kill, and a judge outside
all three — and the decision taken is that this library does not build it. So
they are finished entries, and what is left in Open is what this repository can
still act on.

The one change that would reopen all three at once is durable storage shipping
here, which [ADR 0011](docs/adr/0011-each-seam-ships-one-implementation.md)
refuses. Nothing short of that moves them, so a pass that rediscovers any of them
should add nothing but a pointer to this section.
