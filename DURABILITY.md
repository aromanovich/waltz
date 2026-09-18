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
* **unknown** — nobody has established which. Treat these as open.

Severity is what the caller sees: **silent** (gone, no error), **loud** (gone,
with a named error), **unavailable** (present, unreadable).

**The most important thing on this page:** the two implementations that ship —
`wal/memwal` and `cold/memcold` — both die with the process. They are what lets a
Temporal server boot over this library in `go test` with nothing installed, and
they are not a durability claim of any kind. Everything below that is closed is
closed *in the layer*; a deployment's durability is the durability of the log and
the store it supplies.

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
inferred, and an entry found there is charged to the tail and halts the shard.
`TestAReplayDoesNotTakeAShortPageForTheEndOfTheLog` (`cycle/replay_test.go`); the
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

**A trim past what the cold store holds.** The trim goes to `applied`, which only
a committed drain moves — never to what the window acked.

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

**Both shipped implementations die with the process.** `wal/memwal` and
`cold/memcold` hold everything in memory, so nothing here survives a restart.
This is deliberate — [ADR 0011](docs/adr/0011-each-seam-ships-one-implementation.md) —
and it means no suite in this repository judges storage that outlives a process.
What closes it is a deployment supplying a durable log and store, and running the
conformance suite and `waltest.CheckRetention` against them.

**A backend whose `Fence` never reaches storage.** `RunContractSuite` drives one
`wal.Log` value in one process, so a displaced owner is refused by the same
in-process object its successor has just fenced. A backend that records the epoch
in memory alone passes every fencing case here. Only a failover between two
processes shows it, and this repository has no second process to run. Severity:
silent, and the worst kind — two writers at one seqno.

**A log whose storage expires entries.** Guarantee 5 excuses a trim and nothing
else, so a retention window, a TTL on a table or a compaction that drops old
records each break it silently, and the suite runs in milliseconds.
`waltest.CheckRetention` is that obligation as a function a deployment runs
against its own storage, pointed at a deliberately shortened policy.

**Three of `fold.BasePage`'s four requirements are stated and not checked** — every
row inside the range, keys ascending within and across pages, an empty page
meaning exhausted. The fourth is refused because its breach is one the merge would
carry out itself; the other three break a queue rather than this package, which is
why they are written down instead. A store that pages differently is a deployment's
to catch.

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

**Nothing is staged between two layer nodes, or between clusters.** No partition,
no kill, no failover. [Chapter 15](docs/handbook/15-the-limits-of-the-evidence.md)
is the long form of what a green run does not claim.

---

## Unknown

**Whether any acked stream produces an unpaired `DeleteWorkflowExecution`.** The
fold collapses a deletion into a tombstone on the assumption that Temporal's
deletion flow always pairs it with `DeleteCurrentWorkflowExecution`; an unpaired
one would leave the current row holding pre-window content where the sequential
path updated it. A differential run against the sequential path is what would
judge it.

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
