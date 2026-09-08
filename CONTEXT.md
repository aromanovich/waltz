# waltz

Vocabulary for the WAL layer waltz is
([the handbook](docs/handbook/README.md) is the long form). Russian aliases are
the terms used in the design discussion this came out of; both forms are
canonical.

## Language

**Mutation (мутация)**:
One ExecutionStore-level write request the WAL carries. Eight shapes, one per
arm of the record's oneof: create, update, conflict-resolve, set, delete,
delete-current, and the two history-task calls. The unit of
atomicity: one mutation is one WAL entry.
_Avoid_: operation, write, update (ambiguous about granularity)

**Task record (запись таска)**:
The two mutations that are about a queue rather than about a workflow:
AddHistoryTasks and RangeCompleteHistoryTasks. They name no run, assert nothing,
and are the reason the word "mutation" no longer implies "mutable state". Both
travel through the log, and that is one decision rather than two: a road each
would put the delete's effect at a different moment than the writes it covers.
A delete that transits runs before the drain writes the rows it was meant to
cover and leaves them behind; a delete deferred alone covers a timer created
after the caller's checkpoint, the store's range delete being by fire-time
interval rather than by task id.
_Avoid_: task write (only names half of it)

**Deletion range (диапазон удаления)**:
`[InclusiveMin, ExclusiveMax)` of one task category, as the caller's own
checkpoint states it. The window resolves it the way it resolves every other
write-then-delete pair: a range folding in removes the tasks the window already
holds inside it, and the range itself is applied by the drain that carries it.
It does **not** outlive that drain — a task arriving after a range delete is one
the caller wrote after it, and the sequential path keeps it.
_Avoid_: ack level, bound (both describe the compensation this replaced)

**seqno**:
Position of an entry in a shard's WAL; a per-shard LSN assigned by the single
writer. Total order within a shard, no gaps (contractual).

**commitSeqno**:
Highest seqno the WAL has durably acknowledged. Cumulative: ack of n implies
durability of everything ≤ n. A mutation is confirmed to the caller iff its
seqno ≤ commitSeqno.

**appliedSeqno**:
Highest seqno whose effects have been folded into the cold store. Persisted
atomically with each apply batch; replay starts just above it.

**Tail (хвост)**:
The entries that are durable in the WAL and not yet settled — a count and a byte
total over a seqno range, not a container. The entries themselves are in the log;
what the owner holds in memory is the *window's* folded form of them (see
**Fold**), which is neither the same set — the window empties when a drain
starts, the tail only when that drain commits — nor entry-shaped. It is also
**not** `commitSeqno − appliedSeqno`: sync mode can settle an entry no drain will
ever carry, so the tail is measured from a third position, `resolved`. Everything
above commitSeqno is speculative and visible only to the shard's write path.

**Fold**:
Compaction of a window: merging a workflow's mutations into one summary update.
Mechanical rules only — no Temporal business logic. Snapshot-bearing mutations
reset a workflow's accumulator; deletions turn it into a tombstone.

**Window (окно)**:
The slice of the tail one apply batch folds — in the general case the whole
tail, but a partial drain takes a prefix of it. Fold's rule is stated over a
window: the assertions come from its head, the data from its tail.

**Collapse ratio (коэффициент схлопывания)**:
Mutations in a window divided by the dirty workflows in it — the project's
first metric, and a function of the window rather than a constant. A corpus
that never re-touches a workflow reports 1.0, which reads like a pass and
measures nothing.

**Overlay**:
The read interface of the fold accumulator: a read = base row from the cold
store + what the window holds for that workflow, gated at commitSeqno.

**Merge-on-read (слияние на чтении)**:
One page of a task read answered from the window and the cold store at once —
ascending, deduplicated, inside the requested range and no longer than the
caller's batch size, with the window's undrained deletion ranges subtracted from
the cold store's half of it. The dedup is a safety net rather than the
mechanism: the two sources are disjoint by construction, and what makes the page
honest is the order they are emitted in.
_Avoid_: overlay for tasks (the overlay renders one run's state; this
concatenates two sources and paginates)

**Apply**:
The cycle that writes folded summary updates into the cold store in one
transaction with the appliedSeqno bump and an epoch CAS.

**Drain**:
One pass of that cycle: fold a window, write it in a single transaction, move
appliedSeqno. A drain is all-or-nothing — a rejected one commits and writes
nothing, so appliedSeqno is the only witness to whether it happened.

**Base version**:
The `DBRecordVersion` of a workflow's row in the cold store as of the last
drain — what the folded request asserts, as distinct from the tail's version,
which is what it writes. Read after the epoch is acquired, never before.
_Avoid_: current version (ambiguous between the two)

**Base row (базовая строка)**:
The cold store's own copy of the two rows a delegated assertion stands on: one
run's row, and a workflow's current-execution row with its `last_write_version`
beside it. One value rather than two readers (`baserow`) because whatever
delegates needs both, and absence arrives folded in — a row that is not there is
a nil row and not an error, which is what the assertion is stated over.
_Avoid_: base reader, cold read (the first names half of it, the second names
every read this layer makes)

**Watermark**:
Unqualified, appliedSeqno — the position a drain moves. The apply cycle's
age/size **trigger** watermarks are a different thing and are always
named as triggers.

**Cut point**:
The highest seqno a partial drain may acknowledge: the entry before the first
one it did not apply. Applying past a cut point and acknowledging up to it are
the same bug.

**Replay**:
What a new owner does with the tail it inherits: read (appliedSeqno .. tail],
fold it into a fresh accumulator, drain. It is the third step of a shard
acquire, it runs before the owner serves anything — so "readiness" is that
placement rather than a gate — and it is triggered by a read as much as by a
write.
_Avoid_: recovery (the layer's other recovery is one drain whose outcome was
lost, and the rule they share is the interesting part: read the watermark
first, never re-derive from base versions)

**Condition authority (авторитет условия)**:
The rule that every assertion a mutation carries is verified **before** the ack —
the append is the ack and the ack is the answer, so a check after it has neither
an addressee nor an undo — and the set that rule is about: exactly the
assertions the fold discards. Recorded assertions travel with the drain's
transaction and stay claims about the pre-window row; discarded ones stand on
the window's own state. The two partition, so nothing is checked twice and a new
request shape gets its check for free. The predicate is read-only on the
accumulator, and an assertion the window does not determine is **refused**
rather than admitted.
_Avoid_: validation, precondition check (both suggest something the store would
repeat; this one is what answers instead of the store)

**Provisional entry**:
An entry whose condition had **not** been verified when it became durable,
because the drain carrying it is what answers its caller: every write of sync
mode. Its promise is "this will be applied, or its caller
will be told it was not" — so a condition failure on it at replay is a **drop**
rather than a divergence, where on any other entry it is a halt. Marked by the
writer at the append (`Payload.provisional`), since the two classes cannot be
told apart afterwards.
_Avoid_: unconfirmed, speculative (both describe an entry that is not acked,
which this one is)

**Trim**:
Lazy deletion of WAL entries at or below appliedSeqno, with no safety lag —
recovery reads the watermark rather than the log.
Keeps a log implementation's working set small; part of the latency budget, not
hygiene.

**Backpressure (граница хвоста)**:
The refusal a shard's write meets once its tail passes hard_max in either unit —
or, ahead of both and not a size at all, once its applier cannot read whether
its last drain committed. Raised **before** the append, so a refused mutation is provably not in the log;
returned unwrapped and in the shape the server's own persistence limiter uses,
because the shard's write path matches concrete types and anything it does not
recognise becomes a background re-acquire; never raised on a read and never on
the ShardStore path, since refusing a rangeID renewal would turn degradation
into a lost shard. Degradation, not loss.
_Avoid_: throttling, rate limit (both name a pace; this is a bound on memory)

**Epoch**:
The shard-ownership token carried by every WAL append and checked by apply.
Identical to Temporal's rangeID — one token, not two mechanisms. May grow
without an ownership change (rangeID renews on ID-range exhaustion).
_Avoid_: term, generation (same concept, different literature)

**Cycle (цикл)**:
The layer's state machine, one goroutine per (shard, epoch): it owns the
accumulator, decides when to drain, drives apply, answers reads and task pages
from the window, replays an inherited tail and runs trim beside itself. Its
states are decisions rather than defensive branches — halted-lost is fencing
working, halted-invariant is a divergence this process owns.
_Avoid_: worker, loop (both understate that the placement of a read on this
goroutine is what makes it correct)

**Wrapper (обёртка)**:
The seam into a running server: a decorator over the base `DataStoreFactory`
that takes eleven persistence methods into the layer, refuses a twelfth and
transits the rest. Named for what it does structurally — wrap, don't fork — and
it may name no cold store at all, so which store sits underneath is the
caller's business.
_Avoid_: adapter, proxy (both suggest translation; this one decides routing)

**Passthrough / intercept**:
What the wrapper does, and the first of the two axes the word "mode" names
here. In passthrough every method transits and the layer changes no
bytes — a claim only a whole-store comparison can check, since
passthrough behaves identically by construction and every suite is green over
it either way. In intercept the wrapper takes
eleven methods into the layer, refuses a twelfth and transits the rest. One
field is the whole of the difference: a flag beside it could disagree with it.
_Avoid_: on/off, enabled (they name a switch rather than what changes)

**Sync / windowed (sync- и оконный режим)**:
What window the cycle keeps, and the second axis. Sync mode drains inside every
intercepted write and answers the caller with what that drain said, so the
window is one mutation, the collapse ratio is 1.00 by construction, and a
condition failure belongs to the one caller in it. A windowed mode lets the tail
be non-empty at a call boundary — which is what makes the overlay, the condition
authority and merge-on-read necessary rather than optional. Both are
configurations of one cycle, not two write paths.
_Avoid_: synchronous/asynchronous (a windowed mode's ack is not asynchronous —
it is given at the append)

**Node (композиция)**:
What a running server composes the layer out of: the `wal` section of the
custom datastore's options — the mode — plus the nine policy
settings the server's dynamic config carries, the backends they run over and
the registry a tail is decoded with. A composition, not a cluster member —
the server is the node, this is what it builds. It is the root package,
`waltz`, and `waltz.Layer` is what a composition hands back.

**Checker (проверяльщик)**:
The record a driver writes of the calls it made and what it was told: two
fsynced lines per call, the first before the store is touched and the second
once it has answered, so the gap between them is the third outcome class — a
call nobody knows the result of. It judges nothing; the judge that reads such a
record back is not in this repository. Whatever writes one may not import the
layer, which is the point — an assertion compiled into the layer sees what the
layer *believes* and dies with it under `kill -9`.

**Witness (свидетель)**:
The assertion a run makes over the layer's **own** counters, beside the
assertions of whatever suite it ran. It exists because a layer that came out
empty is passthrough wearing another name, and somebody else's suite is green
over it — so a witness can fail a run every suite passed. Its central claims
invert between sync and windowed modes, which is why both are run. It is one
judged module, `internal/verify/witness`: a run states what it was supposed to
be (`Expect` — the window, and what its suites drove) and hands over what its
instruments saw (`Observed` — `cycle.Totals` required, the store's counts and
the metric emissions optional), so a run that has a capture handler and one
that does not make the same claims.
_Avoid_: smoke check, sanity assert (both name something weaker than the suite;
this is the stronger claim)

**Ownership generation (поколение владения)**:
The unit such a run's length is measured in: one node's life on one shard — a
kill, a successor, and the tail replayed between them. Wall clock is not a unit
here and drains are the layer's own decision, so a schedule stated in either
would be a function of the thing under test. The evidence a run produces is
linear in generations and in nothing else.
_Avoid_: round, iteration (neither names the kill that makes it evidence)

**Cold store (холодное хранилище)**:
Whatever the caller plugs in behind `cold.Store` — an applier and a watermarker,
embedded in one interface because one value has to answer both —
the permanent target of apply, regardless of which WAL backend is in use. No
package of the layer implements one. `cold/memcold` is the one shipped here —
Temporal's own SQL persistence over a database in this process, judged by
Temporal's own persistence suites — and `internal/verify/coldtest` is the double beside
it, for a suite that has to make a drain fail.
_Avoid_: main storage, base (overloaded)

**WAL backend**:
An implementation of the WAL contract (order, fencing, cumulative ack,
gap-freedom, read/trim). The only one shipped here is `wal/memwal`, the contract
in process memory, which is what the logic above the log is tested on and what
keeps the conformance suite (`wal/waltest`) a statement about the contract
rather than about one log. The contract exists so a log built on a quorum
replicator, a database or a journal service can be plugged in without touching
invariants.

**Entry**:
What one `Append` writes, and the unit of everything above it: one mutation, one
seqno, one payload. There is no batch at the log seam and no partial write for a
refusal to be about (ADR 0010) — an append that carried several entries would
have to report what it left behind when only some landed, which none of the
contract's three refusals can say and which a backend with no atomic multi-row
write cannot avoid producing.
_Avoid_: batch, record (the first names a thing this contract does not have, the
second is the mutation's own word)

## One name, one thing

The section above is the vocabulary of the **design** and it holds. What it
never governed is the vocabulary of the **identifiers**, and that is where the
words multiplied — `Policy`, `Registry`, `Held`, `Stats` have no
entry above and had grown three to six meanings apiece by the time anybody
counted.

Two rules, and they are narrower than "pick distinct names":

* **A name may mean two things in two packages.** `waltz.Config` and
  `cycle.Config` are not a defect: the package is the
  disambiguator, which is what package names are for. Do not rename across this
  line, and do not accept a rename that is only about it.
* **A name may not mean two things a reader meets together** — in one package,
  in one file, in one function's body, or on two types a call site holds at
  once. That is the line, because it is where the package name stops
  disambiguating and the reader has to.

Two shapes are worse than a repeated word and are the ones to look for:

* **one name, two return types.** `Tail.Stalled` answers with the stall,
  `Mirror.Stalled` answered with its seqno, and one file held three local names
  for the two. The mirror's is `StalledAt` — a position says so in its name.
* **one question, two answers that disagree.** `CurrentView.Held` and
  `workflowAcc.assertsCurrent` are both "does the window hold this row" and they
  part on `CurrentGuarded`, which is correct — they are a read question and a
  partition question. Neither name said which, and the second is
  `assertsCurrent` now for that reason. A pair like this passes every review,
  because each half is right.

Names that are taken, and by what:

| word | it is | it is not |
|---|---|---|
| **Drain** | one pass of the apply cycle (above) | stopping a layer or a node — that is `Shutdown`, which drains *and* closes |
| **Watermark** | appliedSeqno (above) | the age/size drain triggers, which are triggers |
| **Registry** | `cycle.Manager`, the shards this node holds | `waltz.Registry`, which is task categories |
| **Held** | a read: the window has something to say about this row | carrying a head assertion, which is `asserts*` |
| **Policy** | `cycle.Policy`, a source of `Config` read at the decision | `WAL.StaticConfig()`, which is a `Config` value |
| **Take** | `Window.Take`, which *empties* the window | building a read's view, which is `takeView` |
| **ranges** | undrained range deletes (`fold.Accumulator.ranges`) | task rows, which are `addedTasks` |

The list is not closed and is not a checklist to run. It is where a name goes
when it turns out to have been two, so the next person does not have to count
again. There is no test over any of this and there should not be
([no-lint-in-tests.md](.claude/rules/no-lint-in-tests.md)): a check on spelling
cannot see the two shapes above, which are the ones that cost something.
