# 13. What a write must read from the cold store before it appends

Date: 2026-09-28

## Status

**Proposed, and the first entry here that is not a decision.** Every other ADR in this
directory records a call somebody will otherwise try to reverse; this one records a question
that was asked carefully, answered in part, and left open on purpose — the part that is
settled is what the pre-append reads *are for*, and the part that is not is what to do about
their cost. None of the options under *Consequences* is built: the reads are still the ones
*Context* describes. It is here rather than in the handbook because the handbook says what is
true of the shipped layer, and this is about what the layer might become.

The durability half is not restated here. It lives in `DURABILITY.md` beside the accepted
entry *A backend whose `Fence` never reaches storage*, because that is the entry it changes.

## Context

An intercepted write can reach the cold store twice before its entry is durable, and the two
visits are unrelated to each other.

**The first is the events, and only over some stores.** Over a cold store that does not
declare `cold.HistoryApplier`, `ExecutionStore.write` calls `appendEvents` before
`layer.Write`, and that walks every slot's every batch through `base.AppendHistoryNodes`
serially. The ordering is deliberate and is the reason the tree is never behind the tail:
on that path the record carries no events, so a mutation acked before them would point at
history rows nobody wrote. Over a store that declares it — `cold/memcold` does — the batches
ride the record and the drain writes them (ADR 0014), and this visit does not happen.

**The second is the conditions.** `Cycle.check` runs the condition authority before the
append, and in a windowed mode whatever the window cannot determine is delegated:
`checkDelegated` reads the pre-window rows the assertions stand on. In sync mode the drain
inside the call asserts them and the reads are skipped. The reason the check happens *before*
the append is the whole of it — the ack is the answer, so a condition this layer means to
answer has to be evaluated while the caller is still on the line. After the ack a refusal has
no addressee and no undo.

Three facts about that second cost, each checked rather than assumed:

* **it is per workflow per window, not per write.** `Accumulator.decide` consults
  `peek(namespaceID, workflowID)` first; a workflow the window already holds is answered from
  memory. So the fold's collapse already amortises it — `Delegated.Any()` is documented as the
  test for "this mutation costs a cold-store read";
* **it is up to four round trips.** `Delegated.Settle` walks the current row, then each run
  row, one store call each, in order (three for an update that continues as new, four for a
  conflict-resolve carrying a current mutation and a new run) — the order being load-bearing,
  since the store reports the first failing assertion and upstream's suites assert on the
  error's type;
* **it is far wider than the question.** `RunAssertion.VerifyRow` needs the row's existence
  and `db_record_version` and nothing else — its own comment says so — while `Rows.Run` reads
  the whole `executions` row, blobs included. The current-row read needs run id, state and
  `last_write_version`; the payload the conflict error carries is needed only when the
  assertion *fails*, and the failing path already reads rows back (`apply/failure.go`).

One mutation is not amortisable by construction: a brand-new create. Its current-row assertion
is about **absence**, and no window that has never held the workflow can answer it. Upstream's
own start path does not read at all — it takes the workflow lock, writes optimistically, and
rebuilds everything it needs from the conflict error's payload — so there is no value for a
caller to hand down in place of the read either. Here the choice is binary: read, or ack a
start before knowing whether the workflow exists.

## Decision

None yet. What is decided is only the frame the options are judged in, and it has two parts.

**The reads are a detector, not a barrier.** Nothing is locked between a delegated read and
the append, so the sample can be stale by the time the entry lands. The real barrier is the
epoch assertion inside the drain's transaction, and it runs after the ack. What the reads
therefore buy is not correctness against a simultaneous writer — they do not have it — but
that a *persistent* violation is caught at a repeatable request instead of at a halted shard
holding acked entries no replay will apply.

**Narrowing is free; removing is not.** An option that asks the same question at the same
moment and merely costs less is an engineering change. An option that stops asking it before
the ack is a change to what this layer detects, and belongs in `DURABILITY.md` before it
belongs in code.

## Consequences

The options, sorted by that line.

**Narrow the read (free side).** One batched call answering every delegated assertion of one
mutation, returning only the scalars the predicates read: up to four round trips become one, and a
multi-kilobyte row read becomes an index-only lookup. No invariant moves — same question, same
moment, same answer — and the conflict payload stays on the failing path where it already is.
It would be the first *optional* extension of `baserow.Store`, and optional is the point: the
versioned current-row read is required at construction because without it the layer could
confirm `CurrentEqualsWithVersion` and never refuse it, which is a correctness hole. A narrow
read's absence costs latency only.

**Overlap the events with the reads (free side).** Over a store that does not declare
`cold.HistoryApplier` the two cold-store visits are independent, so the events could travel
while the conditions are being checked. The join has to be before the append, not merely
before the drain, or the ack would outrun the events — so `Write` grows a "work that must
finish before the append" seam and a new invariant to guard. Nothing about orphaned history
rows changes: a refused condition already leaves the events written, since they go first on
that path today.

**Remember the versions this layer wrote (costly side).** The accumulator computes exactly
what the run-row reads ask for and discards it at every drain, which is why the read is per
window rather than per epoch. Keeping it across drains — from **committed** drains only, an
unknown outcome establishing nothing, and cleared on any epoch change — makes every update to
a workflow this owner has already written free forever. It does not help creates. It shortens
the detection window rather than closing it, and it is the cheapest of the costly options.

**Trust the request's own `DBRecordVersion - 1` (costly side).** Zero reads on the update
path. The value is what the caller loaded, and nothing could have moved the row since under
the assumption the design already makes. That assumption is the only thing every alternative
rests on, and it is one statement: *the writers of these rows are exactly this layer.* The
fence covers writers that come **through** the layer; it says nothing about a passthrough node
in a mixed fleet, a migration, an operator's SQL, or a second cluster pointed at the same
database.

**The one change that would settle it rather than trade it:** a cold store that refuses any
row write not carrying the current epoch. Today the epoch is asserted by our own drain — the
layer checking itself — which tells a writer that carries no epoch nothing at all. Push that
into the store and the assumption becomes enforced rather than assumed; the costly options
stop being costly, and the mixed-fleet and migration cases fail loudly instead of silently.
Neither seam asks for this today.

**What is missing before any of this is worth doing** is two numbers, and this repository is
the wrong place to take them (`docs/handbook/15-the-limits-of-the-evidence.md`): the share of
an intercepted write's latency that is the delegated reads rather than the drain's
transaction, and how often a write's workflow is already in the window on real traffic — which
is what decides whether the reads are a per-write cost at all or a rounding error the fold
already absorbed.
