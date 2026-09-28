# 14. A record may carry the event batches its own request produced

Date: 2026-09-28

## Status

Accepted and unconditional: every intercepted write's event history goes through
the layer, and where it lands is the cold store's own property rather than a
setting. It supersedes decision D3 of
[ADR 0008](0008-the-log-carries-history-tasks-and-not-shard-or-event-writes.md)
for the batches a mutable-state request carries, and leaves D3 standing for
every other way history reaches the store.

## Context

D3 left event history out of the log and said the question would be revisited
after mutable state and tasks. It is that revisit, and what forced it is that
almost all of a deployment's event traffic is already inside the requests this
layer intercepts: `NewWorkflowNewEvents` and its four siblings. The wrapper was
pulling those out and writing them through the base store before the append, so
the layer was carrying them and then declining to.

D3's two objections were the right ones and neither has gone away. I10's budget
is denominated in bytes and event blobs are the bulk of them. And writing the
events first is what makes the ordering invariant free: a mutable state can
never point at history nodes nobody wrote, because the nodes are in the store
before the entry that names them is in the log.

What changed is that both have answers now that cost less than the property
they buy. The byte budget is a number, and a deployment that turns this on is
choosing to spend it on history — the bound still holds, the window just holds
fewer mutations. And the ordering becomes stronger rather than weaker when the
batches ride the record: the events and the state are one object with one ack,
so there is no order left to get wrong at the append. It moves to the drain,
where it is one statement of the cold-store contract.

## Decision

**A create, update or conflict-resolve record may carry its own event batches**,
and the codec decides nothing about it: `mutation.Encode` carries whatever the
mutation still holds. What makes that safe is one invariant established at the
writer — `wrapper.ExecutionStore.appendEvents` strips the batches off the
mutation once the base store has taken them, so **a mutation reaching the layer
carries exactly the batches nobody has written yet**. An empty slot encodes to an
absent field, so a record written in the default mode is the record this codec
wrote before the fields existed.

**There is no setting, and the choice is the cold store's.** An applier that
declares `cold.HistoryApplier` writes the batches in the drain's own
publication, so the record carries them; one that does not has them written
through the base store before the append, exactly as every store did before that
interface existed. The wrapper asks the layer which it is
(`wrapper.ShardWriter.WritesHistory`), and the layer answers off the applier it
was composed with — one value, read in one place, taken once at construction.

A key was the first shape of this and it was the wrong one. The bit then had two
homes, the yaml and the store, and one of the two disagreements writes the
batches nowhere: a store told they ride the record, over an applier that never
looks at the field. A capability cannot disagree with itself, and it also says
the true thing — whether the batches can ride the record is not an operator's
preference, it is whether the store underneath will write them.

**The order is pinned at the drain, and the mechanism is not.** `cold.Applier`'s
first obligation becomes *one drain is one publication*: the merged requests, the
task work and the watermark stay one transaction, and `fold.Batch.History` may be
written outside it by whatever means a store has. What may not move is that
every history row is durable **before** that transaction opens. Nodes are
immutable and keyed by `(tree, branch, node, transaction)`, so a repeated write
is the same row and a drain that failed after them leaves orphans nobody
references; the other order cannot be recovered from.

**A store that does not declare it is not refused, it is served the old way.**
`cold.HistoryApplier` is the marker, and an applier without it gets a layer that
writes the events through the base store before the append and hands it a batch
carrying none. So the failure the marker exists to prevent — an applier ignoring
`fold.Batch.History` and committing the mutable state anyway, acked data lost
with every suite green — is unreachable rather than refused: nothing puts
history in a batch a store did not say it would write.

**`ReadHistoryBranch` is merged on read, always.** Whether the window holds nodes
is a fact about the tail this shard inherited — a log written under one store is
replayed by a node composed with another — rather than about this node. The merge is
`fold.Accumulator.HistoryPage`, under the pagination rule the task page already
had.

**Standalone `AppendHistoryNodes` stays passthrough.** It has no mutation whose
condition, epoch and ack it could share, so giving it one is a record kind with
an ordering and an acknowledgement boundary of its own. That is a separate
decision and this one does not take it. Cross-cluster replication is its main
caller, so a deployment running replication keeps writing that history the old
way whatever this key says.

## Consequences

**One append makes a state transition and its events durable together**, and a
refused write leaves nothing behind — where the default path leaves the events
written and unreferenced. The foreground cold-store round trip per event batch
goes away, and the drain writes a window's worth of nodes at once.

**The byte budget counts event blobs wherever the store takes them in the
record.** I10 bounds a shard's tail in bytes and the bound is unchanged, so such
a deployment's window holds fewer mutations and drains sooner.
Nothing here re-derives the defaults; a deployment turning this on should expect
the collapse ratio to fall and should read
[14-where-the-defaults-came-from.md](../handbook/14-where-the-defaults-came-from.md)
before changing the numbers.

**Changing stores is safe and loud.** A build without these fields refuses an entry
that has them (`mutation.rejectUnknownFields` recurses into nested messages) and
halts the shard rather than replaying it short its events. A node composed with a store that
does not take them replays such a tail correctly anyway, because the fold takes
whatever the record held.

**Three history methods still transit past a window that may hold their rows**,
and this is a named exposure rather than an oversight — the decision is that this
library does not choose for a deployment here, because what a deletion aimed at
an undrained node should do depends on where that deployment put its history.

* `TrimHistoryBranch` reads the branch through the new merge, so it *can* see a
  window node, and the `DeleteHistoryNodes` it then issues goes to the cold
  store, where that row is not yet. The delete is a no-op and the drain writes
  the node afterwards. What survives is a node off the transaction chain, which
  the reader's own chain walk skips — a leak rather than a corruption, and one
  the next trim of that branch removes.
* `DeleteHistoryBranch` resolves its ranges through `GetHistoryTreeContainingBranch`,
  which transits: a branch whose tree row is still in the window is invisible to
  it, so the branch is not deleted and its rows stay.
* Both are also reachable from the worker service's history scavenger, which
  holds no shard and so runs passthrough: it reads and deletes through a cold
  store no window is merged into, whatever this key says on the history nodes.

A deployment that wants these closed can drain the window before such a call, or
refuse them outright. Neither is done here.

**The merge makes explicit a store obligation that was already there.** A base
page the cut emits nothing from is left unread and reached again by its own
token, so the store must answer a token it has answered before. That follows
from the cut rule rather than from this merge, and `fold.BasePage` has it too
without saying so. Temporal's SQL plugins satisfy it outright — their history
token is the last row's key — and the case that cannot serve the seam is a
server-side cursor consumed by reading.
