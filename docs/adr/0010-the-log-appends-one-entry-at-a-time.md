# 10. The log appends one entry at a time

Date: 2026-09-03

## Status

Accepted. Narrows the `Append` of [ADR 0002](0002-wal-contract-is-backend-independent.md), whose
five guarantees are unchanged.

## Context

`wal.Log.Append` took `payloads [][]byte` and promised to write them "as one atomic unit", with
three refusals — `ErrFenced`, `ErrAlreadyWritten`, `ErrGap` — of which it said "none of the three
writes anything". A backend whose append is one transaction, one statement or one replicated
command keeps that promise for free.

Not every backend can. A log built over a storage service with no atomic multi-row write — an
append-only journal written as consecutive rows, say — writes a batch as several writes, and a
fence landing between two of them leaves a **prefix** in the log. The contract has no answer for
that state: each of its three refusals would claim the batch is whole one way or the other.

Worse than the missing error is the one that is there. A writer that died mid-batch, restarted and
replayed it is told `ErrAlreadyWritten` — which the contract documents as *the replay's success
signal* — and acknowledges seqnos nobody wrote. Such a backend cannot distinguish the two cases:
"the log holds part of your batch" and "you changed your batch" are the same log state. Making it
answer a torn-batch error there does not work either: it turns `waltest`'s
`DuplicateSeqnoIsAlreadyWritten` red, because the contract pins the opposite.

What kept this unreachable was not a mechanism. `cycle` appends one payload per call — one mutation
is one entry — so no batch could be torn. That rule lived in a rule file and two package doc
comments, three packages away from the code that could break it, and deleting all three broke no
test. Meanwhile group commit was on the design's list, and implementing it would have been the thing
that armed the hazard.

An intermediate design was built and rejected in review: a `MaxAtomicBatch() int` on the contract,
answered `UnboundedBatch` by backends with an atomic write and `1` by those without, with the
conformance suite asserting whichever claim a backend made. It works, and it is worse — it keeps a
parameter that some backends refuse, splits three conformance assertions into two versions, and
leaves every caller asking a question whose answer is 1 wherever the awkward backend is deployed.

## Decision

`Append` takes one payload and writes one entry:

```go
Append(ctx context.Context, shard ShardID, epoch Epoch, seqno Seqno, payload []byte) error
```

There is no batch, at this seam or below it. `wal.AppendState.Taken` is a `bool`, since there
is no count of overlapping seqnos to report; `CheckAppend` derives no last seqno; a nil payload is
refused where an empty batch used to be, and an empty payload is still an entry.

`ErrAlreadyWritten` says what it always meant to: the seqno is taken or it is not, and there is no
half of a write for the answer to be about. Its qualification — "requires the retry to be the same
batch", "a batch that overlaps the log only partly is refused whole and is a caller bug" — goes,
because the state it warned about cannot arise.

## Consequences

Group commit at the log seam is given up, deliberately. Nothing had it: the layer's only append is
`cycle`, one mutation at a time, and the batch parameter was capacity for a feature that was never
built. Reinstating it is a change to this contract, and the reason to refuse it is above: a backend
without an atomic multi-row write would have to reject what it cannot promise, or the contract would
have to grow a refusal that names a partial write. Amortising an fsync across concurrent writers
stays available where it belongs — inside a backend, which is where a database's own group commit
already does it.

**A backend author's obligation gets smaller, and that is the point.** The narrower `Append` is the
one seam where the contract could have demanded something a whole class of storage cannot give.
Whether a particular log has an atomic multi-row write is now a fact about how it is built rather
than a fact the contract has to accommodate.

Per-entry batch framing in a backend that carries one — a frame recording `first == last == seqno`,
and a recovery path that trims a partial batch that can no longer exist — is vestigial rather than
wrong: the frame still encodes the truth and the recovery stays correct. Removing it is a change to
that backend's entry layout, to be taken on its own.
