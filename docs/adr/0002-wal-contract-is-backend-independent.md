# 2. The WAL is a backend-independent contract; gap-freedom is part of it

Date: 2026-07-26

## Status

Accepted. This is the decision the library is built on: waltz implements no log.

## Context

The point of putting a write-ahead log in front of a history shard's cold store is
that the log answers sooner than the store would. Whether it does is a property of
the log, not of this layer — and the first log anybody reaches for is the one
already in the deployment, which is to say the cold store's own database. That log
appends at exactly the latency the store already writes at, so it buys nothing but
the folding, and the layer's reason to exist is only fully paid for by a
purpose-built quorum log arriving later.

Which means "later swappable" is a requirement now, not an aspiration. And a
requirement met by an interface alone is not met: the correctness invariants above
the log — commit rule, recovery point, fencing, replay — have to rest on properties
stated in that interface, or they rest on whatever the first implementation happened
to do.

## Decision

The correctness invariants rest on an explicit backend-independent contract,
expressed as a Go interface (`wal.Log`), not on properties of any implementation:

1. Total order per shard; the single writer (guaranteed by epoch fencing) assigns
   seqno itself.
2. `fence(shardID, epoch)` atomically cuts off appends of all lower epochs.
3. Cumulative ack: `ack(n)` implies entries ≤ n are all quorum-durable.
4. **Gap-freedom: if entry n exists in the log, all entries < n exist.**
5. Readback: `readFrom(seqno)` returns every entry a completed append acked and
   no trim has removed, in seqno order; `trim(upTo)` moves the log's lower end
   and nothing else, so what it leaves stays readable from that new lower end,
   and the shard's ownership and its next seqno survive it.

The contract ships with its conformance suite, `wal/waltest`, which is the whole of
what this library has to say about any log: that it satisfies the five. The only
implementation here is `wal/memwal`, the contract in process memory — a backend and
not a double, so the suite it passes is the suite an author of a second one runs.

## Consequences

Gap-freedom is the deliberate trade-off: it bans backend designs with concurrent
blind appends. We give that class up knowingly — the single writer per shard
serializes seqno assignment anyway, so concurrent appends could only save the
occasional pipelining retry — and in exchange replay and the commitSeqno rule stay
trivial forever: no hole-tracking, no "wait or declare lost" logic in recovery.

Pipelining is best-effort *inside* a backend: batch n may be sent before batch
n−1 is acked, and if the backend executes them out of order the append of n must
fail cleanly and be retried. The contract's guarantees hold; the cost is a rare
retry.

Everything above the log is answerable with no cluster, and that is a consequence
rather than a convenience: the invariants are stated over the five guarantees, so a
log in memory is a complete instrument for judging them. What it cannot judge is
whether a real log keeps its promise under a real failure, which is the backend
author's suite to run and this one's to hand them.

A backend that provides all five naturally — a Raft-style replicated log, a
safekeeper-like quorum log — is swappable without touching invariants I2–I5 or the
replay code. A backend that provides them awkwardly is what [ADR
0010](0010-the-log-appends-one-entry-at-a-time.md) is about: the contract narrowed
rather than grew an exception.
