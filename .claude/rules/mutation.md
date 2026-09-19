---
paths:
  - "mutation/**"
---

# This repo: the WAL's record format

`mutation/` turns one ExecutionStore write request into the bytes a
`wal.Entry` carries, and back (#11). `mutation.proto` is the record's
specification and `mutation.pb.go` is generated from it: the message an entry
carries is `Payload`, while `Mutation` — the hand-written struct in
`mutation.go` — is the in-memory request the codec maps onto it. The proto's own
file comment says why the record is a hand-filled mirror rather than a
reflective codec. What to know before changing any of it:

* **changing the record changes the meaning of every entry already written.**
  `Payload.format` is the only version there is and there is no migration path:
  `Decode` refuses every value but the one this build writes
  (`TestFormatVersionIsChecked`), so a bump refuses an inherited tail rather
  than upgrading it. A tail written by the previous binary is replayed by this
  one (#98), so a field whose meaning moved is a tail that decodes into
  something the writer did not mean. Adding a field at the end is safe,
  renumbering or repurposing one is not. **What holds that is a recorded
  entry**, not the round trip: the round trip drives both halves of one build,
  so a slot swapped on both sides of the mirror passes it — measured, on the
  scalars and on the collections. `record_format_test.go` decodes bytes an
  earlier build wrote and names every slot two same-typed fields could have
  swapped; its constant is not to be re-recorded from a changed encoder, and
  the second arm beside it is what says which side moved;

* **the mirror is filled field by field, and the cost is paid by a guard rather
  than by attention.** A field Temporal adds is a field this package silently
  omits, so the field-set test walks the real request structs and fails on a
  field the mirror has no home for. *Which* structs it walks is decided by
  `kinds.go` rather than by the list: every kind's payload type must have a row,
  so a kind added without one fails by name instead of being walked by nobody,
  which is what the two history-task requests were between #142 and #212. A
  temporal bump that adds a field is expected to fail it — that failure *is* the
  mechanism, not a broken test;

* **a kind is declared once, and the spokes it can be forgotten in fail by
  name.** `kinds.go` holds one row per kind — its name, the `Mutation` field it
  travels in, how to see that field is set, where its shard id is, where its
  rangeID is, and which slices of new events its request carries — and
  `Kind.String`, `RangeID` and `EventSlots` are driven from it. The field is a
  *selector*
  (`func(m *Mutation) any { return &m.Create }`) rather than a name, so a field
  that is renamed breaks its row at compile time; the guard recovers the name
  back out of the pointer, by address, for its messages only. `Kind` and
  `ShardID` stay hand-written fan-outs on purpose (the first is the most-called
  function in the layer), so the table's job is to *hold them to it*:
  `kinds_test.go` walks `reflect` over `Mutation` and fails on a field with no
  row — the one direction Go cannot state, there being no sum type — an
  accessor reading its neighbour's, and a `Kind` or `ShardID` case that
  disagrees with its row. #142 added two kinds and had to find every spoke by
  hand. What the table deliberately does not cover is behaviour — the per-kind
  switches in fold, check and apply do genuinely different things and keep their
  own guards — and the invariant all of them share is now
  `ErrNotExactlyOneRequest` rather than six typed copies of one sentence;

* **blobs are authoritative and the parsed protos are derived.** Where a request
  holds both (`ExecutionInfo`/`ExecutionInfoBlob`, `ExecutionState`/
  `ExecutionStateBlob`) only the blob is carried, and `Decode` derives the proto
  back from it. Upstream builds the two together from one value
  (`execution_manager.go`), so this is faithful — but it is also why a *fixture*
  that sets the struct and not the blob survives a fold and vanishes on replay;

* **three things are dropped on purpose**, each with an invariant behind it:
  `RangeID` on every request that has one, because it is the epoch (I11), it
  travels with the entry, and a copy inside the payload could disagree with it;
  the `*NewEvents` slices, because event history stays out of the WAL in v1 (D3)
  and is written by `AppendHistoryNodes` before the append; and
  `InternalChasmNode.CassandraBlob`, which is set only under Cassandra —
  dropping it silently would lose state, so `Encode` **refuses** a mutation that
  carries one rather than encoding without it;

* **`Encode` is a function of its argument, and that is load-bearing.** Go map
  iteration is randomized, and one mutation encoded 200 times through an
  unsorted encoder produced four distinct byte strings — so every collection
  travels as repeated entries in sorted key order rather than as a proto map.
  Any comparison of two runs of one stream rests on it (a window may be drained
  twice after an ambiguous failure), and so would any future dedup of entries. The
  absent-vs-empty rule is the other half: an empty set encodes as an *absent*
  field, and an absent field decodes to a nil set — one generic per direction
  (`sortedKeys`, `setOf`) so the rule is a single edit and not one per key type;

* **the registry is a parameter and not a package default** (`Decode`,
  `DecodeEntry`). It is the one input that is not a function of the bytes: the
  same payload decodes on one node and fails on another, because the archival
  task category exists only where archival is configured. Replay inherits that
  constraint, which is why `cycle.Deps.Registry` is required and must be the
  server's own;

* **`DecodeEntry` carries one thing `Decode` does not**: whether the entry's ack
  was provisional (#93). Sync mode acks before the condition is verified, so the
  flag rides the payload and replay reads it back to know that a condition
  failure on that entry is a drop rather than a halt. It is the only field about
  the *entry* rather than about the request.
