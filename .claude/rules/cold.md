---
paths:
  - "cold/**"
---

# This repo: the cold store seam and the store that ships at it

`cold/` is the contract a drain lands on — `Applier`, `Watermarker`, and the four
things an implementation owes — and `cold/memcold/` is the one implementation
here. The four obligations are on the package doc and the handbook's
[04-contracts.md](../../docs/handbook/04-contracts.md) is their long form.

What to know before changing any of it:

* **the store is Temporal's, embedded, and that is the decision**
  ([ADR 0012](../../docs/adr/0012-the-built-in-cold-store-embeds-temporals-own-persistence.md)).
  `Store` embeds the `p.ExecutionStore` that `sql.NewFactory` vends and shadows
  none of its 28 methods. So: **do not hand-write a method the embedding
  already answers**, and do not "fix" an inherited one — a divergence from
  upstream is a store that Temporal's suites judge and this repository's
  opinion overrules. What may be added beside them is what Temporal has no
  method for, which today is three things: the folded window's transaction, the
  watermark, and the current row's `last_write_version`;
* **what judges it is Temporal's four exported suites**
  (`conformance_test.go`), and a suite of ours at this seam would be this
  repository's opinion of what a store owes. `Apply` is the exception, because
  no upstream suite knows about it: it is judged by `apply_test.go` and by
  `internal/verify/acceptance`'s both-seams-real run, and every case there was proved by
  staging the defect it exists for;
* **the transaction is opened beside the interface, not inside it.**
  `p.ExecutionStore` has nowhere to declare a write spanning many workflows, so
  `Store` keeps the `sqlplugin.DB` and calls `BeginTx` on it. That is the whole
  reason the store holds a handle at all, and the transferable half of the
  design: an implementer whose driver offers nothing below the per-workflow
  interface cannot satisfy the contract by trying harder inside it;
* **the seven collections are named in two literals, and both are held to the
  type.** `applyMutation` names them off a delta and `applySnapshotCollections`
  off whole state, and a collection missing from either is rows the drain
  *acknowledged* and never wrote — with the watermark committed beside them, so
  the log is trimmed past them. It is the only failure on this path with nothing
  behind it: a drain that refuses, fails or dies leaves its entries in the log
  for the next owner, and this one does not. Nothing above caught it — the four
  conformance suites drive upstream's own 28 methods, none of which goes through
  `Apply` — and dropping the two CHASM lines from `applyMutation` left the whole
  of `go test ./...` green. `TestEveryCollectionOfARunReachesTheDatabase` drives
  every `Upsert*` of a delta and its counterpart on a snapshot through a real
  drain and reads the run back through the store's own read; it is red for all
  fourteen lines. It holds the run against the **entry** the drain carried and
  not against a count, because a count also passes a drain that wrote two
  collections into each other's tables. Its fixture is a hand list of values,
  because what a store will take is not derivable from a type — but there is no
  second list for the snapshot arm or for the read-back, both of which reach the
  same collection by taking the `Upsert` prefix off. It is held to the type in
  both directions: an `Upsert*` with no entry, and an entry for a collection a
  delta no longer has, each fail by name;

* **two orderings in `Apply` are the contract and not transcription** — the
  epoch CAS first, so a lost shard is reported as one rather than as the version
  failure underneath it; and the task range deletes before any task row the
  drain writes, because fold deliberately keeps a task that arrived after a
  range and a delete running later would take it away. SQL executes in issue
  order; an engine that reorders by table has to reproduce both some other way;
* **the attribution readback runs after the rollback, never before.** The
  database is served by one connection, so a read taken while the drain's
  transaction still holds it waits for a transaction waiting for the read. The
  same rule is why `Watermark` opens its own transaction and may not be called
  from inside one;
* **isolation between two stores is the DSN name and nothing else.** The plugin
  keys its connection pool by DSN, and the func `New` returns does not close the
  database — upstream's `connPool.Close` only decrements a refcount. So a test
  that builds isolation on teardown looks right and is false;
* **`txConn` reaches an unexported field by reflection**, because
  `sqlplugin.Tx` names Temporal's own tables and nothing else, and the watermark
  is not one of them. `New` creates that table through the same path, which is
  what turns an upstream rename into a store that refuses to be built rather
  than a drain that silently loses its watermark;
* **`cold` may not name a store and `memcold` least of all**, and `memcold` may
  not import the layer above the seam — `cycle`, the wrapper, `walmetrics` and
  the root package — while the vocabulary the seam is stated in, `fold`, `wal`,
  `apply`, `baserow` and `mutation`, it must name
  ([dependencies.md](dependencies.md)). A store that could see the layer would be
  judged by the thing sitting on top of it.
