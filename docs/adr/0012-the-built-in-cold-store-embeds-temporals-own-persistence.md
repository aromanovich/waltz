# 12. The built-in cold store embeds Temporal's own persistence

Date: 2026-09-07

## Status

Accepted. Says how [ADR 0011](0011-each-seam-ships-one-implementation.md)'s cold-store
implementation is built, and why the obvious way was refused.

## Context

A store at the cold seam has to be two things at once. It has to be a `persistence.ExecutionStore` —
28 methods — and a `persistence.ShardStore`, because that is what a Temporal server calls; and it
has to answer `cold.Applier`, which is one method that writes a folded window as one transaction and
which no upstream interface has a name for.

The first version was written by hand: an in-memory store over maps, in seven files, about 1250
lines. It worked in the sense that the layer's suites were green over it. The objection is not the
line count. **A store this repository writes is judged by this repository's opinion of what a store
owes** — its own absence rules, its own error classes, its own idea of what a conflict-resolve does
to a current row. A history shard's store is the hardest thing in this tree to get right and the
easiest to get *plausibly* wrong, and a plausibly wrong one leaves every suite above it green.

Four facts about upstream made another shape available, and they had to be checked before the design
could be proposed rather than after:

* Temporal's SQLite plugin is `modernc.org/sqlite` — **pure Go**. No cgo, no container, no port. A
  `config.SQL` with `mode=memory` is a database that is created on its first connection and dies
  with the process.
* `sql.NewFactory(cfg, resolver, clusterName, logger, handler)` is exported, returns a
  `*sql.Factory` that implements `persistence.DataStoreFactory`, and vends a real execution store
  and a real shard store.
* `sql.Factory.GetDB()` is exported and hands back the `sqlplugin.DB` underneath.
* **`sqlplugin.DB` has `BeginTx`, and `sqlplugin.Tx` embeds the same `TableCRUD` the DB does.** So a
  transaction spanning many workflows is fully constructible from *outside* the `sql` package,
  against upstream's own statements.

The last of those is the fact the whole design rests on. Without it there is no way to write a
folded window as one transaction over upstream's code, and the choice would have been a hand-written
store or no store at all.

## Decision

`cold/memcold.Store` **embeds** the `persistence.ExecutionStore` that `sql.NewFactory` vends over a
SQLite database in this process, and shadows **none** of its 28 methods. The schema those methods
were written against, the row layouts, the serialisation and the error classes are upstream's and
stay upstream's across a version bump. `Store.ShardStore()` is the shard half of the same database,
undecorated.

Beside that surface — never inside it — the store adds exactly what Temporal has no method for:

* **`Apply`**, the folded window's single transaction, opened on the `sqlplugin.DB` handle the store
  keeps beside the embedded interface. That handle is why the store holds a database at all.
* **`Watermark` and `SetWatermark`**, over `waltz_watermarks`, a table of waltz's own created beside
  the plugin's schema rather than borrowed from a column the server also writes. `SetWatermark`
  takes the transaction rather than opening one, which is the whole of the guarantee that the
  watermark commits with the rows it vouches for.
* **`GetCurrentExecutionWithLastWriteVersion`**, the read `baserow.Store` names: upstream's
  `GetCurrentExecution` selects `last_write_version` and then discards it for want of a field to
  hold it in, and a layer that cannot see that column can confirm a create's assertion but never
  refuse it.

**What judges the inherited surface is Temporal's own four exported suites** —
`NewShardSuite`, `NewExecutionMutableStateSuite`, `NewExecutionMutableStateTaskSuite` and
`NewHistoryEventsSuite` from `go.temporal.io/server/common/persistence/tests` — run unmodified, one
store per suite. A suite written here would be this repository's opinion again, by another route.

## Consequences

**The hand-written store is deleted**, and what replaced it is wiring plus the apply path. The
evidence that this is the right shape rather than a saving is that the four suites passed on the
first wiring attempt with **no store code written at all**: everything that had to be chased was
setup, not semantics.

**A Temporal version bump moves the schema, the statements and the suites together.** That is the
point of the embedding and not a side effect: the three cannot drift into disagreeing, because none
of them is ours.

**What waltz inherits, it also inherits the semantics of — and the drain cannot mirror all of them.**
Four differences are known and deliberate, and each is recorded where the code is:

* upstream's `dbRecordVersion == 0` fallback, which compares `next_event_id` against the request's
  condition, has no analogue: `fold.RunAssertion` carries a base version derived as
  `DBRecordVersion - 1`, so a request at version 0 could only fail. Such a request halts the shard as
  an invariant violation rather than being refused, which is a gap worth a ticket if pre-1.12
  requests are ever in scope;
* a create's `CurrentEqualsWithVersion` assertion is compared against
  **`current_executions.last_write_version`**, where upstream's sequential path joins and compares
  `executions.last_write_version`. This is deliberate: the first column is what
  `GetCurrentExecutionWithLastWriteVersion`, `baserow` and fold's pre-ack check all read, and
  matching upstream here would let the layer acknowledge a write the drain then refuses — a halted
  shard over a write the layer promised;
* a conflict-resolve's current row carries a reduced execution state, because that is what fold
  hands the applier. Upstream writes the full state's blob and start time. This is a fold-level
  shape and not something the applier can repair;
* a row count other than one on an execution-row write is a condition failure here rather than
  upstream's `NotFound`, because `NotFound` classifies as an unknown outcome and would leave the
  caller retrying something no retry can make true. The row is uniquely keyed and this drain
  asserted it one statement earlier.

**One reflection, confined to one table.** `sqlplugin.Tx` names Temporal's own tables and nothing
else, so the watermark table's SQL needs the connection the transaction runs on, which is an
unexported field. The alternative — running it on the pool instead — leaves the transaction it was
meant to join and then waits for the connection that transaction holds. `New` creates the table
through the same path, so an upstream rename is a store that refuses to be built rather than a drain
that silently loses its watermark. Everything else goes through `TableCRUD`.

**Two setup facts are load-bearing and easy to get backwards.** `schema/sqlite.SetupSchema` must
*not* be called: for `mode=memory` the plugin runs the schema setup itself on the first connection,
so an explicit call is a second `CREATE TABLE`. And isolation between two stores is the DSN name and
nothing else — the plugin keys its connection pool by DSN, and the func `New` returns does not close
the database, since upstream's `connPool.Close` only decrements a refcount. Isolation built on
teardown here would look correct and be false.

**This is not a durability claim.** The database is in memory and dies with the process. What it
buys is that having somewhere real to write costs no cluster, no container, no port and no file.

## Considered and not taken: hand-write the store

What was there. Refused above: it is judged by our own opinion of a store's obligations, and it can
be plausibly wrong in exactly the way that leaves the layer's own suites green.

## Considered and not taken: fork or subclass Temporal's `sql` package

A fork gives complete freedom over the write path and costs a schema, a statement set and a merge at
every version bump — the drift the embedding exists to prevent, reintroduced. Subclassing is not a
thing Go offers: what it would mean here is copying the parts that are not exported, which is a fork
under another name.

## Considered and not taken: reach past `sqlplugin` for the whole apply path

Raw SQL on the transaction's connection would make the drain's statements ours to shape. It also
makes them ours to keep in step with a schema that is not, which is the fork's cost paid per
statement. The row work therefore goes through `TableCRUD` — upstream's statements against
upstream's schema — and the connection is reached for exactly one table, the one upstream has no
statement for because it is not upstream's table.
