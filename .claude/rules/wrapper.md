---
paths:
  - "wrapper/**"
  - "*.go"
---

# This repo: the wrappers and the door out

`wrapper/` is the WAL layer's seam into a running server, and the root package
is what composes it — `Layer.AbstractFactory(base)`, the value a custom main
hands to `temporal.WithCustomDataStoreFactory`. What to know before
changing either:

* the wrapper is a **decorator over the base `DataStoreFactory`**, not a
  fork of it: ExecutionStore and ShardStore come back wrapped, every other store
  comes back as the base built it. `wrapper.Options` is the whole of the mode
  switch — no layer is passthrough, a layer is intercept — and there is no
  flag beside it, because a flag and a nil layer could disagree. `Options.Layer`
  is **the only field of it that is a mode**, and it holds `ShardObserver`,
  `ShardWriter`, `ShardReader` and `MetricsSink` together (#44, #78). That too is a decision,
  made twice for the same reason: a writer with no reader is exactly the
  configuration that reads stale, and a write path nobody told about the acquire
  refuses every write for a shard it never got — a misconfiguration the layer
  cannot detect, since that refusal is indistinguishable from fencing. Separate
  fields put both mistakes one config line away; one field makes them
  unrepresentable. `ShardLayer` held one more face until #142 — the layer being
  told what its queues had deleted — and that is gone with the compensation it
  existed for. Consequence for the tests: a fake that fills one half now owes
  the interface the others, so **embed `ShardLayer` and leave it nil** rather
  than writing no-op methods — the fake satisfies the type, still answers only
  its own half, and panics by name if the wrapper ever reaches a half the test
  did not expect, which is the outcome a no-op would swallow;
* **intercept takes eleven methods and refuses a twelfth**: eight writes (#57,
  #142) and three reads (#78, #80), with `CompleteHistoryTask` answered
  `Unimplemented`. (The partition is also described for a human in
  `docs/handbook/01-overview.md`, which goes stale the moment eleven stops
  being eleven.)
  Six of the writes are the four mutable-state ones and both deletes — the
  deletes because #34 decided they are, since routing them around the log would
  force a drain each (~1.8% of the stream) and an outbox would leave a deleted
  execution readable, which #43 measured the suite noticing at call distance 1.
  The other two are `AddHistoryTasks` and `RangeCompleteHistoryTasks`, and they
  are **one decision**: a write deferred to a drain beside a delete that acts
  immediately is a delete that misses the row it was meant to cover, and for a
  scheduled category that is a lost timer rather than a leaked row (the store
  deletes those by fire time and ignores the task ids). ADR 0008 holds the
  boundary — why not shard writes, why not event history. Two of the
  reads are `GetWorkflowExecution` and `GetCurrentExecution`, whose answer one of
  the eight can change; the third is `GetHistoryTasks`, which is merge-on-read and
  whose base closure takes a *request*, because the merge asks the store below a
  different question than the caller asked.
  `TestInterceptModeTakesTheElevenAndOnlyTheEleven` drives all 28 by reflection
  and asserts the partition three ways; the bad failure is silent in every
  direction (a twelfth method taken into the layer is a rule appearing in a
  second place, one of the eleven left transiting is a write the accumulator
  never saw, a read answered from a cold store the window is ahead of, a task
  page with the tail missing from it, or a range delete that removes rows the
  window has not written yet);
* **`CompleteHistoryTask` is refused, in intercept mode only** (#142): the log's
  deletion record is a range per category and a single key is not one, and a
  second deletion shape would be a second thing every reader, every drain and
  every replay has to agree about. Its one caller in the server is the admin
  handler's `RemoveTask`, which today lies about a task still in the window.
  Passthrough transits it, which is what keeps passthrough's "changes nothing"
  claim intact — and the handful of upstream tests that call it are out of
  contract for a deployment running intercept, not open bugs. The reflection
  test has a `refused`
  set beside the other two; a second entry in it would be a second thing the
  record format has no shape for, which is a decision and not a detail;
* **an intercepted write puts its own new events down first**, through the base
  store's `AppendHistoryNodes`, because event history stays out of the WAL in v1
  (D3) and that is exactly where a store's own Create/Update/ConflictResolve
  put them. Forget it and the log holds a mutable state pointing at history
  nodes nobody wrote;
* **a write also hands the layer the store's own two reads** (#74): the condition
  authority verifies an assertion the window does not determine against the
  pre-window row, and this package may not name a cold store any more than
  `cycle` may. They are a `*baserow.Rows` over this store's own base, converted
  once at construction (`baserow.Of`) rather than a closure built per write,
  because that is all they are — `TestTheWritePathIsHandedTheStoresOwnReads` is what a
  swapped or foreign pair fails, and a foreign pair would verify a condition
  against somebody else's row, which is worse than not verifying it;
* **the whole point of the synchronous drain is attributability, and the
  mapping is the real work in it.** At a window of one the failed assertion
  is the one caller's, so `cycle` answers it with the store's own error instead
  of halting — 19 of the suite's 95 mutable-state writes are expected condition
  failures, and a start racing a start is the same class in production. The
  translation lives in `cycle/write.go` and not in the wrapper, which may not
  import anything that reaches a cold store;
* **errors are returned unwrapped, everywhere on this path.** The shard's write
  path switches on concrete error types, so one `fmt.Errorf("…: %w", err)` turns
  a recognised outcome into the default arm — for backpressure a self-inflicted
  failover (#47), for ownership-lost a shard that does not know it lost. The
  test asserts identity (`err == sentinel`), not `errors.Is`, because a wrapped
  error satisfies `errors.Is` and is exactly what is forbidden;
* passthrough's claim is checked here by `TestEveryMethodTransits` (no cluster),
  which drives all 44 forwarding methods by reflection so a method forwarding to
  the wrong base method, or to none, fails by name — arguments matched so the
  base is asserted to see the caller's own request, results checked by identity
  so a wrapped error fails. **What that cannot catch is a wrapper that quietly
  rewrote a request** and still forwarded to the right method. Nothing in this
  repository can: it needs a whole-store comparison of a bare run against a
  wrapped one over a real store, which is a deployment's test to run and is
  worth running once — dropping the tasks off an update leaves every functional
  suite green and shows up only as missing rows;
* `TestTheWholeSurfaceIsCovered` pins 28/6/10 method counts. The reflective test
  enumerates whatever the interface has, so a shrunken interface would still pass
  it; if a temporal bump moves a count, the wrapper and #44's research note both
  need rereading — do not just update the number;
* the ShardStore's one observation is `UpdateShard` with
  `RangeID != PreviousRangeID`. It fires **before** the base store call (I11: the
  log's epoch may never lag the database's) and its error aborts the acquire
  without the rangeID moving. `ShardObserver` is one face of `Options.Layer`
  rather than a field of its own, so the value that hears the acquire is the
  value that takes the writes — in production the apply cycle's registry (#55),
  which was its first implementation and is still the only one;
* **a server does start here, and a green `make test` is still not "the server
  works".** `internal/verify/e2e` boots frontend, history, matching and worker in
  the test process over a composition, registers a namespace and runs a workflow
  with an activity through the SDK — twice, the second arm the passthrough
  control that says the layer was empty. What that run does not carry is load, a
  kill, or a database that outlives the process: it is one workflow against
  `cold/memcold`. The stronger evidence is upstream's own functional suites over
  a composition — `patches/README.md` has the fifteen-line patch that makes them
  reachable, and that is a deployment's run rather than this repository's;
* **`Layer.Options()` is complete as it comes, metrics included**, which is what
  lets a caller wiring the layer into a test compose with a capture handler and
  read what *both* halves of the layer recorded through the one emitter that
  composition holds. A caller that wires its own emitter beside it has two;
* the wrapper is where the server's `metrics.Handler` enters the layer (#59),
  and that is the whole of the metrics plumbing: `NewFactory` hands it to
  anything below implementing `wrapper.MetricsSink` and fills nothing in
  itself. **`Options.Metrics` is the layer's own `*walmetrics.Emitter`, not a
  handler**, and that is what makes one hand-off enough for both halves: the
  stores record through the value the cycles record through, so a binary running
  several services cannot send the wrapper's series to service N's handler and
  the cycles' to service 1's — which is exactly what a handler per `NewFactory`
  call did. Passthrough carries no emitter and needs none: every counter this
  package raises is on the intercepted path. `wal_transiting_writes` is gone
  with the write it counted (#142): nothing on the intercepted side of the
  boundary goes around the log any more, and a series that can only be zero reads
  as a system doing no work.
