---
paths:
  - "fold/**"
---

# This repo: the accumulator

`fold/` accumulates a window of mutations and emits, per dirty workflow,
one merged request plus the assertions that request stands on (#13, prototype on
branch `prototype/fold-emit`). The handbook's
[03-components.md](../../docs/handbook/03-components.md) says why it sits beside
`wal/` rather than under it. The *read* side — the read-only-on-the-accumulator
discipline that `overlay.go`, `check.go`, `tasks.go` and `taskpage.go` share —
is judged by this package's own tests and by nothing above them, which is the
thing to keep in mind below. What to know before changing the fold itself:

* **assertions from the head of the window, data from the tail.** A fold of
  v2..v4 onto a base row at v1 must assert 1 and write 4, and no request type
  expresses that — every one derives its assertion from the version it writes.
  So the accumulator carries the assertion set (`Emitted.RunAssertions`) beside the merged
  request and apply's transaction wrapper substitutes it (#24). Fold's whole job
  is to know what it asserted;

* **that assertion set is derived once, in `assert.go`, and applied twice.**
  `assertionsOf` is a function of the request and of nothing else; `add*`
  records what it returns and `check.go`'s `decide` evaluates or delegates it,
  so a new request shape or a new mode arm is one edit there plus a row in
  `authority_derivation_test.go`'s table. The current row's *write* is derived
  there too (`currentWriteOf*`), because it is a function of the request in
  exactly the same way and the two facts do not follow one another — a bypass
  mode asserts the row and writes nothing, which is a column of that table
  rather than a second list of modes. So the taint guard asks the derivation
  (`want.current != nil`) instead of re-listing them, and the three handlers end
  in one commit-phase verb, `workflowAcc.recordCurrent`. What the two halves still do
  differently is *apply* it — `adopt` keeps a run's existing head and drops the
  fallback, which is why the fallback is `nil` at the three sites whose branch
  already has one — so `TestTheAuthorityDerivesWhatTheFoldRecords` still holds
  them together, now as each consumer against the one derivation rather than
  against each other. It compares on an empty window only, and cannot do
  otherwise: on a real one `decide` evaluates where it would delegate, so the
  comparison would be of the recorded/discarded partition. A differential run
  over the write path cannot see `check.go` at all: it answers before the append
  and its subject is a call, not a row;

* **and it is evaluated once too**: one predicate per assertion type
  (`RunAssertion.against`, `CurrentAssertion.against` in `check.go`), over a row
  the caller resolves. The window resolves it from `workflowAcc.currentView` —
  the row the window's last writer left, or its removal — while
  the pre-append check and apply's attribution resolve it from a row they read
  (`VerifyRow`), and only the resolution may differ. A site that judges a
  condition of its own is how they drift, and the drift is silent: the copies
  were a whole condition apart (`last_write_version`) before this collapsed
  them. **What the pre-append side no longer states by hand is the order**:
  `Delegated.Settle` walks the obligations — the current row, then the run
  rows — and hands each to a caller that reads the row it names, so the
  registration order and the first-failure rule are executable rather than prose
  at both ends of that seam. The reads themselves stay with the caller that has
  them, and so does the refusal of one that brought none
  (`cycle.checkDelegated`, `cycle.ErrNoBaseRow`): fold names the rows and judges
  them, and reaches for nothing. Apply's attribution keeps a walk of its own on
  purpose: it reports *every* diverged row rather than the first, and it needs
  each row's own numbers for what it reports, so it shares the predicate and not
  the walk;

* **the current row's facts are the workflow's, not the request's.** A dirty
  workflow usually drains as one request, but one whose window tombstoned a run
  and created the next one behind it drains as two — and the head-of-window
  current assertion, the recorded current write and `CurrentRemoved` are the
  same for both, because they are properties of the window's effect on one row.
  One `WorkflowRecord` per workflow, pointed at by every request that names it
  (`Emitted.Workflow`). Apply registers them at the *first* request naming the
  record — which the drain marks (`Emitted.FirstOfWorkflow`) rather than each
  consumer re-deriving — and not in a pass of its own: the store reports the
  first failing assertion in registration order, so hoisting them all to the
  front would change which failure a mixed batch reports. Copying them onto each request
  instead is what made apply carry a `currents` map, an `equalCurrent` and an
  error class for two requests of one workflow disagreeing — a disagreement one
  source cannot produce, and the shape now cannot express;

* **the partition itself is `heldRun` and `assertsCurrent`, and both halves ask
  them** — which is the one part of the authority the bullet above cannot
  compare, and nothing checks it — so **do not compare the current row against
  nil anywhere else**: a site that answers the partition itself is how the two
  halves drift, and the drift is silent. The run map is a different matter,
  being indexed for questions that are not the partition;

* **snapshot-bearing requests reset the accumulator (I8), and that is the same
  rule rather than a special case.** A Create followed by ten Updates folds into
  a Create, which needs no assertion rewriting because a Create asserts absence.
  Tasks are the one exception: they are queue entries and not workflow state, so
  they concatenate across the barrier;

* **upsert-vs-delete is resolved per key, inside the accumulator.** Left
  unresolved, the store's own query ordering issues every upsert before every
  delete, so a key re-upserted after being deleted is written and then deleted
  again — an acked write gone, silently. The
  current-execution row obeys the same rule since #54, when apply gained a
  transactional delete for it: a window that writes the row and then removes it
  emits the removal alone (`WorkflowRecord.CurrentRemoved`);

* **buffered events do not merge.** There is one `NewBufferedEvents` slot per
  mutation, the merged mutation's slot is always nil, and the batches ride in
  `Emitted.BufferedBatches` in arrival order, each with its run, for apply to
  write one row per batch. `ClearBufferedEvents` drops what accumulated before
  it and marks the merged mutation to clear what the store holds from before the
  window;

* **a deletion collapses the run's accumulator into a tombstone**, and the
  dropped state's tasks survive as `Emitted.OrphanedTasks` — a task is durable
  in the tail or in the store (I7), and a fold may not quietly lose one. The
  collapse leans on Temporal's deletion flow always pairing
  `DeleteWorkflowExecution` with `DeleteCurrentWorkflowExecution`: without the
  pair a folded window would leave the current row holding pre-window content
  where the sequential path updated it first. Whether unpaired deletes occur in
  acked streams is what a differential run against the sequential path judges;

* **what arrives after a tombstone is decided here**, since §6 of the design
  leaves it open. Such a mutation is impossible in an acked stream — the single
  writer would have seen its assertion fail — so fold returns `ErrAfterTombstone`
  rather than guessing, with two exceptions: another Delete of the same run is an
  idempotent no-op, because deleting an absent row succeeds sequentially too; and
  a Create begins the run's next life behind the tombstone, emitted as its own
  request after the Delete, keeping the head-of-window run assertion rather than
  must-not-exist, because at apply time the pre-window row is still there.
  `DeleteCurrentWorkflowExecution` is **not** a run tombstone: it removes only
  the current row, so later mutations of the run stay legal;

* **`Add` is all-or-nothing, and the shape that makes it so is a rule to read
  rather than a check to run.** A handler validates against `peek` and switches
  to `acc` — which creates — only past its last refusal, because an accumulator
  left behind by a refused mutation makes `heldRun` and `assertsCurrent` answer
  differently on the retry, which is a stale conditional write acked. So **no
  handler returns a non-nil error after calling `acc`**; six of them create, and
  the one being edited is the one to check;

* **a refusal is not an error the caller has to think about.** A few valid
  streams cannot be expressed as merged requests — each would need one run
  stripped out of a request carrying several (a continue-as-new folded into
  somebody else's envelope, a delete of one half of such a pair) — and those
  return `ErrRefused` with the accumulator **exactly as it was**. That is what
  makes drain-and-retry safe, and since #156 the loop is
  `Accumulator.AddOrDrain` rather than one every consumer writes;

* **the request shapes are dereferenced, not checked.** A request whose
  `ExecutionState` is nil cannot reach the store, whose own validation
  dereferences it, and upstream builds the state and its blob together from one
  value. Fold used to tolerate a nil in three of the shapes and dereference it
  in six others, and what the tolerance did was record *no* current-row write —
  a silent miss of exactly the class a differential run exists to catch. Do not add the check
  back to accommodate a sparse fixture; fix the fixture;

* **the read's answer is a hand-filled mirror too, and it is held to its type.**
  `snapshotOfBase` takes the cold store's row apart and `mutableStateOf` puts the
  answer together, and neither is derived from
  `p.InternalWorkflowMutableState` — so a field either stops filling comes back
  zero, which the caller reads as a run that has no such collection and then
  writes the run back without it. A snapshot-bearing write clears the run's
  tables first, so that is an acked write **deleted** rather than an answer that
  was merely stale. `TestEveryFieldOfAReadAnswerIsFilled` enumerates the answer
  off the type and fails by the name of the field nothing fills. Before it
  existed, five of those thirteen fields could be dropped with the whole of
  `go test ./...` green — `ChildExecutionInfos`, `RequestCancelInfos`,
  `SignalInfos`, `ChasmNodes`, `Checksum` — and of the eight that were caught,
  two were caught only by the e2e server timing its workflow out. It claims a
  field is *filled* and not what with, which source each comes from being judged
  case by case in `overlay_test.go`: a field added upstream fails here and is
  decided there. **The copy is a line per collection in both mirrors**, so the
  same hazard has an aliasing half — a map added upstream is handed out shared
  until somebody writes the clone — and it was invisible too. The two read-only
  rules (`TestTheOverlayIsReadOnlyOnTheAccumulator` for the accumulator's maps,
  `TestTheOverlayDoesNotWriteThroughTheBase` for the caller's) write into
  *every* collection of the answer rather than a chosen one. The one clone
  neither reaches is `copySnapshot`'s `SignalRequestedIDs`, and that is a fact
  about the answer rather than a gap: `mutableStateOf` rebuilds it as a sorted
  slice, so that map never leaves the package;

* **the contract with apply**: emitted requests carry no epoch, because
  `mutation.Decode` dropped RangeID on the way in (I11) and apply stamps its
  own. The collapse ratio — mutations in over dirty workflows out — is derivable
  from `Stats`; fold reports the two counts and emits no metric of its own;

* **`Batch` is `Drain`'s to build and nobody else's**, which is why its fields
  are unexported and its consumers read it through `Each`, `Len`, `Empty`,
  `Watermark`, `Stats`, `Tasks`, `Shard` and `Settles`. The last one is there because the
  apply cycle used to reconstruct "did this window ack entries whose fate this
  drain must settle" out of `Stats().MutationsIn` — a number whose purpose is
  the collapse ratio — and the rule it reconstructed lived in three places and
  no module. What apply used to re-check is what the opacity
  buys, and `fold/batch_test.go` is where four of those are judged now:
  tail-seqno order, a watermark at or above both halves of the batch, orphaned
  tasks on tombstones alone, and the shard the batch was folded for. The fifth —
  a workflow record behind every request — has no test and needs none: `Drain`
  is the only thing that can build a non-empty batch and it sets the record on
  every request it emits, so the nil apply used to refuse is unreachable rather
  than unobserved. Do not add a test that asserts it; add one the day a second
  constructor exists. The same
  reasoning runs down into `Emitted`: what a request *is* stays exported data,
  what fold *recorded about* it — the run assertions, the orphans, the record,
  the first-namer mark — is behind a method, since each is a claim only a drain
  can make true.
