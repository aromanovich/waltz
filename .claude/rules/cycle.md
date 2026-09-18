---
paths:
  - "cycle/**"
---

# This repo: the apply cycle

`cycle/` is the thing that runs the write path (#55): one goroutine per (shard,
epoch) owning the accumulator, the drain, the apply transaction and the trim,
plus `cycle.Manager`, the registry the wrapper's `ShardObserver` hook talks to.
Since #78 that goroutine answers the **read** path too (`cycle/read.go`), and
since #80 the merged task read as well (`cycle/tasks.go`) — routing it, that
is: what a task page *holds* is `fold.Accumulator.TaskPage` since #177.
What to know before changing it:

* **a read is a job on the loop, and that placement is the design**
  (#72): the window empties when a drain *starts* and the tail only when its
  transaction *commits*, so a read served anywhere else can fall into the
  interval where a mutation is in neither source — not stale, but a write
  undone. Serving it from the same goroutine closes that by construction, which
  also means **no test here can falsify it**: a read cannot be issued past a
  drain it shares a channel with, and both attempted breaks came back green.
  `TestAReadArrivingMidDrainWaitsForItsOutcome` says so in its own comment;
  believe the comment, not the test name. For a *task* read the same interval is
  worse than stale — the key is skipped, and a queue that reads a range completes
  it — so the placement is the same and the argument is stronger;
* **the loop serves jobs, and what may be asked of it is a list rather than a
  union.** `job` is `func(*state)`; `ask` is generic over the answer and `tell`
  is its error-only half, so an operation is its own closure and the 11-field
  `request`/6-field `reply` unions — where a reply for one operation could be
  filled for another — are gone. Two things went with them and are worth
  knowing. The `reqKind` enum used to make the set of operations visible in one
  place and nothing does now, so **read `ask`'s and `tell`'s call sites before
  adding one**: asking the loop is a decision and not a call, because everything
  on this goroutine is serialised behind the accumulator and the drain, and a job
  that waits on the cold store holds up every write on the shard. And a closure can
  return anything, where `reply`'s fields could not — including the loop's own
  `*state`. Nothing does, and the compiler will not stop it until `state` is
  owned the way `tailstate` is;
* **what a task page holds is not decided here** (#177): the cut, the token, the
  batch arithmetic, the dedup and the subtraction of the window's undrained
  ranges are `fold.Accumulator.TaskPage` (`fold/taskpage.go`), beside the
  window they read. What is left here is the round trip — the base page arrives
  as a callback, so this side builds the request and keeps the context — and the
  three counters (`TaskReads`, pages *routed*; `TaskReadsMerged`, pages that
  carried at least one window task; `TaskCollisions`, node-wide since #152).
  Before changing any rule about a page's contents, read that file: the cut has
  no freedom and its reasoning is stated there, at length;
* a task read on a shard this node does not hold is **refused** with
  `ShardOwnershipLost`, which is the opposite of what the two mutable-state
  reads do with the same case and is not an inconsistency: those have callers
  that legitimately do not own the shard, and this one has exactly one caller —
  whose page, if it came back short a tail, would be completed and acked past.
  That difference is one row of `readRoute` (below), which is where the whole
  question "who may answer this read while the shard changes hands" lives;
* the cold store is reached through a **thunk the wrapper passes**, never named
  here — the store is the caller's, behind `cold.Applier`, and `wrapper` still
  may not name one. The routing rule for a **mutable-state** read turns on the
  *tail* and not on the state — an empty tail is passthrough in either halt
  (which is what keeps sync mode exactly as it was), a non-empty one on
  halted-lost is `ShardOwnershipLost` **unwrapped**, and on halted-invariant it
  stays deliberately unrecognised. **A task read never reaches that rule**: it is
  refused at either halt whatever the tail says, as `ShardOwnershipLost` on
  halted-lost and as the halt itself on halted-invariant, which may not be
  converted. Both halves were real bugs. On halted-lost (#155) the page is short
  another owner's acks, which are in neither this tail nor the cold store. On
  halted-invariant an empty tail makes the page *correct* and the token it
  carries is still the base store's, so a shard re-acquired mid pagination — a
  range id renewal unloads nothing — meets that token at a cycle that merges,
  cannot read it, and finishes the pagination on the base alone; the window that
  drops out is acked task rows, which the range the reader completes deletes. A
  retired cycle has no loop left to ask, so it reads the tail off the mirrored
  atomic `write` already consults — **for the two mutable-state reads only**,
  since #160: a task page is answered by the shard's *current* cycle or not at
  all, which is `Manager.taskPage` and cannot be anywhere else, because being
  superseded is a fact about the registry's map that no cycle can read;
* **`Manager` is the seam, and the three reads on `*Cycle` are unexported for
  it** (#176). There were two doors into the same questions and the cycle's was
  silently the less correct one; the reasoning is on the `Cycle` type and beside
  each read. The append went the same way in #216, for that reason and for
  I11's: the epoch check is `Manager.Write`'s and a caller holding a `*Cycle`
  has already resolved the shard, so an exported append here was a second write
  door that skipped it. It is `Cycle.write` now. The concession that had kept
  it exported — `internal/verify/guard`'s backpressure test needing the value the cycle
  itself refuses with — is gone: it drives `Manager.Write`, which hands the
  refusal back untouched, because `storeError` translates nothing the store's
  own `OperationPossiblySucceeded` reads as definitely-not-committed and I10's
  refusal is one of those. What is left exported is not listed here but
  checked, one name at a time with what it is for, in
  `cycle/surface_test.go` — where the bar is that none of them writes,
  not that each has a caller outside the package;
* **the policy is a source, not a value** (#204): a `Cycle` holds a
  `cycle.Policy` — `func() Config` — and reads it *at* the decision, which is what
  lets the five watermark and cadence numbers move under a shard this node is
  already holding, with no re-acquire and no replay. So do not cache a `Config` on
  the cycle beside it: the field a later read reaches for by mistake would be the
  one the cycle was created with. `Fixed` and `Live` are the only constructors and
  both fill, which is how "every call answers a complete Config" holds for callers
  that cannot call `fill` themselves. The clock is the deliberate exception, taken
  out once at `New` (`Cycle.clock`) because it never moves;
* **`Config.DrainOnRead` is an instrument, not a mode** (#145): a read drains
  the window instead of merging over it, so all three reads are answered by the
  cold store. It is off in `Defaults()`, set by nothing that ships, and it leaves
  with the ticket — `drainonread_test.go` is deliberately its own file for that.
  The reasoning, including why the counters are taken *before* the drain and why
  a halted cycle must not reach it, is on the field and beside `drainForRead`;
* **nothing may hold `Manager.mu` across a cycle's loop**, and that is a lock
  inversion rather than a long hold: the read path resolves through
  `Manager.Shard`, which wants that mutex. `ShardAcquired` broke it for as long
  as it existed (#160). **The mutex is not `Manager`'s any more**: `held`
  (`cycle/held.go`) owns the shard map, the retired counters and the lock
  over both, and every one of its methods finishes its map arithmetic and
  returns without touching a `*Cycle`. So there is no lock on `Manager` to hold
  across a call into a cycle, and the four copy-out prologues that each
  remembered to drop it are gone. `TestAnAcquireDoesNotHoldTheRegistryWhileItAsksASupersededCycle`
  stays as the behavioural half — it fails by timeout if a lock is reintroduced
  on `Manager` — and `held.totals` returns the retired block and the live cycles
  together because a reading that mixed two moments could count one cycle twice,
  in the retired block it had just joined and in the snapshot it had not yet
  left. Ask a cycle for anything — `Stats`, most of all — *outside* the lock;
* **a shutdown is a state on `held` and not merely an emptied map** (`ErrClosed`).
  `takeAll` closes the registry, so an acquire arriving behind `Manager.Close` is
  refused rather than installing a cycle that acks into a log the layer is
  releasing. That acquire is reachable: the shard controller and the layer's
  shutdown are ordered by convention, not by a lock, and `UpdateShard` with a
  moved rangeID drives `ShardAcquired` for as long as the server has shards. What
  such a cycle would cost is not the entries — they are in the log, which is what
  a successor replays — it is that they appear in no `Residue`, and a shutdown
  that answered nothing is the only evidence the caller *removing* the layer has
  that nothing is being stranded (`waltz.UndrainedError` says so at length). The
  fence that acquire already took is not undone: a fence changes ownership and
  nothing else, and the shard's next owner fences above it;
* **the three reads share one order and it has one home, `Cycle.prelude`: the
  readiness gate, the count, the halt rule, the drain.** A halt found inside the
  replay is left to the halt rule rather than returned from the gate (#155).
  Every position is load-bearing and the reasoning is on `prelude` — a halt rule
  asked first sees a running cycle and lets the read merge a window the replay
  has since reset; a count after it loses the reads it passes through, one of
  which a witness asserts on; a drain before the count shows the counters a
  window the read never saw. `prelude_test.go` holds the three positions, each
  against the mutation that moves it, so reordering fails rather than drifts.
  The two mutable-state reads hand `prelude` a `take` that builds their view and
  reports whether the window held the row; it runs again after a drain, which is
  the re-take rule that used to be a statement at each call site. **The task read
  counts outside it**, because `TaskReads` is pages *routed* — a page the gate
  fails is still one this shard was asked for — and it passes no `take`, merging
  over the window rather than rendering a row out of it;
* **the two mutable-state reads have one body, `readOverlay`, and the layer
  above them deliberately does not.** What they share is the order after the
  prelude — the base row where the view needs one, then the render — and they
  differ only in the view they take and in what they call an absence, so a third
  read of that shape is a `windowView` and a message. What was *not* collapsed
  is the routing above: `Manager.GetWorkflowExecution`/`GetCurrentExecution` and
  the two `Cycle.getX` still spell out `Shard`, `ask` and the stopped arm
  apiece. That is two recorded decisions and not an oversight — spelling each
  call site out is what is left of the operation set closures stopped making
  visible, and `decide.go`'s rules keep a method beside each call site so what a
  site can get wrong is which values it hands over. A generic asking the loop
  would take both properties back;
* `Stats.ReadsHeld` is not `Stats.Reads` (and `Stats.TaskReadsMerged` is not
  `Stats.TaskReads`), and the second of each pair is the number a
  witness needs: reads that never crossed a held workflow is what a layer that
  came out empty looks like. In sync mode `ReadsHeld` is **0 by construction**
  and a sync-mode witness asserts it, which is what keeps a whole-store
  comparison meaningful with a read path wired in;
* **a counter is declared once, in `cycle.Counters`, and `Stats` and `Totals`
  embed it** (#153). It is counts only — a **position** may not go in, because a
  new cycle inherits a log's positions and summing one double-counts. Three
  rules are guarded in `counters_test.go` rather than documented (a non-int
  field, a counter `add` forgets, an exported merge); the reasoning is on the
  type;
* **`Totals.Acked` and `Totals.Applied` are positions and stay positions**, and
  the rule above is why they sit *outside* `Counters` rather than why they
  should be booleans. They are summed over the cycles held **now**, never over
  retired ones, so the double-count the rule names cannot happen; and a run's
  log starts empty, which makes a position also a count — that is
  what lets the sync witness do its condition-failure arithmetic
  (`witness.go`'s `Acked - Drains` against the emitted series, and its
  `Acked <= Drains` refusal). The caveat rides on `witness.Observed` because it
  is a property of the fixture and not of the layer. **Do not narrow them to
  `AnyAcked`/`AnyApplied` predicates**: it reads as tightening the counters rule
  and it deletes the only claim that tells sync mode from a run that committed
  everything it acked;
* the states are not defensive branches, they are #46's decisions: **halted-lost**
  is fencing working (drop the tail, do not trim), **halted-invariant** is a
  divergence this process owns — no retry, and it must never be converted to
  `ShardOwnershipLost`, which would hand the bug to the next owner as an
  ordinary failover. `storeError` is where that rule is enforced at the boundary
  (`cycle/decide.go` since it stopped taking a `*Cycle`; `write.go` is its
  one caller): a fenced cycle's refusal *becomes* `ShardOwnershipLost`, and a
  halted-invariant one deliberately stays unrecognised;
* **the condition authority is checked before the append, and it is not an
  extension of `answer`** (#74, `cycle.check`): what the window determines it
  answers with the store's own error at *any* window, because the subject is the
  mutation in this caller's own call and nothing has been acked. The predicate is
  `fold/check.go`, the checked set is the assertions `adopt` discards — do not
  turn it into a list — and the position is forced twice: after I10's bound
  (which is also checked outside the loop) and before `Log.Append`. It is inert
  at a window of one, which is what keeps sync mode from regressing;
* **the delegated read is the residual, and it is skipped in sync mode on
  purpose** (`cycle.checkDelegated`, which `cycle.check` does not reach there):
  an assertion the window hands on stands on
  the pre-window row, so the row is read — through the same base reads the
  wrapper passes for the overlay, inside the loop, with no TOCTOU under fencing.
  In sync mode the drain that asserts them runs inside the same call and its
  outcome is the caller's answer, so the read would buy nothing and cost a round
  trip per write. **The current row is answered exactly, and that took a column**
  (#138): the assertion has three conditions — run id, state and
  `last_write_version` — and the persistence interface's read returns two, so
  until the store returned the third this read could confirm and never refuse
  (`fold.CurrentAssertion.VerifyRow` has the whole reasoning);
* **what those reads travel in is one value, not two fields** — `baserow`,
  a leaf package the wrapper, the cycle and apply all import because it imports
  only Temporal's persistence and so reaches no store. They are needed
  *together* by whatever delegates, so as fields "a run reader and no current
  reader" — or its mirror — was one call site away and detectable only on a
  request that happened to delegate: the same argument `wrapper.ShardLayer`
  makes about a layer's three faces. Of the four states a pair of nilable fields
  had, **three stopped compiling**; the fourth is now a nil `*baserow.Rows`,
  which is a presence problem and Go has no answer for it. So `ErrNoBaseRow`
  stays — beside the caller that brought no rows and **one state to check where
  it was three**, asked inside each arm of the delegated walk
  (`fold.Delegated.Settle`) so the refusal names the row the store would have
  judged first — and stays a *refusal* rather than
  becoming a panic, for the reason written on it: a refused write provably acked
  nothing, and "unreachable" is a claim about today's callers;
* **the settle is gone, and what it was is worth knowing before someone
  rebuilds it** (#89, removed in #138). An unconfirmable current-row assertion
  used to be *settled*: drain the window, put the mutation through alone, hand
  back what the transaction said — correct, and a transaction plus the window's
  collapse plus a mechanism, all for one column. The shape that paid was a
  `CreateWorkflowModeUpdateCurrent`, a start over a reused workflow ID, on a
  workflow the window did not already hold. What replaced it is one more column
  on a point read the store was already issuing:
  `GetCurrentExecutionWithLastWriteVersion` returns `last_write_version` beside
  the current row. The plumbing is worth reading once, because it is the only
  place this layer asks the store below for something `p.ExecutionStore` does
  not declare: `baserow.Store` is a **shape**, not a named type, and `baserow.Of`
  takes it off the store once at construction — refusing by name where the
  wrapper used to assert unchecked. That is the only conversion left on the path
  and it is Temporal's own seam, since the store arrives as `p.ExecutionStore`;
  a caller holding a concrete store that already answers the shape passes
  `baserow.New` and the obligation is the compiler's. **This is the one
  obligation this library puts on the store below**, and a store that does not
  answer it is refused at construction (`baserow.ErrNoVersionedRead`) — where
  the server is still starting and can be told what is missing, rather than
  serving a mode whose conditions it can confirm and never refuse;
* **sync mode changes the reading of exactly one outcome** (#57): a condition
  failure at a window of one is the caller's answer, not a halt, because there
  is exactly one caller in the window to attribute it to. Everything else means
  the same in both modes. The entry stays in the log but is *settled* — the
  tail bound stops counting it (`tailstate.Tail`'s `resolved`) while the watermark does not
  move, because trim goes to the watermark and trimming past what the cold
  store holds strands a recovering owner. Merging those two counters is the bug
  to avoid in both directions;
* an unknown outcome reads the **watermark** and nothing else. Re-deriving the
  answer from base versions is the recovery that applies a committed batch
  twice (`cold.Watermarker`'s whole point: the value the committing transaction
  wrote inside itself, never one derived from the rows), and the cycle does not rebuild a
  *drained* window either — those entries' requests were driven, so re-driving
  them builds a transaction out of mutated state. Replay reads the log, which is
  a different thing;
* **whose clock may cut a drain short is a field of `drainCause` and not the
  context the call site happens to hold** (`detached`). Four drains carry it —
  the three watermarks and the refusal drain, three of them inside a call whose
  caller is not waiting for their outcome, the age tick's on a
  `context.Background` of its own — and what they carry is earlier writers' acked
  mutations, those writers having been told it succeeded and gone. Bounding the
  transaction by whichever writer is on the line turns one expired client
  deadline into a drain that did not commit, which is `halted-invariant` and a
  failover for the whole window. The entries survive in the log, so the cost is
  availability rather than data — but the trigger is an ordinary timeout, which
  is what makes it worth a field rather than a call-site habit. The other five
  keep the caller's context and each carries its reason on the row: sync mode's
  outcome *is* the answer, `drainNow` is what the shutdown budget bounds one
  transaction at a time, a `DrainOnRead` reader is answered out of the cold store
  after it, and replay has no bound of its own, so a caller's clock is the only
  thing that can interrupt it;
* **`Cycle.resolve` detaches whatever the cause says**, and that is a second rule
  rather than the same one: the commonest way an outcome becomes unreadable is
  that very context expiring inside `Apply`, so the one read that could settle it
  would be issued on the context that provably cannot answer. A drain that *had*
  committed then stalls, its writer is told it failed, and the shard refuses
  every write and both reads until the age tick asks again on its own
  `context.Background` — which is what this is, one drain earlier, so a store
  that never answers hangs the loop exactly where it already would.
  `draincontext_test.go` holds both rules, each red only for its own half;
* **replay is `cycle/replay.go`, and it is `start` grown a body** (#98): the
  watermark, then `(appliedSeqno, tail]` in pages of the window's size, folded
  into a fresh accumulator, cut by the **size** watermarks only (everything read
  is already as old as the incident) and ending in a drain. Five things about it
  are decisions, not mechanics:
  - **the readiness gate is the placement**, so there is no flag and nothing to
    refuse: a request arriving mid-replay is already parked in `ask` on its
    own context. What that needed is that **a read triggers replay too** — all
    three of them, since a task page short a key is worse than a stale row: its
    caller completes the range and acks past it;
  - **a provisional entry is carried alone and its condition failure is a drop.**
    Sync mode acks *before* the condition is verified, so
    `Payload.provisional` marks its entries at the append and replay reads it
    back. The settle was the second such path until #138 removed it. On anything else a failure still halts. Do not "simplify" this into
    forgiving every condition failure at replay: the other half is what makes
    this half safe, and both are pinned side by side;
  - **an entry above this cycle's epoch means we are the zombie** — halt lost
    before a row is written, and checked here rather than left to the apply
    transaction's epoch CAS, because §9's steps 1 and 2 are not atomic;
  - **replay has no bound of its own.** I10 bounds what a *running* cycle acks;
    a tail that somehow exceeds it must still be replayed, or the shard is
    unrecoverable. A failed page read leaves the cycle unstarted and the window
    empty, so the next request retries from the watermark — a read that failed is
    not an answer;
  - **an abandoned attempt is dropped only where another one follows, and that
    is the whole of the rule.** A retry reads the watermark again and re-acks
    everything above it, so an attempt the cycle will repeat gives back what it
    took: its accumulator and window are replaced, its acked bytes go back to
    the floor (`Tail.Floor` plants all four numbers, which is what makes it safe
    to run twice), and what it counted is a `Counters` value `start` never
    adopts (`state.counted`). Keeping any of it counts one incident once per
    attempt — and the tail is the number `Cycle.write` reads off the mirror
    *before* it queues anything, so the leak's first symptom is a working shard
    refusing its writers. **Halted inside the replay is the other half**: no
    attempt follows, so the tail stays as the evidence `routeRead` refuses
    reads on and the counters stay with it. The counters therefore
    under-report a committed drain inside an abandoned attempt, deliberately —
    which is the reading `Replayed` and `Dropped` always had, in the direction
    that undercounts a rare incident rather than inventing acks on every retry;
* **`Deps.Registry` is required**, and `NewManager` refuses a nil one
  (`ErrNoRegistry`). It must be the *server's* registry — `waltz.TaskCategories`,
  which is upstream's own two providers and nothing else — because the archival
  category exists only where archival is configured, and an unknown category id
  is fatal to a replay by design. A default registry here would refuse to replay
  exactly the entries carrying archival tasks;
* the cycle starts at the watermark's successor and replays everything above it,
  so by the time it appends, its seqno is past the whole tail. `ErrAlreadyWritten`
  therefore no longer means "somebody left a tail": it means a second writer at
  *this* epoch, which the fence should have made impossible, and the halt keeps
  its old name (`ErrTailNotEmpty`) and its old class;
* **an append error the contract does not name is read back, not assumed**
  (`appendOutcomeOf`, `Cycle.settleAppend`). It is the drain's unknown-outcome
  rule at the other seam and the symmetry is exact: the log is the witness for an
  append as the watermark is for a drain, the read is detached from the caller's
  cancellation for the same reason — a client deadline expiring inside the call
  is the commonest way the outcome became unreadable — and only a *definite*
  answer moves anything. Three things about it are decisions. **An entry that is
  there is a success**, not a halt: the payload is this cycle's own, it is
  durable, and telling the caller it failed is the lie the first rule is about,
  the caller's next write standing on a state this entry is about to move.
  **Nothing there is not a halt either**, which is what keeps a transport blip
  from costing a shard — the contract's three refusals already write nothing, and
  this makes the fourth case say so rather than assume it. And **a read that
  failed halts**, where the drain's equivalent stalls: a stall lets the shard
  keep acking, and the very next thing a writer needs here is that seqno, so
  there is nothing to keep. An append-side stall that refused writes until the
  readback answered would heal where this fails over, and it is a bigger
  mechanism than the one place it would help — a log that answers no reads is
  a log the successor cannot write to either;
* **`cycle/acked.go` is gone and must not come back** (#142). It held I7's bound
  — per (shard, epoch, category), raised only, filled from a queue's goroutine
  under a mutex, snapshotted per drain — and every one of those placements was
  forced by the same thing: the delete had already happened when the layer heard
  about it. A range delete is an entry now, so the bound is part of the window,
  moves in log order and is rebuilt by replay; #139's residual is closed by
  construction rather than counted. What is worth keeping straight if anyone
  reads the old notes: the **drop is now exactly the range** (`[min, max)` under
  the store's own per-category predicate) and not a monotone bound, and **a task
  arriving after a range delete is kept**, because the sequential path keeps it
  and a differential run against the sequential path judges the difference. The window's counters (`AckedRanges`,
  `DroppedTasks`, `WrittenTasks`) survived the move and mean the same things;
* the numbers are measured: 256 mutations / 256 KB is #45's knee, and the 5 s
  age is explicitly *not* — it is a recovery-budget choice the collapse curve
  does not constrain. Changing either means reading #45 first. The I7 drop's
  share was measured on #45's stream too — **13.4% at the shipped cadence**, 60.6%
  at one drain per checkpoint, *smaller* under load — and what moves it is drains
  per queue checkpoint, the share itself being a workload measurement rather than
  a constant of this implementation; the cheap knob is the incumbent's
  `history.*ProcessorUpdateAckInterval`, not this layer's age watermark;
* **the watermark branch runs above this package too; its outcomes are this
  package's own** — `internal/verify/acceptance` drives the layer at
  `cycle.Defaults()`, whose window is 256, so the drain those watermarks trigger
  is exercised there against a real store and a real log, committing. What
  `cycle/cycle_test.go` adds is the outcome: its tests drive fakes because every
  input the state machine reacts to is one of their answers — the log's, the
  applier's, the watermark's — which is how a watermark-triggered window meets a
  condition failure at all. Since the decisions came out (below) one half of that
  branch is judged apart from the cycle as well: what such a failure *means* is
  `attribute` and is enumerated in `decide_test.go`;
* **trim is `cycle/trim`**, beside `tailstate` and `window`, and it runs
  beside the loop rather than in it: a stuck trim must not stop the shard from
  acking and applying, and `TestATrimNeverBlocksADrain` is what says so. The
  cadence, the one trim in flight and the two counters are the module's; the
  cycle hands it a watermark and the two numbers read at the decision
  (`Config.cadence`'s `TrimEvery` and `TrimAfter`) and asks nothing back but
  `Counters` at `stats` and `Wait` at `Retire`. What that bought beyond locality
  is the ownership rule below: the `go` statement is in a package that cannot
  see a `*state`, so it is structural rather than prose, and the cadence
  arithmetic is judged in `trim`'s own tests with a fake log and a driven clock
  where it used to need a whole cycle env.
  A failed trim is retried at the next cadence and never halts anything — and
  its retry is observed rather than assumed, which is #86: a second write issued
  while the first trim is still in flight is *correctly* given no trim at all,
  so a test driving the next cadence waits on `Trimmer.Wait` first. A concurrent
  trim also does **not** cost the appends their I9 immediacy — measured once on
  the research prototype's cluster (#49: the coordinated write count did not
  move) and unmeasurable here, where no log a run uses has such a counter — so
  the cadence's numbers stay free to move for read-cost reasons alone;
* **the tail bound (I10, #47/#56) is not the window watermark**, and the two
  byte counters are different numbers: the window empties when a drain starts,
  the tail only when its transaction commits. An outcome nobody could read
  leaves those entries in the tail, where they belong —
  `TestAnUnresolvedDrainStaysInTheTail` pins it, and merging the counters is
  how the bound loses track of exactly the memory a cold-store incident
  strands;
* **an outcome nobody could read is carried, and the thing that carries it is a
  floor on the tail** (`Tail.Stall`, `unresolved_test.go`). It is not a third
  halt: halting on a failed *read* loses a shard a blip would have healed. What
  it stops is everything else — `Settle` does nothing at all while it stands,
  the write path refuses with I10's own `ResourceExhausted` at both the moments
  it is answered at (the mirror carries the stall for the pre-queue one, because
  the loop a writer would queue behind is inside the very watermark read that is
  failing), **both readers are refused with that same value** (#389: the window
  is gone and the cold store's rows are exactly what could not be confirmed, so
  a merge answers a read out of neither source — a write undone rather than a
  stale one, and a task page short a drain's tasks to the caller that completes
  the range), and every drain re-asks the watermark before it takes the window.
  The seqno, its bytes and its cause are three fields of one stall in
  `tailstate` and not a field here beside two there: they end together, and a
  cause outliving its stall attributes the wrong drain. The reason the ban is on
  *committing* and not merely on the tail's arithmetic is that `apply` moves the
  cold store's watermark by writing it: a later drain that commits tells that
  store the unresolved entries are applied too, and the trim behind it takes
  them out of the log. Two couplings here have no local symptom. **The age tick
  is the only healer** — a stalled cycle refuses its writers and answers no
  reads, so nothing a caller does brings a drain with it, and a tick that fired
  on an aged window alone would leave an empty-window shard stalled for good. And `Cycle.refold` is on the same arm:
  the fold that was refused, whose recovery drain then failed, leaves an entry
  acked and durable in **no window at all**, which is a mutation the next
  watermark move steps over and the trim then deletes;
* **a drain that folds to nothing still settles what it acked** (#214), and the
  guard on it reads in both directions — which is why it is `fold.Batch.Settles`
  and not an `if` here: the drain asks the batch for the position it acked and
  settles on the answer. A window can ack entries and produce an empty batch in
  exactly one shape, and that shape is fold's property rather than this
  package's: `fold/emptydrain_test.go` names it and holds every other kind
  against it. Settling such a window unguarded is the bug in the other direction — a drain over a window that folded *nothing* carries
  watermark zero, and `resolved` taken from it puts the whole log back under
  the tail, so I10 refuses every write on the shard over memory nobody holds.
  `TestADrainOfAnEmptyWindowSettlesNothing` is the one that says so by name;
  before it, the guard was held only by an assertion inside a replay test about
  something else;
* **the tail's arithmetic has one owner, `cycle/tailstate`**, and it spans
  two goroutines on purpose: `tailstate.Tail` is loop-owned like the rest of
  `state`, `tailstate.Mirror` is the same two numbers where `Cycle.write` and
  `Cycle.stoppedRead` can read them off the loop. Every mutator — `Floor`,
  `Ack`, `Settle`, `Stall`, `Resolve` — ends in `publish`, so **moving the tail
  is publishing it**:
  the mirror and the `Metrics.Tail` emission are not a step a call site can
  leave out, which is what they were when four callers each had to remember
  `publish`. Two readers for "is it empty" stay, named apart because which is
  correct depends on whether there is still a loop to ask (`Tail.Empty` on it,
  `Mirror.Empty` after it is gone). `TestTheMirrorFollowsEveryTailMove` drives
  the floor, an append and three of the four settles;
  `TestADrainThatFoldsToNothingStillSettlesWhatItAcked` is the fourth, and the
  stall and its resolve publish too with no test reading the mirror they leave.
  The other direction — a *new* site writing the numbers
  around the mutators — **is a compile error and no longer a test**: the counters
  are unexported fields of an exported type in a package of their own, so
  `s.tail.resolved = 0` does not build in `cycle` at all. It used to be
  `TestNothingOutsideTailGoMovesTheTail`, an AST scan of this package's sources,
  because a *file* is not a privacy boundary in Go and that scan was the only
  place to say so; it could only match the assignment shapes somebody had
  thought of, and `t := &s.tail` walked past it. Making it a package is what
  made it the compiler's job. Note the mirror is a `*tailstate.Mirror` for that
  move: the emitter reaches it through `NewMirror` at construction, where it
  used to be assigned after the literal;
* **the edge between the two is typed, and it is the only one**: `Take` returns
  a `window.Taken` and `Tail.Settle` takes that value and consumes it
  (`Taken.Release`). The bytes the tail releases are therefore always bytes a
  window produced — `Settle(seqno, 0, MoveWatermark)` and any other invented
  number stop compiling — and they are released once, so the six settlement
  branches in `Cycle.drain` cannot subtract one window twice. That second half
  is not fussiness: a tail driven below zero never trips I10 again, which is
  unbounded memory by the road the bound exists to close, and
  `TestASecondSettleOfOneWindowReleasesNothing` is what fails if `Release` stops
  consuming. A take **nobody** settles stays legal and is what the two halt arms
  do deliberately, the entries behind a halted window being acked and the cycle
  finished; Go has no way to make that one a compile error, so it is not one.
  This is why `tailstate` imports `window` at all — for the token, not for the
  window: the two byte counts stay two numbers, and [dependencies.md](dependencies.md)'s
  rule that the tail may not reach `fold` is what keeps them that way — prose
  read when you open the package, and checked by nothing;
* **the window's arithmetic has one owner too, `cycle/window`** — the
  bullet above applied to the other three counters, with two differences.
  Emptying is `Take`, which hands back the bytes the tail goes on holding: a
  step four call sites each had to remember, and two of them (the halt, an
  abandoned replay) zeroed the counters while leaving `oldest` behind. It does
  not report the mutation count, because a window that folded entries can still
  fold to nothing and the batch's own `MutationsIn` is the number a drain
  applied. And it **publishes nothing**, where every move of the tail is a
  publish: the drain's numbers are emitted once the transaction has an outcome,
  so [dependencies.md](dependencies.md) bans `walmetrics` here where `tailstate`
  must hold it.
  `Trips` and `Aged` are separate for replay's reason — it consults the size
  rule and not the age one, which used to live in a comment beside one of two
  inline copies. `s.window.mutations = 0` does not build in `cycle`, and neither
  does the same assignment through a `w := &s.window` — the alias that walked
  past the AST scan the bullet above buried;
* **`Tail.Settle` takes `KeepWatermark`/`MoveWatermark` rather than being written
  three times**: a committed drain moves `applied`, sync mode's answered
  condition failure (#57) and replay's dropped provisional entry do not. That is
  the same rule as the bullet above about the two counters, in the direction
  that strands a recovering owner rather than the one that trips backpressure,
  and it used to live in a comment on the copy that got it right;
* **three rules about the loop's ownership are prose, and two of them still have
  no mechanism**, which is a deliberate ending rather than a gap
  ([no-lint-in-tests.md](no-lint-in-tests.md)). A `*state` is a parameter and
  nothing else — not a result, a field, a channel/map/slice element — and a
  function taking one may contain no `go` statement, because a closure capturing
  it escapes through an environment no position check can see into. That second
  one has the tree's only `go` statement anywhere near the loop, and it is the
  one rule here that stopped being prose: `trim.Trimmer.Drained` takes the
  watermark as a value and starts the goroutine in a package that cannot name a
  `*state` (the trim bullet above). A
  function that assigns `s.st` must publish it to `c.mirroredState` in the same function,
  since every goroutine that is not the loop reads the mirror and `Cycle.write`'s
  pre-queue fast path would go on bounding a halted cycle's tail; `Retire` moves
  the mirror alone and is deliberately outside that one, running with no `*state`
  in hand. None became a type because each has one site today and a wrapper
  around one call site is indirection standing in for a rule — and the scans that
  held them were green on shapes nobody had thought of, which is the whole
  argument for reading this bullet instead;
* **the decisions this package's outcomes turn on are functions of values,
  in `cycle/decide.go`**: what becomes of a read the layer cannot answer
  out of both its sources — one outcome type, `readRoute`, over the five moments
  that observe it (`noCycleRoute`, `loopRoute`, which answers for a running
  cycle, for a stalled one and for a halted one and holds the precedence
  between the last two, `stoppedRoute` and `supersededRoute`, the last two of
  which no cycle can answer for itself) — what a write meets before its append (`writeRefused`: I10's two units and the unresolved drain,
  one rule because the precedence between them is a decision rather than the
  order two calls sit in), the store boundary's translation (`storeError`), the drain's
  attribution (`attribute`) and what a drain's outcome *means* (`settlementOf`,
  which reads `attribute` for the one class that turns on the cause). Four of
  them keep a method beside the call site that supplies the values — the other
  three, `noCycleRoute`, `supersededRoute` and `storeError`, are called from
  `Manager` directly, where their values already are — so the loop reads as it
  did and what a call site can still get wrong is *which* values it hands over.
  This is #174's argument turned on this package — the module that answers for a divergence must be
  judgeable itself — and the attribution rule is why it is not tidiness: its
  window conjunct is **unfalsifiable through a cycle**, since `drainSync` is
  issued at one call site where sync mode's window is one by construction.
  Dropping it (measured) leaves the whole pre-existing suite green, and what it
  buys is a caller told its write failed on entries somebody else wrote, which
  stay in the log marked settled. `decide_test.go` enumerates instead — the
  routing matrix's four rules over state × reader × tail, both units across
  the bound, the recognised and unrecognised errors at each state, every drain
  cause at every window size — and of three deliberate mutations run against it
  two were caught by the tables alone, while the third (the store boundary's
  two checks swapped) was caught by the old tests as well: they are not blind,
  they just cannot enumerate. What stayed on the methods is what a rule may not do:
  `Cycle.writeRefused` emits the refusal's metric, so asking the rule a question
  counts nothing. Two things about the fifth are worth keeping straight, it being
  the one that used to live inside `Cycle.drain`'s body and could therefore only
  be judged by driving a cycle. An unrecognised `apply.Class` halts on the
  **invariant** side, deliberately — the other arm would hand an outcome nobody
  enumerated to the next owner as an ordinary failover. And **which way a
  settlement settles the tail is not part of the value**: that stays one
  statement each in `answer`, `dropProvisional` and the drain's forward path,
  because it is the branch the drain takes rather than a property of the
  settlement. A test claiming otherwise would assert a table against its own
  definition, so `decide_test.go` says that in place of the test, and
  `TestAnUnresolvedDrainStaysInTheTail` is what actually holds it;
* the refusal is checked **before the append**, so a refused write provably
  wrote nothing, and it is `*serviceerror.ResourceExhausted{PERSISTENCE_LIMIT,
  SCOPE_SYSTEM}` **returned unwrapped**: the shard's write path switches on the
  concrete type, so one `%w` turns backpressure into a background re-acquire —
  a self-inflicted failover. `TestTheBackpressureRefusalIsDefinitelyNotCommitted`
  (`internal/verify/guard`) is that claim, with the wrapped copy beside it to show the
  hazard is not theoretical;
* a mutation is **never refused for its own size**: the bound reads the tail as
  it stands, not the tail the mutation would make. The server already accepted
  it under its own 8 MB limit, and a refusal it could only repeat is a stuck
  workflow rather than a degradation — so the tail may overshoot by one entry,
  deliberately;
* `Cycle.write` checks the bound twice — off mirrored atomics before it queues
  anything, and exactly inside the loop. The first is what makes a shard whose
  applier is stuck on a cold store *refuse* its writers instead of parking them
  behind it (`TestTheBoundIsAnsweredWhileTheApplierIsBusy`), which is the same
  unbounded memory by another road. **Only the first may read the mirror**: on
  the loop the tail is exact and the mirror is a drain behind, and both answer in
  the same types, so a function holding a `*state` may not mention it — and
  nothing catches the swap, which is why it is stated twice here. Nothing on the
  loop reads it — with the halt rule reading the mirror the whole of `./...`
  stays green — and the three sites that do are all off it (`Cycle.stoppedRead`,
  `Cycle.residue`, and this fast path);
* the node's budget is a **startup assertion**, which is why `NewManager`
  returns an error: `hard_max × MaxShards` must fit `TailBudgetBytes`, and
  `Defaults()` fits exactly (2 GB over 256 shards is the 8 MB). Its three
  numbers are dynamic-config settings read **once**, when the policy is built,
  and that is what keeps this assertion meaning something — see `waltz.settings`,
  and `Compose`, which opens nothing and reaches nothing, so the refusal is a
  process that does not start rather than one that connected first. It bounds
  encoded bytes and not RSS, and the multiplier is measured — the table this
  tree carries is the handbook's
  [14-where-the-defaults-came-from.md](../../docs/handbook/14-where-the-defaults-came-from.md),
  and it reads **~8.2× and flat**: all six points, a 32× range of tail sizes
  crossed with both locality settings, lie between 8.11 and 8.40. So 8 MB of
  tail is ~68 MB resident and the node's 2 GB is ~17 GB of live heap at the
  bound. That probe caps no workflow pool, so its two locality settings collapse
  1.03 against 1.15 and it shows no locality effect at all, where #50's run on
  the research prototype reached a ratio of 3.30 and a multiplier of 2.94 — so
  quote the number with the collapse ratio it was taken at or not at all, and
  re-measure it rather than carrying it forward, since it was measured on a
  tree, not derived;
* `cycle` may not import a cold store: it *drives* the seam one arrives at,
  which is `cold`'s to state (`cold.Applier`, `cold.Watermarker`), and a package
  holding the log, the accumulator and the write path at once is where a "just
  this once" write would land. The rules
  and their reasoning are in [dependencies.md](dependencies.md); nothing checks
  them. `tailstate` and `window` are listed there separately, because
  that rule is a statement about one package and a sub-package of `cycle` would
  otherwise be covered by nothing. What they forbid is different anyway: the log
  and the accumulator, which are the two things a counter of seqnos and bytes
  would grow reach into first, plus — for `window` alone — the emitter, which is
  what would move a drain's numbers off the outcome that earns them.
