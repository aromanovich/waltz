# Running, deploying and debugging it

The first node starts with an empty log. It acquires a shard, appends work, and begins
draining windows. During the next rollout that node is stopped; another node fences the shard and
replays whatever the first left behind. If all goes well, callers notice neither the handoff nor the
fact that some acknowledged state briefly lived outside the cold store.

Operations must keep that story true. Setup must happen before writers appear.
Shutdown should reduce replay work but may never be required for correctness. A normal ownership
loss must be distinguishable from a correctness halt, and two identical `ResourceExhausted` errors
can demand opposite responses depending on whether the tail is full or a transaction outcome is
unknown.

This chapter follows the system through deployment and a rolling restart, turns to incident
diagnosis — the routing tree first, then the runbooks it routes into — and closes with a developer
appendix that nothing before it depends on. Configuration keys are defined in [chapter
08](08-configuration.md) and metric names and tag values in [chapter
10](10-metrics.md#3-the-reference-table); here they appear only as evidence for a decision you have
to make.

---

## 1. Deployment

waltz is a library, so what deploys is a custom `temporal-server` binary somebody writes over it.
That binary adds one decorator to the shipped server's composition: `layer.AbstractFactory(base)`
wraps the abstract data store factory the server would otherwise have used, and the result goes to
`temporal.WithCustomDataStoreFactory`. Everything else — flags, services, authorizer, dynamic
config — keeps upstream's shape, so an existing deployment's command lines carry over.

What that binary owns, and this library does not, is **everything that has to exist before a shard is
acquired**: the log's storage and whatever schema that needs, the cold store's storage and schema,
and the credentials for both. This library ships no setup command and no schema step, because it has
no schema of its own. The log is a `wal.Log` value the binary constructs, and if constructing it
requires a migration, that migration belongs to the log's own deployment.

### The checklist

1. **Write the `wal` section.** It is a two-key map — `sync` and `drain_on_read` — in the `options`
   of the datastore `persistence.defaultStore` names, and writing the section at all is what turns
   intercept mode on. An **absent** section is passthrough; an unknown key inside it is a refusal to
   start, not a fallback to passthrough. Every *number* is a dynamic-config setting under `wal.*`
   instead of a key here. Where the section goes in the config tree, what each key costs when
   mistyped, and the ready-made configurations are
   [08-configuration.md](08-configuration.md#1-where-the-section-goes).

2. **Make the log ready before any node starts.** Whatever that means for the log being deployed —
   a migration, a topic, a set of tables, a quorum that is up. Nothing in this layer creates it and
   nothing verifies it, so a log that is not ready is discovered at the first `Log.Fence` — which
   reaches the operator as a shard the node cannot acquire, and nothing more specific. A binary that
   wants the earlier, louder failure does the check itself, before it composes the layer.

3. **Restart the history services.** Both keys on the strict surface (`sync`, `drain_on_read`) and
   the four start-up dynamic-config settings (`wal.hardMaxEntries`, `wal.hardMaxBytes`,
   `wal.maxShards`, `wal.tailBudgetBytes`) are read while the policy is built, so changing them means
   a restart of the processes that run the history service.

4. **Verify.** Watch `wal_intercepted_writes` become non-zero: it is the series that says traffic is
   going through the log at all. Have the binary log a start-up line naming the mode it composed and
   the window it composed at. The failure that line catches is silent from both ends: a node that
   came up in passthrough under a file asking for intercept looks healthy, and so does a node
   intercepting when nobody meant it to.

---

## 2. Start and stop order

Two rules, and moving either is not a refactor:

* **compose the layer before the server.** `waltz.Compose` runs before `temporal.NewServer`. The
  node-budget assertion is inside `Compose`, so numbers that do not fit stop the binary before it
  starts, rather than appearing as a log line behind a port that is already listening. `Compose`
  opens nothing and dials nothing, so an operator whose numbers do not fit is told so without
  waiting for a connection to any store.
* **drain the layer after the server has stopped.** `Layer.Shutdown(ctx, budget)` is the shutdown
  drain: every shard that still holds a window is applied into the cold store, one transaction per
  shard, in sequence. It must run when the writers are gone, and `temporal.Server.Start` does not
  give you that moment — it returns as soon as the services are up. A `main` therefore waits for its
  own signal, calls `Server.Stop`, and only then calls `Shutdown`. **The cold store must still be
  open at that point**, which is the one ordering constraint the composing binary owns: the server
  closes its own data store factory's client on the way down, so an applier riding that factory
  drains into a closed client. A binary whose applier holds a connection of its own closes it after
  `Shutdown` and not before.

`budget` is the caller's number, and it buys the whole of that time rather than whatever is left of
the context handed in: `Shutdown` detaches from the caller's cancellation (`context.WithoutCancel`)
before it starts the timer. A shutdown drain runs where a context has just been cancelled — that is
what shutdown means — and a drain inheriting that cancellation would return at once, leaving a tail
behind and nothing in the log that says so.

A drain the budget cuts short is **not** data loss: the entries are in the log, acked, and the next
owner replays them. It costs that owner a read loop and a transaction before it serves anything.

One process composes one layer, whatever services it runs. The server calls `NewFactory` once per
service, and each call decorates that service's own data store factory with the same
already-composed `waltz.Layer`, so every service's stores talk to one registry of cycles. A shard's
cycle therefore carries its epoch from the acquire through the writes that follow. Two layers in one
process would be two windows for one shard, each unaware of the other.

Start-up and shutdown, in order:

```mermaid
sequenceDiagram
    participant Ops as operator / init
    participant Main as the custom main
    participant Layer as waltz.Layer
    participant Srv as temporal.Server
    Ops->>Main: start
    Main->>Main: open the log and the cold store
    Main->>Layer: waltz.Compose: budget assert
    Layer-->>Main: layer, or a non-zero exit
    Main->>Srv: NewServer(WithCustomDataStoreFactory(layer.AbstractFactory(base)))
    Main->>Srv: Start
    Srv-->>Main: returns once the services are up
    Ops->>Main: SIGTERM
    Main->>Srv: Stop: every service stops
    Main->>Layer: Shutdown(ctx, budget): drain every window
    Layer->>Layer: Log.Close
    Main->>Main: close whatever it opened
```

How to read this: the arrow into `waltz.Layer` before `NewServer` is the refusal, because a budget
that does not fit ends the process there. The `Shutdown` and `Log.Close` arrows are the drain, and
they sit after `Server.Stop` on purpose; the binary closes what it opened only after that.

---


## 3. Rolling restarts and failover

A graceful stop is an optimisation, not the foundation of recovery. It tries to drain each held
window and thereby leaves less for a successor, but process death can interrupt it at any point.
Correctness instead comes from fencing plus replay: the new owner establishes a newer epoch before
using the tail, then starts above the watermark committed by the old owner.

A shard is owned by one node at a time, fenced by an epoch carried in the shard's rangeID; the
ownership rules are [06-shard-lifecycle.md](06-shard-lifecycle.md).

**When a node goes away**, gracefully or not:

* on a graceful stop the shutdown drain empties as many windows as the budget allows, so the next
  owner usually inherits little or nothing;
* on `kill -9` there is no drain, and the node leaves a **tail**: entries acked into the log and
  not yet applied to the cold store. Nothing is lost.

**What the next owner replays**: it reads the cold store's `appliedSeqno` watermark, then every log
entry above it, a page of `wal.windowMutations` entries at a time. Those entries are folded into a
fresh accumulator and cut into transactions by the same size triggers a live window uses, and the
replay ends in a drain — so the window is empty before the first caller is served. The age trigger
is not consulted, because every entry here is already as old as the incident. What triggers the
replay is the first read or write to reach the shard, and it runs on the cycle's own goroutine, so a
request that arrives mid-replay waits behind it rather than being refused.

**What to expect in the metrics during a failover**:

* `wal_replayed_entries` rises on the new owner — this is the only place it moves, so a failover
  with a flat counter here means the tail was empty;
* `wal_drains` gains `trigger="replay"` observations;
* `wal_halts` gains `state="halted-lost"` on the *old* owner, if it is still alive and tries to
  write. The new owner's `Log.Fence` at the higher epoch cuts off every append below it, so the old
  cycle's next write comes back `wal.ErrFenced` and it halts there, before the entry is acked. A
  cycle that drains without taking a new write — on the age tick, say — reaches the same halt from
  the other side: its transaction fails the cold store's epoch CAS, which classifies as
  `apply.ClassShardLost`. That is fencing working, and it must not page;
* `wal_unapplied_entries` spikes on the new owner and falls back as the replay drains.

During a rolling restart, do the nodes one at a time and let `wal_unapplied_entries` settle before
taking the next one: a node taking over shards while its own tail is still being replayed is the
same load twice.

---

## 4. "Writes to one shard are failing" — where to go

Incident response begins with what the caller observed, not with a graph that happens to be open.
Route the symptom first, then read the runbook the route names.

```mermaid
flowchart TD
    A["writes to one shard fail"] --> B{"error type?"}
    B -->|"ResourceExhausted PERSISTENCE_LIMIT"| C{"wal_backpressure_refusals limit tag"}
    C -->|"entries or bytes"| D["runbook (a): backpressure"]
    C -->|"unresolved"| E["the applier cannot read its last drain: runbook (a)"]
    B -->|"ShardOwnershipLost"| F["wal_halts state=halted-lost: normal failover, runbook (b)"]
    B -->|"refusal matching ErrHalted"| G["wal_halts state=halted-invariant: page, runbook (b)"]
    B -->|"condition failure"| H["expected traffic: answered before the append, not a layer fault"]
    B -->|"none of these"| I["the cold store, or the log, on its own"]
    D --> J{"wal_unapplied_entries rising?"}
    J -->|"yes"| K["runbook (c): cold store behind"]
    J -->|"no"| L["one shard is hot: check the window keys"]
```

How to read this: start from the error the *caller* saw, not from the dashboard. The two
`ResourceExhausted` branches differ by the `limit` tag on `wal_backpressure_refusals`: `entries`
and `bytes` are I10's size bound, while `unresolved` is the applier being blind rather than behind —
it cannot say whether its last drain committed, so nothing may be applied over it, and no size knob
will clear it.

---

## 5. Runbooks

Seven of them, each explaining the mechanism behind a symptom the tree above has already routed.
Each names the metric series and the configuration key involved. The series are
[10-metrics.md](10-metrics.md#3-the-reference-table), including the alert shape for each of the
conditions below; the keys are
[08-configuration.md](08-configuration.md#3-table-2--the-nine-dynamic-config-settings).

### (a) A shard stopped accepting writes — backpressure or an unresolved drain

* **Symptom.** Callers get `serviceerror.ResourceExhausted` with cause `PERSISTENCE_LIMIT` and
  scope `SYSTEM`. Read the `limit` tag on `wal_backpressure_refusals` before choosing the response.
* **What `entries` or `bytes` means.** Invariant
  [I10](02-concepts-and-invariants.md#the-invariants)'s per-shard bound is doing its job: the tail —
  acked and not yet settled — reached `wal.hardMaxEntries` or `wal.hardMaxBytes`, so the shard
  refuses new writes rather than growing a tail it cannot discharge. The error says `shard N's WAL
  tail is at its limit (…/… entries, …/… bytes): the apply cycle is behind`.
* **What to check for `entries` or `bytes`.** `wal_unapplied_entries` (is the applier behind?),
  `wal_drains` (are drains committing at all?), `wal_halts`, and the cold store's health — this is
  nearly always a cold-store problem rather than a layer one.
* **What to do for `entries` or `bytes`.** Fix the cold store. Refusals stop by themselves once the
  applier catches up. Raising `wal.hardMaxEntries`/`wal.hardMaxBytes` needs a restart, and it is
  arithmetic rather than taste: `hardMaxBytes × maxShards` must fit in `wal.tailBudgetBytes` or the
  node refuses to boot. That budget counts encoded bytes, not resident heap. Measure the
  decoded-memory multiplier for your own workload before raising it — the multiplier in
  [chapter 14](14-where-the-defaults-came-from.md#what-the-budget-costs-resident) was measured on
  another one.
* **What `unresolved` means.** The last drain returned an unknown outcome and the cycle could not
  read `appliedSeqno`, its only witness to whether the transaction committed. The error names the
  seqno it is stuck on: `shard N's apply cycle cannot read the outcome of its drain at seqno S, and
  takes no writes until it can`. This is a stalled tail, not a halt: writers and readers are both
  refused so that nothing can be applied over an ambiguous transaction. No size knob clears it, and
  it is refused ahead of a full tail, since waiting will not clear it either.
* **What to do for `unresolved`.** Restore reads of the cold store's `appliedSeqno` watermark — the
  seqno the drain's own transaction carried, read back through `cold.Watermarker`. The age tick
  re-reads it without operator intervention: a watermark exactly at the drain's seqno
  releases the stall and traffic resumes; a watermark below it proves the drain did not commit and
  turns the shard into `halted-invariant`, at which point follow runbook (b); one past it proves
  another owner has been draining this shard, and the cycle halts `halted-lost`, which is a failover
  and not an incident. If the watermark
  remains unreadable, retain the log and the original drain error and escalate the storage failure.

No refused call in this runbook wrote anything: all three refusals — `entries`, `bytes` and
`unresolved` — are decided before the append. The history node's handling of `PERSISTENCE_LIMIT`
keeps the shard loaded and slows its queues instead of DLQ-ing tasks.

Nothing has to be reconciled afterwards either. A refused write is retried at the version it was
refused at: the caller was told "no", so it still holds the row it read and its assertion still
stands. Once the applier catches up, a replay reads back exactly the acknowledged set — in order,
gap-free, with none of the refusals in it. That is the concrete reason `ResourceExhausted` is the
right answer here and a condition failure is not.

I10 bounds what a slow cold store can cost you; it does not make the two halves independent. If the
log lives in the same database as the cold store, a database-wide incident takes out both at once,
and there is no window in which backpressure is the interesting behaviour at all. Putting the log
where the cold store cannot take it down is a deployment choice, and it is what the contract in
[04-contracts.md](04-contracts.md#what-the-contract-does-not-say-what-an-append-costs) and the
import ban in [03-components.md](03-components.md) exist to allow.

### (b) A shard halted — and which of the two classes

* **Symptom.** `wal_halts` moved. The refusal callers get is the cheapest way to tell the two classes
  apart: `ShardOwnershipLost` under `halted-lost`, and the halt's own error — which matches
  `cycle.ErrHalted` and carries the cause wrapped inside it — under `halted-invariant`.
* **What it means — read the `state` tag, and never sum the two:**
  * `state="halted-lost"` — the shard was fenced away. This is fencing working: the halted cycle
    discards its *window*, the acked entries stay in the log, nothing is trimmed, and the next owner
    replays them. Writes come back as `ShardOwnershipLost`, which the server handles by re-acquiring.
    Expect it on every failover and on every rolling restart. **Not an alert.**
  * `state="halted-invariant"` — an assertion failed inside a window whose failure could not be
    pinned on one caller. There is no retry and no failover: the layer deliberately does not convert
    this into an ownership-lost, because handing a divergence to the next owner as an ordinary
    failover would spread it. **This is the one that pages.**
* **What to check.** For `halted-invariant`, the `apply cycle halted` log line. It carries the shard
  id, the state and the cause, and the cause is the only thing that says which assertion failed.
  Several roads lead here. The ones you will see: an `apply.InvariantViolationError` from a drain;
  `cycle.ErrTailNotEmpty` — "the log holds an entry at a seqno this cycle replayed past", which is
  either a second writer holding this cycle's own epoch or one of this cycle's own appends that
  failed ambiguously and was durable after all; a decode failure, a seqno gap or a shard-id mismatch
  while replaying the tail; a drain whose outcome was unreadable and which a later watermark then
  proved had not committed; an acked entry this cycle could not fold at all; and any apply class
  nobody enumerated. The metrics cannot narrow it further:
  none of the series here carry a shard tag ([chapter 10](10-metrics.md#2-three-shape-decisions-because-they-change-how-you-read-the-numbers)),
  so one shard's state comes from the log lines and from `waltz.Layer.ShardStats(shard)`.
* **What to do.** `halted-lost`: nothing. `halted-invariant`: capture the shard's log before
  anything trims it (a halted cycle's log is not its own to shorten, so it will still be there), and
  treat it as a correctness incident.
* **There is no path back.** No tool, no supported edit and no documented procedure returns a
  `halted-invariant` shard to service. `Cycle.State` is terminal: for as long as that cycle exists
  it refuses every write. A cycle that halted this way with an empty tail still passes both
  mutable-state and task reads through to the cold store; one that halted holding a tail refuses
  every routed read, and it will never drain that tail — a halted cycle does not drain, so only a
  fresh cycle at a higher epoch clears it. Nothing re-acquires the shard on its own, because the
  halt is deliberately not an ownership loss. The halt is not durable, though. Its state is in
  memory, so a process restart — or any acquire at a strictly greater epoch — installs a fresh
  cycle, which reads the watermark and replays the same tail. Whether the shard writes again then
  depends on what diverged: an ambiguous apply outcome need not recur on the replay, while a
  genuine disagreement between what the layer folded and what the store holds is met again by the
  replaying cycle and halts again. The log survives either way, which is why capturing it comes
  first: that capture is what you decide on, and deciding whether to restart at all is the whole
  of what the layer offers here.

### (c) The cold store is falling behind

* **Symptom.** `wal_unapplied_entries` climbing and not returning; `wal_window_age` climbing.
* **What it means.** Entries are being acked into the log faster than drains commit them. The
  layer is absorbing an incident, which is what it is for — but the runway is bounded by
  `wal.hardMaxEntries` / `wal.hardMaxBytes`, and past it you get runbook (a).
* **What to check.** `wal_drains` by `trigger` — a healthy node drains on `trigger="mutations"` or
  `trigger="bytes"`; a node draining almost only on `trigger="age"` is idle rather than behind.
  `wal_drained_mutations` against `wal_drained_workflows` gives the collapse ratio: if it is near 1,
  the batches are not collapsing and the drains are as expensive as the writes.
* **One shape that is not a slow cold store.** A completed task range travels the log like any other
  write, so its delete runs inside the drain's single transaction. A standalone
  `RangeCompleteHistoryTasks` is free to split a very large range across several statements; a
  folded one is not, so a range big enough to trip a limit of the cold store's own fails the *whole
  drain* that carried it rather than degrading. It arrives as an ordinary apply error and no series
  distinguishes it, so read the drain's logged error. The mechanism is
  [05-write-path.md](05-write-path.md).
* **What to do.** Address the cold store. If the shard is simply hot, `wal.windowMutations` and
  `wal.windowBytes` are read at the decision, so they can be moved without a restart — but lowering
  them gives collapse away, and raising them holds more unapplied work per shard.

### (d) Trims are failing

* **Symptom.** `wal_trims` with `outcome="failed"` — the counter's other value is
  `outcome="started"`.
* **What it means.** The trim is the lazy deletion of log entries below the applied watermark. It
  runs beside the apply cycle, not in it, and a failed trim is logged, retried at the next cadence,
  and **halts nothing**. This counter is the only place a failing trim is visible.
* **What to check.** Whether it is failing on every cadence or only occasionally. Trimming is part
  of the latency budget rather than hygiene: a backend's reads get dearer as its log gets longer, so
  a permanently failing trim degrades the layer's latency over hours rather than minutes. How much
  dearer is the log implementation's business, not this layer's.
* **What to do.** The cadence knobs are `wal.trimEvery` (in drains) and `wal.trimAfter` (in time),
  whichever trips first; both are read at the decision, so no restart. Note that raising
  `wal.trimEvery` alone does not keep a log around for a post-mortem — `wal.trimAfter` fires anyway.
* **How much log is left to read is computable.** The cycle trims to the applied watermark with no
  safety lag, so what survives is bounded by `wal.trimEvery` × `wal.windowMutations` entries plus
  whatever the tail currently holds — 4096 entries at the shipped defaults, however long the shard
  has been running, and less on a low-traffic shard where the age trigger fires first. Where those
  two numbers came from is
  [14-where-the-defaults-came-from.md](14-where-the-defaults-came-from.md#the-trim-cadence-16-drains-or-60-seconds).

### (e) Task drops are climbing

* **Symptom.** `wal_dropped_tasks` rising against `wal_written_tasks`, both tagged by task
  category.
* **What it means.** Invariant [I7](02-concepts-and-invariants.md#the-invariants): a committed drain
  did not write task rows whose range the queue had already completed past. It is expected traffic,
  and the two counters are emitted separately on purpose — a pre-divided share cannot tell
  "everything was dropped" from "there were no tasks".
* **What to check.** The ratio per category. Immediate and scheduled categories drop at unrelated
  rates, so compare each against itself over time rather than against the other. A drop rate that
  jumps at the same moment the window grew is the window, not a bug.
* **What to do.** Nothing, in the usual case: a drop is a task row the queue had already completed
  past, so the share climbing is the saving growing. If it has to come down, the quantity to move is
  **drains per queue checkpoint** — more of them, a smaller share — and there are two knobs for it,
  in this order:
  1. the server's own `history.*ProcessorUpdateAckInterval`, **raised**: the queue checkpoints less
     often, so more drains fall between two of them. The cost lands on how fresh those checkpoints
     are and not on the layer's collapse ratio, so it is the cheaper of the two. The ratio and its
     shipped anchor are
     [chapter 07](07-read-path.md#5-invariant-i7--the-tasks-a-drain-does-not-write);
  2. only then the window — `wal.windowMutations` / `wal.windowBytes` / `wal.windowAge`, all read at
     the decision — **shortened**, so fewer tasks sit in a window long enough for their range to be
     completed under them. This one is paid for in the collapse ratio, which is the layer's reason to
     be there; shortening the window to lower a share that costs nothing is a bad trade made twice.

### (f) Merged-page collisions are non-zero

* **Symptom.** `wal_merged_task_collisions` is anything but zero.
* **What it means.** A merged `GetHistoryTasks` page found the same task key in both the window and
  the cold store. The two sources are disjoint by construction — the window drops a task exactly
  when the drain carrying it commits — so **any** non-zero value means something is wrong: a second
  writer for the shard, a drain whose window release did not happen, or a merge reading a stale
  window.
* **What to check.** `wal_merged_task_pages` (are pages being routed at all?) and `wal_halts`. Both
  are node-wide — no series here carries a shard tag — so to get to a shard, read the log lines and
  `waltz.Layer.ShardStats(shard)`, and ask whether two processes could be holding it: the acquire
  path and the epoch fence are [`../../wrapper/shard_store.go`](../../wrapper/shard_store.go).
* **What to do.** Treat it as a correctness incident, like `halted-invariant`. There is no knob; the
  merge itself is [`../../fold/taskpage.go`](../../fold/taskpage.go) and
  [`../../cycle/tasks.go`](../../cycle/tasks.go).

### (g) The node refuses to start

Three distinct refusals, all before anything listens:

* **Budget refusal.** `wal.hardMaxBytes × wal.maxShards` must fit in `wal.tailBudgetBytes`.
  `waltz.Compose` asserts that before it builds anything — `cycle.Config.CheckBudget`, reached
  through `cycle.NewManager` — and the error wraps `cycle.ErrBudget` and spells the arithmetic out
  with the node's own numbers: *N shards × B bytes is that many bytes of tail, over the node's
  budget of T*. `Compose` opens nothing, so this refusal costs no connection and no round trip. Fix
  the arithmetic; all three settings are read once at start-up, so all three need a restart anyway.
* **A log that will not open.** This one is the composing binary's, not the layer's: `Compose` takes
  a `wal.Log` that already exists, so a log that cannot be constructed is a refusal in the `main`
  before the layer is reached, and its message is that binary's to write.
* **Moved or unknown config key.** Each of the nine `wal.*` dynamic-config settings has a legacy
  spelling as a key of the section, and writing that spelling is refused **by name**: the message
  says which setting to write instead, and whether it is read at each decision or once at start-up.
  Any other unrecognised key in the section is refused by the strict decoder, so `snyc: true` stops
  the node rather than leaving it quietly running the mode nobody asked for. The numbers do not
  behave this way: a misspelt *dynamic-config* key is a warning, and the default stands.

A fourth failure used to belong here and no longer does: **a drain landing in one cold store while
the watermark is read from another.** It is worth knowing because the symptom is unlike the other
three — the composition succeeds, ordinary traffic is fine, and it goes wrong only when a drain's
outcome is unknown. The cycle then asks how far the applier's transaction got; the store being asked
never saw that transaction, so the watermark comes back below the drain's seqno, which is exactly the
proof that the drain did not commit. The shard halts `halted-invariant` over a drain that had
written. `Backends.Cold` is one field of one interface (`cold.Store`) because of it, so a binary
composing through `waltz.Compose` cannot express the mistake. A binary standing up a
`cycle.Manager` directly still can: `cycle.Deps` keeps `Writer` and `Recoverer` apart for the suites,
and anything filling them by hand fills both from one value.

---

## 6. Local development

The last two sections are a developer appendix, and nothing above depends on them.

Everything in this repository runs with nothing installed:

```bash
go test ./...
```

No cluster, no container, no port to configure, no cgo, no fixture directory. That falls out of
every backend living in the test process: the log is `wal/memwal`, and the cold store and the base
store are both `cold/memcold` — Temporal's own SQL persistence over an in-memory SQLite database,
through the pure-Go `modernc.org/sqlite` driver. When a suite has to make one of those two
misbehave it reaches for the doubles in `internal/verify/coldtest` and `internal/verify/basetest`.
So there is nothing to connect to and nothing to wait for, and `internal/verify/e2e` starts four
Temporal services on OS-assigned ports on the same terms.

Two things worth knowing about that, both of which are limits rather than features:

* **a green run says nothing about a real deployment's storage**, and cannot: everything here dies
  with the process. What each suite does claim is [11-verification.md](11-verification.md); the
  boundary of the whole set is
  [15-the-limits-of-the-evidence.md](15-the-limits-of-the-evidence.md).
* **the widest evidence available to a composition over this library is upstream's own functional
  suites**, run against the deployment's real store with waltz between. That is not a target here,
  because it needs a store worth running them against. What it takes is one patch —
  `patches/temporal/0001-custom-persistence-test-base-factory.patch`, fifteen lines against
  `tests/testcore/test_cluster.go` — and `patches/README.md` is the recipe.
  `internal/verify/e2e` is the in-tree version of the same idea at a fraction of the coverage: one
  server, one workflow, no installation.

`go vet ./...` and `golangci-lint run` are the other two, and `.golangci.yml` says which linters are
deliberately off and why — a check switched off in silence is one somebody re-enables and then
disables again.

---

## 7. Traps

* **A piped test run can be killed by SIGPIPE and read as a suite that passed.**
  `go test ./... | grep … | head -5` ends the test binary the moment `head` is satisfied, and the
  truncated output looks exactly like a green run. Redirect to a file and read the file. The same
  hazard bites `until cmd | grep -q marker` under `set -o pipefail`: `grep -q` exits on the first
  match and SIGPIPEs the writer, so the loop never succeeds. Capture into a variable and match that.

* **A run under `-race` costs memory per test binary, and `go test` runs several binaries at once.**
  The suites here stand up whole compositions in process, and `-p` defaults to the number of cores,
  so a many-core machine runs that many of them side by side and the kernel kills one. You will see
  `signal: killed` with no `--- FAIL` line anywhere, which reads like a hang rather than like a
  resource limit. Lower `-p` until the run fits.

---


## Where this lives in the code

* [`../../waltz.go`](../../waltz.go) — `Compose`, which reaches the budget assertion through
  `cycle.NewManager`, and `Layer.Shutdown`; the package doc states the lifecycle bracket.
* [`../../settings.go`](../../settings.go) — the nine `wal.*` dynamic-config
  settings, which are live and which are read once, and the refusal for a key that moved.
* [`../../config.go`](../../config.go) — the section's strict decoder and the miscased-section
  refusal.
* [`../../walmetrics/walmetrics.go`](../../walmetrics/walmetrics.go) — every series and every
  tag value named in the runbooks above.
* [`../../cycle/decide.go`](../../cycle/decide.go) — the backpressure refusal, its exact
  message and its unwrapped error type.
* [`../../cycle/cycle.go`](../../cycle/cycle.go) — the halted state and its terminality, the
  refusal every call gets while it stands, and the trim a halted cycle does not do.
* [`../../cycle/trim/trim.go`](../../cycle/trim/trim.go) — the trim's cadence, budget and outcome
  counters.
* [`../../patches/README.md`](../../patches/README.md) — the one patch in this repository, what it
  is for, and why it is not needed to build anything here.
