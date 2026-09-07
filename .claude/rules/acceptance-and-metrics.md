---
paths:
  - "verify/acceptance/**"
  - "verify/witness/**"
  - "walmetrics/**"
---

# This repo: the acceptance, the witness and the metrics

`verify/acceptance/` is one stream of a generated corpus driven end to end
through the fold, at volume, with no cluster and no store;
`verify/witness/` is what a run says about the layer's **own** counters; and
`walmetrics/` is what those counters go out as. What to know:

* **the acceptance's claim is narrow and stated in its own counters**: a stream
  of ~10^5 mutations folds without refusing anything the drain-and-retry contract
  does not cover, at a collapse ratio that is a function of the generator's reuse
  knobs. `WAL_ACCEPTANCE_MUTATIONS` moves the length. What it does **not** claim
  is that a server works, that a cold store accepts the batches, or anything
  about cost — those need a deployment, and the handbook's
  [15-the-limits-of-the-evidence.md](../../docs/handbook/15-the-limits-of-the-evidence.md)
  is where that boundary is written down;
* **the collapse ratio is meaningless without the knob it was measured at.** A
  stream that never re-touches a workflow reports 1.00, and so would a fold that
  collapsed nothing — which is why the run at `WorkflowReuse` 0 is beside the
  default one rather than instead of it: it *must* report 1.00, and if it ever
  collapses, either the knob or the report is lying;
* **a green run over a layer is not evidence that anything went through it**,
  and the failure is silent in both directions: a composition whose layer came
  out empty *is* passthrough, and somebody else's suites are green over it; a
  method that starts transiting into the log when it should not leaves them just
  as green. So a run that composes a layer ends with a **witness** on what the
  layer saw. Verified by breaking it on purpose: dropping the writes from a
  composition leaves every functional test green and fails only the witness;
* **the witness is one judged module and everything in it is a pure function of
  values.** A run states what it was supposed to be (`Expect` — the window, and
  what its callers drove) and hands over what its instruments saw (`Observed` —
  `cycle.Totals` required, the store's counts and the metric emissions optional),
  so a run with a capture handler and one without make the same claims. That is
  the rule "the module whose job is to catch a silent pass must be judgeable
  itself" applied to the witness rather than to one caller of it: an import that
  needed a cluster would end it in the way nothing notices, since **the tests
  would still pass**;
* **each claim has a name and its own row** in `layerClaims`/`controlClaims`, and
  `TestEachClaimHasADefectOnlyItCatches` is the leave-one-out over them — a claim
  no defect reaches exclusively is redundant or unreachable, and both look like a
  witness working. A new claim is a row there and a row in that table; a failure
  reports under its name, so a red run says which half of the layer went missing;
* **the witness's two central claims invert between the modes, and neither
  reading is the other's default**: in sync mode `ReadsHeld == 0` and
  `TailEntries == 0` at the end — the claim the "no replay needed" boundary rests
  on — and in a windowed mode both must be non-zero, plus a
  `wal_drained_mutations` above 1, which is the only number that says a drain
  carried a batch. A windowed run reporting sync mode's numbers *is* sync mode.
  `TailEntries` is **not** `CommitSeqno − AppliedSeqno` in either — sync mode's
  expected condition failures legitimately leave those apart, and merging them
  is the same bug `cycle`'s own notes warn about;
* the store's counter (`Counts.Overlaid`, `wal_overlaid_reads`) counts reads
  *routed* rather than reads answered from the window — a counter that only
  fired on a hit would read zero on an idle cluster and zero on a layer wired up
  wrong. The same pair exists for task pages (`Counts.TaskReads`,
  `wal_merged_task_pages`), plus the direction the overlay's half does not need:
  a run that reads tasks must have *routed* those reads at the merge, since a
  caller reading them around the layer is as green as one reading them through
  it;
* metrics go through the **server's own handler**, which arrives at
  `NewFactory` — *later* than the layer is composed, which is the whole cost of
  the hand-off. The rejected alternative (a main building its own from the same
  config) is a second Prometheus listener on one address;
* three shapes are decisions, not style: per-shard quantities are
  **distributions, not gauges** (a gauge with no shard tag reports whichever
  shard recorded last, and a shard tag is a cardinality class upstream does not
  have anywhere); the collapse ratio is **two counters**, since a metrics stack
  has nowhere to put the locality the rule says never to omit, and the
  history-task drop is two for a second reason as well — the denominator moves
  with the drop, so a pre-divided share cannot tell "everything was dropped" from
  "there were no tasks"; and a per-log counter is **deliberately absent**,
  because every node writing one log contributes to the same number and the log
  is not this library's to instrument;
* `walmetrics` may import nothing that can be measured — not `fold`, not
  `cycle`, not `mutation` ([dependencies.md](dependencies.md)). The hazard is
  specific: a metric is easiest to add where the number already is, and one
  import of fold puts "just read Stats" one line away from the component under
  test;
* `cycle/metrics_test.go` and `wrapper/metrics_test.go` are where the emissions
  are judged, and a witness over a composed run is the only place the whole path
  runs at once. Two breaks were tried: a constant drain trigger (caught), and
  emitting the tail for both tail series (**not** caught until a sync-mode
  condition failure was added, which is the one moment the two numbers differ).
