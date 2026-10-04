# The WAL Layer Handbook

waltz puts a durable per-shard write-ahead log in front of a Temporal history shard's cold store. It
acknowledges a write as soon as the log holds it, and later folds many logged mutations into one
transaction against the store. waltz stores nothing itself: a deployment supplies the log (`wal.Log`)
and the cold store (`cold.Store`), and waltz is the logic between them.
[Chapter 01](01-overview.md) explains why that is worth doing and what the early acknowledgement
obliges the layer to do.

## Who this book is for

The book is written for two readers. One operates a Temporal cluster with this layer and needs to
know which knob changes what, what a counter means at three in the morning, and which alerts show
fencing working as intended. The other changes the layer and needs the invariants, seams and suites
that will judge the change.

The book has two parts. Chapters 01–11 are the reference: what is true of the tree as it stands.
Chapters 12–15 are the deep dives: why it was made that way and what it is not. Running the layer
never needs the deep dives; changing it, or re-proposing a refused design, does.

## Reading paths

As a book:

1. [01-overview.md](01-overview.md): one write, and the problems an early acknowledgement creates.
2. [02-concepts-and-invariants.md](02-concepts-and-invariants.md): the vocabulary and the
   invariants.
3. [03-components.md](03-components.md) and [04-contracts.md](04-contracts.md): the packages and
   the seams. Both are reference; skim them on a first read and come back to them from 05–07.
4. [05-write-path.md](05-write-path.md): one mutation, happy path and failures.
5. [06-shard-lifecycle.md](06-shard-lifecycle.md) and [07-read-path.md](07-read-path.md): changing
   owners, and reading.
6. [08-configuration.md](08-configuration.md), [09-operations.md](09-operations.md) and
   [10-metrics.md](10-metrics.md): configuring, running and measuring.
7. [11-verification.md](11-verification.md): what the evidence proves.
8. The deep dives in order: [12](12-the-write-before-the-layer.md) the baseline write,
   [13](13-designs-that-were-rejected.md) the refused alternatives,
   [14](14-where-the-defaults-came-from.md) where each number came from, and
   [15](15-the-limits-of-the-evidence.md) where the evidence stops.

Operating the layer:

1. [09-operations.md](09-operations.md): deployment, start and stop order, the seven runbooks.
2. [08-configuration.md](08-configuration.md): every key and three ready-made configurations.
3. [10-metrics.md](10-metrics.md): every series and the shape of each alert.
4. [06-shard-lifecycle.md](06-shard-lifecycle.md): what a failover does, which halt class is fencing
   working as designed and which is an incident, and what a killed node leaves behind.

Changing the layer:

1. [01-overview.md](01-overview.md), [02-concepts-and-invariants.md](02-concepts-and-invariants.md)
   (the eleven invariants), [03-components.md](03-components.md) (the import bans and why),
   [04-contracts.md](04-contracts.md).
2. [05-write-path.md](05-write-path.md) and [07-read-path.md](07-read-path.md).
3. [11-verification.md](11-verification.md): the suites that will judge the change.
4. [13-designs-that-were-rejected.md](13-designs-that-were-rejected.md): before proposing a change to
   ownership, the log contract, the window, the reads or how the fold is judged.

Changing a default, or questioning a number:

1. [14-where-the-defaults-came-from.md](14-where-the-defaults-came-from.md): each constant's origin,
   derived or chosen.
2. [08-configuration.md](08-configuration.md): what the key does.
3. [12-the-write-before-the-layer.md](12-the-write-before-the-layer.md): the baseline cost of a
   write without the layer.
4. [15-the-limits-of-the-evidence.md](15-the-limits-of-the-evidence.md): what has never been
   measured.

Debugging an incident:

1. [09's decision tree](09-operations.md#4-writes-to-one-shard-are-failing--where-to-go): start from
   the error the caller saw, not the dashboard. It routes into
   [the seven runbooks](09-operations.md#5-runbooks).
2. [10's reference table](10-metrics.md#3-the-reference-table): several series count something
   narrower than their name suggests.
3. [06's halt classes](06-shard-lifecycle.md#5-halts-the-two-classes): which of the two you are
   looking at, and which one nobody else picks up.

## The chapters

| file | title | what it answers |
|---|---|---|
| [01-overview.md](01-overview.md) | The layer at a glance | What waltz is, what one write costs with and without it, its packages, the two meanings of "mode", and what it is not. |
| [02-concepts-and-invariants.md](02-concepts-and-invariants.md) | The geometry of an acknowledged write | Why the log needs three positions, every term the book uses, and the eleven invariants. |
| [03-components.md](03-components.md) | Components: ownership, knowledge and calls | The packages, their import rules, and which goroutine or mutex owns each value. |
| [04-contracts.md](04-contracts.md) | Contracts at the failure boundaries | How three failures shape every seam: an ambiguous append, an unknown drain outcome, a stale owner racing a current one. Then each seam's signatures, guarantees, refusals and caller obligations. |
| [05-write-path.md](05-write-path.md) | A write, end to end | One `UpdateWorkflowExecution`, its drain, and every failure path, ending in a table from what the caller saw to what the operator sees. |
| [06-shard-lifecycle.md](06-shard-lifecycle.md) | When an owner disappears | How a new rangeID becomes a successor cycle that replays the inherited tail, why the two halt classes are opposites, stopping and trim. |
| [07-read-path.md](07-read-path.md) | Reading acknowledged state before it reaches the store | The overlay, merged task and branch pages, the reads that pass through, and how invariant I7 lets a drain skip a task row whose range is already completed. |
| [08-configuration.md](08-configuration.md) | Choosing the operating envelope | How window benefit trades against replay and memory, the two configuration surfaces, every key and default, the budget refusal, and three recipes. |
| [09-operations.md](09-operations.md) | Running, deploying and debugging it | Deployment, start and stop order, failover, the decision tree and runbooks, local development. |
| [10-metrics.md](10-metrics.md) | Every series the layer emits | Every series with its tags, what it counts, derived quantities, alerts, and in-process counters. |
| [11-verification.md](11-verification.md) | How the layer is judged | The log's contract suite and its blind spot, why the cold store's suites are Temporal's, the fold acceptance and its control, the Temporal server booted in the test process, the witness, checker, guards and doubles, the house rules, and what is not claimed. |

### The deep dives

These four carry the reasoning behind the reference. None of it is needed to run the layer; all of
it is needed to change it without re-deciding something already decided.

| file | title | what it answers |
|---|---|---|
| [12-the-write-before-the-layer.md](12-the-write-before-the-layer.md) | What one write cost before the layer | One state transition against a well-built store (adjacent keys, one gated query, history first), immediate versus distributed transactions, why a log on the same database buys no latency, and so why the goal is fewer writes rather than faster ones. |
| [13-designs-that-were-rejected.md](13-designs-that-were-rejected.md) | The designs that were rejected | Four designs for ownership, one for the log contract, three for the window, four for the reads and two for judging the fold, each with the failure that refused it. |
| [14-where-the-defaults-came-from.md](14-where-the-defaults-came-from.md) | Where the defaults came from | The provenance of each shipped constant: the three drain triggers, trim cadence, the two tail bounds, the node budget and its resident cost, and which are derived rather than chosen. |
| [15-the-limits-of-the-evidence.md](15-the-limits-of-the-evidence.md) | The limits of the evidence | What a green run asserts; what is never measured (latency and the two paths event history takes among it), never staged, or not expressible by any external judge; what the two in-memory seams cost; and why the list is not a roadmap. |

The system map, the one diagram of the whole layer, is in
[Chapter 01](01-overview.md#the-system-map).

## Conventions

* Diagrams are mermaid blocks, so they render both on GitHub and in the built site. `npm run check`
  parses every block and resolves every link and anchor; `npm run build` writes the browsable pages
  and `waltz-handbook.html`, a single self-contained file, into `site/`.
* Links into the code are relative (`../../wal/...`) and point at files that exist. Each chapter
  ends with "Where this lives in the code", the files it was written from. Where prose and code
  disagree, the code is right.
* Links between chapters are checked, anchors included, so that renaming a heading cannot silently
  break links into it. Anchors follow GitHub's slug rule, implemented in `slug.mjs` and used by the
  build, so an anchor works on GitHub and in `site/` alike. A heading with a spaced em dash gets two
  hyphens in its slug.
* Chapters 05–10 are procedures: each section is a step, numbered, and cited as §4 inside the
  chapter and by that number from other chapters. The other chapters name their sections after the
  claim.
* No ticket numbers. History lives in commit messages and issues. A number in these pages is a
  measurement or a default, never a citation.
* Every identifier named here exists in the tree: packages, types, methods, config keys, metric
  series, test names.
* Each fact has one home. Owners: 01 the system map, 02 vocabulary and invariants, 03 packages, 04
  contracts, 05 write path, 06 shard lifecycle, 07 reads, 08 configuration, 09 operations, 10
  metrics, 11 verification, 12 the incumbent's write, 13 refused designs, 14 provenance of the
  defaults, 15 the boundary of the claim.
* A measurement is named with what produced it. A number that cannot be reproduced from this tree,
  such as a percentage from one run or a byte count on one machine, is a historical observation, not
  a property of this revision. The shipped backends run in process, so nothing measured over them
  describes a deployment. Every such number is attributed to the research prototype waltz was
  extracted from, each time it appears. [Chapter 14](14-where-the-defaults-came-from.md) applies the
  rule to each default, and [chapter 15](15-the-limits-of-the-evidence.md) lists what stays
  unmeasured because of it.
