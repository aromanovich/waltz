# The WAL Layer Handbook

waltz puts a durable per-shard write-ahead log in front of a Temporal history shard's cold store. It
acknowledges a write as soon as the log holds it and, in the windowed configuration a deployment
runs, later folds many logged mutations into one transaction against the store. Until that
transaction the store is behind what the caller was told is durable, so reads must merge the log in,
a new owner must replay what its predecessor left, and the layer must refuse writes before that tail
outgrows the node. waltz stores nothing itself: a deployment supplies the log (`wal.Log`) and the
cold store (`cold.Store`), and waltz is the logic between them. [Chapter 01](01-overview.md) explains
why that is worth doing.

## Who this book is for

Two readers. One operates a Temporal cluster with this layer and needs to know which knob changes
what, what a counter means at three in the morning, and which alerts show fencing working as
intended. The other changes the layer and needs the invariants, seams and suites that will judge
the change.

Chapters 01–11 are the reference: what is true of the tree as it stands. Chapters 12–15 are the
deep dives: why it was made that way and what it is not. Running the layer never needs them;
changing it, or re-proposing a refused design, does.

## Reading paths

As a book: 01–15 in order. Skim [03](03-components.md) and [04](04-contracts.md) on a first read;
05–07 link back into them when you need them.

Operating: [09](09-operations.md), [08](08-configuration.md) and [10](10-metrics.md), then
[06](06-shard-lifecycle.md) for what a failover does and what a killed node leaves behind.

Changing: [01](01-overview.md)–[04](04-contracts.md) (the two switches called "mode" in 01, the
eleven invariants in 02, the import bans in 03), [05](05-write-path.md), [07](07-read-path.md), and
[11](11-verification.md) for the suites that will judge the change. Read
[13](13-designs-that-were-rejected.md) before proposing a change to ownership, the log contract,
the window, the reads or how the fold is judged. To change a default: [14](14-where-the-defaults-came-from.md) for its origin,
[08](08-configuration.md) for the key, [12](12-the-write-before-the-layer.md) for the baseline write,
[15](15-the-limits-of-the-evidence.md) for what was never measured.

Debugging an incident:

1. [09's decision tree](09-operations.md#4-writes-to-one-shard-are-failing--where-to-go): start from
   the error the caller saw, not the dashboard. It routes into
   [the seven runbooks](09-operations.md#5-runbooks).
2. [10's reference table](10-metrics.md#3-the-reference-table): several series count something
   narrower than their name suggests.
3. [06's halt classes](06-shard-lifecycle.md#5-halts-the-two-classes): which one is fencing working
   as designed, and which is an incident nobody else picks up.

## The chapters

| file | title | what it answers |
|---|---|---|
| [01-overview.md](01-overview.md) | The layer at a glance | What waltz is, what one write costs with and without it, [the system map](01-overview.md#the-system-map), [the two switches called "mode"](01-overview.md#what-mode-names-here), and what it is not. |
| [02-concepts-and-invariants.md](02-concepts-and-invariants.md) | The geometry of an acknowledged write | The three log positions, every term the book uses, and the eleven invariants. |
| [03-components.md](03-components.md) | Components: ownership, knowledge and calls | The packages, their import rules, and who owns each value. |
| [04-contracts.md](04-contracts.md) | Contracts at the failure boundaries | The three failures every seam is shaped by, and each seam's signatures, guarantees, refusals and caller obligations. |
| [05-write-path.md](05-write-path.md) | A write, end to end | One mutation, its drain, every failure path, and what the caller and the operator each see. |
| [06-shard-lifecycle.md](06-shard-lifecycle.md) | When an owner disappears | Ownership changes, replay of the inherited tail, the two halt classes, stopping and trim. |
| [07-read-path.md](07-read-path.md) | Reading acknowledged state before it reaches the store | The overlay, merged reads, the reads that pass through, and invariant I7, which lets a drain skip completed task ranges. |
| [08-configuration.md](08-configuration.md) | Choosing the operating envelope | Window benefit against replay and memory, both configuration surfaces, every key and default, the budget refusal, three recipes. |
| [09-operations.md](09-operations.md) | Running, deploying and debugging it | Deployment, start and stop order, failover, the decision tree, runbooks, local development. |
| [10-metrics.md](10-metrics.md) | Every series the layer emits | Every series and its tags, derived quantities, alerts, in-process counters. |
| [11-verification.md](11-verification.md) | How the layer is judged | The suites, witness, checker, guards and doubles, the house rules, and what is not claimed. |

### The deep dives

| file | title | what it answers |
|---|---|---|
| [12-the-write-before-the-layer.md](12-the-write-before-the-layer.md) | What one write cost before the layer | A state transition against a well-built store, and why the goal is fewer writes rather than faster ones. |
| [13-designs-that-were-rejected.md](13-designs-that-were-rejected.md) | The designs that were rejected | Fourteen refused designs for ownership, the log contract, the window, the reads and judging the fold, each with the failure that refused it. |
| [14-where-the-defaults-came-from.md](14-where-the-defaults-came-from.md) | Where the defaults came from | Each shipped constant's provenance, and which are derived rather than chosen. |
| [15-the-limits-of-the-evidence.md](15-the-limits-of-the-evidence.md) | The limits of the evidence | What a green run asserts, what is never measured, staged or expressible, and why the list is not a roadmap. |

## Conventions

* Diagrams are mermaid blocks. `npm run check` parses every block and resolves every link and
  anchor; `npm run build` writes the pages and `waltz-handbook.html`, a single self-contained file,
  into `site/`.
* Code links are relative (`../../wal/...`). Each chapter ends with "Where this lives in the code",
  its source files. Where prose and code disagree, the code is right.
* Anchors follow GitHub's slug rule (`slug.mjs`, used by the build). A heading with a spaced em
  dash gets two hyphens in its slug.
* Chapters 05–10 are procedures: each section is a numbered step, cited as §4 inside the chapter and
  by that number from others. The other chapters name sections after the claim.
* No ticket numbers: a number here is a measurement or a default, never a citation.
* Every identifier named exists in the tree: packages, types, methods, config keys, metric series,
  test names.
* Each fact has one home, the chapter whose row above names its subject (01 also owns the system
  map). Another chapter that needs it gives one sentence and a link, never a second explanation.
* A measurement is named with what produced it. The shipped backends (`wal/memwal`, `cold/memcold`)
  run in process, so nothing measured over them describes a deployment. A number not reproducible
  from this tree is a historical observation, attributed each time to the research prototype waltz
  was extracted from.
