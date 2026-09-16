# 9. The tree separates the layer from what judges it

Date: 2026-08-03

## Status

Accepted. Settles the repository layout.

## Context

The packages had sat flat, beside a root package of ~13k lines holding the acceptance, the
fixtures, the probes, the guards and three regressions of somebody else's bugs, all in one
namespace. Nothing in the hierarchy said which package runs in production and which exists to judge
what does: `fold` and `mutgen` looked alike from the tree, and a new test had nowhere obvious to
live except the root.

That layout was not designed; it accreted, one addition at a time, and each addition was locally
reasonable. The cost is paid by whoever reads the repository next.

## Decision

Two groups:

* **the module root** — everything that runs in production: `wal/` (the contract, the in-memory
  backend and the conformance suite), `mutation/`, `fold/`, `apply/`, `cycle/`, `wrapper/`,
  `walmetrics/`, `baserow/`, and the root package `waltz` itself.
* **`internal/verify/`** — everything that judges it and never runs in production: `mutgen/`, `mutbuild/`,
  `drive/`, `foldrun/`, `coldtest/`, `basetest/`, `coldtasks/`, `acceptance/`, `checker/`,
  `witness/`, `guard/`.

Two rules say what the split means: **no non-test file outside `internal/verify/` may import `internal/verify/**`**,
and **the root package is the front door** — `Compose`, the configuration, the settings and
`AbstractFactory`, and nothing else. They are stated in
[`.claude/rules/dependencies.md`](../../.claude/rules/dependencies.md) and no test enforces them
(see the amendment below).

## Consequences

**The dependency order is documented, not nested.** `fold → cycle → apply → wrapper → waltz` is the
direction the layer reads in, and nesting cannot express it: `cycle` imports `fold`, but `fold`
stands alone, so putting it inside `cycle` would be a lie. The handbook's
[03-components.md](../handbook/03-components.md) carries the order. This is the one place where the
tree's legibility rests on prose.

**The root package holds Go files, and that is the whole point of it.** It is not an index: it is
the one composition (`Compose`), the `wal` section, the dynamic-config settings and the door out to
`temporal.WithCustomDataStoreFactory`. A caller of this library writes `waltz.` and stops — which is
what makes "the layer packages are at the root" a decision rather than a leftover, since a
`waltz/layer/cycle` would put a segment on every import path and buy a reader nothing.

**The shared test support is a set of packages, and their APIs are wide.** `internal/verify/mutgen`,
`internal/verify/drive`, `internal/verify/coldtest` and the rest are packages rather than `_test.go` files because
several other packages import them. That width is not incidental — it measures how entangled the
tests were, since a single package let everything reach into one fixture type for free — and it is
the honest price of the split rather than a design to be admired.

**The layout became one of the things the dependency rules were about**, alongside the imports —
until the amendment below removed the check that made either a failure.

## Considered and not taken: `internal/`

The conventional answer for a repository nothing imports, and the one a reader of Go layout advice
would expect. Two facts rule it out. [ADR 0002](0002-wal-contract-is-backend-independent.md)
promises the `wal` contract *together with its conformance suite* to the author of a second backend,
who is by definition outside this module — and `wal/waltest` cannot live under `internal/` at all.
The same is true of `verify/coldtest`: a caller writing a `cold.Applier` over their own store needs
a double to test the seam against.

What is left for `internal/` to buy is protection against an external importer of `fold` or `cycle`,
who is welcome, at the price of a segment on every import path and of two packages sitting outside
the pattern for reasons a reader would have to look up. The rules already state something finer than
`internal/` can express.

## Considered and not taken: the tests somewhere other than beside the code

Every package's tests sit beside it, which is what idiomatic Go asks for, and it is available here
because nothing in this repository needs a cluster: the cold store is `internal/verify/coldtest` and the log
is `wal/memwal`, so `fold`'s tests, `cycle`'s and `wrapper`'s all run in process.

What is under `internal/verify/` is therefore not "the tests": it is the judges that are about no single
package — the corpus and what drives it, the doubles at the two cold-store seams, the witness, the
checker, and the guards, which are the tests that fail when a decision is reverted rather than when
the code is wrong.

## Considered and not taken: renaming the packages

`fold`, `cycle`, `apply` and `wrapper` are insider names, and `wrapper` is the one that actively
misleads — a reader cannot tell what is wrapped.

Against renaming: `Fold`, `Apply`, `Drain`, `Overlay`, `Replay` and `Trim` are entries in
[`CONTEXT.md`](../../CONTEXT.md), which is to say pinned vocabulary rather than accidents of naming.
Grouping already delivers what the rename was for.

What was done instead: a `doc.go` or a package comment whose first line says in plain words what the
package is, the handbook for the order nesting cannot express, and the `CONTEXT.md` entries for
Wrapper, Node, Cycle and Checker.

## Amendment — a third thing at the module root, and one more judge

[ADR 0011](0011-each-seam-ships-one-implementation.md) put an implementation at the cold seam, and
the two-group split does not describe it. `cold/` is the layer (the contract the cycle drives), but
`cold/memcold/` is a **store**: it does not run in production, it does not judge, and it is not a
double. So the module root now holds three kinds of thing rather than two, and what keeps that
legible is a dependency rule rather than a directory — nothing of the layer may import `memcold`,
and what `memcold` may import stops at the vocabulary the seam is stated in: never `cycle`, the
wrapper, `walmetrics` or the root package.

`internal/verify/` gains `e2e/`, which is a judgement in the sense the section above uses: it boots a
Temporal server over the layer and states what the layer must have seen.

The sentence in "the tests somewhere other than beside the code" that says "the cold store is
`internal/verify/coldtest`" is superseded: the cold store is `cold/memcold` and `internal/verify/coldtest` is the
double beside it. The reasoning it was supporting — that nothing here needs a cluster, so every
package's tests sit beside it — is unchanged and is now stronger.

## Amendment — the rules are read, not run

A guard test that parsed Go source to assert over the import graph is deleted, with the six other
tests that asserted over the shape of the code. Nothing above changes about *what* the rules are;
what changes is that no test enforces them.

The reason is not that the rules stopped mattering. It is that a `_test.go` answers "does this code
do what it claims", and "does this package's import list contain that path" is not that question —
it is a lint rule, and `go test ./cycle/` should not be the thing that fails when somebody writes an
import. The repository's own record against the pattern is in
[`.claude/rules/cycle.md`](../../.claude/rules/cycle.md): the scan that preceded `cycle/tailstate`
was green while the invariant it existed for was violable, because it could only match the shapes
its author had thought of.

What replaces it, in the order the successor rule
([`.claude/rules/no-lint-in-tests.md`](../../.claude/rules/no-lint-in-tests.md)) asks for it to be
tried: a compile error where one is available, a real analyzer where a mechanism is genuinely
wanted, and prose otherwise. The package rules and the tree rules took the third road and live in
[`.claude/rules/dependencies.md`](../../.claude/rules/dependencies.md), each with the `why` that
used to print on failure — which was always the load-bearing half, a rule nobody can read being a
rule someone deletes.

**The cost, stated plainly:** an import that breaks one of these rules now compiles, passes and
merges. The rules are loaded into an agent's context whenever the packages they cover are touched,
and reviewed by a person otherwise. If that proves too weak, the answer is `depguard` or
`go-arch-lint` taking the table as configuration — not another check under `go test`.

## Amendment — `verify/` moves under `internal/` for the first public release

`verify/` is now `internal/verify/`. That reverses "Considered and not taken: `internal/`" above,
which is left as it was written.

Two facts ruled `internal/` out there, and neither survives contact with the move as made. The
first was [ADR 0002](0002-wal-contract-is-backend-independent.md)'s promise of the `wal` contract
together with its conformance suite: `wal/waltest` is not under `verify/` and did not move — it
sits beside the contract it judges, which is where that promise needs it, and the same is true of
`wal/memwal` and `cold/memcold`. The second was `verify/coldtest`, said to be what an author of a
`cold.Applier` tests their store against. Its own package comment does not support that: it
records what a drain carried and what watermark it moved, and *interprets nothing*, which makes it
a fixture for composing a layer rather than a judge of a store. What would judge somebody else's
`cold.Applier` does not exist here, and the README says so.

What the section could not weigh is the one thing that changed: this repository is being
published. Before a tag, `internal/` bought protection from an external importer of `fold` or
`cycle` "who is welcome". After one, every package outside `internal/` is a compatibility
commitment — and that is twelve packages of test scaffolding whose APIs this ADR itself calls
wide, and calls the honest price of the split rather than a design to be admired. Wide and
unstable is precisely what a first tag must not promise.

**What the move does not buy, since it is easy to assume otherwise:** `internal/` at the module
root is importable from everywhere inside this module. The rule that no non-test file outside
`internal/verify/` may import it is therefore still prose, still in
[`.claude/rules/dependencies.md`](../../.claude/rules/dependencies.md), and still enforced by
nobody — exactly as the amendment above leaves it. What changed is who *may* import these
packages, not who does.
