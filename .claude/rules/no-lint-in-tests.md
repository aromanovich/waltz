---
paths:
  - "*.go"
  - "wal/**"
  - "mutation/**"
  - "fold/**"
  - "apply/**"
  - "cycle/**"
  - "wrapper/**"
  - "walmetrics/**"
  - "baserow/**"
  - "cold/**"
  - "internal/verify/**"
---

# This repo: a test asserts behaviour, never shape

Three bans, all absolute, all introduced after seven files had grown under
them:

* **no test parses Go source.** Nothing in a `_test.go` imports `go/ast`,
  `go/parser` or `go/token` to assert over the shape of the code.
* **no test asserts over the import graph.** Not with `go list`, not with
  `go/build`, not by reading directories — no "package X may not import Y", no
  "this file must import Z", no rule about where files sit in the tree.
* **no test parses documentation or build files.** Not the handbook, not a
  Makefile, not a yaml the repository ships — and no shelling out to
  `go test -list` to enumerate somebody else's suites. A hand-written Markdown
  or Make parser inside a `_test.go` is the worst form of all three, because it
  is a parser nobody maintains for a language nobody declared.

What went, so nobody re-derives them one at a time:

| file | what it asserted |
|---|---|
| `cycle/stateowner_test.go` | a `*state` is a parameter and nothing else; a function holding one starts no goroutine |
| `cycle/mirror_test.go` | the loop does not read the mirror; assigning `s.st` publishes it |
| `cycle/jobs_test.go` | a hand-kept table of everything that may ask the loop |
| `fold/partition_test.go` | no current-row nil-comparison outside `heldRun`/`assertsCurrent` |
| `fold/allornothing_test.go` | no handler returns an error after calling `acc` |
| a guard over the task-category registry | the no-archival registry is not built inline |
| a guard over the import graph | sixteen package dependency rules, the two tree rules, the module root's shape |
| a reference test in the root package | the configuration table against `knobs`+`settings`, by parsing the markdown |
| two acceptance tests | a Makefile's test selection against upstream's suites, by parsing Make and running `go test -list` |

The second batch is the same genre one step worse: `go/ast` at least
parses Go with Go's own parser, while a `strings.Cut` on a markdown heading is a
parser written in a test for a format the test invented rules about.

A `_test.go` answers **does this code do what it claims**. "Does this function
body contain a `go` statement", "does this package's import list contain that
path", "does the handbook's third table have this row" are not that question. They are lint
rules, and running them under `go test` costs three things: `go test
./cycle/` stops meaning "the cycle works", the diagnostic points at the
scan rather than at the offending line, and the rule is maintained by whoever is
least expecting to.

## What to do instead, in the order to try it

1. **Make it a compile error.** Unexported fields of an exported type in a
   package of its own is what `cycle/tailstate` and `cycle/window`
   are: `s.tail.resolved = 0` does not build. The only option that cannot be
   walked past.
2. **Write a linter, as a linter** — an analyzer on
   `golang.org/x/tools/go/analysis`, tested with `analysistest`, run as
   `go vet -vettool` or a golangci-lint plugin, from its own make target. Same
   work, a home that admits what it is, diagnostics on the right line. For
   dependency rules specifically there are tools that already do it
   (`depguard`, `go-arch-lint`) and take the rules as configuration.

   **That target exists: `make lint`**, golangci-lint plus gopls's `modernize`,
   both pinned in the Makefile and run over the whole tree. A new rule goes in
   `.golangci.yml` beside the ones already there, and the rule for that file is
   the one this whole document is about: it ships green, so a check this
   repository decided against is switched **off there, with the reason**, rather
   than silenced one `//nolint` at a time. Four linters are off for reasons
   written at the entry, and the `//nolint` comments in the tree each name what
   they are for.
3. **State it in prose and stop.** [dependencies.md](dependencies.md) is where
   the package rules went; the ownership rules are in [cycle.md](cycle.md), the
   fold ones in [fold.md](fold.md) and the configuration table's in
   [waltz.md](waltz.md), each beside the reasoning it
   comes from. Where the residue is "two lists move together by hand", say that
   **at both lists**.
   This is a legitimate ending, not a failure to finish: a rule nobody can
   violate without reading the file they are editing is carried adequately by a
   sentence in that file.

## Why the deleted checks were not merely redundant

They read as stronger than they are, and this tree has the receipt. The scan
that preceded `tailstate` "could only match the assignment shapes somebody had
thought of, and `t := &s.tail` walked past it" ([cycle.md](cycle.md)) — green,
while the invariant it existed for was violable. A prose rule claims nothing
mechanical; a green check claims something it has not checked.

The second cost is coupling to syntax. Every body scan matched identifiers by
name — `state`, `mirror`, `acc`, `NewDefaultTaskCategoryRegistry` — so a rename
the compiler follows for free left the scan matching nothing and passing. Two of
them were tables of call sites hand-maintained beside the call sites they name,
which is a second copy of the code kept in step by a test that fails when the
two disagree in either direction.

## What this rule does not say

It does not say the invariants were worthless — every one of them is still a
rule, written where it is read. If one earns a mechanism again it earns option 1
or option 2 above, not an eighth check.

This rule also does not reach reflection. `cycle/surface_test.go`,
`cycle/counters_test.go` and the wrapper's two coverage tests assert over
types at run time rather than over source, and they are **not** covered here.
They are the same family and worth revisiting; that is a separate decision and
nobody should take it by reading this file.
