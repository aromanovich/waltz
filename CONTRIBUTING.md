# Contributing

Three rules here are this repository's own, and nothing enforces them: no linter, no test, on
purpose — the import rules *were* a guard test until the second rule below deleted it. They live
under [`.claude/rules/`](.claude/rules/), which looks like an agent's configuration and is not. It
is where "what to know before changing this" is kept, beside the code it is about, one file per
subsystem.

* **A comment says what only the code cannot** —
  [`.claude/rules/comments.md`](.claude/rules/comments.md). Out: provenance, ticket numbers as
  citation, re-tellings of the ADRs and the handbook, sentences restating the line below. In:
  preconditions and invariants, ordering and ownership, units, zero-value meanings, error semantics
  and the caller's obligations, and the one *why* whose loss a later change would break silently.
* **A test asserts behaviour, never shape** —
  [`.claude/rules/no-lint-in-tests.md`](.claude/rules/no-lint-in-tests.md). No `_test.go` parses Go
  source, asserts over the import graph, or reads the handbook or a build file. A rule about shape
  is prose or a linter; it is never a test, because such a test is green while the invariant it
  exists for is violable.
* **What each package may import is decided** —
  [`.claude/rules/dependencies.md`](.claude/rules/dependencies.md). Every package's bans with the
  reason at each one, and the two rules about the tree. Read it before adding an import: nothing
  will fail if you break it.

The gate is `make test` and `make lint` — `make check` is both. Neither needs anything installed:
no cluster, no container, no fixed port, no cgo.
