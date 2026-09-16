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

# This repo: what a comment may say

Every comment in this tree was rewritten once for a reader opening the file for
the first time, roughly halving them. The
standard it set is not "comment less", it is **say what only the code cannot**,
and the density it lands at is ~28% of non-blank lines over the whole tree —
which is 35% across the non-test files and 22% across the tests, and those two
are what to compare a new file against. Well over the one it belongs to is the
signal to re-read this.

**Cut, every time:**

* **project history and provenance** — "it used to be private", "was two
  functions", "removed in the last refactor", "this replaced the inline switch".
  Git holds it, and a reader who wants it runs `git log -S`;
* **ticket numbers as citation.** A number
  earns its place only when it is the sole home of a rule, not when it is where
  the change happened. Put the reasoning in the commit message and the issue,
  which is where it is looked for;
* **re-tellings of `docs/adr/`, `docs/handbook/` and these rule files.** That is
  where the bulk of it lives and is maintained; a copy here goes stale silently.
  A pointer is fine, a paraphrase is not;
* **argument against alternatives nobody would take**, and rhetorical emphasis
  — including every `**bold**` inside a comment;
* **sentences restating the line below them**, and on a test, the rationale for
  its name or a narration of its arrange/act/assert.

**Licence and attribution headers are not comments in this sense**, and the
first bullet does not reach them. A header naming the upstream a file is derived
from and the licence that code carries is the condition on which the file may be
here at all; `NOTICE` at the root is where the detail lives, and the header is
the pointer to it. Those headers are never cut, never shortened away, and never
counted against the density above.

**Keep, because it has no other home:** preconditions and invariants; ordering
and locking constraints; which goroutine owns what; units and resolutions;
zero-value meanings; error semantics and the caller's obligations; and the
one-sentence *why* whose loss a later change would break silently. Invariant
identifiers (I9, I10) and ADR links stay as pointers.

**Compressing is a mechanical diff, so prove it mechanically.** A Go
token-stream comparison with `COMMENT` tokens dropped must be identical before
and after — that is what makes "comments and nothing else" a fact rather than a
review opinion. No test reads comment text, so a comment-only diff that moves a
suite means the diff was not comment-only.
