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
  - "verify/**"
---

# This repo: what a comment may say

Every comment in this tree was rewritten once for a reader opening the file for
the first time, roughly halving them. The
standard it set is not "comment less", it is **say what only the code cannot**,
and the density it lands at is ~23% of non-blank lines. A new file well over
that is the signal to re-read this.

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
