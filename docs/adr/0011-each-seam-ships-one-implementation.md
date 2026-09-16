# 11. Each seam ships one implementation, and neither of them is a double

Date: 2026-09-07

## Status

Accepted. Makes the two seams symmetric, and retires "waltz implements no persistence" as the
sentence the tree was described by.

## Context

waltz has two seams: `wal.Log`, where an acknowledgement lands, and `cold.Applier` plus
`cold.Watermarker`, where a drain lands. [ADR 0002](0002-wal-contract-is-backend-independent.md)
settled the first and shipped `wal/memwal` with it — a backend and not a double, which is what makes
the conformance suite it passes worth handing to somebody else.

The second seam had nothing. `internal/verify/coldtest` stood in for it: a double that records what a drain
carried and what watermark it moved, and interprets nothing. That asymmetry was never decided. It
followed from a sentence — *this library implements no persistence* — that reads like a principle
and was in fact a description, and it cost three things.

**A whole class of defect was unfindable here.** A double accepts every batch, so a fold rule that
produces a merged request no schema will take is green through the acceptance, through the unit
tests and through the guards. The batches were judged for self-consistency and never executed.

**Nothing in the repository could boot a Temporal server**, because a server needs a store. The
strongest claim any green run could make was "the configuration a person writes composes a layer
that writes", and a composition whose layer quietly fell out of the path makes that claim too.

**The seam's four obligations had no worked example.** One drain is one transaction; the watermark
commits inside it; the epoch is asserted first; the outcome comes back in one of `apply`'s five
classes. A client implementing them had prose, and prose is where the interesting half — how one
opens a transaction spanning many workflows when the interface has nowhere to declare one — is
easiest to leave out.

## Decision

**Each seam ships exactly one implementation, running in this process, and it is an implementation
rather than a stub.** `wal/memwal` at the log; `cold/memcold` at the cold store, which is
[ADR 0012](0012-the-built-in-cold-store-embeds-temporals-own-persistence.md).

Four rules make that a shape rather than an accretion:

1. **One is a maximum as well as a minimum.** A second shipped backend at either seam is refused:
   it makes "the shipped one" ambiguous, doubles what a Temporal bump has to keep green, and does
   the job that belongs to whoever needs that backend.
2. **Neither may be a stub with the interesting parts removed.** `memwal` fences, keeps seqnos
   gapless and survives a trim; `memcold` executes a folded window against Temporal's own schema
   and answers Temporal's own error classes. A shipped implementation that skipped the hard half
   would make every suite above it green for the wrong reason.
3. **Neither may have knobs, hooks or fault injection.** Making a backend misbehave is a *test's*
   need and belongs to a decorator, not to the backend: `waltest.Faulty` at the log seam,
   `internal/verify/coldtest` at the cold one. The doubles therefore stay, and stay outside the
   backends: the log's beside the conformance suite a backend author runs, in `wal/waltest`, and the
   cold store's in `internal/verify/`.
4. **Neither may know waltz.** `memwal` knows the log contract and nothing else; `memcold` answers
   Temporal's interfaces and names no more of this module than the vocabulary the seam is stated
   in — `fold`, `wal`, `apply`, `baserow`, `mutation` — never `cycle`, the wrapper, `walmetrics` or
   the root package. A backend that could see the layer would be judged by the thing sitting on top
   of it.

## Consequences

**A Temporal server now runs inside this repository's test suite.** `internal/verify/e2e` composes the layer
over both shipped backends, hands it to `temporal.WithCustomDataStoreFactory` through a custom
datastore named in the server's own config, boots frontend, history, matching and worker in the test
process, and completes a real workflow through the SDK — with nothing installed, and with a
passthrough control arm and a witness over the layer's counters beside it. That is the claim no
suite below it could make, and it exists only because the cold seam has a store.

**The acceptance drives both seams real.** `TestBothSeamsRealNoServer` folds a generated stream into
`memcold` at the shipped window and then asks the database what it holds, which turns "the layer
handed the right batch down" into "the batch executed and left these rows".

**The symmetry is not complete, and the residue is the honest part.** The log seam exports a
conformance suite; the cold seam exports none. That is not an omission to fix by writing one: what a
cold store owes a *server* is Temporal's to state, and Temporal states it as four exported suites
that `memcold` runs unmodified. What is genuinely missing is a judge for the one method Temporal
does not know about — `Apply` — for somebody else's store. Today a client gets the four obligations
in prose and `memcold` as the worked example. **Nothing exported from here judges a client's
`cold.Applier`**, and naming that beats calling the two seams mirror images.

**The sentence that described the tree is retired**, and what replaces it is narrower and stays
true: waltz *writes to* no store of its own. The layer reaches storage through `cold.Applier`,
`cold.Watermarker` and the base store the wrapper decorates, and through nothing else;
`cold/memcold` sits at the seam rather than in the layer, which is a dependency rule and not a
figure of speech — nothing in the layer may import it, and what it may import stops at the
vocabulary the seam is stated in: never `cycle`, the wrapper, `walmetrics` or the root package.

**Neither shipped backend is durable, and this decision does not make one.** Both live in one
process's memory and die with it. What
[chapter 15](../handbook/15-the-limits-of-the-evidence.md) says about fsync, quorum, a fence that
must reach another machine and a process that has to be killed is unchanged.

**The cost is two implementations to keep green across a Temporal bump**, and it is not symmetric
either: `memwal` is ours and moves when the contract moves, where `memcold` rides upstream's schema
and upstream's suites, so a bump moves both together or fails visibly.

## Considered and not taken: keep the double and ship no cold store

The status quo, and it has a real argument: a library that ships a store has a store's obligations.
What refused it is the first cost above — a batch that no schema accepts is a defect this repository
could not find, however many mutations it folded — and the fact that the obligation being avoided is
imaginary. `memcold` is not offered to a deployment; it is an in-process database that dies with the
test.

## Considered and not taken: several backends at a seam

Two logs would make the conformance suite look better exercised. It would be a false reading: the
suite's audience is an implementation that is not in this module, and running it against a second
one of ours says nothing about the first. Each additional shipped backend is a dependency, a build
input and a thing to keep green, with no claim attached that the contract suite does not already
make available to whoever needs that backend.
