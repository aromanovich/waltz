---
paths:
  - "wal/**"
---

# This repo: the WAL layer

`wal/` is the seam this library is built on rather than a test of someone else's: the contract
(`wal/`), the one backend that ships (`wal/memwal/`, in memory) and, in `wal/waltest/`, the
conformance suite and the fault decorator every caller drives a failing log through.
[ADR 0002](../../docs/adr/0002-wal-contract-is-backend-independent.md) is why the contract exists
and what it promises; the handbook's
[04-contracts.md](../../docs/handbook/04-contracts.md) is the long form of the five guarantees.

What to know before changing any of it:

* neither file in `waltest/` may import a backend, on purpose. The suite imports the
  contract and an assertion library; the decorator imports the contract alone.
  A test that needs a particular log is asserting the wrong thing; so is an
  assertion only one implementation could satisfy;
* a caller wanting a log that fails wraps a real backend in `waltest.Faulty`
  rather than writing an implementation of its own. Three of them had grown
  above this seam and all three had drifted off the contract — one accepted an
  append over a gap, which is the thing gap-freedom exists to refuse, so the
  tests standing on it were driving a log no backend can be;
* the suite covers correctness — the single-writer path and fencing, the
  contention test included — and **nothing about cost**. That is deliberate and
  it is where this library's evidence stops: whether a log answers sooner than
  the cold store would is a property of that log, measured by whoever built it,
  on their cluster. Nothing here can stand in for it, and a number produced here
  would describe a map;
* the contention test contends for real or fails saying it did not: its claimants keep
  racing until each has both had entries acked and been fenced off. A change that
  makes them take turns instead turns it into a test that proves nothing, and it
  reports that rather than passing. Its two `runtime.Gosched()` calls are what
  make the interleaving the suite's own rather than the backend's: a backend over
  a network interleaves because every operation parks the goroutine on a round
  trip, and `memwal`'s park on nothing — remove the yields and it fails on a
  one-P runtime (a single-CPU cgroup is enough) saying the run never contended;
* **`memwal` is a backend and not a double** — no knobs, no fault injection,
  nothing observable around the interface. Its own tests pin what it promises
  *as a substitute*, where it could differ from a log over a real cluster in a
  way the contract has no words for. **Two of those have since become contract
  clauses and moved.** Payload ownership — that the log neither retains a
  caller's slice nor hands out memory of its own — is stated on `Append` and
  `Entry.Payload` now, and the suite asserts it (`PayloadsAreNobodyElsesMemory`),
  so a second backend can no longer violate a written obligation and stay green.
  That the seqnos below a trim stay spent is stated on `Log.Trim` and asserted
  by `AppendBelowATrimIsRefused`, because a backend that hands them out again
  writes a hole into a log everything above reads as gap-free. What stays local
  is *which* refusal: `memwal` keeps its next seqno and says
  `ErrAlreadyWritten`, a backend that keeps a trim marker instead would say
  `ErrGap`, and the contract deliberately picks neither — so the suite asserts
  the refusal and admits both answers, and the choice stays each backend's own
  test to make;
* **the contract has no batch, and that is a decision rather than an omission**
  ([ADR 0010](../../docs/adr/0010-the-log-appends-one-entry-at-a-time.md)).
  `Append` takes one payload. A backend with no atomic multi-row write cannot
  keep the promise a batch parameter implies — a fence landing between two rows
  leaves a prefix, and the contract's three refusals each claim the write is
  whole one way or the other. Worse, the answer such a backend *does* give is
  wrong: a writer that died mid-write and replayed is told `ErrAlreadyWritten`,
  documented as the replay's success signal, and acks seqnos nobody wrote. So
  the parameter is gone rather than qualified.

  Two shapes were tried first and are the ones to refuse if they come back.
  **Answering a torn-batch error on a partial overlap** turns
  `DuplicateSeqnoIsAlreadyWritten` red, because the contract pins the opposite.
  **A `MaxAtomicBatch()` on the contract**, answered `UnboundedBatch` by most
  backends and 1 by the awkward one, works and is worse: a parameter one backend
  refuses, three conformance assertions in two versions apiece, and a question
  whose answer is 1 wherever that backend is deployed. And what all of it
  replaced was not a mechanism at all — the rule used to be "the layer must keep
  to one payload per `Append`", true because `cycle` happens to append that way,
  stated in this file and two doc comments, and deleting all three broke no test;

* **a refusal the contract has no name for is a contract change, not an
  adapter's problem.** A backend can reach a state where an append failed *and*
  the check that would say whether it landed failed too — the outcome is
  unknown, and it is the one answer a caller may not replay on, since a replay
  may write a second entry at a seqno the first attempt still lands at. The
  cycle's append switch recognises only the three sentinels: anything else
  arrives as an ordinary error, `s.next` does not move, and the next mutation
  appends at the same seqno — which is exactly the replay such a state forbids.
  Closing it means either a fourth answer in `wal` meaning "the outcome is
  unknown, do not retry this seqno", or the cycle halting on an append error it
  does not recognise. Both are decisions about every backend rather than about
  one, which is why neither has been taken here;
* the whole of it runs with nothing installed: `go test ./wal/...` is
  milliseconds, and it is the first thing to run on a fresh clone.
