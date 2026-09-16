---
paths:
  - "wal/**"
---

# This repo: the WAL layer

`wal/` is the seam this library is built on rather than a test of someone else's: the contract
(`wal/`), the one backend that ships (`wal/memwal/`, in memory) and, in `wal/waltest/`, the
conformance suite, the fault decorator every caller drives a failing log through, and the
retention check a deployment runs against its own storage beside the expiring log that check
is proved against.
[ADR 0002](../../docs/adr/0002-wal-contract-is-backend-independent.md) is why the contract exists
and what it promises; the handbook's
[04-contracts.md](../../docs/handbook/04-contracts.md) is the long form of the five guarantees.

What to know before changing any of it:

* no file in `waltest/` may import a backend, on purpose. The suite imports the
  contract and an assertion library; the two decorators and the retention check
  import the contract alone. A test that needs a particular log is asserting the
  wrong thing; so is an assertion only one implementation could satisfy;
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
  `ErrAlreadyWritten`, a backend that keeps its entries as rows and derives the
  verdict from the ones around the seqno says `ErrGap`, those rows being what
  the trim deleted, and the contract deliberately picks neither — so the suite asserts
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

* **the contract's context obligations are a conformance case now**
  (`ACancelledContextChangesNothing`), and until they were they were prose only:
  a call whose context was already dead leaves the log exactly as it was, its
  error stays matchable against `context.Canceled`/`DeadlineExceeded`, and an
  argument the contract does not admit outranks the context. `refuse.go` carries
  none of the three — those helpers are the argument rules and the diagnosis
  order — so a backend author had three obligations no green run mentioned. That
  the case is the whole of the coverage is measured rather than assumed: deleting
  every `ctx.Err()` check from `memwal` leaves the other eighteen green and fails
  this one alone;
* **a refusal the contract has no name for is settled by reading the log, not by
  a fourth sentinel.** An append can fail with an error the contract does not
  name — every transport failure does — and whether the entry is in the log is
  then not established. It used to be *assumed*: the cycle's append switch
  recognised the three sentinels, anything else left `s.next` where it was, and
  the next mutation took the same seqno with the first attempt possibly still in
  flight. Which of the two ends up at that position is then the backend's race
  to settle, and a caller was told each of the two answers.
  Two closes were considered and the third is what shipped. **A fourth answer in
  `wal`** meaning "the outcome is unknown" asks every backend to know something
  most cannot: the state it describes is exactly the one where a backend's own
  check failed. **Halting on any unrecognised append error** is correct and
  costs a shard per blip, since the common case is a transport error that wrote
  nothing. What ships instead is `cycle.Cycle.settleAppend`: the log is the
  witness, read back at that one seqno, exactly as the cold store's watermark is
  the witness for a drain — nothing there and the append wrote nothing, this
  cycle's own payload there and the append *succeeded* and the caller is told so,
  anything else (a stranger's entry, or a read that failed) and the shard halts
  holding the log as evidence. So the contract is unchanged and `wal`'s three
  refusals still mean what they meant; what changed is that the cycle stopped
  reading "an error I do not recognise" as "wrote nothing".
  `waltest.Faulty.AfterAppend` is what stages one — a fault asked *after* the
  append has landed, the ambiguity no `OnAppend` can express;
* **the suite's blind spot on time has an instrument, and it is deliberately not
  a case in the suite.** Guarantee 5 excuses a trim and nothing else, so a
  retention window, a TTL on the log's table or a compaction that drops old
  records each break it silently — and the suite runs in milliseconds, so a
  backend that expires entries passes all nineteen and loses the first tail that
  outlives its policy. `waltest.CheckRetention` is that obligation as a function
  a deployment calls: append a run, wait out a window the caller names, require
  every entry back with the log still appendable above them. It returns an error
  rather than taking a `*testing.T` for the reason `internal/verify/drive` does —
  a deployment runs it from whatever harness it has — and it costs its window in
  wall-clock time, which is why it cannot be a twentieth case. Point it at a
  **deliberately shortened policy**: a pass says the entries outlived that
  window, never that the backend has no retention. `waltest.Expiring` is the
  backend it is proved against and the one decorator here that is not a log a
  backend may be — `Faulty` refuses calls, which a correct backend does, while
  this one breaks the readback guarantee on purpose. Without it a deployment
  reading a green check cannot tell it from a check that passes anything, which
  is what `TestTheRetentionCheckIsNotVacuous` exists to say;
* the whole of it runs with nothing installed: `go test ./wal/...` is
  milliseconds, and it is the first thing to run on a fresh clone.
