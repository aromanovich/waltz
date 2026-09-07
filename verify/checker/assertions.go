package checker

import (
	"context"
	"fmt"

	"github.com/aromanovich/waltz/wal"
)

// The assertions themselves. Each is a value carrying the name it reports
// under, the condition under which it may be judged at all, and the predicate
// over the evidence its phase hands it — so an assertion is read in one place,
// and the judge below is a walk over a list rather than a run of branches.
//
// Two catalogues rather than one, because the two phases differ in what a check
// can be. [perSample] is judged at the moment of observation, over one look at
// one shard, and is a pure predicate: [Journal.Observe] has nowhere to put an
// error, so a check able to fail would have its failure dropped. [overRun] is
// judged once the run is quiescent, over one run at a time, and A7's evidence
// is the cold store through the function the harness handed in — a read that
// fails is the instrument failing and has to stop the judging rather than
// become a finding. One type over both would give half the list a return value
// the other half must ignore.
//
// A8 is withdrawn and keeps its number: see [Refused] for the measurement that
// withdrew it, and A9 for what took its place.

// reporter is how an assertion states a finding. The name it reports under is
// the judge's to fill in, so no assertion names itself.
type reporter func(format string, args ...any)

// shardReporter is the same for an assertion whose findings are not all about
// one shard: A7 reports on the shard of the call it judged, A9 on a shard whose
// log holds the entry.
type shardReporter func(shard wal.ShardID, format string, args ...any)

// ---------------------------------------------------------------------------
// one observation
// ---------------------------------------------------------------------------

// sampleEvidence is everything an assertion over a single observation may read.
type sampleEvidence struct {
	policy Policy
	sample Sample
	// entries are the sample's, in seqno order.
	entries []Observed
	// prior is this shard as the journal knew it *before* this sample: the
	// memory A3 and A4 are stated over and the whole difference between a
	// journal and a post-mortem look. It is read-only here — the sample is
	// folded in once every assertion has been judged, so no two assertions over
	// one sample can see different memories.
	prior *shardState
	// highestSeqno is the highest seqno ever observed on this shard, this
	// sample included.
	highestSeqno wal.Seqno
}

// sampleAssertion is one claim about a single observation.
type sampleAssertion struct {
	// Name is what a [Finding] reports under, and what a leave-one-out table is
	// keyed on.
	Name string
	// Gate is what must hold before the predicate means anything. Nil is an
	// assertion answerable from any sample of any run.
	Gate func(sampleEvidence) bool
	// Check reports once per violation.
	Check func(sampleEvidence, reporter)
}

// The two gates. A sample that carries no watermark reading is not a sample
// with a low one, and A6 and A10 are claims about a run that declared it keeps
// its whole log — see [Policy].
func hasWatermark(e sampleEvidence) bool { return e.sample.HasWatermark }
func wholeLog(e sampleEvidence) bool     { return e.policy.WholeLog }

func wholeLogWithWatermark(e sampleEvidence) bool { return wholeLog(e) && hasWatermark(e) }

var perSample = []sampleAssertion{{
	Name: "A1 gap-freedom",
	// Contract guarantee 4, the ground I2 and I5 stand on: what the log holds is
	// one unbroken run of seqnos. A hole is an entry replay would skip.
	Check: func(e sampleEvidence, report reporter) {
		for i := 1; i < len(e.entries); i++ {
			if e.entries[i].Seqno != e.entries[i-1].Seqno+1 {
				report("seqno %d follows %d", e.entries[i].Seqno, e.entries[i-1].Seqno)
			}
		}
	},
}, {
	Name: "A2 epoch monotonic",
	// I4: epochs never decrease along a shard's log. A zombie's append lands
	// above entries its successor already wrote, carrying the epoch the fence
	// cut off — which is this assertion, read straight off the column.
	Check: func(e sampleEvidence, report reporter) {
		for i := 1; i < len(e.entries); i++ {
			if e.entries[i].Epoch < e.entries[i-1].Epoch {
				report("seqno %d at epoch %d follows seqno %d at epoch %d",
					e.entries[i].Seqno, e.entries[i].Epoch, e.entries[i-1].Seqno, e.entries[i-1].Epoch)
			}
		}
	},
}, {
	Name: "A3 entries are immutable",
	// I4, across samples: an entry that exists never changes. Two writers
	// holding one epoch is the shape fencing cannot see — wal.Epoch's own doc
	// comment says they are one writer as far as every guarantee goes — and they
	// race for a seqno, the loser's entry overwritten by the winner's. Nothing
	// in any single sample says so.
	Check: func(e sampleEvidence, report reporter) {
		for _, o := range e.entries {
			if was, ok := e.prior.seen[o.Seqno]; ok && (was.Digest != o.Digest || was.Epoch != o.Epoch) {
				report("seqno %d was epoch %d digest %x, is now epoch %d digest %x",
					o.Seqno, was.Epoch, was.Digest, o.Epoch, o.Digest)
			}
		}
	},
}, {
	Name: "A4 watermark monotonic",
	// I5: the watermark only moves forward. It is the recovery point, and one
	// that went backwards re-applies a committed batch. The earlier of the
	// sample's two reads is the one to compare — it is the value that was true
	// furthest back.
	Gate: hasWatermark,
	Check: func(e sampleEvidence, report reporter) {
		if e.prior.hasWatermark && e.sample.WatermarkBefore < e.prior.maxWatermark {
			report("watermark %d is below the %d already observed",
				e.sample.WatermarkBefore, e.prior.maxWatermark)
		}
	},
}, {
	Name: "A5 recoverable",
	// I5: the log's first surviving entry is at most watermark+1 — the cold
	// store holds everything below, the log everything above, and a gap between
	// them is a shard that can never be recovered. This is the trim's own
	// invariant and the only assertion that catches a trim that ran ahead of the
	// watermark. The *later* read is the sound one: the trim may have fired
	// between the two.
	Gate: hasWatermark,
	Check: func(e sampleEvidence, report reporter) {
		if len(e.entries) > 0 && e.entries[0].Seqno > e.sample.WatermarkAfter+1 {
			report("log starts at %d, watermark is %d", e.entries[0].Seqno, e.sample.WatermarkAfter)
		}
	},
}, {
	Name: "A6 applied ⊆ logged",
	// I2/I5: nothing is applied that was never logged. A watermark naming a
	// seqno no sample ever saw is apply having run ahead of the ack, which is I2
	// broken at its source. It is a false positive against a trimming log — an
	// idle correct shard ends with an empty log and a high watermark — so it is
	// gated on the run's declared property and deliberately not weakened; see
	// [Policy].
	Gate: wholeLogWithWatermark,
	Check: func(e sampleEvidence, report reporter) {
		if e.sample.WatermarkBefore > e.highestSeqno {
			report("watermark %d, highest seqno ever observed %d",
				e.sample.WatermarkBefore, e.highestSeqno)
		}
	},
}, {
	Name: "A10 the run kept its whole log",
	// The run itself, not the layer: a run that declared it keeps its whole log
	// has to have kept it. The trim is what would take it away, and what it takes
	// is the bottom — so a non-empty log that no longer starts at the first seqno
	// is a declaration nobody kept, and A6 is unsound for the rest of the run.
	//
	// It is judged here rather than left to [Policy]'s flags because the flags
	// are a promise about a process this package does not start: with TrimEvery
	// raised and TrimAfter left at its 60 s default, an entire run's history was
	// erased and a correct layer came back red.
	Gate: wholeLog,
	Check: func(e sampleEvidence, report reporter) {
		if len(e.entries) > 0 && e.entries[0].Seqno != wal.FirstSeqno {
			report("the log starts at %d, not %d: the trim ran in a run declared WholeLog, "+
				"which is a misconfigured harness and makes A6 unsound",
				e.entries[0].Seqno, wal.FirstSeqno)
		}
	},
}}

// ---------------------------------------------------------------------------
// the run, once it is over
// ---------------------------------------------------------------------------

// runEvidence is one run as the record and the journal between them have it.
// Both assertions of this phase are claims about a single run, which is why it
// is the unit they are walked over.
type runEvidence struct {
	subject Workflow
	// calls are what the driver made on this run, in the order it made them —
	// which is what makes "the last call" a statement A7 can rest on.
	calls []Call
	// entries is how many the journal ever saw for this run, and shard is the
	// lowest it saw one on — a run lives on one shard, so the two disagree only
	// where a finding is already being reported.
	entries int
	shard   wal.ShardID
	// present is the cold store as the harness reads it.
	present Present
}

// last is the run's last call, and whether it made any.
func (e runEvidence) last() (Call, bool) {
	if len(e.calls) == 0 {
		return Call{}, false
	}
	return e.calls[len(e.calls)-1], true
}

// runAssertion is one claim about one run of a quiesced world.
type runAssertion struct {
	Name string
	Gate func(runEvidence) bool
	// Check reports once per violation. Its error is the instrument failing —
	// never a finding — and it stops the judging: a run whose cold-store read
	// errored has judged nothing, and a checker that swallowed that would be
	// green for the wrong reason.
	Check func(context.Context, runEvidence, shardReporter) error
}

var overRun = []runAssertion{{
	Name: "A7 acked ⟹ the store agrees",
	// I2, the direction that matters: what was acked is there.
	//
	// The gate is narrowness rather than caution. A mutation's effect is
	// superseded by the next one on the same run — a create's row is gone once
	// the delete lands — so "this call's effect is visible" is a statement about
	// the final state, and only the last call makes one. And a call after it
	// whose outcome bounds nothing leaves that final state undetermined: an
	// unknown by definition, and a refusal by #95's correction, since a drain
	// error returned through Manager.Write arrives after the entry is durable. A
	// history-task record names no run, so there is no row to agree about.
	//
	// What it does not claim: that every acked *update* is there. A run whose
	// last acked call is an update is asserted to exist, not to hold that
	// update's version. Saying more would mean deriving what each request leaves
	// behind — which is fold's job, and a second implementation of fold living
	// in the instrument that judges it is the one thing the oracle's law forbids
	// most.
	Gate: func(e runEvidence) bool {
		last, ok := e.last()
		return ok && last.Outcome == Acked && last.Effect != NoClaim
	},
	Check: func(ctx context.Context, e runEvidence, report shardReporter) error {
		last, _ := e.last()
		there, err := e.present(ctx, last)
		if err != nil {
			return fmt.Errorf("checker: reading the cold store for %s: %w", e.subject, err)
		}
		if there != (last.Effect == Exists) {
			report(last.Shard,
				"%s: the last call on it was an acked %s (effect %s) and the cold store says present=%v",
				e.subject, last.Kind, last.Effect, there)
		}
		return nil
	},
}, {
	Name: "A9 no entry without a call",
	// I2 read backwards, and what is left of #94's A8: the log holds no mutation
	// nobody asked for.
	//
	// Counted per run rather than matched per entry, because a driver reuses a
	// run across a chain and two updates on it are indistinguishable at this
	// distance. The count is the sharp part: a run with N recorded calls and N+1
	// entries has an entry the record cannot account for.
	//
	// It is a `≤` and never an `=`, and that is the other half of the same
	// correction: a refused call may have appended and a call in flight when its
	// process died certainly may have, so the record legitimately holds calls
	// with no entry. What it may not hold is the reverse — the `call` line is
	// fsynced *before* the store is called, so an entry whose call was never
	// written down is either a driver that does not write one or a layer that
	// invented a mutation.
	Check: func(_ context.Context, e runEvidence, report shardReporter) error {
		if e.entries <= len(e.calls) {
			return nil
		}
		report(e.shard, "%s has %d entries in the log and %d calls in the record",
			e.subject, e.entries, len(e.calls))
		return nil
	},
}}
