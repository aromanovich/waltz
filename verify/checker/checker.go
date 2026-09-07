// Package checker judges a chaos run of this layer from outside it (#101).
//
// It is the instrument two prototypes settled the inputs of: #94 wrote the
// assertions down and broke the layer under each of them, and #95 built the
// smallest thing that is a node and killed it. What it says is what neither the
// oracle nor the five-mode acceptance can say — that **nothing acked was lost
// and nothing applied was invented, while nodes were being killed**.
//
// # It is a journal, not a look
//
// The measurement that shapes everything else is #94's: [cycle.Cycle] trims to
// the applied watermark with no safety lag, so the surviving log is bounded by
// TrimEvery × Mutations entries however long the run was. A post-mortem look at
// a long run therefore sees a vanishing fraction of it. So the checker
// **observes repeatedly and remembers**: every assertion is over the journal
// rather than over the world, and the driver's record of its own calls is not a
// second instrument beside it but entries in the same journal, made by the one
// observer that can see an outcome at all.
//
// A run that wants A6 out of it raises both trim triggers so that the log *is*
// the history — and because a declaration nobody keeps is worse than no
// declaration, that is [Policy]'s job rather than a caller's, and A10 checks
// that the promise was kept.
//
// # What it may not be
//
// Not assertions inside the layer: those see what the layer believes and die
// with it under kill -9, which is exactly the case P5 exists for. So this is a
// library a harness drives from outside, reading the log through the contract's
// own [wal.Log.ReadFrom] — no SQL of its own, no backend named here (ADR 0002)
// — the watermark through a reader the harness passes in, and the cold store
// through [Present], for the same boundary reason cycle may not name a store:
// the claim under test includes "and it survived", so the read has to be the
// plugin's.
//
// And not a checker that is green because it was weakened. The admission rule
// is the oracle's, transposed: an assertion is admitted when it names the
// invariant it encodes and a defect only it catches, when it names the input it
// needs and asserts nothing that input cannot carry, and when it is green on a
// correct layer under every run the harness drives. A6 is the worked example —
// it failed the third and was **gated** rather than weakened. A red run is a
// finding.
//
// # The assertions
//
// A1–A10 are values and not branches, and [perSample] and [overRun] are the
// whole list: an entry carries the name it reports under, what has to hold
// before it may be judged, and the predicate. A8 is withdrawn and keeps its
// number — see [Refused].
package checker

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/aromanovich/waltz/wal"
)

// Workflow is what a call and an entry are both about: the subject of one
// mutation. Identity is the driver's problem and not the layer's — the payload
// format carries namespace/workflow/run verbatim, so a driver whose runs are
// its own has an identity for free.
type Workflow struct {
	Namespace string
	Workflow  string
	Run       string
}

func (w Workflow) String() string { return w.Namespace + "/" + w.Workflow + "/" + w.Run }

// Effect is what the cold store must show for a mutation that was applied.
//
// Two of the three values are all [Present] can answer — the row is there or it
// is not — and the third is a mutation that makes no claim about a row at all.
type Effect int

const (
	// Exists: the mutation leaves the run's mutable state behind.
	Exists Effect = iota
	// Removed: the mutation is the deletion of it.
	Removed
	// NoClaim: the mutation is not about a run's mutable state, so this
	// instrument has nothing to check it against (#142). Both history-task
	// records are such mutations — they name a shard, a category and a range,
	// and [Present] reads execution rows.
	//
	// It is a value rather than an error because the alternative readings are
	// both wrong: refusing the mutation would stop a driver that is behaving,
	// and counting it as undecoded would report the record as damaged. What A7
	// and A9 do with it is skip it, which is stated at each of them.
	NoClaim
)

func (e Effect) String() string {
	switch e {
	case Removed:
		return "removed"
	case NoClaim:
		return "no-claim"
	default:
		return "exists"
	}
}

// Outcome is what the driver learned about one call it made. Three classes, and
// the third is the whole reason the record has to exist: after a kill -9 there
// are calls whose outcome nobody knows, and an assertion that reads them as
// either of the other two reports findings that are not.
type Outcome int

const (
	// Unknown: the call was in flight when the world changed under it, its
	// deadline expired, or its error was ambiguous. Nothing is promised about
	// it in either direction.
	//
	// This is the checker's identity control, and the parallel with the
	// oracle's is exact: the exception is admitted by *the record* and never by
	// an assertion's author, and it is scoped to the one call.
	Unknown Outcome = iota
	// Acked: the call returned success. I2 says its mutation is durable, and
	// A7 is that read against the cold store.
	Acked
	// Refused: the call returned a definite error — a condition failure, a
	// backpressure refusal, an ownership-lost.
	//
	// **A refusal bounds nothing about the cold store**, which is #95's
	// correction and the reason #94's A8 (refused ⟹ absent) is withdrawn rather
	// than kept. Some refusals are provably pre-append — backpressure (#47),
	// the condition authority (#74), a halted cycle — but a drain error
	// returned up through [cycle.Manager.Write] arrives *after* that call's own
	// entry is durable, and **from outside the two are the same Go type**. It
	// was measured: a losing claimant's 136 acked + 1 refused calls were 137
	// entries, and the winner's replay applied all 137.
	//
	// So the class is kept for the run's own census and for A9's count, and no
	// assertion reads it as absence.
	Refused
)

func (o Outcome) String() string {
	switch o {
	case Acked:
		return "acked"
	case Refused:
		return "refused"
	default:
		return "unknown"
	}
}

// Call is one write the driver made and wrote down.
//
// Node, Incarnation and Seq are its identity in the record and its order within
// one driver's stream: Seq is per process, so a node restarted onto the same
// record path would number a second run from 1 — [Record] is what stops the two
// from colliding.
type Call struct {
	Node        string
	Incarnation int64
	Seq         int64

	Shard   wal.ShardID
	Epoch   wal.Epoch
	Kind    string
	Subject Workflow
	Effect  Effect
	Outcome Outcome
	// Detail is what the driver wrote down beside the outcome — the error's
	// type and text. No assertion reads it; it is here because a finding
	// nobody can investigate is a finding nobody acts on.
	Detail string

	// Dangling is a call whose outcome line never arrived — the call the
	// process was inside when it died. It is [Unknown] like any other, and it
	// is flagged separately only so a run can report how many of them it had:
	// the count is what says the third class was exercised at all.
	Dangling bool
}

// Observed is one log entry as the journal remembers it: enough to answer every
// assertion and not the payload itself, which on a long run is the difference
// between a journal and a copy of the log.
type Observed struct {
	Seqno wal.Seqno
	Epoch wal.Epoch
	// Subject is who the entry's mutation is about, decoded once per seqno by
	// the [Sampler].
	Subject Workflow
	// Digest identifies the payload without keeping it. A3 compares it across
	// samples: two writers holding one epoch race for a seqno and the loser's
	// entry is overwritten by the winner's, and nothing in the end state says
	// so — only an observer that saw both.
	Digest uint64
}

// Sample is one observation of one shard.
//
// The watermark is read **twice**, before the log and after it, and the two are
// not interchangeable. A6 needs the lower bound: a watermark read after the log
// may name an entry appended since, which is not apply running ahead of the ack.
// A5 needs the upper: the trim may have run between the two reads, and the log
// the sample holds was trimmed to a watermark no higher than the later one.
// One read cannot be both, and a checker that used one would report a finding
// per race.
type Sample struct {
	Shard   wal.ShardID
	Entries []Observed

	WatermarkBefore wal.Seqno
	WatermarkAfter  wal.Seqno
	HasWatermark    bool
}

// Finding is a violation. A checker that reports these is doing its job; a
// checker changed until it reports none has stopped being one.
type Finding struct {
	Assertion string
	Shard     wal.ShardID
	Detail    string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s (shard %d): %s", f.Assertion, f.Shard, f.Detail)
}

type shardState struct {
	// seen is every seqno this journal ever observed and what was there. It is
	// the memory the trim takes away.
	seen         map[wal.Seqno]Observed
	maxSeqno     wal.Seqno
	maxWatermark wal.Seqno
	hasWatermark bool
}

// Journal is the checker: feed it samples and calls, ask it for findings.
//
// It is safe for concurrent use, because the sampling loop and the harness
// collecting records are two goroutines by construction.
type Journal struct {
	mu       sync.Mutex
	policy   Policy
	shards   map[wal.ShardID]*shardState
	calls    []Call
	findings []Finding
	samples  int
}

// New returns a journal for a run driven under p.
//
// The policy is taken here rather than set field by field because it carries
// the run's *declared* properties, and A6 stands on one of them. [Policy] is
// also what renders the flags that make the declaration true, so the
// declaration and the run cannot disagree — which is the whole of #101's
// fourth deliverable, and A10 is the same claim checked rather than trusted.
func New(p Policy) *Journal {
	return &Journal{policy: p, shards: map[wal.ShardID]*shardState{}}
}

// Policy reports the run's declared properties.
func (j *Journal) Policy() Policy { return j.policy }

func (j *Journal) state(s wal.ShardID) *shardState {
	st, ok := j.shards[s]
	if !ok {
		st = &shardState{seen: map[wal.Seqno]Observed{}}
		j.shards[s] = st
	}
	return st
}

func (j *Journal) report(assertion string, shard wal.ShardID, format string, args ...any) {
	j.findings = append(j.findings, Finding{assertion, shard, fmt.Sprintf(format, args...)})
}

// Record is the driver telling the journal what it did. Calls of one driver
// must arrive in the order that driver made them, which is what makes "the last
// call on this run" a statement A7 can rest on.
func (j *Journal) Record(calls ...Call) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls = append(j.calls, calls...)
}

// Observe folds one sample in, running every assertion answerable at the moment
// of observation. The ones that need the run to be over are in [Journal.Quiesced].
func (j *Journal) Observe(s Sample) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.samples++
	st := j.state(s.Shard)
	e := j.evidence(s, st)

	for _, a := range perSample {
		if a.Gate != nil && !a.Gate(e) {
			continue
		}
		a.Check(e, func(format string, args ...any) {
			j.report(a.Name, s.Shard, format, args...)
		})
	}

	// The sample joins the memory only once every assertion has judged it, so
	// that A3 and A4 read what the journal knew before it and no assertion can
	// be made to depend on another's position in the catalogue.
	st.absorb(e)
}

func (j *Journal) evidence(s Sample, st *shardState) sampleEvidence {
	entries := slices.Clone(s.Entries)
	slices.SortFunc(entries, func(a, b Observed) int { return cmp.Compare(a.Seqno, b.Seqno) })

	e := sampleEvidence{policy: j.policy, sample: s, entries: entries, prior: st, highestSeqno: st.maxSeqno}
	if n := len(entries); n > 0 && entries[n-1].Seqno > e.highestSeqno {
		e.highestSeqno = entries[n-1].Seqno
	}
	return e
}

func (st *shardState) absorb(e sampleEvidence) {
	for _, o := range e.entries {
		st.seen[o.Seqno] = o
	}
	st.maxSeqno = e.highestSeqno
	if e.sample.HasWatermark {
		if e.sample.WatermarkAfter > st.maxWatermark {
			st.maxWatermark = e.sample.WatermarkAfter
		}
		st.hasWatermark = true
	}
}

// Present is how the journal reaches the cold store: is this call's subject
// there? The reader is the harness's, because the checker may not name a store
// any more than cycle may — and it must be the *plugin's* read, since the claim
// under test includes "and it survived".
type Present func(context.Context, Call) (bool, error)

// Quiesced runs the assertions that need the run over: the shards are drained,
// the tail is applied, and the cold store is now the whole answer.
//
// The error return is for the instrument failing, never for a finding. A
// cold-store read that errored is a run that judged nothing, and a checker that
// swallowed it would be green for the wrong reason.
func (j *Journal) Quiesced(ctx context.Context, present Present) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Quiescence is A7's precondition, and it is checked rather than assumed.
	//
	// "Acked ⟹ the cold store agrees" is false of a shard with a tail, and
	// legitimately so: I2 says an acked mutation is *in the log*, and the cold
	// store catches up when a drain commits. A shard whose watermark has not
	// reached its last entry therefore answers nothing — the run may have been
	// stopped mid-flight, the last drain may have failed, or the shard may be
	// halted — and an instrument that judged it anyway would report one finding
	// per unapplied mutation and call a correct layer broken.
	//
	// It is an error and not a finding for the same reason a failed cold-store
	// read is: the world was not in a state this claim is about. Bringing it
	// into one is the harness's job, and what does it is what does it in
	// production — a successor that acquires the shard and replays.
	for shard, st := range j.shards {
		if st.maxSeqno > st.maxWatermark {
			return fmt.Errorf("checker: shard %d is not quiescent — its log reaches seqno %d and the "+
				"watermark is %d, so %d acked mutations are durable and not yet applied; "+
				"nothing can be asked of the cold store until an owner has replayed them",
				shard, st.maxSeqno, st.maxWatermark, st.maxSeqno-st.maxWatermark)
		}
	}

	runs, err := j.runs(present)
	if err != nil {
		return err
	}
	for _, e := range runs {
		for _, a := range overRun {
			if a.Gate != nil && !a.Gate(*e) {
				continue
			}
			err := a.Check(ctx, *e, func(shard wal.ShardID, format string, args ...any) {
				j.report(a.Name, shard, format, args...)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// runs is the evidence [overRun] is walked over: one entry per run the record
// or the log knows about. A run only the log knows about is exactly what A9 is
// looking for, so the two sources are a union and not a join.
//
// The ownership guard here is not an assertion and must not become one: an
// instrument that cannot tell two drivers' calls apart cannot say which one was
// last, and every claim of this phase rests on that. Two nodes sharing a
// generator seed is what produces it.
func (j *Journal) runs(present Present) (map[Workflow]*runEvidence, error) {
	out := map[Workflow]*runEvidence{}
	at := func(subject Workflow) *runEvidence {
		e, ok := out[subject]
		if !ok {
			e = &runEvidence{subject: subject, present: present}
			out[subject] = e
		}
		return e
	}

	for _, c := range j.calls {
		e := at(c.Subject)
		if len(e.calls) > 0 && e.calls[0].Node != c.Node {
			return nil, fmt.Errorf("checker: run %s is claimed by both %q and %q, "+
				"so no call on it is the last one — give every driver a stream of its own",
				c.Subject, e.calls[0].Node, c.Node)
		}
		e.calls = append(e.calls, c)
	}

	for shard, st := range j.shards {
		for _, o := range st.seen {
			if o.Subject == (Workflow{}) {
				continue
			}
			e := at(o.Subject)
			e.entries++
			// The lowest rather than whichever this map ranged to first, so the
			// shard a finding names is the same on every run of one world.
			if e.entries == 1 || shard < e.shard {
				e.shard = shard
			}
		}
	}
	return out, nil
}

// Findings is what the run found. Empty is the only green.
func (j *Journal) Findings() []Finding {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.findings)
}

// Fired names the distinct assertions that reported, which is what a
// leave-one-out table is built out of.
func (j *Journal) Fired() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	seen := map[string]struct{}{}
	for _, f := range j.findings {
		seen[f.Assertion] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen))
}

// Census is what the run was, and it is the reason a green result means
// anything: a journal that saw nothing reports no findings too.
type Census struct {
	Samples int
	// Remembered is entries per shard the journal holds — which on a trimming
	// run is more than the world still has.
	Remembered map[wal.ShardID]int
	Calls      int
	Acked      int
	Refused    int
	Unknown    int
	Dangling   int
	// Judged is the runs A7 stated something about: a green A7 over zero runs
	// is the shape a broken harness has.
	Judged int
}

func (c Census) String() string {
	return fmt.Sprintf("samples %d, remembered %v, calls %d (acked %d, refused %d, unknown %d, of them dangling %d), runs judged %d",
		c.Samples, c.Remembered, c.Calls, c.Acked, c.Refused, c.Unknown, c.Dangling, c.Judged)
}

// CensusOf counts an arbitrary slice of calls: everything a census says that
// the record alone answers, which is all of it but Samples and Remembered.
//
// It is exported because the harness asks the same question about a *subset* —
// one node's record while the run is still going (has this generation acked its
// quota? has a partitioned node gone silent?), or the record split by the
// driver that wrote it. Those counts and the ones a run is finally judged on
// have to be the same counts: two implementations of "acked" drift the moment
// an outcome class is added, and the copy is in the harness, which is the half
// nothing else judges. Journal.Census is this plus the two fields only the
// journal holds.
func CensusOf(calls []Call) Census {
	c := Census{Calls: len(calls)}
	last := map[Workflow]Call{}
	for _, call := range calls {
		switch call.Outcome {
		case Acked:
			c.Acked++
		case Refused:
			c.Refused++
		default:
			c.Unknown++
		}
		if call.Dangling {
			c.Dangling++
		}
		last[call.Subject] = call
	}
	for _, call := range last {
		if call.Outcome == Acked {
			c.Judged++
		}
	}
	return c
}

// Census reports what the journal has, which every caller should assert on
// before reading Findings: a run that observed nothing is green.
func (j *Journal) Census() Census {
	j.mu.Lock()
	defer j.mu.Unlock()

	c := CensusOf(j.calls)
	c.Samples = j.samples
	c.Remembered = map[wal.ShardID]int{}
	for id, st := range j.shards {
		c.Remembered[id] = len(st.seen)
	}
	return c
}
