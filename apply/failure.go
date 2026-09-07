// Package apply is what a drain's outcome means: the class the cycle branches
// on, the errors that carry it, and the readback that names which row diverged.
//
// It writes nothing. The write path is the caller's, behind cold.Applier, and
// an implementation of that interface speaks back to the cycle in this
// vocabulary — [Refuse] for what it turned away before anything reached the
// cold store, [Attribute] to turn a bare condition failure into the rows it was
// about, and [Classify] for the rule the cycle reads both through.
package apply

import (
	"context"
	"errors"
	"fmt"
	"strings"

	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/wal"
)

// Class is what a drain's outcome demands of the caller. It splits by which
// assertion failed, not by workflow.
type Class int

const (
	// ClassCommitted: the drain committed, watermark included.
	ClassCommitted Class = iota

	// ClassRefused: apply refused the drain before anything reached the cold
	// store. There is no outcome to recover, only an input to fix.
	ClassRefused

	// ClassShardLost: the epoch CAS failed — the shard has a new owner and this
	// writer is fenced. Stop writing under this epoch; do not retry the drain.
	ClassShardLost

	// ClassInvariantViolated: a version or current-row assertion failed. Under
	// fencing this layer is the shard's only writer, so it is a broken
	// invariant and not contention: halt the shard, do not retry. The error is
	// an [*InvariantViolationError] with the attribution.
	ClassInvariantViolated

	// ClassUnknownOutcome: an ambiguous code reached apply and the transaction
	// may or may not have committed. Read the watermark before anything else
	// (cold.Watermarker); re-folding by version instead corrupts.
	ClassUnknownOutcome
)

func (c Class) String() string {
	switch c {
	case ClassCommitted:
		return "committed"
	case ClassRefused:
		return "refused"
	case ClassShardLost:
		return "shard lost"
	case ClassInvariantViolated:
		return "invariant violated"
	case ClassUnknownOutcome:
		return "unknown outcome"
	}
	return fmt.Sprintf("Class(%d)", int(c))
}

// Classify sorts what an Apply returned into the class that decides the
// caller's next move. Anything it cannot prove refused, fenced or
// condition-failed is an unknown outcome: calling a commit a failure is how a
// batch gets applied twice.
func Classify(err error) Class {
	if err == nil {
		return ClassCommitted
	}
	if errors.Is(err, ErrRefused) {
		return ClassRefused
	}
	if _, ok := errors.AsType[*p.ShardOwnershipLostError](err); ok {
		return ClassShardLost
	}
	if isInvariantViolation(err) {
		return ClassInvariantViolated
	}
	return ClassUnknownOutcome
}

func isInvariantViolation(err error) bool {
	var (
		wf   *p.WorkflowConditionFailedError
		cur  *p.CurrentWorkflowConditionFailedError
		cond *p.ConditionFailedError
	)
	return errors.As(err, &wf) || errors.As(err, &cur) || errors.As(err, &cond)
}

// ErrRefused matches, via errors.Is, every error an Apply raises before
// anything is sent to the cold store. A refused drain wrote nothing and needs
// no recovery, unlike everything after Execute.
var ErrRefused = errors.New("apply: refused before anything reached the cold store")

// refusedError marks a pre-Execute error as refused, keeping its message.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }
func (e *refusedError) Is(target error) bool {
	return target == ErrRefused
}

// Refuse marks an error as raised before anything was sent to the cold store,
// which is what makes [Classify] answer [ClassRefused] for it.
func Refuse(err error) error { return &refusedError{err: err} }

// Diverged names one row whose state is not where fold thought it was. RunID is
// empty when the row is the workflow's current-execution row rather than a
// run's base row; AssertedBase and ActualBase are -1 when there is no version
// to report — a MustNotExist assertion, an absent row, any current-row
// divergence. Detail always says what was asserted and what the cold store
// holds.
type Diverged struct {
	NamespaceID string
	WorkflowID  string
	RunID       string

	AssertedBase int64
	ActualBase   int64
	Detail       string

	// HeadSeqno and TailSeqno are the window slice answering for this row: the
	// request's own for a run row, the whole workflow's for the current row,
	// whose assertion is the workflow's rather than any one request's. The cut
	// point is derived from the head.
	HeadSeqno wal.Seqno
	TailSeqno wal.Seqno
}

// InvariantViolationError reports a condition the accumulator vouched for that
// did not hold in the cold store. Terminal for the shard: halt it, do not
// retry. Diverged is the attribution the cold store's own error cannot give,
// since it reports one failing assertion and names no workflow.
type InvariantViolationError struct {
	// Cause is the cold store's own condition failure, as it reached apply.
	Cause error

	// Diverged names every row the readback found somewhere else than fold
	// asserted. It can be empty — the divergence may have been repaired between
	// the transaction and the readback — which changes nothing about the class.
	Diverged []Diverged

	// CutSeqno is the highest seqno a partial re-drain may acknowledge: one
	// below the lowest entry answering for any diverged row. Zero means nothing
	// may be acknowledged, and covers three cases that demand the same of the
	// caller — no divergence was found, the window's first entry diverged, and
	// ReadbackErr, where a row nobody read could answer for an entry below
	// anything seen. Applying anything above it would leave entries applied
	// above any watermark the drain could set.
	CutSeqno wal.Seqno

	// ReadbackErr is set when the attribution read itself failed; Diverged is
	// then incomplete and says only what was seen before the failure.
	ReadbackErr error
}

func (e *InvariantViolationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "apply: invariant violated, halt the shard: %v", e.Cause)
	if len(e.Diverged) > 0 {
		names := make([]string, 0, len(e.Diverged))
		for _, d := range e.Diverged {
			names = append(names, fmt.Sprintf("%s (%s)", d.WorkflowID, d.Detail))
		}
		fmt.Fprintf(&b, "; diverged: %s", strings.Join(names, "; "))
	}
	if e.ReadbackErr != nil {
		fmt.Fprintf(&b, "; attribution incomplete: %v", e.ReadbackErr)
	}
	return b.String()
}

func (e *InvariantViolationError) Unwrap() error { return e.Cause }

// Attribute reads back every row the drain asserted and names the ones that
// diverged; reads only. It must stay after Execute: a base row read before the
// epoch is held can see a previous owner's in-flight transaction land
// underneath it, and the divergence it would then report is not a bug.
func Attribute(
	ctx context.Context, rows *baserow.Rows, cause error, shard wal.ShardID, batch fold.Batch,
) *InvariantViolationError {
	out := &InvariantViolationError{Cause: cause}

	sliceOf := workflowSlices(batch)
	for e := range batch.Each() {
		// One closure, so the seqno pair CutSeqno reads below is written once.
		diverged := func(runID string, assertedBase, actualBase int64, detail string) {
			out.Diverged = append(out.Diverged, Diverged{
				NamespaceID: e.NamespaceID, WorkflowID: e.WorkflowID, RunID: runID,
				AssertedBase: assertedBase, ActualBase: actualBase,
				Detail:    detail,
				HeadSeqno: e.HeadSeqno, TailSeqno: e.TailSeqno,
			})
		}

		for runID, a := range e.RunAssertions() {
			resp, err := rows.Run(ctx, int32(shard), e.NamespaceID, e.WorkflowID, runID)
			if err != nil {
				out.ReadbackErr = fmt.Errorf("reading run %s of workflow %s back: %w", runID, e.WorkflowID, err)
				return out
			}
			if a.VerifyRow(e.WorkflowID, resp) == nil {
				continue
			}
			switch {
			case resp == nil:
				diverged(runID, a.BaseVersion, -1,
					fmt.Sprintf("asserted base version %d, the row is gone", a.BaseVersion))
			case a.MustNotExist:
				diverged(runID, -1, resp.DBRecordVersion,
					fmt.Sprintf("asserted absent, the row exists at version %d", resp.DBRecordVersion))
			default:
				diverged(runID, a.BaseVersion, resp.DBRecordVersion,
					fmt.Sprintf("asserted base version %d, the cold store holds %d", a.BaseVersion, resp.DBRecordVersion))
			}
		}

		// Read back once per workflow record, not once per request naming it —
		// the same first-namer rule the assertions were registered under.
		if cur := e.Workflow().Current; cur != nil && e.FirstOfWorkflow() {
			if d, diverged, err := currentDiverged(ctx, rows, shard, e, cur, sliceOf[e.Workflow()]); err != nil {
				out.ReadbackErr = fmt.Errorf("reading the current row of workflow %s back: %w", e.WorkflowID, err)
				return out
			} else if diverged {
				out.Diverged = append(out.Diverged, d)
			}
		}
	}

	// After the whole batch, not per entry: a scan that returned above left rows
	// nobody read, and any of them could answer for an entry below everything
	// seen so far. The zero the early returns keep is "acknowledge nothing".
	for i, d := range out.Diverged {
		if i == 0 || d.HeadSeqno-1 < out.CutSeqno {
			out.CutSeqno = d.HeadSeqno - 1
		}
	}
	return out
}

// wfSlice is the window slice one workflow contributed to a drain, across every
// request naming it.
type wfSlice struct{ head, tail wal.Seqno }

// workflowSlices measures each workflow's slice. Requests are ordered by tail,
// so the first one naming a workflow need not carry that workflow's lowest
// head, and the current row's assertion is answered for by all of them.
func workflowSlices(batch fold.Batch) map[*fold.WorkflowRecord]wfSlice {
	out := map[*fold.WorkflowRecord]wfSlice{}
	for e := range batch.Each() {
		s, seen := out[e.Workflow()]
		if !seen || e.HeadSeqno < s.head {
			s.head = e.HeadSeqno
		}
		if e.TailSeqno > s.tail {
			s.tail = e.TailSeqno
		}
		out[e.Workflow()] = s
	}
	return out
}

// currentDiverged checks the workflow's current-execution row against fold's
// head-of-window assertion, through the same predicate that judged it before the
// append. The read carries the row's last_write_version, so all four assertion
// kinds are judged on everything the store asserts.
func currentDiverged(
	ctx context.Context, rows *baserow.Rows, shard wal.ShardID,
	e *fold.Emitted, cur *fold.CurrentAssertion, slice wfSlice,
) (Diverged, bool, error) {
	d := Diverged{
		NamespaceID: e.NamespaceID, WorkflowID: e.WorkflowID,
		AssertedBase: -1, ActualBase: -1,
		HeadSeqno: slice.head, TailSeqno: slice.tail,
	}
	resp, version, err := rows.Current(ctx, int32(shard), e.NamespaceID, e.WorkflowID)
	if err != nil {
		return d, false, err
	}
	if cur.VerifyRow(resp, version) == nil {
		return d, false, nil
	}

	current, state := "", enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED
	if resp != nil {
		current, state = resp.RunID, resp.ExecutionState.GetState()
	}
	switch cur.Kind {
	case fold.CurrentMustNotExist:
		d.Detail = fmt.Sprintf("asserted no current row, the current run is %s", current)
	case fold.CurrentEquals:
		d.Detail = fmt.Sprintf("asserted current run %s (by run id), the cold store holds %q", cur.RunID, current)
	case fold.CurrentEqualsWithVersion:
		d.Detail = fmt.Sprintf("asserted current run %s completed at last write version %d, "+
			"the cold store holds %q in state %s at %d",
			cur.RunID, cur.LastWriteVersion, current, state, version)
	case fold.CurrentNotEquals:
		// A claim about a row, so an absent one refuses it as well.
		if resp == nil {
			d.Detail = fmt.Sprintf("asserted current run is not %s, and there is no current row", cur.RunID)
			break
		}
		d.Detail = fmt.Sprintf("asserted current run is not %s, and it is", cur.RunID)
	default:
		// An applier refuses a kind it cannot register before the drain executes,
		// so this is a floor: a halt whose divergence names nothing is unreadable.
		d.Detail = fmt.Sprintf("current-row assertion kind %d reaches no attribution, "+
			"the cold store holds %q in state %s at %d", cur.Kind, current, state, version)
	}
	return d, true, nil
}
