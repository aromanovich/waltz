// Package apply interprets a drain's outcome: the class the cycle branches on,
// the errors that carry it, and the readback that names which row diverged.
//
// It writes nothing; the write path is behind cold.Applier. An applier reports
// back with [Refuse] (turned away before anything reached the cold store) and
// [Attribute] (names the rows behind a condition failure); the cycle reads both
// through [Classify].
//
// It must not name wal.Log or wal.Entry: what to do with the log next (pace,
// trim, halt) is the cycle's policy, not this package's.
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
	// fencing this layer is the only writer, so this is a broken invariant, not
	// contention: halt the shard, do not retry. The error is an
	// [*InvariantViolationError].
	ClassInvariantViolated

	// ClassUnknownOutcome: the transaction may or may not have committed. Read
	// the watermark first (cold.Watermarker); re-folding by version corrupts.
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

// Classify maps an Apply error to the class that decides the caller's next
// move. Anything not provably refused, fenced or condition-failed is an
// unknown outcome: treating a commit as a failure applies a batch twice.
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

// ErrRefused matches, via errors.Is, any error wrapped by [Refuse]. A refused
// drain wrote nothing and needs no recovery.
var ErrRefused = errors.New("apply: refused before anything reached the cold store")

// refusedError marks an error as refused, keeping its message.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }
func (e *refusedError) Is(target error) bool {
	return target == ErrRefused
}

// Refuse marks an error as raised before anything was sent to the cold store,
// so [Classify] returns [ClassRefused] for it.
func Refuse(err error) error { return &refusedError{err: err} }

// Diverged names one row whose state is not what fold asserted. RunID is empty
// for the workflow's current-execution row. A version is -1 when that side has
// none: AssertedBase under MustNotExist, ActualBase when the row is gone, both
// for a current-row divergence. Detail says what was asserted and what the cold
// store holds.
type Diverged struct {
	NamespaceID string
	WorkflowID  string
	RunID       string

	AssertedBase int64
	ActualBase   int64
	Detail       string

	// HeadSeqno and TailSeqno are the window slice behind this row: the
	// request's for a run row, the whole workflow's for the current row.
	// CutSeqno is derived from the head.
	HeadSeqno wal.Seqno
	TailSeqno wal.Seqno
}

// InvariantViolationError reports a condition fold vouched for that did not
// hold in the cold store. Terminal for the shard: halt it, do not retry.
// Diverged adds the attribution the store's error lacks (it names no workflow).
type InvariantViolationError struct {
	// Cause is the cold store's condition failure, as it reached apply.
	Cause error

	// Diverged lists every row the readback found differing from fold's
	// assertion. It may be empty (repaired before the readback); the class is
	// the same.
	Diverged []Diverged

	// CutSeqno is the highest seqno a partial re-drain may acknowledge: one
	// below the lowest head of any diverged row. Zero means acknowledge
	// nothing: no divergence found, [wal.FirstSeqno] diverged, or ReadbackErr
	// is set (an unread row could cover a lower entry).
	CutSeqno wal.Seqno

	// ReadbackErr is set when the attribution read failed; Diverged then holds
	// only what was seen before the failure.
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
// diverged. It only reads. It must run after the transaction that asserted the
// epoch: before that, a previous owner's in-flight transaction can still land
// and show up as a false divergence.
func Attribute(
	ctx context.Context, rows *baserow.Rows, cause error, shard wal.ShardID, batch fold.Batch,
) *InvariantViolationError {
	out := &InvariantViolationError{Cause: cause}

	sliceOf := workflowSlices(batch)
	for e := range batch.Each() {
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

		// Once per workflow, by the first request naming it, as the assertion
		// was registered.
		if cur := e.Workflow().Current; cur != nil && e.FirstOfWorkflow() {
			d, err := currentDiverged(ctx, rows, shard, e, cur, sliceOf[e.Workflow()])
			if err != nil {
				out.ReadbackErr = fmt.Errorf("reading the current row of workflow %s back: %w", e.WorkflowID, err)
				return out
			}
			if d != nil {
				out.Diverged = append(out.Diverged, *d)
			}
		}
	}

	// Only after a complete scan: an early return leaves unread rows that could
	// cover a lower entry, so it keeps CutSeqno at zero (acknowledge nothing).
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

// workflowSlices measures each workflow's slice across all its requests.
// Requests are ordered by tail, so the first need not have the lowest head.
func workflowSlices(batch fold.Batch) map[*fold.WorkflowRecord]wfSlice {
	out := map[*fold.WorkflowRecord]wfSlice{}
	for e := range batch.Each() {
		s, seen := out[e.Workflow()]
		if !seen || e.HeadSeqno < s.head {
			s.head = e.HeadSeqno
		}
		s.tail = max(s.tail, e.TailSeqno)
		out[e.Workflow()] = s
	}
	return out
}

// currentDiverged checks the workflow's current-execution row against fold's
// assertion, with the same predicate used before the append. The read includes
// last_write_version, so all four assertion kinds are fully judged. It returns
// nil, nil when the row held.
func currentDiverged(
	ctx context.Context, rows *baserow.Rows, shard wal.ShardID,
	e *fold.Emitted, cur *fold.CurrentAssertion, slice wfSlice,
) (*Diverged, error) {
	resp, version, err := rows.Current(ctx, int32(shard), e.NamespaceID, e.WorkflowID)
	if err != nil {
		return nil, err
	}
	if cur.VerifyRow(resp, version) == nil {
		return nil, nil
	}

	d := Diverged{
		NamespaceID: e.NamespaceID, WorkflowID: e.WorkflowID,
		AssertedBase: -1, ActualBase: -1,
		HeadSeqno: slice.head, TailSeqno: slice.tail,
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
		// An absent row also fails this assertion.
		if resp == nil {
			d.Detail = fmt.Sprintf("asserted current run is not %s, and there is no current row", cur.RunID)
			break
		}
		d.Detail = fmt.Sprintf("asserted current run is not %s, and it is", cur.RunID)
	default:
		// Unreachable: an applier refuses unknown kinds before executing. Still
		// say something, so the halt is readable.
		d.Detail = fmt.Sprintf("current-row assertion kind %d reaches no attribution, "+
			"the cold store holds %q in state %s at %d", cur.Kind, current, state, version)
	}
	return &d, nil
}
