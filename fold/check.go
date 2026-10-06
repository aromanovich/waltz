package fold

// The condition authority: what the accumulator answers about a mutation's
// assertions before it is acked, and what it hands on.
//
// Only the head of a window is asserted against the database, so without this
// file a stale conditional write would fold in silently. Each assertion is
// either:
//
//   - recorded: the mutation heads its run (or the current row), so apply's
//     transaction asserts it against the cold store. It stands on the
//     pre-window row, which this package may not read, so it is handed back as
//     [Delegated];
//   - discarded: an earlier mutation already heads that run, so it stands on
//     the window's own state and is evaluated here.
//
// A discarded assertion the window does not determine is refused
// ([ErrRefused]); after a drain it is a head again, so one retry suffices.
//
// Checking happens before the append, because after it the caller has been
// told the write succeeded. Failures are the store's own error types,
// unwrapped for the caller's type switch.

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/mutation"
)

// Delegated is what one [Accumulator.Check] could not answer: the recorded
// assertions, which stand on pre-window rows this package may not read. The
// caller reads them via [Delegated.Settle].
type Delegated struct {
	// Current is the current-row assertion, if the window does not hold the row.
	Current *DelegatedCurrent
	// Runs are the run-row assertions the window does not hold state for.
	Runs []DelegatedRun
}

// Any reports whether anything was delegated, i.e. a cold-store read is needed.
func (d Delegated) Any() bool { return d.Current != nil || len(d.Runs) > 0 }

// Settle hands each delegated assertion to the caller, which reads the named
// row and judges it with [DelegatedCurrent.Verify] or [DelegatedRun.Verify].
// It stops at the first non-nil error.
//
// The order is the plugin's registration order, current row before run rows,
// because the store reports the first failing assertion in that order and
// upstream's suites assert on the error's type.
func (d Delegated) Settle(current func(DelegatedCurrent) error, run func(DelegatedRun) error) error {
	if cur := d.Current; cur != nil {
		if err := current(*cur); err != nil {
			return err
		}
	}
	for _, r := range d.Runs {
		if err := run(r); err != nil {
			return err
		}
	}
	return nil
}

// DelegatedCurrent is a delegated current-row assertion and the row it names.
type DelegatedCurrent struct {
	NamespaceID string
	WorkflowID  string

	want CurrentAssertion
}

// DelegatedRun is a delegated run-row assertion and the row it names.
type DelegatedRun struct {
	NamespaceID string
	WorkflowID  string
	RunID       string

	want RunAssertion
}

// Verify evaluates the assertion against the cold store's row; see
// [CurrentAssertion.VerifyRow].
func (d DelegatedCurrent) Verify(base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	return d.want.VerifyRow(base, lastWriteVersion)
}

// Verify evaluates the assertion against the cold store's row; see
// [RunAssertion.VerifyRow].
func (d DelegatedRun) Verify(base *p.InternalGetWorkflowExecutionResponse) error {
	return d.want.VerifyRow(d.WorkflowID, base)
}

// Check reports what the store would have answered for m, as far as this window
// determines it, and hands back what it does not ([Delegated]).
//
// A nil error means nothing the window determines refuses the mutation; the
// delegated assertions are still the caller's to settle.
//
// When the current-row assertion is delegated and a run assertion fails here,
// a doubly-stale caller gets the run error where the store would report the
// current-row one. Both are legitimate answers.
//
// Read-only on the accumulator, so a refusal is safe to retry.
func (a *Accumulator) Check(m mutation.Mutation) (Delegated, error) {
	del, _, err := a.check(m)
	return del, err
}

// check is [Accumulator.Check] plus its counters, for tests.
func (a *Accumulator) check(m mutation.Mutation) (Delegated, coverage, error) {
	v := a.decide(m)
	return v.delegated, v.coverage, v.err
}

// coverage counts how one mutation's assertions were handled: each is either
// evaluated here or recorded for the transaction, never both. A refused one is
// counted only in asserted.
type coverage struct {
	asserted int
	// recorded counts the head assertions, the ones in [Delegated].
	recorded  int
	evaluated int
}

// verdict is what one check saw.
type verdict struct {
	coverage  coverage
	delegated Delegated
	err       error
}

func (v *verdict) evaluate() { v.coverage.asserted++; v.coverage.evaluated++ }

// undecided marks a discarded assertion the window does not determine and
// refuses the mutation: the drain makes it a head again.
func (v *verdict) undecided(what string) {
	v.coverage.asserted++
	if v.err == nil {
		v.err = fmt.Errorf("%w: %s the window does not determine", ErrRefused, what)
	}
}

// fail records a condition failure. Only the first is reported, as the plugin
// does; later assertions are still counted.
func (v *verdict) fail(err error) {
	v.coverage.asserted++
	v.coverage.evaluated++
	if v.err == nil {
		v.err = err
	}
}

func (v *verdict) delegateRun(namespaceID, workflowID, runID string, want RunAssertion) {
	v.coverage.asserted++
	v.coverage.recorded++
	v.delegated.Runs = append(v.delegated.Runs, DelegatedRun{
		NamespaceID: namespaceID, WorkflowID: workflowID, RunID: runID, want: want,
	})
}

func (v *verdict) delegateCurrent(namespaceID, workflowID string, want CurrentAssertion) {
	v.coverage.asserted++
	v.coverage.recorded++
	v.delegated.Current = &DelegatedCurrent{
		NamespaceID: namespaceID, WorkflowID: workflowID, want: want,
	}
}

func (a *Accumulator) decide(m mutation.Mutation) verdict {
	var v verdict
	want, known := assertionsOf(m)
	if !known {
		// Fail an unknown kind before the append rather than at Add after the
		// ack. Not ErrRefused: a drain does not make it knowable.
		v.err = fmt.Errorf("fold: check: %w", mutation.ErrNotExactlyOneRequest)
		return v
	}
	// Fast path only; the walks below would visit nothing.
	if want.empty() {
		return v
	}

	// Current row first; see [Delegated.Settle].
	w := a.peek(want.namespaceID, want.workflowID)
	if want.current != nil {
		a.decideCurrent(&v, w, want.namespaceID, want.workflowID, *want.current)
	}
	for _, r := range want.runs {
		a.decideRun(&v, w, want.namespaceID, want.workflowID, r.runID, r.want)
	}
	return v
}

// decideRun evaluates one run-row assertion, or delegates it if the window does
// not hold the run ([workflowAcc.heldRun]).
func (a *Accumulator) decideRun(v *verdict, w *workflowAcc, namespaceID, workflowID, runID string, want RunAssertion) {
	rs := w.heldRun(runID)
	if rs == nil {
		v.delegateRun(namespaceID, workflowID, runID, want)
		return
	}

	exists, version := true, int64(0)
	switch {
	case rs.tombstoned:
		exists = false
	case rs.owner == nil:
		// The accumulator never produces this; refuse to be safe.
		v.undecided(fmt.Sprintf("the row of run %s", runID))
		return
	default:
		version = rs.owner.writtenVersion(rs.part)
	}

	if err := want.against(workflowID, exists, version); err != nil {
		v.fail(err)
		return
	}
	v.evaluate()
}

// decideCurrent evaluates one current-row assertion, or delegates it if the
// window does not hold the row ([workflowAcc.assertsCurrent]).
//
// Three cases are refused: the row is tainted by an unasserted delete-current
// (so the assertion would be a head the window never stood on); a
// bypass-current write held the row but wrote none; or a surviving guard left
// the row neither the window's write nor the pre-window one.
func (a *Accumulator) decideCurrent(v *verdict, w *workflowAcc, namespaceID, workflowID string, want CurrentAssertion) {
	if !w.assertsCurrent() {
		if w.currentTainted() {
			v.undecided("the current-execution row behind a delete-current")
			return
		}
		v.delegateCurrent(namespaceID, workflowID, want)
		return
	}

	var cw *CurrentWrite
	switch view := w.currentView(); view.Shape {
	case CurrentWritten:
		cw = view.write
	case CurrentGone:
		// Net effect is removal: the row is absent.
	default:
		// Held, but the window determines no row (see above).
		v.undecided("the current-execution row")
		return
	}

	if err := want.against(writtenRow(cw)); err != nil {
		v.fail(err)
		return
	}
	v.evaluate()
}

// --- the predicates -------------------------------------------------------
//
// One assertion against one row, wherever the row came from (the window's
// write, a pre-append read, the drain's transaction, or apply's read-back).
// Each answers with the store's own error, message included, since operators
// compare it with the store's logs.

// against evaluates the run-row assertion. exists is whether the row is there
// and version is its db_record_version, meaningless when it is not.
func (want RunAssertion) against(workflowID string, exists bool, version int64) error {
	switch {
	case want.MustNotExist:
		if exists {
			return runMustNotExist(workflowID)
		}
	case !exists:
		return runMustExist(workflowID)
	case version != want.BaseVersion:
		return runVersionMismatch(workflowID, want.BaseVersion, version)
	}
	return nil
}

// VerifyRow evaluates the run-row assertion against a row as the store returns
// it (nil when absent), answering with the store's own error or nil.
// workflowID is used in the message, as the store's errors name the workflow.
func (want RunAssertion) VerifyRow(workflowID string, base *p.InternalGetWorkflowExecutionResponse) error {
	if base == nil {
		return want.against(workflowID, false, 0)
	}
	return want.against(workflowID, true, base.DBRecordVersion)
}

// currentRow is the three columns the store asserts on, plus a builder for its
// conflict error. When exists is false no other field is read.
type currentRow struct {
	exists           bool
	runID            string
	state            enumsspb.WorkflowExecutionState
	lastWriteVersion int64

	// conflict builds the store's conflict error for msg. Required when exists.
	conflict func(msg string) error
}

// writtenRow is the current row the window will leave; nil cw means none.
func writtenRow(cw *CurrentWrite) currentRow {
	if cw == nil {
		return currentRow{}
	}
	return currentRow{
		exists:           true,
		runID:            cw.RunID,
		state:            cw.State,
		lastWriteVersion: cw.LastWriteVersion,
		conflict:         func(msg string) error { return currentConflict(msg, cw) },
	}
}

// readRow is a current row as the store returns it. State comes from the
// execution state, which one upsert writes together with the column.
func readRow(base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) currentRow {
	if base == nil {
		return currentRow{}
	}
	return currentRow{
		exists:           true,
		runID:            base.RunID,
		state:            base.ExecutionState.GetState(),
		lastWriteVersion: lastWriteVersion,
		conflict:         func(msg string) error { return currentRowConflict(msg, base, lastWriteVersion) },
	}
}

func (want CurrentAssertion) against(row currentRow) error {
	if want.Kind == CurrentMustNotExist {
		if row.exists {
			return row.conflict("must not exist")
		}
		return nil
	}
	// Every other kind needs a row. Hoisted so a new CurrentKind inherits it.
	if !row.exists {
		return &p.CurrentWorkflowConditionFailedError{Msg: "must exist"}
	}
	switch want.Kind {
	case CurrentEquals:
		if row.runID != want.RunID {
			return row.conflict(fmt.Sprintf("current run id %s must be equal to %s", row.runID, want.RunID))
		}
	case CurrentNotEquals:
		if row.runID == want.RunID {
			return row.conflict(fmt.Sprintf("current run id %s must not be equal to %s", row.runID, want.RunID))
		}
	case CurrentEqualsWithVersion:
		// The row must also be COMPLETED (a create over a finished run), as the
		// store requires.
		if row.state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED ||
			row.runID != want.RunID || row.lastWriteVersion != want.LastWriteVersion {
			return row.conflict(fmt.Sprintf(
				"state %d must be equal to %d, current run id %s must be equal to %s, last write version %d must be equal to %d",
				row.state, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				row.runID, want.RunID,
				row.lastWriteVersion, want.LastWriteVersion))
		}
	}
	return nil
}

// VerifyRow evaluates the current-row assertion against a row as the store
// returns it (nil when absent), answering with the store's own error or nil.
// lastWriteVersion is the row's last_write_version column (zero if absent).
//
// The column is passed separately because the response lacks it; without it
// CurrentEqualsWithVersion could never be refused, so the store below must
// provide the versioned read.
func (want CurrentAssertion) VerifyRow(base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	return want.against(readRow(base, lastWriteVersion))
}

// The three run-row failures, worded as a Cassandra-shaped store words them so
// operators can match them to store logs. Only the version mismatch is
// upstream's verbatim (see NOTICE).

func runMustNotExist(workflowID string) error {
	return &p.WorkflowConditionFailedError{Msg: fmt.Sprintf("Workflow %s must not exist", workflowID)}
}

func runMustExist(workflowID string) error {
	return &p.ConditionFailedError{Msg: fmt.Sprintf("Workflow execution %s must exist", workflowID)}
}

func runVersionMismatch(workflowID string, want, actual int64) error {
	return &p.WorkflowConditionFailedError{
		Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: %v",
			want, actual),
		DBRecordVersion: actual,
	}
}

// currentConflict is the plugin's extractCurrentWorkflowConflictError built
// from the blob the window will write, start time included ([startTimeOf]).
func currentConflict(msg string, cw *CurrentWrite) error {
	st, err := serialization.WorkflowExecutionStateFromBlob(cw.StateBlob)
	if err != nil {
		return fmt.Errorf("fold: deserialising the window's current-execution state: %w", err)
	}
	return &p.CurrentWorkflowConditionFailedError{
		Msg:              msg,
		RequestIDs:       st.RequestIds,
		RunID:            st.RunId,
		State:            st.State,
		Status:           st.Status,
		LastWriteVersion: cw.LastWriteVersion,
		StartTime:        startTimeOf(st),
	}
}

// currentRowConflict is currentConflict for a row read from the store.
//
// The run id comes from the response, not the state: upstream's read leaves
// the state's copy empty, and with an empty run id the history service skips
// conflict resolution, request-id dedup included.
func currentRowConflict(msg string, base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	st := base.ExecutionState
	return &p.CurrentWorkflowConditionFailedError{
		Msg:              msg,
		RequestIDs:       st.GetRequestIds(),
		RunID:            base.RunID,
		State:            st.GetState(),
		Status:           st.GetStatus(),
		LastWriteVersion: lastWriteVersion,
		StartTime:        startTimeOf(st),
	}
}

// startTimeOf is the state's start time, nil if none. Without it the start
// path's reuse check sees a zero start time and its minimal-interval refusal
// never fires.
func startTimeOf(st *persistencespb.WorkflowExecutionState) *time.Time {
	if st.GetStartTime() == nil {
		return nil
	}
	t := st.GetStartTime().AsTime()
	return &t
}

// writtenVersion is the db_record_version the merged request will write for
// the run (the newest folded in, not the asserted one).
func (pr *pendingReq) writtenVersion(part partKind) int64 {
	if part == partMutation {
		return pr.mutationPart().DBRecordVersion
	}
	return pr.snapshotPart(part).DBRecordVersion
}
