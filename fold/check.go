package fold

// The condition authority: what the accumulator answers about a mutation's
// preconditions before it is acked, and what it hands on.
//
// Only the head of a window is asserted against the database, so without this
// file a stale conditional write folds in silently. Every assertion is in one of
// two states, which partition them:
//
//   - recorded: the mutation heads its run (or its workflow's current row), so
//     apply's transaction asserts it against the cold store atomically with the
//     write. It stands on the pre-window row, which this package may not read,
//     so it is handed back as [Delegated] for the caller;
//   - discarded: an earlier mutation of this window already heads that run, so
//     the state it stands on is the window's own. Evaluated here.
//
// A discarded assertion the window does not determine is refused rather than
// left unchecked; [ErrRefused] hands it to the next window, where it is a head
// again, which terminates after one drain because an empty window discards
// nothing.
//
// Checking happens before the append: once the entry is durable the caller has
// been told the write succeeded, and a condition evaluated after that has no
// addressee and no undo. The value returned is the store's own error built from
// the window's state, unwrapped for the caller's type switch.

import (
	"fmt"

	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/mutation"
)

// Delegated is what one [Accumulator.Check] could not answer: the assertions
// the fold will record, which stand on the pre-window row rather than on the
// window. This package may not read that row, so the value names it and hands
// the obligations to a caller that can ([Delegated.Settle]).
type Delegated struct {
	// Current is the workflow's current-execution-row assertion, when the
	// mutation carries one this window says nothing about. Nil otherwise.
	Current *DelegatedCurrent
	// Runs are the run-row assertions the window does not hold state for.
	Runs []DelegatedRun
}

// Any reports whether anything was delegated, which is the test for "this
// mutation costs a cold-store read".
func (d Delegated) Any() bool { return d.Current != nil || len(d.Runs) > 0 }

// Settle hands each delegated assertion to the caller, which reads the row it
// names and judges it with the predicate that comes with it
// ([DelegatedCurrent.Verify], [DelegatedRun.Verify]). The first non-nil answer
// is the answer, so nothing past a failing assertion is reached; a delegation
// of nothing calls neither function.
//
// The obligations come in the plugin's registration order, the current row
// before the run rows, because the store reports the first failing assertion in
// that order and upstream's compatibility suites assert on the error's type.
// Whichever function this calls first therefore names the row the store would
// have judged first.
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

// DelegatedCurrent is a current-execution-row assertion the transaction will
// carry, and the row it is about.
type DelegatedCurrent struct {
	NamespaceID string
	WorkflowID  string

	want CurrentAssertion
}

// DelegatedRun is a run-row assertion the transaction will carry, and the row
// it is about.
type DelegatedRun struct {
	NamespaceID string
	WorkflowID  string
	RunID       string

	want RunAssertion
}

// Verify evaluates the delegated current-row assertion against the cold store's
// row. See [CurrentAssertion.VerifyRow] for the arguments and the answer.
func (d DelegatedCurrent) Verify(base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	return d.want.VerifyRow(base, lastWriteVersion)
}

// Verify evaluates the delegated run-row assertion against the cold store's
// row. See [RunAssertion.VerifyRow] for the arguments and the answer.
func (d DelegatedRun) Verify(base *p.InternalGetWorkflowExecutionResponse) error {
	return d.want.VerifyRow(d.WorkflowID, base)
}

// Check reports what the store would have answered for m, as far as this window
// determines it, and hands back what it does not ([Delegated]).
//
// A nil error means nothing this window determines refuses the mutation. It does
// not mean every assertion held: the delegated ones stand on the pre-window row,
// and closing them is the caller's business.
//
// The answer can differ from the store's for a mutation whose current-row
// assertion is delegated and whose run assertion this window refuses: the store
// walks the current row first, so a doubly-stale caller gets the other error
// type here. Both are legitimate answers; only the order is ours.
//
// Read-only on the accumulator, which is what makes a refusal safe to retry.
func (a *Accumulator) Check(m mutation.Mutation) (Delegated, error) {
	del, _, err := a.check(m)
	return del, err
}

// check is [Accumulator.Check] with the counters beside its answer: the
// condition corpus measures them, and the derivation test requires that an empty
// window record every assertion and evaluate none.
func (a *Accumulator) check(m mutation.Mutation) (Delegated, coverage, error) {
	v := a.decide(m)
	return v.delegated, v.coverage, v.err
}

// coverage is how much of one mutation's assertion set this authority accounted
// for, and in which of the two ways. The partition it reports — every assertion
// either evaluated here or recorded for the drain's transaction, and none
// twice — is the claim the authority exists to make, and a share that moves is
// a request shape whose assertions started travelling by a road nobody chose.
//
// A refused assertion is counted in asserted and in neither of the others;
// errors.Is(err, ErrRefused) is the same fact and gets no counter.
type coverage struct {
	// asserted counts the assertions the mutation carries.
	asserted int
	// recorded counts those that head the window: apply's transaction asserts
	// them against the cold store, and they are the ones in [Delegated].
	recorded int
	// evaluated counts those the window determines and this file answered.
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

// fail records the first condition failure. Later assertions are still walked so
// the counters stay honest, but only the first is reported, as the plugin
// reports only the first failing assertion in registration order.
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
		// Silence is the zero verdict, which the caller reads as "nothing
		// refuses this" — an assertion admitted by a derivation that did not
		// recognise the request. Refusing here puts a ninth kind's failure
		// before the append rather than at fold.Add, after it was acked. Not
		// ErrRefused: a drain does not make an unknown kind knowable, and
		// recover retries exactly once.
		v.err = fmt.Errorf("fold: check: %w", mutation.ErrNotExactlyOneRequest)
		return v
	}
	if want.empty() {
		return v
	}

	// The current row before the run rows; the reason is at [Delegated.Settle].
	w := a.peek(want.namespaceID, want.workflowID)
	if want.current != nil {
		a.decideCurrent(&v, w, want.namespaceID, want.workflowID, *want.current)
	}
	for _, r := range want.runs {
		a.decideRun(&v, w, want.namespaceID, want.workflowID, r.runID, r.want)
	}
	return v
}

// decideRun evaluates one run-row assertion, or delegates it to the transaction.
// A run the window does not hold is a head, which is [workflowAcc.heldRun].
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
		// Not a shape the accumulator produces; refusing is the conservative
		// reading.
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

// decideCurrent evaluates one current-execution-row assertion, or delegates it.
// A row the window does not hold is a head, which is [workflowAcc.assertsCurrent].
//
// Unlike a run, a current-row assertion is not always determined, and three
// paths refuse rather than answer: a delete-current with no assertion above it
// tainted the row, so an assertion behind it is refused ahead of the delegation
// rather than recorded as a head the window never stood on; a bypass-current
// write records the head assertion and writes no row; and behind a surviving
// guard the row is neither the window's write nor the pre-window one.
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
		// The window's net effect is removal, so the row is absent whatever it
		// held before.
	default:
		// Held, yet the window determines no row: a bypass-current write
		// records the head assertion and writes nothing, and behind a surviving
		// guard the row is neither the window's write nor the pre-window one.
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
// One assertion against one row, wherever the row came from: the window's own
// write, the pre-window row read before the append, the rows the drain's own
// transaction locks, or the row apply reads back after a failure. The four
// callers differ in how they find the row and in nothing else, and each answers
// with the value below — the store's own error, message included, since that is
// what an operator reading a halted shard's logs compares against the store's.

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
// it, and answers with the store's own error, or nil. base is nil when there is
// no such execution; workflowID names the row in the answer, the store's errors
// naming the workflow rather than the run.
//
// This needs no column beyond the response: every condition the plugin asserts
// about a run row is db_record_version or the row's existence.
func (want RunAssertion) VerifyRow(workflowID string, base *p.InternalGetWorkflowExecutionResponse) error {
	if base == nil {
		return want.against(workflowID, false, 0)
	}
	return want.against(workflowID, true, base.DBRecordVersion)
}

// currentRow is a current-execution row as an assertion is judged against it:
// the three columns the store asserts on, and how to build the payload its
// conflict error carries. When exists is false there is no row and no other
// field is read.
type currentRow struct {
	exists           bool
	runID            string
	state            enumsspb.WorkflowExecutionState
	lastWriteVersion int64

	// conflict builds the store's own conflict error for msg, carrying this
	// row's payload. Required when exists.
	conflict func(msg string) error
}

// writtenRow is the current-execution row the window will leave, nil meaning the
// window's net effect is that there is none.
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

// readRow is a current-execution row as the store returns it. State is read from
// the response's execution state rather than from the column beside it: one
// upsert writes both from one struct.
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
	// Every other kind is a claim about a row, so an absent one fails them all,
	// with a bare message because there is no row to build a payload from.
	// Hoisted rather than repeated per arm, so a fifth CurrentKind inherits the
	// check instead of admitting a condition.
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
		// The row must also be COMPLETED, this assertion being a create over a
		// finished run. Left out, a start over a running run is admitted here
		// and rejected by the store.
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
// returns it, and answers with the store's own error, or nil. base is nil when
// the workflow has no current row, and lastWriteVersion is that row's
// last_write_version column, zero when there is no row.
//
// The column must be passed separately because the plugin asserts run id, state
// and last_write_version while [p.InternalGetCurrentExecutionResponse] carries
// only the first two; a caller that cannot read it could confirm
// CurrentEqualsWithVersion and never refuse it, which is why the versioned read
// is a construction-time requirement of the store below rather than a
// capability the layer degrades without.
func (want CurrentAssertion) VerifyRow(base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	return want.against(readRow(base, lastWriteVersion))
}

// The three run-row failures, in the words a Cassandra-shaped store raises them
// with. The message is part of the answer: an operator reading a halted shard's
// logs compares it against the store's own, so the two must not diverge. A
// store whose wording differs is one this text does not match — only the version
// mismatch is upstream's verbatim (see NOTICE).

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

// currentConflict is the plugin's own extractCurrentWorkflowConflictError, built
// from the window instead of from a row read back. Two fields fall short of the
// store's: StartTime is never carried at all, and RequestIDs are empty when the
// window's last current-row writer was a conflict-resolve, whose rendering holds
// none ([currentWriteOfConflictResolve]).
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
	}
}

// currentRowConflict is currentConflict for a row that was read rather than
// written by the window: the same error, built from the response's
// already-deserialised execution state instead of from a blob.
func currentRowConflict(msg string, base *p.InternalGetCurrentExecutionResponse, lastWriteVersion int64) error {
	st := base.ExecutionState
	return &p.CurrentWorkflowConditionFailedError{
		Msg:              msg,
		RequestIDs:       st.GetRequestIds(),
		RunID:            st.GetRunId(),
		State:            st.GetState(),
		Status:           st.GetStatus(),
		LastWriteVersion: lastWriteVersion,
	}
}

// writtenVersion is the db_record_version the window's state for a run will
// write: the merged request's, which is the newest folded in, not the
// assertion's.
func (pr *pendingReq) writtenVersion(part partKind) int64 {
	if part == partMutation {
		return pr.mutationPart().DBRecordVersion
	}
	return pr.snapshotPart(part).DBRecordVersion
}
