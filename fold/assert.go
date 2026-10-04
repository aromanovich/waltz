package fold

import (
	"fmt"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/mutation"
)

// What one mutation asserts about the rows it stands on, derived from the
// request alone. fold.go records it; check.go evaluates or delegates it.
//
// The current row has two independent facts: the assertion and the write. A
// bypass write asserts without writing; a create behind a tombstone writes
// under an assertion the window already holds.

// asserted is the assertion set one mutation carries. Both deletes and both
// task records assert nothing (the zero value).
type asserted struct {
	namespaceID string
	workflowID  string
	current     *CurrentAssertion
	// runs are in the store's registration order, as Delegated.Runs promises.
	runs []assertedRun
}

// assertedRun is one run row's assertion and the request slot its state travels
// in. part is the fold's; the authority ignores it.
type assertedRun struct {
	runID string
	part  partKind
	want  RunAssertion
}

// forRun returns the first assertion for runID, nil if none.
func (a asserted) forRun(runID string) *RunAssertion {
	for i := range a.runs {
		if a.runs[i].runID == runID {
			return &a.runs[i].want
		}
	}
	return nil
}

func (a asserted) empty() bool { return a.current == nil && len(a.runs) == 0 }

// assertionsOf derives the set for any mutation. False for an unknown kind,
// which the caller must refuse rather than read as "asserts nothing".
func assertionsOf(m mutation.Mutation) (asserted, bool) {
	switch m.Kind() {
	case mutation.KindCreate:
		return assertCreate(m.Create), true
	case mutation.KindUpdate:
		return assertUpdate(m.Update), true
	case mutation.KindConflictResolve:
		return assertConflictResolve(m.ConflictResolve), true
	case mutation.KindSet:
		return assertSet(m.Set), true
	case mutation.KindDelete, mutation.KindDeleteCurrent:
		return asserted{}, true
	case mutation.KindAddTasks, mutation.KindRangeCompleteTasks:
		return asserted{}, true
	default:
		return asserted{}, false
	}
}

func assertCreate(req *p.InternalCreateWorkflowExecutionRequest) asserted {
	snap := &req.NewWorkflowSnapshot
	a := asserted{namespaceID: snap.NamespaceID, workflowID: snap.WorkflowID}

	switch req.Mode {
	case p.CreateWorkflowModeBrandNew:
		a.current = &CurrentAssertion{Kind: CurrentMustNotExist}
	case p.CreateWorkflowModeUpdateCurrent:
		a.current = &CurrentAssertion{
			Kind:             CurrentEqualsWithVersion,
			RunID:            req.PreviousRunID,
			LastWriteVersion: req.PreviousLastWriteVersion,
		}
	}
	a.runs = append(a.runs, assertedRun{snap.RunID, partSnapshot, RunAssertion{MustNotExist: true}})
	return a
}

func assertUpdate(req *p.InternalUpdateWorkflowExecutionRequest) asserted {
	mut := &req.UpdateWorkflowMutation
	a := asserted{namespaceID: mut.NamespaceID, workflowID: mut.WorkflowID}

	switch req.Mode {
	case p.UpdateWorkflowModeUpdateCurrent:
		a.current = &CurrentAssertion{Kind: CurrentEquals, RunID: mut.RunID}
	case p.UpdateWorkflowModeBypassCurrent:
		a.current = &CurrentAssertion{Kind: CurrentNotEquals, RunID: mut.RunID}
	}
	a.runs = append(a.runs, assertedRun{mut.RunID, partMutation, RunAssertion{BaseVersion: mut.DBRecordVersion - 1}})
	if ns := req.NewWorkflowSnapshot; ns != nil {
		// The continued-as-new run is under the same workflow.
		a.runs = append(a.runs, assertedRun{ns.RunID, partNewSnapshot, RunAssertion{MustNotExist: true}})
	}
	return a
}

func assertConflictResolve(req *p.InternalConflictResolveWorkflowExecutionRequest) asserted {
	reset := &req.ResetWorkflowSnapshot
	a := asserted{namespaceID: reset.NamespaceID, workflowID: reset.WorkflowID}

	switch req.Mode {
	case p.ConflictResolveWorkflowModeUpdateCurrent:
		// The store asserts the mutated current run if any, else the reset run.
		runID := reset.ExecutionState.RunId
		if req.CurrentWorkflowMutation != nil {
			runID = req.CurrentWorkflowMutation.ExecutionState.RunId
		}
		a.current = &CurrentAssertion{Kind: CurrentEquals, RunID: runID}
	case p.ConflictResolveWorkflowModeBypassCurrent:
		a.current = &CurrentAssertion{Kind: CurrentNotEquals, RunID: reset.ExecutionState.RunId}
	}
	a.runs = append(a.runs, assertedRun{reset.RunID, partSnapshot, RunAssertion{BaseVersion: reset.DBRecordVersion - 1}})
	if cur := req.CurrentWorkflowMutation; cur != nil {
		a.runs = append(a.runs, assertedRun{cur.RunID, partMutation, RunAssertion{BaseVersion: cur.DBRecordVersion - 1}})
	}
	if ns := req.NewWorkflowSnapshot; ns != nil {
		// The plugin asserts nothing here, but apply will assert this
		// ([Emitted.RunAssertions]), so it must hold.
		a.runs = append(a.runs, assertedRun{ns.RunID, partNewSnapshot, RunAssertion{MustNotExist: true}})
	}
	return a
}

func assertSet(req *p.InternalSetWorkflowExecutionRequest) asserted {
	snap := &req.SetWorkflowSnapshot
	return asserted{
		namespaceID: snap.NamespaceID,
		workflowID:  snap.WorkflowID,
		runs: []assertedRun{
			{snap.RunID, partSnapshot, RunAssertion{BaseVersion: snap.DBRecordVersion - 1}},
		},
	}
}

// The current-row write each kind performs, rendered as the store's path for
// that kind renders it ([CurrentWrite]). Set and the non-asserting kinds write
// none.
//
// The execution state is dereferenced, not nil-checked: the store requires it,
// and tolerating nil would silently record no write.
//
// Call the fallible one before [Accumulator.acc]: an error after it would leave
// a refused mutation in the window, and merging in place changes what it reads.

// currentWriteOfSnapshot is the row a snapshot writes, off its own state blob.
func currentWriteOfSnapshot(snap *p.InternalWorkflowSnapshot) *CurrentWrite {
	return &CurrentWrite{
		RunID:            snap.RunID,
		StateBlob:        snap.ExecutionStateBlob,
		LastWriteVersion: snap.LastWriteVersion,
		State:            snap.ExecutionState.State,
	}
}

func currentWriteOfCreate(req *p.InternalCreateWorkflowExecutionRequest) *CurrentWrite {
	switch req.Mode {
	case p.CreateWorkflowModeBrandNew, p.CreateWorkflowModeUpdateCurrent:
		return currentWriteOfSnapshot(&req.NewWorkflowSnapshot)
	}
	return nil
}

// currentWriteOfUpdate renders the row off the mutation's execution state, or
// off the new run's snapshot for a continue-as-new.
func currentWriteOfUpdate(req *p.InternalUpdateWorkflowExecutionRequest) (*CurrentWrite, error) {
	if req.Mode != p.UpdateWorkflowModeUpdateCurrent {
		return nil, nil
	}
	if ns := req.NewWorkflowSnapshot; ns != nil {
		return currentWriteOfSnapshot(ns), nil
	}
	mut := &req.UpdateWorkflowMutation
	blob, err := serialization.WorkflowExecutionStateToBlob(mut.ExecutionState)
	if err != nil {
		return nil, fmt.Errorf("fold: serialising the execution state of run %s: %w", mut.RunID, err)
	}
	return &CurrentWrite{
		RunID:            mut.RunID,
		StateBlob:        blob,
		LastWriteVersion: mut.LastWriteVersion,
		State:            mut.ExecutionState.State,
	}, nil
}

// currentWriteOfConflictResolve renders the row off the new run's snapshot if
// any, else the reset's. It must use the full blob: a reduced state would
// durably lose start_time and non-create request ids, breaking
// WorkflowIdReuseMinimalInterval.
func currentWriteOfConflictResolve(req *p.InternalConflictResolveWorkflowExecutionRequest) *CurrentWrite {
	if req.Mode != p.ConflictResolveWorkflowModeUpdateCurrent {
		return nil
	}
	snap := &req.ResetWorkflowSnapshot
	if ns := req.NewWorkflowSnapshot; ns != nil {
		snap = ns
	}
	return currentWriteOfSnapshot(snap)
}
