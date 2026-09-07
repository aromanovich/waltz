package fold

import (
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/aromanovich/waltz/mutation"
)

// What one mutation says about the rows it stands on: a function of the request
// and of nothing else. fold.go records it, check.go evaluates or delegates it;
// the two differ in how they apply it, never in what it is.
//
// The current-execution row has two facts here, not one — the assertion its
// head carries and the write its tail leaves — and neither implies the other: a
// bypass write asserts the row without writing it, and a create behind a
// tombstone writes it under an assertion the window already holds.

// asserted is the assertion set one mutation carries. The zero value is what
// the four kinds that assert nothing derive: both deletes and both task records.
type asserted struct {
	namespaceID string
	workflowID  string
	current     *CurrentAssertion
	// runs are in the store's own registration order, which Delegated.Runs is
	// promised in.
	runs []assertedRun
}

// assertedRun is one run row's assertion and the request slot its state travels
// in. part is the fold's; the authority ignores it.
type assertedRun struct {
	runID string
	part  partKind
	want  RunAssertion
}

// forRun answers one run's assertion, first match: two slots naming the same run
// are two assertions, which is what the authority would evaluate.
func (a asserted) forRun(runID string) *RunAssertion {
	for i := range a.runs {
		if a.runs[i].runID == runID {
			return &a.runs[i].want
		}
	}
	return nil
}

func (a asserted) empty() bool { return a.current == nil && len(a.runs) == 0 }

// assertionsOf derives the set for any mutation. False for a kind it does not
// recognise, which the caller must refuse rather than read as "nothing asserts":
// silence is an assertion admitted without reading the request.
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
		// The continued-as-new run sits under the mutation's own workflow, both
		// here and in the store.
		a.runs = append(a.runs, assertedRun{ns.RunID, partNewSnapshot, RunAssertion{MustNotExist: true}})
	}
	return a
}

func assertConflictResolve(req *p.InternalConflictResolveWorkflowExecutionRequest) asserted {
	reset := &req.ResetWorkflowSnapshot
	a := asserted{namespaceID: reset.NamespaceID, workflowID: reset.WorkflowID}

	switch req.Mode {
	case p.ConflictResolveWorkflowModeUpdateCurrent:
		// The store asserts against the run it believes is current: the mutated
		// current run when there is one, the reset run otherwise (mss.go).
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
		// The plugin registers no assertion for this run, but fold records one
		// and apply asserts it (registerRun): what the drain will assert is
		// what has to hold.
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

// The current-row write each kind performs, rendered the way the store's own
// path for that kind renders it ([CurrentWrite]). Set and the four kinds that
// assert nothing write nothing, so they have none.
//
// The execution state is dereferenced rather than checked: a request without one
// cannot reach the store, and tolerating a nil would record no current-row write
// where the sequential path made one.
//
// The two fallible ones must be called before [Accumulator.acc], both because an
// error past it leaves the window changed by a mutation that was refused, and
// because the fold merges requests in place: what they read is the arriving
// request's own state.

func currentWriteOfCreate(req *p.InternalCreateWorkflowExecutionRequest) *CurrentWrite {
	snap := &req.NewWorkflowSnapshot
	switch req.Mode {
	case p.CreateWorkflowModeBrandNew, p.CreateWorkflowModeUpdateCurrent:
		// The store's create passes the snapshot's state blob through.
		return &CurrentWrite{
			RunID:            snap.RunID,
			StateBlob:        snap.ExecutionStateBlob,
			LastWriteVersion: snap.LastWriteVersion,
			State:            snap.ExecutionState.State,
		}
	}
	return nil
}

// currentWriteOfUpdate: the store's update path re-serialises the full execution
// state, and a continue-as-new passes the new run's snapshot blob through.
func currentWriteOfUpdate(req *p.InternalUpdateWorkflowExecutionRequest) (*CurrentWrite, error) {
	if req.Mode != p.UpdateWorkflowModeUpdateCurrent {
		return nil, nil
	}
	if ns := req.NewWorkflowSnapshot; ns != nil {
		return &CurrentWrite{
			RunID:            ns.RunID,
			StateBlob:        ns.ExecutionStateBlob,
			LastWriteVersion: ns.LastWriteVersion,
			State:            ns.ExecutionState.State,
		}, nil
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

// currentWriteOfConflictResolve: a reduced state — run, create request id, state
// and status — taken from the new run when there is one, else from the reset.
func currentWriteOfConflictResolve(req *p.InternalConflictResolveWorkflowExecutionRequest) (*CurrentWrite, error) {
	if req.Mode != p.ConflictResolveWorkflowModeUpdateCurrent {
		return nil, nil
	}
	reset := &req.ResetWorkflowSnapshot
	st, lwv := reset.ExecutionState, reset.LastWriteVersion
	if ns := req.NewWorkflowSnapshot; ns != nil {
		st, lwv = ns.ExecutionState, ns.LastWriteVersion
	}
	blob, err := serialization.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
		RunId:           st.RunId,
		CreateRequestId: st.CreateRequestId,
		State:           st.State,
		Status:          st.Status,
	})
	if err != nil {
		return nil, fmt.Errorf("fold: serialising the current state of run %s: %w", st.RunId, err)
	}
	return &CurrentWrite{RunID: st.RunId, StateBlob: blob, LastWriteVersion: lwv, State: st.State}, nil
}
