// Package basetest is an in-memory [baserow.Store]: the pre-window rows, for
// tests that stand up a write path without a database.
//
// It exists so absence is answered one way everywhere. A missing row is a
// NotFound, which [baserow.Rows] turns into the nil row delegated assertions
// are stated over; a double answering absence differently would leave the
// suite green and the rule unjudged.
package basetest

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/baserow"
)

// Store holds run rows by run id and current rows by workflow id, and counts
// reads of each. An unstaged store is what a create asserts against: every
// read is an absence. Locked, because the cycle reads while the test stages.
type Store struct {
	mu       sync.Mutex
	runs     map[string]int64
	current  map[string]row
	err      error
	failRuns map[string]error
	reads    Reads
}

// row is a current-execution row: run, state and last_write_version.
type row struct {
	runID            string
	state            enumsspb.WorkflowExecutionState
	lastWriteVersion int64
}

// Reads counts reads of each kind, which most tests here assert on: the current
// row is read for a delegated assertion, the run row only when the window holds
// nothing for that run.
type Reads struct {
	Run     int
	Current int
}

// New returns a store with no rows.
func New() *Store {
	return &Store{
		runs:     map[string]int64{},
		current:  map[string]row{},
		failRuns: map[string]error{},
	}
}

// Rows wraps the store as the write path takes it, both reads in one value.
func (s *Store) Rows() *baserow.Rows { return baserow.New(s) }

// SetRun stages a run row at db_record_version, which an update asserts on.
func (s *Store) SetRun(runID string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runID] = version
}

// SetCurrent stages a workflow's current-execution row.
func (s *Store) SetCurrent(
	workflowID, runID string, state enumsspb.WorkflowExecutionState, lastWriteVersion int64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current[workflowID] = row{runID: runID, state: state, lastWriteVersion: lastWriteVersion}
}

// SetRunning stages a running current run, with no version: only a completed
// run's version is asserted on.
func (s *Store) SetRunning(workflowID, runID string) {
	s.SetCurrent(workflowID, runID, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, 0)
}

// SetCompleted stages a finished previous run at lastWriteVersion, which a
// start reusing the workflow ID asserts on.
func (s *Store) SetCompleted(workflowID, runID string, lastWriteVersion int64) {
	s.SetCurrent(workflowID, runID, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, lastWriteVersion)
}

// Holds stages what an update asserts: the current row names runID and that
// run's row is at version. A window opening with a create needs neither.
func (s *Store) Holds(workflowID, runID string, version int64) {
	s.SetRunning(workflowID, runID)
	s.SetRun(runID, version)
}

// DeleteRun removes a run's row, as a drain carrying a tombstone does.
func (s *Store) DeleteRun(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runs, runID)
}

// DeleteCurrent removes a workflow's current-execution row.
func (s *Store) DeleteCurrent(workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.current, workflowID)
}

// FailAll makes every read fail with err, like an unreachable cold store.
func (s *Store) FailAll(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

// FailRun makes reads of one run's row fail with err, like a transient error
// mid-batch.
func (s *Store) FailRun(runID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRuns[runID] = err
}

// Reads returns the read counts so far.
func (s *Store) Reads() Reads {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *Store) GetWorkflowExecution(
	_ context.Context, req *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads.Run++
	if s.err != nil {
		return nil, s.err
	}
	if err, ok := s.failRuns[req.RunID]; ok {
		return nil, err
	}
	version, ok := s.runs[req.RunID]
	if !ok {
		return nil, serviceerror.NewNotFoundf("workflow execution not found for run %s", req.RunID)
	}
	// A returned row always carries a state.
	return &p.InternalGetWorkflowExecutionResponse{
		State:           &p.InternalWorkflowMutableState{},
		DBRecordVersion: version,
	}, nil
}

func (s *Store) GetCurrentExecutionWithLastWriteVersion(
	_ context.Context, req *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads.Current++
	if s.err != nil {
		return nil, 0, s.err
	}
	cur, ok := s.current[req.WorkflowID]
	if !ok {
		return nil, 0, serviceerror.NewNotFoundf("no current execution for workflow %s", req.WorkflowID)
	}
	// RunID is set only on the response, not inside ExecutionState, matching
	// upstream. Filling both would hide a consumer that reads the copy real
	// stores leave empty.
	return &p.InternalGetCurrentExecutionResponse{
		RunID:          cur.runID,
		ExecutionState: &persistencespb.WorkflowExecutionState{State: cur.state},
	}, cur.lastWriteVersion, nil
}
