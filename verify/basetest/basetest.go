// Package basetest is the pre-window rows in memory.
//
// It is the second adapter at the seam [baserow.Store] names, and the reason it
// exists is the reason [github.com/aromanovich/waltz/verify/coldtest] does:
// the first adapter needs the patched plugin, so every package that has to
// stand a write path up wrote a pair of maps and an absence rule of its own.
//
// Absence is what makes that costly rather than untidy. A row that is not there
// arrives as a NotFound and [baserow.Rows] turns it into a nil row, which is
// what a delegated assertion is stated over — so a double answering absence its
// own way leaves the suite green and the rule unjudged. Here it is answered
// once.
//
// Unlike coldtest this package names the seam it stands at. coldtest may not
// name [github.com/aromanovich/waltz/cycle], the seam being that
// package's; this one is an adapter at a seam whose whole purpose is to be
// reachable from the three packages that read through it.
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

// Store is one cold store's two mutable-state reads: runs by id, current rows
// by workflow id, and a count of what was asked of each.
//
// The zero rows are the useful starting point rather than a degenerate one: a
// store nobody has staged is the store a create asserts against, and every read
// of it is an absence.
//
// The reads run on the cycle's own goroutine while a test stages from its own,
// so the map access is locked.
type Store struct {
	mu       sync.Mutex
	runs     map[string]int64
	current  map[string]row
	err      error
	failRuns map[string]error
	reads    Reads
}

// row is a current-execution row as the patched store answers it: the run, its
// state, and the last_write_version column beside it.
type row struct {
	runID            string
	state            enumsspb.WorkflowExecutionState
	lastWriteVersion int64
}

// Reads is what a store was asked for, counted apart. Which of the two a
// request costs is the property most of these tests are about: the current row
// is read for an assertion the window handed on, and the run's row only where
// the window holds nothing for it.
type Reads struct {
	Run     int
	Current int
}

// Total is the two counts together, for a caller that cares only that a read
// happened.
func (r Reads) Total() int { return r.Run + r.Current }

// New is a store holding no rows: every read is an absence.
func New() *Store {
	return &Store{
		runs:     map[string]int64{},
		current:  map[string]row{},
		failRuns: map[string]error{},
	}
}

// Rows is the store as the write path takes it. The two reads travel as one
// value, so a fixture cannot bring one reader and not the other.
func (s *Store) Rows() *baserow.Rows { return baserow.New(s) }

// SetRun gives a run a row at a db_record_version, which is what an update's
// delegated assertion stands on.
func (s *Store) SetRun(runID string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runID] = version
}

// SetCurrent gives a workflow a current-execution row in full.
func (s *Store) SetCurrent(
	workflowID, runID string, state enumsspb.WorkflowExecutionState, lastWriteVersion int64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current[workflowID] = row{runID: runID, state: state, lastWriteVersion: lastWriteVersion}
}

// SetRunning gives a workflow a live current run. No version: only the
// completed shape is asserted against one.
func (s *Store) SetRunning(workflowID, runID string) {
	s.SetCurrent(workflowID, runID, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, 0)
}

// SetCompleted is the row a start over a reused workflow ID asserts on: the
// previous run, finished, at a given last_write_version.
func (s *Store) SetCompleted(workflowID, runID string, lastWriteVersion int64) {
	s.SetCurrent(workflowID, runID, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, lastWriteVersion)
}

// Holds is a workflow as an update asserts it: the current row names runID, and
// that run's own row is at version. A window opening with an update needs both;
// one opening with a create asserts absence and needs neither.
func (s *Store) Holds(workflowID, runID string, version int64) {
	s.SetRunning(workflowID, runID)
	s.SetRun(runID, version)
}

// DeleteRun takes a run's row out, as a drain carrying a tombstone does.
func (s *Store) DeleteRun(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runs, runID)
}

// DeleteCurrent takes a workflow's current-execution row out.
func (s *Store) DeleteCurrent(workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.current, workflowID)
}

// FailAll makes every read fail with err instead of answering, which is what a
// cold store nobody can reach does to the condition authority.
func (s *Store) FailAll(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

// FailRun makes one run's row fail while the rest answer, which is what a blip
// or a dead session does to an attribution mid-batch.
func (s *Store) FailRun(runID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRuns[runID] = err
}

// Reads is what has been asked of this store so far.
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
	// A row the store answers always carries a state, whatever the caller reads
	// off it.
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
	return &p.InternalGetCurrentExecutionResponse{
		RunID:          cur.runID,
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: cur.runID, State: cur.state},
	}, cur.lastWriteVersion, nil
}
