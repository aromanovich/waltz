// Package mutbuild builds single well-formed mutations for unit tests: the
// request Temporal's ExecutionManager hands the store, with the ids named by
// the caller. [mutgen] cannot do this, since its shapes come from a random walk.
//
// Every mutation carries ExecutionStateBlob, because the read path
// deserialises it; the layer never receives a request without one.
//
// Each shape runs through Temporal's own validators before it is returned, as
// mutgen does. An invalid fixture is a test bug, so it panics with the
// validator's message.
//
// The check covers only what a constructor builds. A few tests mutate the
// result afterwards on purpose, to drive paths Temporal would never send but
// the layer must still handle.
//
// fold's fixtures do not use this package: they favour legible values over
// valid ones and would fail every validator. There is deliberately no
// "do not check" mode.
package mutbuild

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aromanovich/waltz/mutation"
)

// Builder builds one shard's mutations. The shard is an int32, not a
// wal.ShardID, to keep the log's types out of a package that states what
// Temporal produces.
type Builder struct{ shard int32 }

// For returns the builder for a shard.
func For(shard int32) Builder { return Builder{shard: shard} }

// SnapshotOpt adjusts a snapshot (a whole run's state) before validation.
type SnapshotOpt func(*p.InternalWorkflowSnapshot)

// MutationOpt adjusts an update's mutation before validation.
type MutationOpt func(*p.InternalWorkflowMutation)

// Create is a start: CreateWorkflowModeBrandNew, one run at DBRecordVersion 1,
// asserting nothing holds the workflow id.
func (b Builder) Create(ns, wf, run string, opts ...SnapshotOpt) mutation.Mutation {
	req := &p.InternalCreateWorkflowExecutionRequest{
		ShardID: b.shard,
		// RangeID stays zero: it is the epoch (I11), stamped by the driver.
		Mode:                p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: b.snapshot(ns, wf, run, 1, opts),
	}
	b.validateCreate(req)
	return mutation.Mutation{Create: req}
}

// CreateOver is a start over a workflow id whose previous run has finished:
// CreateWorkflowModeUpdateCurrent, asserting the current row names previousRun
// at previousLastWriteVersion.
func (b Builder) CreateOver(
	ns, wf, run, previousRun string, previousLastWriteVersion int64, opts ...SnapshotOpt,
) mutation.Mutation {
	m := b.Create(ns, wf, run, opts...)
	m.Create.Mode = p.CreateWorkflowModeUpdateCurrent
	m.Create.PreviousRunID = previousRun
	m.Create.PreviousLastWriteVersion = previousLastWriteVersion
	// Re-validated, because the validator reads the mode.
	b.validateCreate(m.Create)
	return m
}

// Update is an ordinary update: UpdateWorkflowModeUpdateCurrent at version,
// asserting the run row is at version − 1.
func (b Builder) Update(ns, wf, run string, version int64, opts ...MutationOpt) mutation.Mutation {
	state := runningState(run)
	m := p.InternalWorkflowMutation{
		NamespaceID: ns,
		WorkflowID:  wf,
		RunID:       run,
		// Only the blob is recorded; the struct alone would vanish on replay.
		ExecutionState:     state,
		ExecutionStateBlob: stateBlob(state),
		DBRecordVersion:    version,
	}
	for _, opt := range opts {
		opt(&m)
	}
	req := &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                b.shard,
		Mode:                   p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: m,
	}
	check("update", p.ValidateUpdateWorkflowStateStatus(
		m.ExecutionState.State, m.ExecutionState.Status))
	check("update", p.ValidateUpdateWorkflowModeState(req.Mode,
		p.WorkflowMutation{ExecutionState: m.ExecutionState}, nil))
	return mutation.Mutation{Update: req}
}

// UpdateBypassingCurrent updates a run that is not the workflow's current one:
// UpdateWorkflowModeBypassCurrent asserts the current row names another run
// and does not write it. It is the only shape that asserts the current row
// without writing it. The state is zombie because this mode's validator
// refuses created or running, while [Builder.Update]'s mode refuses zombie.
func (b Builder) UpdateBypassingCurrent(
	ns, wf, run string, version int64, opts ...MutationOpt,
) mutation.Mutation {
	state := runningState(run)
	state.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	m := p.InternalWorkflowMutation{
		NamespaceID:        ns,
		WorkflowID:         wf,
		RunID:              run,
		ExecutionState:     state,
		ExecutionStateBlob: stateBlob(state),
		DBRecordVersion:    version,
	}
	for _, opt := range opts {
		opt(&m)
	}
	req := &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                b.shard,
		Mode:                   p.UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: m,
	}
	check("update", p.ValidateUpdateWorkflowStateStatus(
		m.ExecutionState.State, m.ExecutionState.Status))
	check("update", p.ValidateUpdateWorkflowModeState(req.Mode,
		p.WorkflowMutation{ExecutionState: m.ExecutionState}, nil))
	return mutation.Mutation{Update: req}
}

// Set replaces one run's state, asserting nothing about the current row. It
// does assert the run row is at version − 1, which the fixture must stage.
func (b Builder) Set(ns, wf, run string, version int64, opts ...SnapshotOpt) mutation.Mutation {
	snap := b.snapshot(ns, wf, run, version, opts)
	// A set is validated by the update rule, as in mutgen.
	check("set", p.ValidateUpdateWorkflowStateStatus(
		snap.ExecutionState.State, snap.ExecutionState.Status))
	return mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
		ShardID:             b.shard,
		SetWorkflowSnapshot: snap,
	}}
}

// ConflictResolve is a reset, carrying up to three runs: the reset run, the
// previously current run and a new run. An empty currentRun or newRun omits
// that part, giving the four combinations the mode validator distinguishes.
//
// This method picks the states, since the validator has a rule per
// combination (the reset run closed, the current run not created or running,
// the new run not zombie). It also fills the execution-info blob on every
// part, because a real store's applier dereferences it.
func (b Builder) ConflictResolve(ns, wf, resetRun, currentRun, newRun string, version int64) mutation.Mutation {
	closed := func(s *p.InternalWorkflowSnapshot) {
		s.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.ExecutionStateBlob = stateBlob(s.ExecutionState)
	}
	withInfo := func(s *p.InternalWorkflowSnapshot) { s.ExecutionInfoBlob = named("info") }

	reset := b.snapshot(ns, wf, resetRun, version, []SnapshotOpt{closed, withInfo})
	req := &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID:               b.shard,
		Mode:                  p.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: reset,
	}
	check("conflict resolve", p.ValidateUpdateWorkflowStateStatus(
		reset.ExecutionState.State, reset.ExecutionState.Status))

	var current *p.WorkflowMutation
	if currentRun != "" {
		state := runningState(currentRun)
		state.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		state.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		req.CurrentWorkflowMutation = &p.InternalWorkflowMutation{
			NamespaceID:        ns,
			WorkflowID:         wf,
			RunID:              currentRun,
			ExecutionState:     state,
			ExecutionStateBlob: stateBlob(state),
			ExecutionInfoBlob:  named("info"),
			DBRecordVersion:    version,
		}
		check("conflict resolve", p.ValidateUpdateWorkflowStateStatus(state.State, state.Status))
		current = &p.WorkflowMutation{ExecutionState: state}
	}

	var added *p.WorkflowSnapshot
	if newRun != "" {
		snap := b.snapshot(ns, wf, newRun, 1, []SnapshotOpt{withInfo})
		req.NewWorkflowSnapshot = &snap
		check("conflict resolve", p.ValidateCreateWorkflowStateStatus(
			snap.ExecutionState.State, snap.ExecutionState.Status))
		added = &p.WorkflowSnapshot{ExecutionState: snap.ExecutionState}
	}

	check("conflict resolve", p.ValidateConflictResolveWorkflowModeState(req.Mode,
		p.WorkflowSnapshot{ExecutionState: reset.ExecutionState}, added, current))
	return mutation.Mutation{ConflictResolve: req}
}

// Delete removes one run's rows. It asserts nothing, so nothing is validated.
func (b Builder) Delete(ns, wf, run string) mutation.Mutation {
	return mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
		ShardID: b.shard, NamespaceID: ns, WorkflowID: wf, RunID: run,
	}}
}

// DeleteCurrent removes the workflow's current-execution row, conditional on it
// still naming this run.
func (b Builder) DeleteCurrent(ns, wf, run string) mutation.Mutation {
	return mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID: b.shard, NamespaceID: ns, WorkflowID: wf, RunID: run,
	}}
}

// AddTasks writes tasks to one category. It names no run. An empty list gives
// a request with no rows, the one mutation that folds to nothing.
func (b Builder) AddTasks(category tasks.Category, list ...p.InternalHistoryTask) mutation.Mutation {
	req := &p.InternalAddHistoryTasksRequest{ShardID: b.shard}
	if len(list) > 0 {
		req.Tasks = map[tasks.Category][]p.InternalHistoryTask{category: list}
	}
	return mutation.Mutation{AddTasks: req}
}

// RangeComplete deletes a category's tasks in [inclusiveMin, exclusiveMax).
func (b Builder) RangeComplete(category tasks.Category, inclusiveMin, exclusiveMax tasks.Key) mutation.Mutation {
	return mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             b.shard,
		TaskCategory:        category,
		InclusiveMinTaskKey: inclusiveMin,
		ExclusiveMaxTaskKey: exclusiveMax,
	}}
}

// WithTaskMap sets an update's history tasks for several categories.
func WithTaskMap(byCategory map[tasks.Category][]p.InternalHistoryTask) MutationOpt {
	return func(m *p.InternalWorkflowMutation) { m.Tasks = byCategory }
}

// WithState sets the run's state and status, for example to close it. The
// validators check this pair, so an invalid one panics.
func WithState(
	state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus,
) SnapshotOpt {
	return func(s *p.InternalWorkflowSnapshot) {
		s.ExecutionState.State, s.ExecutionState.Status = state, status
		s.ExecutionStateBlob = stateBlob(s.ExecutionState)
	}
}

// WithInfoBlob sets the execution-info blob, for bytes a test wants to find
// again or a large payload.
func WithInfoBlob(blob *commonpb.DataBlob) SnapshotOpt {
	return func(s *p.InternalWorkflowSnapshot) { s.ExecutionInfoBlob = blob }
}

// Task is one history-task row whose blob is name.
func Task(name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{Blob: named(name)}
}

// snapshot builds a running run's whole state at version, then applies opts.
func (b Builder) snapshot(ns, wf, run string, version int64, opts []SnapshotOpt) p.InternalWorkflowSnapshot {
	state := runningState(run)
	s := p.InternalWorkflowSnapshot{
		NamespaceID:        ns,
		WorkflowID:         wf,
		RunID:              run,
		ExecutionState:     state,
		ExecutionStateBlob: stateBlob(state),
		DBRecordVersion:    version,
	}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}

// validateCreate runs a create's two validators; [Builder.CreateOver] reruns
// it after changing the mode.
func (b Builder) validateCreate(req *p.InternalCreateWorkflowExecutionRequest) {
	state := req.NewWorkflowSnapshot.ExecutionState
	check("create", p.ValidateCreateWorkflowStateStatus(state.State, state.Status))
	check("create", p.ValidateCreateWorkflowModeState(req.Mode,
		p.WorkflowSnapshot{ExecutionState: state}))
}

// runningState is the RUNNING state and status every shape starts from. To
// close a run, change both with [WithState]; the validators refuse a
// COMPLETED state with RUNNING status.
//
// StartTime is set because upstream always sets it and the workflow-id reuse
// check reads it from the current row; without it a test cannot catch a
// current-row rendering that drops it.
func runningState(run string) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.NewString(),
		RunId:           run,
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime:       timestamppb.New(startedAt),
	}
}

// startedAt is every fixture's start time, fixed rather than read from the
// clock.
var startedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// stateBlob serialises an execution state as the ExecutionManager does.
func stateBlob(state *persistencespb.WorkflowExecutionState) *commonpb.DataBlob {
	blob, err := serialization.WorkflowExecutionStateToBlob(state)
	if err != nil {
		panic(err) // a valid proto cannot fail to serialise
	}
	return blob
}

func named(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(s), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
}

// check panics on a validator error: an invalid fixture is a test bug.
func check(shape string, err error) {
	if err != nil {
		panic(fmt.Sprintf("mutbuild: built an invalid %s: %v", shape, err))
	}
}
