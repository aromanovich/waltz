// Package mutbuild builds one mutation of a given shape, well-formed, with no
// cluster: the request Temporal's ExecutionManager hands the store, as a unit
// test needs it — one at a time, with the ids named rather than drawn.
//
// # Why this is not verify/mutgen
//
// [mutgen] holds the same knowledge and cannot lend it. Every shape there is a
// method on its `*Generator`, driven by a rand walk over a key space it also
// owns, so a test that wants *one* create has nothing to ask. Three packages
// therefore wrote their own, and they disagreed: `cycle`'s create carried
// `ExecutionStateBlob` because the read path deserialises it and a nil one will
// not do, `apply`'s carried none, and nothing said which was right. A
// mutation built here carries the blob — a fixture that omits it is one no read
// path above the fold could have served, which is not a shape this layer ever
// receives.
//
// # Validity is Temporal's own answer, checked at build time
//
// Every shape that has a validator runs through it before it is returned, the
// way `mutgen` runs the same four over its stream: an invalid fixture is a bug
// in the test rather than a case a caller handles, so it panics with what
// the validator said. That is the whole depth of this package — a caller learns
// eight constructors and gets the store's own admission rules for free, where
// before each package restated a subset of them in a struct literal and none
// checked any.
//
// The check covers what a constructor builds, and cannot cover what a caller
// does to the value afterwards. Two fixtures in this tree deliberately step
// outside — `apply`'s create at `CreateWorkflowModeBypassCurrent` over a
// running state, and `cycle`'s continue-as-new out of a running run —
// and both stay where they are, built here and then mutated at the call site.
// Neither is a request Temporal would send; both drive a path the layer must
// still have an answer for, which is why they are not "fixed".
//
// # What is deliberately absent
//
//   - `fold`'s fixtures. They are a different kind of object: their
//     scalars are chosen to be *legible* in an assertion (`info-v3`, a bare
//     `ExecutionState{RunId: run}`), so a fold's output can be attributed to the
//     mutation that produced it. They would fail every validator above, and
//     rightly — fold merges, it does not admit. A builder with a
//     "do not check" mode is a builder whose check nobody trusts, so there is
//     none.
//   - nothing, since [Builder.ConflictResolve] arrived. It was absent on the
//     grounds that adding it with no caller would be a guess at what a caller
//     wants, and it belonged here the day a validating caller needed one: that
//     caller is the applier's guard for the two arms a reset's second and third
//     parts are written by, which a mutation sweep found reachable from no
//     fixture in the tree. [Builder.Set] is the one shape whose only caller is
//     still this package's own test.
package mutbuild

import (
	"fmt"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/aromanovich/waltz/mutation"
)

// Builder is one shard's mutations. The shard is an int32 and not a
// `wal.ShardID` for the reason `mutgen` takes one too: what this package states
// is what Temporal's write path produces, and the log's own types are the layer
// answering that question for itself.
type Builder struct{ shard int32 }

// For is the builder for a shard.
func For(shard int32) Builder { return Builder{shard: shard} }

// SnapshotOpt shapes a snapshot before its request is validated — a create's
// new run, and every other shape carrying a whole run's state.
type SnapshotOpt func(*p.InternalWorkflowSnapshot)

// MutationOpt is the same for a mutation: the delta an update carries.
type MutationOpt func(*p.InternalWorkflowMutation)

// Create is a start: `CreateWorkflowModeBrandNew`, one run at
// `DBRecordVersion` 1, asserting that nothing holds the workflow id.
func (b Builder) Create(ns, wf, run string, opts ...SnapshotOpt) mutation.Mutation {
	req := &p.InternalCreateWorkflowExecutionRequest{
		ShardID: b.shard,
		// RangeID is left zero deliberately: it is the epoch (I11), stamped by
		// whoever drives the request, and a value here would be a second source
		// of truth for it.
		Mode:                p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: b.snapshot(ns, wf, run, 1, opts),
	}
	b.validateCreate(req)
	return mutation.Mutation{Create: req}
}

// CreateOver is a start over a workflow id whose previous run has finished:
// `CreateWorkflowModeUpdateCurrent`, carrying the assertion the current row is
// judged by — the previous run and the last-write-version column a versioned
// current-row read returns beside it.
func (b Builder) CreateOver(
	ns, wf, run, previousRun string, previousLastWriteVersion int64, opts ...SnapshotOpt,
) mutation.Mutation {
	m := b.Create(ns, wf, run, opts...)
	m.Create.Mode = p.CreateWorkflowModeUpdateCurrent
	m.Create.PreviousRunID = previousRun
	m.Create.PreviousLastWriteVersion = previousLastWriteVersion
	// Re-checked: the mode moved, and the mode is half of what the validator
	// reads.
	b.validateCreate(m.Create)
	return m
}

// Update is an ordinary link in a run's chain: `UpdateWorkflowModeUpdateCurrent`
// at the given `DBRecordVersion`, which asserts the row one below it.
func (b Builder) Update(ns, wf, run string, version int64, opts ...MutationOpt) mutation.Mutation {
	state := runningState(run)
	m := p.InternalWorkflowMutation{
		NamespaceID: ns,
		WorkflowID:  wf,
		RunID:       run,
		// Both, and not the struct alone: only the blob is recorded, so a
		// fixture carrying the state by itself survives a fold and vanishes on
		// replay.
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

// Set is the snapshot-bearing write that asserts nothing about the current row —
// a set repairs one run's state and claims nothing about which run is current.
// The run itself it does assert, at the given version − 1, which is the row a
// fixture has to have staged.
func (b Builder) Set(ns, wf, run string, version int64, opts ...SnapshotOpt) mutation.Mutation {
	snap := b.snapshot(ns, wf, run, version, opts)
	// The store's own rule for a set is the update pair, which is what mutgen
	// checks its own sets against.
	check("set", p.ValidateUpdateWorkflowStateStatus(
		snap.ExecutionState.State, snap.ExecutionState.Status))
	return mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
		ShardID:             b.shard,
		SetWorkflowSnapshot: snap,
	}}
}

// ConflictResolve is a reset, and the only shape here that carries more than one
// run: the run being reset, the run that was current until now, and the new run
// the reset starts. An empty currentRun or newRun leaves that part out, which is
// how the four combinations Temporal's mode validator distinguishes are reached.
//
// The states are this method's rather than a caller's, because that validator has
// a rule per combination — with all three parts the current and the reset run must
// both be closed and the new run may not be a zombie — so a caller choosing them
// would be choosing whether the request is one the store admits.
//
// It fills the execution-info blob on all three parts, which no other shape here
// does. A reset is only worth building against a real store, the applier
// dereferences that blob once per part, and leaving it to the caller would mean
// three option lists for one request.
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

// Delete removes one run's rows. It names a run and asserts nothing, so there
// is no validator to run.
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

// AddTasks is one queue's write: a task record, which names no run and travels
// in the batch's task half. An empty list is a request carrying no rows, which
// is the one mutation that folds to nothing.
func (b Builder) AddTasks(category tasks.Category, list ...p.InternalHistoryTask) mutation.Mutation {
	req := &p.InternalAddHistoryTasksRequest{ShardID: b.shard}
	if len(list) > 0 {
		req.Tasks = map[tasks.Category][]p.InternalHistoryTask{category: list}
	}
	return mutation.Mutation{AddTasks: req}
}

// RangeComplete is the other half of that queue's traffic: the deletion range
// it declares garbage, in its own checkpoint's terms.
func (b Builder) RangeComplete(category tasks.Category, inclusiveMin, exclusiveMax tasks.Key) mutation.Mutation {
	return mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             b.shard,
		TaskCategory:        category,
		InclusiveMinTaskKey: inclusiveMin,
		ExclusiveMaxTaskKey: exclusiveMax,
	}}
}

// WithTaskMap is the same across several categories at once, in the shape the
// request carries them.
func WithTaskMap(byCategory map[tasks.Category][]p.InternalHistoryTask) MutationOpt {
	return func(m *p.InternalWorkflowMutation) { m.Tasks = byCategory }
}

// WithState replaces the run's state and status pair — the one field of a
// snapshot a fixture legitimately varies, since it is what a chain's last link
// moves when the run closes, and the pair every validator above is stated over.
// It is therefore also what makes this package's own check provable: an
// invalid pair handed in here is refused, which is what
// TestAnInvalidStateIsRefusedRatherThanBuilt drives — the two validators told
// apart by a pair that satisfies one and not the other.
func WithState(
	state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus,
) SnapshotOpt {
	return func(s *p.InternalWorkflowSnapshot) {
		s.ExecutionState.State, s.ExecutionState.Status = state, status
		s.ExecutionStateBlob = stateBlob(s.ExecutionState)
	}
}

// WithInfoBlob replaces the execution-info blob, which is where a fixture puts
// bytes it wants to find again — or a payload it wants to be large.
func WithInfoBlob(blob *commonpb.DataBlob) SnapshotOpt {
	return func(s *p.InternalWorkflowSnapshot) { s.ExecutionInfoBlob = blob }
}

// Task is one history-task row, named by its blob.
func Task(name string) p.InternalHistoryTask {
	return p.InternalHistoryTask{Blob: named(name)}
}

// snapshot is the whole-run state every snapshot-bearing shape carries.
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

// validateCreate runs both of a create's validators. Split out because
// [Builder.CreateOver] moves the mode after the fact and the mode is half of
// what the second one reads.
func (b Builder) validateCreate(req *p.InternalCreateWorkflowExecutionRequest) {
	state := req.NewWorkflowSnapshot.ExecutionState
	check("create", p.ValidateCreateWorkflowStateStatus(state.State, state.Status))
	check("create", p.ValidateCreateWorkflowModeState(req.Mode,
		p.WorkflowSnapshot{ExecutionState: state}))
}

// runningState is a live run: the state and status pair every shape here is
// built at. Temporal admits a status of RUNNING for every state but COMPLETED,
// so a fixture closing a run moves both through [WithState] or the validators
// refuse it.
func runningState(run string) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.NewString(),
		RunId:           run,
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
}

// stateBlob serialises an execution state the way the ExecutionManager does
// before the store sees the request.
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

// check ends the test rather than returning: a fixture the store would refuse
// is a bug in whoever asked for it, and there is no caller that could do
// anything with the error but fail.
func check(shape string, err error) {
	if err != nil {
		panic(fmt.Sprintf("mutbuild: built an invalid %s: %v", shape, err))
	}
}
