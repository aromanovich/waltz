package mutgen

// The steps: one call to step() is one thing that happens to one workflow, and
// what may happen is decided by the state the stream has already put that
// workflow in. That is the whole validity model — a request is only ever built
// for a state the store would accept it in, and the exported validators below
// are asked to confirm it before it is emitted.

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aromanovich/waltz/mutation"
)

// step advances the stream by one workflow's worth of work, queueing the one or
// two mutations it produced.
func (g *Generator) step() error {
	// The two history-task steps come first and each returns on its own: they
	// are shard-level rather than a workflow's, so folding them into the switch
	// below would make them a *replacement* for a workflow's work only when that
	// workflow happened to be in the right state.
	if g.chance(g.cfg.RangeCompleteRate) {
		g.emitRangeComplete()
		if len(g.queue) > 0 {
			return nil
		}
	}
	w := g.pick()
	if g.chance(g.cfg.AddTasksRate) {
		if r := w.run; r != nil {
			if err := g.emitAddTasks(w, r); err != nil {
				return err
			}
			if len(g.queue) > 0 {
				return nil
			}
		}
	}
	switch {
	case w.run != nil:
		// A live run: another link in the chain, or the snapshot barrier.
		if g.chance(g.cfg.SnapshotRate) {
			return g.emitSnapshotBarrier(w)
		}
		return g.emitUpdate(w)
	case w.closed != nil:
		// A completed run still holding the current-execution row: either the
		// workflow is deleted, or the id is reused by the run that follows it.
		if g.chance(g.cfg.TombstoneRate) {
			return g.emitDeletePair(w)
		}
		return g.emitCreate(w, p.CreateWorkflowModeUpdateCurrent)
	default:
		return g.emitCreate(w, p.CreateWorkflowModeBrandNew)
	}
}

// newRun is a run as it exists the moment it is created: version 1, the events
// a create writes already behind it, and a last-write-version of its own —
// per run rather than fixed, because the current row's last_write_version is
// what a create over a previous run has to assert, and a constant would let a
// wrong value pass.
func (g *Generator) newRun() *runState {
	return &runState{
		runID:            g.newUUID(),
		createRequestID:  g.newUUID(),
		version:          1,
		nextEventID:      3,
		lastWriteVersion: g.rng.Int64N(1 << 20),
		state:            enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		// Event ids start at 1 upstream (0 is EmptyEventID, "no event"), so a
		// scheduled-event key of 0 is a shape no real mutation carries. The
		// generator models what upstream writes, and every measurement taken
		// over it is a function of the streams these seeds produce.
		nextActivity: 1,
	}
}

// pick chooses the workflow this step touches: an existing key with probability
// WorkflowReuse, a fresh one otherwise. A key space that is full forces reuse,
// which is what makes Workflows a cap rather than a suggestion.
func (g *Generator) pick() *workflowState {
	room := g.cfg.Workflows == 0 || len(g.pool) < g.cfg.Workflows
	if len(g.pool) == 0 || (room && !g.chance(g.cfg.WorkflowReuse)) {
		w := &workflowState{workflowID: fmt.Sprintf("wf-%d", len(g.pool))}
		g.pool = append(g.pool, w)
		return w
	}
	return g.pool[g.rng.IntN(len(g.pool))]
}

// ---------------------------------------------------------------- the requests

func (g *Generator) emitCreate(w *workflowState, mode p.CreateWorkflowMode) error {
	r := g.newRun()

	snapshot, err := g.snapshot(w, r, g.cfg.Upserts)
	if err != nil {
		return err
	}
	req := &p.InternalCreateWorkflowExecutionRequest{
		ShardID: g.cfg.ShardID,
		// RangeID is deliberately left zero: it is the epoch (invariant I11),
		// stamped by whoever drives the request, and a copy here would be a
		// second source of truth.
		Mode:                mode,
		NewWorkflowSnapshot: snapshot,
	}
	if mode == p.CreateWorkflowModeUpdateCurrent {
		req.PreviousRunID = w.closed.runID
		req.PreviousLastWriteVersion = w.closed.lastWriteVersion
	}

	if err := p.ValidateCreateWorkflowStateStatus(r.state, r.status); err != nil {
		return fmt.Errorf("mutgen: generated an invalid create: %w", err)
	}
	if err := p.ValidateCreateWorkflowModeState(mode,
		p.WorkflowSnapshot{ExecutionState: snapshot.ExecutionState}); err != nil {
		return fmt.Errorf("mutgen: generated an invalid create: %w", err)
	}

	if w.created > 0 {
		g.recreations++
	}
	w.created++
	w.run, w.closed = r, nil
	g.runs++
	g.queue = append(g.queue, mutation.Mutation{Create: req})
	return nil
}

func (g *Generator) emitUpdate(w *workflowState) error {
	r := w.run

	// The last link closes the run — without it a chain would run forever and
	// the stream would never exercise a workflow id being reused or deleted —
	// and a closing run may close by continuing as new instead, which is the
	// one request that carries two runs. Both are decided before anything is
	// written, because the state the mutation carries differs.
	closing := r.updates+1 >= g.cfg.MaxChainLength
	continuing := closing && g.chance(g.cfg.ContinueAsNewRate)

	r.version++
	r.nextEventID += 1 + g.rng.Int64N(3)
	r.updates++
	switch {
	case continuing:
		// The store's own rule for an update carrying a new run: the run being
		// updated may not be left created or running
		// (ValidateUpdateWorkflowModeState case 2), which is exactly what
		// continuing as new means.
		r.state = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		r.status = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	case closing:
		r.state = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		r.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	default:
		r.state = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
		r.status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}

	mut, err := g.mutation(w, r)
	if err != nil {
		return err
	}
	req := &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:                g.cfg.ShardID,
		Mode:                   p.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mut,
	}

	var next *runState
	if continuing {
		next = g.newRun()
		snapshot, err := g.snapshot(w, next, g.cfg.Upserts)
		if err != nil {
			return err
		}
		req.NewWorkflowSnapshot = &snapshot
		if err := p.ValidateCreateWorkflowStateStatus(next.state, next.status); err != nil {
			return fmt.Errorf("mutgen: generated an invalid continue-as-new: %w", err)
		}
	}

	if err := p.ValidateUpdateWorkflowStateStatus(r.state, r.status); err != nil {
		return fmt.Errorf("mutgen: generated an invalid update: %w", err)
	}
	var newSnapshot *p.WorkflowSnapshot
	if next != nil {
		newSnapshot = &p.WorkflowSnapshot{ExecutionState: req.NewWorkflowSnapshot.ExecutionState}
	}
	if err := p.ValidateUpdateWorkflowModeState(req.Mode,
		p.WorkflowMutation{ExecutionState: mut.ExecutionState}, newSnapshot); err != nil {
		return fmt.Errorf("mutgen: generated an invalid update: %w", err)
	}

	switch {
	case continuing:
		// The new run takes the current-execution row; the run just continued
		// stays in the store and the stream never touches it again. Deleting it
		// would need the delete of a non-current run, which the package comment
		// explains is left out.
		w.run, w.closed = next, nil
		g.runs++
	case closing:
		w.run, w.closed = nil, r
	}
	g.queue = append(g.queue, mutation.Mutation{Update: req})
	return nil
}

// emitSnapshotBarrier picks between the two snapshot-bearing shapes a live run
// can take. They share SnapshotRate because the accumulator treats them the same
// way — both reset it (invariant I8) — so the interesting knob is how often a
// barrier happens, not which one it was.
func (g *Generator) emitSnapshotBarrier(w *workflowState) error {
	if g.chance(0.5) {
		return g.emitSet(w)
	}
	return g.emitConflictResolve(w)
}

// emitConflictResolve is the other snapshot barrier: a reset of the current run
// at the next version, with no new run and no current mutation. The multi-part
// shapes — reset + new, reset + current — are deliberately not generated; what
// this is for is a reset arriving inside a chain rather than at the head of
// one.
func (g *Generator) emitConflictResolve(w *workflowState) error {
	r := w.run
	r.version++
	r.nextEventID += 1 + g.rng.Int64N(3)

	snapshot, err := g.snapshot(w, r, 0)
	if err != nil {
		return err
	}
	req := &p.InternalConflictResolveWorkflowExecutionRequest{
		ShardID: g.cfg.ShardID,
		// UpdateCurrent: the store asserts the current row names the run being
		// reset and writes it back, which is what holds for a live current run.
		Mode:                  p.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: snapshot,
	}

	// The store checks the reset snapshot with the *update* validator (mss.go),
	// and Temporal's mode validator has a rule of its own for the three parts.
	if err := p.ValidateUpdateWorkflowStateStatus(r.state, r.status); err != nil {
		return fmt.Errorf("mutgen: generated an invalid conflict-resolve: %w", err)
	}
	if err := p.ValidateConflictResolveWorkflowModeState(req.Mode,
		p.WorkflowSnapshot{ExecutionState: snapshot.ExecutionState}, nil, nil); err != nil {
		return fmt.Errorf("mutgen: generated an invalid conflict-resolve: %w", err)
	}

	// A snapshot-bearing request replaces the run's state items, buffered events
	// included: the store deletes them and writes the snapshot's.
	r.buffered = 0
	g.queue = append(g.queue, mutation.Mutation{ConflictResolve: req})
	return nil
}

// emitSet is the snapshot barrier inside a chain: SetWorkflowExecution replaces
// the run's whole state at the next version, asserting the previous one, and
// asserts nothing about the current-execution row.
func (g *Generator) emitSet(w *workflowState) error {
	r := w.run
	r.version++
	r.nextEventID += 1 + g.rng.Int64N(3)

	// A snapshot is the run's whole state, so it carries every live key rather
	// than a delta. The store deletes the run's state items and writes these.
	snapshot, err := g.snapshot(w, r, 0)
	if err != nil {
		return err
	}
	req := &p.InternalSetWorkflowExecutionRequest{
		ShardID:             g.cfg.ShardID,
		SetWorkflowSnapshot: snapshot,
	}

	if err := p.ValidateUpdateWorkflowStateStatus(r.state, r.status); err != nil {
		return fmt.Errorf("mutgen: generated an invalid set: %w", err)
	}

	// As for a conflict-resolve: the store replaces the run's state items, so
	// whatever was buffered is gone.
	r.buffered = 0
	g.queue = append(g.queue, mutation.Mutation{Set: req})
	return nil
}

// emitDeletePair deletes a workflow the way Temporal's own delete flow does:
// the current-execution pointer first, the mutable state second
// (`service/history/shard/context_impl.go`, stages 2 and 3). The order is part
// of the corpus rather than an accident — fold collapses a run into a tombstone
// and leans on the pair, so a corpus that emitted them the other way round
// would be testing a stream the server never produces.
func (g *Generator) emitDeletePair(w *workflowState) error {
	r := w.closed
	g.queue = append(g.queue,
		mutation.Mutation{DeleteCurrent: &p.DeleteCurrentWorkflowExecutionRequest{
			ShardID:     g.cfg.ShardID,
			NamespaceID: g.namespaceID,
			WorkflowID:  w.workflowID,
			RunID:       r.runID,
		}},
		mutation.Mutation{Delete: &p.DeleteWorkflowExecutionRequest{
			ShardID:     g.cfg.ShardID,
			NamespaceID: g.namespaceID,
			WorkflowID:  w.workflowID,
			RunID:       r.runID,
		}},
	)
	// The key stays in the pool with no run behind it, so a later step creates
	// a brand-new run under the same workflow id.
	w.closed = nil
	return nil
}

// ---------------------------------------------------------------- the payloads

// mutation builds the delta of one update: the run's scalars at their new
// values, the sub-entity upserts and deletes the knobs asked for, and the
// window's history tasks.
func (g *Generator) mutation(w *workflowState, r *runState) (p.InternalWorkflowMutation, error) {
	infoBlob, stateBlob, checksumBlob, err := g.rowBlobs(w, r)
	if err != nil {
		return p.InternalWorkflowMutation{}, err
	}

	mut := p.InternalWorkflowMutation{
		NamespaceID:        g.namespaceID,
		WorkflowID:         w.workflowID,
		RunID:              r.runID,
		ExecutionInfo:      g.executionInfo(w, r),
		ExecutionInfoBlob:  infoBlob,
		ExecutionState:     g.executionState(r),
		ExecutionStateBlob: stateBlob,
		Checksum:           checksumBlob,
		NextEventID:        r.nextEventID,
		LastWriteVersion:   r.lastWriteVersion,
		DBRecordVersion:    r.version,
	}

	// Keys upserted by this mutation are off limits to its delete set: a key in
	// both sets at once is a shape Temporal's diff never produces, and the one
	// the store resolves wrongly — so a corpus containing it would blame fold
	// for the store's ordering.
	var upsertedActivities []int64
	var upsertedTimers []string
	for i := range g.cfg.Upserts {
		if i%2 == 0 {
			key := g.pickActivityKey(r)
			blob, err := g.activityBlob(key, r)
			if err != nil {
				return p.InternalWorkflowMutation{}, err
			}
			if mut.UpsertActivityInfos == nil {
				mut.UpsertActivityInfos = map[int64]*commonpb.DataBlob{}
			}
			mut.UpsertActivityInfos[key] = blob
			upsertedActivities = append(upsertedActivities, key)
		} else {
			key := g.pickTimerKey(r)
			blob, err := g.timerBlob(key, r)
			if err != nil {
				return p.InternalWorkflowMutation{}, err
			}
			if mut.UpsertTimerInfos == nil {
				mut.UpsertTimerInfos = map[string]*commonpb.DataBlob{}
			}
			mut.UpsertTimerInfos[key] = blob
			upsertedTimers = append(upsertedTimers, key)
		}
		g.upserts++
	}

	if g.chance(g.cfg.DeleteAfterUpsert) {
		if key, ok := pickDeletable(g.rng, r.liveActivities, upsertedActivities); ok {
			mut.DeleteActivityInfos = map[int64]struct{}{key: {}}
			r.liveActivities = remove(r.liveActivities, key)
			r.goneActivities = append(r.goneActivities, key)
			g.subDeletes++
		}
		if key, ok := pickDeletable(g.rng, r.liveTimers, upsertedTimers); ok {
			mut.DeleteTimerInfos = map[string]struct{}{key: {}}
			r.liveTimers = remove(r.liveTimers, key)
			r.goneTimers = append(r.goneTimers, key)
			g.subDeletes++
		}
	}

	// Buffered events: one slot per mutation, which is the whole of why a chain
	// of them matters — they do not merge, so two mutations must stay two
	// batches. A clear is what completing a workflow task does, so it is only
	// generated once there is something to clear.
	if g.chance(g.cfg.BufferedRate) {
		if mut.NewBufferedEvents, err = g.bufferedEvents(r); err != nil {
			return p.InternalWorkflowMutation{}, err
		}
		r.buffered++
	}
	if r.buffered > 0 && g.chance(g.cfg.BufferedRate) {
		mut.ClearBufferedEvents = true
		// The clear takes the store's rows; a batch in this same mutation is
		// written after it, and stays.
		r.buffered = 0
		if mut.NewBufferedEvents != nil {
			r.buffered = 1
		}
	}

	if mut.Tasks, err = g.historyTasks(w, r); err != nil {
		return p.InternalWorkflowMutation{}, err
	}
	return mut, nil
}

// snapshot builds a whole-run image: every key the run holds live, plus extra
// keys when it is a create (a run has to start with something).
func (g *Generator) snapshot(w *workflowState, r *runState, extraKeys int) (p.InternalWorkflowSnapshot, error) {
	for i := range extraKeys {
		if i%2 == 0 {
			key := r.nextActivity
			r.nextActivity++
			r.liveActivities = append(r.liveActivities, key)
		} else {
			key := fmt.Sprintf("timer-%d", r.nextTimer)
			r.nextTimer++
			r.liveTimers = append(r.liveTimers, key)
		}
		g.distinctKeys++
		g.upserts++
	}

	infoBlob, stateBlob, checksumBlob, err := g.rowBlobs(w, r)
	if err != nil {
		return p.InternalWorkflowSnapshot{}, err
	}
	snapshot := p.InternalWorkflowSnapshot{
		NamespaceID:        g.namespaceID,
		WorkflowID:         w.workflowID,
		RunID:              r.runID,
		ExecutionInfo:      g.executionInfo(w, r),
		ExecutionInfoBlob:  infoBlob,
		ExecutionState:     g.executionState(r),
		ExecutionStateBlob: stateBlob,
		Checksum:           checksumBlob,
		NextEventID:        r.nextEventID,
		LastWriteVersion:   r.lastWriteVersion,
		DBRecordVersion:    r.version,
	}

	if len(r.liveActivities) > 0 {
		snapshot.ActivityInfos = make(map[int64]*commonpb.DataBlob, len(r.liveActivities))
		for _, key := range r.liveActivities {
			if snapshot.ActivityInfos[key], err = g.activityBlob(key, r); err != nil {
				return p.InternalWorkflowSnapshot{}, err
			}
		}
	}
	if len(r.liveTimers) > 0 {
		snapshot.TimerInfos = make(map[string]*commonpb.DataBlob, len(r.liveTimers))
		for _, key := range r.liveTimers {
			if snapshot.TimerInfos[key], err = g.timerBlob(key, r); err != nil {
				return p.InternalWorkflowSnapshot{}, err
			}
		}
	}
	if snapshot.Tasks, err = g.historyTasks(w, r); err != nil {
		return p.InternalWorkflowSnapshot{}, err
	}
	return snapshot, nil
}

// historyTasks draws TaskDensity tasks on average, each in one of the four
// categories a persisted queue has. It is the only source of them here:
// upstream's own generator maps every category to an empty slice, so a stream
// taken from it exercises none of the task path fold has to concatenate.
func (g *Generator) historyTasks(w *workflowState, r *runState) (map[tasks.Category][]p.InternalHistoryTask, error) {
	n := int(g.cfg.TaskDensity)
	if g.rng.Float64() < g.cfg.TaskDensity-float64(n) {
		n++
	}
	if n == 0 {
		return nil, nil
	}

	key := definition.NewWorkflowKey(g.namespaceID, w.workflowID, r.runID)
	out := make(map[tasks.Category][]p.InternalHistoryTask)
	for range n {
		taskID := g.nextID
		g.nextID++
		fireTime := baseTime.Add(time.Duration(taskID) * time.Second)

		var category tasks.Category
		var task tasks.Task
		switch g.rng.IntN(4) {
		case 0:
			category, task = tasks.CategoryTransfer, &tasks.CloseExecutionTask{
				WorkflowKey:         key,
				VisibilityTimestamp: fireTime,
				TaskID:              taskID,
				Version:             r.lastWriteVersion,
			}
		case 1:
			category, task = tasks.CategoryTimer, &tasks.UserTimerTask{
				WorkflowKey:         key,
				VisibilityTimestamp: fireTime,
				TaskID:              taskID,
				EventID:             r.nextEventID,
			}
		case 2:
			category, task = tasks.CategoryVisibility, &tasks.UpsertExecutionVisibilityTask{
				WorkflowKey:         key,
				VisibilityTimestamp: fireTime,
				TaskID:              taskID,
			}
		default:
			category, task = tasks.CategoryReplication, &tasks.SyncWorkflowStateTask{
				WorkflowKey:         key,
				VisibilityTimestamp: fireTime,
				TaskID:              taskID,
				Version:             r.lastWriteVersion,
			}
		}

		blob, err := g.serializer.SerializeTask(task)
		if err != nil {
			return nil, fmt.Errorf("mutgen: serializing a %s task: %w", category.Name(), err)
		}
		out[category] = append(out[category], p.InternalHistoryTask{Key: task.GetKey(), Blob: blob})
		g.recordEmitted(category, task.GetKey())
		g.tasks++
		g.tasksByCat[int32(category.ID())]++
	}
	return out, nil
}

// ---------------------------------------------------------------- key choice

// pickKey returns the key an upsert names: one the run already holds or one it
// deleted with probability KeyReuse, a fresh one otherwise. Reusing a deleted
// key is the upsert-after-delete half of fold's per-key rule; reusing a live one
// is what makes a window's upserts merge instead of pile up.
//
// live and gone are taken by pointer because the reuse arm moves a key between
// them, and fresh mints one for the arm that does not — the two key kinds differ
// in nothing else, and the rng is touched in the same order either way, which is
// what keeps a recorded corpus valid across this.
func pickKey[K comparable](g *Generator, live, gone *[]K, fresh func() K) K {
	if n := len(*live) + len(*gone); n > 0 && g.chance(g.cfg.KeyReuse) {
		i := g.rng.IntN(n)
		if i < len(*live) {
			return (*live)[i]
		}
		key := (*gone)[i-len(*live)]
		*gone = remove(*gone, key)
		*live = append(*live, key)
		return key
	}
	key := fresh()
	*live = append(*live, key)
	g.distinctKeys++
	return key
}

func (g *Generator) pickActivityKey(r *runState) int64 {
	return pickKey(g, &r.liveActivities, &r.goneActivities, func() int64 {
		key := r.nextActivity
		r.nextActivity++
		return key
	})
}

func (g *Generator) pickTimerKey(r *runState) string {
	return pickKey(g, &r.liveTimers, &r.goneTimers, func() string {
		key := fmt.Sprintf("timer-%d", r.nextTimer)
		r.nextTimer++
		return key
	})
}

// pickDeletable chooses a live key the current mutation did not upsert, so that
// every delete names a key the run actually holds, which is what makes the
// delete resolve against an upsert rather than against nothing.
func pickDeletable[K comparable](rng *rand.Rand, live, upserted []K) (K, bool) {
	eligible := make([]K, 0, len(live))
	for _, key := range live {
		if !slices.Contains(upserted, key) {
			eligible = append(eligible, key)
		}
	}
	if len(eligible) == 0 {
		var zero K
		return zero, false
	}
	return eligible[rng.IntN(len(eligible))], true
}

func remove[K comparable](keys []K, key K) []K {
	if i := slices.Index(keys, key); i >= 0 {
		return slices.Delete(keys, i, i+1)
	}
	return keys
}

// ---------------------------------------------------------------- blobs

// rowBlobs are the three blobs every execution row is written from. All three
// must be non-nil: a store reads .Data off each of them to build the row, and
// the one this was written against does so without a check, so a missing
// checksum is a panic rather than a rejection.
func (g *Generator) rowBlobs(w *workflowState, r *runState) (info, state, checksum *commonpb.DataBlob, err error) {
	if info, err = g.serializer.WorkflowExecutionInfoToBlob(g.executionInfo(w, r)); err != nil {
		return nil, nil, nil, err
	}
	if state, err = g.serializer.WorkflowExecutionStateToBlob(g.executionState(r)); err != nil {
		return nil, nil, nil, err
	}
	if checksum, err = g.serializer.ChecksumToBlob(g.checksum(r)); err != nil {
		return nil, nil, nil, err
	}
	return info, state, checksum, nil
}

// executionInfo is deliberately thin, and carries no protobuf map field:
// SearchAttributes and Memo would make the serialized bytes vary between runs of
// the same seed, which is the one property this package must not lose.
func (g *Generator) executionInfo(w *workflowState, r *runState) *persistencespb.WorkflowExecutionInfo {
	return &persistencespb.WorkflowExecutionInfo{
		NamespaceId:          g.namespaceID,
		WorkflowId:           w.workflowID,
		FirstExecutionRunId:  r.runID,
		TaskQueue:            "mutgen",
		WorkflowTypeName:     "mutgen-workflow",
		StateTransitionCount: r.version,
		LastFirstEventId:     r.nextEventID - 1,
		ActivityCount:        int64(len(r.liveActivities)),
		UserTimerCount:       int64(len(r.liveTimers)),
		StartTime:            timestamppb.New(baseTime),
		LastUpdateTime:       timestamppb.New(baseTime.Add(time.Duration(r.version) * time.Second)),
	}
}

func (g *Generator) executionState(r *runState) *persistencespb.WorkflowExecutionState {
	// RequestIds is left empty on purpose: it is a map, so it would break the
	// seed's determinism, and it is also the field serialization back-fills on
	// the way out of a blob, so a value written empty does not come back empty
	// and whoever compares the two has to allow for it.
	return &persistencespb.WorkflowExecutionState{
		RunId:           r.runID,
		CreateRequestId: r.createRequestID,
		State:           r.state,
		Status:          r.status,
		StartTime:       timestamppb.New(baseTime),
	}
}

// checksum is real rather than empty so the column is exercised: the manager
// writes an empty Checksum message when a mutation has none, and an empty
// message is what a store that dropped the field would also produce.
func (g *Generator) checksum(r *runState) *persistencespb.Checksum {
	return &persistencespb.Checksum{
		Version: 1,
		Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
		Value:   binary.LittleEndian.AppendUint64(nil, uint64(r.version)),
	}
}

// bufferedEvents is a batch as the store takes it: serialized history events.
// The event carries no map field for the same reason nothing else here does —
// the bytes have to be a function of the seed.
func (g *Generator) bufferedEvents(r *runState) (*commonpb.DataBlob, error) {
	return g.serializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId:   r.nextEventID,
		EventTime: timestamppb.New(baseTime.Add(time.Duration(r.version) * time.Second)),
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Version:   r.lastWriteVersion,
		TaskId:    r.version,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
				TimerId:        fmt.Sprintf("timer-%d", r.nextTimer),
				StartedEventId: r.nextEventID - 1,
			},
		},
	}})
}

func (g *Generator) activityBlob(key int64, r *runState) (*commonpb.DataBlob, error) {
	return g.serializer.ActivityInfoToBlob(&persistencespb.ActivityInfo{
		Version:          r.lastWriteVersion,
		ScheduledEventId: key,
		ActivityId:       fmt.Sprintf("activity-%d", key),
		StartedEventId:   r.nextEventID,
		Attempt:          1,
		TaskQueue:        "mutgen",
		ScheduledTime:    timestamppb.New(baseTime.Add(time.Duration(key) * time.Second)),
	})
}

func (g *Generator) timerBlob(key string, r *runState) (*commonpb.DataBlob, error) {
	return g.serializer.TimerInfoToBlob(&persistencespb.TimerInfo{
		Version:        r.lastWriteVersion,
		TimerId:        key,
		StartedEventId: r.nextEventID,
		ExpiryTime:     timestamppb.New(baseTime.Add(time.Duration(r.version) * time.Minute)),
	})
}

// ---------------------------------------------------------------- primitives

// baseTime is where every timestamp this package writes is measured from. A
// fixed instant rather than time.Now for the same reason the ids come out of the
// seeded source: the stream is a function of the seed or it is not reproducible.
var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func (g *Generator) chance(p float64) bool {
	return g.rng.Float64() < p
}

// newUUID draws a v4 UUID out of the seeded source. uuid.New would be a second,
// unseeded source of randomness — and namespace ids and run ids have to be
// parseable UUIDs, because the plugin calls primitives.MustParseUUID on them.
func (g *Generator) newUUID() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], g.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], g.rng.Uint64())
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return uuid.UUID(b).String()
}

// ------------------------------------------------------- the history-task path

// emittedTasks is one category's ledger: the keys the stream has written, in
// ascending order, and how far its own range deletes have reached.
//
// It exists so that a range delete can be drawn from keys that are actually
// there. A range over an interval nothing was ever written into is a mutation a
// consumer will happily judge and a rule nothing exercised — the same failure
// the collapse ratio has at WorkflowReuse 0.
type emittedTasks struct {
	category tasks.Category
	keys     []tasks.Key
	// covered is how many of keys the stream's own deletes already cover, and
	// completedTo the exclusive maximum the last of them named. Together they
	// are what makes the next range butt-joined to the last, which is the shape
	// a queue's checkpoints have.
	covered     int
	completedTo tasks.Key
}

// recordEmitted adds one written task to the ledger.
//
// It is called from every place a task reaches a request, which is why it is a
// method on the generator rather than a line inside historyTasks: a task written
// by a standalone AddHistoryTasks is exactly as deletable as one carried by a
// mutation, and a ledger that held only the second would generate ranges that
// covered half of what they should.
func (g *Generator) recordEmitted(category tasks.Category, key tasks.Key) {
	id := int32(category.ID())
	e := g.emitted[id]
	if e == nil {
		e = &emittedTasks{category: category, completedTo: taskFloor(category)}
		g.emitted[id] = e
		g.cats = append(g.cats, category)
	}
	e.keys = append(e.keys, key)
}

// taskFloor is where a category's first range starts: below every key the
// stream can produce, in the column the store below actually ranges on.
func taskFloor(category tasks.Category) tasks.Key {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(0)
	}
	return tasks.NewKey(baseTime.Add(-time.Hour), 0)
}

// emitAddTasks queues a standalone AddHistoryTasks on a run the stream holds.
//
// It names a live run because the tasks have to be serialisable against one —
// the blobs carry a workflow key — and not because the store cares: its task
// rows are keyed by (shard, category, key) and the request's workflow id never
// reaches them.
func (g *Generator) emitAddTasks(w *workflowState, r *runState) error {
	groups, err := g.historyTasks(w, r)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		// TaskDensity drew zero. An AddHistoryTasks with no tasks is a call the
		// server never makes, so the step simply produced nothing.
		return nil
	}
	g.queue = append(g.queue, mutation.Mutation{AddTasks: &p.InternalAddHistoryTasksRequest{
		ShardID:     g.cfg.ShardID,
		NamespaceID: g.namespaceID,
		WorkflowID:  w.workflowID,
		Tasks:       groups,
	}})
	return nil
}

// emitRangeComplete queues a range delete over keys the stream has already
// written, butt-joined to the last range of that category.
//
// The cut is drawn among the keys not yet covered, so every range this generator
// produces covers at least one task — which is what [Report.TasksCovered]
// counts and what a corpus acceptance gates on. A category with nothing left to
// cover produces nothing rather than an empty range: an empty range is a
// statement the store runs and a rule nothing exercises.
func (g *Generator) emitRangeComplete() {
	if len(g.cats) == 0 {
		return
	}
	e := g.emitted[int32(g.cats[g.rng.IntN(len(g.cats))].ID())]
	left := len(e.keys) - e.covered
	if left <= 0 {
		return
	}
	take := 1 + g.rng.IntN(left)
	last := e.keys[e.covered+take-1]

	from := e.completedTo
	upTo := nextAbove(e.category, last)
	e.covered += take
	e.completedTo = upTo
	g.tasksCovered += take

	g.queue = append(g.queue, mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             g.cfg.ShardID,
		TaskCategory:        e.category,
		InclusiveMinTaskKey: from,
		ExclusiveMaxTaskKey: upTo,
	}})
}

// nextAbove is the exclusive maximum that covers key and nothing after it, in
// the column the category is ranged on.
//
// For a scheduled category the task id is zeroed, which is what upstream's own
// checkpoint does (queues/queue_base.go's rangeCompleteTasks) and what makes the
// fire-time-only delete below it mean what the caller intends.
//
// The bump is a microsecond, and a nanosecond is not enough: a stored fire time
// is microseconds, so a maximum a nanosecond above a task's fire time truncates
// to that same fire time and the store's DELETE covers nothing. A corpus that
// generated such a range would judge the deletion rule in name only, which is
// the same trap [Config.RangeCompleteRate] warns about, arriving through the
// store's resolution instead of through the key space. The stream's fire times
// are a second apart, so a microsecond separates any two of them.
func nextAbove(category tasks.Category, key tasks.Key) tasks.Key {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(key.TaskID + 1)
	}
	return tasks.NewKey(key.FireTime.Truncate(time.Microsecond).Add(time.Microsecond), 0)
}
