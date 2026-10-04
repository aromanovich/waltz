package mutgen

// Each step() call is one event for one workflow (or, for the history-task
// shapes, for the shard). What may happen depends on the state the stream has
// already put the workflow in: a request is only built for a state the store
// would accept, and Temporal's validators confirm it before it is emitted.

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

// step advances the stream by one event, queueing the one or two mutations it
// produced.
func (g *Generator) step() error {
	// The shard-level history-task steps come first and return on their own,
	// independent of the picked workflow's state.
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
		// A completed, still-current run: delete the workflow or reuse the id.
		if g.chance(g.cfg.TombstoneRate) {
			return g.emitDeletePair(w)
		}
		return g.emitCreate(w, p.CreateWorkflowModeUpdateCurrent)
	default:
		return g.emitCreate(w, p.CreateWorkflowModeBrandNew)
	}
}

// newRun is a just-created run at version 1. Its last-write-version is random
// per run because a create over a previous run asserts it, and a constant
// would let a wrong value pass.
func (g *Generator) newRun() *runState {
	return &runState{
		runID:            g.newUUID(),
		createRequestID:  g.newUUID(),
		version:          1,
		nextEventID:      3,
		lastWriteVersion: g.rng.Int64N(1 << 20),
		state:            enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		// Event ids start at 1 upstream (0 is EmptyEventID).
		nextActivity: 1,
	}
}

// pick chooses the workflow for this step: an existing key with probability
// WorkflowReuse, else a fresh one. A full key space (Workflows) forces reuse.
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
		// RangeID stays zero: it is the epoch (I11), stamped by the driver.
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
		g.rep.Recreations++
	}
	w.created++
	w.run, w.closed = r, nil
	g.rep.Runs++
	g.queue = append(g.queue, mutation.Mutation{Create: req})
	return nil
}

func (g *Generator) emitUpdate(w *workflowState) error {
	r := w.run

	// The last link closes the run, possibly by continuing as new, so ids get
	// reused or deleted. Decided up front because it changes the state the
	// mutation carries.
	closing := r.updates+1 >= g.cfg.MaxChainLength
	continuing := closing && g.chance(g.cfg.ContinueAsNewRate)

	r.version++
	r.nextEventID += 1 + g.rng.Int64N(3)
	r.updates++
	switch {
	case continuing:
		// An update carrying a new run must leave the old one closed
		// (ValidateUpdateWorkflowModeState case 2).
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
		// The new run becomes current; the old run stays in the store untouched,
		// since the delete pair is the only deletion shape generated.
		w.run, w.closed = next, nil
		g.rep.Runs++
	case closing:
		w.run, w.closed = nil, r
	}
	g.queue = append(g.queue, mutation.Mutation{Update: req})
	return nil
}

// emitSnapshotBarrier emits one of the two snapshot-bearing shapes, which fold
// treats alike (both reset the accumulator, I8).
func (g *Generator) emitSnapshotBarrier(w *workflowState) error {
	if g.chance(0.5) {
		return g.emitSet(w)
	}
	return g.emitConflictResolve(w)
}

// barrierSnapshot moves the run to its next version and snapshots its whole
// state. The store replaces the run's state items, buffered events included,
// so the unflushed count resets. Both shapes are checked with the update
// validator, as a store checks reset and set snapshots.
func (g *Generator) barrierSnapshot(w *workflowState, kind string) (p.InternalWorkflowSnapshot, error) {
	r := w.run
	r.version++
	r.nextEventID += 1 + g.rng.Int64N(3)

	snapshot, err := g.snapshot(w, r, 0)
	if err != nil {
		return p.InternalWorkflowSnapshot{}, err
	}
	if err := p.ValidateUpdateWorkflowStateStatus(r.state, r.status); err != nil {
		return p.InternalWorkflowSnapshot{}, fmt.Errorf("mutgen: generated an invalid %s: %w", kind, err)
	}
	r.buffered = 0
	return snapshot, nil
}

// emitConflictResolve resets the current run at the next version, with no new
// run and no current mutation; multi-part resets are not generated. It puts a
// reset inside a chain.
func (g *Generator) emitConflictResolve(w *workflowState) error {
	snapshot, err := g.barrierSnapshot(w, "conflict-resolve")
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

	// Temporal's mode validator has a rule of its own for the three parts.
	if err := p.ValidateConflictResolveWorkflowModeState(req.Mode,
		p.WorkflowSnapshot{ExecutionState: snapshot.ExecutionState}, nil, nil); err != nil {
		return fmt.Errorf("mutgen: generated an invalid conflict-resolve: %w", err)
	}

	g.queue = append(g.queue, mutation.Mutation{ConflictResolve: req})
	return nil
}

// emitSet replaces the run's whole state at the next version, asserting the
// previous version and nothing about the current-execution row.
func (g *Generator) emitSet(w *workflowState) error {
	snapshot, err := g.barrierSnapshot(w, "set")
	if err != nil {
		return err
	}
	g.queue = append(g.queue, mutation.Mutation{Set: &p.InternalSetWorkflowExecutionRequest{
		ShardID:             g.cfg.ShardID,
		SetWorkflowSnapshot: snapshot,
	}})
	return nil
}

// emitDeletePair deletes a workflow in Temporal's order: current-execution row
// first, then mutable state (shard/context_impl.go, stages 2 and 3). fold's
// tombstone collapse relies on this order.
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

// mutation builds one update's delta: new scalars, sub-entity upserts and
// deletes, and history tasks.
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

	// A key is never both upserted and deleted in one mutation: Temporal never
	// produces it and the store resolves it wrongly.
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
		g.rep.Upserts++
	}

	if g.chance(g.cfg.DeleteAfterUpsert) {
		if key, ok := pickDeletable(g.rng, r.liveActivities, upsertedActivities); ok {
			mut.DeleteActivityInfos = map[int64]struct{}{key: {}}
			r.liveActivities = remove(r.liveActivities, key)
			r.goneActivities = append(r.goneActivities, key)
			g.rep.SubDeletes++
		}
		if key, ok := pickDeletable(g.rng, r.liveTimers, upsertedTimers); ok {
			mut.DeleteTimerInfos = map[string]struct{}{key: {}}
			r.liveTimers = remove(r.liveTimers, key)
			r.goneTimers = append(r.goneTimers, key)
			g.rep.SubDeletes++
		}
	}

	// Buffered events: one batch per mutation; batches do not merge. A clear
	// (completing a workflow task) needs something buffered.
	if g.chance(g.cfg.BufferedRate) {
		if mut.NewBufferedEvents, err = g.bufferedEvents(r); err != nil {
			return p.InternalWorkflowMutation{}, err
		}
		r.buffered++
	}
	if r.buffered > 0 && g.chance(g.cfg.BufferedRate) {
		mut.ClearBufferedEvents = true
		// The clear removes stored rows; a batch in the same mutation is
		// written after it and stays.
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

// snapshot builds a whole-run image of every live key, plus extraKeys new
// ones (a create must start with something).
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
		g.rep.DistinctKeys++
		g.rep.Upserts++
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

// historyTasks draws TaskDensity tasks on average, each in one of transfer,
// timer, visibility or replication. Upstream's generator emits none, so this
// is what exercises fold's task path.
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
		g.rep.Tasks++
		g.rep.TasksByCat[int32(category.ID())]++
	}
	return out, nil
}

// ---------------------------------------------------------------- key choice

// pickKey returns the key an upsert names: with probability KeyReuse a live or
// deleted one (exercising per-key merge and upsert-after-delete), else
// fresh(). live and gone are pointers because reuse moves a key between them.
// The rng is consumed in the same order for both key types, keeping the
// stream a function of the seed.
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
	g.rep.DistinctKeys++
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

// pickDeletable chooses a live key the current mutation did not upsert, so
// every delete names a key the run really holds.
func pickDeletable[K comparable](rng *rand.Rand, live, upserted []K) (K, bool) {
	eligible := slices.DeleteFunc(slices.Clone(live), func(key K) bool {
		return slices.Contains(upserted, key)
	})
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

// rowBlobs returns the three blobs every execution row is written from. All
// must be non-nil: stores read .Data without a nil check, so a missing one
// panics.
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

// executionInfo is deliberately thin and has no protobuf map fields (such as
// SearchAttributes or Memo), which would make the bytes vary per seed run.
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
	// RequestIds stays empty: it is a map. Deserialisation back-fills it, so a
	// round-trip comparison must allow for that.
	return &persistencespb.WorkflowExecutionState{
		RunId:           r.runID,
		CreateRequestId: r.createRequestID,
		State:           r.state,
		Status:          r.status,
		StartTime:       timestamppb.New(baseTime),
	}
}

// checksum is non-empty so a store that drops the field is caught: an empty
// one is what the manager writes by default.
func (g *Generator) checksum(r *runState) *persistencespb.Checksum {
	return &persistencespb.Checksum{
		Version: 1,
		Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
		Value:   binary.LittleEndian.AppendUint64(nil, uint64(r.version)),
	}
}

// bufferedEvents is a batch of serialized history events, with no map fields.
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

// baseTime anchors every timestamp, fixed rather than time.Now for
// reproducibility.
var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func (g *Generator) chance(p float64) bool {
	return g.rng.Float64() < p
}

// newUUID draws a v4 UUID from the seeded source (uuid.New is unseeded). Ids
// must parse as UUIDs: the SQL store parses them with MustParseUUID.
func (g *Generator) newUUID() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], g.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], g.rng.Uint64())
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return uuid.UUID(b).String()
}

// ------------------------------------------------------- the history-task path

// emittedTasks is one category's ledger: keys written, ascending, and how far
// range deletes have reached. Ranges are drawn from it so they cover real
// tasks.
type emittedTasks struct {
	category tasks.Category
	keys     []tasks.Key
	// covered counts keys already deleted; the next range starts there,
	// adjacent to the last, as queue checkpoints are.
	covered int
}

// recordEmitted adds one written task to the ledger. Every path that writes a
// task must call it, standalone AddHistoryTasks included.
func (g *Generator) recordEmitted(category tasks.Category, key tasks.Key) {
	id := int32(category.ID())
	e := g.emitted[id]
	if e == nil {
		e = &emittedTasks{category: category}
		g.emitted[id] = e
		g.cats = append(g.cats, category)
	}
	e.keys = append(e.keys, key)
}

// taskFloor is where a category's first range starts: below every key the
// stream produces, in the column the store ranges on.
func taskFloor(category tasks.Category) tasks.Key {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(0)
	}
	return tasks.NewKey(baseTime.Add(-time.Hour), 0)
}

// emitAddTasks queues a standalone AddHistoryTasks on a live run. The run is
// needed only to build the task blobs; the store keys task rows by (shard,
// category, key).
func (g *Generator) emitAddTasks(w *workflowState, r *runState) error {
	groups, err := g.historyTasks(w, r)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		// No tasks drawn; the server never sends an empty AddHistoryTasks.
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

// emitRangeComplete queues a range delete over written keys, adjacent to the
// category's previous range. Every range covers at least one task (counted in
// [Report.TasksCovered]); with nothing left to cover it emits nothing.
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

	from := taskFloor(e.category)
	if e.covered > 0 {
		from = nextAbove(e.category, e.keys[e.covered-1])
	}
	upTo := nextAbove(e.category, e.keys[e.covered+take-1])
	e.covered += take
	g.rep.TasksCovered += take

	g.queue = append(g.queue, mutation.Mutation{RangeCompleteTasks: &p.RangeCompleteHistoryTasksRequest{
		ShardID:             g.cfg.ShardID,
		TaskCategory:        e.category,
		InclusiveMinTaskKey: from,
		ExclusiveMaxTaskKey: upTo,
	}})
}

// nextAbove is the exclusive maximum covering key and nothing after it.
//
// For a scheduled category the task id is zeroed, as upstream's checkpoint
// does (queues/queue_base.go rangeCompleteTasks). The fire time is bumped by a
// microsecond, not a nanosecond: stored fire times have microsecond
// resolution, so a nanosecond bump truncates back and the DELETE covers
// nothing. Generated fire times are a second apart, so this is safe.
func nextAbove(category tasks.Category, key tasks.Key) tasks.Key {
	if category.Type() == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(key.TaskID + 1)
	}
	return tasks.NewKey(key.FireTime.Truncate(time.Microsecond).Add(time.Microsecond), 0)
}
