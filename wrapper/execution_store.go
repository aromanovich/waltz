package wrapper

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/baserow"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
	"github.com/aromanovich/waltz/walmetrics"
)

// ExecutionStore is the WAL layer's ExecutionStore. In intercept mode it
// changes 12 of its 28 methods: the eight writes the record format covers
// (four mutable-state, two deletes, two history-task calls) and the four reads
// those writes can affect. It refuses CompleteHistoryTask
// ([ErrCompleteHistoryTaskUnsupported]). The rest delegate in both modes, and
// passthrough changes nothing.
type ExecutionStore struct {
	base p.ExecutionStore

	// layer is [Options.Layer]; nil means passthrough.
	layer ShardLayer

	// baseRows is the write path's pre-window reads, resolved once.
	baseRows *baserow.Rows

	// baseTasks and baseHistory are base's method values, taken once so a
	// merged page does not allocate a closure.
	baseTasks   func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error)
	baseHistory func(context.Context, *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error)

	// emit reports writes and overlaid reads, tagged by store method.
	emit *walmetrics.Emitter

	// The counters behind [Counts]. They duplicate metrics because a metrics
	// handler is write-only and the acceptance must ask the store directly.
	intercepted    atomic.Int64
	tasksWritten   atomic.Int64
	tasksCompleted atomic.Int64
	overlaid       atomic.Int64
	taskReads      atomic.Int64
	historyReads   atomic.Int64
}

var _ p.ExecutionStore = (*ExecutionStore)(nil)

// NewExecutionStore decorates base. In intercept mode base must also be a
// [baserow.Store], since the write path reads pre-window rows; otherwise it
// returns [baserow.ErrNoVersionedRead] so the server fails at startup instead
// of serving a mode it cannot. The check is here, not in the parameter type,
// because base arrives through Temporal's factory as [p.ExecutionStore].
func NewExecutionStore(base p.ExecutionStore, opts Options) (*ExecutionStore, error) {
	emit := opts.Metrics
	if emit == nil {
		// A private noop, so recording never needs a nil check.
		emit = walmetrics.New(nil)
	}
	s := &ExecutionStore{base: base, layer: opts.Layer, emit: emit}
	s.baseTasks = base.GetHistoryTasks
	s.baseHistory = base.ReadHistoryBranch
	if opts.Layer != nil {
		rows, err := baserow.Of(base)
		if err != nil {
			return nil, err
		}
		s.baseRows = rows
	}
	return s, nil
}

// Counts is this store's own traffic. Calls are counted on the way in, so
// refused writes and pages the window added nothing to are included. All are
// zero in passthrough mode.
type Counts struct {
	// Intercepted counts mutable-state writes and the two deletes.
	Intercepted int64
	// TasksWritten counts AddHistoryTasks, TasksCompleted
	// RangeCompleteHistoryTasks.
	TasksWritten   int64
	TasksCompleted int64
	// Overlaid counts mutable-state reads routed through the layer.
	Overlaid int64
	// TaskReads counts GetHistoryTasks pages, HistoryReads ReadHistoryBranch
	// pages.
	TaskReads    int64
	HistoryReads int64
}

// Counts reports the counters. Safe to call from any goroutine.
func (s *ExecutionStore) Counts() Counts {
	return Counts{
		Intercepted:    s.intercepted.Load(),
		TasksWritten:   s.tasksWritten.Load(),
		TasksCompleted: s.tasksCompleted.Load(),
		Overlaid:       s.overlaid.Load(),
		TaskReads:      s.taskReads.Load(),
		HistoryReads:   s.historyReads.Load(),
	}
}

// interceptRow is the per-kind bookkeeping of an intercepted write: the store
// method its metrics are tagged with and the counter it raises.
type interceptRow struct {
	op      string
	counter func(*ExecutionStore) *atomic.Int64
}

func interceptedOf(s *ExecutionStore) *atomic.Int64    { return &s.intercepted }
func tasksWrittenOf(s *ExecutionStore) *atomic.Int64   { return &s.tasksWritten }
func tasksCompletedOf(s *ExecutionStore) *atomic.Int64 { return &s.tasksCompleted }

// interception maps each intercepted [mutation.Kind] to its row. A kind with
// no row has every write refused ([ExecutionStore.write]);
// TestEveryInterceptedKindHasARow catches that by name.
var interception = [mutation.KindCount]interceptRow{
	mutation.KindCreate:             {op: "CreateWorkflowExecution", counter: interceptedOf},
	mutation.KindUpdate:             {op: "UpdateWorkflowExecution", counter: interceptedOf},
	mutation.KindConflictResolve:    {op: "ConflictResolveWorkflowExecution", counter: interceptedOf},
	mutation.KindSet:                {op: "SetWorkflowExecution", counter: interceptedOf},
	mutation.KindDelete:             {op: "DeleteWorkflowExecution", counter: interceptedOf},
	mutation.KindDeleteCurrent:      {op: "DeleteCurrentWorkflowExecution", counter: interceptedOf},
	mutation.KindAddTasks:           {op: "AddHistoryTasks", counter: tasksWrittenOf},
	mutation.KindRangeCompleteTasks: {op: "RangeCompleteHistoryTasks", counter: tasksCompletedOf},
}

// write is intercept mode's write path for all eight intercepted kinds. If the
// record does not carry the new history events, it first appends them to the
// cold store; then it writes the mutation to the log and returns the layer's
// answer (in sync mode, the drain's outcome).
//
// Errors are returned unwrapped: ContextImpl.handleWriteErrorLocked
// type-switches on them, and wrapping turns an expected condition failure into
// a background re-acquire.
func (s *ExecutionStore) write(ctx context.Context, m mutation.Mutation) error {
	row := interception[m.Kind()]
	if row.counter == nil {
		// Unreachable today. Refused rather than panicking, so no write goes
		// uncounted.
		return fmt.Errorf("wrapper: %w: kind %s reaches no interception row", mutation.ErrNotExactlyOneRequest, m.Kind())
	}
	if len(m.EventSlots()) != 0 && !s.layer.WritesHistory() {
		if err := s.appendEvents(ctx, m); err != nil {
			return err
		}
	}
	row.counter(s).Add(1)
	s.emit.InterceptedWrite(row.op)
	return s.layer.Write(ctx, m, wal.Epoch(m.RangeID()), s.baseRows)
}

// appendEvents writes the mutation's new history events through the base
// store before the mutation is acked, then clears them from the mutation. It
// runs only when the record does not carry the events; skipping it would ack a
// mutable state pointing at history nobody wrote, and no functional suite
// would notice. When the record carries them, the drain writes them instead
// (ADR 0014).
//
// Clearing keeps one invariant below: a mutation reaching the layer carries
// exactly the events not yet written, so the codec and fold need no mode.
func (s *ExecutionStore) appendEvents(ctx context.Context, m mutation.Mutation) error {
	for _, slot := range m.EventSlots() {
		for _, events := range slot {
			if err := s.base.AppendHistoryNodes(ctx, events); err != nil {
				return err
			}
		}
	}
	m.ClearEvents()
	return nil
}

func (s *ExecutionStore) Close() { s.base.Close() }

func (s *ExecutionStore) GetName() string { return s.base.GetName() }

func (s *ExecutionStore) GetHistoryBranchUtil() p.HistoryBranchUtil {
	return s.base.GetHistoryBranchUtil()
}

// --- mutable-state writes: go to the WAL via write -------------------------
//
// The epoch is the request's rangeID, which the plugin's own write would have
// conditioned on (I11).

func (s *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context, request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	if s.layer == nil {
		return s.base.CreateWorkflowExecution(ctx, request)
	}
	if err := s.write(ctx, mutation.Mutation{Create: request}); err != nil {
		return nil, err
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (s *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context, request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	if s.layer == nil {
		return s.base.UpdateWorkflowExecution(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{Update: request})
}

func (s *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context, request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	if s.layer == nil {
		return s.base.ConflictResolveWorkflowExecution(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{ConflictResolve: request})
}

func (s *ExecutionStore) SetWorkflowExecution(
	ctx context.Context, request *p.InternalSetWorkflowExecutionRequest,
) error {
	if s.layer == nil {
		return s.base.SetWorkflowExecution(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{Set: request})
}

// --- deletes: also go to the WAL ------------------------------------------
//
// Bypassing the log would force a drain per delete. Neither request carries a
// rangeID; the epoch CAS at the start of the drain's transaction fences them.

func (s *ExecutionStore) DeleteWorkflowExecution(
	ctx context.Context, request *p.DeleteWorkflowExecutionRequest,
) error {
	if s.layer == nil {
		return s.base.DeleteWorkflowExecution(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{Delete: request})
}

func (s *ExecutionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context, request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	if s.layer == nil {
		return s.base.DeleteCurrentWorkflowExecution(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{DeleteCurrent: request})
}

// --- mutable-state reads: overlaid by the layer ---------------------------
//
// The cold store lags the window, so it alone may return stale or deleted
// state. The layer gets the base read as a closure and decides whether to
// call it.

func (s *ExecutionStore) GetCurrentExecution(
	ctx context.Context, request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, error) {
	if s.layer == nil {
		return s.base.GetCurrentExecution(ctx, request)
	}
	s.overlaid.Add(1)
	s.emit.OverlaidRead("GetCurrentExecution")
	return s.layer.GetCurrentExecution(ctx, request, func(ctx context.Context) (*p.InternalGetCurrentExecutionResponse, error) {
		return s.base.GetCurrentExecution(ctx, request)
	})
}

func (s *ExecutionStore) GetWorkflowExecution(
	ctx context.Context, request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	if s.layer == nil {
		return s.base.GetWorkflowExecution(ctx, request)
	}
	s.overlaid.Add(1)
	s.emit.OverlaidRead("GetWorkflowExecution")
	return s.layer.GetWorkflowExecution(ctx, request, func(ctx context.Context) (*p.InternalGetWorkflowExecutionResponse, error) {
		return s.base.GetWorkflowExecution(ctx, request)
	})
}

// --- reads that stay passthrough ------------------------------------------

func (s *ExecutionStore) ListConcreteExecutions(
	ctx context.Context, request *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return s.base.ListConcreteExecutions(ctx, request)
}

// --- history tasks --------------------------------------------------------

// AddHistoryTasks goes to the log, like RangeCompleteHistoryTasks. If only the
// delete acted immediately, it could miss a not-yet-drained task: for a
// scheduled category, a lost timer.
func (s *ExecutionStore) AddHistoryTasks(
	ctx context.Context, request *p.InternalAddHistoryTasksRequest,
) error {
	if s.layer == nil {
		return s.base.AddHistoryTasks(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{AddTasks: request})
}

// GetHistoryTasks merges the window into the page. Without the merge a queue
// would find nothing in the cold store, complete the range and ack past a task
// it never saw.
func (s *ExecutionStore) GetHistoryTasks(
	ctx context.Context, request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	if s.layer == nil {
		return s.base.GetHistoryTasks(ctx, request)
	}
	s.taskReads.Add(1)
	s.emit.MergedTaskPage()
	return s.layer.GetHistoryTasks(ctx, request, s.baseTasks)
}

// ErrCompleteHistoryTaskUnsupported is intercept mode's answer to a single-key
// task completion: the log only records range deletes. The only caller is the
// admin RemoveTask API.
var ErrCompleteHistoryTaskUnsupported = serviceerror.NewUnimplemented(
	"CompleteHistoryTask is not supported by the WAL layer: the log's deletion " +
		"record is a range per category, not a key")

// CompleteHistoryTask is refused in intercept mode.
func (s *ExecutionStore) CompleteHistoryTask(
	ctx context.Context, request *p.CompleteHistoryTaskRequest,
) error {
	if s.layer == nil {
		return s.base.CompleteHistoryTask(ctx, request)
	}
	return ErrCompleteHistoryTaskUnsupported
}

// RangeCompleteHistoryTasks goes to the log, so the delete keeps log order,
// survives replay, and lands in the same transaction as the rows it covers.
func (s *ExecutionStore) RangeCompleteHistoryTasks(
	ctx context.Context, request *p.RangeCompleteHistoryTasksRequest,
) error {
	if s.layer == nil {
		return s.base.RangeCompleteHistoryTasks(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{RangeCompleteTasks: request})
}

// --- replication DLQ ------------------------------------------------------

func (s *ExecutionStore) PutReplicationTaskToDLQ(
	ctx context.Context, request *p.PutReplicationTaskToDLQRequest,
) error {
	return s.base.PutReplicationTaskToDLQ(ctx, request)
}

func (s *ExecutionStore) GetReplicationTasksFromDLQ(
	ctx context.Context, request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetReplicationTasksFromDLQResponse, error) {
	return s.base.GetReplicationTasksFromDLQ(ctx, request)
}

func (s *ExecutionStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context, request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	return s.base.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (s *ExecutionStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context, request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return s.base.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (s *ExecutionStore) IsReplicationDLQEmpty(
	ctx context.Context, request *p.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	return s.base.IsReplicationDLQEmpty(ctx, request)
}

// --- history V2: the event trees ------------------------------------------
//
// Only ReadHistoryBranch goes through the layer. The deletes and tree reads
// delegate on purpose: how to treat a history row still in the window depends
// on where the deployment keeps history (ADR 0014).

func (s *ExecutionStore) AppendHistoryNodes(
	ctx context.Context, request *p.InternalAppendHistoryNodesRequest,
) error {
	return s.base.AppendHistoryNodes(ctx, request)
}

func (s *ExecutionStore) DeleteHistoryNodes(
	ctx context.Context, request *p.InternalDeleteHistoryNodesRequest,
) error {
	return s.base.DeleteHistoryNodes(ctx, request)
}

// ReadHistoryBranch is always merged in intercept mode, even if this node's
// records do not carry events: the inherited tail may have been written by a
// node that did. The tree id is parsed here because the codec belongs to the
// base store ([p.ExecutionStore.GetHistoryBranchUtil]), which the layer must
// not name.
func (s *ExecutionStore) ReadHistoryBranch(
	ctx context.Context, request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {
	if s.layer == nil {
		return s.base.ReadHistoryBranch(ctx, request)
	}
	branch, err := s.base.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}
	s.historyReads.Add(1)
	s.emit.OverlaidRead("ReadHistoryBranch")
	return s.layer.ReadHistoryBranch(ctx, request, branch.GetTreeId(), s.baseHistory)
}

func (s *ExecutionStore) ForkHistoryBranch(
	ctx context.Context, request *p.InternalForkHistoryBranchRequest,
) error {
	return s.base.ForkHistoryBranch(ctx, request)
}

func (s *ExecutionStore) DeleteHistoryBranch(
	ctx context.Context, request *p.InternalDeleteHistoryBranchRequest,
) error {
	return s.base.DeleteHistoryBranch(ctx, request)
}

func (s *ExecutionStore) GetHistoryTreeContainingBranch(
	ctx context.Context, request *p.InternalGetHistoryTreeContainingBranchRequest,
) (*p.InternalGetHistoryTreeContainingBranchResponse, error) {
	return s.base.GetHistoryTreeContainingBranch(ctx, request)
}

func (s *ExecutionStore) GetAllHistoryTreeBranches(
	ctx context.Context, request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {
	return s.base.GetAllHistoryTreeBranches(ctx, request)
}
