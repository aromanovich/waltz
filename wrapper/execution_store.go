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

// ExecutionStore is the WAL layer's ExecutionStore: 28 methods, of which
// intercept mode answers twelve differently and refuses a thirteenth
// (CompleteHistoryTask, see [ErrCompleteHistoryTaskUnsupported]); passthrough
// changes none. The twelve are the eight writes the record format has a shape
// for (four mutable-state, two deletes, two history-task calls) and the four
// reads one of those eight can change the answer to. The other 15 transit in
// both modes.
type ExecutionStore struct {
	base p.ExecutionStore

	// layer is [Options.Layer], the mode itself: nil is passthrough.
	layer ShardLayer

	// baseRows is the write path's pair of pre-window reads ([baserow.Rows]),
	// resolved and boxed once at construction.
	baseRows *baserow.Rows

	// baseTasks is the merged read's base: the store's own method value, taken
	// once rather than allocated at every merged page.
	baseTasks func(context.Context, *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error)

	// baseHistory is the same for the branch page, and taken for the same
	// reason: a closure per page would allocate one and say nothing more.
	baseHistory func(context.Context, *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error)

	// emit sends the same numbers to the server's metrics stack, the writes and
	// the overlaid reads tagged by store method.
	emit *walmetrics.Emitter

	// intercepted counts the mutable-state writes and the two tombstones that
	// took the WAL path, tasksWritten and tasksCompleted the two halves of the
	// history-task path. They duplicate metrics because a metrics handler is
	// write-only and the acceptance has to ask this store what it did.
	intercepted    atomic.Int64
	tasksWritten   atomic.Int64
	tasksCompleted atomic.Int64
	// overlaid counts the mutable-state reads through the layer, taskReads the
	// task pages routed at the merge — routed, not merged: a page the layer
	// refuses, or one the window contributed nothing to, is in it.
	overlaid  atomic.Int64
	taskReads atomic.Int64
	// historyReads counts the branch pages routed through the merge — routed,
	// not merged: a page the window contributed nothing to is in it.
	historyReads atomic.Int64
}

var _ p.ExecutionStore = (*ExecutionStore)(nil)

// NewExecutionStore decorates base, and refuses rather than returning a store
// that cannot serve the mode it was asked for.
//
// Intercept mode converts base to [baserow.Store] and passthrough does not,
// which is the whole of why the conversion is here rather than in a parameter
// type: base arrives as [p.ExecutionStore] through Temporal's factory
// interface, and only one of the two modes reads a pre-window row. There is no
// honest way to serve intercept mode over a store that cannot answer the
// version-carrying read, so the error is [baserow.ErrNoVersionedRead], returned
// where the server is still starting and can be told which read is missing.
func NewExecutionStore(base p.ExecutionStore, opts Options) (*ExecutionStore, error) {
	emit := opts.Metrics
	if emit == nil {
		// Options with no emitter in them ([Options.Metrics]): a private noop,
		// so that every record below is one branch rather than two.
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

// Counts is what this store knows about its own traffic. Every field counts on
// the way in, so a write the tail refused is in it, and all are zero in
// passthrough mode.
type Counts struct {
	// Intercepted is the mutable-state writes and the two tombstones that took
	// the WAL path.
	Intercepted int64
	// TasksWritten is AddHistoryTasks and TasksCompleted is
	// RangeCompleteHistoryTasks.
	TasksWritten   int64
	TasksCompleted int64
	// Overlaid is the mutable-state reads routed through the layer.
	Overlaid int64
	// TaskReads is GetHistoryTasks pages routed at the merge, and HistoryReads
	// is ReadHistoryBranch pages.
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

// interceptRow is the per-kind half of an intercepted write: the store method
// the metrics are tagged with, and the counter it raises. Behaviour stays in
// the methods; this is the bookkeeping, in the shape the mutation package's own
// kinds table states its per-kind facts in.
type interceptRow struct {
	op      string
	counter func(*ExecutionStore) *atomic.Int64
}

// The three counters, named rather than written per row: six of the eight rows
// raise the same one.
func interceptedOf(s *ExecutionStore) *atomic.Int64    { return &s.intercepted }
func tasksWrittenOf(s *ExecutionStore) *atomic.Int64   { return &s.tasksWritten }
func tasksCompletedOf(s *ExecutionStore) *atomic.Int64 { return &s.tasksCompleted }

// interception is that table, indexed by [mutation.Kind]. Every kind the
// wrapper takes into the layer has a row; the guard is
// TestEveryInterceptedKindHasARow, so a kind added without one fails by name
// rather than as a refusal of every write of that kind ([ExecutionStore.write]).
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

// write is intercept mode's whole write path: one mutation into the log, and
// whatever the layer answered, the drain's outcome in sync mode — preceded,
// where the record does not carry them, by the request's new events into the
// cold store. All eight intercepted writes come through here and read their own
// row off the kind, so neither step is a method's to remember. The error is
// returned exactly as it arrives, since ContextImpl.handleWriteErrorLocked
// type-switches on these values and one %w turns an expected condition failure
// into a background re-acquire.
func (s *ExecutionStore) write(ctx context.Context, m mutation.Mutation) error {
	row := interception[m.Kind()]
	if row.counter == nil {
		// Unreachable while every caller names a kind with a row, and a refusal
		// rather than a panic because the alternative is a write counted nowhere.
		return fmt.Errorf("wrapper: %w: kind %s reaches no interception row", mutation.ErrNotExactlyOneRequest, m.Kind())
	}
	// Asked only where there is something to write: five of the eight kinds
	// carry no batches at all, and the question has one answer on a write with
	// none.
	if len(m.EventSlots()) != 0 && !s.layer.WritesHistory() {
		if err := s.appendEvents(ctx, m); err != nil {
			return err
		}
	}
	row.counter(s).Add(1)
	s.emit.InterceptedWrite(row.op)
	return s.layer.Write(ctx, m, wal.Epoch(m.RangeID()), s.baseRows)
}

// appendEvents writes the mutation's new history events through the base store,
// before the mutation that refers to them is acked, and strips them off the
// mutation once they are down. It is the writer for records that do not carry
// the batches: skipping them would ack a mutable state pointing at history rows
// nobody wrote — which no functional suite sees, the entry being durable and
// correct. Where the record carries them the drain is the writer instead, inside
// the transaction that writes the mutable state naming them (ADR 0014).
//
// The strip is what lets everything below hold one invariant: a mutation
// reaching the layer carries exactly the batches nobody has written yet. So the
// codec and the fold need no mode of their own — they carry and apply what the
// mutation holds — and the layer folds the object it appended rather than one
// that differs from it in a field.
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

// --- the four mutable-state writes: the WAL's traffic ---------------------
//
// Each hands its request to write, which puts the events down where the record
// does not carry them, acks the mutation, and answers with what the layer said.
// The epoch is the request's own rangeID, the token the plugin's own write
// would have conditioned its transaction on (invariant I11).

func (s *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context, request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	if s.layer == nil {
		return s.base.CreateWorkflowExecution(ctx, request)
	}
	if err := s.write(ctx, mutation.Mutation{Create: request}); err != nil {
		return nil, err
	}
	// The response type has no fields; nothing to carry over from the drain.
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

// --- the tombstones -------------------------------------------------------
//
// They go into the WAL like the four above: routing deletes around the log would
// force a drain per delete, and an outbox would leave a deleted execution
// readable. Neither request carries a rangeID, so neither names an epoch; the
// epoch CAS the drain's transaction opens with fences them instead.

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

// --- the two mutable-state reads: the overlay -----------------------------
//
// The cold store is behind the window, so a read answered from it alone returns
// a state the layer knows to be stale, or a workflow it knows to be deleted.
// This store hands over the request and its own base read as a closure; the
// layer decides whether to call it, and nothing here merges or interprets.

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

// AddHistoryTasks goes into the log, as one decision with the range delete: a
// write deferred to a drain beside a delete that acts immediately is a delete
// that misses the row it was meant to cover, which for a scheduled category is
// a lost timer.
func (s *ExecutionStore) AddHistoryTasks(
	ctx context.Context, request *p.InternalAddHistoryTasksRequest,
) error {
	if s.layer == nil {
		return s.base.AddHistoryTasks(ctx, request)
	}
	return s.write(ctx, mutation.Mutation{AddTasks: request})
}

// GetHistoryTasks is the read whose omission loses a task rather than staling
// an answer: notification carries no payload, so a queue reading its range from
// the cold store alone finds nothing there, completes the range and acks past a
// key it never saw.
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

// ErrCompleteHistoryTaskUnsupported is what intercept mode answers a single-key
// task completion with: the log's deletion record is a range per category, and
// a second deletion shape would be another thing every reader, drain and replay
// has to agree about. Its one caller is the history handler's RemoveTask,
// behind the admin API of the same name.
var ErrCompleteHistoryTaskUnsupported = serviceerror.NewUnimplemented(
	"CompleteHistoryTask is not supported by the WAL layer: the log's deletion " +
		"record is a range per category, not a key")

// CompleteHistoryTask is refused in intercept mode and transits in passthrough.
func (s *ExecutionStore) CompleteHistoryTask(
	ctx context.Context, request *p.CompleteHistoryTaskRequest,
) error {
	if s.layer == nil {
		return s.base.CompleteHistoryTask(ctx, request)
	}
	return ErrCompleteHistoryTaskUnsupported
}

// RangeCompleteHistoryTasks goes into the log, so the deletion moves in log
// order, is rebuilt by replay with the rest of the window, and lands in the same
// transaction as the rows it covers. Answered at the append like every other
// intercepted write: a range delete this layer acks is one it will apply.
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
// One of the seven is answered by the layer. The other six transit in both
// modes, which for the two deletions and the tree read is a decision rather than
// an omission: what a delete aimed at a history row still in the window should do
// depends on where that deployment put its history, and this library does not
// choose for it. ADR 0014 names the exposure.

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

// ReadHistoryBranch is merged on read whatever the store below does with a
// write's batches, and not only where the records carry them: what decides
// whether the window holds event batches is the tail this shard inherited — a log
// written under one store is replayed by a node composed with another — and not
// this node. Passthrough still transits, there being no window to merge. The tree
// id comes out of the branch token here because the codec
// for it is the base store's ([p.ExecutionStore.GetHistoryBranchUtil]) and the
// layer may name no store.
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
