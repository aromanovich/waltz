package fold_test

// The current-row write rule. The current-execution row's content is a
// last-writer effect, rendered in that request's own form, while the merged
// request's kind is the head of the window. WorkflowRecord.CurrentWrite
// therefore carries what the sequential path would have left; these tests pin
// who the last writer is and in which form each kind writes.

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aromanovich/waltz/fold"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

// stateOf builds the execution state a real request carries. A request that
// writes the current row and carries none panics in the fold.
func stateOf(run string, state enumsspb.WorkflowExecutionState) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		RunId:           run,
		CreateRequestId: "create-" + run,
		State:           state,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
}

func drainOne(t *testing.T, ms ...mutation.Mutation) fold.Batch {
	t.Helper()
	a := fold.New(shard)
	add(t, a, ms...)
	return a.Drain()
}

// reqs is the batch's merged requests, in the order it emits them.
func reqs(b fold.Batch) []*fold.Emitted { return slices.Collect(b.Each()) }

// wr is the workflow record request i stands on: the current-row facts, which
// belong to the workflow rather than to any one of its requests.
func wr(b fold.Batch, i int) *fold.WorkflowRecord { return reqs(b)[i].Workflow() }

func TestCurrentWriteTracksTheLastWriter(t *testing.T) {
	t.Run("an update writes its full state", func(t *testing.T) {
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t, mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }))
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)
		require.Equal(t, runX, cw.RunID)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, cw.State)

		want, err := serialization.WorkflowExecutionStateToBlob(st)
		require.NoError(t, err)
		require.Equal(t, want.Data, cw.StateBlob.Data,
			"the update path re-serialises the full state, and the write must match it byte for byte")
	})

	t.Run("a set writes nothing", func(t *testing.T) {
		b := drainOne(t, mkSet(runX, 3))
		require.Equal(t, 1, b.Len())
		require.Nil(t, wr(b, 0).CurrentWrite, "the store's Set path never touches the current row")
	})

	t.Run("updates merged under a set keep their write", func(t *testing.T) {
		// Sequentially the update wrote the current row and the set did not, so
		// the row ends as the update left it; the merged request is Set-headed
		// and would write nothing.
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t,
			mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
			mkSet(runX, 3),
		)
		require.Equal(t, 1, b.Len())
		require.Equal(t, mutation.KindSet, reqs(b)[0].Request.Kind())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw, "the update's current-row write must survive the snapshot barrier")
		require.Equal(t, runX, cw.RunID)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, cw.State)
	})

	t.Run("the last writer wins", func(t *testing.T) {
		b := drainOne(t,
			mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) {
				m.ExecutionState = stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
			}),
			mkUpdate(runX, 3, func(m *p.InternalWorkflowMutation) {
				m.ExecutionState = stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
			}),
		)
		require.Equal(t, 1, b.Len())
		require.NotNil(t, wr(b, 0).CurrentWrite)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, wr(b, 0).CurrentWrite.State,
			"the write reproduces the window's last writer, not its head")
	})

	t.Run("a create passes the snapshot blob through", func(t *testing.T) {
		b := drainOne(t, mkCreate(runX))
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)
		require.Equal(t, runX, cw.RunID)
		require.Equal(t, []byte("state-v1"), cw.StateBlob.Data,
			"the store's create passes ExecutionStateBlob through untouched")
	})

	t.Run("a continue-as-new hands the row to the new run", func(t *testing.T) {
		can := mkUpdate(runX, 3, func(m *p.InternalWorkflowMutation) {
			m.ExecutionState = stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
		})
		newSnap := snapshot(runY, 1)
		can.Update.NewWorkflowSnapshot = &newSnap
		b := drainOne(t, can)
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)
		require.Equal(t, runY, cw.RunID, "the new run takes over the current row")
		require.Equal(t, []byte("state-v1"), cw.StateBlob.Data, "via its snapshot's blob slot")
	})

	t.Run("a conflict-resolve passes the reset snapshot's blob through", func(t *testing.T) {
		cr := mkConflictResolve(runX, 3, func(s *p.InternalWorkflowSnapshot) {
			s.ExecutionState = stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		})
		b := drainOne(t, cr)
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)
		require.Equal(t, runX, cw.RunID)
		require.Equal(t, []byte("state-v3"), cw.StateBlob.Data,
			"the store's conflict-resolve path writes the snapshot's own state blob, like every other path that carries one")
	})

	// The fields a rendering of four scalars dropped. Both are durable: the
	// row's columns are recovered from this blob, and of the two only the
	// create request id is ever back-filled by a later read — so a start time
	// short here is a namespace's reuse interval measured against the zero time
	// ever after, and an attached start's request id short here is a dedup that
	// misses.
	t.Run("a conflict-resolve keeps the start time and every request id", func(t *testing.T) {
		began := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		st.StartTime = timestamppb.New(began)
		st.RequestIds = map[string]*persistencespb.RequestIDInfo{
			"create-" + runX: {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
			"attached":       {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED},
		}
		stBlob, err := serialization.WorkflowExecutionStateToBlob(st)
		require.NoError(t, err)

		b := drainOne(t, mkConflictResolve(runX, 3, func(s *p.InternalWorkflowSnapshot) {
			s.ExecutionState, s.ExecutionStateBlob = st, stBlob
		}))
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)

		got, err := serialization.WorkflowExecutionStateFromBlob(cw.StateBlob)
		require.NoError(t, err)
		require.NotNil(t, got.StartTime, "the row's start_time column is derived from this blob")
		require.True(t, got.StartTime.AsTime().Equal(began), "got %v", got.StartTime.AsTime())
		require.Contains(t, got.RequestIds, "attached",
			"a non-create request id is never back-filled, so dropping it here loses the dedup for good")
	})

	t.Run("an update after a conflict-resolve wins over its reduced form", func(t *testing.T) {
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t,
			mkConflictResolve(runX, 3, func(s *p.InternalWorkflowSnapshot) {
				s.ExecutionState = stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
			}),
			mkUpdate(runX, 4, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
		)
		require.Equal(t, 1, b.Len())
		cw := wr(b, 0).CurrentWrite
		require.NotNil(t, cw)
		want, err := serialization.WorkflowExecutionStateToBlob(st)
		require.NoError(t, err)
		require.Equal(t, want.Data, cw.StateBlob.Data,
			"the trailing update's full form is what the sequential path left behind")
	})

	t.Run("a delete-current of the window's own write removes it", func(t *testing.T) {
		// Sequentially the update writes the row, the delete-current's guard
		// matches what it wrote, and the row goes. The window's net effect is
		// removal whatever the row named before it, so the write is dropped and
		// the removal reported unconditional. Emitting both would leave which
		// one wins to the plugin's statement order, which is not a contract.
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t,
			mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
			mkDeleteCurrent(runX),
		)
		require.Equal(t, 2, b.Len(), "the update and the delete-current both emit")
		for i, e := range reqs(b) {
			require.Same(t, wr(b, 0), e.Workflow(),
				"both requests of one workflow stand on one record")
			require.Nil(t, wr(b, i).CurrentWrite, "the delete removed what the window wrote")
			require.True(t, wr(b, i).CurrentRemoved)
		}

		// The rule apply registers a record's own assertions under, and the
		// reason it is the batch's to state: two consumers read it — the drive
		// and the attribution that reads back what the drive asserted — and a
		// record asserted twice, or attributed under a different rule than it
		// was asserted under, is a disagreement nothing else would catch.
		var first []bool
		for e := range b.Each() {
			require.Same(t, wr(b, 0), e.Workflow())
			first = append(first, e.FirstOfWorkflow())
		}
		require.Equal(t, []bool{true, false}, first,
			"one workflow, two requests: the first names the record and the second does not")
	})

	// The overlay and the drain answer the same question about the current row
	// — what did this window do to it — off one derivation, while the record
	// states it as two fields that can express what no shape can: a write and a
	// removal together. So this is the table that says they agree: the shape the
	// overlay renders and the record apply asserts, from one window, in one
	// place.
	t.Run("the overlay and the drain agree about the row", func(t *testing.T) {
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		write := func() mutation.Mutation {
			return mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st })
		}

		for _, c := range []struct {
			name    string
			window  []mutation.Mutation
			want    fold.CurrentShape
			written bool
			removed bool
		}{
			{"a write", []mutation.Mutation{write()}, fold.CurrentWritten, true, false},
			{"a write the window then removed", []mutation.Mutation{write(), mkDeleteCurrent(runX)}, fold.CurrentGone, false, true},
			{"a guard over the base", []mutation.Mutation{mkDeleteCurrent(runY)}, fold.CurrentGuarded, false, false},
			{"a run touched without the row", []mutation.Mutation{mkSet(runX, 3)}, fold.CurrentUnheld, false, false},
		} {
			t.Run(c.name, func(t *testing.T) {
				a := fold.New(shard)
				add(t, a, c.window...)

				require.Equal(t, c.want, a.ViewCurrent(nsID, wfID).Shape)

				b := a.Drain()
				require.NotZero(t, b.Len())
				rec := wr(b, 0)
				require.Equal(t, c.written, rec.CurrentWrite != nil, "the record's write")
				require.Equal(t, c.removed, rec.CurrentRemoved, "the record's removal")
				require.False(t, rec.CurrentWrite != nil && rec.CurrentRemoved,
					"a write and a removal of one row may never be emitted together")
			})
		}
	})

	t.Run("each visits every request once, in the batch's own order", func(t *testing.T) {
		b := drainOne(t, mkUpdate(runX, 2), mkUpdate(runY, 2))
		require.Equal(t, 2, b.Len(), "two runs, two requests, one workflow record")

		var order []wal.Seqno
		firsts := 0
		for e := range b.Each() {
			order = append(order, e.TailSeqno)
			if e.FirstOfWorkflow() {
				firsts++
			}
		}
		require.Equal(t, []wal.Seqno{reqs(b)[0].TailSeqno, reqs(b)[1].TailSeqno}, order,
			"apply registers in this order, so Each may not reorder it")
		require.Equal(t, 1, firsts, "one record, so exactly one request names it first")
	})

	t.Run("a delete-current of another run leaves the write alone", func(t *testing.T) {
		// Sequentially a no-op: the row names the window's own write, not runY.
		// The net effect is the write, and the delete is not emitted.
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t,
			mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
			mkDeleteCurrent(runY),
		)
		require.Equal(t, 1, b.Len(), "the no-op delete is not emitted")
		require.NotNil(t, wr(b, 0).CurrentWrite)
		require.Equal(t, runX, wr(b, 0).CurrentWrite.RunID)
		require.False(t, wr(b, 0).CurrentRemoved)
	})

	t.Run("a write after a delete-current supersedes it", func(t *testing.T) {
		// The row is removed and then written again, so it ends holding the
		// write. The delete has no effect left to reproduce, and keeping it
		// would race the write inside one transaction.
		st := stateOf(runX, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING)
		b := drainOne(t,
			mkUpdate(runX, 2, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
			mkDeleteCurrent(runX),
			mkUpdate(runX, 3, func(m *p.InternalWorkflowMutation) { m.ExecutionState = st }),
		)
		require.Equal(t, 1, b.Len(), "the superseded delete-current leaves the window")
		require.Equal(t, mutation.KindUpdate, reqs(b)[0].Request.Kind())
		require.NotNil(t, wr(b, 0).CurrentWrite)
		require.False(t, wr(b, 0).CurrentRemoved)
	})

	t.Run("a delete-current with no write above it stays guarded", func(t *testing.T) {
		// Nothing in the window wrote the row, so the guard is a question about
		// the pre-window row and the store's own semantics reproduce the
		// sequential path.
		b := drainOne(t, mkDeleteCurrent(runX))
		require.Equal(t, 1, b.Len())
		require.Nil(t, wr(b, 0).CurrentWrite)
		require.False(t, wr(b, 0).CurrentRemoved, "the removal is conditional on what the row names")
	})

}
