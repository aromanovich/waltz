package memcold

// Internal, because the claim is about columns no read of this store returns:
// both arms of the current-row read answer from the blob first, so a scalar
// written wrong is invisible from outside this file and stays wrong until
// something that is not this layer reads the table — a fallback read of a row
// whose blob a later schema drops, or a query written by an operator.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aromanovich/waltz/fold"
)

// TestTheCurrentRowsColumnsComeFromItsBlob drives the derivation
// [writeCurrentRow]'s comment claims: [fold.CurrentWrite] carries the run, the
// state and the last write version, and every other column is recovered from the
// serialised state so that the row's scalars and its blob cannot disagree.
//
// The start time is the one worth a message. It is what the workflow-id reuse
// check above measures against, nothing ever back-fills it, and a NULL there is
// read as a run that began at the zero time — so `WorkflowIdReuseMinimalInterval`
// never fires again for that workflow, the reuse arm skips its refusal and the
// terminate arm terminates a live run instead of answering ResourceExhausted.
// Dropping the line leaves every read in this repository answering correctly,
// the blob being read first.
func TestTheCurrentRowsColumnsComeFromItsBlob(t *testing.T) {
	ctx := context.Background()
	s, release, err := New("memcold-current-columns")
	require.NoError(t, err)
	t.Cleanup(release)

	const shardID = 1
	ns, workflowID, run := primitives.MustParseUUID(uuid.NewString()), "wf", uuid.NewString()
	began := time.Date(2026, 9, 30, 4, 5, 6, 0, time.UTC)

	state := &persistencespb.WorkflowExecutionState{
		RunId:           run,
		CreateRequestId: "the-create-request",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		StartTime:       timestamppb.New(began),
	}
	blob, err := serialization.WorkflowExecutionStateToBlob(state)
	require.NoError(t, err)

	tx, err := s.db.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, writeCurrentRow(ctx, tx, shardID, ns, workflowID, &fold.CurrentWrite{
		RunID:            run,
		StateBlob:        blob,
		LastWriteVersion: 7,
		State:            state.State,
	}, false))
	require.NoError(t, tx.Commit())

	row, err := s.db.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID,
	})
	require.NoError(t, err)

	require.NotNil(t, row.StartTime,
		"a NULL start_time is read as a run that began at the zero time, which is every reuse "+
			"interval measured as ~2000 years and the minimal-interval refusal never firing again")
	require.True(t, row.StartTime.Equal(began), "got %v", row.StartTime)
	require.Equal(t, "the-create-request", row.CreateRequestID)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, row.Status)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, row.State)
	require.EqualValues(t, 7, row.LastWriteVersion)
	require.Equal(t, run, row.RunID.String())

	// A state that carries none writes none, and this arm is the one nothing
	// drove: every fixture in the tree fills the field, so the nil check is
	// removable with the whole of `go test ./...` green — and what it does instead
	// is dereference, inside the drain's transaction, on the shard's own
	// goroutine. The fold's twin of this function has the same claim, and `fold`'s
	// is the one DURABILITY.md's start-time entry names.
	withoutStart := &persistencespb.WorkflowExecutionState{
		RunId:           run,
		CreateRequestId: "the-create-request",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	bare, err := serialization.WorkflowExecutionStateToBlob(withoutStart)
	require.NoError(t, err)

	tx, err = s.db.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, writeCurrentRow(ctx, tx, shardID, ns, workflowID, &fold.CurrentWrite{
		RunID:            run,
		StateBlob:        bare,
		LastWriteVersion: 8,
		State:            withoutStart.State,
	}, true))
	require.NoError(t, tx.Commit())

	row, err = s.db.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: ns, WorkflowID: workflowID,
	})
	require.NoError(t, err)
	require.Nil(t, row.StartTime, "a state with no start time writes no start time rather than inventing one")
	require.EqualValues(t, 8, row.LastWriteVersion, "and the row is the one that was just written")
}
