package memcold

// Internal, because the arm under test cannot be reached from outside: every
// write this store makes fills the current row's state blob, so a row without
// one is a record written by a build older than that column and there is no
// exported way to stage it.

import (
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
)

// TestTheCurrentRowsStateIsReadBlobFirstThenColumns drives both arms of the one
// derivation every current-row assertion in this package is judged against —
// the drain's, under its own lock, and the layer's versioned read.
//
// Neither arm was driven. Every fixture writes the row through a real write,
// which always fills the blob, so the columns arm was reachable from nothing:
// dropping its run id or its status, or forcing every read down it, each left
// the whole of `go test ./...` green. What those answer is not cosmetic. The
// conflict a refused write carries is built from this value, and a conflict
// naming no run is one the history service declines to resolve at all —
// request-id dedup included, which is what a retried start needs.
//
// The two arms carry different amounts, which is why the order between them is
// the claim and not a detail: the blob has the run id and every request id the
// run has accumulated, the columns have the create request id alone.
func TestTheCurrentRowsStateIsReadBlobFirstThenColumns(t *testing.T) {
	run, columnsRequest := uuid.NewString(), uuid.NewString()
	started := time.Date(2026, 9, 19, 3, 0, 0, 0, time.UTC).UTC()

	// The columns say one thing and the blob another, so which arm answered is
	// readable off the result rather than inferred.
	blobState := &persistencespb.WorkflowExecutionState{
		RunId:           run,
		CreateRequestId: "from-the-blob",
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		StartTime:       timestamppb.New(started),
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			"from-the-blob": {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventId: 1},
			"a-later-one":   {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, EventId: 7},
		},
	}
	encoded, err := serialization.WorkflowExecutionStateToBlob(blobState)
	require.NoError(t, err)

	columnsOnly := &sqlplugin.CurrentExecutionsRow{
		ShardID:          1,
		NamespaceID:      primitives.MustParseUUID(uuid.NewString()),
		WorkflowID:       "a-workflow",
		RunID:            primitives.MustParseUUID(run),
		CreateRequestID:  columnsRequest,
		StartTime:        &started,
		LastWriteVersion: 41,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	t.Run("a row with no blob is rebuilt from the columns beside it", func(t *testing.T) {
		state, err := executionStateOf(columnsOnly)
		require.NoError(t, err)

		require.Equal(t, run, state.RunId,
			"a state naming no run is a conflict the history service declines to resolve")
		require.Contains(t, state.RequestIds, columnsRequest,
			"the create request id is the only one these columns have, and a retried start "+
				"deduplicates against it")
		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			state.RequestIds[columnsRequest].EventType)
		require.Equal(t, columnsRequest, state.CreateRequestId)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state.State)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state.Status,
			"the status is half of what a create over a finished run asserts")
		require.Equal(t, started, state.StartTime.AsTime(),
			"upstream reads an absent start time as the zero time, so every interval the "+
				"workflow-id reuse check measures against it becomes enormous")
	})

	t.Run("a row with no start time carries none rather than the zero time", func(t *testing.T) {
		row := *columnsOnly
		row.StartTime = nil

		state, err := executionStateOf(&row)
		require.NoError(t, err)
		require.Nil(t, state.StartTime)
	})

	t.Run("a row with a blob is read out of the blob and not the columns", func(t *testing.T) {
		row := *columnsOnly
		row.Data, row.DataEncoding = encoded.Data, encoded.EncodingType.String()

		state, err := executionStateOf(&row)
		require.NoError(t, err)

		require.Equal(t, "from-the-blob", state.CreateRequestId,
			"the columns are the fallback, not the answer: they carry one request id where "+
				"the blob carries every one the run has accumulated")
		require.Contains(t, state.RequestIds, "a-later-one",
			"a request id only the blob has must survive the read")
		require.NotContains(t, state.RequestIds, columnsRequest)
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state.State,
			"and the state too, which is what a create over a finished run asserts on")
	})

	t.Run("a blob is trusted only when both of its columns are there", func(t *testing.T) {
		// Each half alone. A DataBlob with no encoding cannot be decoded at
		// all, so reading either as "there is a blob" turns every row upstream
		// would have answered out of its columns into a failed read — and the
		// caller of a failed current-row read cannot settle its condition.
		for name, half := range map[string]func(*sqlplugin.CurrentExecutionsRow){
			"bytes with no encoding": func(r *sqlplugin.CurrentExecutionsRow) { r.Data = encoded.Data },
			"an encoding with no bytes": func(r *sqlplugin.CurrentExecutionsRow) {
				r.DataEncoding = encoded.EncodingType.String()
			},
		} {
			t.Run(name, func(t *testing.T) {
				row := *columnsOnly
				half(&row)

				state, err := executionStateOf(&row)
				require.NoError(t, err, "half a blob must fall back, not fail the read")
				require.Equal(t, columnsRequest, state.CreateRequestId,
					"and it must fall back to the columns, which are the half that is readable")
			})
		}
	})

	t.Run("a blob that will not deserialise is reported, never answered from the columns", func(t *testing.T) {
		row := *columnsOnly
		row.Data, row.DataEncoding = []byte("not a proto"), encoded.EncodingType.String()

		_, err := executionStateOf(&row)
		require.Error(t, err,
			"answering out of the columns would hand the caller a row that names no request id "+
				"the run has, with no way to tell it from one that never had any")
	})

	t.Run("the response carries the run and the version beside the state", func(t *testing.T) {
		row := *columnsOnly
		row.Data, row.DataEncoding = encoded.Data, encoded.EncodingType.String()

		resp, version, err := currentRowResponse(&row)
		require.NoError(t, err)
		require.Equal(t, run, resp.RunID,
			"upstream's own read fills the response's field and leaves the state's copy to the "+
				"blob, and a conflict built from the state alone names nobody")
		require.EqualValues(t, 41, version)

		absent, zero, err := currentRowResponse(nil)
		require.NoError(t, err)
		require.Nil(t, absent, "absence is a nil row and not an error")
		require.Zero(t, zero)
	})
}
