package baserow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/baserow"
)

// store answers both reads with whatever the test put in it, and keeps the
// requests, since stamping the shard is half of what this package is for.
type store struct {
	// Embedded and nil: the fake answers the two reads this package makes and
	// would panic on anything else, which is what a test of it should do.
	p.ExecutionStore

	runResp     *p.InternalGetWorkflowExecutionResponse
	runErr      error
	currentResp *p.InternalGetCurrentExecutionResponse
	version     int64
	currentErr  error

	runReq     *p.GetWorkflowExecutionRequest
	currentReq *p.GetCurrentExecutionRequest
}

func (s *store) GetWorkflowExecution(
	_ context.Context, req *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	s.runReq = req
	return s.runResp, s.runErr
}

func (s *store) GetCurrentExecutionWithLastWriteVersion(
	_ context.Context, req *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, int64, error) {
	s.currentReq = req
	return s.currentResp, s.version, s.currentErr
}

// plainStore is Temporal's own interface and nothing more: the shape a build
// over an unpatched checkout would hand over.
type plainStore struct{ p.ExecutionStore }

func TestAnAbsentRowIsANilRowAndNotAnError(t *testing.T) {
	below := &store{
		runErr:     serviceerror.NewNotFound("no such run"),
		currentErr: serviceerror.NewNotFound("no current run"),
	}
	rows := baserow.New(below)

	run, err := rows.Run(t.Context(), 3, "ns", "wf", "run")
	require.NoError(t, err, "absence is the answer the condition authority asserts on")
	require.Nil(t, run)

	current, version, err := rows.Current(t.Context(), 3, "ns", "wf")
	require.NoError(t, err)
	require.Nil(t, current)
	require.Zero(t, version, "no row carries no last_write_version")
}

func TestAReadThatFailedIsNotAnAbsentRow(t *testing.T) {
	failed := errors.New("the datashard is unavailable")
	below := &store{runErr: failed, currentErr: failed}
	rows := baserow.New(below)

	_, err := rows.Run(t.Context(), 3, "ns", "wf", "run")
	require.ErrorIs(t, err, failed, "a read that failed must not read as a row that is gone")

	_, _, err = rows.Current(t.Context(), 3, "ns", "wf")
	require.ErrorIs(t, err, failed)
}

func TestTheShardIsStampedOntoBothReads(t *testing.T) {
	below := &store{
		runResp:     &p.InternalGetWorkflowExecutionResponse{DBRecordVersion: 7},
		currentResp: &p.InternalGetCurrentExecutionResponse{RunID: "run"},
		version:     11,
	}
	rows := baserow.New(below)

	run, err := rows.Run(t.Context(), 3, "ns", "wf", "run")
	require.NoError(t, err)
	require.Same(t, below.runResp, run)
	require.Equal(t, &p.GetWorkflowExecutionRequest{
		ShardID: 3, NamespaceID: "ns", WorkflowID: "wf", RunID: "run",
	}, below.runReq)

	current, version, err := rows.Current(t.Context(), 3, "ns", "wf")
	require.NoError(t, err)
	require.Same(t, below.currentResp, current)
	require.EqualValues(t, 11, version, "the row's last_write_version travels beside it")
	require.Equal(t, &p.GetCurrentExecutionRequest{
		ShardID: 3, NamespaceID: "ns", WorkflowID: "wf",
	}, below.currentReq)
}

func TestAStoreWithoutTheVersionedReadIsRefusedByName(t *testing.T) {
	_, err := baserow.Of(plainStore{})
	require.ErrorIs(t, err, baserow.ErrNoVersionedRead,
		"a store that cannot carry last_write_version could confirm a current-row assertion and never refuse one")

	rows, err := baserow.Of(&store{})
	require.NoError(t, err)
	require.NotNil(t, rows)
}
