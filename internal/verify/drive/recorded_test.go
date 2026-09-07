package drive_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/internal/verify/checker"
	"github.com/aromanovich/waltz/internal/verify/drive"
	"github.com/aromanovich/waltz/mutation"
	"github.com/aromanovich/waltz/wal"
)

const (
	shard = wal.ShardID(1)
	epoch = wal.Epoch(7)
)

// store answers a create with whatever the test put in it, and — because the
// order of the two things is the contract — reads the record back while the
// call is in flight.
type store struct {
	p.ExecutionStore

	err   error
	block time.Duration
	path  string
	// linesDuringCall is the record as it stood while the store was being
	// called.
	linesDuringCall []checker.Line
	calls           int
}

func (s *store) CreateWorkflowExecution(
	ctx context.Context, _ *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	s.calls++
	if s.path != "" {
		s.linesDuringCall, _ = checker.ReadRecord(s.path)
	}
	if s.block > 0 {
		select {
		case <-time.After(s.block):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, s.err
}

func recorder(t *testing.T, below *store) (drive.Recorder, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "record.jsonl")
	below.path = path
	rec, err := checker.NewRecord(path, "node-1", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rec.Close() })
	return drive.Recorder{Store: below, Record: rec, Shard: shard, Epoch: epoch}, path
}

func aCreate() mutation.Mutation {
	run := uuid.NewString()
	return mutation.Mutation{Create: &p.InternalCreateWorkflowExecutionRequest{
		ShardID: int32(shard),
		Mode:    p.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: p.InternalWorkflowSnapshot{
			NamespaceID: uuid.NewString(),
			WorkflowID:  "recorded",
			RunID:       run,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: uuid.NewString(),
				RunId:           run,
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			DBRecordVersion: 1,
		},
	}}
}

func events(t *testing.T, path string) []string {
	t.Helper()
	lines, err := checker.ReadRecord(path)
	require.NoError(t, err)
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.Event)
	}
	return out
}

// The order is the whole of it: a process killed inside the call must leave a
// call nothing accounts for, never an entry nothing accounts for. Nothing but
// this can check it: a run that is killed leaves the record as its evidence,
// which is no use if the order it was written in is unknown.
func TestTheCallIsDurableBeforeTheStoreIsTouched(t *testing.T) {
	below := &store{}
	rec, path := recorder(t, below)

	out, err := rec.Call(context.Background(), aCreate())
	require.NoError(t, err)
	require.True(t, out.Acked())

	require.Len(t, below.linesDuringCall, 1,
		"the store was called before the call line was fsynced, so a kill here would leave an "+
			"entry with no call")
	require.Equal(t, checker.EventCall, below.linesDuringCall[0].Event)
	require.Equal(t, []string{checker.EventCall, checker.EventOutcome}, events(t, path))
}

func TestARefusedCallStillGetsItsOutcome(t *testing.T) {
	below := &store{err: serviceerror.NewInvalidArgument("no")}
	rec, path := recorder(t, below)

	out, err := rec.Call(context.Background(), aCreate())
	require.NoError(t, err)
	require.False(t, out.Acked())
	require.Equal(t, checker.Refused, out.Outcome)
	require.Contains(t, out.Detail(), "no")
	require.Equal(t, []string{checker.EventCall, checker.EventOutcome}, events(t, path),
		"a refusal is an answer: it is the subject matter and not a reason to stop recording")
}

func TestAFencedRefusalIsNotedBesideItsOutcome(t *testing.T) {
	below := &store{err: &p.ShardOwnershipLostError{ShardID: int32(shard), Msg: "taken"}}
	rec, path := recorder(t, below)

	out, err := rec.Call(context.Background(), aCreate())
	require.NoError(t, err)
	require.True(t, out.Fenced())
	require.Equal(t, []string{checker.EventCall, checker.EventOutcome, checker.EventFenced},
		events(t, path),
		"a case reading the record for a fence must find one wherever the caller's own note went")
}

// The deadline is per call and is released per call: a driver whose first call
// hit its bound must not find the second one already out of time.
func TestTheDeadlineIsOneCallsAndIsReleasedWithIt(t *testing.T) {
	below := &store{block: 50 * time.Millisecond}
	rec, path := recorder(t, below)
	rec.Timeout = 5 * time.Millisecond

	out, err := rec.Call(context.Background(), aCreate())
	require.NoError(t, err)
	require.Equal(t, checker.Unknown, out.Outcome,
		"a call that hit its deadline is the third class: the driver does not know")

	below.block = 0
	out, err = rec.Call(context.Background(), aCreate())
	require.NoError(t, err)
	require.True(t, out.Acked(), "the second call got a budget of its own")
	require.Equal(t, 2, below.calls)
	require.Equal(t, []string{
		checker.EventCall, checker.EventOutcome, checker.EventCall, checker.EventOutcome,
	}, events(t, path))
}

// A record that cannot be written is a run that proves nothing, so the error a
// caller has to act on is the record's and never the store's.
func TestTheErrorHandedBackIsTheRecordsAndNotTheStores(t *testing.T) {
	below := &store{err: errors.New("the datashard is unavailable")}
	rec, _ := recorder(t, below)

	out, err := rec.Call(context.Background(), aCreate())
	require.NoError(t, err, "the store's own failure is the subject matter, not a driver error")
	require.Equal(t, checker.Unknown, out.Outcome)

	require.NoError(t, rec.Record.Close())
	_, err = rec.Call(context.Background(), aCreate())
	require.Error(t, err, "a record that cannot be written stops the driver")
}
