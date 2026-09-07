package wrapper

// The history-task path through the wrapper: AddHistoryTasks and
// RangeCompleteHistoryTasks go into the log in intercept mode and transit in
// passthrough; CompleteHistoryTask is refused in intercept mode.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"

	"github.com/aromanovich/waltz/wal"
)

func rangeComplete() *p.RangeCompleteHistoryTasksRequest {
	return &p.RangeCompleteHistoryTasksRequest{
		ShardID:             7,
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(time.Unix(1700000000, 0).UTC(), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(time.Unix(1700000900, 0).UTC(), 0),
	}
}

// No expectation is set on the base store, so any call to it fails the
// controller: the cold store hears nothing until a drain carries the range.
func TestARangeCompletionGoesIntoTheLogAndNotIntoTheStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	layer := &recordingLayer{}
	store := newStore(t, base, Options{Layer: layer})

	req := rangeComplete()
	require.NoError(t, store.RangeCompleteHistoryTasks(context.Background(), req))

	require.Len(t, layer.got, 1)
	require.Same(t, req, layer.got[0].RangeCompleteTasks,
		"the layer is handed the caller's own request, range and all")
	require.Equal(t, int64(1), store.Counts().TasksCompleted)
}

// The layer's error reaches the caller unwrapped, like every error on this
// path.
func TestARangeCompletionIsAnsweredAtTheAppend(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	boom := errors.New("the tail is full")
	layer := &recordingLayer{err: boom}
	store := newStore(t, base, Options{Layer: layer})

	err := store.RangeCompleteHistoryTasks(context.Background(), rangeComplete())
	require.True(t, err == boom, //nolint:errorlint // identity is the assertion
		"the layer's error travels out untouched, like every error on this path")
}

// With no layer the call reaches the base method and nothing is counted.
func TestARangeCompletionWithNobodyWatchingIsATransit(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	req := rangeComplete()
	base.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), req).Return(nil).Times(1)

	store := newStore(t, base, Options{})
	require.NoError(t, store.RangeCompleteHistoryTasks(context.Background(), req))
	require.Equal(t, Counts{}, store.Counts(), "passthrough counts nothing")
}

// The other half of the task path, counted apart from the six mutable-state
// writes.
func TestAddHistoryTasksGoesIntoTheLogToo(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := versioned(ctrl)
	layer := &recordingLayer{}
	store := newStore(t, base, Options{Layer: layer})

	req := &p.InternalAddHistoryTasksRequest{ShardID: 7, RangeID: 4}
	require.NoError(t, store.AddHistoryTasks(context.Background(), req))

	require.Len(t, layer.got, 1)
	require.Same(t, req, layer.got[0].AddTasks)
	require.Equal(t, []wal.Epoch{4}, layer.epochs,
		"the request's rangeID is the epoch it was written under (invariant I11)")
	require.Equal(t, Counts{TasksWritten: 1}, store.Counts())
}

// The log's deletion record is a range per category, so a single key has no
// shape in it: intercept refuses rather than deleting a row the cold store may
// not hold yet and reporting success. Passthrough transits it.
func TestTheSingleKeyCompletionIsRefusedInInterceptModeOnly(t *testing.T) {
	ctx := context.Background()
	req := &p.CompleteHistoryTaskRequest{
		ShardID:      7,
		TaskCategory: tasks.CategoryTimer,
		TaskKey:      tasks.NewKey(time.Unix(1700000900, 0).UTC(), 3),
	}

	t.Run("intercept", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		base := versioned(ctrl)
		layer := &recordingLayer{}
		store := newStore(t, base, Options{Layer: layer})

		err := store.CompleteHistoryTask(ctx, req)
		require.True(t, err == ErrCompleteHistoryTaskUnsupported, //nolint:errorlint // identity is the assertion
			"the refusal is returned unwrapped, like every other answer on this path")
		require.IsType(t, &serviceerror.Unimplemented{}, err,
			"the caller is an admin handler and this is the status it has a branch for")
		require.Empty(t, layer.got, "a refusal writes nothing")
	})

	t.Run("passthrough", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		base := versioned(ctrl)
		base.EXPECT().CompleteHistoryTask(gomock.Any(), req).Return(nil).Times(1)
		require.NoError(t, newStore(t, base, Options{}).CompleteHistoryTask(ctx, req))
	})
}
