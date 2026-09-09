package wrapper

// The ShardStore wrapper's one observation — UpdateShard with a rangeID that
// moved — and the three ways it can be got wrong: reporting a heartbeat as an
// acquire, reporting the acquire after the base store has already moved the
// rangeID, and letting a refused acquire through to the base store.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"

	"github.com/aromanovich/waltz/wal"
)

var errObserver = errors.New("the log is fenced at a higher epoch")

// acquire is one call to ShardAcquired.
type acquire struct {
	shard wal.ShardID
	epoch wal.Epoch
}

// recordingObserver records the acquires it was told about, and their order
// relative to the base store: the base store's own expectation appends "base"
// to the same events slice.
//
// The embedded [ShardLayer] is left nil: it satisfies the write and read halves
// this fake has no opinion about, and a call to either panics naming the method
// rather than passing silently, which verifies that the
// ShardStore path touches nothing but ShardAcquired.
type recordingObserver struct {
	ShardLayer
	events   []string
	acquired []acquire
	err      error
}

func (o *recordingObserver) ShardAcquired(_ context.Context, shard wal.ShardID, epoch wal.Epoch) error {
	o.events = append(o.events, "observer")
	o.acquired = append(o.acquired, acquire{shard, epoch})
	return o.err
}

func TestShardAcquireIsReportedBeforeTheRangeIDMoves(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockShardStore(ctrl)
	obs := &recordingObserver{}

	req := &p.InternalUpdateShardRequest{ShardID: 7, RangeID: 42, PreviousRangeID: 41}
	base.EXPECT().UpdateShard(gomock.Any(), req).DoAndReturn(
		func(context.Context, *p.InternalUpdateShardRequest) error {
			obs.events = append(obs.events, "base")
			return nil
		}).Times(1)

	require.NoError(t, NewShardStore(base, Options{Layer: obs}).UpdateShard(context.Background(), req))

	require.Equal(t, []acquire{{shard: 7, epoch: 42}}, obs.acquired,
		"the epoch is the new rangeID, not the previous one (I11)")
	require.Equal(t, []string{"observer", "base"}, obs.events,
		"the WAL fence comes before the rangeID bump: the epoch in the log may never lag the one in the database")
}

func TestShardHeartbeatIsNotAnAcquire(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock.NewMockShardStore(ctrl)
	obs := &recordingObserver{}
	store := NewShardStore(base, Options{Layer: obs})

	ctx := context.Background()
	// What updateShardInfo sends: the rangeID it already holds, on both fields.
	// There are far more heartbeats than acquires.
	heartbeat := &p.InternalUpdateShardRequest{ShardID: 7, RangeID: 42, PreviousRangeID: 42}
	base.EXPECT().UpdateShard(gomock.Any(), heartbeat).Return(nil).Times(1)
	require.NoError(t, store.UpdateShard(ctx, heartbeat))

	// The other two ShardStore calls a live shard makes, neither of them an
	// acquire: the first load, whose request has no rangeID field at all, and
	// the controller's linger probe, which sends the one it already holds.
	getOrCreate := &p.InternalGetOrCreateShardRequest{ShardID: 7}
	base.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreate).Return(nil, nil).Times(1)
	_, err := store.GetOrCreateShard(ctx, getOrCreate)
	require.NoError(t, err)

	assert := &p.AssertShardOwnershipRequest{ShardID: 7, RangeID: 42}
	base.EXPECT().AssertShardOwnership(gomock.Any(), assert).Return(nil).Times(1)
	require.NoError(t, store.AssertShardOwnership(ctx, assert))

	require.Empty(t, obs.acquired, "only a rangeID that moved is an acquire")
}

func TestARefusedAcquireDoesNotMoveTheRangeID(t *testing.T) {
	ctrl := gomock.NewController(t)
	// No expectation at all, so any call to the base store fails: a refused
	// acquire must leave the shard's rangeID where it was.
	base := mock.NewMockShardStore(ctrl)
	obs := &recordingObserver{err: errObserver}

	err := NewShardStore(base, Options{Layer: obs}).UpdateShard(context.Background(),
		&p.InternalUpdateShardRequest{ShardID: 7, RangeID: 42, PreviousRangeID: 41})

	require.True(t, err == errObserver, //nolint:errorlint // identity is the assertion
		"the observer's error must reach the shard controller unwrapped, got %v", err)
}
