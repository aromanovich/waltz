package memcold_test

// Every suite above is written as though it had the database to itself, and a
// second store leaking rows into the first turns "green" into "green when run
// alone". These are the two directions that can go wrong.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"

	"github.com/aromanovich/waltz/cold/memcold"
)

func TestTwoStoresShareNoShard(t *testing.T) {
	ctx := context.Background()
	serializer := serialization.NewSerializer()

	first := p.NewShardManager(newStore(t).ShardStore(), serializer)
	second := p.NewShardManager(newStore(t).ShardStore(), serializer)

	created, err := first.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 7},
	})
	require.NoError(t, err)
	require.EqualValues(t, 7, created.ShardInfo.RangeId)

	// Same shard ID, different initial range: the second store has to create
	// the row rather than find the first store's.
	found, err := second.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 99},
	})
	require.NoError(t, err)
	require.EqualValues(t, 99, found.ShardInfo.RangeId)
}

func TestOneStoreKeepsItsOwnRows(t *testing.T) {
	ctx := context.Background()
	serializer := serialization.NewSerializer()
	store := newStore(t)

	// The same database reached the two ways a caller reaches it: directly, and
	// through the seam a server comes in by.
	served, err := memcold.NewAbstractDataStoreFactory(store).
		NewFactory(config.CustomDatastoreConfig{}, resolver.NewNoopResolver(), clusterName, log.NewNoopLogger(), metrics.NoopMetricsHandler).
		NewShardStore()
	require.NoError(t, err)

	written := p.NewShardManager(store.ShardStore(), serializer)
	read := p.NewShardManager(served, serializer)

	_, err = written.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 7},
	})
	require.NoError(t, err)

	found, err := read.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          1,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 99},
	})
	require.NoError(t, err)
	require.EqualValues(t, 7, found.ShardInfo.RangeId)
}
