package drive

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// Both takers return the rangeID a write against the shard must carry: every
// write path asserts it (AssertShard in the plugin's mss.go), so a mutation
// applied without one is rejected for the one reason that says nothing about
// what was being driven. Which of the two a caller wants is decided by whether
// it needs to look like an owner arriving.

// TakeShardBumpingRangeID takes the shard the way a history node does: the first
// load, then the rangeID bump that renewRangeLocked sends. That bump is what a
// ShardStore wrapper sees as an acquire — a heartbeat is what it must not — so
// this is the taker that gives the shard an apply cycle.
func TakeShardBumpingRangeID(ctx context.Context, store p.ShardStore, shardID int32) (int64, error) {
	manager, info, err := getOrCreateShard(ctx, store, shardID)
	if err != nil {
		return 0, err
	}
	previous := info.RangeId
	info.RangeId++
	if err := manager.UpdateShard(ctx, &p.UpdateShardRequest{
		ShardInfo:       info,
		PreviousRangeID: previous,
	}); err != nil {
		return 0, err
	}
	return info.RangeId, nil
}

// TakeShardWithoutBumpingRangeID only makes sure the shard row exists, which is
// all a caller writing straight into the cold store needs: with no wrapper under
// it, an acquire nothing observes buys nothing.
func TakeShardWithoutBumpingRangeID(ctx context.Context, store p.ShardStore, shardID int32) (int64, error) {
	_, info, err := getOrCreateShard(ctx, store, shardID)
	if err != nil {
		return 0, err
	}
	return info.RangeId, nil
}

func getOrCreateShard(ctx context.Context, store p.ShardStore, shardID int32) (p.ShardManager, *persistencespb.ShardInfo, error) {
	manager := p.NewShardManager(store, serialization.NewSerializer())
	got, err := manager.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: &persistencespb.ShardInfo{ShardId: shardID, RangeId: 1},
	})
	if err != nil {
		return nil, nil, err
	}
	return manager, got.ShardInfo, nil
}
