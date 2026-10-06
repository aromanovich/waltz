package drive

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// TakeShardBumpingRangeID takes the shard as a history node does: load it, then
// bump the rangeID as renewRangeLocked does. A ShardStore wrapper treats that
// bump (not a heartbeat) as an acquire, which starts the shard's apply cycle.
// It returns the rangeID that [Apply] must stamp; the store rejects writes
// without it.
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
