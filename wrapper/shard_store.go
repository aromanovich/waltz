package wrapper

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/wal"
)

// ShardStore is the WAL layer's ShardStore. Five methods delegate unchanged;
// UpdateShard is the layer's only view of shard ownership. History calls it
// from two places:
//
//   - renewRangeLocked (acquire, rangeID exhaustion) with
//     RangeID = PreviousRangeID + 1: a new epoch (I11);
//   - updateShardInfo (periodic) with RangeID == PreviousRangeID: a heartbeat.
type ShardStore struct {
	base p.ShardStore

	// layer is [Options.Layer]; nil means passthrough.
	layer ShardLayer
}

var _ p.ShardStore = (*ShardStore)(nil)

func NewShardStore(base p.ShardStore, opts Options) *ShardStore {
	return &ShardStore{base: base, layer: opts.Layer}
}

func (s *ShardStore) Close() { s.base.Close() }

func (s *ShardStore) GetName() string { return s.base.GetName() }

func (s *ShardStore) GetClusterName() string { return s.base.GetClusterName() }

// GetOrCreateShard delegates. It is not an acquire signal: a re-acquire skips
// it and the admin GetShard API calls it with no shard context. The epoch
// arrives with UpdateShard.
func (s *ShardStore) GetOrCreateShard(
	ctx context.Context, request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	return s.base.GetOrCreateShard(ctx, request)
}

// UpdateShard fences the WAL at the new epoch before writing the rangeID, so
// the log's epoch never lags the database's. If fencing fails, the base store
// is not called and the acquire fails, to be retried by the shard context.
//
// The test is != rather than >, so a rangeID that went backwards reaches the
// layer and is refused instead of passing as a heartbeat.
func (s *ShardStore) UpdateShard(ctx context.Context, request *p.InternalUpdateShardRequest) error {
	if s.layer != nil && request.RangeID != request.PreviousRangeID {
		if err := s.layer.ShardAcquired(
			ctx, wal.ShardID(request.ShardID), wal.Epoch(request.RangeID),
		); err != nil {
			return err
		}
	}
	return s.base.UpdateShard(ctx, request)
}

// AssertShardOwnership delegates. Nothing may rely on it: Temporal's SQL and
// Cassandra stores return nil without checking, and dynamic config can turn
// off the loop that calls it.
func (s *ShardStore) AssertShardOwnership(
	ctx context.Context, request *p.AssertShardOwnershipRequest,
) error {
	return s.base.AssertShardOwnership(ctx, request)
}
