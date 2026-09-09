package wrapper

import (
	"context"

	p "go.temporal.io/server/common/persistence"

	"github.com/aromanovich/waltz/wal"
)

// ShardStore is the WAL layer's ShardStore: six methods, five of them a pure
// transit and UpdateShard the layer's only window onto shard ownership. The
// history service sends UpdateShard from two places with the same shape and
// different meaning:
//
//   - renewRangeLocked, on acquire and on rangeID exhaustion, with
//     RangeID = PreviousRangeID + 1: the shard changing hands, and the birth of
//     an epoch (I11);
//   - updateShardInfo, periodically, with RangeID == PreviousRangeID: a
//     heartbeat that carries no news.
//
// Comparing the two fields is the whole of the distinction.
type ShardStore struct {
	base p.ShardStore

	// layer is [Options.Layer], the mode itself: nil is passthrough. No emitter
	// beside it, this store raising no counters of its own.
	layer ShardLayer
}

var _ p.ShardStore = (*ShardStore)(nil)

func NewShardStore(base p.ShardStore, opts Options) *ShardStore {
	return &ShardStore{base: base, layer: opts.Layer}
}

func (s *ShardStore) Close() { s.base.Close() }

func (s *ShardStore) GetName() string { return s.base.GetName() }

func (s *ShardStore) GetClusterName() string { return s.base.GetClusterName() }

// GetOrCreateShard transits. It looks like the acquire signal and is not one:
// it runs on first load only (a re-acquire skips it) and the admin GetShard API
// calls it with no shard context behind it. The epoch arrives with UpdateShard.
func (s *ShardStore) GetOrCreateShard(
	ctx context.Context, request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	return s.base.GetOrCreateShard(ctx, request)
}

// UpdateShard reports an acquire before delegating: the WAL is fenced at the
// new epoch first and the rangeID lands second, so the epoch in the log never
// lags the one in the database. A failed observer fails the acquire without the
// base store being called and leaves the previous owner's rangeID in place for
// the controller to retry.
//
// The test is inequality rather than "greater than", so that a rangeID which
// went backwards reaches the observer to be refused instead of passing as a
// heartbeat.
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

// AssertShardOwnership transits. Whether it probes the epoch at all is the base
// plugin's business — Temporal's own SQL and Cassandra stores both answer nil —
// and dynamic config can switch off the shard controller loop that drives it:
// nothing may be keyed on it.
func (s *ShardStore) AssertShardOwnership(
	ctx context.Context, request *p.AssertShardOwnershipRequest,
) error {
	return s.base.AssertShardOwnership(ctx, request)
}
