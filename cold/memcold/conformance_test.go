package memcold_test

// The store is judged by Temporal's own four suites and by nothing written
// here. They are the definition of done: a suite of ours would be this
// package's opinion of what a cold store owes, where these are the server's,
// and the server is the only caller memcold will ever have.
//
// Each suite gets a store of its own — they number their shards from one and
// upwards, so two sharing a database would collide.

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/tests"

	"github.com/aromanovich/waltz/cold/memcold"
)

const clusterName = "memcold-conformance"

func newStore(t *testing.T) *memcold.Store {
	t.Helper()
	store, release, err := memcold.New(clusterName)
	if err != nil {
		t.Fatalf("memcold.New: %v", err)
	}
	t.Cleanup(release)
	return store
}

func TestShardSuite(t *testing.T) {
	store := newStore(t)
	suite.Run(t, tests.NewShardSuite(
		t,
		store.ShardStore(),
		serialization.NewSerializer(),
		log.NewNoopLogger(),
	))
}

func TestExecutionMutableStateSuite(t *testing.T) {
	store := newStore(t)
	suite.Run(t, tests.NewExecutionMutableStateSuite(
		t,
		store.ShardStore(),
		store,
		serialization.NewSerializer(),
		&p.HistoryBranchUtilImpl{},
		log.NewNoopLogger(),
	))
}

func TestExecutionMutableStateTaskSuite(t *testing.T) {
	store := newStore(t)
	suite.Run(t, tests.NewExecutionMutableStateTaskSuite(
		t,
		store.ShardStore(),
		store,
		serialization.NewSerializer(),
		log.NewNoopLogger(),
	))
}

func TestHistoryEventsSuite(t *testing.T) {
	store := newStore(t)
	suite.Run(t, tests.NewHistoryEventsSuite(t, store, log.NewNoopLogger()))
}
