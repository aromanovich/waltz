// Package memcold is the cold store waltz ships: Temporal's own SQL
// persistence over an in-process SQLite database that dies with the process.
// It is a real store, not a test double. The execution store is upstream's,
// embedded whole (all 28 methods, schema, row layouts, error classes), and
// none of its methods is shadowed here.
//
// What waltz adds beside it: [Store.Apply], the batched write of a folded
// window, which commits the watermark ([Store.Watermark]) and the window's
// event history in the same transaction (the store declares
// [cold.HistoryApplier] via [Store.AppliesHistory], so every composition over
// it carries an intercepted write's event batches in the record); and
// [Store.GetCurrentExecutionWithLastWriteVersion], the current-row read that
// returns the last_write_version Temporal's response type cannot hold. The 28
// inherited methods know nothing of waltz and must not.
//
// The package stays small because the store is not rewritten: Temporal's four
// exported suites in go.temporal.io/server/common/persistence/tests judge it
// exactly as they judge a plugin. The database is mode=memory with a name
// minted per store, so it needs no cluster, container, port or file.
//
// # Bringing your own
//
// A client on a real database does not use or wrap this package. It
// implements cold.Applier and cold.Watermarker over its own store and owes the
// four obligations the cold package documents, plus bounding its own calls.
// [Store.Apply] is the worked example. persistence.ExecutionStore cannot
// express a transaction spanning many workflows, so the store keeps the
// database handle beside the embedded store and opens the transaction there.
// A driver with no such handle cannot meet the contract and must refuse
// rather than apply a batch in pieces.
package memcold

import (
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	sqliteplugin "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
)

// Store is a Temporal history shard's cold store over one ephemeral database.
// Every execution-store method is upstream's, by embedding. [Store.ShardStore]
// is undecorated, because waltz folds no shard-row writes.
type Store struct {
	p.ExecutionStore

	// db opens a folded window's transaction: BeginTx exists on sqlplugin.DB,
	// not on p.ExecutionStore.
	db sqlplugin.DB

	shards  p.ShardStore
	factory *sql.Factory
}

var _ p.ExecutionStore = (*Store)(nil)

// New creates a fresh database and returns the store over it, plus a func that
// releases the store's handle.
//
// Stores are isolated only by the unique database name (the plugin pools
// connections by DSN). The release func does not close the database, so never
// rely on teardown for isolation.
//
// clusterName is what the shard store returns from GetClusterName; it must
// match the server's ClusterMetadata.
func New(clusterName string) (*Store, func(), error) {
	cfg := config.SQL{
		PluginName:   sqliteplugin.PluginName,
		DatabaseName: "memcold_" + uuid.NewString(),
		ConnectAttributes: map[string]string{
			"mode": "memory",
			// Under cache=private a second connection would silently open
			// an empty database; only the plugin's one-connection pool would
			// prevent it.
			"cache": "shared",
		},
	}

	logger := log.NewNoopLogger()
	factory := sql.NewFactory(cfg, resolver.NewNoopResolver(), clusterName, logger, metrics.NoopMetricsHandler)

	// In memory mode the plugin creates the schema on first connection, so do
	// not also call schema/sqlite.SetupSchema: it would fail on CREATE TABLE.
	db, err := factory.GetDB()
	if err != nil {
		factory.Close()
		return nil, nil, fmt.Errorf("memcold: opening %s: %w", cfg.DatabaseName, err)
	}
	if err := setupWatermarks(db); err != nil {
		factory.Close()
		return nil, nil, err
	}
	executions, err := factory.NewExecutionStore()
	if err != nil {
		factory.Close()
		return nil, nil, fmt.Errorf("memcold: execution store over %s: %w", cfg.DatabaseName, err)
	}
	shards, err := factory.NewShardStore()
	if err != nil {
		factory.Close()
		return nil, nil, fmt.Errorf("memcold: shard store over %s: %w", cfg.DatabaseName, err)
	}

	return &Store{
		ExecutionStore: executions,
		db:             db,
		shards:         shards,
		factory:        factory,
	}, factory.Close, nil
}

// ShardStore is the shard half of the same database.
func (s *Store) ShardStore() p.ShardStore { return s.shards }
