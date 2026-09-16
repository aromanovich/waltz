// Package memcold is the one cold store this repository ships: Temporal's own
// SQL persistence, running over a SQLite database that lives in this process
// and dies with it. It is a store, not a test double. The execution store is
// upstream's, embedded whole — all 28 methods, the schema they were written
// against, the row layouts, the error classes — and none of them is shadowed
// here. What waltz adds beside them is the batched write of a folded window —
// the one thing this package exists for and the one thing Temporal has no
// method for — with the watermark that write commits inside itself, and the
// current-row read that carries the last_write_version Temporal's own response
// type has nowhere to hold.
//
// That embedding is why the package is this small, and why it will not grow. A
// history shard's store is the hardest thing here to get right and the easiest
// to get plausibly wrong; the way to be sure of this one is not to write it.
// What says so is the four exported suites in
// go.temporal.io/server/common/persistence/tests, which judge this store
// exactly as they judge a plugin and pass without this package answering one of
// their calls itself. A store this repository had written would be judged by
// this repository's opinion of what a store owes; those suites are the server's.
//
// The database is ephemeral by construction — mode=memory, a name minted per
// store — so having somewhere real to write costs no cluster, no container, no
// port and no file.
//
// The 28 inherited methods know nothing of waltz and must not: they are the
// store a Temporal server calls, and a store that could see the layer would be
// judged by the thing it sits under. The layer's vocabulary enters through
// [Store.Apply] and [Store.Watermark] alone, beside that surface rather than
// inside it.
//
// # Bringing your own
//
// A client running against a real database does not use this and does not
// subclass it. It supplies cold.Applier and cold.Watermarker over its own
// store, and what it owes there is the four things the cold package's doc
// states.
//
// This package is the worked example of all four — [Store.Apply] is where they
// are stated statement by statement — and the shape it lands on is the
// transferable part. persistence.ExecutionStore has nowhere to
// declare a transaction spanning many workflows, so the store holds the
// database handle beside the embedded store and opens the transaction there.
// An implementer whose driver offers no such handle — no way to reach below the
// per-workflow interface — cannot satisfy the contract by trying harder inside
// it, and should say so rather than land a batch in pieces.
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
// It is an execution store by embedding, so every method not shadowed here is
// upstream's and stays upstream's across a version bump. [Store.ShardStore] is
// the shard half of the same database, undecorated: no write waltz folds is a
// shard-row write.
type Store struct {
	p.ExecutionStore

	// The seam a folded window's single transaction is opened on. BeginTx lives
	// on sqlplugin.DB and not on p.ExecutionStore, so a transaction spanning
	// many workflows is constructible only from beside the interface.
	db sqlplugin.DB

	shards  p.ShardStore
	factory *sql.Factory
}

var _ p.ExecutionStore = (*Store)(nil)

// New creates a database of its own and returns the cold store over it, plus
// the func that releases the store's handle on it.
//
// Two stores never see each other's rows, and the name is the whole of that
// guarantee: the plugin keys its connection pool by DSN. Releasing is no part
// of it — the pool holds the connection past the returned func — so isolation
// may never be built on tearing a store down.
//
// clusterName is what the shard store answers GetClusterName with; the store
// cannot derive it, and a server whose ClusterMetadata says otherwise gets a
// store that disagrees with it.
func New(clusterName string) (*Store, func(), error) {
	cfg := config.SQL{
		PluginName:   sqliteplugin.PluginName,
		DatabaseName: "memcold_" + uuid.NewString(),
		ConnectAttributes: map[string]string{
			"mode": "memory",
			// The plugin pins the pool to one connection, and under
			// cache=private that pin is the only thing keeping the rows
			// reachable: a second connection would silently open an empty
			// database of its own. Shared demotes the pin to an optimisation.
			"cache": "shared",
		},
	}

	logger := log.NewNoopLogger()
	factory := sql.NewFactory(cfg, resolver.NewNoopResolver(), clusterName, logger, metrics.NoopMetricsHandler)

	// For a memory-mode database the plugin runs the v3 schema setup itself on
	// the first connection, so asking for the handle here is what creates the
	// tables — and schema/sqlite.SetupSchema afterwards is a second CREATE
	// TABLE, not a no-op.
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
