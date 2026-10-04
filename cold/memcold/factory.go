package memcold

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
)

// AbstractDataStoreFactory exposes a [Store] to a server as a custom
// datastore: name one in Persistence.DataStores and pass this to
// temporal.WithCustomDataStoreFactory. Wrap it in
// wrapper.NewAbstractDataStoreFactory to put the WAL in front of it.
type AbstractDataStoreFactory struct {
	store *Store
}

var _ client.AbstractDataStoreFactory = (*AbstractDataStoreFactory)(nil)

// NewAbstractDataStoreFactory returns the store as a server sees it. Every
// service gets the same database, created by [New].
func NewAbstractDataStoreFactory(store *Store) *AbstractDataStoreFactory {
	return &AbstractDataStoreFactory{store: store}
}

// NewFactory ignores every argument: the database, cluster name, logger and
// metrics handler were fixed by [New], and it does not rebuild for different
// ones.
func (f *AbstractDataStoreFactory) NewFactory(
	config.CustomDatastoreConfig,
	resolver.ServiceResolver,
	string,
	log.Logger,
	metrics.Handler,
) p.DataStoreFactory {
	return &dataStoreFactory{store: f.store}
}

// dataStoreFactory vends the execution and shard stores from the [Store], and
// every other store (matching, metadata, queues, nexus endpoints) from the SQL
// factory unchanged.
type dataStoreFactory struct {
	store *Store
}

var _ p.DataStoreFactory = (*dataStoreFactory)(nil)

// Close does nothing: every service's factory wraps the one [Store], so closing
// would shut the sql.Factory under services still using it.
func (f *dataStoreFactory) Close() {}

func (f *dataStoreFactory) NewExecutionStore() (p.ExecutionStore, error) {
	return f.store, nil
}

func (f *dataStoreFactory) NewShardStore() (p.ShardStore, error) {
	return f.store.shards, nil
}

func (f *dataStoreFactory) NewTaskStore() (p.TaskStore, error) {
	return f.store.factory.NewTaskStore()
}

func (f *dataStoreFactory) NewFairTaskStore() (p.TaskStore, error) {
	return f.store.factory.NewFairTaskStore()
}

func (f *dataStoreFactory) NewMetadataStore() (p.MetadataStore, error) {
	return f.store.factory.NewMetadataStore()
}

func (f *dataStoreFactory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return f.store.factory.NewClusterMetadataStore()
}

func (f *dataStoreFactory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return f.store.factory.NewQueue(queueType)
}

func (f *dataStoreFactory) NewQueueV2() (p.QueueV2, error) {
	return f.store.factory.NewQueueV2()
}

func (f *dataStoreFactory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return f.store.factory.NewNexusEndpointStore()
}
