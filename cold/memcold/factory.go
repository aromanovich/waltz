package memcold

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
)

// AbstractDataStoreFactory puts a [Store] behind the seam a real server reaches
// a non-plugin store through: name a custom datastore in
// Persistence.DataStores and hand this to temporal.WithCustomDataStoreFactory.
// Wrap it in the layer's own decorator
// (wrapper.NewAbstractDataStoreFactory) and the WAL sits between the history
// service and this database.
type AbstractDataStoreFactory struct {
	store *Store
}

var _ client.AbstractDataStoreFactory = (*AbstractDataStoreFactory)(nil)

// NewAbstractDataStoreFactory is the store as a server sees it. Every service
// of that server gets the same database, because there is only one and it was
// created when store was.
func NewAbstractDataStoreFactory(store *Store) *AbstractDataStoreFactory {
	return &AbstractDataStoreFactory{store: store}
}

// NewFactory ignores every argument. The database, its cluster name, its logger
// and its metrics handler were fixed by [New]; a server presenting different
// ones is a caller that composed a store for one cluster and started another,
// which this cannot repair and will not paper over by rebuilding underneath it.
func (f *AbstractDataStoreFactory) NewFactory(
	config.CustomDatastoreConfig,
	resolver.ServiceResolver,
	string,
	log.Logger,
	metrics.Handler,
) p.DataStoreFactory {
	return &dataStoreFactory{store: f.store}
}

// dataStoreFactory vends the two stores waltz is about from the [Store], and
// everything else — matching, metadata, cluster metadata, the queues, nexus
// endpoints — from the SQL factory unchanged. None of those is inside the
// layer, and none of them is this package's to have an opinion about.
type dataStoreFactory struct {
	store *Store
}

var _ p.DataStoreFactory = (*dataStoreFactory)(nil)

// Close does nothing. The database's lifetime is the func [New] returned, and a
// server builds one of these per service: closing here would let the first
// service to shut down take the database away from the rest.
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
