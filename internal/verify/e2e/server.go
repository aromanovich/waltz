// Package e2e is the whole of waltz under a running Temporal server: the four
// services in this process, over the cold store this repository ships, with a
// real workflow driven through the frontend by the SDK.
//
// It is the one claim no suite below it can make. The conformance suites say
// the store answers what a store owes; the acceptance says the layer folds what
// it acked and lands it. Neither says a *server* composed over both comes up,
// hands its shards to the layer, and completes a workflow — and a layer that
// quietly fell out of the path is green in every one of them.
//
// Nothing here needs a cluster, a container, a fixed port or cgo. The database
// is cold/memcold's, in memory; the ports are the OS's; the log is wal/memwal.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	sqliteplugin "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/freeport"
	"go.temporal.io/server/temporal"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ClusterName is what the config names this cluster and what the cold store
// must have been built with: the shard store answers GetClusterName from the
// store's own value, and a server whose ClusterMetadata says something else
// reads shards it did not write.
const ClusterName = "active"

// Shards is the cluster's shard count. Small so that one workflow's writes land
// in one shard's window rather than being spread until no window folds
// anything, and above one so the layer is asked for a shard by hash rather than
// by there being only one.
const Shards = 4

// Cluster is a Temporal server running in this process, plus the address its
// frontend answers on.
type Cluster struct {
	server   temporal.Server
	hostPort string
	silenced *atomic.Bool
}

// Start builds the server's configuration around base and starts it. base is
// reached because Persistence names a custom datastore and nothing else: that
// naming is the whole of how temporal.WithCustomDataStoreFactory's value enters
// a server's persistence graph, so a run that reaches base at all has proved
// the production door works.
//
// Visibility is the one store that does not come from base. It is SQLite in
// memory, its own database: the custom-datastore seam vends persistence stores,
// and a visibility store comes from a different factory the server takes
// separately. Nothing waltz does is inside visibility.
//
// The server is started, which does not mean it serves; [Cluster.Client] is
// where a caller waits for that.
//
// The server's logger is the harness's own and not the caller's, because
// [Cluster.Stop] silences it (see [gate]) and a caller's layer keeps logging
// after that: the shutdown drain runs once the server is down.
func Start(base persistenceclient.AbstractDataStoreFactory) (*Cluster, error) {
	frontendPort := freeport.MustGetFreePort()
	hostPort := fmt.Sprintf("127.0.0.1:%d", frontendPort)

	silence := &atomic.Bool{}
	logger := log.NewZapLogger(log.BuildZapLogger(log.Config{Stdout: true, Level: "error"}).WithOptions(
		zap.WrapCore(func(c zapcore.Core) zapcore.Core { return gate{c, silence} }),
		// One line per complaint. Upstream attaches a stack trace to every
		// error, and the interesting ones here are refusals a service already
		// names in full — a converging ring, a caller ahead of a cache — where
		// forty lines of frames per refusal are what buries the run's own
		// output.
		zap.AddStacktrace(zapcore.FatalLevel),
	))

	cfg := &config.Config{}
	cfg.Global.Membership = config.Membership{
		MaxJoinDuration:  30 * time.Second,
		BroadcastAddress: "127.0.0.1",
	}
	cfg.Persistence = config.Persistence{
		DefaultStore:     "waltz",
		VisibilityStore:  "visibility",
		NumHistoryShards: Shards,
		DataStores: map[string]config.DataStore{
			"waltz": {CustomDataStoreConfig: &config.CustomDatastoreConfig{Name: "waltz"}},
			// mode=memory makes the plugin create the schema on the first
			// connection, and the plugin's connection pool is keyed by DSN, so
			// the four services share this one database rather than opening
			// four empty ones.
			"visibility": {SQL: &config.SQL{
				PluginName:        sqliteplugin.PluginName,
				DatabaseName:      "waltz_e2e_visibility_" + uuid.NewString(),
				ConnectAttributes: map[string]string{"mode": "memory", "cache": "shared"},
			}},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
	cfg.ClusterMetadata = &cluster.Config{
		EnableGlobalNamespace:    false,
		FailoverVersionIncrement: 10,
		MasterClusterName:        ClusterName,
		CurrentClusterName:       ClusterName,
		ClusterInformation: map[string]cluster.ClusterInformation{
			ClusterName: {
				Enabled:                true,
				InitialFailoverVersion: 1,
				RPCAddress:             hostPort,
			},
		},
	}
	cfg.DCRedirectionPolicy = config.DCRedirectionPolicy{Policy: "noop"}
	cfg.Services = map[string]config.Service{
		"frontend": service(frontendPort),
		"history":  service(freeport.MustGetFreePort()),
		"matching": service(freeport.MustGetFreePort()),
		"worker":   service(freeport.MustGetFreePort()),
	}
	cfg.Archival = config.Archival{
		History:    config.HistoryArchival{State: "disabled"},
		Visibility: config.VisibilityArchival{State: "disabled"},
	}
	cfg.NamespaceDefaults = config.NamespaceDefaults{
		Archival: config.ArchivalNamespaceDefaults{
			History:    config.HistoryArchivalNamespaceDefaults{State: "disabled"},
			Visibility: config.VisibilityArchivalNamespaceDefaults{State: "disabled"},
		},
	}
	// Without this the server panics on a client built any other way; the
	// address is the frontend's own.
	cfg.PublicClient = config.PublicClient{HostPort: hostPort}

	srv, err := temporal.NewServer(
		temporal.WithConfig(cfg),
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithLogger(logger),
		temporal.WithCustomDataStoreFactory(base),
		temporal.WithDynamicConfigClient(dynamicconfig.StaticClient{
			// A namespace registered after the server is up reaches the
			// history and matching services through this cache, and its
			// default refresh is the largest fixed cost this test would
			// otherwise pay.
			dynamicconfig.NamespaceCacheRefreshInterval.Key(): []dynamicconfig.ConstrainedValue{
				{Value: time.Second},
			},
			dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key(): []dynamicconfig.ConstrainedValue{
				{Value: true},
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("e2e: building the server: %w", err)
	}
	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("e2e: starting the server: %w", err)
	}
	return &Cluster{server: srv, hostPort: hostPort, silenced: silence}, nil
}

// gate is the server's log with a switch: what four services say while they are
// being torn down is not what a run is read by, and here it is a burst of
// membership complaints per arm, each with a stack trace. Installed at the
// zapcore rather than around log.Logger because components take child loggers
// (log.With), and a wrapper the children do not carry filters nothing they say.
type gate struct {
	zapcore.Core
	silenced *atomic.Bool
}

func (g gate) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if g.silenced.Load() {
		return ce
	}
	return g.Core.Check(e, ce)
}

func (g gate) With(fields []zapcore.Field) zapcore.Core {
	return gate{g.Core.With(fields), g.silenced}
}

// service is one service's RPC configuration: a port the OS chose, bound to
// loopback, and a membership port of its own.
func service(grpcPort int) config.Service {
	svc := config.Service{}
	svc.RPC.GRPCPort = grpcPort
	svc.RPC.MembershipPort = freeport.MustGetFreePort()
	svc.RPC.BindOnLocalHost = true
	return svc
}

// HostPort is the frontend's address.
func (c *Cluster) HostPort() string { return c.hostPort }

// Stop stops the server. It must return before the layer is shut down: the
// shutdown drain writes what the services still hold.
func (c *Cluster) Stop() error {
	c.silenced.Store(true)
	return c.server.Stop()
}

// Client waits for the frontend to serve and then dials it. A listening socket
// says only that gRPC is up: the frontend accepts connections while its own
// dependencies are still starting and refuses every call until they are.
//
// The wait is gRPC's health service rather than any client call, and that is
// what keeps a run readable: every workflowservice method an unhealthy frontend
// refuses is logged as a service failure with a stack trace, so a probe of ours
// would print as the server's own trouble. Dialing the SDK client eagerly asks
// GetSystemInfo, which is one of them.
func (c *Cluster) Client(ctx context.Context, namespace string) (client.Client, error) {
	if err := c.awaitServing(ctx); err != nil {
		return nil, err
	}
	conn, err := client.DialContext(ctx, client.Options{
		HostPort:  c.hostPort,
		Namespace: namespace,
		Logger:    noopSDKLogger{},
	})
	if err != nil {
		return nil, fmt.Errorf("e2e: dialling the frontend: %w", err)
	}
	return conn, nil
}

// workflowService is the name the frontend publishes its serving status under.
const workflowService = "temporal.api.workflowservice.v1.WorkflowService"

// awaitServing blocks until the frontend reports SERVING.
func (c *Cluster) awaitServing(ctx context.Context) error {
	conn, err := grpc.NewClient(c.hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("e2e: connecting to the frontend: %w", err)
	}
	defer func() { _ = conn.Close() }()

	health := healthpb.NewHealthClient(conn)
	var last error
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("e2e: the frontend never served: %w", errors.Join(ctx.Err(), last))
		}
		resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: workflowService})
		switch {
		case err != nil:
			last = err
		case resp.GetStatus() != healthpb.HealthCheckResponse_SERVING:
			last = fmt.Errorf("the frontend reports %s", resp.GetStatus())
		default:
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// noopSDKLogger silences the SDK's own logger. The server's log level is what a
// run is read by; the SDK logs a warning per retried poll while the namespace
// propagates, which is a fact of the test rather than of the code under it.
type noopSDKLogger struct{}

func (noopSDKLogger) Debug(string, ...any) {}
func (noopSDKLogger) Info(string, ...any)  {}
func (noopSDKLogger) Warn(string, ...any)  {}
func (noopSDKLogger) Error(string, ...any) {}
