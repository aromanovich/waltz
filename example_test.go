package waltz_test

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/temporal"

	"github.com/aromanovich/waltz"
	"github.com/aromanovich/waltz/cold/memcold"
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wal/memwal"
)

// README's "The composition" section carries this function's body as its
// sample, so an edit to either belongs in both. It has no `// Output:`
// comment and is therefore compiled and never run, which is the point: it
// starts a server, and what it is here to catch is a rename that leaves the
// README's code not compiling.
func Example() {
	// The cold store. memcold is the one shipped here — Temporal's own SQL
	// persistence over a database in this process — and a deployment puts its
	// own cold.Store here instead.
	store, release, err := memcold.New("active")
	if err != nil {
		panic(err)
	}
	defer release()

	layer, err := waltz.Compose(
		waltz.Backends{
			Log:  memwal.New(), // your wal.Log; memwal is the one shipped here
			Cold: store,        // your cold.Store
		},
		cycle.Fixed(cycle.Defaults()),
		waltz.DefaultTaskCategories(),
		log.NewCLILogger(),
		nil, // the server's metrics handler arrives later, through the factory
	)
	if err != nil {
		panic(err)
	}

	// base is the persistence factory that owns the cold data — memcold's here,
	// your plugin's otherwise.
	var base client.AbstractDataStoreFactory = memcold.NewAbstractDataStoreFactory(store)

	// The server's own configuration, read the way a stock server reads it. It
	// must name a custom datastore in Persistence.DataStores; that naming is the
	// whole of how the factory below enters its persistence graph.
	cfg, err := config.LoadConfig("development", "config", "")
	if err != nil {
		panic(err)
	}

	s, err := temporal.NewServer(
		temporal.WithConfig(cfg),
		temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base)),
	)
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	defer func() {
		_ = s.Stop()
		// After the server has stopped: the shutdown drain still needs a store
		// to write to. `defer release()` above runs after this one, which is the
		// order that leaves the drain a database.
		// A main that finds entries here logs them before it exits: they are in
		// the log for the next owner, and nothing replays them if this node comes
		// back without the wal section.
		if err := layer.Shutdown(context.Background(), 30*time.Second); err != nil {
			panic(err)
		}
	}()

	// Start returns once the services are up, so a main that did not wait here
	// would run both deferred shutdowns immediately — and the ordering they are
	// written for is the whole point of them.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}
