# waltz

The logic of a write-ahead log for the Temporal server's history shards. **You bring the storage —
both ends of it**: the log, [`wal.Log`](wal/wal.go#L148), and the database,
[`cold.Store`](cold/cold.go#L67). Five methods on the log and two on the database are nearly the
whole of what waltz asks you to implement.

Temporal's history service is write-heavy: one workflow moves through hundreds of state transitions,
and each is a database transaction that rewrites the same mutable-state rows and inserts task rows
that are often deleted moments later. The idea is to put a durable per-shard log in front of that
database, acknowledge a write as soon as the log holds it, and fold many acknowledged writes into
one later transaction. A hundred transitions become a hundred log appends plus one database
transaction instead of a hundred — whatever an append costs.

**waltz is not a persistence implementation, and that is the first thing to understand about it.**
It writes to no disk, opens no connection and speaks no wire protocol; it contains no line of code
that would. What it is, is everything between two interfaces you implement: the log an
acknowledgement lands in ([`wal.Log`](wal/wal.go#L148)) and the database a fold lands on
([`cold.Store`](cold/cold.go#L67)). Both are yours to write over whatever storage you run. What
waltz owns is the part that is genuinely hard — the window, the fold, the fencing, the replay, the
bound on unapplied work, and what each of them must do when a write, a process or a shard handover
fails.

The server does not know any of this. waltz ships as a persistence decorator: you compose it over
the plugin that owns your data and hand the result to `temporal.WithCustomDataStoreFactory`, the
same door a custom persistence backend already goes through. Temporal goes on believing it writes
to a database, reads from a database, and that what it read is true.

## Status: read this first

**This is a research materialisation, not a deployable system.** The logic is complete and tested;
the storage is not there.

Because the storage is yours, this repository has to supply something at both seams in order to run
at all — so it ships two implementations that live *in the test process*. They are why
`go test ./...` boots a real Temporal server and runs a real workflow on a fresh clone. They are
also why:

- **nothing here is durable.** Both live in one process's memory and die with it. No fsync, no
  network and no quorum has ever been in the path of anything in this tree.
- **no process has ever been killed.** Replay is exercised in process; a real handover between two
  owners on two machines is not.
- **no backend has been run under load**, and no number produced here is a performance claim.

Taking waltz to production means writing a `wal.Log` over real storage and a `cold.Store` over your
real database, and testing the parts this repository cannot reach.
[Chapter 15](docs/handbook/15-the-limits-of-the-evidence.md) is that boundary in full.

**Built against `go.temporal.io/server` v1.29.6 and Go 1.26.** The server version is not a soft
floor. A log entry mirrors that version's write-request structs field by field, hand-written rather
than reflected, so a field a newer server adds is a field this codec has nowhere to put — an
acknowledged write folded into the database with part of it missing. A guard in `mutation` makes
that a failing test at the bump rather than a lost column.

## The first rule

**Data once acknowledged is never lost.** Refusing a write is acceptable. Taking the server down is
acceptable. Losing a write that was acknowledged is not — and it is not made acceptable by the
storage underneath having been configured badly. A warning followed by a successful write is a lie
the caller acts on.

This outranks throughput and availability, and it is what the design is shaped by. An early
acknowledgement is easy; keeping it honest across a crash is the whole problem. It is why one drain
is one transaction with its watermark inside it, and why a drain whose outcome is unknown is
resolved by reading that watermark back rather than by re-applying.

[`DURABILITY.md`](DURABILITY.md) is the standing list of every known way that rule can break: what
is closed, with the mechanism and a test that fails without it; what is open, including the two
shipped implementations dying with the process; and what nobody has established, which is treated as
open. Read it before trusting a green run, and add to it before fixing anything it does not name.

## Try it

```sh
git clone https://github.com/aromanovich/waltz && cd waltz
make test    # go test ./... -count=1
make lint    # golangci-lint plus gopls's modernize, both pinned in the Makefile
```

No cluster, no container, no fixed port, no cgo, no build tag. A fresh clone runs everything there
is, including a four-service Temporal server booting over waltz and completing a workflow through
the SDK (`internal/verify/e2e`).

## The composition

```go
package main

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

func main() {
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
```

The lifecycle brackets the server's, and both ends matter. `Compose` opens nothing and reaches
nothing, so a policy whose memory budget does not add up stops a process that has connected to
nothing yet, rather than a node already serving. `Layer.Shutdown` runs last so that every window
still open has somewhere to drain.

Configuration is a `wal` section inside the custom datastore's own options: absent means
passthrough, malformed means a refusal to start rather than a node quietly running the other mode.
The operating numbers live in the server's dynamic config: nine settings, five of them re-read at
the decision that consults them and four read once, when the node builds its policy.
[Chapter 08](docs/handbook/08-configuration.md) is every key.

## The two seams

waltz sits between two things it does not own, and a deployment replaces both.

| | the contract | shipped here | what judges your implementation |
|---|---|---|---|
| the log | [`wal.Log`](wal/wal.go#L148) | `wal/memwal`, in process memory | `wal/waltest` — this repository's conformance suite: twenty-one cases, one call |
| the database | [`cold.Store`](cold/cold.go#L67) | `cold/memcold`, Temporal's own SQL persistence over in-process SQLite | Temporal's four exported persistence suites, which `memcold` runs unmodified |

`wal.Log` is an append-only, fenced, gap-free sequence of entries per shard — five methods, opaque
payloads, no Temporal type anywhere in it. Running the suite against your backend is one call:

```go
func TestMyBackendKeepsTheContract(t *testing.T) {
	waltest.RunContractSuite(t, myBackend())
}
```

`cold.Store` is two methods: `Apply`, which commits a folded window as one transaction, and
`Watermark`, which reads back the seqno that transaction carried. It is one interface rather than
the two halves it is made of because **one value has to answer both** — a watermark is only
meaningful about the transactions that wrote it, so a layer reading it from anywhere else trims a
log against a witness that never saw it.

What waltz asks for beyond those two is one read, and it is on the persistence plugin the layer
decorates rather than on `cold.Store`: a current-execution row with `last_write_version` beside it,
the column Temporal's own response type has nowhere to carry, and the one a create's condition is
decided on. A base store that cannot answer it is refused while the server is still starting, rather
than run in a reduced mode.

The two rows are not mirror images, and the asymmetry is worth knowing before you start. The log's
contract is waltz's own invention, so waltz owes it a suite and ships one. What a *database* owes a
Temporal server is Temporal's to state, and Temporal states it as four suites it exports — so
**nothing exported from here judges somebody else's `cold.Store`.** What is written down instead
is the four obligations an implementation carries, with `cold/memcold` as the worked example of all
four: [chapter 04](docs/handbook/04-contracts.md) has both.

## Documentation

[**`docs/handbook`**](docs/handbook) is the book, and it is where every question this page raises is
answered properly. Chapters 01–11 are the reference — the components, the contracts, the write and
read paths, the shard lifecycle, the configuration keys, the metrics, the runbooks, the suites.
Chapters 12–15 are the reasoning the reference states without arguing for it: what one write cost
before any of this existed, which designs were tried and refused, where each shipped default came
from, and what a green test run does not say.

Start at [chapter 01](docs/handbook/01-overview.md). Every identifier, default and count named in
those pages exists in the code, and every chapter ends with the files it draws from.

[`docs/adr`](docs/adr) holds the decisions somebody will otherwise try to reverse, and
[`CONTEXT.md`](CONTEXT.md) is the vocabulary they are written in.

## Licence

MIT — see [LICENSE](LICENSE). Parts of `cold/memcold` are derived from Temporal's own persistence
implementation, which is MIT-licensed; [NOTICE](NOTICE) reproduces that licence and says which files
are derived and how.
