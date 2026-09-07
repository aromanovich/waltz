# Putting a composition under Temporal's own functional suites

Nothing in this repository needs a patched Temporal. The library builds against
stock `go.temporal.io/server`, and every suite under `verify/` runs without one.

This directory holds one patch, and it is not about building waltz. It is about
the strongest evidence a composition over waltz can produce: upstream's own
functional suites — real frontend, history and matching, real workers, real
workflows — running against your datastore instead of SQL or Cassandra.

`0001-custom-persistence-test-base-factory.patch` is fifteen lines against
`tests/testcore/test_cluster.go`. Everything needed to boot such a cluster is
already exported and already threaded: `NewTestClusterFactoryWithCustomTestBaseFactory`
takes a `PersistenceTestBaseFactory`, and the cluster it builds hands that base's
`AbstractDataStoreFactory` to the server. What stands in the way is one call
site — every suite in `./tests` embeds `FunctionalTestBase`, whose `SetupSuite`
builds its cluster from the no-argument `NewTestClusterFactory()`, which switches
on `-persistenceType` / `-persistenceDriver` and panics on anything else. The
patch adds a package-level override read at that one site.

## Using it

Against a checkout of the same version this module requires:

```sh
git clone --branch v1.29.6 https://github.com/temporalio/temporal.git
git -C temporal apply /path/to/waltz/patches/temporal/0001-custom-persistence-test-base-factory.patch
```

Then, in a module with `replace go.temporal.io/server => ./temporal`, set the
override from `TestMain` before any suite runs, with a `PersistenceTestBaseFactory`
whose `AbstractDataStoreFactory` is your base decorated by waltz:

```go
testcore.CustomPersistenceTestBaseFactory = myFactory
```

`go build` of `go.temporal.io/server/tests/testcore` needs `-tags test_dep` —
upstream's own switch for `common/testing/testhooks`, patched or not.

## Its status

This is a patch worth deleting, and the way to delete it is a pull request: the
hook one layer down is already public, so upstreaming it is threading the last
inch of a seam that already exists. Until then it is a patch against a checkout
that nothing shipped compiles against.
