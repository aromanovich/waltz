# 3. The WAL layer runs in-process, not as a sidecar

Date: 2026-07-26

## Status

Accepted.

## Context

The original design placed the WAL layer
(writer, tail, fold, overlay, apply, trim) in a sidecar process colocated 1:1
with each history node, reached from a thin DataStore plugin over gRPC/unix
socket. But using a custom persistence store already requires building a
custom temporal-server main — so the process boundary was pure addition: an RPC
protocol to design and version, a second deploy unit, health-coupling between
node and sidecar, and an open research question on how history nodes survive
sidecar restarts. It also made one line of the draft impossible as written: a
sidecar cannot invoke the server's in-process queue-processor notification.

## Decision

The WAL layer is a library inside the custom temporal-server main, wrapping
ExecutionStore/ShardStore behind the standard DataStoreFactory extension point.
No sidecar, no RPC layer. The whole of how a server is built over it is

```go
temporal.WithCustomDataStoreFactory(layer.AbstractFactory(base))
```

where `base` is the plugin whose stores hold the cold data.

## Consequences

- Process death is an ordinary history-node failure: shards fail over, the new
  owner replays the WAL tail. The separate "node alive, persistence dead" mode
  disappears, and with it the question of how a node survives its sidecar.
- Shard lifecycle (acquire/close) is observed directly by the wrappers instead
  of being inferred from the persistence call stream; the fence-before-rangeID
  ordering on acquire is implemented trivially in the ShardStore wrapper.
- Queue-processor notification needs no work in steady state: the stock
  server already calls `engine.NotifyNewTasks` after each successful persistence
  write (`service/history/workflow/transaction_impl.go` at the required
  v1.29.6), so tasks living only in the tail are known to processors.
- The cost: tail memory now competes with the history node's caches in one
  process. The backpressure cap (invariant I10) times shards-per-node must be
  budgeted into node RAM explicitly.
- Memory isolation was the sidecar's main argument; it is deliberately traded
  away on the grounds that I10 bounds tail growth by construction — if the cap
  works there is nothing to isolate, and if it doesn't, the sidecar would die
  too, just with extra failure modes.

An organizational reason (a separate team owning and deploying the WAL layer on
its own cadence) would be grounds to revisit; no such reason exists today.
