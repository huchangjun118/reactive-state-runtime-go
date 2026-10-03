# Reactive State Runtime

A dependency-graph runtime for Go that coordinates mutable state, derived
computations, and asynchronous side effects. The runtime is self-contained:
it uses only the Go standard library and does not require external services.

## Core model

- `State[T]` is a root value. State changes are the only input to the graph.
- `Derived[T]` is computed from states and other derived nodes. A derived node
  is recomputed only when an input changes.
- `Effect` is an asynchronous leaf side effect triggered by committed input
  changes.

Handles are created through the runtime:

```go
rt := reactive.NewRuntime(reactive.Config{
    MaxConcurrentEffects: 4,
    MaxEffectQueue:       256,
    QueuePolicy:          reactive.QueuePolicyReject,
    ErrorPolicy:          reactive.ErrorPolicyIsolate,
    EffectTimeout:        2 * time.Second,
})
defer rt.Close()

count := reactive.NewState(rt, 0)
doubled := reactive.NewDerived(rt, func(_ context.Context, values reactive.Values) (int, error) {
    return values[0].(int) * 2, nil
}, count)
reactive.NewEffect(rt, func(ctx context.Context, values reactive.Values) error {
    fmt.Println("doubled:", values[0].(int))
    return nil
}, doubled)

version, err := rt.Update(func(tx *reactive.Tx) error {
    count.Set(tx, 21)
    return nil
})
```

`Values` is a positional snapshot of the node's dependencies. It is owned by
the runtime and must not be retained after the callback returns.

## Batch transactions

`Runtime.Update` runs a transaction function once. All state changes are staged
and become visible only after every affected derived node has been recomputed.

- A transaction commits exactly one new version.
- If the transaction function returns an error, no staged state is applied.
- With `ErrorPolicyFailFast`, a derived computation error rolls back all state
  and derived results to the last committed version.
- With `ErrorPolicyIsolate`, valid state changes commit. Failed derived nodes
  keep their last good value and expose an error; dependent derived nodes fail
  with the same error. Unrelated branches continue normally.
- A later successful recompute clears a derived error and restores the node to
  a consistent state.

Reads through `Get` return the latest committed version. Readers never observe
a partially applied batch.

## Diamond dependencies and evaluation order

Affected nodes are reached through dependency edges. During one batch:

- Each affected derived node is evaluated at most once.
- Dependencies are evaluated before dependents.
- Diamond dependencies therefore see the same latest input values on every
  branch.
- Evaluation does not use map iteration order, random values, wall clocks, or
  traversal-dependent behavior. Node IDs and dependency edge insertion order
  determine graph and scheduling order.

## Asynchronous effects

Effects are scheduled only after a transaction commits successfully.

- Each effect has at most one in-flight execution.
- A run is identified by a monotonically increasing run ID.
- If a newer commit arrives while an effect is queued, the queued run is
  replaced.
- If a newer commit arrives while an effect is running, the older run is
  allowed to finish but its result and error are discarded. The latest input
  snapshot is scheduled next.
- `EffectTimeout` cancels the effect context. Effects should observe `ctx.Done`
  for cooperative cancellation.
- `Close` cancels all running effects and waits for them to finish.
- `MaxConcurrentEffects` bounds the number of simultaneously running effects.
- `MaxEffectQueue` bounds the number of queued runs.
- `QueuePolicyReject` drops a run when the queue is full and increments
  `Metrics.EffectDrops`.
- `QueuePolicyBlock` waits for queue capacity.
- Slow effects occupy only their own execution slot and do not prevent
  unrelated effects from running.

Effects receive the dependency snapshot committed for their triggering
version. The runtime never writes effect return values back to state; effects
should communicate through external systems or explicit transactions.

## Dynamic graph changes

Nodes and edges can be added or removed after startup:

- `AddDependency` adds a state or derived dependency to a derived node or
  effect. Cycles are rejected.
- `RemoveDependency` removes an edge and recomputes the affected part of the
  graph.
- `Remove` detaches a node. Removing a state with dependents is rejected so
  derived computations cannot silently lose required inputs.
- Removed effects are canceled and never scheduled again.
- Removed nodes are deleted from the runtime graph. The runtime retains no
  per-transaction history, so long-running processes do not accumulate graph
  or task history.

Graph changes take effect under the same runtime lock used by transactions.
Updates already being propagated use snapshots captured at scheduling time and
are not mutated by later graph changes.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `MaxConcurrentEffects` | `4` | Maximum number of effects running at the same time. |
| `MaxEffectQueue` | `1024` | Maximum number of queued effect runs. |
| `QueuePolicy` | `QueuePolicyReject` | Behavior when the queue is full: reject or block. |
| `ErrorPolicy` | `ErrorPolicyIsolate` | Transaction behavior for derived errors: isolate or fail fast. |
| `EffectTimeout` | `0` | Per-run effect timeout. Zero disables timeouts. |

## Observability

`Observer` receives best-effort callbacks for:

- committed and failed transactions,
- derived computation errors,
- effect errors,
- effects skipped because a dependency failed,
- queue-full drops.

`Metrics` exposes atomic counters for transactions, transaction failures,
derived errors, effect runs, effect errors, queue drops, and skipped effects.

## Concurrency

Concurrent `Update` calls are serialized by the runtime. Each successful call
produces one committed version and one deterministic graph evaluation pass.
Concurrent commits cannot produce lost updates or partially published state.
Final values depend on the order in which the serialized transactions run; the
runtime guarantees that order is atomic and that all resulting versions remain
consistent.
