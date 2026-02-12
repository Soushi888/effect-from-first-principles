# Chapter 7: Concurrency and Fibers

> **Building on:** This chapter uses `Effect.gen`, typed errors (`catchTag`), `acquireRelease`, `Scope`, `Context.Tag`, and `Layer` from Chapters 3–6. You should understand how `Effect<A, E, R>` tracks dependencies and how Layers provide them.

## The Concurrency Gap

Distributed applications are inherently concurrent. A service handles multiple requests simultaneously. A replication protocol fans out writes to peer nodes in parallel. A health monitor pings every node in the cluster at the same time. Concurrency isn't optional — it's the baseline.

JavaScript gives you `Promise.all` for parallelism. It works for the simple case:

```typescript
const [users, orders, inventory] = await Promise.all([
  fetchUsers(),
  fetchOrders(),
  fetchInventory(),
]);
```

But the simple case is rarely the real case. What happens when:

- `fetchOrders()` fails — are the other two cancelled, or do they run to completion, wasting resources?
- You want at most 5 concurrent requests to avoid overwhelming a downstream service?
- A parent request is cancelled — do its child requests stop?
- Two concurrent tasks need to communicate through a shared queue?
- You need to race two strategies and use whichever finishes first?

`Promise.all` answers none of these. Promises are eager — once created, they run. There's no cancellation, no bounded concurrency, no parent-child lifecycle management. You bolt these on with `AbortController`, manual semaphores, and careful bookkeeping — all of which are error-prone and don't compose.

Effect's fiber system solves this from first principles.

## Fibers: Lightweight Concurrent Units

A **fiber** is Effect's unit of concurrency. Think of it as a lightweight, cooperative thread managed by the Effect runtime — not an OS thread. You can create thousands of fibers without concern. Each fiber runs an Effect and can be joined (wait for its result), interrupted (cancel it), or left to run in the background.

### Forking a Fiber

```typescript
import { Effect, Fiber } from "effect";

const backgroundTask = Effect.gen(function* () {
  yield* Effect.log("Background work starting");
  yield* Effect.sleep("2 seconds");
  yield* Effect.log("Background work complete");
  return 42;
});

const program = Effect.gen(function* () {
  // Fork spawns a fiber — the task starts running concurrently
  const fiber = yield* Effect.fork(backgroundTask);

  // Meanwhile, do other work
  yield* Effect.log("Main fiber doing other work");
  yield* Effect.sleep("1 second");

  // Join waits for the fiber to complete and retrieves its result
  const result = yield* Fiber.join(fiber);
  yield* Effect.log(`Background result: ${result}`);
});
```

`Effect.fork` returns immediately with a `Fiber<A, E>` handle. The forked Effect runs concurrently. `Fiber.join` waits for the fiber to finish and gives you its result — or propagates its error if it failed.

This is fundamentally different from Promises. A Promise starts executing the moment it's created. An Effect describes work; `Effect.fork` explicitly starts it in a new fiber. You control when concurrency begins.

### Interrupting a Fiber

Cancellation is first-class. Any fiber can be interrupted at any time:

```typescript
const longRunning = Effect.gen(function* () {
  yield* Effect.log("Starting long task");
  yield* Effect.sleep("10 seconds"); // Interruptible point
  yield* Effect.log("This may never print");
  return "done";
});

const program = Effect.gen(function* () {
  const fiber = yield* Effect.fork(longRunning);

  yield* Effect.sleep("1 second");
  yield* Effect.log("Patience exhausted, interrupting");

  yield* Fiber.interrupt(fiber);
  // The fiber is stopped. Its finalizers run. Resources are cleaned up.

  yield* Effect.log("Continuing after interruption");
});
```

When a fiber is interrupted:

1. It stops executing at the next interruptible point (yield boundaries, sleep, I/O)
2. All its `Scope` finalizers run — resources are cleaned up
3. All its child fibers are interrupted too

That third point is crucial. It's **structured concurrency**.

## Structured Concurrency

In unstructured concurrency (Promises, raw threads), a spawned task has no relationship to its parent. If the parent dies, the child keeps running — an orphan. If the child fails, the parent might never know.

Effect enforces structured concurrency: **every fiber has a parent, and child fibers cannot outlive their parent.** When a parent fiber completes (success, failure, or interruption), all its children are interrupted automatically.

```typescript
const parent = Effect.gen(function* () {
  // Fork three children
  yield* Effect.fork(monitorPeerA);
  yield* Effect.fork(monitorPeerB);
  yield* Effect.fork(monitorPeerC);

  // If the parent is interrupted or fails, all three monitors stop.
  // No orphan fibers. No resource leaks. No background work continuing
  // after the parent has given up.

  yield* runMainLoop();
});
```

This is the same principle as `Scope` from Chapter 5 — structured lifetimes. A resource can't outlive its scope. A fiber can't outlive its parent. Both guarantee cleanup. Both prevent leaks.

### Daemon Fibers: Opting Out

Occasionally you need a fiber that outlives its parent — a background logger, a metrics reporter. `Effect.forkDaemon` detaches the fiber from the parent scope:

```typescript
const program = Effect.gen(function* () {
  // This fiber is tied to the global scope, not the parent
  yield* Effect.forkDaemon(metricsReporter);

  // Parent can finish without interrupting the reporter
  yield* handleRequest();
});
```

Use this sparingly. Daemon fibers bypass structured concurrency and require manual lifecycle management. In most cases, attach long-lived fibers to a `Scope` that matches their intended lifetime rather than using daemon fibers.

## Parallel Execution with Effect.all

We saw `Effect.all` in Chapter 3 for combining effects sequentially. Add a concurrency option and it runs them in parallel:

```typescript
// Sequential (default)
const sequential = Effect.all([
  pingNode("a"),
  pingNode("b"),
  pingNode("c"),
]);

// Fully parallel
const parallel = Effect.all(
  [pingNode("a"), pingNode("b"), pingNode("c")],
  { concurrency: "unbounded" }
);

// Bounded concurrency — at most 5 at a time
const bounded = Effect.all(
  nodeIds.map((id) => pingNode(id)),
  { concurrency: 5 }
);
```

Bounded concurrency is essential for distributed systems. If you have 200 peer nodes, you don't want 200 simultaneous connections — that overwhelms the network and the downstream services. `{ concurrency: 5 }` runs at most 5 at a time, queuing the rest.

### Error Behavior in Parallel

By default, `Effect.all` with concurrency **fails fast**: if any effect fails, the others are interrupted and the error propagates. This is usually what you want — why keep pinging nodes if the first one revealed a network partition?

For cases where you want all results regardless of individual failures, use `Effect.allSettled`:

```typescript
// All results, even failures
const results = yield* Effect.allSettled([
  pingNode("a"),
  pingNode("b"),
  pingNode("c"),
]);
// Type: [Exit<PingResult, PingError>, Exit<PingResult, PingError>, ...]
// Each element is either a Success or Failure
```

Or combine with `{ mode: "validate" }` to accumulate errors (the applicative pattern from Chapters 2 and 4):

```typescript
const validated = Effect.all(
  [validateField1, validateField2, validateField3],
  { concurrency: "unbounded", mode: "validate" }
);
// Runs all in parallel, collects all errors
```

## Racing Effects

Sometimes you want the *fastest* result, not all results. Query three replicas and use whichever responds first:

```typescript
// Race — first to succeed wins, others are interrupted
const fastest = Effect.race(
  queryReplica("replica-a"),
  queryReplica("replica-b")
);

// raceAll — race many
const fastestOfMany = Effect.raceAll([
  queryReplica("replica-a"),
  queryReplica("replica-b"),
  queryReplica("replica-c"),
]);
```

When one effect succeeds, the others are interrupted immediately — no wasted work. If all fail, the error from the last one propagates.

### Timeout

A timeout is a race against the clock:

```typescript
const withTimeout = queryReplica("replica-a").pipe(
  Effect.timeout("5 seconds")
);
// Type: Effect<Option<Result>, QueryError>
// None if timed out, Some(result) if completed in time

// Or fail with a specific error on timeout
const withTimeoutError = queryReplica("replica-a").pipe(
  Effect.timeoutFail({
    duration: "5 seconds",
    onTimeout: () => new TimeoutError("replica-a", 5000),
  })
);
// Type: Effect<Result, QueryError | TimeoutError>
```

Timeouts are essential in distributed systems — you can't wait forever for a node that might be down. Effect makes them composable: you can timeout individual operations, entire pipelines, or combine timeouts with retries (Chapter 9).

## Concurrent Communication Primitives

Fibers need to communicate. Effect provides three purely functional primitives: `Ref`, `Deferred`, and `Queue`.

### Ref — Shared Mutable State, Safely

`Ref<A>` is a concurrent-safe mutable reference. It's the purely functional version of a variable — all access goes through Effects, guaranteeing that concurrent reads and writes are safe:

```typescript
import { Effect, Ref } from "effect";

const counter = Effect.gen(function* () {
  const ref = yield* Ref.make(0);

  // Fork 100 fibers that each increment the counter
  yield* Effect.all(
    Array.from({ length: 100 }, () =>
      Ref.update(ref, (n) => n + 1)
    ),
    { concurrency: "unbounded" }
  );

  const final = yield* Ref.get(ref);
  yield* Effect.log(`Final count: ${final}`); // Always 100
});
```

`Ref.update` is atomic — no lost updates even under high concurrency. Compare this to a regular `let counter = 0` with `counter++` in async callbacks, which can lose increments.

The operations mirror what we've seen throughout the series:

```typescript
Ref.make(initial)              // Create a Ref
Ref.get(ref)                   // Read the current value
Ref.set(ref, value)            // Replace the value
Ref.update(ref, f)             // Apply a function atomically
Ref.modify(ref, f)             // Update and return a derived value
```

If you've used `State` in functional programming, `Ref` is its concurrent counterpart. Same `get`/`set`/`update` interface, safe for multiple fibers.

### Deferred — A One-Time Signal

`Deferred<A, E>` is a single-assignment variable. It starts empty and can be completed exactly once — with a value or an error. Any fiber waiting on it blocks until it's completed:

```typescript
import { Effect, Deferred } from "effect";

const handshakeProtocol = Effect.gen(function* () {
  const ready = yield* Deferred.make<void, HandshakeError>();

  // Fiber 1: perform setup, then signal readiness
  yield* Effect.fork(
    Effect.gen(function* () {
      yield* performSetup();
      yield* Deferred.succeed(ready, undefined);
    })
  );

  // Fiber 2: wait for the signal, then proceed
  yield* Effect.fork(
    Effect.gen(function* () {
      yield* Deferred.await(ready); // Blocks until completed
      yield* startProcessing();
    })
  );
});
```

`Deferred` is the building block for coordination patterns: barriers (wait for N fibers to reach a point), handshakes (signal between two fibers), and one-shot event notification.

### Queue — Concurrent Producer-Consumer

`Queue<A>` is a concurrent, backpressure-aware channel between fibers:

```typescript
import { Effect, Queue } from "effect";

const producerConsumer = Effect.gen(function* () {
  // Bounded queue — producers block when full (backpressure)
  const queue = yield* Queue.bounded<PeerMessage>(100);

  // Producer: receive messages from the network and enqueue
  const producer = Effect.gen(function* () {
    yield* Effect.forever(
      Effect.gen(function* () {
        const msg = yield* receiveFromNetwork();
        yield* Queue.offer(queue, msg);
      })
    );
  });

  // Consumer: dequeue and process
  const consumer = Effect.gen(function* () {
    yield* Effect.forever(
      Effect.gen(function* () {
        const msg = yield* Queue.take(queue);
        yield* processMessage(msg);
      })
    );
  });

  // Run both concurrently
  yield* Effect.all([producer, consumer], { concurrency: 2 });
});
```

Queue variants control backpressure behavior:

| Constructor | When full | When empty |
|---|---|---|
| `Queue.bounded(n)` | `offer` blocks | `take` blocks |
| `Queue.unbounded()` | Never blocks | `take` blocks |
| `Queue.dropping(n)` | New items dropped | `take` blocks |
| `Queue.sliding(n)` | Oldest items dropped | `take` blocks |

For distributed systems, `bounded` is almost always what you want. It prevents a fast producer from overwhelming a slow consumer — backpressure propagates naturally.

## A Realistic Example: Parallel Cluster Health Check

Let's combine these primitives into a realistic system. We're building a cluster health monitor that:

1. Pings all nodes in parallel with bounded concurrency
2. Collects results into a shared state
3. Reports a summary

```typescript
import { Effect, Ref, Duration } from "effect";

interface HealthStatus {
  readonly nodeId: string;
  readonly healthy: boolean;
  readonly latencyMs: number;
}

interface ClusterHealth {
  readonly statuses: HealthStatus[];
  readonly checkedAt: Date;
}

class HealthCheckError {
  readonly _tag = "HealthCheckError" as const;
  constructor(readonly nodeId: string, readonly cause: string) {}
}

const pingNode = (nodeId: string): Effect.Effect<HealthStatus, HealthCheckError> =>
  Effect.gen(function* () {
    const start = yield* Effect.sync(() => Date.now());
    yield* Effect.tryPromise({
      try: () => fetch(`http://${nodeId}:8080/health`),
      catch: (e) => new HealthCheckError(nodeId, String(e)),
    }).pipe(
      Effect.timeoutFail({
        duration: "3 seconds",
        onTimeout: () => new HealthCheckError(nodeId, "timeout"),
      })
    );
    const latency = yield* Effect.sync(() => Date.now() - start);
    return { nodeId, healthy: true, latencyMs: latency };
  });

const checkCluster = (
  nodeIds: string[]
): Effect.Effect<ClusterHealth> =>
  Effect.gen(function* () {
    const results = yield* Effect.forEach(
      nodeIds,
      (nodeId) =>
        pingNode(nodeId).pipe(
          // Don't fail the whole check if one node is down
          Effect.catchAll((error) =>
            Effect.succeed({
              nodeId: error.nodeId,
              healthy: false,
              latencyMs: -1,
            })
          )
        ),
      { concurrency: 10 } // At most 10 concurrent pings
    );

    return {
      statuses: results,
      checkedAt: new Date(),
    };
  });

// Run it periodically
const healthMonitor = (nodeIds: string[]) =>
  Effect.gen(function* () {
    const latestHealth = yield* Ref.make<ClusterHealth>({
      statuses: [],
      checkedAt: new Date(),
    });

    // Periodic checker
    yield* Effect.fork(
      checkCluster(nodeIds).pipe(
        Effect.tap((health) => Ref.set(latestHealth, health)),
        Effect.tap((health) => {
          const down = health.statuses.filter((s) => !s.healthy);
          return down.length > 0
            ? Effect.logWarning(`${down.length} nodes unhealthy: ${down.map((s) => s.nodeId).join(", ")}`)
            : Effect.log("All nodes healthy");
        }),
        Effect.repeat({ schedule: { type: "spaced", millis: 10_000 } }),
      )
    );

    // Expose current health for queries
    return {
      getHealth: Ref.get(latestHealth),
    };
  });
```

This example combines several patterns:

- `Effect.forEach` with `{ concurrency: 10 }` for bounded parallel pings
- `Effect.catchAll` per node so one failure doesn't abort the whole check
- `Effect.timeoutFail` per ping so one slow node doesn't stall everything
- `Ref` for sharing the latest health snapshot between fibers
- `Effect.fork` with `Effect.repeat` for periodic background work
- Structured concurrency — if the parent program is interrupted, the health monitor stops

## Exercises

### Exercise 7.1: Fork and Join

Fork two concurrent tasks and combine their results:

```typescript
const taskA: Effect.Effect<string> = Effect.gen(function* () {
  yield* Effect.sleep("1 second");
  return "result-a";
});

const taskB: Effect.Effect<string> = Effect.gen(function* () {
  yield* Effect.sleep("2 seconds");
  return "result-b";
});

// Fork both, join both, return combined result
// Total time should be ~2 seconds (parallel), not ~3 seconds (sequential)
const program: Effect.Effect<[string, string]> = Effect.gen(function* () {
  // todo()
});
```

### Exercise 7.2: Timeout and Fallback

Build a function that queries a primary node with a 2-second timeout. If it times out, query a fallback node. If that also times out, return a cached default.

```typescript
class QueryTimeoutError {
  readonly _tag = "QueryTimeoutError" as const;
  constructor(readonly nodeId: string) {}
}

const queryNode = (
  nodeId: string
): Effect.Effect<string, QueryTimeoutError> => {
  // todo() — simulate with Effect.sleep + Effect.succeed
  // Make "primary" take 5 seconds (will timeout)
  // Make "fallback" take 1 second (will succeed)
};

const resilientQuery = (key: string): Effect.Effect<string> => {
  // todo()
  // 1. Query "primary" with 2-second timeout
  // 2. On timeout, query "fallback" with 2-second timeout
  // 3. On timeout, return "cached-default"
};
```

### Exercise 7.3: Bounded Parallel Execution

Given a list of 50 URLs, fetch them all with at most 5 concurrent requests. Collect successes and failures separately.

```typescript
const urls: string[] = Array.from({ length: 50 }, (_, i) => `https://api.example.com/data/${i}`);

interface FetchResult {
  readonly url: string;
  readonly status: "success" | "failure";
  readonly data?: string;
  readonly error?: string;
}

const fetchAllBounded = (
  urls: string[],
  maxConcurrency: number
): Effect.Effect<FetchResult[]> => {
  // todo()
  // Hint: Effect.forEach with concurrency option
  // Catch errors per-URL so one failure doesn't cancel the rest
};
```

### Exercise 7.4: Producer-Consumer with Queue

Build a system where a producer generates numbered messages and N consumers process them. Use a bounded queue for backpressure.

```typescript
const producerConsumerSystem = (
  messageCount: number,
  consumerCount: number,
  queueCapacity: number
): Effect.Effect<void> => {
  // todo()
  // 1. Create a bounded queue
  // 2. Producer: offer messages 1..messageCount, then offer a "done" sentinel
  // 3. Consumers: take and log each message until they see "done"
  // 4. Run producer + all consumers concurrently
  //
  // Hint: use Queue.offer and Queue.take
  // Hint: consumers can check for a special "shutdown" message
};
```

### Exercise 7.5: Racing Replicas

Query three replicas simultaneously and return the first successful result. If all three fail, return a combined error.

```typescript
class ReplicaError {
  readonly _tag = "ReplicaError" as const;
  constructor(readonly replicaId: string, readonly cause: string) {}
}

const queryReplica = (
  replicaId: string,
  key: string
): Effect.Effect<string, ReplicaError> => {
  // todo() — simulate with random delays and occasional failures
};

const queryWithRedundancy = (
  key: string,
  replicas: string[]
): Effect.Effect<string, ReplicaError[]> => {
  // todo()
  // Race all replicas. First success wins, others are interrupted.
  // If all fail, collect all errors.
  //
  // Hint: Effect.raceAll for the racing part
  // But raceAll propagates the last error, not all errors.
  // For collecting all errors, consider a different approach:
  //   fork all, collect results, find first success or all failures
};
```

### Exercise 7.6: Concurrent State with Ref

Build a rate limiter using `Ref`. It should allow at most N requests per time window.

```typescript
interface RateLimiter {
  readonly acquire: Effect.Effect<void, RateLimitExceeded>;
}

class RateLimitExceeded {
  readonly _tag = "RateLimitExceeded" as const;
  constructor(readonly retryAfterMs: number) {}
}

const makeRateLimiter = (
  maxRequests: number,
  windowMs: number
): Effect.Effect<RateLimiter> => {
  // todo()
  // Use Ref to track timestamps of recent requests
  // acquire: check if under limit, add timestamp if so, fail if not
  //
  // Hint: store an array of timestamps in a Ref
  // On each acquire, filter out expired timestamps, then check count
};
```

### Exercise 7.7 (Challenge): Scatter-Gather with Quorum

Implement a quorum read: send a query to N replicas, wait for a majority (N/2 + 1) to respond, return the most recent value. Interrupt the remaining replicas once quorum is reached.

```typescript
interface VersionedValue {
  readonly value: string;
  readonly version: number;
}

const quorumRead = (
  key: string,
  replicas: string[]
): Effect.Effect<VersionedValue, QuorumError> => {
  // todo()
  // 1. Fork a query to each replica
  // 2. Collect results into a Ref as they arrive
  // 3. When majority have responded, pick the highest version
  // 4. Interrupt remaining fibers
  //
  // Hint: use Deferred to signal when quorum is reached
  // Hint: each forked fiber updates the Ref and checks if quorum is met
  // Hint: when quorum is met, Deferred.succeed signals the main fiber
};
```

*This exercise combines fibers, Ref, Deferred, and structured concurrency. It's a real distributed systems pattern — the quorum read from Dynamo-style databases.*

## Summary

Effect's fiber system provides structured concurrency: every fiber has a parent, children can't outlive parents, and interruption propagates through the fiber tree. This prevents the orphan processes, resource leaks, and dangling callbacks that plague Promise-based concurrent code.

The concurrency primitives — `Ref`, `Deferred`, `Queue` — are all purely functional. They're Effects that describe operations, composed with the same `map`/`flatMap`/`gen` toolkit. A `Ref` is a concurrent-safe state cell. A `Deferred` is a one-shot signal. A `Queue` is a backpressure-aware channel.

| Need | Promise-based | Effect |
|---|---|---|
| Run in parallel | `Promise.all` (no cancellation) | `Effect.all` + concurrency option |
| Bounded concurrency | Manual semaphore | `{ concurrency: 5 }` |
| Cancel on first success | Not built-in | `Effect.race` / `Effect.raceAll` |
| Timeout | `AbortController` (manual) | `Effect.timeout` |
| Cancel children on parent failure | Not possible | Automatic (structured concurrency) |
| Shared mutable state | `let` (race conditions) | `Ref` (atomic) |
| One-time signal | Manual Promise + resolve | `Deferred` |
| Producer-consumer | Manual, no backpressure | `Queue` (bounded, dropping, sliding) |

## Quick Reference

```typescript
import { Effect, Fiber, Ref, Deferred, Queue } from "effect";

// --- Forking and joining ---
const fiber = yield* Effect.fork(effect);        // Start concurrent fiber
const result = yield* Fiber.join(fiber);          // Wait for result
yield* Fiber.interrupt(fiber);                    // Cancel a fiber
yield* Effect.forkDaemon(effect);                 // Detach from parent

// --- Parallel execution ---
Effect.all(effects, { concurrency: "unbounded" }) // Fully parallel
Effect.all(effects, { concurrency: 5 })            // Bounded parallel
Effect.forEach(items, f, { concurrency: 10 })      // Map with bounded concurrency
Effect.allSettled(effects)                          // All results, even failures

// --- Racing ---
Effect.race(effectA, effectB)                      // First success wins
Effect.raceAll(effects)                            // Race many
effect.pipe(Effect.timeout("5 seconds"))           // Timeout as Option
effect.pipe(Effect.timeoutFail({ duration, onTimeout }))  // Timeout as error

// --- Ref (concurrent state) ---
const ref = yield* Ref.make(initialValue);
const value = yield* Ref.get(ref);
yield* Ref.set(ref, newValue);
yield* Ref.update(ref, (old) => transform(old));

// --- Deferred (one-time signal) ---
const d = yield* Deferred.make<A, E>();
yield* Deferred.succeed(d, value);                 // Complete with value
yield* Deferred.fail(d, error);                    // Complete with error
const value = yield* Deferred.await(d);            // Block until completed

// --- Queue (producer-consumer) ---
const q = yield* Queue.bounded<A>(capacity);
yield* Queue.offer(q, item);                       // Enqueue (blocks if full)
const item = yield* Queue.take(q);                 // Dequeue (blocks if empty)
const size = yield* Queue.size(q);                 // Current size
yield* Queue.shutdown(q);                          // Close the queue
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 8a — Streams: Lazy, Effectful Sequences, where we apply the same composition patterns to potentially infinite sequences of values — with resource safety, backpressure, and all the familiar operations: map, flatMap, filter, fold.*
