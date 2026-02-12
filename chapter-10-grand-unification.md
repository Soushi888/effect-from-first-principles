# Chapter 10: The Grand Unification — Patterns Across Effect

> **Building on:** This chapter synthesizes all previous chapters. Key types: `Option<A>`, `Either<E, A>`, `Effect<A, E, R>`, `Stream<A, E, R>`, `Layer<A, E, R>`, `Schedule<Out, In, R>`, `Scope`, `Fiber`, `Ref`, `Queue`.

## You Already Know This

Over nine chapters, you've built, used, and composed six different types: `Option`, `Either`, `Effect`, `Stream`, `Layer`, and `Schedule`. Each chapter introduced its type through concrete problems — missing values, typed errors, async I/O, sequences, dependencies, timing. Each time, we built up operations from scratch and discovered the same toolkit.

It's time to name what you've been doing.

Let's lay out the evidence. Here's every type we've encountered, with the core operations side by side:

```
                 wrap          transform      chain           combine
Option           some(a)       map(oa, f)     flatMap(oa, f)  zip(oa, ob)
Either           right(a)      map(ea, f)     flatMap(ea, f)  zip(ea, eb)
Effect           succeed(a)    map(eff, f)    flatMap(eff, f) zip(effA, effB)
Stream           make(a)       map(s, f)      flatMap(s, f)   zip(sA, sB)
Schedule         succeed(a)    map(s, f)      flatMap(s, f)   —
Layer            succeed(t,v)  —              —               merge(lA, lB)
```

Six types. The same four operations. This isn't coincidence — it's the deepest pattern in functional programming.

## Three Levels of Power

The operations form a hierarchy. Each level builds on the one below.

### Level 1: Functor — "I can transform what's inside"

The only requirement: `map`. Given an `F<A>` and a function `A => B`, produce an `F<B>`.

```typescript
// Every type we've seen is a Functor
pipe(Option.some(8080),              Option.map((p) => p * 2));      // Option<number>
pipe(Either.right(8080),             Either.map((p) => p * 2));      // Either<E, number>
pipe(Effect.succeed(8080),           Effect.map((p) => p * 2));      // Effect<number>
pipe(Stream.make(1, 2, 3),          Stream.map((n) => n * 2));      // Stream<number>
```

Functor says: "I don't care what container you're in — I can reach inside and transform the value." The container's structure (absence, error channel, async, sequence) is preserved. Only the value changes.

The law is simple: mapping the identity function does nothing. `map(fa, x => x)` equals `fa`. And mapping two functions in sequence is the same as mapping their composition: `map(map(fa, f), g)` equals `map(fa, x => g(f(x)))`.

You've been relying on these laws implicitly every time you chained `.pipe(Effect.map(f), Effect.map(g))` and expected it to work. It works because `Effect` is a lawful Functor.

### Level 2: Applicative — "I can combine independent containers"

Add `zip` (or equivalently, `all`). Given an `F<A>` and an `F<B>`, produce an `F<[A, B]>` — combine two independent values in their containers.

```typescript
// Combine independent values
Option.zip(Option.some("host"),      Option.some(8080));             // Option<[string, number]>
Either.zip(parseHost(raw),           parsePort(raw));                // Either<E, [string, number]>
Effect.zip(fetchConfig,              fetchSecrets);                  // Effect<[Config, Secrets]>
Effect.all([checkA, checkB, checkC]);                                // Effect<[A, B, C]>
```

The critical word is **independent**. `zip` and `all` combine values that don't depend on each other. This is why they can run in parallel (for `Effect`) and why they can accumulate errors (for validation).

Remember Chapter 2's validation problem? We wanted to validate three fields and report *all* errors, not just the first. That's the applicative pattern: independent computations, combined results.

```typescript
// Applicative: independent, can accumulate errors
Effect.all(
  [validateHost(raw), validatePort(raw), validateTtl(raw)],
  { mode: "validate" }
);

// Monadic (next level): dependent, must short-circuit
Effect.gen(function* () {
  const host = yield* validateHost(raw);     // port validation depends on...
  const port = yield* validatePort(raw);     // ...nothing, but flatMap forces sequencing
  const conn = yield* connect(host, port);   // THIS depends on host and port
});
```

### Level 3: Monad — "I can chain dependent computations"

Add `flatMap`. Given an `F<A>` and a function `A => F<B>`, produce an `F<B>`. The function *uses the value from the first computation* to decide what computation to run next.

```typescript
// flatMap: the second step depends on the first step's value
pipe(
  findNode("primary"),                    // Effect<NodeRecord>
  Effect.flatMap((node) =>                // Use the node to decide what to do next
    fetchConfig(node.configId)            // This call depends on node.configId
  )
);

pipe(
  Stream.make("node-a", "node-b"),
  Stream.flatMap((nodeId) =>              // For each nodeId, produce a sub-stream
    streamNodeEvents(nodeId)              // Which events to stream depends on which node
  )
);
```

`flatMap` introduces **dependency** between steps. Step 2 can't begin until step 1 completes, because step 2 *needs step 1's result*. This is why `flatMap` short-circuits on failure — if step 1 fails, there's no value to pass to step 2.

And this is why `Effect.gen` exists:

```typescript
Effect.gen(function* () {
  const config = yield* loadConfig;           // Step 1
  const db = yield* connect(config.dbUrl);    // Step 2 depends on step 1
  const users = yield* db.query("...");       // Step 3 depends on step 2
  return users;                               // Final result depends on step 3
});
```

Every `yield*` is a `flatMap`. The generator syntax makes the dependencies visible — each line can use any variable from the lines above. This is the same insight as Scala's for-comprehension and Haskell's do-notation, expressed in TypeScript's generator syntax.

### The Hierarchy

```
Monad       (flatMap)    — chain dependent computations, short-circuit on failure
  ↑ extends
Applicative (zip/all)    — combine independent computations, can accumulate errors
  ↑ extends
Functor     (map)        — transform a value inside a container
```

Every Monad is an Applicative. Every Applicative is a Functor. But not every Functor is an Applicative (you can't always combine), and not every Applicative is a Monad (you can't always chain dependencies).

In Effect-TS, `Option`, `Either`, `Effect`, and `Stream` are all Monads — they support all three levels. This is why the same patterns work everywhere.

## The Universal Destructor: match / fold

There's one more operation that appears everywhere — the way you *exit* the container and make a decision based on what's inside:

```typescript
// Option: handle presence and absence
Option.match(opt, {
  onNone: () => "nothing",
  onSome: (a) => `got: ${a}`,
});

// Either: handle success and failure
Either.match(either, {
  onLeft: (e) => `error: ${e}`,
  onRight: (a) => `value: ${a}`,
});

// Effect: handle success and failure (after running)
Effect.match(effect, {
  onSuccess: (a) => `ok: ${a}`,
  onFailure: (e) => `fail: ${e}`,
});

// Exit (what you get after running an Effect):
Exit.match(exit, {
  onSuccess: (a) => `completed: ${a}`,
  onFailure: (cause) => `failed: ${Cause.pretty(cause)}`,
});
```

`match` (also called `fold` in some traditions) is the universal destructor. It's the inverse of the constructors — where `some` and `none` *build* an Option, `match` *eliminates* it by handling both cases. Where `succeed` and `fail` build an Effect, `match` handles both outcomes.

This is the principle of **algebraic data types**: every type is built from a fixed set of constructors, and `match` ensures you've handled every case. TypeScript's discriminated unions give us this with exhaustiveness checking.

## Why pipe + Module Functions?

In Haskell or Scala, these patterns are expressed through **typeclasses** — interfaces like `Functor`, `Applicative`, `Monad` that types implement. TypeScript doesn't have typeclasses. It has:

- Interfaces (but no higher-kinded types, so you can't write `Functor<F>`)
- Classes (but union types can't have methods)
- Functions (these compose freely)

Effect-TS chose the pragmatic encoding: **module functions with pipe**.

```typescript
// Haskell:  fmap (+1) (Just 5)
// Scala:    Some(5).map(_ + 1)
// Effect:   pipe(Option.some(5), Option.map(n => n + 1))
```

`pipe` is TypeScript's answer to method chaining. It works on *any* value, including union types (which can't have methods). Each module (`Option`, `Either`, `Effect`, `Stream`) exports the same set of function names. The consistency is maintained by convention and the type checker, not by a typeclass mechanism.

This means you carry the pattern in your head rather than in the type system. When you see a new Effect module, you know to look for `map`, `flatMap`, `zip`, `match`. They'll be there, and they'll work the way you expect.

## The Effects-as-Data Principle: One Last Time

Let's revisit the deepest principle one final time. Every major type in Effect is a **description** — a pure value representing something that *will* happen, not something that *has* happened.

```
Type          Describes                    Executed by
─────────────────────────────────────────────────────────────
Effect        A computation                runPromise / runSync
Layer         A dependency recipe          Effect.provide
Stream        A sequence of values         Stream.run / runCollect
Schedule      A timing policy              Effect.retry / Effect.repeat
Scope         A resource lifetime          Effect.scoped
```

Descriptions compose before execution. This is why you can:

- Build a complete application as a single `Effect`, inspect its type, provide dependencies, and run it at the very end
- Construct an entire Layer graph, verify it at compile time, and build all services in one shot
- Define a stream pipeline with multiple stages, then consume it with a single `runCollect`
- Compose a retry policy from primitives, then attach it to any Effect

The separation of description from execution is what makes testing, composition, and reasoning possible. You can test a Layer by providing mock dependencies. You can test a Schedule by checking its delays without waiting. You can test a Stream pipeline without actual I/O. Everything is a value, and values are easy to test.

## The Complete Pattern Map

Here's the full table — every type, every operation, across the entire series:

| | **Functor** (`map`) | **Applicative** (`zip`/`all`) | **Monad** (`flatMap`) | **Destructor** (`match`/`fold`) |
|---|---|---|---|---|
| **Option** | Transform value if present | Combine if both present | Chain operations that might be absent | `match(onNone, onSome)` |
| **Either** | Transform right value | Combine rights (accumulate lefts) | Chain, short-circuit on Left | `match(onLeft, onRight)` |
| **Effect** | Transform success | Combine independent effects | Chain dependent effects | `match(onFailure, onSuccess)` |
| **Stream** | Transform each element | Pair elements from two streams | Chain to sub-streams | `run` with Sink |
| **Schedule** | Transform output | — | Chain policies | Consumed by retry/repeat |
| **Layer** | — | Merge services | — | Consumed by provide |

And the cross-cutting concerns that the `Effect` type tracks in its three parameters:

| Parameter | What it tracks | How it composes | Chapter |
|---|---|---|---|
| `A` (Success) | What the computation produces | `map` transforms it | 3 |
| `E` (Error) | How the computation can fail | `flatMap` unions them; `catchTag` removes them | 4a–4b |
| `R` (Requirements) | What the computation needs | `flatMap` unions them; `provide` removes them | 6 |

The `E` and `R` channels are dual: both are unions that grow through composition and shrink through handling/providing. The type system tracks both, and the compiler refuses to run until `E` is handled and `R` is satisfied.

## What You've Built

Over ten chapters, you've gone from "why does try/catch not compose?" to composing concurrent, resource-safe, dependency-injected, retry-resilient streaming pipelines — all with typed errors and compile-time verification.

Here's a complete program that uses nearly every concept:

```typescript
import { Effect, Layer, Stream, Schedule, Ref, Queue, pipe, Duration } from "effect";

// Services (Chapter 6)
class Config extends Context.Tag("Config")<Config, {
  readonly peers: string[];
  readonly batchSize: number;
  readonly checkInterval: Duration.Duration;
}>() {}

class PeerClient extends Context.Tag("PeerClient")<PeerClient, {
  readonly query: (peerId: string, key: string) => Effect.Effect<string, PeerError>;
  readonly broadcast: (msg: unknown) => Effect.Effect<void, PeerError>;
}>() {}

// Typed errors (Chapters 4a–4b)
class PeerError extends Data.TaggedError("PeerError")<{
  readonly peerId: string;
  readonly cause: string;
}> {}

class AggregationError extends Data.TaggedError("AggregationError")<{
  readonly message: string;
}> {}

// Retry policy (Chapter 9)
const peerRetryPolicy = pipe(
  Schedule.exponential("200 millis"),
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3)),
);

// Resource-safe stream pipeline (Chapters 5, 7, 8)
const replicationPipeline = Effect.gen(function* () {
  const config = yield* Config;
  const client = yield* PeerClient;
  const queue = yield* Queue.bounded<{ key: string; value: string }>(1000);
  const processedCount = yield* Ref.make(0);

  // Producer: accept writes into the queue
  const producer = Stream.fromQueue(queue);

  // Consumer: replicate to all peers with batching and retries
  const consumer = producer.pipe(
    Stream.groupedWithin(config.batchSize, "2 seconds"),
    Stream.mapEffect((batch) =>
      Effect.forEach(
        config.peers,
        (peerId) =>
          client.broadcast({ type: "replicate", items: batch }).pipe(
            Effect.retry(peerRetryPolicy),
            Effect.catchTag("PeerError", (e) =>
              Effect.logWarning(`Replication to ${e.peerId} failed: ${e.cause}`)
            ),
          ),
        { concurrency: "unbounded" }
      ).pipe(
        Effect.tap(() => Ref.update(processedCount, (n) => n + Chunk.size(batch)))
      )
    ),
    Stream.runDrain,
  );

  // Health monitor running concurrently (Chapter 7)
  const monitor = pipe(
    Ref.get(processedCount),
    Effect.tap((count) => Effect.log(`Processed ${count} items`)),
    Effect.repeat(Schedule.spaced(config.checkInterval)),
  );

  // Run consumer and monitor concurrently (Chapter 7)
  yield* Effect.fork(monitor);
  yield* Effect.fork(consumer);

  // Return the queue for the caller to push writes into
  return { enqueue: (item: { key: string; value: string }) => Queue.offer(queue, item) };
});

// Layer composition (Chapter 6)
const ConfigLive = Layer.succeed(Config, {
  peers: ["peer-1:8080", "peer-2:8080", "peer-3:8080"],
  batchSize: 50,
  checkInterval: Duration.seconds(10),
});

const PeerClientLive = Layer.scoped(
  PeerClient,
  Effect.gen(function* () {
    const httpClient = yield* makeHttpClient(); // acquireRelease (Chapter 5)
    return {
      query: (peerId, key) =>
        httpClient.get(`http://${peerId}/data/${key}`).pipe(
          Effect.mapError((e) => new PeerError({ peerId, cause: String(e) })),
          Effect.timeout("3 seconds"),
          Effect.flatten,
        ),
      broadcast: (msg) =>
        Effect.forEach(
          (yield* Config).peers,
          (peerId) =>
            httpClient.post(`http://${peerId}/replicate`, msg).pipe(
              Effect.mapError((e) => new PeerError({ peerId, cause: String(e) }))
            ),
          { concurrency: "unbounded" }
        ).pipe(Effect.map(() => undefined)),
    };
  })
);

const MainLayer = ConfigLive.pipe(
  Layer.provideMerge(PeerClientLive),
);

// Run (Chapter 3)
const main = replicationPipeline.pipe(
  Effect.provide(MainLayer),
  Effect.catchAll((e) => Effect.logError(`Pipeline failed: ${e}`)),
);

Effect.runPromise(Effect.scoped(main));
```

This program:

- **Declares services** with typed interfaces and `Context.Tag` (Chapter 6)
- **Defines typed errors** with tagged discriminated unions (Chapters 4a–4b)
- **Manages resources** with `acquireRelease` and `Scope` (Chapter 5)
- **Streams data** through a batched pipeline with `groupedWithin` (Chapters 8a–8b)
- **Runs concurrently** with forked fibers and bounded parallelism (Chapter 7)
- **Retries failures** with exponential backoff and jitter (Chapter 9)
- **Shares state** safely through `Ref` and `Queue` (Chapter 7)
- **Composes layers** into a dependency graph verified at compile time (Chapter 6)
- **Short-circuits on typed errors** with automatic union tracking (Chapter 3)

And the entire program is a description — a pure value. Nothing runs until `Effect.runPromise`. You can test it by providing mock layers. You can reason about it by reading the types.

## Where to Go from Here

### The Effect Ecosystem

Effect is more than the core `effect` package. The ecosystem includes:

- **@effect/platform** — cross-platform HTTP clients, file system, terminal, command execution
- **@effect/sql** — typed database access with connection pooling
- **@effect/rpc** — type-safe remote procedure calls between services
- **@effect/cluster** — distributed systems primitives: sharding, pub/sub, durable execution
- **@effect/opentelemetry** — built-in observability with spans and metrics

Each library follows the same patterns: services declared with `Context.Tag`, provided as `Layer`, composed with `pipe`, producing typed `Effect` values. Once you know the core, every extension is familiar.

### Testing with Effect

Effect programs are inherently testable because dependencies are explicit:

```typescript
// Test by providing mock layers
const testProgram = myBusinessLogic.pipe(
  Effect.provide(Layer.merge(
    MockDatabaseLayer,
    MockHttpClientLayer,
  ))
);

// Run in tests
const result = await Effect.runPromise(testProgram);
expect(result).toEqual(expectedValue);
```

The `@effect/vitest` package provides deeper integration with test frameworks, including automatic resource cleanup between tests.

### Production Patterns

As you build production services, you'll encounter patterns that combine multiple chapters:

- **Graceful shutdown**: `Scope` finalizers + fiber interruption + queue draining
- **Health endpoints**: `Ref` holding latest status + periodic `Schedule` checks
- **Request middleware**: `Layer` providing tracing context + error mapping + timeout
- **Event sourcing**: `Stream` of events + `Ref` for projections + `Queue` for commands
- **Saga orchestration**: `Effect.gen` for the happy path + `catchTag` for compensating actions

### Further Reading

- **Effect Documentation** — [effect.website](https://effect.website) — comprehensive API reference and guides
- **Functional Programming in Scala** (Chiusano & Bjarnason) — the Red Book that inspired this series
- **Haskell Programming from First Principles** — deeper type theory if you want it
- **Designing Data-Intensive Applications** (Kleppmann) — distributed systems patterns that these tools implement

## Exercise 10.1 (Capstone): Build a Mini Replication Service

This exercise ties together every major concept from the series. You'll build a simplified distributed key-value store with quorum reads, streaming replication, and resilient peer communication.

### Part A: Service Interfaces and Error Types

Define the service interfaces and error hierarchy. Think about which errors are expected and which are defects.

```typescript
import { Effect, Context, Data, Stream, Queue, Ref, Schedule, Layer, Duration, Chunk } from "effect";

// --- Error Types (Chapters 4a–4b) ---

class StoreError extends Data.TaggedError("StoreError")<{
  readonly key: string;
  readonly cause: string;
}> {}

class PeerError extends Data.TaggedError("PeerError")<{
  readonly peerId: string;
  readonly cause: string;
}> {}

class QuorumError extends Data.TaggedError("QuorumError")<{
  readonly key: string;
  readonly required: number;
  readonly received: number;
}> {}

// --- Service Interfaces (Chapter 6) ---

class Config extends Context.Tag("Config")<Config, {
  readonly peers: ReadonlyArray<string>;
  readonly replicationFactor: number;
  readonly quorumSize: number;
  readonly heartbeatInterval: Duration.Duration;
}>();

class KeyValueStore extends Context.Tag("KeyValueStore")<KeyValueStore, {
  readonly get: (key: string) => Effect.Effect<string, StoreError>;
  readonly set: (key: string, value: string) => Effect.Effect<void, StoreError>;
}>();

class PeerClient extends Context.Tag("PeerClient")<PeerClient, {
  readonly read: (peerId: string, key: string) => Effect.Effect<string, PeerError>;
  readonly send: (peerId: string, entries: ReadonlyArray<{ key: string; value: string }>) => Effect.Effect<void, PeerError>;
  readonly ping: (peerId: string) => Effect.Effect<boolean, PeerError>;
}>();
```

### Part B: Quorum Read

Implement a scatter-gather quorum read: fan out to N replicas concurrently, collect the first `quorumSize` successful responses, and return the most common value. If fewer than `quorumSize` respond, fail with `QuorumError`.

```typescript
const quorumRead = (key: string): Effect.Effect<
  string,
  QuorumError | PeerError,
  Config | PeerClient
> => {
  // todo()
  //
  // Hints:
  // 1. Get config and peerClient from the context
  // 2. Fork a read to each peer (Effect.forEach with concurrency: "unbounded")
  // 3. Use a Deferred<string> to signal when quorum is reached
  // 4. Use a Ref<string[]> to collect successful responses
  // 5. Each forked read: on success, update the Ref and check if quorum is met
  // 6. If quorum is met, complete the Deferred with the majority value
  // 7. Race the Deferred against a timeout
  // 8. If not enough responses arrive, fail with QuorumError
};
```

### Part C: Replication Stream

Build a replication pipeline: entries are pushed into a `Queue`, consumed as a `Stream`, batched, and broadcast to all peers with retry.

```typescript
const replicationPipeline = (
  replicationQueue: Queue.Queue<{ key: string; value: string }>
): Effect.Effect<void, never, Config | PeerClient> => {
  // todo()
  //
  // Hints:
  // 1. Stream.fromQueue(replicationQueue) to create the source stream
  // 2. Stream.groupedWithin(20, "1 second") for batching
  // 3. For each batch, broadcast to all peers from Config
  // 4. Use Effect.retry with Schedule.exponential("200 millis")
  //      composed with Schedule.recurs(3) for each peer send
  // 5. Use Effect.catchTag("PeerError", ...) to log and continue on failure
  //      (one failed peer shouldn't stop replication to others)
  // 6. Stream.runDrain to consume the pipeline
};
```

### Part D: Wire Everything Together

Compose the services with Layers and run the complete system.

```typescript
// Build the Layer graph:
// - ConfigLive: hardcoded or from environment
// - KeyValueStoreLive: in-memory Map wrapped in Ref
//     - On `set`, also enqueue entries for replication
// - PeerClientLive: simulated with Effect.sleep for latency
//
// The main program should:
// 1. Create a replication queue
// 2. Fork the replicationPipeline
// 3. Fork a heartbeat loop: ping each peer on a schedule, log status
// 4. Perform a few set operations (which trigger replication)
// 5. Perform a quorum read
// 6. Log the result

const MainLayer: Layer.Layer<Config | KeyValueStore | PeerClient> = {
  // todo() — compose ConfigLive, KeyValueStoreLive, PeerClientLive
};

const main: Effect.Effect<void> = {
  // todo()
  // Hint: Effect.gen with forked pipelines, then test operations
};

// Effect.runPromise(Effect.provide(main, MainLayer));
```

### What This Exercise Covers

| Concept | Chapter | Where it appears |
|---|---|---|
| Services + `Context.Tag` | 6 | `Config`, `KeyValueStore`, `PeerClient` |
| Tagged errors | 4a | `StoreError`, `PeerError`, `QuorumError` |
| Resources + `Scope` | 5 | HTTP client lifecycle (if you add `acquireRelease`) |
| Concurrency + `Ref` + `Deferred` | 7 | Quorum scatter-gather, shared state |
| Streams + batching | 8a, 8b | Replication pipeline with `groupedWithin` |
| Scheduling + retry | 9 | Heartbeat loop, exponential backoff on peer sends |
| Layer composition | 6 | `MainLayer` wiring |
| `pipe` composition | 2 | Throughout |

*This is a challenging exercise. Start with Part A (types only), then implement each part incrementally. Ask for hints on any specific part.*

## A Final Word

The word "Monad" appeared in Chapter 10, not Chapter 1. That was deliberate.

Too many functional programming resources start with the theory — "A monad is a monoid in the category of endofunctors" — and leave the reader more confused than when they started. The Red Book's insight was to invert this: build the intuition through practice, then name the pattern once it's already familiar.

You implemented `map` and `flatMap` on `Option` before knowing they were Functor and Monad operations. You used `Effect.all` with `{ mode: "validate" }` before knowing it was the applicative pattern. You composed `Schedule` values before knowing they follow the same algebraic laws as everything else.

The patterns are the same everywhere because they capture something fundamental about computation: you can transform values (`map`), combine independent values (`zip`), and sequence dependent steps (`flatMap`). Whether you're handling missing data, typed errors, async I/O, infinite streams, dependency graphs, or retry policies — the algebra is the same.

Now you know the algebra. Go build something.

---

*This concludes the series. If you'd like solutions to any exercises from any chapter, just ask.*
