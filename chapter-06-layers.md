# Chapter 6: Dependency Injection with Layers

> **Building on:** This chapter uses `Effect.gen`, typed errors, `acquireRelease`, and `Scope` from Chapters 3–5. Key types so far: `Option<A>`, `Either<E, A>`, `Effect<A, E, R>`, `Scope`.

## The Dependency Problem

In Chapter 5, we built scoped resources — database connections, HTTP clients, message queues — and composed them in flat blocks. But we hardcoded the wiring: `makeDatabase(config.dbUrl)` knows where to get the URL, and the pipeline function builds everything itself.

In a real distributed application, this becomes untenable. Consider a service with this dependency graph:

```
AppConfig
  ├─→ DatabasePool
  │     └─→ UserRepository
  │           └─→ UserService
  ├─→ HttpClient
  │     ├─→ ServiceRegistry
  │     └─→ PeerClient
  │           └─→ ReplicationService
  └─→ MetricsReporter
```

`UserService` needs `UserRepository`, which needs `DatabasePool`, which needs `AppConfig`. `ReplicationService` needs `PeerClient`, which needs `HttpClient`, which also needs `AppConfig`. The configuration is shared. The HTTP client is shared. Some services need scoped resources.

How do you wire this up? The common approaches in TypeScript each have problems:

**Constructor injection** — pass dependencies as parameters. Works for small apps, but the top-level function ends up with a dozen parameters and must know the entire graph. Testing requires manually building the whole tree.

**Global singletons** — import from a module. No way to swap implementations for testing. Hidden dependencies. Initialization order is fragile.

**DI frameworks** — Spring-style containers. Runtime wiring, stringly-typed, no compile-time safety. Errors appear at startup when the container can't resolve a dependency.

Effect's approach is different: **the type system tracks dependencies, and `Layer` is a composable recipe for satisfying them.**

## The R Parameter: Declaring What You Need

Recall that `Effect<A, E, R>` has three parameters. We've used `A` (success) and `E` (error) extensively. Now it's time for `R` — the Requirements channel.

`R` declares what services an Effect needs to run. It appears as a union type, and the compiler refuses to run the Effect until every requirement is satisfied:

```typescript
import { Effect, Context } from "effect";

// Declare a service using Context.Tag
class DatabasePool extends Context.Tag("DatabasePool")<
  DatabasePool,
  {
    readonly query: (sql: string) => Effect.Effect<unknown[], QueryError>;
    readonly execute: (sql: string) => Effect.Effect<void, QueryError>;
  }
>() {}

// Use the service in an Effect — it appears in the R parameter
const getUsers = Effect.gen(function* () {
  const db = yield* DatabasePool; // Access the service
  const rows = yield* db.query("SELECT * FROM users");
  return rows as User[];
});
// Type: Effect<User[], QueryError, DatabasePool>
//                                   ^^^^^^^^^^^
// The compiler knows this needs a DatabasePool to run
```

`Context.Tag` creates a service identifier — a unique token that represents a capability your code needs. When you `yield*` a tag inside `Effect.gen`, two things happen:

1. You get access to the service implementation
2. The service appears in the `R` parameter of the resulting Effect

This is a *declaration*, not a request. Nothing is resolved yet. The Effect just says "I need a `DatabasePool` to run."

### Services Compose Through flatMap

When you chain Effects that need different services, their requirements union automatically — exactly like error types:

```typescript
class AppConfig extends Context.Tag("AppConfig")<
  AppConfig,
  { readonly dbUrl: string; readonly peerEndpoints: string[] }
>() {}

class MetricsReporter extends Context.Tag("MetricsReporter")<
  MetricsReporter,
  { readonly increment: (metric: string) => Effect.Effect<void> }
>() {}

const registerUser = (name: string) =>
  Effect.gen(function* () {
    const db = yield* DatabasePool;          // Needs DatabasePool
    const metrics = yield* MetricsReporter;  // Needs MetricsReporter

    yield* db.execute(`INSERT INTO users (name) VALUES ('${name}')`);
    yield* metrics.increment("users.registered");
  });
// Type: Effect<void, QueryError, DatabasePool | MetricsReporter>
//                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
// Both requirements tracked automatically
```

The compiler computed `DatabasePool | MetricsReporter` for us. If we chain this with another Effect that needs `AppConfig`, the union grows to `DatabasePool | MetricsReporter | AppConfig`. Every requirement is tracked. Nothing is forgotten.

And here's the constraint: **you cannot run this Effect until all requirements are provided.** `Effect.runPromise` only accepts `Effect<A, E, never>` — an Effect with no unsatisfied requirements:

```typescript
// This won't compile — R is not never
Effect.runPromise(registerUser("Alice"));
// Type error: Effect<void, QueryError, DatabasePool | MetricsReporter>
//             is not assignable to Effect<void, QueryError, never>
```

The compiler forces you to provide every dependency before execution. No runtime "service not found" errors. No missing bindings discovered in production. The type system is your dependency graph validator.

## Layer: A Recipe for Building Services

A `Layer<Out, Error, In>` is a recipe that describes how to build a service:

- **Out** — the service(s) it produces
- **Error** — what can go wrong during construction
- **In** — what it needs to build the service (its own dependencies)

```typescript
import { Layer, Effect } from "effect";

// A Layer that produces AppConfig, needs nothing
const AppConfigLive: Layer.Layer<AppConfig> = Layer.succeed(
  AppConfig,
  { dbUrl: "postgres://localhost/mydb", peerEndpoints: ["peer1:8080", "peer2:8080"] }
);

// A Layer that produces DatabasePool, might fail, needs AppConfig
const DatabasePoolLive: Layer.Layer<DatabasePool, DbConnectionError, AppConfig> =
  Layer.scoped(
    DatabasePool,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const pool = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => createPool(config.dbUrl),
          catch: (e) => new DbConnectionError(String(e)),
        }),
        (pool) => Effect.promise(() => pool.end()).pipe(
          Effect.catchAll(() => Effect.void)
        )
      );
      return {
        query: (sql) => Effect.tryPromise({
          try: () => pool.query(sql).then((r) => r.rows),
          catch: (e) => new QueryError(sql, String(e)),
        }),
        execute: (sql) => Effect.tryPromise({
          try: () => pool.query(sql).then(() => undefined),
          catch: (e) => new QueryError(sql, String(e)),
        }),
      };
    })
  );
```

Notice `Layer.scoped` — it creates a layer from a scoped Effect, meaning the resource lifecycle is tied to the layer's lifetime. When the application shuts down, the pool is closed. This is `acquireRelease` from Chapter 5, integrated into the dependency system.

### Layer Constructors

| Constructor | Use when | Resource management |
|---|---|---|
| `Layer.succeed(tag, value)` | Pure, constant value | None |
| `Layer.effect(tag, effect)` | Effectful construction | None |
| `Layer.scoped(tag, scopedEffect)` | Resource with cleanup | Tied to layer lifetime |
| `Layer.function(tag, fn)` | Synchronous factory | None |

## Composing Layers

Layers compose with three operations that mirror the dependency graph:

### Layer.provide — Satisfy a Layer's Input

```typescript
// DatabasePoolLive needs AppConfig
// AppConfigLive provides AppConfig

const DatabasePoolWithConfig: Layer.Layer<DatabasePool, DbConnectionError> =
  DatabasePoolLive.pipe(Layer.provide(AppConfigLive));
// AppConfig requirement is gone — satisfied by AppConfigLive
```

`Layer.provide` connects one layer's output to another's input. The `In` requirement disappears from the composed layer because it's been satisfied.

### Layer.merge — Provide Multiple Services

```typescript
const MetricsReporterLive: Layer.Layer<MetricsReporter> = Layer.succeed(
  MetricsReporter,
  { increment: (metric) => Effect.log(`METRIC: ${metric}`) }
);

// Merge produces both DatabasePool AND MetricsReporter
const AppLayer: Layer.Layer<DatabasePool | MetricsReporter, DbConnectionError> =
  Layer.merge(DatabasePoolWithConfig, MetricsReporterLive);
```

`Layer.merge` combines two layers that produce different services. The output is the union of both. This is how you build up the full set of services your application needs.

### Layer.provideMerge — Provide and Keep

Sometimes a layer needs a dependency, and downstream code also needs that same dependency:

```typescript
// Both DatabasePoolLive AND UserRepositoryLive need AppConfig
// Layer.provideMerge satisfies the requirement AND keeps the service available

const combined = AppConfigLive.pipe(
  Layer.provideMerge(DatabasePoolLive)
);
// Type: Layer<AppConfig | DatabasePool, DbConnectionError>
// AppConfig is still available for other layers that need it
```

## Wiring the Full Application

Let's wire up the dependency graph from the beginning of this chapter:

```typescript
import { Effect, Layer, Context } from "effect";

// --- Service declarations ---

class AppConfig extends Context.Tag("AppConfig")<
  AppConfig,
  {
    readonly dbUrl: string;
    readonly httpBaseUrl: string;
    readonly peerEndpoints: string[];
  }
>() {}

class DatabasePool extends Context.Tag("DatabasePool")<
  DatabasePool,
  {
    readonly query: (sql: string) => Effect.Effect<unknown[], QueryError>;
  }
>() {}

class HttpClient extends Context.Tag("HttpClient")<
  HttpClient,
  {
    readonly get: (path: string) => Effect.Effect<unknown, HttpError>;
    readonly post: (path: string, body: unknown) => Effect.Effect<unknown, HttpError>;
  }
>() {}

class UserRepository extends Context.Tag("UserRepository")<
  UserRepository,
  {
    readonly findById: (id: string) => Effect.Effect<User | null, QueryError>;
    readonly save: (user: User) => Effect.Effect<void, QueryError>;
  }
>() {}

class PeerClient extends Context.Tag("PeerClient")<
  PeerClient,
  {
    readonly broadcast: (message: PeerMessage) => Effect.Effect<void, PeerError>;
    readonly query: (nodeId: string, key: string) => Effect.Effect<unknown, PeerError>;
  }
>() {}

// --- Layer implementations ---

const AppConfigLive = Layer.succeed(AppConfig, {
  dbUrl: "postgres://localhost/mydb",
  httpBaseUrl: "https://registry.cluster.local",
  peerEndpoints: ["peer1:8080", "peer2:8080", "peer3:8080"],
});

const DatabasePoolLive = Layer.scoped(
  DatabasePool,
  Effect.gen(function* () {
    const config = yield* AppConfig;
    const pool = yield* makePool(config.dbUrl); // acquireRelease from Ch. 5
    return { query: (sql) => runQuery(pool, sql) };
  })
);

const HttpClientLive = Layer.scoped(
  HttpClient,
  Effect.gen(function* () {
    const config = yield* AppConfig;
    const client = yield* makeHttpClient(config.httpBaseUrl); // acquireRelease
    return {
      get: (path) => httpGet(client, path),
      post: (path, body) => httpPost(client, path, body),
    };
  })
);

const UserRepositoryLive = Layer.effect(
  UserRepository,
  Effect.gen(function* () {
    const db = yield* DatabasePool;
    return {
      findById: (id) =>
        db.query(`SELECT * FROM users WHERE id = '${id}'`).pipe(
          Effect.map((rows) => (rows[0] as User) ?? null)
        ),
      save: (user) =>
        db.query(`INSERT INTO users ...`).pipe(Effect.map(() => undefined)),
    };
  })
);

const PeerClientLive = Layer.effect(
  PeerClient,
  Effect.gen(function* () {
    const config = yield* AppConfig;
    const http = yield* HttpClient;
    return {
      broadcast: (msg) =>
        Effect.all(
          config.peerEndpoints.map((ep) =>
            http.post(`${ep}/messages`, msg).pipe(
              Effect.catchAll((e) => Effect.logWarning(`Peer ${ep} unreachable: ${e}`))
            )
          ),
          { concurrency: "unbounded" }
        ).pipe(Effect.map(() => undefined)),
      query: (nodeId, key) =>
        http.get(`${nodeId}/data/${key}`).pipe(
          Effect.mapError((e) => new PeerError(nodeId, String(e)))
        ),
    };
  })
);

// --- Compose the full layer ---

const MainLayer = AppConfigLive.pipe(
  Layer.provideMerge(DatabasePoolLive),    // AppConfig → DatabasePool (keeps AppConfig)
  Layer.provideMerge(HttpClientLive),      // AppConfig → HttpClient (keeps AppConfig)
  Layer.provideMerge(UserRepositoryLive),  // DatabasePool → UserRepository
  Layer.provideMerge(PeerClientLive),      // AppConfig + HttpClient → PeerClient
);
// Type: Layer<AppConfig | DatabasePool | HttpClient | UserRepository | PeerClient, ...>
```

Every dependency is satisfied. Every resource has cleanup. The type system verified the entire graph at compile time.

### Providing Layers to an Effect

```typescript
// The business logic — doesn't know how anything is built
const handleRegistration = (name: string) =>
  Effect.gen(function* () {
    const users = yield* UserRepository;
    const peers = yield* PeerClient;

    const existing = yield* users.findById(name);
    if (existing) return existing;

    const newUser: User = { id: name, createdAt: new Date() };
    yield* users.save(newUser);
    yield* peers.broadcast({ type: "user.created", payload: newUser });
    return newUser;
  });
// Type: Effect<User, QueryError | PeerError, UserRepository | PeerClient>

// Wire it up and run
const program = handleRegistration("alice").pipe(
  Effect.provide(MainLayer)
);
// Type: Effect<User, QueryError | PeerError | DbConnectionError | HttpError>
// R is now `never` — all dependencies satisfied

Effect.runPromise(program);
```

`Effect.provide(MainLayer)` satisfies all the requirements in one shot. The `R` parameter becomes `never`. The error type gains the construction errors from the layers (`DbConnectionError`, `HttpError`). This is correct — if the database pool can't be created, the whole program fails with that error.

## Testing with Mock Layers

The real power of this system: swap the entire implementation by providing a different layer. No mocking frameworks. No patching imports. Just a different value.

```typescript
// --- Test layers ---
const TestConfig = Layer.succeed(AppConfig, {
  dbUrl: "memory://test",
  httpBaseUrl: "http://localhost:0",
  peerEndpoints: [],
});

// In-memory database for tests
const TestDatabasePool = Layer.succeed(DatabasePool, {
  query: (sql) => {
    // Simple in-memory implementation
    if (sql.includes("SELECT")) return Effect.succeed([{ id: "test", name: "Test User" }]);
    return Effect.succeed([]);
  },
});

// Silent metrics for tests
const TestMetricsReporter = Layer.succeed(MetricsReporter, {
  increment: () => Effect.void,
});

// Compose test layers
const TestLayer = Layer.merge(
  Layer.merge(TestConfig, TestDatabasePool),
  TestMetricsReporter
).pipe(
  Layer.provideMerge(UserRepositoryLive) // Use the real UserRepository with test DB
);

// Same business logic, different wiring
const testProgram = handleRegistration("alice").pipe(
  Effect.provide(TestLayer)
);
```

Notice: `UserRepositoryLive` is the *real* implementation — we only replaced what's beneath it. The repository's logic is tested against a fake database. The business logic in `handleRegistration` doesn't change at all. This is integration testing with surgical control over which layers are real and which are mocked.

## Layer Memoization

By default, layers are **memoized** within a single `Effect.provide` call. If two services both depend on `AppConfig`, the config layer runs once and the result is shared:

```typescript
// DatabasePoolLive needs AppConfig
// HttpClientLive needs AppConfig
// AppConfigLive runs ONCE, both receive the same value

const MainLayer = AppConfigLive.pipe(
  Layer.provideMerge(DatabasePoolLive),
  Layer.provideMerge(HttpClientLive),
);
```

This is usually what you want — you don't want two database pools because two layers independently constructed one. Memoization ensures resources are shared correctly.

If you need a fresh instance (rare), use `Layer.fresh`:

```typescript
const freshConfig = Layer.fresh(AppConfigLive);
// This layer always re-executes, never shares
```

## The Pattern Continues

`Layer` follows the same principles as everything in Effect:

**It's a description.** A `Layer` doesn't build anything when created. It describes how to build services. Construction happens when you `Effect.provide` it to a program.

**It composes.** `Layer.provide`, `Layer.merge`, and `Layer.provideMerge` combine layers into larger layers, just as `flatMap` chains effects and `zip` combines them.

**The type system tracks everything.** Unsatisfied dependencies appear in the `In` parameter. The compiler refuses to run a program with unmet requirements. No "service not found" at runtime.

**Resources are managed.** `Layer.scoped` ties resource lifetimes to the application. When the program ends, resources are released. This is `Scope` from Chapter 5, integrated into the dependency system.

| Concept | Chapter 2-3 | Chapter 5 | Chapter 6 |
|---|---|---|---|
| Description | `Either`, `Effect` | `acquireRelease` | `Layer` |
| What it describes | A computation | A resource lifecycle | A dependency recipe |
| Composition | `flatMap`, `zip` | Multiple `yield*` in `scoped` | `provide`, `merge` |
| Type tracking | `E` (errors) | `Scope` in `R` | `In` and `Out` |
| Execution | `runPromise` | `Effect.scoped` | `Effect.provide` |

## Exercises

### Exercise 6.1: Define a Service

Create a `KeyValueStore` service with `get`, `set`, and `delete` operations. Define the tag, the interface, and an in-memory implementation layer.

```typescript
import { Effect, Context, Layer } from "effect";

class KeyNotFoundError {
  readonly _tag = "KeyNotFoundError" as const;
  constructor(readonly key: string) {}
}

// Define the service tag and interface
class KeyValueStore extends Context.Tag("KeyValueStore")<
  KeyValueStore,
  {
    // todo() — define get, set, delete operations
  }
>() {}

// In-memory implementation
const KeyValueStoreLive: Layer.Layer<KeyValueStore> = // todo()

// Use it
const program = Effect.gen(function* () {
  const kv = yield* KeyValueStore;
  yield* kv.set("node:abc", "10.0.0.1");
  const value = yield* kv.get("node:abc");
  return value;
});
```

### Exercise 6.2: Layered Dependencies

Build a three-layer dependency chain: `Config → Database → UserService`. Each layer depends on the one before it.

```typescript
class Config extends Context.Tag("Config")<
  Config,
  { readonly connectionString: string; readonly maxConnections: number }
>() {}

class Database extends Context.Tag("Database")<
  Database,
  { readonly query: (sql: string) => Effect.Effect<unknown[]> }
>() {}

class UserService extends Context.Tag("UserService")<
  UserService,
  {
    readonly getUser: (id: string) => Effect.Effect<User | null>;
    readonly createUser: (name: string) => Effect.Effect<User>;
  }
>() {}

// Implement each layer
const ConfigLive: Layer.Layer<Config> = // todo()

const DatabaseLive: Layer.Layer<Database, never, Config> = // todo()

const UserServiceLive: Layer.Layer<UserService, never, Database> = // todo()

// Compose them into a single layer that produces all three services
const AppLayer: Layer.Layer<Config | Database | UserService> = // todo()

// Write a program that uses UserService and provide the full layer
const program: Effect.Effect<User | null> = // todo()
```

### Exercise 6.3: Testing with Layer Substitution

Take the `UserService` from Exercise 6.2 and write two tests: one with a real (in-memory) database layer, one with a mock that always returns a fixed user.

```typescript
// Test 1: Real UserService with in-memory Database
const InMemoryDatabase: Layer.Layer<Database> = // todo()

const testWithRealLogic = UserServiceLive.pipe(
  Layer.provide(InMemoryDatabase)
);

// Test 2: Mock UserService that returns fixed data
const MockUserService: Layer.Layer<UserService> = // todo()

// Write test Effects that use Effect.provide with each layer
const test1: Effect.Effect<void> = // todo() — assert createUser then getUser works
const test2: Effect.Effect<void> = // todo() — assert mock always returns the same user
```

### Exercise 6.4: Scoped Layer with Resource Cleanup

Create a `ConnectionPool` layer that acquires N connections on startup and releases them on shutdown. Log each acquisition and release so you can verify the lifecycle.

```typescript
class ConnectionPool extends Context.Tag("ConnectionPool")<
  ConnectionPool,
  {
    readonly getConnection: Effect.Effect<Connection, PoolError>;
    readonly releaseConnection: (conn: Connection) => Effect.Effect<void>;
  }
>() {}

class PoolError {
  readonly _tag = "PoolError" as const;
  constructor(readonly message: string) {}
}

// Implement using Layer.scoped
const ConnectionPoolLive: Layer.Layer<ConnectionPool, PoolError, Config> = // todo()
// Acquire: create connections, log "Pool created with N connections"
// Release: close all connections, log "Pool closed, N connections released"

// Run a program and observe the logs:
const demo = Effect.gen(function* () {
  const pool = yield* ConnectionPool;
  const conn = yield* pool.getConnection;
  yield* Effect.log("Doing work with connection...");
  yield* pool.releaseConnection(conn);
}).pipe(
  Effect.provide(ConnectionPoolLive.pipe(Layer.provide(ConfigLive)))
);

// Expected log output:
// Pool created with 5 connections
// Doing work with connection...
// Pool closed, 5 connections released
```

### Exercise 6.5: Shared Dependencies

Build a layer graph where two services share a dependency. Verify that the shared dependency is only constructed once.

```typescript
class Logger extends Context.Tag("Logger")<
  Logger,
  { readonly log: (msg: string) => Effect.Effect<void> }
>() {}

class ServiceA extends Context.Tag("ServiceA")<
  ServiceA,
  { readonly doA: Effect.Effect<string> }
>() {}

class ServiceB extends Context.Tag("ServiceB")<
  ServiceB,
  { readonly doB: Effect.Effect<string> }
>() {}

// Logger layer that logs its own construction
const LoggerLive: Layer.Layer<Logger> = Layer.effect(
  Logger,
  Effect.gen(function* () {
    yield* Effect.log(">>> Logger constructed <<<"); // Should appear only ONCE
    return { log: (msg) => Effect.log(msg) };
  })
);

// ServiceA and ServiceB both depend on Logger
const ServiceALive: Layer.Layer<ServiceA, never, Logger> = // todo()
const ServiceBLive: Layer.Layer<ServiceB, never, Logger> = // todo()

// Compose so that Logger is shared
const AppLayer: Layer.Layer<ServiceA | ServiceB> = // todo()
// Hint: Layer.provide feeds Logger into both. Memoization ensures one construction.

// Verify: run a program using both ServiceA and ServiceB
// ">>> Logger constructed <<<" should appear exactly once in the output
```

### Exercise 6.6 (Challenge): Dynamic Layer from Configuration

Build a system where the layer graph itself depends on configuration. For example, the config specifies whether to use an in-memory store or a real database.

```typescript
class StorageBackend extends Context.Tag("StorageBackend")<
  StorageBackend,
  {
    readonly read: (key: string) => Effect.Effect<string | null>;
    readonly write: (key: string, value: string) => Effect.Effect<void>;
  }
>() {}

// Two implementations
const InMemoryStorageLive: Layer.Layer<StorageBackend> = // todo()
const DatabaseStorageLive: Layer.Layer<StorageBackend, DbError, Config> = // todo()

// Choose based on config
const StorageBackendLive: Layer.Layer<StorageBackend, DbError, Config> =
  Layer.effect(
    StorageBackend,
    Effect.gen(function* () {
      const config = yield* Config;
      // todo() — how do you choose between implementations?
      // Hint: you can't yield* a Layer, but you can build the service
      //       implementation inline based on the config value
    })
  );
```

*This exercise reveals an important nuance: Layers are static recipes composed at build time, but the service implementation within a Layer can make dynamic decisions. The Layer graph shape is fixed; the behavior inside each node can vary.*

## Summary

`Layer` is Effect's dependency injection system — a composable, type-safe way to wire services together. Each service declares its interface with `Context.Tag`. Each `Layer` describes how to build a service from its dependencies. The compiler tracks unsatisfied requirements in the `R` parameter and refuses to run until everything is provided.

This gives you:

- **Compile-time dependency verification** — no "service not found" at runtime
- **Testability by construction** — swap layers, not import mocks
- **Resource management built in** — `Layer.scoped` ties resource lifetimes to the application
- **Memoization** — shared dependencies are constructed once
- **Composition** — layers combine with `provide`, `merge`, and `provideMerge`

The `Layer` is the last major structural piece of Effect. With `Effect` (computation), `Scope` (resources), and `Layer` (dependencies), you have the full toolkit for structuring a distributed application. Everything that follows — concurrency, streams, scheduling — builds on this foundation.

## Quick Reference

```typescript
import { Effect, Context, Layer } from "effect";

// --- Declare a service ---
class MyService extends Context.Tag("MyService")<
  MyService,
  { readonly doSomething: Effect.Effect<string> }
>() {}

// --- Access a service ---
Effect.gen(function* () {
  const svc = yield* MyService;        // Adds MyService to R
  return yield* svc.doSomething;
})

// --- Build a layer ---
Layer.succeed(tag, implementation)      // Constant value
Layer.effect(tag, effect)               // Effectful construction
Layer.scoped(tag, scopedEffect)         // With resource cleanup
Layer.function(tag, fn)                 // Synchronous factory

// --- Compose layers ---
layer.pipe(Layer.provide(dependency))   // Satisfy a requirement
Layer.merge(layerA, layerB)             // Produce both services
layer.pipe(Layer.provideMerge(dep))     // Satisfy AND keep available

// --- Provide to a program ---
effect.pipe(Effect.provide(layer))      // Satisfy R, get Effect<A, E>

// --- Testing ---
Layer.succeed(tag, mockImplementation)  // Simple mock
Layer.effect(tag, testSetupEffect)      // Mock with setup logic

// --- Layer lifecycle ---
Layer.fresh(layer)                      // Disable memoization
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 7 — Concurrency and Fibers, where we discover that Effect's fiber system gives us structured concurrency — child computations that can't outlive their parents, safe cancellation, and concurrent coordination primitives that are purely functional.*
