# Chapter 5: Resource Management and Scoping

> **Building on:** This chapter uses `Effect.gen`, `pipe`, typed errors (`catchTag`, `catchAll`), and `Data.TaggedError` from Chapters 3–4. Key types so far: `Option<A>`, `Either<E, A>`, `Effect<A, E, R>`.

## The Resource Problem

Every distributed application manages resources that must be cleaned up: database connections, file handles, network sockets, locks, temporary files, subscription handles. The pattern is always the same — acquire, use, release — and the challenge is always the same: guaranteeing release even when "use" fails.

Here's a function that reads a configuration file, connects to a database, and queries some data:

```typescript
async function loadUserData(configPath: string): Promise<UserData[]> {
  const file = await fs.open(configPath, "r");
  try {
    const content = await file.readFile("utf-8");
    const config = JSON.parse(content);

    const db = await createConnection(config.dbUrl);
    try {
      const users = await db.query("SELECT * FROM users");
      return users;
    } finally {
      await db.close(); // What if this throws?
    }
  } finally {
    await file.close(); // What if the db.close() above threw and this also throws?
  }
}
```

This is *almost* correct. But notice the problems:

**Nested try/finally doesn't compose.** Every new resource adds a nesting level. With three resources, you have three levels of indentation. With five — a realistic number for a distributed service that needs a config, a database, an HTTP client, a message queue, and a metrics reporter — the code becomes a sideways pyramid.

**Cleanup errors are swallowed.** If `db.close()` throws, the `finally` block for `file.close()` still runs — but the error from `db.close()` is lost. If *both* throw, one error silently disappears. In a distributed system, cleanup failures (failing to release a distributed lock, failing to deregister from a service registry) can cause cascading problems.

**Resource lifetimes are implicit.** Nothing in the type system tells you that `db` must be closed, or that it's only valid within a certain scope. You could accidentally return the connection and use it after `close()` — a use-after-free bug, caught only at runtime.

**Resources can't be composed as values.** You can't write a function that returns "a database connection that will be cleaned up" as a composable building block. Each callsite must manage its own try/finally.

Effect solves all four problems with two ideas: `acquireRelease` (the bracket pattern) and `Scope` (structured resource lifetimes).

## acquireRelease: The Bracket Pattern

The core idea: pair every acquisition with its release, and let Effect guarantee the release runs — even on failure, even on interruption.

```typescript
import { Effect } from "effect";

class DatabaseConnection {
  constructor(readonly url: string) {}
  query(sql: string) { /* ... */ }
  close() { /* ... */ }
}

class DbConnectionError {
  readonly _tag = "DbConnectionError" as const;
  constructor(readonly url: string, readonly cause: unknown) {}
}

const makeDbConnection = (url: string) =>
  Effect.acquireRelease(
    // Acquire: create the resource
    Effect.tryPromise({
      try: () => createConnection(url),
      catch: (e) => new DbConnectionError(url, e),
    }),
    // Release: clean up, guaranteed to run
    (connection) => Effect.sync(() => connection.close()).pipe(
      Effect.catchAll((e) => Effect.logError("Failed to close DB connection", e))
    )
  );
```

`Effect.acquireRelease` takes two arguments:

1. **Acquire** — an Effect that creates the resource. If this fails, release never runs (nothing to clean up).
2. **Release** — a function from the resource to an Effect that cleans it up. This runs *no matter what* — success, failure, interruption, defect.

The return type is `Effect<DatabaseConnection, DbConnectionError, Scope>`. Notice the third parameter — `Scope`. This is the key: the resource is *tied to a scope*. When the scope closes, the release runs. We'll explore `Scope` in detail shortly.

### Using a Scoped Resource

To use a scoped resource, you need to provide a `Scope`. The simplest way is `Effect.scoped`:

```typescript
const program = Effect.scoped(
  Effect.gen(function* () {
    const db = yield* makeDbConnection("postgres://localhost/mydb");
    const users = yield* Effect.tryPromise({
      try: () => db.query("SELECT * FROM users"),
      catch: (e) => new QueryError(String(e)),
    });
    return users;
  })
);
// Type: Effect<User[], DbConnectionError | QueryError>
// Note: Scope is gone from R — it's been provided by Effect.scoped
```

`Effect.scoped` creates a scope, runs the effect within it, and closes the scope when done. When the scope closes, all resources acquired within it are released — in reverse order of acquisition.

The beauty: `db` is only valid inside the `Effect.scoped` block. The type system doesn't let it escape. You can't accidentally use a closed connection.

## Composing Multiple Resources

Here's where the approach shines. Need three resources? Just `yield*` them:

```typescript
const program = Effect.scoped(
  Effect.gen(function* () {
    const config = yield* makeConfigReader("/etc/myservice/config.yaml");
    const db = yield* makeDbConnection(config.dbUrl);
    const queue = yield* makeMessageQueue(config.queueUrl);

    // Use all three — if anything fails, all are cleaned up
    const data = yield* db.query("SELECT * FROM outbox");
    for (const row of data) {
      yield* queue.publish(row);
    }
  })
);
```

No nesting. No pyramid. Three resources, three lines of acquisition, and Effect guarantees they're all released in reverse order: queue first, then database, then config reader. If the query fails after the queue is connected, both are cleaned up. If the queue publish fails, all three are cleaned up.

Compare this to the nested try/finally version:

```typescript
// The try/finally version of the same logic
async function manual() {
  const config = await openConfigReader("/etc/myservice/config.yaml");
  try {
    const db = await createConnection(config.dbUrl);
    try {
      const queue = await connectQueue(config.queueUrl);
      try {
        const data = await db.query("SELECT * FROM outbox");
        for (const row of data) {
          await queue.publish(row);
        }
      } finally {
        await queue.disconnect();
      }
    } finally {
      await db.close();
    }
  } finally {
    config.close();
  }
}
```

The Effect version is flat. The manual version is a pyramid. And the Effect version handles cleanup errors, interruption, and concurrent cleanup correctly. The manual version doesn't.

## Scope: Structured Resource Lifetimes

`Scope` is a first-class value representing a resource lifetime. When you create a scoped resource with `acquireRelease`, you're saying "this resource lives as long as the scope it's attached to." When the scope ends, the resource is released.

This gives us structured lifetimes — the same principle as structured concurrency (which we'll explore in Chapter 7). A resource belongs to a scope. A scope is bounded. Resources can't outlive their scope.

### How Scope Works

```typescript
// acquireRelease attaches a finalizer to the current Scope
// When the Scope closes, all finalizers run in reverse order

const resource = Effect.acquireRelease(
  acquire, // Runs when the effect is executed
  release  // Runs when the enclosing Scope closes
);

// Effect.scoped creates and closes a Scope:
Effect.scoped(
  Effect.gen(function* () {
    // A scope exists here
    const r1 = yield* resource1; // Finalizer 1 attached
    const r2 = yield* resource2; // Finalizer 2 attached
    yield* useResources(r1, r2);
    // Scope closes here → finalizer 2 runs, then finalizer 1
  })
);
```

### addFinalizer: Attach Custom Cleanup

Sometimes you don't have a resource to acquire — you just want to run cleanup code when the scope closes:

```typescript
import { Effect, Scope } from "effect";

const withTempDirectory = (prefix: string) =>
  Effect.gen(function* () {
    const dir = yield* Effect.sync(() => fs.mkdtempSync(prefix));

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        fs.rmSync(dir, { recursive: true, force: true });
      }).pipe(
        Effect.catchAll((e) =>
          Effect.logWarning(`Failed to clean up temp dir ${dir}: ${e}`)
        )
      )
    );

    return dir;
  });

// Usage
const program = Effect.scoped(
  Effect.gen(function* () {
    const tmpDir = yield* withTempDirectory("/tmp/myservice-");
    yield* writeFiles(tmpDir);
    yield* processFiles(tmpDir);
    // tmpDir is automatically removed when scope closes
  })
);
```

`Effect.addFinalizer` attaches a cleanup function to the current scope. It's useful for side-effectful setup that needs corresponding teardown — registering/deregistering with a service registry, creating/removing temp files, starting/stopping a background process.

### Finalizer Ordering and the Exit Value

Finalizers receive an `Exit` value that tells them *why* the scope is closing:

```typescript
const makeConnection = (url: string) =>
  Effect.acquireRelease(
    connect(url),
    (conn, exit) =>
      exit._tag === "Success"
        ? gracefulDisconnect(conn)        // Clean shutdown
        : immediateDisconnect(conn).pipe( // Something failed — disconnect fast
            Effect.tap(() => Effect.logWarning(`Abrupt disconnect from ${url}`))
          )
  );
```

The `exit` parameter is `Exit<unknown, unknown>` — success or failure. This lets you choose different cleanup strategies: graceful shutdown on success, fast cleanup on failure. For a distributed system, this matters — you might want to flush pending messages on graceful shutdown but skip the flush if the connection is already broken.

## Resources as Values: Building a Service

One of Effect's most powerful ideas is that resources are *values* — descriptions of acquisition and release that can be passed around, composed, and provided to other code. This connects directly to `Layer`, which we'll cover in Chapter 6. For now, let's see the pattern.

```typescript
// A resource is just an Effect that requires Scope
const makeHttpClient = (
  baseUrl: string
): Effect.Effect<HttpClient, HttpClientError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => createHttpClient({ baseUrl, keepAlive: true }),
      catch: (e) => new HttpClientError(String(e)),
    }),
    (client) =>
      Effect.promise(() => client.destroy()).pipe(
        Effect.catchAll(() => Effect.void)
      )
  );

// A higher-level resource that depends on another resource
const makeServiceRegistry = (
  client: HttpClient
): Effect.Effect<ServiceRegistry, RegistryError, Scope.Scope> =>
  Effect.acquireRelease(
    registerSelf(client).pipe(
      Effect.map((registration) => new ServiceRegistry(client, registration))
    ),
    (registry) => deregisterSelf(registry).pipe(
      Effect.catchAll((e) =>
        Effect.logError(`Failed to deregister: ${e}`)
      )
    )
  );
```

Notice `makeServiceRegistry` takes an `HttpClient` and returns a scoped `ServiceRegistry`. The service registry *uses* the HTTP client during its lifetime. When the scope closes, the registry deregisters (using the client), and then the client is destroyed — in the correct order.

```typescript
const program = Effect.scoped(
  Effect.gen(function* () {
    const client = yield* makeHttpClient("https://registry.cluster.local");
    const registry = yield* makeServiceRegistry(client);

    // Both are live and connected
    yield* registry.announceReady();
    yield* runMainLoop(registry);
    // Scope closes:
    //   1. registry deregisters (using client — still alive)
    //   2. client is destroyed
  })
);
```

The reverse-order cleanup is automatic and correct. The registry can use the client during its cleanup because the client hasn't been destroyed yet. This is a common pattern in distributed services: you need to deregister from a discovery service (using an HTTP client) before shutting down the HTTP client.

## A Complete Example: Multi-Resource Pipeline

Let's build a realistic data pipeline that reads events from a message queue, processes them with a database, and publishes results to another queue. Every resource must be cleaned up on failure.

```typescript
import { Effect, pipe } from "effect";

// --- Resource constructors ---
const makeConfig = Effect.acquireRelease(
  Effect.try({
    try: () => loadConfigFromDisk("/etc/pipeline/config.json"),
    catch: (e) => new ConfigError(String(e)),
  }),
  () => Effect.log("Config released")
);

const makeDatabase = (url: string) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => createPool({ url, maxConnections: 10 }),
      catch: (e) => new DatabaseError("connect", String(e)),
    }),
    (pool) =>
      Effect.tryPromise({
        try: () => pool.end(),
        catch: () => undefined as never // Log but don't fail
      }).pipe(
        Effect.catchAll(() => Effect.log("Pool close failed, continuing"))
      )
  );

const makeConsumer = (brokerUrl: string, topic: string) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => createConsumer({ brokerUrl, topic, groupId: "pipeline" }),
      catch: (e) => new QueueError("consumer", String(e)),
    }),
    (consumer) =>
      Effect.promise(() => consumer.disconnect()).pipe(
        Effect.catchAll(() => Effect.void)
      )
  );

const makeProducer = (brokerUrl: string) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => createProducer({ brokerUrl }),
      catch: (e) => new QueueError("producer", String(e)),
    }),
    (producer) =>
      Effect.promise(() => producer.disconnect()).pipe(
        Effect.catchAll(() => Effect.void)
      )
  );

// --- The pipeline ---
const pipeline = Effect.scoped(
  Effect.gen(function* () {
    // Acquire all resources — cleanup is automatic
    const config = yield* makeConfig;
    const db = yield* makeDatabase(config.dbUrl);
    const consumer = yield* makeConsumer(config.brokerUrl, "events");
    const producer = yield* makeProducer(config.brokerUrl);

    yield* Effect.log("Pipeline started, all resources acquired");

    // Process events
    yield* pipe(
      consumeEvents(consumer),
      Effect.flatMap((event) =>
        Effect.gen(function* () {
          const enriched = yield* enrichFromDb(db, event);
          yield* publishResult(producer, enriched);
        })
      ),
      Effect.forever // Keep processing until interrupted
    );

    // If we get here (or if anything fails):
    // 1. producer disconnects
    // 2. consumer disconnects
    // 3. database pool closes
    // 4. config released
  })
);
```

Four resources, one flat block, guaranteed cleanup in reverse order. The `Effect.forever` keeps the pipeline running until it's interrupted (e.g., by a shutdown signal). When interrupted, the scope closes and all resources clean up. No try/finally. No nesting. No missed cleanups.

## Connecting to the Pattern

Resources follow the same "descriptions first, execution later" principle as everything in Effect:

- `Effect.acquireRelease(acquire, release)` is a *description* of a resource lifecycle — it doesn't acquire anything until run.
- `Effect.scoped(effect)` is a *description* of a scope boundary — it doesn't create the scope until run.
- `Effect.addFinalizer(cleanup)` is a *description* of cleanup logic — it doesn't attach anything until run.

And the familiar toolkit applies. Resources are Effects, so you can `map`, `flatMap`, and `pipe` them:

```typescript
// Transform the resource value
const makePort = pipe(
  makeConfig,
  Effect.map((config) => config.listenPort)
);

// Chain resource-producing effects
const makeEnrichedDb = pipe(
  makeConfig,
  Effect.flatMap((config) => makeDatabase(config.dbUrl))
);
```

This is the same composition we've used on `Option`, `Either`, and `Effect` throughout this series. Resources aren't special — they're Effects with a `Scope` requirement.

## Exercises

### Exercise 5.1: Your First Scoped Resource

Create a scoped resource representing a temporary file. The file should be created on acquisition and deleted on release.

```typescript
import { Effect, Scope } from "effect";
import * as fs from "fs";

class TempFileError {
  readonly _tag = "TempFileError" as const;
  constructor(readonly operation: string, readonly cause: string) {}
}

interface TempFile {
  readonly path: string;
  readonly write: (content: string) => Effect.Effect<void, TempFileError>;
  readonly read: Effect.Effect<string, TempFileError>;
}

const makeTempFile = (
  name: string
): Effect.Effect<TempFile, TempFileError, Scope.Scope> => {
  // todo()
  // Acquire: create the file, return a TempFile object
  // Release: delete the file (log if deletion fails)
};

// Use it:
const program = Effect.scoped(
  Effect.gen(function* () {
    const file = yield* makeTempFile("scratch.txt");
    yield* file.write("hello distributed world");
    const content = yield* file.read;
    return content;
    // File is deleted here, even if write or read failed
  })
);
```

### Exercise 5.2: Compose Two Resources

Build two resources — a configuration reader and a logger that depends on the configuration — and compose them in a single scope.

```typescript
interface AppConfig {
  readonly logLevel: string;
  readonly logFile: string;
}

interface Logger {
  readonly info: (msg: string) => Effect.Effect<void>;
  readonly error: (msg: string) => Effect.Effect<void>;
}

const makeConfigReader = (
  path: string
): Effect.Effect<AppConfig, ConfigError, Scope.Scope> => {
  // todo() — acquire: read and parse file; release: log "config released"
};

const makeLogger = (
  config: AppConfig
): Effect.Effect<Logger, LoggerError, Scope.Scope> => {
  // todo() — acquire: open log file; release: flush and close file
};

// Compose them:
const program = Effect.scoped(
  Effect.gen(function* () {
    const config = yield* makeConfigReader("/etc/app/config.json");
    const logger = yield* makeLogger(config);
    yield* logger.info("Application started");
    // todo() — what order are they released?
  })
);
```

### Exercise 5.3: Conditional Cleanup

Create a resource where the cleanup behavior depends on whether the scope closed due to success or failure. Use the `Exit` parameter in the release function.

```typescript
import { Effect, Exit } from "effect";

interface DistributedLock {
  readonly key: string;
  readonly release: () => Promise<void>;
  readonly forceRelease: () => Promise<void>;
}

const makeDistributedLock = (
  key: string
): Effect.Effect<DistributedLock, LockError, Scope.Scope> => {
  // todo()
  // Acquire: obtain the distributed lock
  // Release:
  //   - On success: graceful release (flush pending operations, then release)
  //   - On failure: force release (skip flush, release immediately)
  // Hint: the release function receives (resource, exit) where exit tells you why
};
```

### Exercise 5.4: Resource Pool

Build a simple connection pool as a scoped resource. The pool creates N connections on acquisition and closes them all on release.

```typescript
interface ConnectionPool {
  readonly acquire: Effect.Effect<Connection, PoolExhaustedError>;
  readonly release: (conn: Connection) => Effect.Effect<void>;
  readonly size: Effect.Effect<number>;
}

class PoolExhaustedError {
  readonly _tag = "PoolExhaustedError" as const;
}

const makeConnectionPool = (
  url: string,
  poolSize: number
): Effect.Effect<ConnectionPool, ConnectionError, Scope.Scope> => {
  // todo()
  // Acquire: create poolSize connections, store in a mutable array or use Ref
  // Provide acquire/release that take from and return to the pool
  // Release: close all connections
  //
  // Hint: for a simple version, use a mutable array.
  // For a concurrent-safe version (preview of Chapter 7), use Effect.Ref
};
```

### Exercise 5.5: Multi-Resource Pipeline

Build a data export pipeline that:
1. Reads configuration (scoped)
2. Connects to a source database (scoped)
3. Opens an output file (scoped)
4. Reads all rows from the database and writes them to the file
5. Cleans up everything on success or failure

```typescript
const exportData = (
  configPath: string,
  outputPath: string,
  query: string
): Effect.Effect<number, ExportError> => {
  // todo()
  // Use Effect.scoped with three acquireRelease resources
  // Return the number of rows exported
  // Make sure resources are released in the correct order
};
```

### Exercise 5.6 (Challenge): Nested Scopes

Sometimes you need a resource whose lifetime is shorter than the outer scope. Create a program where:
- An HTTP client lives for the entire program
- For each request, a temporary trace context is created and cleaned up after the request completes

```typescript
const server = Effect.scoped(
  Effect.gen(function* () {
    const client = yield* makeHttpClient("https://api.cluster.local");

    // Handle 3 requests — each gets its own trace context
    for (const req of requests) {
      // Each iteration should create and clean up its own trace context
      yield* Effect.scoped(
        Effect.gen(function* () {
          const trace = yield* makeTraceContext(req.traceId);
          // todo() — process request using client and trace
          // trace is cleaned up here, but client is still alive
        })
      );
    }
    // client is cleaned up here
  })
);

// Implement makeTraceContext:
const makeTraceContext = (
  traceId: string
): Effect.Effect<TraceContext, TraceError, Scope.Scope> => {
  // todo()
  // Acquire: register trace span with the tracing system
  // Release: close the span and report timing
};
```

*Hint: Nested `Effect.scoped` calls create nested scopes. The inner scope closes before the outer one. This is structured resource management — the same idea as structured concurrency, which we'll cover in Chapter 7.*

## Summary

Resources in Effect are values — descriptions of acquisition and release, composed with the same `map`/`flatMap` toolkit as everything else. `Effect.acquireRelease` pairs creation with cleanup, and `Scope` ensures cleanup runs when the scope closes — in reverse order, even on failure or interruption.

This solves the distributed-systems resource problem: you can compose five resources in a flat block, and Effect guarantees correct, ordered cleanup regardless of how the program terminates.

| Concept | Traditional TypeScript | Effect-TS |
|---|---|---|
| Single resource | `try/finally` | `acquireRelease` + `scoped` |
| Multiple resources | Nested `try/finally` | Multiple `yield*` in one `scoped` |
| Cleanup on failure | Manual, error-prone | Automatic, guaranteed |
| Cleanup order | Manual (easy to get wrong) | Reverse acquisition order (automatic) |
| Resource lifetime | Implicit, runtime bugs | `Scope` type parameter, compile-time |
| Conditional cleanup | Extra flags and branches | `Exit` parameter in release |
| Composing resources | Not possible as values | `map`, `flatMap`, `pipe` |

## Quick Reference

```typescript
import { Effect, Scope } from "effect";

// --- Core resource pattern ---
Effect.acquireRelease(
  acquire,                              // Effect that creates the resource
  (resource, exit) => release           // Effect that cleans up; exit tells you why
)
// Returns: Effect<Resource, AcquireError, Scope>

// --- Running scoped resources ---
Effect.scoped(effect)                   // Create a scope, run, close scope
// Removes Scope from R, releases all resources on completion

// --- Adding cleanup to current scope ---
Effect.addFinalizer((exit) => cleanup)  // Attach cleanup to enclosing scope

// --- Nested scopes ---
Effect.scoped(                          // Outer scope
  Effect.gen(function* () {
    const outer = yield* outerResource;
    yield* Effect.scoped(               // Inner scope
      Effect.gen(function* () {
        const inner = yield* innerResource;
        // inner released here
      })
    );
    // outer released here
  })
)

// --- Resource composition (same toolkit as always) ---
pipe(resource, Effect.map(f))           // Transform the resource
pipe(resource, Effect.flatMap(f))       // Chain to another scoped effect
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 6 — Dependency Injection with Layers, where we discover that `Layer` is the natural extension of scoped resources — a composable, testable way to wire up your entire application's dependency graph, with the type system ensuring nothing is missing.*
