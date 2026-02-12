# Chapter 3: The Effect Type — Core Operations

> **Building on:** This chapter uses `Option<A>`, `Either<E, A>`, `pipe`, `map`, `flatMap`, and `zip` from Chapters 1–2. You should be comfortable reading pipe chains and understand how `flatMap` short-circuits on failure.

## From Either to Effect

In Chapter 2, we built `Either<E, A>` and discovered it gives us typed errors and compositional chaining. But `Either` is synchronous and pure — it can't represent a network call, a database query, or reading the clock. In distributed applications, *almost everything* is effectful.

We could wrap `Either` in a `Promise`:

```typescript
async function fetchNode(id: string): Promise<Either<NetworkError, NodeRecord>> {
  try {
    const response = await fetch(`/nodes/${id}`);
    if (!response.ok) return left(new NetworkError(response.status));
    return right(await response.json());
  } catch (e) {
    return left(new NetworkError(0));
  }
}
```

But now composition breaks down. To chain two such functions, we need to unwrap the `Promise`, check the `Either`, then wrap again:

```typescript
async function fetchNodeConfig(
  nodeId: string
): Promise<Either<NetworkError | NotFoundError, NodeConfig>> {
  const nodeResult = await fetchNode(nodeId);
  if (nodeResult._tag === "Left") return nodeResult;
  const configResult = await fetchConfig(nodeResult.right.configId);
  return configResult;
}
```

We're back to manual plumbing. The typed errors are there, but the composition ergonomics are gone. Every function is a new layer of `await` + `if Left return`.

`Effect<A, E, R>` solves this by unifying the value channel, the error channel, and the async execution into a single composable type. It's `Either` that can also do I/O — plus a third type parameter `R` for dependencies. Let's build up to it.

## Constructors: Building Effects

Every Effect starts with a constructor — a way to lift a value, an error, or a side-effectful operation into the `Effect` type.

### Pure values

```typescript
import { Effect } from "effect";

// An Effect that always succeeds with 42
const success: Effect.Effect<number> = Effect.succeed(42);

// An Effect that always fails with an error
class NotFoundError {
  readonly _tag = "NotFoundError" as const;
  constructor(readonly id: string) {}
}
const failure: Effect.Effect<never, NotFoundError> = 
  Effect.fail(new NotFoundError("abc"));
```

Notice the types. `Effect.succeed(42)` has type `Effect<number, never, never>` — it always succeeds with a `number`, never fails (`never` in the error channel), and requires nothing to run (`never` in the requirements channel). `Effect.fail` is the mirror: `Effect<never, NotFoundError, never>`.

### Wrapping synchronous side effects

```typescript
// Effect.sync wraps a lazy, synchronous computation
const now: Effect.Effect<number> = Effect.sync(() => Date.now());

// The thunk is not called until the Effect is run
// This is referentially transparent — 'now' is a description
```

`Effect.sync` takes a thunk `() => A` and wraps it. The thunk isn't called when you create the Effect — only when you run it. This is the "effects as data" principle from Chapter 1.

### Wrapping Promises

```typescript
// Effect.tryPromise wraps an async operation with typed errors
const fetchNode = (id: string): Effect.Effect<NodeRecord, NetworkError> =>
  Effect.tryPromise({
    try: () => fetch(`/nodes/${id}`).then((r) => r.json() as Promise<NodeRecord>),
    catch: (unknown) => new NetworkError(String(unknown)),
  });
```

`Effect.tryPromise` takes a `try` function that returns a `Promise` and a `catch` function that maps the unknown rejection into a typed error. The result is a fully typed `Effect<NodeRecord, NetworkError>`.

### Wrapping callbacks and other patterns

```typescript
// Effect.async for callback-based APIs
const readMessage = Effect.async<string, ConnectionError>((resume) => {
  socket.onmessage = (event) => resume(Effect.succeed(event.data));
  socket.onerror = (err) => resume(Effect.fail(new ConnectionError(err)));
});

// Effect.promise for Promises you know won't reject
const delay = (ms: number): Effect.Effect<void> =>
  Effect.promise(() => new Promise((resolve) => setTimeout(resolve, ms)));
```

Here's a summary of the constructor landscape:

| Constructor | Input | Error handling | Use when |
|---|---|---|---|
| `Effect.succeed(value)` | Pure value | Can't fail | You have a value |
| `Effect.fail(error)` | Error value | Always fails | You have an error |
| `Effect.sync(() => a)` | Sync thunk | May throw (becomes defect) | Synchronous side effect |
| `Effect.try(() => a)` | Sync thunk | Catches exceptions | Sync code that throws |
| `Effect.tryPromise({ try, catch })` | Async thunk | Maps rejection to typed error | Async code |
| `Effect.promise(() => p)` | Async thunk | Rejection becomes defect | Promise that shouldn't reject |
| `Effect.async((resume) => ...)` | Callback | You control it | Callback-based APIs |

## The Familiar Toolkit: map, flatMap, zip, all

Now the payoff. The same operations we built on `Option` and `Either` exist on `Effect`, with the same semantics.

### map — Transform the Success Value

```typescript
const port: Effect.Effect<number, ParseError> = parsePort("8080");

const doubled: Effect.Effect<number, ParseError> = pipe(
  port,
  Effect.map((p) => p * 2)
);
```

If `port` fails, `map` propagates the error unchanged. If it succeeds, the function runs. Same as `Option.map` and `Either.map`.

### flatMap — Chain Effects

```typescript
const fetchAndValidate = (nodeId: string) =>
  pipe(
    fetchNode(nodeId),                          // Effect<NodeRecord, NetworkError>
    Effect.flatMap((node) => validateNode(node)) // Effect<ValidNode, ValidationError>
  );
// Result: Effect<ValidNode, NetworkError | ValidationError>
```

Look at the error type. `flatMap` unions the error channels automatically: `NetworkError | ValidationError`. This is the typed error composition we saw on `Either`, now working across async boundaries.

If `fetchNode` fails, `validateNode` never runs. Short-circuiting — the same behavior as `Either.flatMap` and `Option.flatMap`.

### zip — Combine Independent Effects

```typescript
const both = pipe(
  fetchNode("node-a"),
  Effect.zip(fetchNode("node-b"))
);
// Effect<[NodeRecord, NodeRecord], NetworkError>
```

`zip` runs both effects and combines their results into a tuple. Error types are unioned. By default, `zip` executes sequentially (left then right), but we'll see concurrent variants in Chapter 7.

### all — Combine Many Effects

```typescript
const nodes = Effect.all([
  fetchNode("node-a"),
  fetchNode("node-b"),
  fetchNode("node-c"),
]);
// Effect<[NodeRecord, NodeRecord, NodeRecord], NetworkError>

// Also works with objects:
const config = Effect.all({
  host: resolveHost("primary"),
  port: parsePort("8080"),
  timeout: Effect.succeed(5000),
});
// Effect<{ host: string; port: number; timeout: number }, HostError | ParseError>
```

`Effect.all` is the workhorse for combining independent effects. It takes an array or object of effects and returns an effect of the combined results. We'll revisit it with concurrency options in Chapter 7.

## Effect.gen — Sequential Code That Reads Like Async/Await

Chains of `pipe` and `flatMap` work, but deeply nested pipelines get hard to read. `Effect.gen` gives us a generator-based syntax that looks like imperative code while preserving all the benefits of typed effects:

```typescript
// With flatMap chains:
const registerNode = (request: RegistrationRequest) =>
  pipe(
    validateRequest(request),
    Effect.flatMap((validated) =>
      pipe(
        checkUniqueness(validated.nodeId),
        Effect.flatMap(() =>
          pipe(
            writeRecord(validated),
            Effect.flatMap((record) =>
              pipe(
                notifyPeers(record),
                Effect.map(() => record)
              )
            )
          )
        )
      )
    )
  );

// With Effect.gen — same semantics, readable:
const registerNode = (request: RegistrationRequest) =>
  Effect.gen(function* () {
    const validated = yield* validateRequest(request);
    yield* checkUniqueness(validated.nodeId);
    const record = yield* writeRecord(validated);
    yield* notifyPeers(record);
    return record;
  });
```

Each `yield*` is like an `await` in async/await, but it:

1. Unwraps the success value from the Effect
2. Short-circuits on failure (like `flatMap`)
3. **Tracks the error type** — the resulting Effect's error channel is the union of all yielded effects' errors

The second point is crucial. In async/await, `await` on a rejected promise throws an untyped exception. In `Effect.gen`, `yield*` on a failed effect propagates a *typed* error. The compiler knows exactly what can go wrong.

If you've used Scala's for-comprehensions or Haskell's do-notation, `Effect.gen` is the same idea. It's syntactic sugar for nested `flatMap` calls, made possible by JavaScript generators.

### When to use gen vs. pipe

Both styles are valid. Here's a guideline:

- **Use `Effect.gen`** for sequential workflows where steps depend on previous results. It reads top-to-bottom and makes the "happy path" obvious.
- **Use `pipe`** for transformations, one-liner compositions, and when you want point-free style. It's more concise for simple chains.

```typescript
// pipe shines for simple transforms
const getPort = pipe(
  loadConfig,
  Effect.map((c) => c.port),
  Effect.orElseSucceed(() => 3000)
);

// gen shines for multi-step workflows
const program = Effect.gen(function* () {
  const config = yield* loadConfig;
  const db = yield* connectToDatabase(config.dbUrl);
  const users = yield* db.query("SELECT * FROM users");
  return users;
});
```

They compose together freely — you can `yield*` a piped effect inside `gen`, or `pipe` a `gen` block with additional transformations:

```typescript
const program = Effect.gen(function* () {
  const port = yield* pipe(
    loadConfig,
    Effect.map((c) => c.port),
    Effect.orElseSucceed(() => 3000)
  );
  yield* startServer(port);
});
```

## Typed Errors Compose Automatically

This is the feature that sets Effect apart from `Promise` and `async/await`. Let's trace through the types carefully.

```typescript
class DnsError {
  readonly _tag = "DnsError" as const;
  constructor(readonly hostname: string) {}
}

class ConnectionError {
  readonly _tag = "ConnectionError" as const;
  constructor(readonly url: string) {}
}

class TimeoutError {
  readonly _tag = "TimeoutError" as const;
  constructor(readonly ms: number) {}
}

// Each function declares its specific error
const resolveHost = (name: string): Effect.Effect<string, DnsError> => /* ... */;
const connect = (ip: string): Effect.Effect<Socket, ConnectionError> => /* ... */;
const handshake = (socket: Socket): Effect.Effect<Session, TimeoutError> => /* ... */;

// Chain them — watch the error type
const establish = (hostname: string) =>
  Effect.gen(function* () {
    const ip = yield* resolveHost(hostname);         // may fail: DnsError
    const socket = yield* connect(ip);               // may fail: ConnectionError
    const session = yield* handshake(socket);         // may fail: TimeoutError
    return session;
  });

// TypeScript infers:
// establish: (hostname: string) => Effect<Session, DnsError | ConnectionError | TimeoutError>
```

The compiler computed `DnsError | ConnectionError | TimeoutError` for us. No manual union. No `catch (e: unknown)`. Every caller of `establish` knows exactly the three ways it can fail and can handle them by tag:

```typescript
const resilientEstablish = (hostname: string) =>
  establish(hostname).pipe(
    Effect.catchTag("DnsError", (e) =>
      establish(`fallback-${e.hostname}`)  // Try fallback host
    ),
    Effect.catchTag("TimeoutError", (e) =>
      Effect.fail(new ConnectionError(`Timeout after ${e.ms}ms`))  // Reclassify
    )
    // ConnectionError is still in the error channel — must be handled upstream
  );
```

With `Promise`, the equivalent code catches `unknown` and you're on your own:

```typescript
// Promise version — no type safety on errors
try {
  const session = await establish(hostname);
} catch (e) {
  // e is `unknown`. Is it a DNS error? Connection error? Timeout?
  // A typo in a property name? A null reference? You don't know.
}
```

## A Realistic Example: Multi-Step Service Registration

Let's put it all together with a realistic distributed-systems workflow. We're registering a service with a discovery cluster:

```typescript
import { Effect, pipe } from "effect";

// --- Error types ---
class ValidationError {
  readonly _tag = "ValidationError" as const;
  constructor(readonly field: string, readonly reason: string) {}
}

class RegistryUnavailableError {
  readonly _tag = "RegistryUnavailableError" as const;
  constructor(readonly endpoint: string) {}
}

class DuplicateServiceError {
  readonly _tag = "DuplicateServiceError" as const;
  constructor(readonly serviceId: string) {}
}

class StorageError {
  readonly _tag = "StorageError" as const;
  constructor(readonly operation: string, readonly cause: string) {}
}

// --- Domain types ---
interface ServiceRegistration {
  readonly serviceId: string;
  readonly endpoint: string;
  readonly healthCheckUrl: string;
  readonly metadata: Record<string, string>;
}

interface ServiceRecord extends ServiceRegistration {
  readonly registeredAt: Date;
  readonly ttl: number;
}

// --- Individual steps (implementations omitted for clarity) ---
declare const validateRegistration: (
  input: ServiceRegistration
) => Effect.Effect<ServiceRegistration, ValidationError>;

declare const checkAvailability: (
  serviceId: string
) => Effect.Effect<void, RegistryUnavailableError | DuplicateServiceError>;

declare const persistRecord: (
  registration: ServiceRegistration
) => Effect.Effect<ServiceRecord, StorageError>;

declare const announceToCluster: (
  record: ServiceRecord
) => Effect.Effect<void, RegistryUnavailableError>;

// --- Composed workflow ---
const registerService = (input: ServiceRegistration) =>
  Effect.gen(function* () {
    // Validate input
    const validated = yield* validateRegistration(input);

    // Check the service ID isn't taken
    yield* checkAvailability(validated.serviceId);

    // Persist to local store
    const record = yield* persistRecord(validated);

    // Announce to the cluster — retry up to 3 times on failure
    yield* pipe(
      announceToCluster(record),
      Effect.retry({ times: 3 })
    );

    return record;
  });

// TypeScript infers:
// registerService: (input: ServiceRegistration) => Effect.Effect<
//   ServiceRecord,
//   ValidationError | RegistryUnavailableError | DuplicateServiceError | StorageError,
//   never
// >
```

Read that inferred type. Four distinct error types, automatically tracked. The caller can handle each one differently — retry on `RegistryUnavailableError`, show a message on `DuplicateServiceError`, alert on `StorageError`. The type system ensures nothing is forgotten.

And notice: we used `Effect.retry` without building any retry infrastructure. We just described *what* to retry and *how much*. In Chapter 9, we'll explore `Schedule` — the composable retry/repeat policy system. For now, `{ times: 3 }` is enough.

## Running Effects

An Effect is a description. To get a result, you run it:

```typescript
// Run and get a Promise (most common in applications)
const promise: Promise<ServiceRecord> = Effect.runPromise(registerService(input));

// Run and get a Promise of Either (captures errors as values)
const promiseEither = Effect.runPromiseEither(registerService(input));

// Run synchronously (only works if the Effect has no async steps)
const result: ServiceRecord = Effect.runSync(Effect.succeed(42));

// In a real application, you typically have one runPromise at the entry point:
const main = pipe(
  registerService(input),
  Effect.catchTag("ValidationError", (e) =>
    Effect.fail(new UserFacingError(`Invalid ${e.field}: ${e.reason}`))
  ),
  Effect.catchTag("DuplicateServiceError", (e) =>
    Effect.fail(new UserFacingError(`Service ${e.serviceId} already exists`))
  )
);

Effect.runPromise(main).catch(console.error);
```

The key discipline: compose descriptions as deep as you can, run at the boundary. Your business logic is a pure function that returns an Effect. The `runPromise` call lives in `main` or in your HTTP handler — at the edge.

## Exercises

### Exercise 3.1: Effect Constructors

Create Effects from different sources:

```typescript
import { Effect } from "effect";

// (a) An Effect that succeeds with the string "pong"
const pong: Effect.Effect<string> = // todo()

// (b) An Effect that fails with a TimeoutError
class TimeoutError {
  readonly _tag = "TimeoutError" as const;
  constructor(readonly ms: number) {}
}
const timeout: Effect.Effect<never, TimeoutError> = // todo()

// (c) An Effect that reads the current timestamp
const timestamp: Effect.Effect<number> = // todo()

// (d) An Effect that parses JSON, with typed errors
class JsonParseError {
  readonly _tag = "JsonParseError" as const;
  constructor(readonly input: string, readonly message: string) {}
}
const parseJson = (input: string): Effect.Effect<unknown, JsonParseError> => {
  // todo()
  // Hint: use Effect.try with a catch handler
};
```

### Exercise 3.2: Chaining with flatMap

Build a pipeline that resolves a service endpoint from a configuration. Each step might fail with a distinct error.

```typescript
class ConfigMissingError {
  readonly _tag = "ConfigMissingError" as const;
  constructor(readonly key: string) {}
}

class DnsResolutionError {
  readonly _tag = "DnsResolutionError" as const;
  constructor(readonly hostname: string) {}
}

class HealthCheckError {
  readonly _tag = "HealthCheckError" as const;
  constructor(readonly endpoint: string) {}
}

// Simulate these — use Effect.succeed/Effect.fail to control outcomes
const getConfigValue = (key: string): Effect.Effect<string, ConfigMissingError> => {
  // todo() — return "discovery.local" for key "service.host", fail otherwise
};

const resolveHostname = (host: string): Effect.Effect<string, DnsResolutionError> => {
  // todo() — return "10.0.1.5" for "discovery.local", fail otherwise
};

const checkHealth = (ip: string): Effect.Effect<boolean, HealthCheckError> => {
  // todo() — return true for "10.0.1.5", fail otherwise
};

// Chain all three with pipe + Effect.flatMap
const discoverService: Effect.Effect<
  boolean,
  ConfigMissingError | DnsResolutionError | HealthCheckError
> = // todo()
```

### Exercise 3.3: Rewrite with Effect.gen

Take your `discoverService` from Exercise 3.2 and rewrite it using `Effect.gen`. The result should be identical in behavior and type.

```typescript
const discoverServiceGen: Effect.Effect<
  boolean,
  ConfigMissingError | DnsResolutionError | HealthCheckError
> = Effect.gen(function* () {
  // todo()
});
```

### Exercise 3.4: Combining Independent Effects

Three independent health checks need to run. Use `Effect.all` to combine them.

```typescript
const checkDatabase: Effect.Effect<string, HealthCheckError> =
  Effect.succeed("db: healthy");

const checkCache: Effect.Effect<string, HealthCheckError> =
  Effect.succeed("cache: healthy");

const checkQueue: Effect.Effect<string, HealthCheckError> =
  Effect.fail(new HealthCheckError("queue:5672"));

// (a) Combine all three — what happens when one fails?
const allChecks: Effect.Effect<string[], HealthCheckError> = // todo()

// (b) Run the result and observe the behavior
// Hint: use Effect.runPromiseExit to see the full result
```

### Exercise 3.5: Manual Retry, Then Discover Effect.retry

First, implement retry logic manually using recursion and `Effect.gen`:

```typescript
const retryManual = <A, E>(
  effect: Effect.Effect<A, E>,
  maxAttempts: number
): Effect.Effect<A, E> => {
  // todo()
  // Hint: catch the error, check attempts remaining, recurse or fail
  // Use Effect.catchAll and recursion
};
```

Then replace it with the built-in:

```typescript
import { Effect, Schedule } from "effect";

// Same behavior, one line:
const retryBuiltin = <A, E>(
  effect: Effect.Effect<A, E>,
  maxAttempts: number
): Effect.Effect<A, E> => {
  // todo()
  // Hint: Effect.retry with Schedule.recurs
};
```

Compare the two. What does the built-in give you that the manual version doesn't? (Think about: delay between retries, jitter, composition of retry policies.)

### Exercise 3.6 (Challenge): Error Type Narrowing

Start with an Effect that can fail with three error types. Handle them one at a time and observe how the error type narrows:

```typescript
class AuthError {
  readonly _tag = "AuthError" as const;
  constructor(readonly reason: string) {}
}

class RateLimitError {
  readonly _tag = "RateLimitError" as const;
  constructor(readonly retryAfter: number) {}
}

class ServerError {
  readonly _tag = "ServerError" as const;
  constructor(readonly status: number) {}
}

declare const callApi: Effect.Effect<
  string,
  AuthError | RateLimitError | ServerError
>;

// Handle AuthError — what is the remaining error type?
const step1 = pipe(
  callApi,
  Effect.catchTag("AuthError", (e) => Effect.succeed("unauthorized: using default"))
);
// step1 type: Effect.Effect<string, ???>
// todo() — fill in the error type

// Handle RateLimitError with a retry
const step2 = pipe(
  step1,
  Effect.catchTag("RateLimitError", (e) =>
    pipe(
      Effect.sleep(`${e.retryAfter} seconds`),
      Effect.flatMap(() => callApi) // Note: this reintroduces all three errors!
    )
  )
);
// step2 type: Effect.Effect<string, ???>
// todo() — what's the error type now? Why?

// This exercise reveals an important subtlety about error handling and
// re-introduction of error types. Think carefully about step2's type.
```

## Summary

The `Effect<A, E, R>` type unifies synchronous and asynchronous computation with typed errors and declared dependencies. Its core operations — `map`, `flatMap`, `zip`, `all` — are the same toolkit we built on `Option` and `Either`, now working across async boundaries.

`Effect.gen` gives us sequential syntax that reads like async/await but preserves typed error composition. Every `yield*` unwraps a success or short-circuits with a typed error — the compiler tracks the union automatically.

| Pattern | Option | Either | Effect |
|---|---|---|---|
| Wrap success | `some(a)` | `right(a)` | `Effect.succeed(a)` |
| Represent failure | `none` | `left(e)` | `Effect.fail(e)` |
| Transform | `Option.map` | `Either.map` | `Effect.map` |
| Chain | `Option.flatMap` | `Either.flatMap` | `Effect.flatMap` |
| Short-circuits? | Yes (on None) | Yes (on Left) | Yes (on failure) |
| Error types | None (no info) | `E` (typed) | `E` (typed, auto-unioned) |
| Sequential syntax | — | — | `Effect.gen` + `yield*` |

The pattern is the same. The context grows richer.

## Quick Reference

```typescript
import { Effect, pipe } from "effect";

// --- Constructors ---
Effect.succeed(value)                        // Pure success
Effect.fail(error)                           // Typed failure
Effect.sync(() => sideEffect)                // Sync side effect
Effect.try({ try: () => a, catch: toErr })   // Sync with typed error
Effect.tryPromise({ try, catch })            // Async with typed error
Effect.promise(() => promise)                // Async (rejection = defect)
Effect.async((resume) => { ... })            // Callback-based

// --- Core operations ---
pipe(effect, Effect.map(f))                  // Transform success
pipe(effect, Effect.flatMap(f))              // Chain (auto-unions errors)
Effect.zip(effectA, effectB)                 // Combine two
Effect.all([e1, e2, e3])                     // Combine many (array)
Effect.all({ a: e1, b: e2 })                // Combine many (object)

// --- Generator syntax ---
Effect.gen(function* () {
  const a = yield* effectA;                  // Unwrap or short-circuit
  const b = yield* effectB;
  return f(a, b);
})

// --- Error handling (preview — Chapters 4a–4b go deep) ---
pipe(effect, Effect.catchTag("Tag", handler))
pipe(effect, Effect.catchAll(handler))
pipe(effect, Effect.orElseSucceed(() => fallback))
pipe(effect, Effect.retry({ times: n }))

// --- Running ---
Effect.runPromise(effect)                    // Run → Promise<A>
Effect.runSync(effect)                       // Run → A (sync only)
Effect.runPromiseExit(effect)                // Run → Promise<Exit<A, E>>
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 4a — Error Handling: Tagged Errors and Recovery, where we explore typed errors vs. defects, surgical error handling with `catchTag`, fallback chains, and error transformation.*
