# Chapter 4a: Error Handling — Tagged Errors and Recovery

> **Building on:** This chapter uses `Effect.gen`, `pipe`, `Effect.fail`, `Effect.succeed`, and the `Effect<A, E, R>` type from Chapter 3. You should understand how `flatMap` unions error types and how `yield*` works inside `Effect.gen`.

## Two Kinds of Failure

In distributed systems, not all failures are equal. Some are expected and recoverable — a peer node is temporarily unreachable, a request fails validation, a cache entry has expired. Others are unexpected and fatal — a null pointer dereference, an out-of-memory condition, a logic bug that should never happen.

Most languages smash these together. Java has checked and unchecked exceptions, but in practice everyone catches `Exception` and hopes for the best. TypeScript has only `throw`, and `catch` gives you `unknown`. There's no compile-time distinction between "this network call might time out" and "someone passed null where they shouldn't have."

Effect makes this distinction first-class:

- **Expected errors** (the `E` channel): errors your program anticipates and can recover from. They appear in the type signature. Callers *must* handle them.
- **Defects** (unexpected failures): bugs, invariant violations, things that "shouldn't happen." They don't appear in the `E` type. They propagate as unrecoverable failures.

```typescript
import { Effect } from "effect";

// Expected error — in the E channel, caller sees it in the type
class TimeoutError {
  readonly _tag = "TimeoutError" as const;
  constructor(readonly ms: number) {}
}

const withTimeout: Effect.Effect<Response, TimeoutError> = // ...

// Defect — not in the type, indicates a bug
const parseConfig = (json: string): Effect.Effect<Config> =>
  Effect.try({
    try: () => JSON.parse(json) as Config,
    catch: () => new JsonSyntaxError(json), // This becomes a typed error
  });

// But what if someone passes a non-string? That's a defect — a bug in the caller.
// Effect.die creates an unrecoverable failure:
const assertNonEmpty = (s: string): Effect.Effect<string> =>
  s.length > 0
    ? Effect.succeed(s)
    : Effect.die(new Error("Invariant violated: empty string"));
```

The rule of thumb: **if the caller should handle it, it's an expected error (`Effect.fail`). If it indicates a bug, it's a defect (`Effect.die`).** Network timeouts are expected. Null dereferences are defects. Configuration validation errors are expected. A missing case in a switch statement is a defect.

## Tagged Error Types: The Pattern

In Chapter 3, we defined error types with a `_tag` field. This isn't arbitrary — it's the foundation of Effect's error handling strategy. Tagged error types form a discriminated union that TypeScript can narrow through pattern matching:

```typescript
class DnsError {
  readonly _tag = "DnsError" as const;
  constructor(readonly hostname: string) {}
}

class ConnectionRefusedError {
  readonly _tag = "ConnectionRefusedError" as const;
  constructor(readonly address: string, readonly port: number) {}
}

class TlsHandshakeError {
  readonly _tag = "TlsHandshakeError" as const;
  constructor(readonly reason: string) {}
}

class AuthenticationError {
  readonly _tag = "AuthenticationError" as const;
  constructor(readonly service: string) {}
}

type NetworkError =
  | DnsError
  | ConnectionRefusedError
  | TlsHandshakeError
  | AuthenticationError;
```

Each error type is a class with `readonly _tag` set to a string literal. This gives us three things:

1. **Exhaustive handling.** TypeScript can check we've handled every case.
2. **Selective handling.** We can catch one tag and let others propagate.
3. **Automatic union tracking.** `flatMap` unions the tags, and `catchTag` narrows them.

You can also use `Data.TaggedError` from the `effect` package for a more concise pattern:

```typescript
import { Data } from "effect";

class DnsError extends Data.TaggedError("DnsError")<{
  readonly hostname: string;
}> {}

class ConnectionRefusedError extends Data.TaggedError("ConnectionRefusedError")<{
  readonly address: string;
  readonly port: number;
}> {}
```

`Data.TaggedError` gives you the `_tag`, proper equality, a useful `toString()`, and integration with Effect's error handling — all for free.

## Catching Errors by Tag

`Effect.catchTag` is the surgical tool. It catches one specific error type by its `_tag` and leaves all others in the error channel:

```typescript
declare const connectToService: Effect.Effect<
  Connection,
  DnsError | ConnectionRefusedError | TlsHandshakeError | AuthenticationError
>;

// Handle only DnsError — try a fallback hostname
const withDnsFallback = connectToService.pipe(
  Effect.catchTag("DnsError", (error) =>
    connectToService.pipe(
      // Remap the hostname and retry — but this reintroduces all error types
      Effect.mapError(() => new DnsError(`fallback-${error.hostname}`))
    )
  )
);
// Type: Effect<Connection, DnsError | ConnectionRefusedError | TlsHandshakeError | AuthenticationError>
```

Notice: catching `DnsError` and retrying with `connectToService` reintroduces all four error types. If instead we handle it with a pure fallback, the type narrows:

```typescript
const withDnsDefault = connectToService.pipe(
  Effect.catchTag("DnsError", () =>
    Effect.succeed(defaultConnection)
  )
);
// Type: Effect<Connection, ConnectionRefusedError | TlsHandshakeError | AuthenticationError>
```

`DnsError` is gone from the union. The compiler tracked that we handled it. This is the power of typed errors — you can see at a glance which failure modes remain.

### catchTags — Handle Multiple Tags at Once

```typescript
const resilient = connectToService.pipe(
  Effect.catchTags({
    DnsError: (e) => connectViaIp(e.hostname),
    ConnectionRefusedError: (e) =>
      Effect.fail(new ServiceDownError(e.address)),
    TlsHandshakeError: () => connectInsecure(),
  })
);
// Remaining error type: AuthenticationError | <errors from handlers>
```

Each handler can succeed, fail with new errors, or retry. The resulting error type is the union of unhandled errors plus any new errors introduced by the handlers.

## catchAll and catchSome

Sometimes you want to catch everything, or catch based on a runtime predicate rather than a tag:

```typescript
// catchAll — handle any error in the E channel
const withFallback = connectToService.pipe(
  Effect.catchAll((error) => {
    // error: DnsError | ConnectionRefusedError | TlsHandshakeError | AuthenticationError
    console.error(`Connection failed: ${error._tag}`);
    return Effect.succeed(defaultConnection);
  })
);
// Type: Effect<Connection, never> — all errors handled

// catchSome — handle conditionally, return Option
import { Option } from "effect";

const withPartialRecovery = connectToService.pipe(
  Effect.catchSome((error) => {
    if (error._tag === "DnsError" || error._tag === "ConnectionRefusedError") {
      return Option.some(Effect.succeed(defaultConnection));
    }
    return Option.none(); // Don't handle — let it propagate
  })
);
```

`catchAll` is the nuclear option — it wipes the error channel clean. Use it at the boundary of your application, where you need to convert all remaining errors into a response.

## Transforming the Error Channel

Sometimes you don't want to *handle* an error — you want to *remap* it. Maybe you're building a library and want to wrap low-level errors in a domain-specific type:

### mapError — Transform Every Error

```typescript
class ServiceError {
  readonly _tag = "ServiceError" as const;
  constructor(readonly message: string, readonly cause: unknown) {}
}

// Wrap all network errors into a single ServiceError
const publicApi = connectToService.pipe(
  Effect.mapError((error) => new ServiceError(
    `Connection failed: ${error._tag}`,
    error
  ))
);
// Type: Effect<Connection, ServiceError>
```

`mapError` is like `map` but for the error channel. It transforms `E` without touching `A`.

### mapBoth — Transform Both Channels

```typescript
const transformed = connectToService.pipe(
  Effect.mapBoth({
    onFailure: (error) => new ServiceError(error._tag, error),
    onSuccess: (conn) => conn.sessionId,
  })
);
// Type: Effect<string, ServiceError>
```

## The orElse Family: Fallback Chains

In distributed systems, fallback strategies are essential. Maybe you try the primary endpoint, then the secondary, then a cached result:

```typescript
const connectToPrimary: Effect.Effect<Connection, NetworkError> = // ...
const connectToSecondary: Effect.Effect<Connection, NetworkError> = // ...
const useCachedConnection: Effect.Effect<Connection, CacheError> = // ...

// orElse — try the next effect if the first fails
const resilientConnect = connectToPrimary.pipe(
  Effect.orElse(() => connectToSecondary),
  Effect.orElse(() => useCachedConnection)
);
// Type: Effect<Connection, CacheError>
// Only the last fallback's error type remains

// orElseSucceed — provide a default value on any failure
const alwaysConnect = connectToPrimary.pipe(
  Effect.orElseSucceed(() => localOnlyConnection)
);
// Type: Effect<Connection, never> — can't fail

// orElseFail — replace the error with a different one
const withCleanError = connectToPrimary.pipe(
  Effect.orElseFail(() => new ServiceUnavailableError())
);
// Type: Effect<Connection, ServiceUnavailableError>
```

`orElse` builds a fallback chain. Each fallback can introduce its own errors, but previous errors are discarded — only the final fallback's error type survives. This makes sense: if fallback A failed and we're trying fallback B, we don't care about A's error anymore.

## Exercises

### Exercise 4.1: Define an Error Hierarchy

Design error types for a distributed key-value store. Think about which errors are expected (callers should handle) and which are defects (indicate bugs).

```typescript
// Define these error types with _tag fields:
// - KeyNotFoundError (expected — the key doesn't exist)
// - StaleReadError (expected — the read was from a replica that's behind)
// - QuorumNotReachedError (expected — not enough replicas responded)
// - CorruptedDataError (defect — data on disk is corrupted, a bug or hardware failure)
// - InvariantViolationError (defect — internal logic error)

// todo() — define the types

// Then create an Effect that might fail with any expected error:
declare const readKey: (
  key: string
) => Effect.Effect<string, /* what goes here? */>;
```

### Exercise 4.2: Surgical Error Recovery

Given a multi-step pipeline, handle each error type differently:

```typescript
class NodeUnreachableError {
  readonly _tag = "NodeUnreachableError" as const;
  constructor(readonly nodeId: string) {}
}

class DataCorruptedError {
  readonly _tag = "DataCorruptedError" as const;
  constructor(readonly key: string) {}
}

class ConflictError {
  readonly _tag = "ConflictError" as const;
  constructor(readonly key: string, readonly versions: string[]) {}
}

declare const replicateKey: (
  key: string, targetNode: string
) => Effect.Effect<void, NodeUnreachableError | DataCorruptedError | ConflictError>;

// Handle each error type differently:
// - NodeUnreachableError → retry with a different node from a node list
// - DataCorruptedError → log an alert and fail (this is serious)
// - ConflictError → attempt automatic resolution by picking latest version
const resilientReplicate = (
  key: string, nodes: string[]
): Effect.Effect<void, DataCorruptedError> => {
  // todo()
  // Hint: use catchTag for each, try nodes in sequence with orElse
};
```

## Summary

This chapter covered Effect's typed error system for catching and transforming expected errors. Tagged error types with `_tag` enable surgical recovery — handle one error type while the compiler tracks what's left. The error type narrows with each `catchTag`, giving you compile-time assurance that every failure mode is addressed.

The key tools:
- **`catchTag`** / **`catchTags`**: surgical, per-tag error handling
- **`catchAll`** / **`catchSome`**: broad error handling
- **`mapError`** / **`mapBoth`**: error transformation without handling
- **`orElse`** / **`orElseSucceed`** / **`orElseFail`**: fallback chains

| Tool | What it does | Error type after |
|---|---|---|
| `catchTag("T", f)` | Handle one error by tag | Removes `T`, adds `f`'s errors |
| `catchTags({...})` | Handle multiple tags | Removes handled tags, adds handlers' errors |
| `catchAll(f)` | Handle all expected errors | Replaces `E` with `f`'s errors |
| `catchSome(f)` | Conditionally handle | Unchanged (can't narrow statically) |
| `mapError(f)` | Transform error value | Replaces `E` with `f`'s return |
| `orElse(f)` | Fallback on any error | Replaces `E` with fallback's errors |
| `orElseSucceed(f)` | Default value on error | `never` (can't fail) |

## Quick Reference

```typescript
import { Effect, Data } from "effect";

// --- Typed error construction ---
class MyError extends Data.TaggedError("MyError")<{
  readonly reason: string;
}> {}

// --- Catching by tag ---
pipe(effect, Effect.catchTag("MyError", (e) => handler))
pipe(effect, Effect.catchTags({ Tag1: h1, Tag2: h2 }))

// --- Catching broadly ---
pipe(effect, Effect.catchAll((e) => handler))
pipe(effect, Effect.catchSome((e) => Option.some(handler)))

// --- Transforming errors ---
pipe(effect, Effect.mapError((e) => newError))
pipe(effect, Effect.mapBoth({ onFailure: f, onSuccess: g }))

// --- Fallbacks ---
pipe(effect, Effect.orElse(() => fallbackEffect))
pipe(effect, Effect.orElseSucceed(() => defaultValue))
pipe(effect, Effect.orElseFail(() => newError))

// --- Defects (preview — covered in depth in Chapter 4b) ---
Effect.die(throwable)                       // Create defect
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 4b — Deep Error Model: Cause, Schema, and Accumulation, where we explore Effect's rich Cause type, validated parsing with Schema, and the difference between fail-fast and error accumulation.*
