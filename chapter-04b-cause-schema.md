# Chapter 4b: Deep Error Model — Cause, Schema, and Accumulation

> **Building on:** This chapter uses tagged error types, `catchTag`, `catchAll`, `mapError`, and `orElse` from Chapter 4a. You should understand the distinction between expected errors (`E` channel) and defects, and how error types narrow through handling.

In Chapter 4a we covered catching and transforming expected errors — the `E` channel that appears in your type signatures. Now we go deeper into Effect's error model: the `Cause` type that captures the full story of failure (including defects and parallel failures), validated parsing with `Schema`, and the distinction between fail-fast and error-accumulating composition.

## The Cause Type: Effect's Rich Error Model

So far we've talked about the `E` channel — the typed, expected errors. But what about defects? Interruptions? What if two parallel effects both fail? This is where `Cause` comes in.

`Cause<E>` is Effect's internal representation of *everything that can go wrong*. It's a recursive data structure that captures:

```typescript
type Cause<E> =
  | Empty                           // No error
  | Fail<E>                         // Expected error (the E channel)
  | Die                             // Defect (unexpected, a bug)
  | Interrupt                       // Fiber was interrupted
  | Sequential<Cause<E>, Cause<E>>  // First failed, then second failed
  | Parallel<Cause<E>, Cause<E>>    // Both failed concurrently
```

Most of the time, you work with `E` through `catchTag` and `catchAll` and never see `Cause` directly. But when you need the full picture — for logging, debugging, or building error-reporting systems — `Cause` is there:

```typescript
import { Effect, Cause } from "effect";

const program = Effect.gen(function* () {
  // ... something that might fail
});

// runPromiseExit gives you Exit<A, E>, which contains Cause<E> on failure
const exit = await Effect.runPromiseExit(program);

if (exit._tag === "Failure") {
  const cause: Cause.Cause<MyError> = exit.cause;

  // Pretty-print the full cause tree
  console.error(Cause.pretty(cause));

  // Extract just the expected errors
  const failures: MyError[] = Cause.failures(cause);

  // Extract just the defects
  const defects: unknown[] = Cause.defects(cause);

  // Check if the fiber was interrupted
  const wasInterrupted: boolean = Cause.isInterrupted(cause);
}
```

### Why Cause Matters for Distributed Systems

In a distributed system, failures compound. Consider a scatter-gather operation: you send requests to five peer nodes in parallel. Three succeed, one times out, and one returns a corrupted response (a defect). With `Promise.allSettled`, you get a flat array and lose the structure. With Effect's parallel combinators, the `Cause` captures both failures in a `Parallel` node:

```typescript
const gatherFromPeers = Effect.all(
  [queryPeer("a"), queryPeer("b"), queryPeer("c"), queryPeer("d"), queryPeer("e")],
  { concurrency: "unbounded" }
);

// If peer "c" times out and peer "d" throws a defect:
// Cause = Parallel(
//   Fail(TimeoutError("c")),
//   Die(TypeError: cannot read property 'data' of undefined)
// )
```

This structure is invaluable for diagnostics. You can traverse the `Cause` tree to build structured error reports, aggregate failure rates, or decide whether to retry (expected errors) or alert (defects).

### Catching Defects (When You Must)

Normally, defects propagate to the top. But sometimes — in a long-running service — you want to catch defects so one bad request doesn't crash the whole process:

```typescript
// catchAllCause — catches expected errors AND defects
const bulletproof = handleRequest(req).pipe(
  Effect.catchAllCause((cause) => {
    if (Cause.isFailure(cause)) {
      // Expected error — handle normally
      return Effect.succeed(errorResponse(Cause.failures(cause)));
    }
    // Defect or interruption — log and return 500
    Effect.logError("Unexpected failure", cause);
    return Effect.succeed(internalServerError());
  })
);

// sandbox — exposes the full Cause as the error channel
const sandboxed: Effect.Effect<Response, Cause.Cause<AppError>> =
  handleRequest(req).pipe(Effect.sandbox);

// unsandbox — reverses sandbox
const restored: Effect.Effect<Response, AppError> =
  sandboxed.pipe(Effect.unsandbox);
```

`Effect.sandbox` lifts the `Cause` into the error channel, letting you use `map`, `flatMap`, and `catchAll` on the full cause. `Effect.unsandbox` puts it back. This is a power tool — use it sparingly, at the boundary.

## Validated Parsing with Schema

Many distributed application errors come from one place: parsing untrusted input. HTTP requests, configuration files, messages from peer nodes — all need validation. Effect provides `Schema` for this, which builds on the same typed-error foundations.

```typescript
import { Schema } from "effect";

// Define a schema for a peer message
const PeerMessage = Schema.Struct({
  type: Schema.Literal("heartbeat", "data", "control"),
  sourceId: Schema.String.pipe(Schema.nonEmptyString()),
  timestamp: Schema.Number.pipe(Schema.positive()),
  payload: Schema.Unknown,
});

// Infer the TypeScript type
type PeerMessage = typeof PeerMessage.Type;

// Parse with typed errors
const parseMessage = (raw: unknown): Effect.Effect<PeerMessage, Schema.ParseError> =>
  Schema.decodeUnknown(PeerMessage)(raw);

// Use in a pipeline
const handleIncoming = (raw: unknown) =>
  Effect.gen(function* () {
    const message = yield* parseMessage(raw);

    switch (message.type) {
      case "heartbeat":
        yield* updatePeerStatus(message.sourceId, message.timestamp);
        break;
      case "data":
        yield* processData(message.sourceId, message.payload);
        break;
      case "control":
        yield* handleControl(message.sourceId, message.payload);
        break;
    }
  });
```

`Schema.decodeUnknown` returns an `Effect` that fails with `ParseError` — a structured error type that tells you exactly which fields failed and why. This is the validation problem from Chapter 2 solved at scale.

`Schema` does much more — encoding, transformations, branded types — but the core idea is the same: validation as a typed Effect that either succeeds with a validated value or fails with structured errors.

## Fail-Fast vs. Error Accumulation: A Practical Comparison

Let's crystallize the distinction from Chapter 2 with full Effect code. Suppose we're validating a service registration with three independent fields:

```typescript
class FieldError extends Data.TaggedError("FieldError")<{
  readonly field: string;
  readonly message: string;
}> {}

const validateId = (id: string): Effect.Effect<string, FieldError> =>
  id.length >= 3 && id.length <= 64
    ? Effect.succeed(id)
    : Effect.fail(new FieldError({ field: "id", message: "must be 3-64 characters" }));

const validateEndpoint = (url: string): Effect.Effect<URL, FieldError> =>
  Effect.try({
    try: () => new URL(url),
    catch: () => new FieldError({ field: "endpoint", message: "invalid URL" }),
  });

const validateTtl = (ttl: number): Effect.Effect<number, FieldError> =>
  ttl >= 10 && ttl <= 86400
    ? Effect.succeed(ttl)
    : Effect.fail(new FieldError({ field: "ttl", message: "must be 10-86400 seconds" }));
```

**Fail-fast** with `Effect.gen` — stops at the first error:

```typescript
const validateFailFast = (input: RawRegistration) =>
  Effect.gen(function* () {
    const id = yield* validateId(input.id);
    const endpoint = yield* validateEndpoint(input.endpoint);
    const ttl = yield* validateTtl(input.ttl);
    return { id, endpoint, ttl };
  });
// If id is invalid, we never check endpoint or ttl
```

**Error accumulation** with `Effect.validate` — collects all errors:

```typescript
const validateAccumulating = (input: RawRegistration) =>
  Effect.all(
    [
      validateId(input.id),
      validateEndpoint(input.endpoint),
      validateTtl(input.ttl),
    ],
    { mode: "validate" }
  ).pipe(
    Effect.map(([id, endpoint, ttl]) => ({ id, endpoint, ttl }))
  );
// Fails with Array<FieldError> containing ALL validation errors
```

The `{ mode: "validate" }` option tells `Effect.all` to run every effect even if some fail, and accumulate errors into an array. This is the applicative pattern from Chapter 2, built into Effect.

When to use which:

- **Fail-fast** when steps are *dependent* (step 2 needs step 1's result) or when you want to short-circuit expensive operations.
- **Accumulate** when steps are *independent* and the user benefits from seeing all errors at once (form validation, config checking, batch processing).

## Exercises

### Exercise 4.3: Working with Cause

Write a function that runs an Effect and produces a structured error report from the Cause:

```typescript
interface ErrorReport {
  readonly expectedErrors: string[];
  readonly defects: string[];
  readonly wasInterrupted: boolean;
}

const generateReport = <A, E extends { _tag: string }>(
  effect: Effect.Effect<A, E>
): Effect.Effect<ErrorReport> => {
  // todo()
  // Hint: Use Effect.sandbox to get the Cause, then inspect it
  // Use Cause.failures, Cause.defects, Cause.isInterrupted
};
```

### Exercise 4.4: Configuration Validator with Accumulation

Build a complete configuration validator for a distributed service. It should validate all fields and report every error.

```typescript
import { Effect, Data } from "effect";

class ConfigFieldError extends Data.TaggedError("ConfigFieldError")<{
  readonly field: string;
  readonly value: unknown;
  readonly expected: string;
}> {}

interface ClusterConfig {
  readonly nodeId: string;
  readonly listenPort: number;
  readonly peerEndpoints: string[];
  readonly replicationFactor: number;
  readonly heartbeatIntervalMs: number;
}

// Implement validators for each field:
const validateNodeId = (id: string): Effect.Effect<string, ConfigFieldError> => {
  // todo() — alphanumeric, 3-32 characters
};

const validateListenPort = (port: number): Effect.Effect<number, ConfigFieldError> => {
  // todo() — 1024-65535 (non-privileged ports)
};

const validatePeerEndpoints = (
  endpoints: string[]
): Effect.Effect<string[], ConfigFieldError> => {
  // todo() — non-empty array, each is a valid URL
};

const validateReplicationFactor = (
  factor: number
): Effect.Effect<number, ConfigFieldError> => {
  // todo() — 1, 3, 5, or 7 (odd numbers for quorum)
};

const validateHeartbeatInterval = (
  ms: number
): Effect.Effect<number, ConfigFieldError> => {
  // todo() — 100-30000
};

// Combine with error accumulation
const validateClusterConfig = (raw: Record<string, unknown>): Effect.Effect<
  ClusterConfig,
  ConfigFieldError[]
> => {
  // todo()
  // Hint: Effect.all with { mode: "validate" }
};
```

### Exercise 4.5: Schema Validation

Define a Schema for a network message and use it in an Effect pipeline. Handle parse errors gracefully.

```typescript
import { Schema, Effect } from "effect";

// Define a Schema for this message type:
// {
//   version: 1 (literal),
//   action: "sync" | "query" | "replicate",
//   nodeId: non-empty string,
//   timestamp: positive number,
//   data: { key: string, value: string } (optional)
// }

const NetworkMessage = // todo() — use Schema.Struct, Schema.Literal, Schema.optional, etc.

type NetworkMessage = typeof NetworkMessage.Type;

// Build a pipeline that:
// 1. Parses a raw JSON string into the schema
// 2. Routes based on the action field
// 3. Returns a response string
const processMessage = (rawJson: string): Effect.Effect<string, ParseError | ProcessingError> => {
  // todo()
};
```

### Exercise 4.6 (Challenge): Error Middleware

Build a reusable error middleware that wraps any Effect and:
1. Catches expected errors and converts them to a standardized `AppError` format
2. Catches defects and logs them before converting to `AppError`
3. Adds a correlation ID to every error for tracing

```typescript
class AppError {
  readonly _tag = "AppError" as const;
  constructor(
    readonly code: string,
    readonly message: string,
    readonly correlationId: string,
    readonly isRetryable: boolean
  ) {}
}

const withErrorMiddleware = <A, E extends { readonly _tag: string }>(
  effect: Effect.Effect<A, E>,
  correlationId: string,
  classifyRetryable: (tag: string) => boolean
): Effect.Effect<A, AppError> => {
  // todo()
  // Hint: Use Effect.catchAllCause to handle both errors and defects
  // Use Cause.failureOption and Cause.dieOption to inspect the cause
};
```

## Summary

This chapter explored Effect's deeper error model. `Cause<E>` captures the full error story — expected errors, defects, interruptions, and their composition through sequential and parallel failures. `Schema` provides validated parsing that integrates with the typed error system. And `Effect.all` with `{ mode: "validate" }` gives you error accumulation for independent validations.

The key distinction: **expected errors** (the `E` channel) are for callers to handle; **defects** are bugs that propagate to the top. `Cause` bridges both when you need the complete picture.

| Tool | What it does | Error type after |
|---|---|---|
| `catchAllCause(f)` | Handle errors + defects | Replaces entire cause |
| `sandbox` | Expose Cause as E | `Cause<E>` |
| `unsandbox` | Reverse sandbox | `E` |
| `Effect.all({mode: "validate"})` | Accumulate errors | `E[]` |
| `Schema.decodeUnknown(S)(raw)` | Validated parsing | `ParseError` |

## Quick Reference

```typescript
import { Effect, Cause, Data, Schema } from "effect";

// --- Cause inspection ---
Cause.pretty(cause)                         // Human-readable string
Cause.failures(cause)                       // Extract expected errors
Cause.defects(cause)                        // Extract defects
Cause.isInterrupted(cause)                  // Check interruption

// --- Catching defects ---
pipe(effect, Effect.catchAllCause((cause) => handler))
Effect.sandbox(effect)                      // Cause<E> as error channel
Effect.unsandbox(effect)                    // Reverse sandbox

// --- Defect construction ---
Effect.die(throwable)                       // Create defect

// --- Fail-fast vs. accumulation ---
Effect.gen(function* () { ... })            // Fail-fast (monadic)
Effect.all(effects, { mode: "validate" })   // Accumulate errors (applicative)

// --- Schema validation ---
Schema.decodeUnknown(MySchema)(raw)         // Effect<A, ParseError>
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 5 — Resource Management and Scoping, where we tackle the problem of guaranteeing cleanup in a world where anything can fail, and discover that resources, like errors, are best treated as composable values.*
