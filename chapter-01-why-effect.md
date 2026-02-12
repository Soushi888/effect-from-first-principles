# Chapter 1: Why Effect?

## The Pain You Already Know

Let's build something real. You're writing a service that registers a new node in a distributed network. It needs to:

1. Validate the incoming request
2. Check that the node ID isn't already taken (remote call)
3. Write the node record to the local store
4. Notify peer nodes of the new registration (remote call)
5. Return the result

Here's what this looks like in everyday TypeScript:

```typescript
async function registerNode(request: NodeRegistrationRequest): Promise<NodeRecord> {
  // Step 1: Validate
  if (!request.nodeId || !request.endpoint) {
    throw new Error("Invalid request: missing nodeId or endpoint");
  }

  // Step 2: Check uniqueness
  let existing: NodeRecord | null;
  try {
    existing = await fetchNodeFromRegistry(request.nodeId);
  } catch (e) {
    throw new Error(`Registry lookup failed: ${(e as Error).message}`);
  }
  if (existing) {
    throw new Error(`Node ${request.nodeId} already registered`);
  }

  // Step 3: Write to local store
  let record: NodeRecord;
  try {
    record = await writeToStore({
      nodeId: request.nodeId,
      endpoint: request.endpoint,
      registeredAt: new Date(),
    });
  } catch (e) {
    throw new Error(`Store write failed: ${(e as Error).message}`);
  }

  // Step 4: Notify peers — but what if this fails after the write succeeded?
  try {
    await notifyPeers(record);
  } catch (e) {
    // Do we roll back the write? Retry? Log and continue?
    // Nobody knows. The error is just `unknown`.
    console.error("Peer notification failed, but record was written:", e);
  }

  return record;
}
```

Take a moment to study this function. It *works*, but notice the problems accumulating:

**The error channel is untyped.** Every `catch` block receives `unknown`. The caller of `registerNode` gets back `Promise<NodeRecord>` — the type signature says nothing about the four distinct ways this function can fail. The compiler won't help you handle them.

**Error handling doesn't compose.** Each `try/catch` is an island. We can't combine them, transform them, or build fallback chains. If we want to retry step 2 with exponential backoff, we need a manual loop with counter variables. If we want to retry step 4 independently of step 2, we're nesting even deeper.

**Resource cleanup is fragile.** What if step 3 opens a database connection? We'd add a `finally` block. What if step 4 also opens an HTTP connection? Nested `try/finally`. What if the cleanup itself throws? We're back to writing infrastructure instead of business logic.

**Dependencies are invisible.** This function secretly depends on a registry client, a local store, and a peer notification service. Nothing in the type signature tells you that. Testing requires mocking global imports or dependency injection frameworks.

**There's no structured concurrency.** What if we want to notify peers in parallel? We'd use `Promise.all`, but if one notification fails, what happens to the others? Are they cancelled? Do they run to completion while we've already thrown? We have no control.

This is the reality of building distributed applications in TypeScript. Every service you write is a variations of this pattern: a multi-step workflow crossing network boundaries, where any step can fail in distinct ways, where resources need cleanup, and where the type system provides no guidance.

## What If Errors Were Just Data?

Let's fix the first problem — untyped errors — without any library. What if instead of throwing, we returned errors as values?

```typescript
type Either<E, A> = { readonly _tag: "Left"; readonly left: E }
                  | { readonly _tag: "Right"; readonly right: A };

const left  = <E>(e: E): Either<E, never> => ({ _tag: "Left", left: e });
const right = <A>(a: A): Either<never, A> => ({ _tag: "Right", right: a });
```

Now we can write functions that declare their error types:

```typescript
function validateRequest(
  request: NodeRegistrationRequest
): Either<ValidationError, ValidatedRequest> {
  if (!request.nodeId || !request.endpoint) {
    return left({ _tag: "ValidationError", message: "Missing required fields" });
  }
  return right({ nodeId: request.nodeId, endpoint: request.endpoint });
}
```

This is better! The type signature tells you exactly what can go wrong. But now we need to chain these operations — if validation succeeds, check uniqueness; if that succeeds, write to store. We need a way to sequence operations that might fail:

```typescript
function flatMap<E1, A, E2, B>(
  self: Either<E1, A>,
  f: (a: A) => Either<E2, B>
): Either<E1 | E2, B> {
  if (self._tag === "Left") return self;
  return f(self.right);
}
```

Look at that return type: `Either<E1 | E2, B>`. TypeScript's union types naturally express "this can fail with errors from the first step OR errors from the second step." The error types *compose*.

But `Either` only handles synchronous code. Our registry lookup is async. We could use `Promise<Either<E, A>>`, but then we're manually unwrapping promises everywhere. We need something that handles both effects (async, I/O) and typed errors in one structure.

## Effects as Data

Here is the core idea of this entire series:

> An **Effect** is a *description* of a computation. It declares what it will produce, how it can fail, and what it needs to run. Nothing happens until you explicitly execute it.

This is the same principle as the `Either` we just built — errors as data, not exceptions — but extended to everything: async operations, resource management, dependencies, concurrency, retries.

In the `effect` library, this description is captured by one type:

```typescript
Effect<Success, Error, Requirements>
//       A        E         R
```

- **Success (`A`)** — the value produced when the computation succeeds
- **Error (`E`)** — the typed error(s) that can occur
- **Requirements (`R`)** — the services/dependencies needed to run

Let's rewrite our opening example:

```typescript
import { Effect } from "effect";

// Each error is a tagged type — pattern-matchable
class ValidationError {
  readonly _tag = "ValidationError";
  constructor(readonly message: string) {}
}

class RegistryError {
  readonly _tag = "RegistryError";
  constructor(readonly cause: unknown) {}
}

class DuplicateNodeError {
  readonly _tag = "DuplicateNodeError";
  constructor(readonly nodeId: string) {}
}

class StoreError {
  readonly _tag = "StoreError";
  constructor(readonly cause: unknown) {}
}

class NotificationError {
  readonly _tag = "NotificationError";
  constructor(readonly failedPeers: string[]) {}
}
```

Now the workflow:

```typescript
const registerNode = (
  request: NodeRegistrationRequest
): Effect.Effect<
  NodeRecord,
  ValidationError | RegistryError | DuplicateNodeError | StoreError,
  RegistryClient | NodeStore | PeerNotifier
> =>
  Effect.gen(function* () {
    // Step 1: Validate — fails with ValidationError
    const validated = yield* validateRequest(request);

    // Step 2: Check uniqueness — fails with RegistryError | DuplicateNodeError
    const existing = yield* checkNodeExists(validated.nodeId);
    if (existing) {
      yield* Effect.fail(new DuplicateNodeError(validated.nodeId));
    }

    // Step 3: Write — fails with StoreError
    const record = yield* writeNodeRecord(validated);

    // Step 4: Notify peers — we can handle this separately
    yield* notifyPeers(record).pipe(
      Effect.retry({ times: 3 }),
      Effect.catchTag("NotificationError", (e) =>
        Effect.logWarning(`Peer notification failed: ${e.failedPeers}`)
      )
    );

    return record;
  });
```

Read the type signature. It tells you *everything*:
- This produces a `NodeRecord`
- It can fail with four distinct error types
- It requires three services to run: `RegistryClient`, `NodeStore`, `PeerNotifier`

And notice what `Effect.gen` gives us: code that reads top-to-bottom like the original `async/await` version, but with typed errors that compose automatically. Each `yield*` is like an `await` that also propagates typed errors — the `E` parameter of the final Effect is the union of all the errors from each step.

**Nothing has executed yet.** `registerNode` returns a *description*. We compose descriptions first, then run them:

```typescript
// Build the full program with dependencies provided
const program = registerNode(request).pipe(
  Effect.provide(RegistryClientLive),
  Effect.provide(NodeStoreLive),
  Effect.provide(PeerNotifierLive)
);

// NOW execute
Effect.runPromise(program);
```

This separation — describe first, execute later — is what makes everything else possible: testing with mock services, adding retry policies after the fact, running with different configurations. The program is a value you can manipulate.

## Pure Functions and Referential Transparency

Let's ground this in a principle. A function is **pure** if:

1. It always returns the same output for the same input
2. It has no side effects (no I/O, no mutation, no throwing)

```typescript
// Pure — same input, same output, no effects
const add = (a: number, b: number): number => a + b;

// Impure — throws (a hidden output path)
const divide = (a: number, b: number): number => {
  if (b === 0) throw new Error("Division by zero");
  return a / b;
};

// Impure — depends on external state
const now = (): number => Date.now();

// Impure — performs I/O
const log = (msg: string): void => console.log(msg);
```

A **referentially transparent** expression can be replaced by its value without changing program behavior. `add(2, 3)` can always be replaced by `5`. But `now()` can't be replaced by any fixed number — it returns something different each time.

Why does this matter for distributed applications? Because in a distributed system, you need to *reason about* what your code does: will this retry correctly? Will resources be cleaned up? Are errors handled? If your functions have hidden effects, you can't compose them reliably.

Effect's approach: make every effect *explicit* in the type system. A function that might fail returns `Effect<A, E, R>`, not `A`. A function that reads from a database requires `DatabaseService` in its `R` parameter. The types become a map of your program's behavior.

```typescript
// This is a pure function! It returns a description, not a result.
const fetchUser = (id: string): Effect.Effect<User, HttpError, HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient;
    const response = yield* client.get(`/users/${id}`);
    return response.json as User;
  });

// This is also pure — composing descriptions is pure
const fetchUserOrDefault = (id: string) =>
  fetchUser(id).pipe(
    Effect.orElseSucceed(() => DEFAULT_USER)
  );
```

The function `fetchUser` doesn't fetch anything. It builds a description that says "when you run me, I'll need an `HttpClient`, I'll make a GET request, and I might fail with an `HttpError`." The description is a plain value — you can store it, pass it around, compose it, test it.

## The Substitution Test

Here's a quick way to check if an expression is referentially transparent. Can you replace it with its result without changing the program?

```typescript
// Referentially transparent:
const x = add(2, 3);        // x = 5
const y = add(2, 3);        // y = 5
const z = x + y;            // z = 10
// Replace: z = add(2,3) + add(2,3) = 5 + 5 = 10 ✓

// NOT referentially transparent:
const a = Date.now();
const b = Date.now();
const c = a === b;           // might be false!
// Can't replace both with the same value ✗
```

With Effect, side-effectful operations become referentially transparent *descriptions*:

```typescript
const getTime = Effect.sync(() => Date.now());

// These are the same description — both are pure values
const a = getTime;
const b = getTime;

// This builds a new description that sequences them
const program = Effect.gen(function* () {
  const t1 = yield* a;
  const t2 = yield* b;
  return t1 === t2;  // might be false — but that's when we RUN it
});
```

The descriptions `a` and `b` are identical values. The side effect (reading the clock) only happens when we run the program. Until then, everything is pure and composable.

## Exercises

### Exercise 1.1: Classify Pure vs. Impure

For each function, determine whether it's pure or impure. If impure, identify the side effect.

```typescript
// (a)
const length = (s: string): number => s.length;

// (b)
const greet = (name: string): string => {
  console.log(`Hello, ${name}`);
  return `Hello, ${name}`;
};

// (c)
const parsePort = (s: string): number => {
  const n = parseInt(s, 10);
  if (isNaN(n)) throw new Error(`Invalid port: ${s}`);
  return n;
};

// (d)
let counter = 0;
const increment = (): number => ++counter;

// (e)
const hash = (data: Uint8Array): string => {
  // Assume: deterministic SHA-256 implementation
  return sha256(data);
};
```

### Exercise 1.2: Refactor to Either

Rewrite `parsePort` from Exercise 1.1(c) so it's pure. Use the `Either` type we defined earlier.

```typescript
type Either<E, A> = { readonly _tag: "Left"; readonly left: E }
                  | { readonly _tag: "Right"; readonly right: A };

class ParseError {
  readonly _tag = "ParseError";
  constructor(readonly input: string, readonly reason: string) {}
}

const parsePort = (s: string): Either<ParseError, number> => {
  // todo()
};
```

### Exercise 1.3: Chain Either Operations

Using `flatMap` from earlier in this chapter, chain together `parsePort` and a new function `validatePort` that checks the port is within range 1–65535.

```typescript
class PortRangeError {
  readonly _tag = "PortRangeError";
  constructor(readonly port: number) {}
}

const validatePort = (port: number): Either<PortRangeError, number> => {
  // todo()
};

// Chain parsePort and validatePort — what is the resulting error type?
const parseAndValidatePort = (s: string): Either</* what goes here? */, number> => {
  // todo()
};
```

### Exercise 1.4: Your First Effect

Install Effect and rewrite `parseAndValidatePort` as an Effect. Use `Effect.gen`.

```typescript
import { Effect } from "effect";

class ParseError {
  readonly _tag = "ParseError" as const;
  constructor(readonly input: string, readonly reason: string) {}
}

class PortRangeError {
  readonly _tag = "PortRangeError" as const;
  constructor(readonly port: number) {}
}

const parsePort = (s: string): Effect.Effect<number, ParseError> => {
  // todo() — hint: use Effect.succeed and Effect.fail
};

const validatePort = (port: number): Effect.Effect<number, PortRangeError> => {
  // todo()
};

// Chain them together — what does TypeScript infer for the error type?
const parseAndValidatePort = (s: string): Effect.Effect<number, /* ? */> => {
  // todo() — hint: use Effect.gen with yield*
};

// Run it
Effect.runPromise(
  parseAndValidatePort("8080").pipe(
    Effect.match({
      onSuccess: (port) => console.log(`Valid port: ${port}`),
      onFailure: (e) => console.error(`Failed: ${e._tag}`),
    })
  )
);
```

### Exercise 1.5: The Substitution Test

Consider these two programs. Are they equivalent? Why or why not?

```typescript
// Program A
const fetchConfig = Effect.tryPromise({
  try: () => fetch("/config").then(r => r.json()),
  catch: () => new ConfigError()
});

const programA = Effect.gen(function* () {
  const c1 = yield* fetchConfig;
  const c2 = yield* fetchConfig;
  return [c1, c2];
});

// Program B
const programB = Effect.gen(function* () {
  const c = yield* fetchConfig;
  return [c, c];
});
```

*Hint: Think about when the fetch actually happens. Are `c1` and `c2` in Program A guaranteed to be the same value?*

## Summary

| Concept | Traditional TypeScript | Effect-TS |
|---|---|---|
| Error handling | `try/catch`, untyped `unknown` | `Effect<A, E, R>` with typed `E` |
| Async operations | `Promise<A>` (no typed errors) | `Effect<A, E, R>` (unified sync/async) |
| Dependencies | Hidden imports, DI frameworks | `R` parameter, `Layer` |
| Resource cleanup | `try/finally` (fragile) | `Scope`, `acquireRelease` |
| Concurrency | `Promise.all` (no cancellation) | Fibers, structured concurrency |
| Composition | Manual, ad-hoc | `pipe`, `map`, `flatMap`, `gen` |
| Retries | Manual loops | `Effect.retry` + `Schedule` |

**Key takeaways:**

- An `Effect<A, E, R>` is a *description* of a computation — a pure value that declares its success type, error type, and dependencies.
- Nothing runs until you call `Effect.runPromise` or `Effect.runSync`.
- Errors as values (not exceptions) enable typed composition — the compiler tracks every way your program can fail.
- The same patterns (`map`, `flatMap`, `pipe`) that work on `Either` also work on `Effect`. We'll see this recur on every type in this series.

## Quick Reference

```typescript
// Constructors
Effect.succeed(value)                  // Always succeeds with value
Effect.fail(error)                     // Always fails with error
Effect.sync(() => value)               // Wraps synchronous side effect
Effect.tryPromise({ try, catch })      // Wraps a Promise with typed error

// Composition
Effect.map(effect, f)                  // Transform success value
Effect.flatMap(effect, f)              // Chain to next Effect
Effect.gen(function* () { ... })       // Generator syntax (like async/await)

// Error handling
Effect.catchTag(effect, tag, handler)  // Handle specific error by _tag
Effect.orElseSucceed(effect, fallback) // Provide fallback value

// Execution
Effect.runPromise(effect)              // Run and get Promise<A>
Effect.runSync(effect)                 // Run synchronously (if possible)

// Composition utility
pipe(value, fn1, fn2, fn3)            // Left-to-right function composition
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 2 — Option, Either, and the Algebra of Errors, where we build these types from scratch and discover the patterns that recur everywhere in Effect.*
