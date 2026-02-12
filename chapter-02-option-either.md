# Chapter 2: Option, Either, and the Algebra of Errors

## The Billion-Dollar Problem

Every distributed application deals with missing data. A peer node hasn't responded yet. A configuration key isn't set. A cache lookup returns nothing. In standard TypeScript, this is represented with `null` or `undefined` — Tony Hoare's famous "billion-dollar mistake."

```typescript
function findNode(id: string): NodeRecord | null {
  // ...
}

const node = findNode("abc");
const endpoint = node.endpoint; // 💥 Runtime: Cannot read property 'endpoint' of null
```

TypeScript's strict null checks help, but they don't give you *operations*. You can't chain nullable values, combine them, or transform them without manual null-checking at every step:

```typescript
function getNodeEndpoint(id: string): string | null {
  const node = findNode(id);
  if (node === null) return null;
  const config = getNodeConfig(node.configId);
  if (config === null) return null;
  return config.endpoint;
}
```

Three lines of logic, four lines of null-checking. And this is a simple case — in a real distributed system, you might chain five or six lookups, each of which might return nothing. The null-checking buries the intent.

We're going to fix this by building `Option` from scratch, then `Either`, discovering that they share an identical algebra. Then we'll connect these hand-built types to Effect's production-grade implementations.

## Building Option from Scratch

An `Option<A>` is a value that is either present (`Some<A>`) or absent (`None`):

```typescript
type Option<A> = { readonly _tag: "None" }
               | { readonly _tag: "Some"; readonly value: A };

const none: Option<never> = { _tag: "None" };
const some = <A>(value: A): Option<A> => ({ _tag: "Some", value });
```

That's it. A discriminated union with two cases. Now let's build the operations that make it useful.

### map — Transform the Value Inside

If we have an `Option<A>` and a function `A => B`, we want an `Option<B>`. If the value is absent, there's nothing to transform — we get `None` back:

```typescript
const map = <A, B>(self: Option<A>, f: (a: A) => B): Option<B> => {
  if (self._tag === "None") return none;
  return some(f(self.value));
};

// Usage
const port: Option<number> = some(8080);
const portStr: Option<string> = map(port, (p) => `Port: ${p}`);
// { _tag: "Some", value: "Port: 8080" }

const missing: Option<number> = none;
const missingStr: Option<string> = map(missing, (p) => `Port: ${p}`);
// { _tag: "None" }
```

The function `f` never runs if the value is absent. `map` respects the structure — it reaches inside the `Option`, applies the function, and rewraps the result.

### flatMap — Chain Operations That Might Fail

`map` handles `A => B`, but what about `A => Option<B>`? If our transformation itself might produce nothing, `map` would give us `Option<Option<B>>` — nested options. We need to flatten:

```typescript
const flatMap = <A, B>(self: Option<A>, f: (a: A) => Option<B>): Option<B> => {
  if (self._tag === "None") return none;
  return f(self.value);
};
```

This is the operation that solves our chaining problem:

```typescript
const findNode = (id: string): Option<NodeRecord> => { /* ... */ };
const getConfig = (configId: string): Option<NodeConfig> => { /* ... */ };

// Without flatMap — manual null-checking
// With flatMap — declarative chaining
const getEndpoint = (nodeId: string): Option<string> =>
  flatMap(
    findNode(nodeId),
    (node) => flatMap(
      getConfig(node.configId),
      (config) => some(config.endpoint)
    )
  );
```

If `findNode` returns `None`, the entire chain is `None`. If `getConfig` returns `None`, same result. Only if both succeed do we get `Some(endpoint)`. The logic reads as a pipeline, not a ladder of `if` statements.

### getOrElse — Escape the Option

At some point you need the actual value. `getOrElse` provides a default when the value is absent:

```typescript
const getOrElse = <A>(self: Option<A>, defaultValue: () => A): A => {
  if (self._tag === "None") return defaultValue();
  return self.value;
};

const port = getOrElse(findPort("main"), () => 3000);
// Always a number — no null possible
```

### match — The Universal Destructor

`match` (sometimes called `fold`) handles both cases explicitly:

```typescript
const match = <A, B>(
  self: Option<A>,
  onNone: () => B,
  onSome: (a: A) => B
): B => {
  if (self._tag === "None") return onNone();
  return onSome(self.value);
};

const message = match(
  findNode("abc"),
  () => "Node not found",
  (node) => `Found node at ${node.endpoint}`
);
```

Every possible state is handled. No runtime surprises.

### zip — Combine Two Options

What if we need *both* of two optional values?

```typescript
const zip = <A, B>(self: Option<A>, that: Option<B>): Option<[A, B]> => {
  if (self._tag === "None") return none;
  if (that._tag === "None") return none;
  return some([self.value, that.value]);
};

const both = zip(findNode("a"), findNode("b"));
// Option<[NodeRecord, NodeRecord]> — Some only if both exist
```

Pause for a moment. We've built five operations: `map`, `flatMap`, `getOrElse`, `match`, `zip`. Remember these names. You'll see them again — on `Either`, on `Effect`, on `Stream`, on almost every type in this series.

## Building Either from Scratch

`Option` tells us *whether* something went wrong, but not *what*. In a distributed system, the difference between "network timeout" and "permission denied" is crucial — they demand different responses. `Either<E, A>` carries an error value on the failure path:

```typescript
type Either<E, A> = { readonly _tag: "Left"; readonly left: E }
                  | { readonly _tag: "Right"; readonly right: A };

const left  = <E>(e: E): Either<E, never> => ({ _tag: "Left", left: e });
const right = <A>(a: A): Either<never, A> => ({ _tag: "Right", right: a });
```

By convention, `Left` is the error channel and `Right` is the success channel (a mnemonic: "right" is correct).

Now watch what happens when we build the same operations:

### map

```typescript
const map = <E, A, B>(self: Either<E, A>, f: (a: A) => B): Either<E, B> => {
  if (self._tag === "Left") return self;
  return right(f(self.right));
};
```

Identical structure to `Option.map`. The `Left` case propagates unchanged, just like `None`.

### flatMap

```typescript
const flatMap = <E1, A, E2, B>(
  self: Either<E1, A>,
  f: (a: A) => Either<E2, B>
): Either<E1 | E2, B> => {
  if (self._tag === "Left") return self;
  return f(self.right);
};
```

Same shape as `Option.flatMap`, but look at the return type: `Either<E1 | E2, B>`. The error types *union* automatically. This is TypeScript's type system working for us — it tracks every possible error path through the chain.

### match

```typescript
const match = <E, A, B>(
  self: Either<E, A>,
  onLeft: (e: E) => B,
  onRight: (a: A) => B
): B => {
  if (self._tag === "Left") return onLeft(self.left);
  return onRight(self.right);
};
```

### zip

```typescript
const zip = <E1, A, E2, B>(
  self: Either<E1, A>,
  that: Either<E2, B>
): Either<E1 | E2, [A, B]> => {
  if (self._tag === "Left") return self;
  if (that._tag === "Left") return that;
  return right([self.right, that.right]);
};
```

### getOrElse

```typescript
const getOrElse = <E, A>(self: Either<E, A>, defaultValue: (e: E) => A): A => {
  if (self._tag === "Left") return defaultValue(self.left);
  return self.right;
};
```

## The Pattern Emerges

Let's put these side by side:

| Operation | Option | Either |
|---|---|---|
| Wrap a value | `some(a)` | `right(a)` |
| Represent failure | `none` | `left(e)` |
| Transform success | `map(oa, f)` | `map(ea, f)` |
| Chain operations | `flatMap(oa, f)` | `flatMap(ea, f)` |
| Handle both cases | `match(oa, onNone, onSome)` | `match(ea, onLeft, onRight)` |
| Combine two values | `zip(oa, ob)` | `zip(ea, eb)` |
| Extract with default | `getOrElse(oa, () => a)` | `getOrElse(ea, (e) => a)` |

The operations are structurally identical. The only difference is that `Either` carries an error value where `Option` has `None`. We could even define `Option<A>` as `Either<void, A>` — and indeed, that's roughly how Effect-TS thinks about it internally.

This isn't a coincidence. It's a deep pattern we'll revisit throughout this series. When you see `map`, `flatMap`, `zip`, and `match` on a new type, you already know how they behave.

## Enter Effect's Option and Either

Now that we've built these from scratch and understand the algebra, let's see how Effect-TS provides them. The `effect` package exports `Option` and `Either` modules with the same operations — plus `pipe`-based composition.

```typescript
import { Option, Either, pipe } from "effect";

// Option
const port = Option.some(8080);
const none = Option.none();

const doubled = pipe(
  port,
  Option.map((p) => p * 2)
);

// Either
const parsed: Either.Either<number, ParseError> = Either.right(8080);

const validated = pipe(
  parsed,
  Either.flatMap((port) =>
    port > 0 && port <= 65535
      ? Either.right(port)
      : Either.left(new PortRangeError(port))
  )
);
```

### Why pipe?

In our hand-built versions, we wrote `map(option, f)`. Effect-TS uses `pipe` instead:

```typescript
// Hand-built style
const result = map(flatMap(option, f), g);

// Pipe style — reads top-to-bottom, left-to-right
const result = pipe(
  option,
  Option.flatMap(f),
  Option.map(g)
);
```

`pipe` takes an initial value and passes it through a chain of functions. Each function receives the result of the previous one. This solves two problems:

1. **Readability.** Nested calls read inside-out. Pipes read in execution order.
2. **TypeScript limitation.** TypeScript doesn't support extension methods. We can't add `.map()` to our union types. But we can pipe values through module functions.

`pipe` is the composition backbone of Effect-TS. Get comfortable with it — it appears everywhere.

### Reading Pipe Chains: A Visual Guide

A pipe chain reads **top-to-bottom**: start with a value, then apply each function in order. Each line receives the output of the previous line:

```typescript
const result = pipe(
  Option.some(8080),          // Start with: Option<number>
  Option.map((p) => p + 1),   // → Option<number>  (8081)
  Option.filter((p) => p > 0), // → Option<number>  (still 8081, passes filter)
  Option.map(String),          // → Option<string>  ("8081")
);
```

Read it as: "Start with `some(8080)`, then map `+1`, then filter positive, then map to string."

A common mistake is reading inside-out (like nested function calls). Don't do this:

```typescript
// ❌ Don't read as: "map(filter(map(some(8080))))"
// ✅ Read as: "start → step 1 → step 2 → step 3"
```

When a chain includes `flatMap`, the mental model is the same — but the function returns a new container, and the chain continues with what's inside:

```typescript
const endpoint = pipe(
  findNode("abc"),                    // Start: Option<NodeRecord>
  Option.flatMap((n) => getConfig(n.configId)), // → Option<Config>
  Option.map((c) => c.endpoint),      // → Option<string>
  Option.getOrElse(() => "default"),   // → string (exits the Option)
);
```

Each line flows into the next. If any step produces `None`, everything below it is skipped and the result is `None` (until `getOrElse` provides a fallback). This top-to-bottom reading is the same pattern you'll use for `Effect`, `Stream`, and every other pipe chain in this series.

### flow — Compose Without an Initial Value

`pipe` applies functions to a value. `flow` composes functions into a new function:

```typescript
import { flow } from "effect";

// pipe: apply immediately
const result = pipe(8080, addOne, double); // 16162

// flow: build a reusable function
const addOneThenDouble = flow(addOne, double);
const result = addOneThenDouble(8080); // 16162
```

Use `pipe` when you have a value and want to transform it. Use `flow` when you want to build a transformation pipeline for later use.

## Short-Circuiting: flatMap Is a One-Way Gate

A critical property of `flatMap` — on `Option`, `Either`, and later `Effect` — is **short-circuiting**. Once a failure occurs, all subsequent operations are skipped:

```typescript
const validateConfig = (raw: RawConfig): Either.Either<ValidatedConfig, ConfigError> =>
  pipe(
    parseHost(raw.host),                        // Step 1: might fail
    Either.flatMap((host) => parsePort(raw.port) // Step 2: skipped if Step 1 failed
      .pipe(Either.map((port) => ({ host, port })))
    ),
    Either.flatMap(validateTLS)                  // Step 3: skipped if Step 1 or 2 failed
  );
```

If `parseHost` returns `Left`, steps 2 and 3 never execute. The error propagates directly to the final result. This is exactly how exceptions work in `try/catch` — but with typed errors and no hidden control flow.

This behavior is called **fail-fast** or **monadic** error handling. It's the right default for sequential operations where later steps depend on earlier ones.

But sometimes you want a different behavior...

## Accumulating Errors: The Validation Problem

Consider validating a network configuration. Every field might have an error, and you want to report *all* of them — not stop at the first:

```typescript
// With flatMap (short-circuiting) — user sees one error at a time
// "Fix host" → they fix it → "Fix port" → they fix it → "Fix TLS" → ...

// What we want: "host is invalid, port is out of range, TLS cert expired"
```

`flatMap` can't do this because each step depends on the previous one's *value*. But validation doesn't have that dependency — the fields are independent. We need a different combinator.

Here's `zipValidated` — it runs both sides and accumulates errors:

```typescript
const zipValidated = <E, A, B>(
  self: Either<E[], A>,
  that: Either<E[], B>
): Either<E[], [A, B]> => {
  if (self._tag === "Left" && that._tag === "Left") {
    return left([...self.left, ...that.left]); // Accumulate!
  }
  if (self._tag === "Left") return self;
  if (that._tag === "Left") return that;
  return right([self.right, that.right]);
};
```

When both sides fail, we collect all errors into an array. This is the **applicative** pattern — combining independent computations, in contrast to the **monadic** `flatMap` which sequences dependent ones.

We'll see this distinction appear repeatedly. For now, remember:

- **flatMap** → sequential, fail-fast, for dependent steps
- **zip/all with validation** → parallel (conceptually), error-accumulating, for independent steps

## Bridging to Effect

`Option` and `Either` are for pure, synchronous operations. When you need async I/O, resource management, or dependency injection, you need `Effect`. But the types interoperate cleanly:

```typescript
import { Effect, Option, Either } from "effect";

// Lift Either into Effect
const fromEither = <E, A>(either: Either.Either<A, E>): Effect.Effect<A, E> =>
  Either.match(either, {
    onLeft: Effect.fail,
    onRight: Effect.succeed,
  });

// Effect provides this built-in
const effect1: Effect.Effect<number, ParseError> =
  Effect.fromEither(parsePort("8080"));

// Run an Effect and capture the error channel as Either
const effect2: Effect.Effect<Either.Either<number, ParseError>> =
  Effect.either(riskyComputation);

// Run an Effect and capture absence as Option
const effect3: Effect.Effect<Option.Option<NodeRecord>> =
  Effect.option(findNode("abc"));
```

`Effect.either` is particularly useful when you want to "catch" errors without handling them — you get the full `Either` to inspect:

```typescript
const program = Effect.gen(function* () {
  const result = yield* Effect.either(riskyCall);
  
  if (Either.isLeft(result)) {
    // We can inspect the typed error and decide what to do
    yield* Effect.logWarning(`Call failed: ${result.left._tag}`);
    return defaultValue;
  }
  
  return result.right;
});
```

## Exercises

### Exercise 2.1: Implement map2

`map2` combines two `Option` values using a function. If either is `None`, the result is `None`.

```typescript
const map2 = <A, B, C>(
  oa: Option<A>,
  ob: Option<B>,
  f: (a: A, b: B) => C
): Option<C> => {
  // todo()
  // Hint: you can implement this with flatMap and map
};

// Test: combine host and port into an address
const host = Option.some("10.0.0.1");
const port = Option.some(8080);

const address = map2(host, port, (h, p) => `${h}:${p}`);
// Should be: Option.some("10.0.0.1:8080")
```

### Exercise 2.2: Implement traverse

`traverse` takes an array and a function that returns an `Option`. If any element produces `None`, the whole result is `None`. Otherwise, collect all the `Some` values.

```typescript
const traverse = <A, B>(
  items: ReadonlyArray<A>,
  f: (a: A) => Option<B>
): Option<ReadonlyArray<B>> => {
  // todo()
  // Hint: use reduce with flatMap
};

// Test: parse all ports, fail if any is invalid
const inputs = ["8080", "3000", "443"];
const ports = traverse(inputs, parsePort);
// Should be: Option.some([8080, 3000, 443])

const badInputs = ["8080", "abc", "443"];
const badPorts = traverse(badInputs, parsePort);
// Should be: Option.none()
```

### Exercise 2.3: Implement sequence

`sequence` is a special case of `traverse` where the function is the identity. Turn an `Array<Option<A>>` into an `Option<Array<A>>`.

```typescript
const sequence = <A>(
  items: ReadonlyArray<Option<A>>
): Option<ReadonlyArray<A>> => {
  // todo()
  // Hint: this is a one-liner if you have traverse
};
```

### Exercise 2.4: Validation Pipeline with Either

Build a configuration validator that accumulates *all* errors using `Either`. Define validators for each field and combine them.

```typescript
import { Either, pipe } from "effect";

type NetworkConfig = {
  readonly host: string;
  readonly port: number;
  readonly maxRetries: number;
};

class ConfigError {
  readonly _tag = "ConfigError" as const;
  constructor(readonly field: string, readonly message: string) {}
}

// Implement these validators — each returns Either<ConfigError, validatedValue>
const validateHost = (input: string): Either.Either<string, ConfigError> => {
  // todo() — non-empty, valid hostname characters
};

const validatePort = (input: number): Either.Either<number, ConfigError> => {
  // todo() — between 1 and 65535
};

const validateMaxRetries = (input: number): Either.Either<number, ConfigError> => {
  // todo() — between 0 and 10
};

// Combine all three. How would you accumulate errors?
// Hint: Effect provides Either.all which can be used in "validate" mode
// For the manual approach, think about zipValidated from the chapter
const validateConfig = (raw: {
  host: string;
  port: number;
  maxRetries: number;
}): Either.Either<NetworkConfig, ConfigError[]> => {
  // todo()
};
```

### Exercise 2.5: Bridging to Effect

Take your `validateConfig` from Exercise 2.4 and use it inside an Effect pipeline. If validation succeeds, "connect" to the host (simulate with a log). If it fails, report all errors.

```typescript
import { Effect, Either, Console } from "effect";

const configureAndConnect = (raw: {
  host: string;
  port: number;
  maxRetries: number;
}): Effect.Effect<string, ConfigError[]> => {
  // todo()
  // Hint: Effect.fromEither to lift, then Effect.flatMap or Effect.gen
};

// Run it
Effect.runPromise(
  configureAndConnect({ host: "10.0.0.1", port: 8080, maxRetries: 3 }).pipe(
    Effect.match({
      onSuccess: (msg) => console.log(msg),
      onFailure: (errors) =>
        errors.forEach((e) => console.error(`${e.field}: ${e.message}`)),
    })
  )
);
```

### Exercise 2.6 (Challenge): Option as Either

Show that `Option<A>` is equivalent to `Either<void, A>`. Write conversion functions in both directions.

```typescript
const optionToEither = <A>(opt: Option<A>): Either<void, A> => {
  // todo()
};

const eitherToOption = <A>(either: Either<void, A>): Option<A> => {
  // todo()
};

// Think about: what does this equivalence tell us about the relationship
// between "absence" and "failure with no information"?
```

## Summary

We built `Option` and `Either` from scratch and discovered that they share an identical set of operations: `map`, `flatMap`, `zip`, `match`, `getOrElse`. The only difference is that `Either` carries an error value where `Option` has `None`.

We also discovered two modes of composition:

- **Monadic** (`flatMap`): sequential, fail-fast — for dependent steps where step 2 needs the value from step 1
- **Applicative** (`zip`/`all` with accumulation): for independent computations where we want all errors

Finally, we saw how Effect-TS provides production-grade `Option` and `Either` with `pipe`-based composition, and how they bridge to `Effect` for async/effectful operations.

The key revelation: **the algebra is the same regardless of the container.** `map`, `flatMap`, and `zip` follow the same laws and intuitions on `Option`, `Either`, and — as we'll see in the next chapter — on `Effect` itself.

| Operation | What it does | Shape |
|---|---|---|
| `map` | Transform the value inside | `F<A> → (A → B) → F<B>` |
| `flatMap` | Chain with a function that returns the same container | `F<A> → (A → F<B>) → F<B>` |
| `zip` | Combine two containers | `F<A> → F<B> → F<[A, B]>` |
| `match` | Handle all cases | `F<A> → (handlers) → B` |
| `getOrElse` | Extract with a fallback | `F<A> → (() → A) → A` |

## Quick Reference

```typescript
import { Option, Either, pipe, flow } from "effect";

// --- Option ---
Option.some(value)                           // Wrap a value
Option.none()                                // Absent
pipe(opt, Option.map(f))                     // Transform
pipe(opt, Option.flatMap(f))                 // Chain
pipe(opt, Option.getOrElse(() => fallback))  // Extract
Option.match(opt, { onNone, onSome })        // Handle both cases

// --- Either ---
Either.right(value)                          // Success
Either.left(error)                           // Failure
pipe(ea, Either.map(f))                      // Transform success
pipe(ea, Either.flatMap(f))                  // Chain (fail-fast)
Either.match(ea, { onLeft, onRight })        // Handle both cases

// --- pipe & flow ---
pipe(value, f1, f2, f3)                      // Apply: f3(f2(f1(value)))
flow(f1, f2, f3)                             // Compose: (x) => f3(f2(f1(x)))

// --- Bridging ---
Effect.fromEither(either)                    // Either → Effect
Effect.either(effect)                        // Effect → Effect<Either<A, E>>
Effect.option(effect)                        // Effect → Effect<Option<A>>
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 3 — The Effect Type: Core Operations, where we explore `Effect.gen`, typed error composition, and the full power of the `Effect<A, E, R>` triple.*
