# Chapter 9: Scheduling, Retries, and Repetition

> **Building on:** This chapter uses `Effect.gen`, typed errors (`catchTag`), `Fiber.fork`, `Ref`, and `Stream` from Chapters 3–8. Key concepts: the `E` channel for typed errors, `pipe` composition, and the "effects as data" principle.

## Failure Is Normal

In a distributed system, failure isn't an edge case — it's the steady state. Networks partition. Services restart. DNS caches go stale. Load balancers rotate connections. A request that succeeds 99.9% of the time will fail thousands of times per day at scale.

The question isn't *whether* things fail, but *how you respond*. And the response is almost never "give up immediately." It's:

- Retry after a short delay
- Retry with exponential backoff so you don't hammer a recovering service
- Add jitter so 500 clients don't all retry at the same instant
- Give up after 5 attempts or 30 seconds, whichever comes first
- Retry only on transient errors (timeout, 503), not permanent ones (401, 404)

In typical TypeScript, this becomes a tangle of loops, counters, timers, and conditionals:

```typescript
async function fetchWithRetry(url: string): Promise<Response> {
  let attempt = 0;
  const maxAttempts = 5;
  const startTime = Date.now();
  const maxTotalTime = 30_000;

  while (true) {
    try {
      const response = await fetch(url);
      if (response.status === 503) throw new Error("Service unavailable");
      return response;
    } catch (error) {
      attempt++;
      if (attempt >= maxAttempts) throw error;
      if (Date.now() - startTime > maxTotalTime) throw new Error("Total timeout");

      // Exponential backoff with jitter
      const baseDelay = Math.min(1000 * Math.pow(2, attempt), 16_000);
      const jitter = Math.random() * baseDelay * 0.1;
      await new Promise((r) => setTimeout(r, baseDelay + jitter));
    }
  }
}
```

Thirty lines of retry infrastructure. Now imagine you need different retry policies for different operations — one for database writes, one for HTTP calls, one for peer communication. You're copy-pasting and tweaking the loop everywhere.

And this is just retries. What about repeating an operation on a schedule — a health check every 10 seconds, a cache refresh every 5 minutes, a metrics report every minute? Another set of loops, another set of timer management.

Effect solves this with `Schedule` — a composable description of *when* and *how* to retry or repeat.

## Schedule: A Value That Describes Timing

A `Schedule<Out, In>` is a pure value that describes a recurring pattern. It doesn't run anything — it's a recipe that says "here's when to do the next iteration, here's how long to wait, here's when to stop." You compose schedules from primitives, then attach them to effects.

```typescript
import { Schedule, Effect } from "effect";

// Retry up to 3 times, no delay
const threeRetries = Schedule.recurs(3);

// Retry with 1-second spacing
const spacedRetries = Schedule.spaced("1 second");

// Retry with exponential backoff: 100ms, 200ms, 400ms, 800ms, ...
const exponential = Schedule.exponential("100 millis");

// Retry with Fibonacci delays: 100ms, 100ms, 200ms, 300ms, 500ms, 800ms, ...
const fibonacci = Schedule.fibonacci("100 millis");
```

Each of these is a value. Nothing has executed. No timers are running. They're descriptions, like everything else in Effect.

## Attaching Schedules to Effects

### retry — Try Again on Failure

```typescript
const fetchNode = (id: string): Effect.Effect<NodeRecord, NetworkError> =>
  Effect.tryPromise({
    try: () => fetch(`http://registry:8080/nodes/${id}`).then((r) => r.json()),
    catch: (e) => new NetworkError(String(e)),
  });

// Retry up to 3 times with exponential backoff
const resilientFetch = fetchNode("node-a").pipe(
  Effect.retry(Schedule.exponential("200 millis").pipe(
    Schedule.compose(Schedule.recurs(3))
  ))
);
```

`Effect.retry` re-executes the effect when it fails, following the schedule. When the schedule exhausts (e.g., `recurs(3)` after 3 retries), the last error propagates.

### repeat — Do It Again on Success

```typescript
// Check cluster health every 10 seconds, forever
const monitor = checkClusterHealth.pipe(
  Effect.repeat(Schedule.spaced("10 seconds"))
);

// Collect metrics 5 times with 1-second spacing
const fiveReadings = readMetric.pipe(
  Effect.repeat(Schedule.recurs(5).pipe(Schedule.compose(Schedule.spaced("1 second"))))
);
```

`Effect.repeat` re-executes the effect when it *succeeds*. It's the dual of `retry` — retry responds to failure, repeat responds to success.

### The retry/repeat Distinction

| | On success | On failure |
|---|---|---|
| `Effect.retry(e, s)` | Returns the value | Retries per schedule |
| `Effect.repeat(e, s)` | Repeats per schedule | Fails immediately |

This is clean and composable. Need retry *and* repeat? Combine them:

```typescript
// Check health, retry on failure, repeat forever on success
const robustMonitor = checkClusterHealth.pipe(
  Effect.retry(Schedule.exponential("1 second").pipe(
    Schedule.compose(Schedule.recurs(3))
  )),
  Effect.repeat(Schedule.spaced("10 seconds"))
);
```

If a health check fails, it retries up to 3 times with exponential backoff. If it ultimately succeeds, it waits 10 seconds and checks again. If it ultimately fails (all 3 retries exhausted), the repeat stops and the error propagates.

## Composing Schedules

The real power of `Schedule` comes from composition. Schedules are values that combine with operators — just like Effects compose with `flatMap` and Streams compose with `pipe`.

### Intersection — Both Must Continue

```typescript
// Exponential backoff, but at most 5 times
const boundedExponential = Schedule.intersect(
  Schedule.exponential("100 millis"),
  Schedule.recurs(5)
);
// Delays: 100ms, 200ms, 400ms, 800ms, 1600ms, then stop
// (takes the LONGER delay from each schedule at each step)
```

`Schedule.intersect` continues only while *both* schedules want to continue. At each step, it uses the longer of the two delays. This is how you add a retry limit to a delay strategy.

### Union — Either Can Continue

```typescript
// Use whichever schedule allows the next retry
const lenient = Schedule.union(
  Schedule.recurs(3),
  Schedule.spaced("5 seconds")
);
// Continues if EITHER schedule wants to continue
// Takes the SHORTER delay at each step
```

`Schedule.union` continues while *either* schedule wants to continue. It uses the shorter delay. This is less common but useful for "retry at least N times OR for at least T seconds."

### Capping Delays

```typescript
// Exponential backoff, but never wait more than 10 seconds
const capped = Schedule.exponential("100 millis").pipe(
  Schedule.either(Schedule.spaced("10 seconds"))
);

// Even simpler — use the built-in cap
const cappedSimple = Schedule.exponential("100 millis").pipe(
  Schedule.delayed(() => Duration.min(Duration.seconds(10)))
);
```

### Adding Jitter

Jitter prevents the "thundering herd" problem — when a service recovers and all clients retry simultaneously:

```typescript
const withJitter = Schedule.exponential("200 millis").pipe(
  Schedule.jittered  // Adds ±20% random variance to each delay
);
```

`Schedule.jittered` adds random noise to each delay. This is almost always what you want in a distributed system. Without jitter, exponential backoff can synchronize clients: all clients fail at t=0, all retry at t=200ms, all retry at t=400ms — hitting the recovering service in synchronized waves.

## Building Real Retry Policies

Let's compose a production-grade retry policy from primitives:

```typescript
import { Schedule, Duration, Effect, pipe } from "effect";

// Production retry policy:
// - Exponential backoff starting at 200ms
// - With ±20% jitter
// - Maximum delay capped at 10 seconds
// - At most 5 retries
// - Total elapsed time at most 30 seconds
const productionRetryPolicy = pipe(
  Schedule.exponential("200 millis"),
  Schedule.jittered,
  Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.seconds(10))),
  Schedule.intersect(Schedule.recurs(5)),
  Schedule.intersect(Schedule.elapsed.pipe(
    Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.seconds(30)))
  ))
);

// Attach it
const resilientCall = apiCall.pipe(
  Effect.retry(productionRetryPolicy)
);
```

Read the composition top to bottom:

1. Start with exponential backoff from 200ms
2. Add jitter to prevent thundering herd
3. Cap individual delays at 10 seconds
4. Stop after 5 retries
5. Stop if 30 seconds total have elapsed

Each line adds one constraint. The composition is declarative and readable. Compare this to the 30-line `while` loop we started with — and note that this version is *reusable* across any Effect.

### Conditional Retry

Sometimes you only want to retry certain errors:

```typescript
class TransientError {
  readonly _tag = "TransientError" as const;
  constructor(readonly message: string) {}
}

class PermanentError {
  readonly _tag = "PermanentError" as const;
  constructor(readonly message: string) {}
}

// Only retry transient errors
const selectiveRetry = apiCall.pipe(
  Effect.retry({
    schedule: productionRetryPolicy,
    while: (error) => error._tag === "TransientError",
  })
);
// PermanentError fails immediately — no retries wasted
```

The `while` predicate inspects each error. If it returns `false`, the retry stops and the error propagates immediately. This is essential: retrying a 404 Not Found is pointless, but retrying a 503 Service Unavailable makes sense.

## Schedule Outputs

Every schedule produces an output at each step, which can be observed and used for logging or decision-making:

```typescript
// recurs outputs the attempt number
const withLogging = apiCall.pipe(
  Effect.retry(
    Schedule.recurs(3).pipe(
      Schedule.tapOutput((attempt) =>
        Effect.log(`Retry attempt ${attempt}`)
      )
    )
  )
);

// exponential outputs the current delay duration
const withDelayLogging = apiCall.pipe(
  Effect.retry(
    Schedule.exponential("200 millis").pipe(
      Schedule.intersect(Schedule.recurs(5)),
      Schedule.tapOutput(([duration, attempt]) =>
        Effect.log(`Attempt ${attempt}, next delay: ${Duration.toMillis(duration)}ms`)
      )
    )
  )
);

// elapsed outputs total time since first attempt
const withElapsedLogging = apiCall.pipe(
  Effect.retry(
    Schedule.exponential("200 millis").pipe(
      Schedule.intersect(Schedule.elapsed),
      Schedule.tapOutput(([_, elapsed]) =>
        Effect.log(`Total elapsed: ${Duration.toMillis(elapsed)}ms`)
      )
    )
  )
);
```

Schedule outputs are typed — `Schedule.recurs` outputs `number`, `Schedule.exponential` outputs `Duration`, `Schedule.elapsed` outputs `Duration`. When you intersect two schedules, the output is a tuple of both outputs. The types compose, just like error types on Effects.

## Schedules Are the Pattern Again

Let's step back and see the recurring theme. A `Schedule` is:

- A **description** of a timing policy — nothing runs until attached to an effect
- **Composable** — `intersect`, `union`, `pipe` build complex policies from simple ones
- **Typed** — outputs carry type information that composes through operators

This is the same "effects as data" principle we've seen on every type:

| Type | Describes | Composed with | Executed by |
|---|---|---|---|
| `Effect<A, E, R>` | A computation | `flatMap`, `zip`, `pipe` | `runPromise` |
| `Layer<Out, E, In>` | A dependency recipe | `provide`, `merge` | `Effect.provide` |
| `Stream<A, E, R>` | A sequence | `map`, `flatMap`, `merge` | `run` + Sink |
| `Schedule<Out, In>` | A timing policy | `intersect`, `union`, `pipe` | `retry` / `repeat` |

Schedules aren't a special retry library bolted onto Effect. They're another instance of the same pattern: pure values describing behavior, composed before execution.

## A Realistic Example: Resilient Peer Communication

Let's build a peer communication layer with different retry strategies for different operations:

```typescript
import { Effect, Schedule, Duration, pipe } from "effect";

// --- Retry policies for different scenarios ---

// Heartbeats: fast retry, short window (connectivity check)
const heartbeatRetry = pipe(
  Schedule.exponential("50 millis"),
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3)),
);

// Data replication: patient retry, longer window (data must eventually arrive)
const replicationRetry = pipe(
  Schedule.exponential("500 millis"),
  Schedule.jittered,
  Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.seconds(30))),
  Schedule.intersect(Schedule.recurs(10)),
);

// Service registration: very patient, with cap (critical but not time-sensitive)
const registrationRetry = pipe(
  Schedule.exponential("1 second"),
  Schedule.jittered,
  Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.minutes(1))),
  Schedule.intersect(Schedule.elapsed.pipe(
    Schedule.whileOutput(Duration.lessThanOrEqualTo(Duration.minutes(5)))
  )),
);

// --- Operations with their retry policies ---

const sendHeartbeat = (peerId: string) =>
  pingPeer(peerId).pipe(
    Effect.retry(heartbeatRetry),
    Effect.catchAll((error) =>
      Effect.gen(function* () {
        yield* Effect.logWarning(`Heartbeat to ${peerId} failed after retries: ${error._tag}`);
        yield* markPeerUnreachable(peerId);
      })
    )
  );

const replicateData = (key: string, value: string, targetPeer: string) =>
  sendToPeer(targetPeer, { type: "replicate", key, value }).pipe(
    Effect.retry({
      schedule: replicationRetry,
      while: (error) => error._tag !== "PeerRejectedError", // Don't retry rejections
    }),
    Effect.tapError((error) =>
      Effect.logError(`Replication of ${key} to ${targetPeer} failed: ${error._tag}`)
    )
  );

const registerWithDiscovery = (serviceInfo: ServiceInfo) =>
  registerService(serviceInfo).pipe(
    Effect.retry(registrationRetry),
    Effect.tap(() => Effect.log("Successfully registered with discovery service")),
    Effect.tapError((error) =>
      Effect.logError(`Registration failed permanently: ${error._tag}`)
    )
  );

// --- Periodic operations ---

// Heartbeat every peer every 5 seconds
const heartbeatLoop = (peerIds: string[]) =>
  Effect.forEach(peerIds, sendHeartbeat, { concurrency: "unbounded" }).pipe(
    Effect.repeat(Schedule.spaced("5 seconds"))
  );

// Re-register every 60 seconds (lease renewal)
const registrationRenewal = (serviceInfo: ServiceInfo) =>
  registerWithDiscovery(serviceInfo).pipe(
    Effect.repeat(Schedule.spaced("60 seconds"))
  );

// Metrics emission: every 10 seconds, with a different strategy
const metricsLoop = collectAndEmitMetrics.pipe(
  Effect.repeat(
    Schedule.spaced("10 seconds").pipe(
      Schedule.tapOutput(() => Effect.log("Metrics emitted"))
    )
  ),
  Effect.retry(Schedule.forever) // If emission fails, retry immediately and resume schedule
);
```

Each operation has a retry policy tuned to its needs. Heartbeats fail fast (50ms base, 3 retries). Replication is patient (500ms base, 10 retries, 30s cap). Registration is very patient (1s base, 5 minutes total). The policies are named values, reusable, testable.

And the periodic operations — heartbeat loop, registration renewal, metrics emission — use `Effect.repeat` with schedules. The heartbeat loop also uses `Effect.forEach` with unbounded concurrency (from Chapter 7) to ping all peers simultaneously.

## Exercises

### Exercise 9.1: Basic Schedule Composition

Build a retry schedule that:
- Starts with a 100ms delay
- Doubles each time (exponential)
- Retries at most 4 times

```typescript
const mySchedule: Schedule.Schedule<unknown, unknown> = // todo()

// Test it by attaching to a failing effect and logging each attempt
const test = Effect.gen(function* () {
  let attempt = 0;
  yield* Effect.failSync(() => {
    attempt++;
    console.log(`Attempt ${attempt} at ${Date.now()}`);
    return new Error("always fails");
  }).pipe(Effect.retry(mySchedule), Effect.catchAll(() => Effect.void));
});
```

### Exercise 9.2: Capped Exponential with Jitter

Build the "production-grade" retry policy described in the chapter:
- Exponential backoff from 200ms
- Jitter
- Max delay 10 seconds
- Max 5 retries
- Max 30 seconds total elapsed

```typescript
const productionPolicy = // todo()

// Verify by logging each retry with its delay and total elapsed time
```

### Exercise 9.3: Conditional Retry by Error Type

Create an Effect that randomly fails with either `TransientError` or `PermanentError`. Retry only on transient errors, with exponential backoff.

```typescript
class TransientError {
  readonly _tag = "TransientError" as const;
  constructor(readonly message: string) {}
}

class PermanentError {
  readonly _tag = "PermanentError" as const;
  constructor(readonly message: string) {}
}

const unreliableCall: Effect.Effect<string, TransientError | PermanentError> = {
  // todo() — randomly fail with one or the other, sometimes succeed
};

const withSelectiveRetry: Effect.Effect<string, TransientError | PermanentError> = {
  // todo()
  // Retry on TransientError with exponential backoff (max 3)
  // Fail immediately on PermanentError
};
```

### Exercise 9.4: Periodic with Retry

Build a health check that runs every 15 seconds. If a check fails, retry up to 3 times with 1-second spacing before reporting unhealthy. Then continue the 15-second loop regardless.

```typescript
const periodicHealthCheck = (nodeId: string): Effect.Effect<void> => {
  // todo()
  // Inner: check health, retry 3 times on failure
  // Outer: repeat every 15 seconds
  // On final failure: log warning but continue the loop
};
```

### Exercise 9.5: Schedule Outputs for Monitoring

Build a retry wrapper that logs detailed information about each retry attempt using schedule outputs:

```typescript
const verboseRetry = <A, E extends { _tag: string }>(
  effect: Effect.Effect<A, E>,
  policy: Schedule.Schedule<unknown, E>
): Effect.Effect<A, E> => {
  // todo()
  // Log for each retry:
  //   "Retry #N after Xms (total elapsed: Yms) — error: <tag>"
  //
  // Hint: compose the policy with Schedule.recurs and Schedule.elapsed
  //       to get attempt number and total time as outputs
  // Hint: use Schedule.tapInput to access the error
  // Hint: use Schedule.tapOutput to log
};
```

### Exercise 9.6: Different Policies for Different Services

Build a function that creates a retry policy based on the criticality of the operation:

```typescript
type Criticality = "low" | "medium" | "high" | "critical";

const policyFor = (criticality: Criticality): Schedule.Schedule<unknown, unknown> => {
  // todo()
  // low:      3 retries, 100ms exponential, no jitter
  // medium:   5 retries, 200ms exponential, jitter, 10s cap
  // high:     10 retries, 500ms exponential, jitter, 30s cap, 2 min total
  // critical: 20 retries, 1s exponential, jitter, 60s cap, 10 min total
};

// Usage:
const fetchConfig = loadRemoteConfig.pipe(
  Effect.retry(policyFor("critical"))
);

const fetchCachedData = loadFromCache.pipe(
  Effect.retry(policyFor("low"))
);
```

### Exercise 9.7 (Challenge): Circuit Breaker

Build a circuit breaker using `Schedule` and `Ref`. A circuit breaker tracks failure rates and "opens" (stops retrying) when failures exceed a threshold, then "half-opens" after a cooldown to let one request through.

```typescript
type CircuitState = "closed" | "open" | "half-open";

interface CircuitBreaker {
  readonly call: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E | CircuitOpenError>;
  readonly state: Effect.Effect<CircuitState>;
}

class CircuitOpenError {
  readonly _tag = "CircuitOpenError" as const;
  constructor(readonly cooldownMs: number) {}
}

const makeCircuitBreaker = (config: {
  readonly failureThreshold: number;   // Open after this many failures
  readonly cooldownMs: number;         // Wait this long before half-open
  readonly successThreshold: number;   // Close after this many successes in half-open
}): Effect.Effect<CircuitBreaker> => {
  // todo()
  //
  // State machine:
  // CLOSED: normal operation, count consecutive failures
  //   → if failures >= threshold → OPEN
  // OPEN: reject all calls with CircuitOpenError
  //   → after cooldownMs → HALF-OPEN
  // HALF-OPEN: allow one call through
  //   → if success → CLOSED
  //   → if failure → OPEN
  //
  // Hint: use Ref to track state and failure count
  // Hint: use Effect.sync(() => Date.now()) to track when circuit opened
};
```

*The circuit breaker pattern is essential for distributed systems — it prevents a failing downstream service from consuming all your retry budget and cascading failures through the system. Netflix's Hystrix popularized it; Effect lets you build it from primitives.*

## Summary

`Schedule` is a composable description of timing — when to retry, when to repeat, how long to wait, when to stop. Like everything in Effect, it's a pure value composed before execution. You build complex policies from simple primitives: exponential backoff, jitter, caps, limits, and elapsed time bounds.

The separation between *what* to do (the Effect) and *when* to do it (the Schedule) is fundamental. The same Effect can be retried with different policies in different contexts. The same policy can be applied to different Effects. They compose independently.

| Primitive | What it does |
|---|---|
| `Schedule.recurs(n)` | Stop after n repetitions |
| `Schedule.spaced(d)` | Fixed delay between each |
| `Schedule.exponential(d)` | Doubling delay |
| `Schedule.fibonacci(d)` | Fibonacci delay sequence |
| `Schedule.forever` | Never stop |
| `Schedule.elapsed` | Track total elapsed time |

| Combinator | What it does |
|---|---|
| `Schedule.intersect(a, b)` | Continue while both continue (longer delay) |
| `Schedule.union(a, b)` | Continue while either continues (shorter delay) |
| `Schedule.jittered` | Add random variance to delays |
| `Schedule.whileOutput(pred)` | Continue while output satisfies predicate |
| `Schedule.tapOutput(f)` | Observe each step (for logging) |
| `Schedule.tapInput(f)` | Observe each input (the error or value) |
| `Schedule.delayed(f)` | Transform the delay at each step |

## Quick Reference

```typescript
import { Schedule, Effect, Duration } from "effect";

// --- Primitives ---
Schedule.recurs(n)                         // n repetitions
Schedule.once                              // Exactly one retry
Schedule.forever                           // Never stop
Schedule.spaced("1 second")               // Fixed delay
Schedule.exponential("100 millis")         // Doubling delay
Schedule.fibonacci("100 millis")           // Fibonacci delay
Schedule.elapsed                           // Total elapsed time

// --- Composition ---
Schedule.intersect(a, b)                   // Both must continue
Schedule.union(a, b)                       // Either can continue
schedule.pipe(Schedule.jittered)           // Add ±20% noise
schedule.pipe(Schedule.whileOutput(pred))  // Conditional stop
schedule.pipe(Schedule.delayed(f))         // Transform delays

// --- Attaching ---
effect.pipe(Effect.retry(schedule))        // Retry on failure
effect.pipe(Effect.repeat(schedule))       // Repeat on success
effect.pipe(Effect.retry({                 // Conditional retry
  schedule: mySchedule,
  while: (error) => isRetryable(error),
}))

// --- Observing ---
schedule.pipe(Schedule.tapOutput(f))       // Log each step
schedule.pipe(Schedule.tapInput(f))        // Log each trigger
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 10 — The Grand Unification, where we step back and reveal the recurring patterns that tie every chapter together: Functor, Applicative, Monad — not as abstract theory, but as the practical patterns you've already been using throughout this entire series.*
