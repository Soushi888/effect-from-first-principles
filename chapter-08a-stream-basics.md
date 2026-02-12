# Chapter 8a: Streams — Lazy, Effectful Sequences

> **Building on:** This chapter uses `Effect.gen`, `pipe`, typed errors, `acquireRelease`, `Layer`, `Fiber.fork`, `Queue`, and `Ref` from Chapters 3–7. Key types: `Effect<A, E, R>`, `Scope`, `Layer`, `Fiber`, `Queue`, `Ref`.

## Beyond Single Values

So far, every Effect we've built produces one value. `fetchUser` returns one user. `pingNode` returns one health status. `registerService` returns one record. But distributed systems constantly deal with *sequences* of values over time:

- A message queue delivers a continuous flow of events
- A database query streams rows rather than loading everything into memory
- A replication log is an append-only sequence of changes
- A cluster monitoring system produces health snapshots every 10 seconds
- A file contains thousands of lines to process one by one

You could model these as `Effect<Array<A>>`, but that forces the entire collection into memory before processing starts. For a million-row query or an infinite event stream, that's not viable.

You could use a callback or an async iterator:

```typescript
// Async iterator — the built-in JS approach
async function* readLines(path: string): AsyncGenerator<string> {
  const file = await fs.open(path, "r");
  try {
    for await (const line of file.readLines()) {
      yield line;
    }
  } finally {
    await file.close();
  }
}

// But: no typed errors, no backpressure, no concurrency control,
// no composable resource management, no retry, no timeout per element.
// And combining two async generators? Manual, painful, error-prone.
```

Effect's `Stream` type fills this gap. A `Stream<A, E, R>` is a lazy, potentially infinite sequence of values that:

- Produces values of type `A`
- Can fail with typed errors `E`
- Requires services `R`
- Manages resources safely (files close, connections drop, even on failure)
- Supports backpressure (a slow consumer slows the producer)
- Composes with `map`, `flatMap`, `filter`, `zip` — the same toolkit, once again

## Creating Streams

### From Known Values

```typescript
import { Stream, Effect } from "effect";

// From explicit values
const greetings = Stream.make("hello", "world", "from", "the", "cluster");

// From an iterable
const numbers = Stream.fromIterable([1, 2, 3, 4, 5]);

// A single value
const one = Stream.succeed(42);

// A stream that fails immediately
const failed = Stream.fail(new ConnectionError("peer-a"));

// An empty stream (completes with zero elements)
const empty = Stream.empty;
```

### From Effects

A stream where each element is produced by an Effect:

```typescript
// Repeat a single effect, producing its result each time
const timestamps = Stream.repeatEffect(
  Effect.sync(() => Date.now())
);
// Infinite stream: 1707123456789, 1707123456790, 1707123456791, ...

// Repeat with a schedule (every 5 seconds)
const periodicPing = Stream.repeatEffectWithSchedule(
  pingNode("primary"),
  Schedule.spaced("5 seconds")
);
```

### unfold — Building Streams from State

`unfold` is the fundamental stream constructor. It takes an initial state and a function that produces the next element and the next state — or signals completion:

```typescript
// Count from 1 to 10
const counting = Stream.unfold(1, (n) =>
  n <= 10 ? Option.some([n, n + 1] as const) : Option.none()
);
// Produces: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

// Fibonacci sequence (infinite)
const fibonacci = Stream.unfold([0, 1] as const, ([a, b]) =>
  Option.some([a, [b, a + b] as const] as const)
);
// Produces: 0, 1, 1, 2, 3, 5, 8, 13, 21, ...

// Paginated API — unfold through pages
const allPages = Stream.unfoldEffect(
  1, // Start at page 1
  (page) =>
    fetchPage(page).pipe(
      Effect.map((response) =>
        response.data.length === 0
          ? Option.none()  // No more data — stop
          : Option.some([response.data, page + 1] as const)
      )
    )
);
// Produces: [page1Items], [page2Items], ..., until empty page
```

`unfold` is to streams what recursion is to functions — the primitive from which all other construction patterns can be built. `unfoldEffect` is the effectful variant, where each step can perform I/O.

If you recall Chapter 2's `traverse` and the Red Book's lazy lists, `unfold` is the same idea: build a sequence from a state transition function, producing elements lazily.

### From Asynchronous Sources

```typescript
// From an async iterable (e.g., Node.js readable stream)
const lines = Stream.fromAsyncIterable(
  readableStream[Symbol.asyncIterator](),
  (error) => new ReadError(String(error))
);

// From a Queue (connects to Chapter 7's producer-consumer)
const fromQueue = <A>(queue: Queue.Queue<A>): Stream.Stream<A> =>
  Stream.fromQueue(queue);

// From a callback-based event emitter
const events = Stream.async<PeerEvent, ConnectionError>((emit) => {
  peerSocket.on("data", (data) => emit(Effect.succeed(Chunk.of(data))));
  peerSocket.on("error", (err) => emit(Effect.fail(Option.some(new ConnectionError(err)))));
  peerSocket.on("end", () => emit(Effect.fail(Option.none()))); // Signal completion
});
```

`Stream.async` bridges callback-based APIs into the stream world. The `emit` callback accepts an Effect that produces a `Chunk` of elements (for batching) or fails with `Option.none()` to signal completion.

## The Familiar Toolkit

Here it comes again. The same operations we've built on `Option`, `Either`, `Effect` — they all appear on `Stream`, with the same semantics.

### map — Transform Each Element

```typescript
const nodeIds = Stream.make("node-a", "node-b", "node-c");

const endpoints = nodeIds.pipe(
  Stream.map((id) => `https://${id}.cluster.local:8080`)
);
// Produces: "https://node-a.cluster.local:8080", ...
```

### filter — Keep Elements That Match

```typescript
const healthyOnly = healthStatuses.pipe(
  Stream.filter((status) => status.healthy)
);
```

### flatMap — Chain Streams

`flatMap` on a stream is powerful: for each element of the source, produce an entire sub-stream, and concatenate the results:

```typescript
// For each node, stream all its keys
const allKeys: Stream.Stream<KeyRecord, QueryError, DatabasePool> =
  nodeIds.pipe(
    Stream.flatMap((nodeId) => streamNodeKeys(nodeId))
  );

// If nodeIds is ["a", "b", "c"] and each node has 1000 keys,
// allKeys produces 3000 KeyRecord values, lazily
```

This is the monadic bind for streams — the same `flatMap` pattern, but each step can produce multiple values.

### take, takeWhile, drop — Slice the Stream

```typescript
const firstTen = infiniteStream.pipe(Stream.take(10));

const untilError = healthChecks.pipe(
  Stream.takeWhile((s) => s.healthy)
);

const skipHeader = csvLines.pipe(Stream.drop(1));
```

These operations work on infinite streams — `take(10)` stops after 10 elements, even if the source is infinite. The upstream is interrupted, resources are cleaned up.

### scan — Running Accumulation

`scan` is like `reduce`, but emits every intermediate result:

```typescript
// Running total of message sizes
const runningTotal = messageSizes.pipe(
  Stream.scan(0, (total, size) => total + size)
);
// Input:  10, 20, 30, 40
// Output: 0, 10, 30, 60, 100
```

`scan` appeared in the Red Book's lazy list chapter for exactly this use: incremental aggregation over a sequence. Here it works on effectful, potentially infinite streams.

### zip — Combine Two Streams Element-Wise

```typescript
const names = Stream.make("alice", "bob", "charlie");
const scores = Stream.make(95, 87, 92);

const paired = Stream.zip(names, scores);
// Produces: ["alice", 95], ["bob", 87], ["charlie", 92]

// Stops when either stream ends — same as zip on Option
```

### The Pattern Table Grows

| Operation | Option | Either | Effect | Stream |
|---|---|---|---|---|
| Wrap value(s) | `some(a)` | `right(a)` | `succeed(a)` | `make(a, b, ...)` |
| Failure | `none` | `left(e)` | `fail(e)` | `fail(e)` |
| Transform | `map` | `map` | `map` | `map` |
| Chain | `flatMap` | `flatMap` | `flatMap` | `flatMap` |
| Combine | `zip` | `zip` | `zip` | `zip` |
| Extract / consume | `match` | `match` | `runPromise` | `run` + Sink |

Same algebra. Different context. `Option` handles absence. `Either` handles typed errors. `Effect` handles async + errors + dependencies. `Stream` handles *sequences* of all the above.

## Consuming Streams with Sinks

A stream produces values. A `Sink` consumes them and produces a final result. Think of a Sink as the stream equivalent of `reduce`:

```typescript
import { Stream, Sink } from "effect";

// Collect all elements into an array
const allItems = Stream.make(1, 2, 3, 4, 5).pipe(
  Stream.run(Sink.collectAll())
);
// Effect<Chunk<number>>

// Fold into a single value
const sum = Stream.make(1, 2, 3, 4, 5).pipe(
  Stream.run(Sink.fold(0, () => true, (acc, n) => acc + n))
);
// Effect<number> → 15

// Execute a side effect for each element
const logged = eventStream.pipe(
  Stream.run(Sink.forEach((event) => Effect.log(`Event: ${event.type}`)))
);
// Effect<void>

// Take only the first element
const first = eventStream.pipe(
  Stream.run(Sink.head())
);
// Effect<Option<Event>>
```

`Stream.run(sink)` converts a `Stream<A, E, R>` into an `Effect<B, E, R>` — collapsing the sequence into a single result. The stream is consumed lazily; elements are pulled through the pipeline one chunk at a time.

For many common cases, convenience methods exist directly on Stream:

```typescript
// These are equivalent:
Stream.runCollect(stream)           // same as Stream.run(stream, Sink.collectAll())
Stream.runForEach(stream, f)        // same as Stream.run(stream, Sink.forEach(f))
Stream.runFold(stream, init, f)     // same as Stream.run(stream, Sink.fold(init, ...))
Stream.runHead(stream)              // same as Stream.run(stream, Sink.head())
```

## Exercises

### Exercise 8.1: Build a Stream with unfold

Create a stream that generates powers of 2 up to a maximum value:

```typescript
const powersOfTwo = (max: number): Stream.Stream<number> => {
  // todo()
  // Hint: unfold with state being the current power
  // Stop when the value exceeds max
};

// Test: powersOfTwo(100) should produce 1, 2, 4, 8, 16, 32, 64
```

### Exercise 8.2: Stream Transformations

Given a stream of log lines, build a pipeline that:
1. Filters to only ERROR and WARN lines
2. Extracts the timestamp and message
3. Takes the first 50 matching lines

```typescript
interface LogEntry {
  readonly timestamp: string;
  readonly level: string;
  readonly message: string;
}

const parseLogLine = (line: string): Option.Option<LogEntry> => {
  // todo() — parse "[2024-01-15T10:30:00Z] ERROR: something went wrong"
};

const processLogs = (
  lines: Stream.Stream<string>
): Stream.Stream<LogEntry> => {
  // todo()
  // 1. Filter to lines containing "ERROR" or "WARN"
  // 2. Parse each line with parseLogLine
  // 3. Filter out None results (use Stream.filterMap)
  // 4. Take first 50
};
```

### Exercise 8.3: Paginated API as a Stream

Convert a paginated REST API into a single continuous stream. The API returns `{ items: T[], nextCursor: string | null }`.

```typescript
interface Page<T> {
  readonly items: T[];
  readonly nextCursor: string | null;
}

class ApiError {
  readonly _tag = "ApiError" as const;
  constructor(readonly status: number, readonly message: string) {}
}

declare const fetchPage: (cursor: string | null) => Effect.Effect<Page<NodeRecord>, ApiError>;

const allRecords: Stream.Stream<NodeRecord, ApiError> = {
  // todo()
  // Hint: Stream.unfoldChunkEffect is perfect for paginated APIs
  // State is the cursor (string | null)
  // Each step fetches a page and emits its items as a Chunk
  // Stop when nextCursor is null
};
```

## Summary

`Stream` extends the Effect algebra to sequences of values. The same `map`, `flatMap`, `filter`, `zip`, and `fold` patterns we've seen on `Option`, `Either`, and `Effect` appear again — now operating on lazy, potentially infinite, resource-safe sequences.

Streams connect naturally to the rest of Effect: they use `Scope` for resource management, `Queue` for concurrent communication, `Layer` for dependencies, and the same typed error system.

| Need | Array | AsyncGenerator | Stream |
|---|---|---|---|
| Lazy evaluation | ✗ (eager) | ✓ | ✓ |
| Typed errors | ✗ | ✗ | ✓ |
| Resource safety | ✗ | Manual | Automatic |
| Backpressure | N/A | Partial | Built-in |
| Concurrent processing | ✗ | ✗ | `mapEffect` + concurrency |
| Batching | Manual | Manual | `groupedWithin` |
| Merge multiple sources | Manual | Manual | `Stream.merge` |
| Composable | `.map`/`.filter` | Awkward | `pipe` + full toolkit |

## Quick Reference

```typescript
import { Stream, Sink, Chunk } from "effect";

// --- Constructors ---
Stream.make(a, b, c)                          // From values
Stream.fromIterable(array)                    // From iterable
Stream.unfold(init, step)                     // From state function
Stream.unfoldEffect(init, step)               // Effectful unfold
Stream.unfoldChunkEffect(init, step)          // Unfold producing chunks
Stream.repeatEffect(effect)                   // Repeat an effect
Stream.fromQueue(queue)                       // From a Queue

// --- Transformations ---
Stream.map(stream, f)                         // Transform elements
Stream.mapEffect(stream, f, { concurrency })  // Effectful transform
Stream.filter(stream, predicate)              // Keep matching
Stream.filterMap(stream, f)                   // Filter + map (Option)
Stream.flatMap(stream, f)                     // Chain sub-streams
Stream.scan(stream, init, f)                  // Running accumulation
Stream.take(stream, n)                        // First n elements
Stream.takeWhile(stream, pred)                // Take while true
Stream.drop(stream, n)                        // Skip first n

// --- Combining ---
Stream.zip(a, b)                              // Element-wise pairing

// --- Consuming ---
Stream.runCollect(stream)                     // → Effect<Chunk<A>>
Stream.runForEach(stream, f)                  // → Effect<void>
Stream.runFold(stream, init, f)               // → Effect<B>
Stream.runHead(stream)                        // → Effect<Option<A>>
Stream.runDrain(stream)                       // → Effect<void> (discard values)
Stream.run(stream, sink)                      // Custom Sink
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 8b — Stream Patterns for Distributed Systems, where we explore resource-safe streams, chunking, merging, and build a realistic distributed event processor.*
