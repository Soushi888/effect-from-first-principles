# Chapter 8b: Stream Patterns for Distributed Systems

> **Building on:** This chapter uses `Stream` constructors, `map`, `flatMap`, `filter`, `Sink`, and `Stream.run` from Chapter 8a, plus `acquireRelease` (Ch5), `Queue` (Ch7), and `Layer` (Ch6).

In Chapter 8a we covered the fundamentals: creating streams, the familiar transformation toolkit (`map`, `flatMap`, `filter`, `zip`, `scan`), and consuming streams with Sinks. Now we turn to the patterns that make streams indispensable in production distributed systems: resource safety, batching, merging concurrent sources, and connecting streams to queues.

## Resource-Safe Streams

Streams often operate on resources — files, connections, subscriptions. `Stream.acquireRelease` creates a stream that acquires a resource, uses it, and guarantees cleanup:

```typescript
const fileLines = (path: string): Stream.Stream<string, FileError> =>
  Stream.acquireRelease(
    Effect.try({
      try: () => fs.openSync(path, "r"),
      catch: (e) => new FileError(path, "open", String(e)),
    }),
    (fd) => Effect.sync(() => fs.closeSync(fd))
  ).pipe(
    Stream.flatMap((fd) =>
      Stream.unfoldEffect(undefined, () =>
        Effect.try({
          try: () => {
            const line = readLineSync(fd);
            return line === null
              ? Option.none()
              : Option.some([line, undefined] as const);
          },
          catch: (e) => new FileError(path, "read", String(e)),
        })
      )
    )
  );

// Usage — the file is closed when the stream completes, fails, or is interrupted
const program = fileLines("/var/log/cluster.log").pipe(
  Stream.filter((line) => line.includes("ERROR")),
  Stream.take(100),  // File closes after 100 error lines, even if the file is huge
  Stream.runCollect
);
```

When `take(100)` stops the stream, the finalizer runs and the file closes. If the stream fails, the file closes. If a downstream consumer is interrupted, the file closes. Resources are managed by the same `Scope` system from Chapter 5.

## Chunking and Batching

Streams in Effect operate on `Chunk<A>` internally — contiguous arrays that are more efficient than processing individual elements. This is transparent most of the time, but you can control chunking explicitly for performance:

```typescript
// Group elements into fixed-size batches
const batched = eventStream.pipe(
  Stream.grouped(100)
);
// Stream<Chunk<Event>> — each element is a batch of 100

// Buffer and flush based on count or time
const buffered = eventStream.pipe(
  Stream.groupedWithin(100, "1 second")
);
// Emits a batch when 100 elements accumulate OR 1 second passes, whichever comes first
// Essential for write-coalescing in distributed systems
```

`groupedWithin` is a workhorse for distributed applications. Imagine writing to a database: you don't want to insert one row at a time (too many round trips) or buffer forever (too much latency). `groupedWithin(100, "1 second")` gives you the best of both: batch up to 100 items, but flush at least every second.

## Merging and Interleaving

Multiple streams can be combined in different ways:

```typescript
// Concatenate: first stream, then second stream (sequential)
const all = Stream.concat(streamA, streamB);

// Merge: interleave elements from both as they arrive (concurrent)
const merged = Stream.merge(streamA, streamB);

// Merge many streams
const allPeerEvents = Stream.mergeAll(
  peerStreams,
  { concurrency: 10 }
);
```

`Stream.merge` is concurrent — it pulls from both streams simultaneously and emits elements as they arrive. This is how you combine event streams from multiple peer nodes into a single unified stream.

## A Realistic Example: Distributed Event Processor

Let's build an event processor that reads from a message queue, enriches events with data from a database, and writes results to an output topic — all as a streaming pipeline with resource safety, backpressure, and batched writes.

```typescript
import { Stream, Effect, Sink, Queue, Chunk } from "effect";

// --- Types ---
interface RawEvent {
  readonly eventId: string;
  readonly nodeId: string;
  readonly type: string;
  readonly payload: unknown;
}

interface EnrichedEvent extends RawEvent {
  readonly nodeName: string;
  readonly region: string;
  readonly processedAt: Date;
}

class ProcessingError {
  readonly _tag = "ProcessingError" as const;
  constructor(readonly eventId: string, readonly cause: string) {}
}

// --- Stream pipeline ---
const eventProcessor = Effect.gen(function* () {
  const db = yield* DatabasePool;
  const consumer = yield* MessageConsumer;
  const producer = yield* MessageProducer;

  // Step 1: Source — read events from the consumer as a stream
  const rawEvents: Stream.Stream<RawEvent, ConsumerError> =
    Stream.repeatEffect(consumer.receive());

  // Step 2: Enrich — look up node metadata from the database
  const enriched = rawEvents.pipe(
    Stream.mapEffect((event) =>
      Effect.gen(function* () {
        const node = yield* db.query(
          `SELECT name, region FROM nodes WHERE id = '${event.nodeId}'`
        );
        const meta = node[0] as { name: string; region: string } | undefined;
        return {
          ...event,
          nodeName: meta?.name ?? "unknown",
          region: meta?.region ?? "unknown",
          processedAt: new Date(),
        } satisfies EnrichedEvent;
      }).pipe(
        Effect.catchAll((e) =>
          Effect.succeed({
            ...event,
            nodeName: "lookup-failed",
            region: "unknown",
            processedAt: new Date(),
          } satisfies EnrichedEvent)
        )
      )
    , { concurrency: 5 }) // Enrich up to 5 events concurrently
  );

  // Step 3: Batch — group into batches for efficient writes
  const batched = enriched.pipe(
    Stream.groupedWithin(50, "2 seconds")
  );

  // Step 4: Sink — write each batch to the output topic
  yield* batched.pipe(
    Stream.runForEach((batch) =>
      Effect.gen(function* () {
        yield* producer.sendBatch(
          Chunk.toReadonlyArray(batch).map((e) => ({
            topic: "enriched-events",
            key: e.nodeId,
            value: JSON.stringify(e),
          }))
        );
        yield* Effect.log(`Wrote batch of ${Chunk.size(batch)} events`);
      })
    )
  );
});

// Wire up and run
const program = eventProcessor.pipe(
  Effect.provide(MainLayer),
  Effect.catchAll((e) => Effect.logError(`Pipeline failed: ${e}`)),
);

Effect.runPromise(program);
```

This pipeline:

- **Streams lazily** — only pulls from the queue as fast as it can process
- **Enriches concurrently** — up to 5 database lookups at a time
- **Batches intelligently** — 50 events or 2 seconds, whichever comes first
- **Handles errors per-element** — a failed lookup doesn't crash the pipeline
- **Cleans up resources** — when the pipeline stops (or is interrupted), consumers and producers disconnect

The entire thing reads as a declarative pipeline: source → enrich → batch → write. Each stage is a stream transformation. The composition is the same `pipe` we've used since Chapter 2.

## Connecting Streams to Queues

Streams and Queues from Chapter 7 interoperate naturally:

```typescript
const bridged = Effect.gen(function* () {
  const queue = yield* Queue.bounded<PeerEvent>(1000);

  // Producer side: push events into the queue from a callback source
  yield* Effect.fork(
    Effect.forever(
      Effect.gen(function* () {
        const event = yield* receiveFromPeer();
        yield* Queue.offer(queue, event);
      })
    )
  );

  // Consumer side: drain the queue as a stream and process it
  yield* Stream.fromQueue(queue).pipe(
    Stream.filter((e) => e.type === "data"),
    Stream.mapEffect(processEvent, { concurrency: 3 }),
    Stream.runDrain
  );
});
```

`Stream.fromQueue` converts a Queue into a Stream, giving you the full stream toolkit (filter, map, batch, merge) on top of the concurrent queue from Chapter 7.

## Exercises

### Exercise 8.4: Resource-Safe File Processing

Build a stream pipeline that reads lines from an input file, transforms them, and writes to an output file. Both files must be closed on completion, failure, or interruption.

```typescript
const transformFile = (
  inputPath: string,
  outputPath: string,
  transform: (line: string) => string
): Effect.Effect<number, FileError> => {
  // todo()
  // 1. Create a resource-safe stream from the input file
  // 2. Transform each line
  // 3. Write to the output file (use a Sink or runForEach)
  // 4. Return the number of lines processed
  //
  // Hint: use Stream.acquireRelease for both files
  // Hint: use Ref to count processed lines
};
```

### Exercise 8.5: Merge Multiple Event Sources

Three peer nodes each produce an event stream. Merge them into a single stream, tag each event with its source, and batch-process the results.

```typescript
interface PeerEvent {
  readonly source: string;
  readonly type: string;
  readonly data: unknown;
}

const peerStream = (nodeId: string): Stream.Stream<PeerEvent, ConnectionError> => {
  // todo() — simulate with Stream.repeatEffect + random delays
};

const mergedPipeline: Effect.Effect<void, ConnectionError> = {
  // todo()
  // 1. Create streams for three peers
  // 2. Merge them with Stream.mergeAll
  // 3. Group into batches of 20 or every 5 seconds
  // 4. Log each batch size
};
```

### Exercise 8.6: Backpressure Demonstration

Build a producer-consumer system where the producer is fast and the consumer is slow. Show that a bounded queue provides backpressure and the producer slows down.

```typescript
const backpressureDemo = Effect.gen(function* () {
  const produced = yield* Ref.make(0);
  const consumed = yield* Ref.make(0);

  // todo()
  // 1. Create a bounded queue (capacity 10)
  // 2. Producer: emit items as fast as possible, incrementing 'produced'
  // 3. Consumer: process each item with a 100ms delay, incrementing 'consumed'
  // 4. Run both concurrently for 2 seconds, then interrupt
  // 5. Log final produced vs consumed counts
  //
  // Expected: produced count will be close to consumed count + queue capacity
  // because backpressure prevents the producer from getting too far ahead
});
```

### Exercise 8.7 (Challenge): Real-Time Aggregation

Build a stream pipeline that implements a sliding-window aggregation over a stream of metrics. Every 10 seconds, emit the average, min, and max of the values received in the last 30 seconds.

```typescript
interface MetricPoint {
  readonly timestamp: number;
  readonly value: number;
}

interface WindowAggregation {
  readonly windowEnd: number;
  readonly count: number;
  readonly avg: number;
  readonly min: number;
  readonly max: number;
}

const slidingWindowAggregation = (
  metrics: Stream.Stream<MetricPoint>,
  windowSizeMs: number,
  emitIntervalMs: number
): Stream.Stream<WindowAggregation> => {
  // todo()
  // This is a real stream processing pattern used in monitoring systems.
  //
  // Hint: use Stream.scan to maintain a buffer of recent points
  // Hint: use Stream.schedule to control emission timing
  // Hint: on each emission, filter the buffer to the window and compute stats
};
```

*This pattern — sliding window aggregation over a time-series stream — is the core of distributed monitoring systems. If you can implement this, you can build a metrics pipeline.*

## Summary

This chapter covered the production stream patterns: resource-safe streams with `acquireRelease`, intelligent batching with `groupedWithin`, concurrent merging of multiple sources, and the bridge between streams and queues. Together with the fundamentals from Chapter 8a, you now have the full toolkit for building streaming data pipelines in Effect.

| Pattern | Tool | Use case |
|---|---|---|
| Resource safety | `Stream.acquireRelease` | Files, connections, subscriptions |
| Fixed batching | `Stream.grouped(n)` | Bulk operations |
| Time-bounded batching | `Stream.groupedWithin(n, d)` | Write coalescing, latency control |
| Sequential combination | `Stream.concat` | Ordered sources |
| Concurrent combination | `Stream.merge` / `mergeAll` | Multiple event sources |
| Queue bridge | `Stream.fromQueue` | Producer-consumer patterns |

## Quick Reference

```typescript
import { Stream, Sink, Chunk } from "effect";

// --- Resource safety ---
Stream.acquireRelease(acquire, release)        // Scoped stream
Stream.ensuring(stream, finalizer)             // Add cleanup

// --- Batching ---
Stream.grouped(stream, n)                     // Fixed-size batches
Stream.groupedWithin(stream, n, duration)     // Count or time batches

// --- Combining ---
Stream.concat(a, b)                           // Sequential
Stream.merge(a, b)                            // Concurrent interleave
Stream.mergeAll(streams, { concurrency })      // Merge many
```

---

*Try the exercises yourself first! Ask me for solutions to any specific exercise.*

*Next: Chapter 9 — Scheduling, Retries, and Repetition, where we discover that retry policies and repeat schedules are composable values — yet another instance of the "effects as data" principle — and build sophisticated resilience strategies from simple primitives.*
