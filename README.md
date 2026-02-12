# Effect from First Principles

A guide to [Effect-TS](https://effect.website) following the Red Book pedagogy: build from scratch, discover the algebra, name the pattern last.

## Table of Contents

### Preface

- [Introduction](chapter-00-introduction.md) — What this book is, who it's for, and how to read it

### Part I: Foundations

- [Chapter 1 — Why Effect?](chapter-01-why-effect.md)
  The real cost of `try/catch`, `Promise`, and manual error handling. Programs as values.

- [Chapter 2 — Option, Either, and the Algebra of Errors](chapter-02-option-either.md)
  Building `Option` and `Either` from scratch. `map`, `flatMap`, `zip`, `pipe`. The pattern emerges.

- [Chapter 3 — The Effect Type: Core Operations](chapter-03-effect-core.md)
  From `Either` to `Effect`. `Effect.gen`, typed error composition, and the `Effect<A, E, R>` triple.

### Part II: Error Handling

- [Chapter 4a — Error Handling: Tagged Errors and Recovery](chapter-04a-error-handling.md)
  Tagged error types, `catchTag`, `catchAll`, `mapError`, `orElse`. Surgical error recovery.

- [Chapter 4b — Deep Error Model: Cause, Schema, and Accumulation](chapter-04b-cause-schema.md)
  The `Cause` type, defects vs. expected errors, `Schema` validation, fail-fast vs. error accumulation.

### Part III: Resources and Dependencies

- [Chapter 5 — Resource Management and Scoping](chapter-05-resources.md)
  `acquireRelease`, `Scope`, composable resource lifetimes. Guaranteed cleanup in a world where anything can fail.

- [Chapter 6 — Dependency Injection with Layers](chapter-06-layers.md)
  `Context.Tag`, `Layer`, compile-time dependency graphs. Testable, composable service wiring.

### Part IV: Concurrency

- [Chapter 7 — Concurrency and Fibers](chapter-07-concurrency.md)
  Fibers, structured concurrency, `Ref`, `Queue`, `Deferred`, `race`, bounded parallelism.

### Part V: Streams

- [Chapter 8a — Streams: Lazy, Effectful Sequences](chapter-08a-stream-basics.md)
  `Stream` constructors, `map`, `flatMap`, `filter`, `Sink`. The same algebra on sequences.

- [Chapter 8b — Stream Patterns for Distributed Systems](chapter-08b-stream-patterns.md)
  Resource-safe streams, `groupedWithin`, merging, a realistic distributed event processor.

### Part VI: Scheduling and Resilience

- [Chapter 9 — Scheduling, Retries, and Repetition](chapter-09-scheduling.md)
  `Schedule` as composable values. Exponential backoff, jitter, windowing, conditional retry.

### Part VII: Unification

- [Chapter 10 — The Grand Unification](chapter-10-grand-unification.md)
  Functor, Applicative, Monad — named at last. The complete pattern map. Capstone exercise.

## The Recurring Pattern

Every chapter discovers the same operations on a new type:

| | `wrap` | `transform` | `chain` | `combine` |
|---|---|---|---|---|
| **Option** | `some(a)` | `map` | `flatMap` | `zip` |
| **Either** | `right(a)` | `map` | `flatMap` | `zip` |
| **Effect** | `succeed(a)` | `map` | `flatMap` | `zip` |
| **Stream** | `make(a)` | `map` | `flatMap` | `zip` |
| **Schedule** | `succeed(a)` | `map` | `flatMap` | — |
| **Layer** | `succeed(t,v)` | — | — | `merge` |

## Prerequisites

- Comfortable with TypeScript
- No functional programming background required
- No prior Effect experience needed

## Acknowledgments

Inspired by [Functional Programming in Scala](https://www.manning.com/books/functional-programming-in-scala) (the Red Book) by Paul Chiusano and Rune Bjarnason.

## License

This work is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
