# ADR 0001: Lease-Based Orchestrator Architecture

## Status

Accepted

## Context

The orchestrator must support:

- In-memory execution state while preserving realistic concurrency semantics.
- Safe worker coordination via leases instead of direct locks across process boundaries.
- Client-visible idempotency for invoke operations.
- External callback ingestion and real-time event fan-out.

## Decision

Use a layered architecture:

- `internal/runstore`: lease-aware in-memory store with idempotency index and append-only event log.
- `internal/service`: orchestration use-cases (invoke, control, callback, lease/complete).
- `internal/dispatcher`: polling worker loop that acquires leases and transitions runs.
- `internal/httpapi`: REST + SSE transport boundary.

Key behaviors:

- A run is leaseable when non-terminal and its lease is absent/expired.
- Invoke idempotency is keyed by idempotency token to existing run ID mapping.
- Control requests set a signal that is consumed by the dispatcher before completion.
- SSE streams replay historical events from a sequence offset and then push live events.

## Consequences

Positive:

- Testable core logic without HTTP/server coupling.
- Deterministic event chronology via monotonically increasing sequence IDs.
- Minimal dependencies and low operational complexity.

Tradeoffs:

- In-memory storage is process-local and non-durable.
- Dispatcher is intentionally simple (single worker loop in this scaffold).

- Changelog: minor updates.

- Changelog: minor updates.
