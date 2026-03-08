# nexus-workforce-orchestrator

A lightweight Go orchestrator scaffold for workforce-style task runs.

## Features

- Lease-based in-memory run store abstraction.
- Idempotent invoke API (`Idempotency-Key` header or body key).
- Control API for run cancellation intent.
- Event callback API for external progress/completion signals.
- SSE event stream for all lifecycle events.
- Minimal dispatcher loop that leases queued work and completes/cancels runs.

## Quick start

```bash
make run
```

Service runs on `:8080` by default (override with `PORT`).

## API

- `POST /v1/runs/invoke`
- `POST /v1/runs/{run_id}/control`
- `POST /v1/runs/{run_id}/callback`
- `GET /v1/events/stream?since=<seq>`
- `GET /healthz`

### Invoke example

```bash
curl -X POST http://localhost:8080/v1/runs/invoke \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"input":"sync payroll"}'
```

### Control example

```bash
curl -X POST http://localhost:8080/v1/runs/run-000001/control \
  -H 'Content-Type: application/json' \
  -d '{"signal":"cancel"}'
```

### Stream example

```bash
curl -N http://localhost:8080/v1/events/stream
```

## Testing

```bash
make test
```

- Changelog: minor updates.

- Changelog: minor updates.

- Changelog: minor updates.
