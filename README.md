# easyMQ

**easyMQ** is a production-ready, open-source, self-hosted general-purpose job
scheduling and background-processing service. Applications create background
jobs through a simple HTTP API; **BullMQ + Redis do the queueing** (workers,
retries, concurrency, delays, priorities, scheduling, job lifecycle) while
easyMQ provides the API, worker orchestration, execution, and operations
around them.

easyMQ knows nothing about your business domain — no WhatsApp, no AI, no
webhooks-as-a-concept, no database assumptions. A job is an arbitrary JSON
`payload` plus an `execution` description (v1: outbound HTTP request).

## Features

- Immediate, delayed, and recurring (cron / interval) jobs
- Deduplication and debounce (replace) semantics
- Retries with fixed / exponential backoff, priorities, LIFO
- Job inspection, listing (bounded pagination), removal, manual retry,
  delayed-job promotion and delay changes
- Queue pause / resume, inspection, job counts
- Distributed active-job cancellation (cooperative, AbortSignal-based)
- Bearer-token authentication, strict input validation, stable error codes
- Separate `api` / `worker` roles (scale independently) or single-container
  `both` mode
- Structured JSON logging, liveness / readiness probes, graceful shutdown
- Docker image + Compose deployment with persistent Redis

## Architecture

```text
                ┌─────────────┐      ┌─────────────┐
                │ easyMQ API  │ × N  │easyMQ Worker│ × N
                └──────┬──────┘      └──────┬──────┘
                       │  BullMQ            │  BullMQ Workers
                       ▼                    ▼  + QueueEvents
                    ┌──────────────────────────┐
                    │        Redis × 1         │
                    │ (BullMQ data + tiny      │
                    │  queue discovery index)  │
                    └──────────────────────────┘
```

- **API role**: HTTP API, queue/job/schedule management, inspection,
  pause/resume, cancellation requests. Creates **no** BullMQ Workers.
- **Worker role**: discovers registered queues, runs one BullMQ Worker per
  queue, executes jobs via executors, handles distributed cancellation.
  Any number of workers may share a queue — BullMQ distributes and locks.
- **Queue discovery**: easyMQ keeps only a Redis *set of queue names*.
  Registration (`SADD` + `PUBLISH` in one atomic `MULTI`) is idempotent.
  Workers load the set at startup and listen for new registrations
  (reconciling on reconnect — no polling). There is **no queue deletion**
  in v1; empty queues stay registered harmlessly.
- **Executors**: jobs carry `execution: { type: "http", ... }`. The
  `HttpExecutor` performs the outbound request. New executor types plug
  into the same `execute(job, signal) → result` contract without touching
  queue infrastructure.

## Quick start

Requirements: Node.js 20+, Redis 7+.

```bash
npm install
cp .env.example .env        # set API_TOKEN (or AUTH_DISABLED=true for dev)
npm run dev                 # APP_ROLE=both by default
```

With Docker (app + persistent Redis):

```bash
API_TOKEN=super-secret docker compose up --build
curl http://localhost:3000/health/live
```

Create and run a job (dev, auth disabled):

```bash
curl -s -X POST localhost:3000/jobs \
  -H 'content-type: application/json' \
  -d '{"queue":"emails","payload":{"to":"a@example.com"},
       "execution":{"type":"http","url":"https://example.com/hook",
                    "method":"POST","body":{"hello":"world"}}}' | jq .
```

## Docker deployment

- `docker-compose.yml` runs `easymq` (role `both`) + Redis with a persistent
  `redis-data` volume and health checks. Redis is **not** published to the
  host by default.
- Scale out with dedicated roles (see the commented `easymq-api` /
  `easymq-worker` services in `docker-compose.yml`), or point
  `REDIS_URL` at an externally managed Redis and drop the bundled service.

## External Redis configuration

Set `REDIS_URL` (e.g. `redis://user:password@host:6379/0`). easyMQ requires
`maxRetriesPerRequest: null` semantics for BullMQ blocking connections and
configures that itself. Use Redis persistence (AOF/RDB) — BullMQ state,
including scheduled jobs, lives in Redis.

## Environment variables

See `.env.example` for the full list. Highlights:

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection |
| `REDIS_KEY_PREFIX` | `easymq` | Key prefix for BullMQ + easyMQ keys |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `3000` | HTTP listen address |
| `API_TOKEN` | — | Bearer token (required unless `AUTH_DISABLED=true`) |
| `AUTH_DISABLED` | `false` | Dev-only auth bypass — never in production |
| `APP_ROLE` | `both` | `api` \| `worker` \| `both` |
| `WORKER_CONCURRENCY` | `10` | Jobs per queue per worker instance |
| `HTTP_TIMEOUT_MS` | `30000` | Default execution timeout |
| `HTTP_MAX_RESPONSE_BYTES` | `1048576` | Response-size limit |
| `HTTP_MAX_REDIRECTS` | `5` | Redirect-following limit |
| `HTTP_ALLOW_PRIVATE_NETWORK` | `false` | Allow loopback/private targets |
| `CANCELLATION_TTL_SECONDS` | `300` | Cancellation marker TTL |
| `SHUTDOWN_TIMEOUT_MS` | `30000` | Grace period for active jobs |
| `LOG_LEVEL` / `LOG_PRETTY` | `info` / `false` | Logging |
| `DEFAULT_ATTEMPTS` / `DEFAULT_BACKOFF_TYPE` / `DEFAULT_BACKOFF_DELAY_MS` | `3` / `exponential` / `5000` | Retry defaults |
| `DEFAULT_REMOVE_ON_COMPLETE_COUNT` / `DEFAULT_REMOVE_ON_FAIL_COUNT` | `1000` / `5000` | Finished-job retention |
| `PAGE_DEFAULT_LIMIT` / `PAGE_MAX_LIMIT` | `50` / `200` | Pagination bounds |

## Authentication

All routes except `GET /health/live` and `GET /health/ready` require
`Authorization: Bearer <API_TOKEN>` (compared in constant time). Tokens come
from the environment, are never logged, and 401 responses contain no secrets.
`AUTH_DISABLED=true` is an explicit development-only bypass.

## API

Base URL: `http://host:port`. Errors look like
`{"error":{"code":"NOT_FOUND","message":"...","resource":{...}}}` with stable
`code` values (`VALIDATION_ERROR`, `UNAUTHENTICATED`, `NOT_FOUND`,
`CONFLICT`, `JOB_NOT_ACTIVE`, `CANCELLED`, `EXECUTOR_*`, `SSRF_BLOCKED`,
`SERVICE_UNAVAILABLE`, `INTERNAL_ERROR`). Raw BullMQ/Redis errors never leak.

### Queues

```bash
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/pause
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/resume
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/counts
```

Pausing stops new deliveries; already-active jobs keep running.

### Jobs

Create (immediate):

```bash
curl -s -X POST localhost:3000/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"queue":"emails","name":"welcome",
       "payload":{"to":"a@example.com"},
       "execution":{"type":"http","url":"https://example.com/hook","method":"POST"}}'
```

Inspect, list (bounded, deterministic order), remove, retry, promote, delay:

```bash
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id
curl -H "Authorization: Bearer $TOKEN" \
  'localhost:3000/queues/emails/jobs?state=failed&limit=20&offset=0'
curl -X DELETE -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id/retry
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id/promote
curl -X PATCH -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id/delay \
  -H 'content-type: application/json' -d '{"delayMs":5000}'
```

List states: `waiting, active, completed, failed, delayed, prioritized,
waiting-children, unknown` (repeatable `state=` or comma-separated).
`limit` defaults to 50, capped at 200 — listings are never unbounded.
Ordering: jobs are concatenated in the requested state order, each in BullMQ
index order (`asc=true` reverses). Response: `{jobs, offset, limit,
nextOffset}` (`nextOffset: null` = last page).

### Delayed jobs

```bash
curl -s -X POST localhost:3000/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"queue":"reminders","delayMs":3600000,
       "payload":{"user":"u-1"},
       "execution":{"type":"http","url":"https://example.com/nudge"}}'
```

Delayed jobs can be promoted (`.../promote`) or have their delay changed
(`PATCH .../delay`) — BullMQ rejects these for non-delayed jobs with a
`409 CONFLICT`.

### Recurring schedules

Exactly one of `pattern` (cron) or `everyMs` (interval, ms) is required.
`timezone` is an IANA name (e.g. `Europe/Berlin`); cron patterns are evaluated
in that zone, defaulting to UTC.

```bash
# Every 5 minutes
curl -s -X POST localhost:3000/schedules \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"poll","queue":"sync","everyMs":300000,
       "payload":{},"execution":{"type":"http","url":"https://example.com/poll"}}'

# Nightly cron in a specific timezone
curl -s -X POST localhost:3000/schedules \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"nightly","queue":"reports","pattern":"0 2 * * *","timezone":"Europe/Berlin",
       "payload":{},"execution":{"type":"http","url":"https://example.com/report"}}'

curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/reports/schedules
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/reports/schedules/nightly
curl -X DELETE -H "Authorization: Bearer $TOKEN" localhost:3000/queues/reports/schedules/nightly
```

Upsert is idempotent by `id` (create-or-replace). Scheduled jobs use the same
payload/execution/attempts/backoff/retention template options as regular jobs
(BullMQ template options exclude per-run `delay`/`jobId`/deduplication).

### Deduplication

```bash
curl -s -X POST localhost:3000/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"queue":"imports","deduplication":{"id":"import-42","ttlMs":60000},
       "payload":{},"execution":{"type":"http","url":"https://example.com/import"}}'
```

A second submission with the same id inside the TTL returns the existing job
instead of creating a duplicate (`replace: true` swaps the pending record).

### Debounce

Debounce is deduplication with delay + replace: resubmitting the same id
within `delayMs` replaces the pending job instead of queueing another one.

```bash
curl -s -X POST localhost:3000/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"queue":"search-index","debounce":{"id":"reindex","delayMs":30000},
       "payload":{},"execution":{"type":"http","url":"https://example.com/reindex"}}'
```

### Retries and backoff

```bash
curl -s -X POST localhost:3000/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"queue":"emails","attempts":5,
       "backoff":{"type":"exponential","delayMs":1000},
       "payload":{},"execution":{"type":"http","url":"https://example.com/hook"}}'
```

Defaults come from `DEFAULT_ATTEMPTS` / `DEFAULT_BACKOFF_*`. Transient
executor failures (timeouts, connection errors) retry; deterministic failures
(SSRF-blocked, redirect limit, oversize response, bad job data) fail
permanently. Failed/completed jobs can be rerun via `.../retry`.

### Priorities

`priority` is an integer `0`–`2097151` (`0` = no explicit priority). Lower
numbers run first among prioritized jobs. Example: `"priority": 10`.
(LIFO ordering is available via `"lifo": true`.)

### Queue pause / resume

See Queues above. Note pausing affects delivery, not execution: use
cancellation for active jobs.

### Job cancellation

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" localhost:3000/queues/emails/jobs/job-id/cancel
```

Only **active** jobs can be cancelled (`409 JOB_NOT_ACTIVE` otherwise — remove
waiting/delayed jobs instead). Flow: verify active → write short-lived marker
 → publish signal → every worker aborts locally → executor aborts work. The
attempt then fails with the stable reason `easymq:cancelled` and is **not**
retried (surfaced as `failed` state — BullMQ has no cancelled state).
Cancellation is **cooperative**: an executor that ignores its AbortSignal runs
to its own outcome. Markers carry the attempt identity, so retries or reused
job ids are never affected by stale cancellations.

### Job lifecycle

`waiting → active → completed`, or `→ failed` (retryable until attempts run
out), `delayed → waiting` (after delay/promotion), `prioritized` for
explicit priorities. Cancelled attempts land in `failed` with reason
`easymq:cancelled`. Retention of finished jobs follows
`removeOnComplete` / `removeOnFail` (`{count}` / `{ageSeconds}`), defaulting
to the configured counts. `failedReason` and the executor result are exposed
on the job resource.

### Worker configuration

`WORKER_CONCURRENCY` sets jobs processed concurrently per queue per worker
instance. Run more worker containers (`APP_ROLE=worker`) for throughput;
BullMQ handles distribution and locking. Workers pick up existing and newly
registered queues automatically.

### API / worker roles

- `both` (default): single-container deployment — serves HTTP and processes.
- `api`: serve HTTP only (no Workers, no subscriptions).
- `worker`: process jobs only (no HTTP server).
- Health `role` field and logs always report the configured role.

### Redis persistence

All queue/job/schedule state lives in Redis via BullMQ. Enable Redis
persistence (the Compose file uses AOF) or jobs and schedules are lost on
Redis data loss. easyMQ adds only the queue-name set, cancellation markers
(short TTL), and pub/sub channels.

### Health

- `GET /health/live` — process is running. Never touches Redis.
- `GET /health/ready` — role-aware readiness (Redis reachable). Returns
  `503 SERVICE_UNAVAILABLE` with per-check details when not ready.
  Both are unauthenticated for orchestrator probes.

### Production deployment

1. Set a strong `API_TOKEN` (never `AUTH_DISABLED=true`).
2. Run dedicated `api` (×N) and `worker` (×M) deployments behind your
   load balancer; keep Redis private with persistence enabled.
3. Tune `WORKER_CONCURRENCY`, executor limits, `SHUTDOWN_TIMEOUT_MS`, and
   retention to your workload.
4. Ship logs (JSON, `service: easymq`, `role`, `queue`, `jobId`, `event`
   fields; tokens/cookies/credentials are redacted) to your log platform —
   no external monitoring service is required.

### Development

```bash
npm install
npm run dev        # tsx watch
npm run build      # tsc -> dist/
npm start          # node dist/index.js
npm run typecheck  # tsc --noEmit
npm run lint       # eslint
npm run format     # prettier
```

### Testing

```bash
npm test                    # all tests (needs Redis on 127.0.0.1:6379)
npm run test:unit           # config, schemas, mapping, executor, processor
npm run test:integration    # real Redis: queues, jobs, schedules, workers, cancellation, e2e
REDIS_URL=redis://host:6379 npm run test:integration
```

Integration tests use a fake executor (no external HTTP) and unique key
prefixes per file so parallel runs never interfere.
