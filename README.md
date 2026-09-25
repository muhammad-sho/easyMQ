# easyMQ

**easyMQ** is a lightweight, simplified RabbitMQ-like message broker backed
by Redis — plus exactly two extra message features: **deleting a queued
message** and **changing/resetting a queued message's TTL**.

```text
Producer
   ↓
Named Queue
   ↓
Multiple Consumers
```

A message is simply a message — an id plus arbitrary JSON data:

```json
{
  "id": "msg_123",
  "data": {
    "message": "hello"
  }
}
```

There is no job abstraction, no executor, no webhook, no scheduler.
Consumers decide what to do with messages. Redis is the only backend; its
internals never leak into the API.

## Behavior

RabbitMQ-like semantics, implemented atomically in Redis Lua scripts so any
number of competing consumers can share a queue safely:

- Named, durable queues (everything lives in Redis until acked/deleted)
- Multiple consumers on the same queue, competing for messages (FIFO)
- Consumer concurrency is client-side: poll in parallel
- Prefetch: max messages leased to one consumer at once
- Acknowledgement removes the message permanently
- Unacknowledged messages: leased with a visibility timeout; past it, the
  message is automatically redelivered (`deliveryCount` increments,
  `redelivered: true`)
- Explicit requeue returns a leased message to the ready tail
- Consumer cancellation requeues that consumer's leases
- Retry behavior = redelivery with `deliveryCount`/`redelivered` flags —
  the consumer decides when to give up (ack, requeue, or delete)

Two extra operations beyond classic queues:

- `DELETE /queues/{queue}/messages/{messageId}` — remove a message that is
  still waiting (ready or delayed). Leased (unacked) messages conflict
  (`409`) until acked or requeued.
- `PUT /queues/{queue}/messages/{messageId}/ttl` — change/reset a waiting
  message's TTL counted from the time of the call. When the TTL passes, the
  message simply becomes available to consumers — it never triggers HTTP
  requests, webhooks, or executors.

## Quick start

### Docker (recommended)

Zero configuration — no `.env`, no secrets file, no local build:

```bash
git clone https://github.com/muhammad-sho/easyMQ.git
cd easyMQ
docker compose up -d
```

```bash
docker compose ps
docker compose logs -f easymq
```

Health endpoints (unauthenticated):

```bash
curl http://localhost:3000/health/live
curl http://localhost:3000/health/ready
```

On first start easyMQ generates a random API token and prints it in the
`easymq` logs (stable for the life of the Redis volume). Use it for API calls:

```bash
TOKEN=$(docker compose logs easymq | sed -n 's/.*API token: \([^ ]*\).*/\1/p' | tail -1)
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/queues
```

`.env` is **optional**. Application defaults are production-safe; the image
defaults `REDIS_URL` to the Compose Redis service. See
[Configuration](#configuration) to override defaults.

### Node.js (local development)

Requirements: Node.js 20+, Redis 7+.

```bash
npm install
npm run dev                 # set API_TOKEN or AUTH_DISABLED=true as needed
```

Publish and consume (dev, auth disabled):

```bash
curl -s -X POST localhost:3000/queues/orders/messages \
  -H 'content-type: application/json' \
  -d '{"id":"msg_123","data":{"message":"hello"}}' | jq .

curl -s -X POST localhost:3000/queues/orders/consume \
  -H 'content-type: application/json' \
  -d '{"consumerId":"worker-1","count":10}' | jq .

curl -X POST localhost:3000/queues/orders/messages/msg_123/ack \
  -H 'content-type: application/json' -d '{"consumerId":"worker-1"}'
```

## API

Base URL: `http://host:port`. Errors look like
`{"error":{"code":"NOT_FOUND","message":"...","resource":{...}}}` with stable
`code` values (`VALIDATION_ERROR`, `UNAUTHENTICATED`, `NOT_FOUND`,
`CONFLICT`, `SERVICE_UNAVAILABLE`, `INTERNAL_ERROR`). Backend failures are
classified: invalid input → `400`, unreachable Redis → `503`, unexpected
failures → `500`. Malformed JSON bodies map to `400 VALIDATION_ERROR`, never
`INTERNAL_ERROR`.

### Queues

```bash
curl -X PUT -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
curl -X DELETE -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
```

- `PUT /queues/{queue}` declares the queue (idempotent) and returns
  `{queue, created}`.
- `GET /queues` lists queues with ready/delayed/unacked depths.
- `GET /queues/{queue}` returns depths, per-consumer leases, and lifetime
  counters (`published`, `delivered`, `acked`, `requeued`, `deleted`).
- `DELETE /queues/{queue}` removes the queue and every message in it
  (`204`). Acks for its in-flight leases afterwards `404`.

Publishing also auto-declares the queue.

### Messages

Publish (optional `id`, optional `ttlMs` delay before availability):

```bash
curl -s -X POST localhost:3000/queues/orders/messages \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"data":{"message":"hello"},"ttlMs":60000}'
```

Response (`201`): `{id, queue, state, availableAt, createdAt,
deliveryCount: 0}`. A duplicate `id` returns `409 CONFLICT`. Payloads over
`MAX_MESSAGE_BYTES` return `400`.

Inspect:

```bash
curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders/messages/msg_123
```

### Consuming

```bash
curl -s -X POST localhost:3000/queues/orders/consume \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"consumerId":"worker-1","count":10,"visibilityTimeoutMs":30000,"prefetch":100}' | jq .
```

Response: `{consumerId, messages: [{id, data, deliveryCount, redelivered,
visibleAt}]}`. Consuming an unknown queue returns `404` — declare (or
publish) first.

- `consumerId` (optional, generated when empty) identifies the lease owner
  for prefetch accounting, ack ownership checks, and cancellation.
- `count` (default 1, capped by `MAX_CONSUME_COUNT`) bounds this call.
- `visibilityTimeoutMs` (default `DEFAULT_VISIBILITY_TIMEOUT_MS`) is the
  per-message lease: ack within it, or the message is redelivered.
- `prefetch` (default `DEFAULT_PREFETCH`) caps the consumer's outstanding
  leases; a consumer at its cap receives zero messages until it acks.
- Competing consumers each get different messages; delivery is FIFO.

### Acknowledge / requeue

```bash
curl -X POST localhost:3000/queues/orders/messages/msg_123/ack \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"consumerId":"worker-1"}'

curl -X POST localhost:3000/queues/orders/messages/msg_123/requeue \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"consumerId":"worker-1"}'
```

- Ack removes the message permanently (`{acked: true, deliveries}`).
- Requeue returns a leased message to the ready tail
  (`{requeued: true, state: "ready", deliveries}`); the next delivery has
  an incremented `deliveryCount` and `redelivered: true`.
- Only leased (`unacked`) messages can be acked/requeued — otherwise `409`.
  A mismatched `consumerId` also returns `409`.
- `consumerId` is optional; supply it to stop one consumer from settling
  another's leases.

### Delete a queued message

```bash
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  localhost:3000/queues/orders/messages/msg_123
```

Removes the message if it is still waiting (`204`). Missing messages `404`;
leased messages `409` (ack or requeue first).

### Change/reset a message TTL

```bash
curl -X PUT localhost:3000/queues/orders/messages/msg_123/ttl \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"ttl":60000}'
```

Sets availability to now + `ttl` ms and moves the message between ready and
delayed accordingly (`{id, queue, state, availableAt}`). `0` makes it
available immediately. When the TTL passes, the message is delivered to
consumers normally — nothing else happens. Missing messages `404`; leased
messages `409`.

### Cancel a consumer

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  localhost:3000/queues/orders/consumers/worker-1/cancel
```

Requeues all of that consumer's leased messages (`{cancelled: true,
requeued}`); idempotent.

## Authentication

All routes except `GET /health/live` and `GET /health/ready` require
`Authorization: Bearer <API_TOKEN>` (constant-time compare).

There is **no default/shared secret**. When `API_TOKEN` is unset, startup
generates a cryptographically random token and persists it in Redis
(`<REDIS_KEY_PREFIX>:auth:api-token`) so restarts keep the same token while
the Redis volume lives. The token is printed in startup logs. Set
`API_TOKEN` to pin a value (multi-instance deployments, secret managers).
`AUTH_DISABLED=true` is an explicit development-only bypass — never in
production.

## Docker deployment

Published image: **`ghcr.io/muhammad-sho/easymq`**.

Every push to `main` builds the image from `Dockerfile` and publishes
`ghcr.io/muhammad-sho/easymq:latest` plus immutable
`ghcr.io/muhammad-sho/easymq:sha-<short-sha>` tags (GitHub Actions +
`GITHUB_TOKEN`; PRs never publish `latest`).

### Normal deployment

```bash
docker compose up -d
```

No `.env`, no configuration step. Edit `docker-compose.yml` only if you want
different **deployment** settings (port, container/hostname, storage path).
Application behavior is not configured in Compose.

`docker-compose.yml` contains only deployment settings: image, names,
restart policy, port 3000, and a host bind mount for Redis under
`./data/redis`. Redis is not published to the host. If the GHCR package is
private for your account, `docker login ghcr.io` first.

### Advanced configuration

All advanced settings are **optional** environment variables (`.env` is never
required). Application built-in defaults apply first; environment variables
override them. See `.env.example` and [Configuration](#configuration).

#### Image pinning

Edit the `image:` field in `docker-compose.yml` (e.g.
`ghcr.io/muhammad-sho/easymq:sha-abc1234`).

#### External Redis

Drop the `redis` service and point easyMQ at your instance:

```yaml
services:
  easymq:
    image: ghcr.io/muhammad-sho/easymq:latest
    container_name: easymq
    hostname: easymq
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      REDIS_URL: redis://user:password@redis.example.com:6379/0
```

Or set `REDIS_URL` in an optional `.env` file. Enable Redis persistence
(AOF/RDB) on your instance — queue state lives in Redis.

#### Local image build

```bash
docker build -t ghcr.io/muhammad-sho/easymq:local .
# Edit image: in docker-compose.yml, or override via Compose file.
```

## Configuration

`.env` is **never required** for Docker or local operation. Hierarchy:

1. Application defaults (safe for production)
2. Optional environment variables / `.env` override those defaults

`.env.example` is an optional reference for every setting. The normal user
does not need it.

### Environment variables

| Variable                       | Default                  | Purpose                                              |
| ------------------------------ | ------------------------ | ---------------------------------------------------- |
| `REDIS_URL`                    | `redis://127.0.0.1:6379` | Redis connection (`redis://redis:6379` in the image) |
| `REDIS_KEY_PREFIX`             | `easymq`                 | Key prefix for broker keys + managed API token       |
| `API_HOST` / `API_PORT`        | `0.0.0.0` / `3000`       | HTTP listen address                                  |
| `API_TOKEN`                    | —                        | Optional bearer pin; generated + Redis-persisted if unset |
| `AUTH_DISABLED`                | `false`                  | Dev-only auth bypass — never in production           |
| `DEFAULT_VISIBILITY_TIMEOUT_MS`| `30000`                  | Default per-message lease on consume                 |
| `DEFAULT_PREFETCH`             | `100`                    | Default max leases per consumer                      |
| `MAX_CONSUME_COUNT`            | `100`                    | Cap per consume call                                 |
| `SWEEPER_INTERVAL_MS`          | `1000`                   | Background sweep for TTL/lease expiry                |
| `MAX_MESSAGE_BYTES`            | `1048576`                | Max JSON payload size                                |
| `LOG_LEVEL` / `LOG_PRETTY`     | `info` / `false`         | Logging                                              |

## Redis persistence

All queue/message state lives in Redis. The Compose file bind-mounts
`./data/redis` so Redis snapshots persist across container recreation
(Redis's default RDB persistence). Queue data is lost on Redis data loss —
enable AOF as well for stricter durability. Unacked leases keep their
deadlines in Redis, so redelivery survives restarts with no custom recovery.

On the Docker host, Redis recommends `vm.overcommit_memory=1` for reliable
background saves:

```bash
sysctl vm.overcommit_memory=1
echo 'vm.overcommit_memory = 1' | sudo tee /etc/sysctl.d/99-redis.conf
```

## Health

- `GET /health/live` — process is running. Never touches Redis.
- `GET /health/ready` — Redis reachable. Returns `503
  SERVICE_UNAVAILABLE` with generic per-check status when not ready;
  backend failure details stay in server logs.
  Both are unauthenticated for orchestrator probes.

## Production deployment

1. Prefer the managed token from logs or set a strong `API_TOKEN` (never
   `AUTH_DISABLED=true`).
2. Run N API containers behind your load balancer; keep Redis private with
   persistence enabled. Consumers scale by polling in parallel.
3. Tune `DEFAULT_VISIBILITY_TIMEOUT_MS` (keep it above your consumer's
   processing time), `DEFAULT_PREFETCH`, and `SWEEPER_INTERVAL_MS` to your
   workload.
4. Ship logs (JSON, `service: easymq`; tokens are redacted) to your log
   platform — no external monitoring service is required.

## Graceful shutdown

On `SIGTERM`/`SIGINT`: stop accepting HTTP requests → stop the background
sweeper → release Redis connections → exit. Leases live in Redis, so
in-flight messages are redelivered after their visibility timeout.

## n8n integration

Native community nodes live in [`n8n-nodes-easymq/`](n8n-nodes-easymq/) —
independently publishable/installable, speaking only to the HTTP API above:

- **EasyMQ** node: publish, consume, get, acknowledge, requeue, delete,
  set TTL, plus queue declare/stats/list/delete.
- **EasyMQ Trigger** node: pick a queue, poll on a schedule, configure
  batch size / consumer id / prefetch / visibility timeout, with auto or
  manual acknowledgement.

Install in n8n via **Settings → Community Nodes → Install** with
`n8n-nodes-easymq` (or `npm install n8n-nodes-easymq` in `~/.n8n`), then add
an **EasyMQ API** credential (Base URL + API Token). See the
[subproject README](n8n-nodes-easymq/README.md).

## Development

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
npm run test:unit           # config, ids, schemas, routes (mocked broker)
npm run test:integration    # real Redis: broker flows + HTTP API
REDIS_URL=redis://host:6379 npm run test:integration
```

Integration tests use unique key prefixes per file so parallel runs never
interfere.

### Architecture

```text
                 ┌──────────────┐
                 │  easyMQ API  │ × N (stateless, share one Redis)
                 └──────┬───────┘
                        │  Lua scripts (atomic consume/ack/requeue/...)
                        ▼
                 ┌──────────────────────────┐
                 │        Redis × 1         │
                 │ (ready lists, delayed +  │
                 │  unacked ZSETs, message   │
                 │  hashes, queue registry)  │
                 └──────────────────────────┘
```

Per queue: a FIFO `ready` list, a `delayed` sorted set (TTL/availableAt),
an `unacked` sorted set (visibility deadlines), per-message hashes, and a
consumer registry with per-consumer pending sets for prefetch and
cancellation. A lightweight background sweeper (one interval, bounded Lua
per queue) promotes due messages and reclaims expired leases; consume calls
also settle both inline, so behavior never depends on sweep timing.
