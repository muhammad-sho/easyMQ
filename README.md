# easyMQ

**easyMQ is a lightweight Redis-backed message broker inspired by RabbitMQ's
queue/consumer model.** Its purpose is to provide the useful simplicity of
Redis together with the consumer-based processing model of RabbitMQ.

Producers publish messages to named queues. Consumers attach to a queue and
**messages are pushed to them** over a persistent connection — no polling.
Consumers acknowledge each message when done; anything left unacknowledged
is redelivered.

```text
Producer
   │
   ▼
 Queue
   │
   ├── Consumer 1
   ├── Consumer 2
   └── Consumer 3
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

Beyond the classic queue primitives, easyMQ adds exactly two distinctive
capabilities:

- **Delete a specific queued message** before it is ever delivered.
- **Change/reset a specific queued message's TTL** (delay or release it).

There is no job abstraction, no executor, no webhook, no scheduler, no
exchange/routing layer. Consumers decide what to do with messages. Redis is
the only backend; its internals never leak into the API.

## Why easyMQ exists

Full brokers like RabbitMQ are powerful but heavy to run and operate.
Raw Redis lists are light but leave you to reinvent leases, redelivery,
and consumer bookkeeping.

easyMQ intentionally does **not** try to reproduce all of RabbitMQ. It
focuses on a small set of queue/consumer primitives with Redis as the
backend:

- named, durable queues with competing consumers (FIFO)
- leases with visibility timeouts and automatic redelivery
- explicit acknowledge / requeue
- consumer cancellation
- persistent push subscriptions (WebSocket) — messages arrive instantly
- plus message delete and TTL reset

If you need exchanges, routing keys, clustering, or AMQP compatibility, use
RabbitMQ. If you need a few dependable queue primitives in one container
plus Redis, easyMQ is enough.

## Concepts

### Queues

Named, durable, declared idempotently (`PUT /queues/{queue}`).
Publishing to a missing queue declares it automatically. Everything about
a queue lives in Redis until it is acked or deleted, so queues survive
restarts. `GET /queues/{queue}` reports `ready` / `delayed` / `unacked`
depths, per-consumer leases, and lifetime counters.

### Messages

An id plus arbitrary JSON data. Messages waiting for delivery are either
**ready** (a FIFO list) or **delayed** (hidden until their TTL passes).
Publishing accepts an optional `id` (generated when omitted; duplicates
conflict) and an optional `ttlMs` delay before availability.

### Consumers

A consumer attaches to a queue with an identity (`consumerId`, generated
when omitted) and a **prefetch**: the maximum number of messages leased to
it at once. Multiple consumers on one queue compete for messages — each
message goes to exactly one available consumer. Consumers scale by adding
more consumer connections, each getting different messages.

The primary consumption path is a **persistent WebSocket subscription**
(`GET /queues/{queue}/subscribe`): the broker pushes each message the
moment it becomes available. A plain HTTP `POST .../consume` call exists
for scripts and one-shot clients.

### Acknowledgements

Delivery leases a message; acknowledgement removes it permanently.
Settlement is per message:

- **ack** — done, remove it.
- **requeue** — reject it; it returns to the ready tail and is redelivered
  with an incremented `deliveryCount` and `redelivered: true`.

Only leased (`unacked`) messages can be settled — anything else is a
`409 CONFLICT`. An optional owner check (`consumerId`) stops one consumer
from settling another's leases.

### Redelivery

Every delivery carries a visibility timeout (lease). If the lease expires
before the message is acked — the consumer crashed, was too slow, or its
connection dropped — the message is automatically redelivered. Closing a
persistent connection requeues its pending messages immediately, like a
dropped RabbitMQ channel. Retry policy is the consumer's decision (ack,
requeue, or delete) using the `deliveryCount` / `redelivered` flags.

### TTL

A message published with `ttlMs` (or moved via `PUT .../ttl`) waits in the
delayed set until its time passes, then becomes available to consumers
normally — it never triggers anything else. `ttl: 0` releases a message
immediately. TTLs of leased messages conflict until the lease settles.

### Message deletion

`DELETE .../messages/{id}` removes a message that is still waiting (ready
or delayed) so it is never delivered. Leased messages conflict until acked
or requeued.

### Message lifecycle

```text
Published
   ↓
Queued (ready … or delayed until its TTL passes)
   ↓
Delivered to consumer (leased with a visibility timeout)
   ↓
 ┌───────────────┐
 │               │
ACK            Reject (requeue)
 │               │
 ▼               ▼
Removed        Requeued → redelivered
```

Unacked past the lease (or orphaned by a dropped connection), a message
returns to the queue on its own. TTL behavior is separate: a TTL only
controls *when* a waiting message becomes available.

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

Publish and read back (dev, auth disabled):

```bash
curl -s -X POST localhost:3000/queues/orders/messages \
  -H 'content-type: application/json' \
  -d '{"id":"msg_123","data":{"message":"hello"}}' | jq .

curl -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders/messages/msg_123 | jq .
```

## Consuming

### Persistent subscriptions (recommended)

Open `GET /queues/{queue}/subscribe` as a WebSocket with the usual
`Authorization: Bearer <token>` header, send one hello frame, and receive
a `message` frame for every delivery — instantly, with no polling:

```text
EasyMQ Trigger
      │
      │ persistent connection
      ▼
   easyMQ
      │
      │ message arrives
      ▼
consumer runs immediately
```

```jsonc
// → { "action": "hello", "consumerId": "worker-1", "prefetch": 10,
//     "visibilityTimeoutMs": 30000 }
// ← { "type": "ready", "queue": "orders", "consumerId": "worker-1", ... }
// ← { "type": "message", "queue": "orders", "id": "msg_123",
//     "data": {...}, "deliveryCount": 1, "redelivered": false, ... }
// → { "action": "ack", "id": "msg_123" }
// ← { "type": "acked", "id": "msg_123", "deliveries": 1 }
```

Settle with `ack` / `requeue` frames, end the consumer with `cancel` or by
closing the socket. A dropped socket requeues its pending messages, so
unacknowledged work is redelivered. The full frame protocol is documented
in [docs/API.md](docs/API.md#subscribe-websocket).

### One-shot HTTP consume

`POST /queues/{queue}/consume` leases up to `count` waiting messages for
scripts, tests, and clients that cannot hold a socket open. It shares the
exact same atomic lease path as subscriptions — one implementation, two
transports.

## n8n integration

Native community nodes live in [`n8n-nodes-easymq/`](n8n-nodes-easymq/) and
behave like n8n's RabbitMQ nodes: the trigger holds a persistent consumer
connection and every message starts an execution immediately.

Install in n8n via **Settings → Community Nodes → Install** with
`n8n-nodes-easymq` (or `npm install n8n-nodes-easymq` in `~/.n8n`), then add
an **EasyMQ API** credential (Base URL + API Token). See the
[subproject README](n8n-nodes-easymq/README.md).

### EasyMQ Trigger

```text
Credentials
Queue

Options
  + Acknowledge (Immediately / Execution Finishes /
    Execution Finishes Successfully / Specified Later in Workflow)
  + Max Concurrent Executions
  + Visibility Timeout (Ms)
  + Consumer ID
```

- **Event-driven, never polling.** The trigger consumes over a persistent
  connection; canceling/deactivating the workflow closes it and requeues
  pending messages for redelivery.
- **Acknowledge modes** mirror the RabbitMQ Trigger: `Immediately` acks on
  delivery; `Execution Finishes` acks when the run ends (success or
  failure); `Execution Finishes Successfully` requeues on failure;
  `Specified Later in Workflow` leaves settlement to an EasyMQ node.
- **Max Concurrent Executions** caps how many trigger executions process at
  once (the RabbitMQ Trigger calls this Parallel Message Processing
  Limit). Flow control stays internal — there is no user-facing prefetch.
- Each output item carries everything a later node needs: `queue`,
  `messageId`, `consumerId`, `data`, `deliveryCount`, `redelivered`.

### EasyMQ node

Simple operations, no raw API details:

```text
Publish · Acknowledge · Requeue · Delete · Set TTL · Get
```

plus queue declare/stats/list/delete. Acknowledge-family fields default to
the trigger item (`={{ $json.messageId }}` …), so this needs no manual IDs:

```text
EasyMQ Trigger
      ↓
   Process
      ↓
EasyMQ → Acknowledge
```

or, with automatic acknowledgement, just:

```text
EasyMQ Trigger (Acknowledge: Execution Finishes Successfully)
      ↓
   Process
```

## API overview

Base URL: `http://host:port`. Every route except the health probes needs
`Authorization: Bearer <API_TOKEN>`.

| Area | Routes |
| --- | --- |
| Queues | `PUT /queues/{queue}` · `GET /queues` · `GET /queues/{queue}` · `DELETE /queues/{queue}` |
| Messages | `POST /queues/{queue}/messages` · `GET .../messages/{id}` · `DELETE .../messages/{id}` · `PUT .../messages/{id}/ttl` |
| Consuming | `POST /queues/{queue}/consume` · `GET /queues/{queue}/subscribe` (WebSocket) |
| Settlement | `POST .../messages/{id}/ack` · `POST .../messages/{id}/requeue` · `POST .../consumers/{id}/cancel` |
| Health | `GET /health/live` · `GET /health/ready` (unauthenticated) |

Errors are stable `{error: {code, message, resource?}}` bodies: invalid
input → `400 VALIDATION_ERROR`, bad token → `401 UNAUTHENTICATED`,
missing queue/message → `404 NOT_FOUND`, settling a message that is not
leased (or owned by someone else) → `409 CONFLICT`, unreachable Redis →
`503 SERVICE_UNAVAILABLE`.

The complete reference — authentication, every endpoint, the WebSocket
frame protocol, message format, and errors — lives in
**[docs/API.md](docs/API.md)**.

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
   persistence enabled. Consumers scale by adding persistent subscriptions
   (each gets different messages); keep per-consumer prefetch bounded.
3. Tune `DEFAULT_VISIBILITY_TIMEOUT_MS` (keep it above your consumer's
   processing time), `DEFAULT_PREFETCH`, and `SWEEPER_INTERVAL_MS` to your
   workload.
4. Ship logs (JSON, `service: easymq`; tokens are redacted) to your log
   platform — no external monitoring service is required.

## Graceful shutdown

On `SIGTERM`/`SIGINT`: stop accepting HTTP requests → stop the background
sweeper → cancel persistent consumers (their pending messages requeue) →
release Redis connections → exit. Leases live in Redis, so in-flight
messages are redelivered after their visibility timeout.

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
npm run test:integration    # real Redis: broker flows + HTTP API + WebSocket
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

A `SubscriptionManager` reacts to broker change notifications (publish,
requeue, TTL change, cancel, sweep movement) and fills persistent
consumers through the same atomic consume path — one lease implementation,
two transports (WebSocket push, HTTP pull).
