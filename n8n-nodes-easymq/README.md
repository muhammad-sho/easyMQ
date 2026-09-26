# n8n-nodes-easymq

Native n8n community nodes for [easyMQ](../README.md) — a lightweight,
Redis-backed message broker with a RabbitMQ-like queue/consumer model.
Behaves like n8n's RabbitMQ nodes, without the RabbitMQ weight.

## Install

In n8n: **Settings → Community Nodes → Install** → `n8n-nodes-easymq`.
Or `npm install n8n-nodes-easymq` in `~/.n8n`, then restart n8n.

Add an **EasyMQ API** credential: the easyMQ **Base URL**
(e.g. `http://easymq:3000`) and the **API Token** from the easyMQ startup
logs (or your pinned `API_TOKEN` value).

## EasyMQ Trigger

Listens to an easyMQ queue **over a persistent consumer connection** —
every message starts an execution immediately. There is no polling
interval; deactivating the workflow closes the connection and pending
messages are requeued for redelivery.

```text
Credentials
Queue

Options
  + Acknowledge
  + Max Concurrent Executions
  + Visibility Timeout (Ms)
  + Consumer ID
```

### Acknowledge

Mirrors the RabbitMQ Trigger:

- **Immediately** — acked as soon as it is delivered to n8n.
- **Execution Finishes** — acked when the execution finishes, success or
  failure.
- **Execution Finishes Successfully** — acked only on success; on failure
  the message is requeued and redelivered.
- **Specified Later in Workflow** — the trigger does not settle. Add an
  **EasyMQ → Acknowledge** node later; its fields pick up the trigger
  item automatically. If the run ends without acknowledgement, success
  acks and failure requeues.

### Max Concurrent Executions

At most this many trigger executions process at the same time (default
`1`; hidden while Acknowledge is `Immediately`, where flow control stays
internal). Like the RabbitMQ Trigger's Parallel Message Processing Limit:
combining a limit above 1 with `Immediately` switches to Execution
Finishes.

### Visibility Timeout (Ms)

Lease per message (default `60000`): unacked past this timeout the message
is redelivered. Keep it above your workflow runtime.

### Consumer ID

Lease identity (generated when empty). Use a distinct id per workflow when
several workflows share a queue.

### Output item

```json
{
  "queue": "orders",
  "messageId": "msg_kgTkgkO1DBXA",
  "consumerId": "cons_abc",
  "data": { "message": "hello" },
  "deliveryCount": 1,
  "redelivered": false
}
```

### Troubleshooting

**"Timed out waiting for the easyMQ hello reply" on activation.**
The trigger opened the connection but the server never answered. Check,
in order:

1. easyMQ is **2.1.0+** (the subscribe endpoint is new) and reachable
   from n8n: `docker compose pull && docker compose up -d`, then
   `curl http://<host>:3000/health/ready`.
2. Redis is healthy (`/health/ready` returns 200, not 503).
3. The easyMQ logs around activation (`docker compose logs easymq`):
   `Consumer connected` means the hello succeeded; `subscribe-failed` /
   `frame-failed` entries explain the refusal.

## EasyMQ node

Simple operations over queues and messages:

```text
Message: Publish · Acknowledge · Requeue · Delete · Set TTL · Get
Queue:   Declare · Get Statistics · List · Delete
```

Acknowledge-family fields default to the trigger item
(`={{ $json.queue }}`, `={{ $json.messageId }}`, `={{ $json.consumerId }}`),
so manual acknowledgement needs no wiring:

```text
EasyMQ Trigger (Specified Later in Workflow)
      ↓
   Process
      ↓
EasyMQ → Acknowledge
```

Or automatic:

```text
EasyMQ Trigger (Execution Finishes Successfully)
      ↓
   Process
```

## Examples

Publish from any workflow (Message ID is required — it is the upsert key;
tick **Upsert** to update an existing ID instead of conflicting):

```text
Schedule Trigger → EasyMQ (Publish: queue "orders", id "order-42", data {...})
```

Competing workers — two activated workflows with triggers on the same
queue each get different messages, FIFO.

Delay a message by 60 s, or pull one forward now:

```text
EasyMQ → Set TTL (ttl 60000 / 0)
```

Drop a queued message before it is ever delivered:

```text
EasyMQ → Delete
```

## Development

```bash
npm install
npm run typecheck
npm run lint
npm run format
npm run build        # tsc -> dist/
```

## Publish

Releases are published to npm by the [`publish-n8n`
workflow](../.github/workflows/publish-n8n.yml), which uses the
`NPM_TOKEN` repository secret:

```bash
npm version patch|minor|major   # bumps n8n-nodes-easymq/package.json
git push origin main
git tag n8n-nodes-easymq-v0.3.0  # must match package.json
git push origin n8n-nodes-easymq-v0.3.0
```

Pushing the tag builds, verifies, and runs `npm publish --access public`.
Manual publish (needs an npm token with 2FA bypass or `--otp`):

```bash
npm publish --access public
```
