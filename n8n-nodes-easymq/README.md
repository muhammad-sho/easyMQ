# n8n-nodes-easymq

Native [n8n](https://n8n.io) community nodes for
[easyMQ](https://github.com/muhammad-sho/easyMQ) — a lightweight,
Redis-backed, RabbitMQ-like message broker.

The nodes talk to easyMQ through its normal HTTP API; no broker logic is
duplicated here.

## Nodes

### EasyMQ

Queue/message operations without hand-built HTTP requests:

- **Message**: Publish, Consume (competing-consumer poll), Get, Acknowledge,
  Requeue, Delete (queued messages), Set TTL (change/reset a queued
  message's TTL)
- **Queue**: Declare, Get Statistics, List, Delete

Consume output items look like:

```json
{
  "id": "msg_abc123",
  "queue": "orders",
  "data": { "message": "hello" },
  "deliveryCount": 1,
  "redelivered": false,
  "visibleAt": 1758832340000
}
```

### EasyMQ Trigger

Polls a queue on a schedule and emits one item per message:

- **Queue** — declared automatically if missing
- **Batch Size** — max messages per poll
- **Consumer ID** — lease owner; use a distinct id per workflow when several
  workflows share one queue (default `n8n-trigger`)
- **Prefetch** — max messages leased to this consumer at once
- **Visibility Timeout** — unacked past this timeout, the message is
  redelivered. Keep it above your workflow runtime when Auto Acknowledge
  is on.
- **Auto Acknowledge** (default on) — ack on emit. Turn off and ack
  manually with the EasyMQ node (`$json.id` + the same consumer id) when
  the workflow itself decides the outcome.

## Credentials

Create an **EasyMQ API** credential:

- **Base URL** — e.g. `http://easymq:3000` (no trailing slash)
- **API Token** — the token from the easyMQ startup logs, or your pinned
  `API_TOKEN`

Use **Test** to verify (it lists queues).

## Install

In n8n (**Settings → Community Nodes → Install**), enter
`n8n-nodes-easymq`. Or manually:

```bash
cd ~/.n8n
npm install n8n-nodes-easymq
```

Docker-based n8n: install from the UI, or mount a data dir with the
package installed. Restart n8n after installing.

Requires easyMQ 2.x and n8n 1.x.

## Develop

```bash
npm install
npm run build      # tsc -> dist/
npm run typecheck
npm run lint
```

## Publish

```bash
npm version patch|minor|major
npm publish --access public
```
