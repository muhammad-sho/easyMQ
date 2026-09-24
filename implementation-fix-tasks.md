# easyMQ implementation fix tasks

This document turns the code review findings into implementation work. Keep
BullMQ as the source of truth for queue and job lifecycle state; do not add a
second job store or polling-based queue monitor. Preserve easyMQ's public API
independence from BullMQ except where a stable easyMQ contract is explicitly
described below.

## Priority 1 — prevent undiscoverable work

### Register queues before enqueueing

In `JobService.createJob` and `ScheduleService.upsertSchedule`, validate and
register the queue before calling `Queue.add` or `Queue.upsertJobScheduler`.
Registration of an empty queue is harmless and preferable to writing BullMQ
state that worker instances cannot discover. Keep registration idempotent and
keep the registry limited to queue names; do not copy job or schedule state
into it.

If the BullMQ operation fails after registration, leave the empty name in the
registry. Do not attempt to roll the registration back: another API instance
may have added work to that queue concurrently. Map registry Redis failures to
a service-unavailable API error and do not claim the job or schedule was
accepted.

**Acceptance:**

- Job registration completes before `Queue.add` is called.
- Schedule registration completes before `Queue.upsertJobScheduler` is called.
- A failed enqueue/upsert may leave a registered empty queue, but cannot leave
  new BullMQ work undiscoverable through the normal API flow.
- Repeated registration remains safe across API and worker instances.
- Unit tests assert call ordering and failure behavior; integration tests prove
  a second worker instance discovers a newly submitted job/schedule.

## Priority 1 — make HTTP network policy operator-controlled

Remove `allowPrivateNetwork` from `HttpExecution`, the public Zod execution
schema, and any request-to-executor mapping. The only switch for private
address access must be the worker's `HTTP_ALLOW_PRIVATE_NETWORK` configuration.
An API caller must not be able to elevate that policy for an individual job.
Continue blocking cloud metadata endpoints even when private-network access is
enabled.

Keep outbound validation on every redirect hop. Reject unsafe DNS results if
any resolved address is blocked; do not accept a hostname merely because one
of its addresses is public. Document the remaining DNS-rebinding limitation if
the HTTP client does not pin the validated address to the connection.

**Acceptance:**

- With the global setting false, submitted job fields cannot enable requests to
  loopback, link-local, or private network addresses.
- With the global setting true, legitimate self-hosted private endpoints work,
  but metadata-service addresses remain blocked.
- URL validation is performed for the initial target and each redirect target.
- Unit tests cover IPv4/IPv6 loopback, link-local, private, metadata, and mixed
  DNS answers, as well as attempts to submit the removed override.

## Priority 1 — harden distributed cancellation

Keep cancellation internal to easyMQ: API requests persist a short-lived marker
and publish a prompt signal; each worker instance uses BullMQ's local
`Worker.cancelJob()` for its own active attempt. Redis pub/sub is a notification
channel, not the sole durable record of cancellation.

Give each marker enough identity to distinguish both the job generation and
the attempt. Use the BullMQ job's immutable creation timestamp together with
the active `attemptsMade` value, and validate both on the API and worker sides.
Do not rely on queue name, job ID, and attempt count alone: a removed job ID can
be reused with a reset attempt count. Use an atomic Redis operation for marker
creation/publication if needed to avoid partially completed cancellation
requests; retain a TTL as final cleanup protection.

Subscribe before starting workers. If a cancellation subscriber reconnects,
perform a one-time reconciliation of markers against currently active jobs in
the queues managed by that process, then locally cancel only matching active
attempts. Do not introduce a periodic Redis poll. The processor's pre-execution
marker check remains necessary for a cancellation that arrives before an
attempt begins.

Define cancellation as cooperative. If the attempt completes before the
cancellation takes effect, keep the BullMQ terminal result; do not rewrite a
completed job as cancelled. Clear a marker only when it belongs to the same
job generation/attempt, and use TTL cleanup if the process crashes before
clearing it. If cancellation takes effect, throw BullMQ's unrecoverable error
with the stable easyMQ cancellation reason so retries do not run.

**Acceptance:**

- Cancellation reaches the active worker across multiple easyMQ instances.
- A lost pub/sub notification is recovered after subscriber reconnect without
  recurring polling.
- A request racing with completion has a deterministic documented outcome and
  cannot affect a later retry or a newly created job reusing the same ID.
- Repeated cancellation requests for the same active attempt are safe.
- Tests cover cancellation before execution, during execution, during retry,
  completion race, Redis subscriber disconnect/reconnect, multiple workers,
  and remove/recreate with the same job ID.

## Priority 1 — do not forward credentials across origins

The HTTP executor currently strips only `authorization` and `cookie` on a
cross-origin redirect. Because arbitrary headers are supported, headers such
as `x-api-key`, `x-auth-token`, and caller-defined credentials can otherwise
reach the redirect destination.

On an origin change (scheme, host, or port), do not forward caller-supplied
headers. Continue to validate the destination with the SSRF policy and honor
the redirect limit. Same-origin redirects may preserve headers. Keep normal
generic HTTP methods, bodies, and caller-supplied headers available for the
initial request.

**Acceptance:**

- Same-origin redirects retain headers and follow existing method/body
  redirect semantics.
- Cross-origin redirects receive none of the original caller-supplied
  headers, including standard and custom credential headers.
- Tests use two local origins and assert the destination receives no supplied
  headers after a cross-origin redirect.

## Priority 2 — redact execution credentials from job responses

`toEasyMQJob` currently returns the stored execution descriptor verbatim.
Introduce an API-safe execution representation that keeps useful metadata
(type, URL, method, and non-secret options) but never returns values from
`execution.headers`. Apply the same mapping to create, get, and list responses.
Do not mutate the stored job data because the worker needs the original
headers to execute the request.

Avoid logging request headers, response bodies, or credentials. Keep the
redaction policy consistent across errors and structured logs.

**Acceptance:**

- A recognizable secret submitted in an authorization or custom header is
  absent from create, inspect, and list API responses.
- The stored execution config remains usable by the worker.
- Tests assert secret values do not occur in serialized API responses or
  captured logs.

## Priority 2 — enforce the graceful-shutdown deadline

Keep normal shutdown graceful: stop accepting HTTP requests, stop workers from
fetching new work, wait for active work up to the configured deadline, then
force-close remaining workers and release QueueEvents, pub/sub subscriptions,
BullMQ Queue objects, and owned Redis connections. A `Promise.race` timeout by
itself is insufficient because it does not cancel the shutdown promise or
release its open handles.

Use the installed BullMQ 6.3.8 worker close/force-close API and verify its
behavior against the installed declarations/source. Ensure cleanup remains
idempotent if graceful close and forced close overlap. Rely on BullMQ stalled
job recovery for work interrupted by forced termination; do not add a custom
job recovery mechanism.

**Acceptance:**

- A cooperative executor drains normally before the deadline.
- A non-cooperative executor cannot keep the process alive past the configured
  deadline after force-close is invoked.
- Interrupted work is recoverable by another Worker under BullMQ's lifecycle.
- Tests verify shutdown ordering, bounded completion time, and cleanup of
  Redis clients/subscriptions.

## Priority 2 — classify backend errors without leaking internals

Do not translate every `upsertJobScheduler` error into a request-validation
error. Separate invalid schedule input from Redis/BullMQ operational failures.
Map unavailable Redis to the stable service-unavailable response and
unexpected backend failures to an internal error. Apply the same rule to job
creation and other service methods.

Do not interpolate raw BullMQ or Redis error messages into public API messages
or `details`; retain the original error only as a server-side cause for
structured logging. Keep public error codes and response shape stable.

**Acceptance:**

- Invalid input returns a validation response.
- Redis unavailability returns service unavailable.
- Unexpected BullMQ errors return an internal error.
- API responses contain no Redis host, Lua script, stack, or raw backend
  message; server logs retain enough context for diagnosis without secrets.
- Tests cover each classification and response-redaction case.

## Priority 3 — fix pagination and remove unused event resources

For job and schedule listings, return `nextOffset: null` when there are no
further results, including when the result count is an exact multiple of the
page size. Fetch one extra item or use an accurate total to determine whether
another page exists; do not infer solely from `page.length === limit`.

`WorkerManager` creates a `QueueEvents` instance for every managed queue but
does not consume queue events. Remove those instances and their Redis
connections unless a concrete cross-worker event consumer is added. Local
Worker events may continue to support local logging.

**Acceptance:**

- Pagination tests cover empty results, partial pages, exact multiples, and
  final pages for both jobs and schedules.
- No QueueEvents object is created per queue when there is no consumer.
- Queue/job state continues to come from BullMQ, not a local event cache.

## Verification and environment notes

Run typecheck and lint, then unit/API tests and Redis-backed integration tests.
Integration coverage should include at least two worker managers sharing the
same Redis prefix. The prior review environment could not connect to Redis at
`127.0.0.1:6379`, and its sandbox rejected local listener binds with `EPERM`;
those runtime suites must be rerun in an environment where Redis and local
test sockets are available.
