# easyMQ Architecture and Implementation Plan

## 1. Project Goal

Build **easyMQ** as a production-ready, open-source, self-hosted general-purpose job scheduling and background-processing service.

easyMQ provides a simple HTTP API for applications to create and manage background jobs while **BullMQ 6.3.8 and Redis remain responsible for queueing, scheduling, workers, retries, concurrency, delays, priorities, and job lifecycle management**.

The service must not contain assumptions about WhatsApp, Facebook, AI, webhooks, databases, or any particular business domain.

The public API must expose easyMQ's own stable types and schemas rather than exposing BullMQ types directly.

---

# 2. Technology

Use the already initialized versions:

* Node.js
* TypeScript
* BullMQ 6.3.8
* ioredis 6.0.0
* Fastify 5.12.5
* Pino for structured logging
* Zod or equivalent for input validation
* Vitest for testing

Do not change dependency versions unless an actual compatibility problem requires it.

BullMQ's installed declarations and official documentation are the source of truth for supported APIs.

Do not invent BullMQ methods, options, events, or configuration.

---

# 3. Deployment Architecture

Use separate **API and worker roles** within the same deployable application.

### API role

Responsible for:

* HTTP API
* Queue/job management
* Queue registration
* Schedule management
* Queue inspection
* Job inspection
* Queue control
* Cancellation requests

API instances do **not** create BullMQ Workers.

### Worker role

Responsible for:

* Discovering registered queues
* Creating BullMQ Workers
* Processing jobs
* QueueEvents
* Executor execution
* Distributed cancellation

Multiple worker instances must be able to process the same queue safely through BullMQ's normal worker distribution and locking.

The role is selected through configuration/environment variables.

This allows deployments such as:

```text
easyMQ API  × N
easyMQ Worker × N
Redis × 1
```

while still allowing a simple single-container deployment for small installations.

---

# 4. Repository Structure

```text
easyMQ/
├── src/
│   ├── app/
│   │   ├── build-app.ts
│   │   └── lifecycle.ts
│   │
│   ├── config/
│   │   ├── env.ts
│   │   └── schema.ts
│   │
│   ├── api/
│   │   ├── routes/
│   │   │   ├── queues.ts
│   │   │   ├── jobs.ts
│   │   │   ├── schedules.ts
│   │   │   └── health.ts
│   │   ├── schemas/
│   │   ├── errors.ts
│   │   └── server.ts
│   │
│   ├── queues/
│   │   ├── queue-service.ts
│   │   └── queue-catalog.ts
│   │
│   ├── jobs/
│   │   ├── job-service.ts
│   │   └── job-types.ts
│   │
│   ├── workers/
│   │   ├── worker-manager.ts
│   │   ├── job-processor.ts
│   │   └── cancellation.ts
│   │
│   ├── executors/
│   │   ├── executor.ts
│   │   └── http-executor.ts
│   │
│   ├── infrastructure/
│   │   ├── bullmq/
│   │   ├── redis/
│   │   └── logging/
│   │
│   ├── health/
│   │   └── health-service.ts
│   │
│   └── index.ts
│
├── tests/
│   ├── unit/
│   ├── api/
│   └── integration/
│
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── LICENSE
├── package.json
├── package-lock.json
├── tsconfig.json
├── eslint.config.js
└── prettier.config.js
```

Do not introduce additional architectural layers unless they provide a concrete benefit.

Avoid excessive repository/port/manager abstractions.

---

# 5. Dependency Flow

The primary dependency direction should be:

```text
Fastify routes
      ↓
API validation / translation
      ↓
Application services
      ↓
BullMQ-facing interfaces
      ↓
BullMQ infrastructure
      ↓
BullMQ
      ↓
Redis
```

Workers follow:

```text
BullMQ Worker
      ↓
JobProcessor
      ↓
Executor
      ↓
External system
```

The API layer must not directly manipulate Redis.

Application services must not depend on Fastify.

BullMQ-specific objects should not leak into the public API.

---

# 6. Queue Discovery

BullMQ remains the source of truth for actual queue/job state.

easyMQ maintains only a small Redis **queue discovery index**.

The registry contains queue names only.

It must not contain:

* Job state
* Job payloads
* Job status
* Retry information
* Worker state
* Queue statistics

Register a queue when a job or recurring schedule is created.

Use an atomic Redis operation where registration and notification need to happen together.

Registration must be idempotent.

There is no separate queue database.

### Queue deletion

Do not implement queue deletion in v1.

Queues remain registered even when empty.

This avoids unsafe races between queue deletion and producers/workers.

Queue listing returns the registered queue names.

---

# 7. Worker Lifecycle

Only worker-role processes create BullMQ Workers.

At worker startup:

1. Subscribe to queue-registration notifications.
2. Load the existing queue registry.
3. Create workers for registered queues.
4. Continue listening for new queue registrations.

When a new queue is registered:

1. Notification is received.
2. WorkerManager creates the Worker if it does not already exist.

Registration is idempotent.

On Redis subscription reconnect, reload the queue registry so missed notifications do not prevent worker discovery.

Do not periodically poll Redis.

Each worker instance may maintain one BullMQ Worker per registered queue.

Empty queues are acceptable; BullMQ's blocking mechanism waits for work.

---

# 8. Job Processing

A job contains:

```text
payload
execution
```

These are separate concepts.

`payload` is arbitrary JSON supplied by the application.

`execution` describes how easyMQ should execute the job.

Initial execution type:

```text
execution.type = "http"
```

The execution abstraction must allow additional executor types later.

Do not couple the queue/job model to HTTP.

---

# 9. Executor Interface

Keep the executor contract small.

Conceptually:

```text
execute(job, signal) → result
```

The executor receives an `AbortSignal`.

The initial implementation is:

```text
HttpExecutor
```

It performs the configured outbound HTTP request.

A future executor can be added without changing the BullMQ queue architecture.

---

# 10. HTTP Executor

The HTTP executor should support generic HTTP requests while applying sensible security controls.

Support:

* URL
* HTTP method
* headers
* request body
* timeout
* response handling

Implement:

* Request timeout
* Response-size limit
* Redirect limit
* Redirect target validation
* Credential/header protection across origins
* Hop-by-hop header rejection
* Credential redaction in logs
* SSRF protection

By default, block destinations resolving to:

* Loopback
* Link-local
* Metadata-service addresses
* Other unsafe private/internal destinations

Private-network access must be configurable because self-hosted installations may legitimately need to call internal services.

Do not make the executor unusably restrictive.

---

# 11. Job Capabilities

The public API must support:

* Immediate jobs
* Delayed jobs
* Recurring schedules
* Deduplication
* Debounce
* Debounce replacement
* Retry configuration
* Fixed backoff
* Exponential backoff
* Priority
* Job inspection
* Job listing
* Job removal
* Job cancellation
* Manual retry
* Delayed-job promotion/change where supported
* Job status inspection
* Queue pause/resume
* Queue inspection
* Queue listing

Use native BullMQ functionality wherever possible.

Do not implement a custom scheduler or queue.

---

# 12. Recurring Schedules

v1 includes recurring schedules.

Use BullMQ 6.3.8's supported Job Scheduler APIs.

Expose operations for:

* Create/upsert
* Update
* List
* Get
* Remove

Do not implement a custom scheduler.

The API should use an easyMQ representation rather than exposing BullMQ's internal types directly.

Document the supported schedule representation and timezone behavior clearly.

---

# 13. Cancellation

easyMQ must provide distributed active-job cancellation.

BullMQ's `Worker.cancelJob()` is instance-local, so implement easyMQ's own distributed coordination.

Cancellation flow:

```text
API
 ↓
Verify job is active
 ↓
Write short-lived cancellation marker
 ↓
Publish cancellation signal
 ↓
Worker receives signal
 ↓
Worker.cancelJob()
 ↓
AbortSignal reaches executor
 ↓
Executor aborts work
```

The cancellation marker should identify the specific job safely, including sufficient identity/version information to avoid affecting a reused job ID.

Cancellation must handle races with:

* Job completion
* Retry
* Worker failure
* Redis reconnect

Before starting an execution attempt, the processor must check whether the job has been cancelled.

The executor must honor the AbortSignal.

Cancellation must not trigger another retry.

Because BullMQ does not have a native cancelled state, the final BullMQ state will be represented as failed with a stable easyMQ cancellation error code.

Document this clearly in the API.

Cancellation is cooperative and must not be presented as guaranteed termination of arbitrary external work.

---

# 14. Queue Controls

Support:

* Queue pause
* Queue resume
* Queue inspection
* Queue job counts
* Queue state information

Use BullMQ's native queue operations.

Do not implement a custom pause mechanism.

Remember that pausing a queue does not cancel jobs that are already active.

---

# 15. Public API Design

The public API must be simple enough for users who do not understand BullMQ.

Use generic concepts:

```text
queues
jobs
schedules
```

Avoid exposing BullMQ classes or internal implementation details.

The API should make common operations straightforward while still allowing advanced options.

At minimum provide endpoints covering:

### Queues

```text
List queues
Get queue information
Pause queue
Resume queue
Inspect queue jobs/counts
```

### Jobs

```text
Create job
Get job
List jobs
Remove job
Cancel job
Retry job
Promote/change delayed job where supported
```

### Schedules

```text
Create/upsert schedule
Get schedule
List schedules
Remove schedule
```

### Health

```text
Liveness
Readiness
```

---

# 16. API Defaults

The API contract must explicitly define defaults before implementation.

Define:

### Pagination

Use cursor-based or stable page-based pagination where appropriate.

Set reasonable default and maximum page sizes.

Never allow unbounded job listings.

### Ordering

Document deterministic ordering for list endpoints.

### Returned job fields

Return stable easyMQ fields rather than raw BullMQ objects.

Do not expose internal Redis/BullMQ implementation details.

### Retention

Define explicit defaults for completed and failed job retention.

Allow callers to override retention when appropriate through supported BullMQ options.

### Scheduling

Define the accepted schedule representation and timezone behavior.

### HTTP body

Define how JSON and other supported body types are represented.

Keep the initial API simple and explicit.

---

# 17. Authentication

v1 uses a configurable Bearer API token.

Requirements:

* Token comes from environment configuration.
* Never hardcode secrets.
* Never log tokens.
* Authentication is disabled only through an explicit development configuration if needed.
* Document the behavior clearly.

Multi-tenancy and complex authorization are outside v1.

---

# 18. Configuration

All operational configuration must be environment-based.

Include configuration for:

* Redis connection
* API host/port
* API authentication
* Application role
* Worker concurrency
* HTTP executor limits
* Request timeout
* Response-size limit
* Cancellation TTL
* Shutdown timeout
* Logging level
* Queue/job defaults

Validate configuration at startup.

Fail clearly on invalid configuration.

---

# 19. Redis Connections

Create an explicit Redis connection manager.

Do not create unmanaged Redis connections per request.

Respect BullMQ's requirements for:

* Worker connections
* Queue connections
* QueueEvents
* Blocking operations
* Pub/sub

Clearly define ownership of connections.

During shutdown:

1. Stop application activity.
2. Close Workers.
3. Close QueueEvents.
4. Close cancellation subscriptions.
5. Close Queue/Redis objects.
6. Close Redis connections owned directly by easyMQ.
7. Close Fastify.

Do not close a connection that is still required by a BullMQ object.

---

# 20. Health and Readiness

Provide separate:

### Liveness

Indicates that the process is running.

Should not fail merely because Redis is temporarily unavailable.

### Readiness

Indicates that the service can perform its configured role.

For API instances, verify required Redis/BullMQ connectivity.

For worker instances, verify required Redis/subscription readiness.

Health routes should not contain Redis connection logic themselves.

---

# 21. Observability

Use structured JSON logging.

Include useful fields such as:

* Timestamp
* Level
* Service
* Role
* Queue
* Job ID
* Event
* Error code

Never log:

* API tokens
* Authorization headers
* Cookies
* Sensitive credentials

Provide useful worker/queue/job lifecycle logs.

Do not require an external monitoring platform.

---

# 22. Graceful Shutdown

On SIGTERM/SIGINT:

1. Stop accepting new HTTP requests.
2. Stop assigning/creating new workers.
3. Stop workers from fetching new jobs.
4. Allow active jobs to finish within the configured shutdown deadline.
5. Close QueueEvents.
6. Close cancellation subscriptions.
7. Release BullMQ/Redis resources.
8. Close Fastify.
9. Exit.

If the shutdown deadline is exceeded, log the timeout and allow the process/container to terminate.

BullMQ should be allowed to recover unfinished work after a crash.

Do not implement a custom recovery mechanism.

---

# 23. Error Handling

Create stable easyMQ error codes.

Do not expose raw BullMQ/Redis errors directly as the public API contract.

Errors should contain appropriate:

* HTTP status
* easyMQ error code
* human-readable message
* relevant resource information where safe

Internal errors should retain useful structured logs.

---

# 24. Testing

### Unit tests

Without Redis where possible:

* Configuration validation
* Request validation
* API schemas
* Service logic
* BullMQ option mapping
* Error mapping
* Cancellation logic
* Executor behavior
* HTTP timeout
* Redirect handling
* Response-size limits

### API tests

Use Fastify's testing/injection capabilities.

Test routes independently from Redis and BullMQ where possible by injecting application dependencies.

### Integration tests

Use a real Redis instance.

Test:

* Queue registration
* Queue discovery
* Multiple worker instances
* Job creation
* Delayed jobs
* Recurring schedules
* Deduplication
* Debounce
* Replacement
* Retry/backoff
* Priority
* Queue pause/resume
* Job removal
* Job inspection
* Distributed cancellation
* Redis reconnect behavior
* Worker failure/recovery
* Graceful shutdown

Use a fake executor for BullMQ integration tests so tests do not depend on external HTTP services.

---

# 25. Docker

Provide:

```text
Dockerfile
docker-compose.yml
```

Docker Compose should include:

* easyMQ
* Redis
* Redis persistent volume
* Health checks
* Environment configuration

Support both:

```text
easyMQ + bundled Redis
```

and:

```text
easyMQ + externally managed Redis
```

Do not expose Redis publicly by default.

The application image should be suitable for production deployment.

---

# 26. Documentation

README must explain:

* What easyMQ is
* Why it exists
* Architecture
* Features
* Quick start
* Docker deployment
* External Redis configuration
* Environment variables
* Authentication
* API
* Jobs
* Delayed jobs
* Recurring schedules
* Deduplication
* Debounce
* Retries
* Backoff
* Priorities
* Queue pause/resume
* Job cancellation
* Job lifecycle
* Worker configuration
* API/worker roles
* Redis persistence
* Production deployment
* Development
* Testing

Provide practical curl examples.

Examples should start simple and progressively demonstrate advanced features.

---

# 27. Design Principles

The implementation must follow these principles:

* BullMQ is the queue engine.
* Redis is the persistence/backend for BullMQ.
* Do not build another queue.
* Do not build another scheduler.
* Do not poll Redis unnecessarily.
* Prefer BullMQ native primitives.
* Keep public types independent of BullMQ.
* Keep HTTP concerns out of application services.
* Keep executor concerns out of queue infrastructure.
* Keep Redis state minimal.
* Avoid unnecessary abstractions.
* Prefer explicit code over clever abstractions.
* Make common operations extremely easy.
* Preserve access to advanced capabilities.
* Make behavior deterministic and documented.
* Handle race conditions explicitly.
* Design correctly for multiple easyMQ instances.

---

# 28. Important Implementation Constraints

Do not add:

* A database
* A custom queue implementation
* A custom scheduler
* A mandatory external monitoring service
* A frontend/dashboard
* Multi-tenancy
* Complex authorization
* Additional executor types beyond HTTP
* Unnecessary frameworks

unless they are required to satisfy an explicitly documented requirement.

The service should remain a focused backend infrastructure component.

---

# 29. Final Implementation Requirement

Before writing code, verify all BullMQ 6.3.8 APIs against the installed package declarations and official documentation.

Then implement the architecture completely.

Do not stop after creating scaffolding.

The final implementation should include:

* Working API
* Working workers
* Working Redis integration
* Working job execution
* Working scheduling
* Working cancellation
* Authentication
* Validation
* Error handling
* Health/readiness
* Logging
* Graceful shutdown
* Tests
* Docker deployment
* Environment configuration
* README documentation

Run type checking, linting, formatting checks, unit tests, and integration tests before considering the implementation complete.

Do not claim functionality is complete if it has not been tested.
