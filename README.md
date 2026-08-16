# Pulse Queue

A distributed job queue built with Java, Spring Boot, PostgreSQL, Redis, and Docker. An API accepts work, Redis delivers it to workers, and PostgreSQL is the durable system of record for job state.

## What it demonstrates

- Separate API and worker processes from one application image.
- Atomic job-state transitions: `QUEUED → PROCESSING → SUCCEEDED`, `RETRY_SCHEDULED`, or `DEAD_LETTER`.
- Exponential retry delays capped at five minutes.
- A Redis sorted set schedules delayed retries; a worker promotes due jobs back into the ready queue.
- A Redis dead-letter queue receives jobs that exhaust their retry budget.
- Flyway migrations own the PostgreSQL schema; Hibernate only validates it.

## Run

```powershell
docker compose up --build
```

Submit a normal job:

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/jobs -ContentType 'application/json' -Body '{"queueName":"notifications","payload":"{\"message\":\"welcome\"}","maxAttempts":3}'
```

Submit a job that demonstrates retries and the dead-letter queue:

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/jobs -ContentType 'application/json' -Body '{"queueName":"notifications","payload":"{\"simulateFailure\":true}","maxAttempts":3}'
```

Use `GET /api/jobs/{id}` to watch a job move through its states, or `GET /api/jobs` to see all jobs.

## Deliberate limitation

Redis delivery is at-least-once. A production implementation would add a transactional outbox to close the small gap between persisting a job and enqueueing it, then make downstream processors idempotent.
