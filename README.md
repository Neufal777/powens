# Webhook Dispatcher

Accepts webhook jobs over HTTP, stores them durably, and delivers them
concurrently with HMAC signatures, exponential backoff and a dead-letter queue.

**Delivery semantics: at-least-once.** Receivers must deduplicate on
`X-Webhook-Id`.

---

## About the AI in here

I used AI heavily on this, so let me be straight about who decided what: **I drove
the design, AI did most of the typing, and nothing went in that I hadn't read, run
and understood.** You encouraged AI tools, so I'd rather show you where the line
sits than have you guess.

- **Every design call is mine.** Table-as-queue instead of a broker. One worker
  per claimed job instead of a channel pool. At-least-once. Fail-fast on 4xx.
  Letting in-flight deliveries finish on shutdown instead of killing them. Each
  one is a trade-off I picked on purpose and wrote up in
  [Key decisions](#key-decisions) along with what it costs.
- **I threw away most of the first version.** What came out initially had six
  packages, a `Store` interface with a single implementation, idempotency keys, a
  requeue endpoint, a `/stats` endpoint, and a dispatcher with a semaphore, a
  poller and a wake-up channel. It all worked. It was just solving problems I
  don't have at this size. Cutting it down to one package and one worker loop was
  the deliberate part — and the part that actually took judgment.
- **Catching its mistakes is the real test, and there was a good one.** It passed
  the cancelled shutdown context straight into the in-flight HTTP request. That's
  the textbook `context` pattern, it compiles, and every test still passed — but
  it's wrong here: it aborts a request the receiver may already have processed,
  so it would duplicate a delivery on every single deploy. You only catch that if
  you actually understand the delivery semantics you picked. The rest are in
  [On AI tooling](#on-ai-tooling).
- **I checked behaviour instead of trusting it.** Every output in the run steps
  above is real captured output, not illustrative. I ran the shutdown and
  crash-recovery scenarios with jobs genuinely in flight, because "it should
  work" and "I watched it work" are different claims.
- **Where it genuinely helped**: SQL scaffolding, table-driven tests, the
  Dockerfile, this README.

Not perfect, and I know where it's thin — that's
[What I would change](#what-i-would-change-before-production).

---

## Run it

Docker is the only requirement.

| | |
|---|---|
| `make demo` | build, start, and enqueue one job per delivery outcome |
| `make logs` | follow both services |
| `make dead` | the dead-letter queue |
| `make test` | `go test -race ./...` |
| `make down` | stop and clean up |

### 1. Start it

```bash
make demo
```

```
202 succeeds-first-try
202 succeeds-after-2-retries
202 dies-after-max-attempts
202 dies-immediately-422
```

Four `202`s means four jobs are already on disk. The first build takes a few
minutes (it compiles the SQLite driver and runs the tests inside the image).

### 2. Watch the retries

```bash
make logs
```

The `flaky` job is the one to watch — the receiver 503s it twice, then accepts.
Note the growing, jittered backoff and the varying `worker` field:

```
delivery failed, will retry  worker=6 attempt=1 status=503 retry_in=1.87s
delivery failed, will retry  worker=7 attempt=2 status=503 retry_in=3.48s
delivered                    worker=1 attempt=3 status=200
```

### 3. Check the dead-letter queue

After ~30s:

```bash
make dead
```

```
dies-immediately-422       attempts=1  code=422
dies-after-max-attempts    attempts=5  code=500
```

The contrast is the retry policy: a 4xx dies on the **first** attempt, a 5xx uses
all **five**.

### 4. Send your own

```bash
curl -X POST localhost:8080/webhooks -H 'Content-Type: application/json' -d '{
  "event_type": "payment.completed",
  "payload": {"amount": 4200, "currency": "EUR"},
  "destination_url": "http://receiver:9090/hook"
}'
```

> `receiver:9090`, not `localhost:9090` — that URL is resolved from inside the
> dispatcher container.

Receiver endpoints, one per failure mode: `/hook` (200), `/hook/flaky` (503 twice
then 200), `/hook/fail` (always 500), `/hook/reject` (422, non-retryable). It
verifies the HMAC on every request and logs duplicates.

### 5. Prove graceful shutdown loses nothing

```bash
for i in $(seq 1 8); do
  curl -sS -o /dev/null -X POST localhost:8080/webhooks -H 'Content-Type: application/json' \
    -d '{"event_type":"payment.completed","payload":{"n":'$i'},"destination_url":"http://receiver:9090/hook"}'
done

docker compose stop dispatcher && docker compose logs dispatcher | tail -3
```

The in-flight attempt is **finished, not aborted** — aborting it would risk
duplicating a delivery the receiver already processed:

```
"msg":"delivered", "attempt":1, "status":200
"msg":"shutting down"
"msg":"shutdown complete"
```

Restart with `docker compose start dispatcher`: nothing is lost, nothing is stuck.

### 6. Prove crash recovery works

Same, but with `SIGKILL` and an unreachable destination so jobs hang in flight:

```bash
for i in $(seq 1 8); do
  curl -sS -o /dev/null -X POST localhost:8080/webhooks -H 'Content-Type: application/json' \
    -d '{"event_type":"payment.completed","payload":{"n":'$i'},"destination_url":"http://10.255.255.1:9999/hook"}'
done
sleep 3
docker compose kill dispatcher && docker compose start dispatcher
docker compose logs dispatcher | grep orphaned
```

```
"msg":"requeued jobs orphaned by a previous crash","count":8
```

Rows left in `delivering` belong to a process that no longer exists, so startup
resets them. Without this they would be stuck forever.

### API and config

| | |
|---|---|
| `POST /webhooks` | enqueue → `202` |
| `GET /webhooks?status=` | list jobs; `status=dead` is the dead-letter queue |
| `GET /healthz` | liveness |

Env vars: `WEBHOOK_SECRET` (required), `WORKERS`, `MAX_ATTEMPTS`, `ADDR`,
`DB_PATH`.

---

## How it works

```
cmd/dispatcher/   cmd/receiver/      thin entry points, no logic
internal/webhook/
    run.go        config, wiring, signals, shutdown order
    api.go        HTTP ingest + inspection
    store.go      SQLite — the queue, the only file with SQL
    worker.go     claim → sign → POST → retry
    receiver.go   test receiver (local demos only)
```

```
POST /webhooks ──► validate ──► INSERT (fsync) ──► 202
                                     │
                            ┌────────┴────────┐
                            │   jobs table    │   status + attempts
                            │                 │   + next_attempt_at
                            └────────┬────────┘
                     claim one │ │ │ │ record outcome
                        worker 1 · 2 · … · N        (WORKERS = parallelism)
                            └── POST + HMAC ──► destination
```

**The queue is a table, not a broker.** Each row carries its own status, attempt
count and `next_attempt_at`, so the retry schedule and the "who owns this job"
lease both survive a restart. Three statements are the whole coordination
mechanism:

```sql
-- claim: one atomic statement, so two workers can never get the same job
UPDATE jobs SET status='delivering'
WHERE id = (SELECT id FROM jobs WHERE status='pending' AND next_attempt_at<=?
            ORDER BY next_attempt_at LIMIT 1)
RETURNING *;

-- at startup: a row still 'delivering' belongs to a process that is gone
UPDATE jobs SET status='pending' WHERE status='delivering';

-- the dead-letter queue is just a query
SELECT * FROM jobs WHERE status='dead';
```

---

## Key decisions

**At-least-once.** A duplicate is something the receiver absorbs by deduplicating
on `X-Webhook-Id`; a lost `payment.completed` is a payment nobody was told about.
At-most-once would turn every network blip into a lost event, and exactly-once is
not achievable over HTTP to a third party — after a timeout you cannot know
whether the receiver processed the request. The duplicate window is specific: the
delivery succeeded but we died before recording it.

**One worker per claimed job, no channel pool.** A claimed job is a *lease*,
invisible to everyone else, so a job sitting in a channel buffer is work in limbo
that shutdown has to hand back explicitly. Claiming one at a time means we never
hold a lease we are not acting on, and there is no queue to drain on the way out.
The cost is one small query per idle worker per 500ms — free at these volumes.

**Exponential backoff with jitter, and fail-fast on 4xx.** Exponential because a
receiver that is down needs time to recover. Jitter because failures arrive in
correlated batches, and without it they retry in lockstep and hit the recovering
receiver as a herd. 5xx/408/429/network errors are retried; other 4xx go straight
to the dead-letter queue, because retrying identical bytes against a deterministic
rejection is waste. The risk I accepted: a receiver that returns 400 for a
transient problem gets dead-lettered instead of retried.

**Shutdown finishes in-flight attempts.** The HTTP server stops first (so every
`202` is committed), then workers stop claiming but complete the job they hold —
on `context.Background()`, deliberately not the cancelled context. Aborting a
request already sent would manufacture a duplicate on every deploy.

**SQLite, no broker.** `synchronous=FULL` fsyncs on commit, which is what makes
the `202` an honest promise. Redis is not durable by default, and Kafka is
unjustified complexity here. The cost: a single writer, so this does not scale
past one instance — `store.go` is the only file that would change to move to
Postgres with `FOR UPDATE SKIP LOCKED`.

**HTTP+JSON, not gRPC.** The producers are services, but whoever inspects the
dead-letter queue at 3am is a human, and with HTTP that is `curl`. We already
depend on `net/http` for outbound deliveries, and JSON payloads pass through
untouched.

**Signature: `v1=HMAC-SHA256(secret, "<unix-ts>.<body>")`.** The timestamp is
signed alongside the body so a captured delivery cannot be replayed forever; the
receiver rejects anything outside a tolerance window, and the timestamp cannot be
edited in transit. Compared with `hmac.Equal`, in constant time.

---

## Testing

`make test` — 29 cases, ~3s, race-clean. They also run inside the Docker build, so
a broken commit cannot produce an image.

Tests use a real SQLite file and a real `httptest` server, because the interesting
behaviour here *is* the SQL and the HTTP. Covered, chosen for value over coverage:
end-to-end delivery (first-try, retry-then-succeed, dead after exactly
`MAX_ATTEMPTS`, 422 dead after exactly 1); shutdown finishing an in-flight
delivery; the queue's safety properties (exclusive leases, backoff, crash
recovery); signing adversarially (tampered payload, tampered timestamp, replay,
wrong secret); a backoff overflow regression; and ingest validation.

---

## What I would change before production

**Required**

- **Auth on the ingest API.** Anyone who can reach the port can enqueue a job to
  any URL.
- **Full SSRF defence.** Today: http(s) only, no redirects. Missing: a private-IP
  denylist (`169.254.169.254` above all) and pinning the resolved IP to close DNS
  rebinding.
- **Postgres instead of SQLite**, so more than one instance can share the queue.
- **Per-destination secrets** with rotation, instead of one global secret.

**Important**

- **Per-destination rate limiting and a circuit breaker.** Today one dead endpoint
  with a large backlog can occupy every worker. This is the biggest fairness gap.
- **An attempt history table.** Each job only keeps its last error and status code.
- **Retention**: the table grows forever.
- **Honour `Retry-After`** on 429/503 instead of always using our own curve.
- **Metrics and tracing** (queue depth, attempts, latency, dead rate).
- **A requeue endpoint** for the dead-letter queue — I had one and removed it while
  simplifying; recovering a dead job today means an `UPDATE` by hand.

**Known limitation.** A crash does not consume an attempt, which is deliberate —
the job should not be punished for our failure. But it means a job that
reliably crashes the process would retry forever. The fix is a separate
"times claimed" counter, not counting it as an attempt.

---

## Assumptions

- One secret for the whole service, not one per destination.
- No ordering guarantees. Jobs are independent, and retries reorder them anyway;
  ordering would need per-key serialisation, which is a different design.
- The dead-letter queue is exposed as a query, not a separate store.
- Payloads are ≤1MB and are stored as-is, since we sign the exact bytes.
- No multi-tenancy, quotas or per-destination configuration.

---

## On AI tooling

Useful for the mechanical layer: SQL scaffolding, table-driven tests, the
Dockerfile, this README. What it got wrong, and what I changed:

- **The shutdown bug.** The generated code passed the cancelled shutdown context
  into the in-flight HTTP request. That is the textbook `context` propagation
  pattern and it is wrong here: it aborts a request the receiver may already have
  processed, manufacturing a duplicate on every deploy. Now the delivery runs on
  `context.Background()`, bounded by its own timeout.
- **Over-engineering by default.** The first pass had six packages, a `Store`
  interface with one implementation, idempotency keys, a `/stats` endpoint, a
  requeue endpoint, and a semaphore-based dispatcher with a poller and a wake-up
  channel. Collapsing it to one package and one worker loop is most of the
  difference between the first version and this one.
- **Backoff overflow.** `base << attempt` overflows `int64` at high attempt counts
  and wraps negative, which would retry instantly in a tight loop. There is a
  regression test for it now.
- **Confidently wrong SQL.** It used a partial unique index as an `ON CONFLICT`
  target without SQLite's required `WHERE` clause. It failed at runtime, not at
  compile time — which is exactly why the tests hit a real database.
