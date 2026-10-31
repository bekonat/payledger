# payledger

A wallet and payments backend written in Go. Money is stored as a **double-entry ledger** in PostgreSQL. Every transfer is **idempotent**. Events reach Kafka through a **transactional outbox**, and a separate consumer service processes them **exactly once** using an inbox table.

The system has three processes:

```
                 REST :8080 / gRPC :9090
 client ──────────────▶ ┌──────────┐   one transaction:
                        │  ledger  │── balances + entries + idempotency key + outbox row
                        └────┬─────┘
             rate limit ─────┤ Redis (GCRA, Lua)
                             ▼
                        ┌──────────┐
                        │PostgreSQL│
                        └────┬─────┘
           FOR UPDATE        │ outbox
           SKIP LOCKED       ▼
                        ┌──────────┐  ledger.transfers   ┌──────────┐  inbox dedup  ┌──────────┐
                        │  relay   │ ──────────────────▶ │  Kafka   │ ────────────▶ │ notifier │
                        └──────────┘   (at least once)   └──────────┘  DLQ, retries └──────────┘

 OpenTelemetry trace: HTTP request → ledger.transfer → outbox.publish → notifier.handle
```

## Features

- **Double-entry ledger.** Each transfer writes two entries that sum to zero. Deposits and withdrawals go through a per-currency system account, so the balances of all accounts in one currency always sum to zero. A `CHECK` constraint backs up the "no negative balance" rule at the database level.
- **No lost updates and no deadlocks.** Both accounts are locked with `SELECT … ORDER BY id FOR UPDATE`. Because every transaction takes its locks in the same order, A→B and B→A running at the same time cannot deadlock. A test proves this: with unordered locks it fails with `SQLSTATE 40P01`.
- **Idempotency keys.** The key is inserted first in the same transaction as the transfer. A concurrent duplicate waits on the unique index. Once the first request commits, the duplicate gets the stored transfer back (`200`, `Idempotent-Replayed: true`). Reusing a key with a different body returns `409`. If the first attempt fails, its key is rolled back with it, so the client can safely retry.
- **Transactional outbox.** The event row is written in the same transaction as the money movement, so an event can never be lost or published for a transfer that was rolled back. The relay claims batches with `FOR UPDATE SKIP LOCKED`, so several replicas can run side by side. It backs off exponentially when Kafka is unavailable, exports outbox lag as a metric, and deletes old rows in small chunks.
- **Idempotent consumer.** The notifier commits Kafka offsets only after processing. It deduplicates redeliveries with a `processed_events` inbox table that is written in the same transaction as its side effects. Transient errors are retried with backoff, and poison messages go to a DLQ topic with error headers.
- **REST and gRPC** over the same service layer, with consistent error mapping (`422`/`FailedPrecondition`, `409`/`AlreadyExists`, …). Includes gRPC health checking and reflection, plus an OpenAPI 3 spec served at `/openapi.yaml`.
- **Rate limiting** with the GCRA algorithm in a single atomic Redis Lua script. It is fail-open: a Redis outage does not take payments down.
- **Observability.** Prometheus metrics (requests, latency, transfers by result, outbox lag and batch size, consumer outcomes) and JSON logs with `trace_id`. OpenTelemetry traces follow a request **across Kafka**: the trace context is stored in the outbox row and in the Kafka message headers.
- **Keyset pagination** for account statements, backed by a matching `(account_id, id DESC)` index.
- **Migrations** are embedded in the binary and guarded by `pg_advisory_lock`, so replicas that start at the same time do not race.
- **Tests:** unit tests with fakes, `httptest`, gRPC over `bufconn`, and Redis via `miniredis`. Integration tests against real PostgreSQL cover concurrency invariants, deadlock freedom, duplicate request storms, rollback, and SKIP LOCKED. An end-to-end test runs the full docker compose stack in CI.

## Tech stack

Go 1.26 · PostgreSQL 16 (pgx/v5) · Kafka (segmentio/kafka-go) · Redis (go-redis/v9) · gRPC + protobuf · OpenTelemetry + Jaeger · Prometheus · Docker Compose · GitHub Actions

## Running

```bash
docker compose up --build -d     # postgres, redis, kafka, jaeger, prometheus, ledger, relay, notifier
./scripts/e2e.sh                 # transfer via REST and wait for the notification from Kafka
```

| Service | URL |
|---------|-----|
| Ledger REST / gRPC | http://localhost:8080 · localhost:9090 |
| Notifier | http://localhost:8081/api/v1/notifications?owner_id=… |
| Relay metrics | http://localhost:8082/metrics |
| Jaeger UI | http://localhost:16686 |
| Prometheus | http://localhost:9091 |

### Example

```bash
A=$(curl -s -X POST localhost:8080/api/v1/accounts -d '{"owner_id":"alice","currency":"KZT"}' | jq -r .id)
B=$(curl -s -X POST localhost:8080/api/v1/accounts -d '{"owner_id":"bob","currency":"KZT"}' | jq -r .id)

# 5000.00 KZT = 500000 tiyn
curl -X POST localhost:8080/api/v1/deposits -H 'Idempotency-Key: dep-1' \
  -d "{\"account_id\":\"$A\",\"amount\":500000}"

curl -X POST localhost:8080/api/v1/transfers -H 'Idempotency-Key: tr-1' \
  -d "{\"from_account_id\":\"$A\",\"to_account_id\":\"$B\",\"amount\":125050}"

curl "localhost:8080/api/v1/accounts/$A/entries?limit=20"
curl "localhost:8081/api/v1/notifications?owner_id=bob"
# {"notifications":[{"message":"Transfer +1250.50 KZT from alice. Balance: 1250.50 KZT", ...}]}
```

gRPC (reflection is enabled):

```bash
grpcurl -plaintext -d '{"owner_id":"carol","currency":"USD"}' localhost:9090 ledger.v1.LedgerService/CreateAccount
```

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/accounts` | Create an account (`owner_id`, `currency`: KZT/USD/EUR/RUB) |
| GET | `/api/v1/accounts/{id}` | Account and current balance |
| GET | `/api/v1/accounts/{id}/entries?cursor=&limit=` | Statement, newest first |
| POST | `/api/v1/deposits` | Deposit, requires `Idempotency-Key` |
| POST | `/api/v1/withdrawals` | Withdrawal, requires `Idempotency-Key` |
| POST | `/api/v1/transfers` | Transfer between accounts, requires `Idempotency-Key` |
| GET | `/api/v1/transfers/{id}` | Get a transfer |
| GET | `/healthz`, `/readyz`, `/metrics`, `/openapi.yaml` | Operations |

Amounts are integers in minor units, so floating point is never used for money. Error responses are JSON, for example `422 {"error":"unsupported currency","field":"currency"}`.

## Load test

Run on a 4-core laptop. PostgreSQL, the ledger and the load generator all run on the same machine. Each request is a transfer with a new idempotency key:

| Scenario | Throughput | p50 | p95 | p99 | Errors |
|----------|-----------:|----:|----:|----:|-------:|
| 100 accounts, 32 clients | 2236 req/s | 13 ms | 24 ms | 33 ms | 0 |
| 4 hot accounts, 32 clients | 1277 req/s | 14 ms | 77 ms | 126 ms | 0 |

After 46 475 transfers, every invariant still held: the sum of all KZT balances was 0, no user balance was negative, and every balance equaled the sum of its entries. Hot accounts show the cost of row locks: transfers on the same account are serialized, so tail latency grows. To reproduce: `make up && make load`.

## Design decisions and trade-offs

- **Why the outbox and not "write to the DB, then publish to Kafka"?** If the process crashes between the two steps, either the event is lost or it describes a transfer that never committed. The outbox makes the event part of the same ACID transaction. The cost is at-least-once delivery, which the consumer handles with the inbox table.
- **Ordering.** The Kafka key is the transfer id, so events from different relay batches can be reordered. The notifier does not depend on order. A consumer that builds balances would key by account and use `balance_after` from the entries.
- **Hot system account.** Every deposit in a currency locks the same system row. At high deposit rates this row becomes the bottleneck. The usual fix is to split it into N system accounts and choose one by hash.
- **Rate limiter clock.** The limiter takes the time from the application server, which keeps it testable. Clock skew between replicas therefore slightly changes the effective rate. Redis `TIME` inside the script would avoid that.
- **Authentication** is out of scope. The service is meant to sit behind an API gateway. `X-Client-ID` stands in for the authenticated client in rate limiting.

## Configuration

| Variable | Default | Used by |
|----------|---------|---------|
| `DATABASE_URL` | required | all |
| `HTTP_ADDR` | `:8080` (ledger), `:8081` (notifier) | ledger, notifier |
| `GRPC_ADDR` | `:9090` | ledger |
| `REDIS_URL` | empty = rate limiting off | ledger |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | `50` / `100` | ledger |
| `KAFKA_BROKERS` | `localhost:9092` | relay, notifier |
| `OUTBOX_BATCH_SIZE` | `100` | relay |
| `OUTBOX_POLL_INTERVAL` | `200ms` | relay |
| `OUTBOX_MAX_BACKOFF` | `10s` | relay |
| `OUTBOX_RETENTION` | `72h` | relay |
| `METRICS_ADDR` | `:8082` | relay |
| `KAFKA_GROUP_ID` | `notifier` | notifier |
| `MAX_ATTEMPTS` / `RETRY_BACKOFF` | `5` / `200ms` | notifier |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty = no export | all |
| `SHUTDOWN_TIMEOUT` | `10s` | ledger, notifier |

## Project structure

```
cmd/
  ledger/              REST + gRPC server, migrations, graceful shutdown
  relay/               outbox → Kafka publisher
  notifier/            Kafka consumer and notifications API
  loadtest/            load generator used for the table above
internal/
  domain/              entities, validation, domain errors, event schema
  ledger/              business logic: transfers, idempotency, outbox events
  postgres/            pgx store, row locking, outbox source, inbox store, migrator
  outbox/              relay loop: batching, backoff, metrics, trace links
  notifier/            idempotent consumer, retries, DLQ, message texts
  kafka/               kafka-go writer/reader, topic creation
  ratelimit/           GCRA limiter on Redis
  transport/httpapi/   handlers, middleware, error mapping
  transport/grpcapi/   gRPC server, interceptor, status mapping
  observability/       Prometheus metrics, OpenTelemetry setup, trace-aware logger
  config/              environment configuration
api/                   OpenAPI spec and protobuf definitions
gen/                   generated gRPC code (make proto)
migrations/            embedded SQL migrations for ledger and notifier
```

## Testing

```bash
make test               # unit tests with -race
make test-integration   # against PostgreSQL from docker compose (or TEST_DATABASE_URL)
make e2e                # full stack: REST → outbox → Kafka → notifier
```

Integration tests run 1000 random concurrent transfer attempts and then check the invariants. They also send opposite transfers at the same time to check for deadlocks, and send 10 identical requests in parallel to check that money moves exactly once. Other tests cover rollback of a failed transfer together with its idempotency key, two relays claiming disjoint outbox batches, and inbox deduplication.
