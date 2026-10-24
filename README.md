# Distributed Wagering Processor

High-throughput, distributed financial wagering processor built with **NestJS**, **TypeORM**, **PostgreSQL**, and **AWS SQS FIFO**, adhering to enterprise financial standards, exact decimal arithmetic, transactional idempotency, and concurrency safety.

---

## 1. Quick Start

### Prerequisites
- **Node.js**: v24.x (Tested on Node 24.0.1)
- **Yarn**: v1.22+
- **Docker & Docker Compose**: v24+

### Setup & Launch

1. **Start Infrastructure (PostgreSQL & LocalStack SQS FIFO)**:
   ```bash
   docker compose up -d postgres localstack
   ```

2. **Install Dependencies & Configure Environment**:
   ```bash
   yarn install
   cp .env.example .env
   ```

3. **Run Database Migrations**:
   ```bash
   yarn migration:run
   ```

4. **Start Application**:
   ```bash
   yarn start:dev
   ```

   The application starts on port `3000`.
   - **Swagger / OpenAPI Documentation**: `http://localhost:3000/docs`
   - **Prometheus Metrics**: `http://localhost:3000/metrics`
   - **Liveness Probe**: `http://localhost:3000/health/liveness`
   - **Readiness Probe**: `http://localhost:3000/health/readiness`

---

## 2. Test Execution

The codebase provides test suites categorized by scope:

| Suite | Command | Description |
|---|---|---|
| **Unit Tests** | `yarn test:unit` | Fast isolated tests for Domain Aggregates, Value Objects, Enums, Metrics, and Services (mocked DB/SQS). |
| **Integration Tests** | `yarn test:integration` | End-to-end HTTP tests against real PostgreSQL and LocalStack SQS (triggers, outbox, migration). |
| **Concurrency Suite** | `yarn test:concurrency` | 8 parallel race-condition scenarios (idempotency storms, balance depletion, simultaneous reversals, out-of-order delivery). |
| **Load Test** | `yarn test:load` | 100 concurrent operations measuring RPS, p95/p99 latency, and verifying zero ledger divergence. |
| **All Tests** | `yarn test:all` | Executes full test harness sequentially. |

---

## 3. Core Architecture & Financial Guarantees

### Exact Decimal Precision (`Money` Value Object)
- Floating-point representations (`number`, `float`, `double`) are strictly prohibited in financial paths.
- Monetary amounts use `decimal.js` with fixed 2-decimal scale rounding (`ROUND_HALF_EVEN`), serialized as exact string representations (`numeric(20, 2)` in PostgreSQL).

### Idempotency Guarantee
- Every mutating request requires an `Idempotency-Key` header.
- A canonical SHA-256 payload hash (`payloadHash`) sorts JSON keys recursively.
  - **Identical Key + Identical Payload**: Returns the previously calculated response with HTTP 200 without duplicate execution or double debits/credits.
  - **Identical Key + Different Payload**: Rejects immediately with HTTP 409 Conflict (`IDEMPOTENCY_CONFLICT`).

### Per-Wallet Concurrency Control (`SELECT ... FOR UPDATE`)
- Concurrent operations on the same wallet serialize via row-level pessimistic locking (`SELECT ... FOR UPDATE` on `wallets` table).
- Independent wallets process in parallel without contention.
- Optimistic locking `version` column increments on each balance mutation, preventing lost updates.

### Immutable Ledger & DB Trigger
- Every financial mutation creates a balance snapshot in `wallet_ledger_entries`.
- PostgreSQL trigger `trg_immutable_wallet_ledger_entries` executes `BEFORE UPDATE OR DELETE`, blocking any modification or deletion at the schema level.

### Transactional Outbox & SQS FIFO
- Domain events (`WagerTransactionProcessed`, `WalletBalanceChanged`, `WagerTransactionPendingReference`) are written to `outbox_messages` in the same SQL transaction as wallet balance mutations.
- Background worker `OutboxPublisherCron` claims unpublished messages using `SELECT ... FOR UPDATE SKIP LOCKED` and publishes to SQS FIFO (`integration-events.fifo`), keyed by `MessageGroupId = walletId`.

### Out-of-Order Reversals (`PENDING_REFERENCE`)
- If a `ROLLBACK` or `REFUND` arrives before its original transaction, it enters `PENDING_REFERENCE` status and emits `WagerTransactionPendingReference`.
- Worker `PendingReferenceCron` retries resolution with exponential backoff up to 5 attempts or 60 seconds TTL before final rejection with `REFERENCE_NOT_FOUND`.

---

## 4. API Endpoints Overview

| Method | Path | Description |
|---|---|---|
| `POST` | `/wallets` | Creates wallet with initial balance and `OPENING` ledger entry. |
| `GET` | `/wallets/:id` | Returns current wallet balance and version. |
| `GET` | `/wallets/:id/ledger` | Returns cursor-paginated ledger entries (`nextCursor`). |
| `GET/POST` | `/wallets/:id/reconciliation` | Verifies mathematical equality between stored wallet balance and sum of ledger credits/debits. |
| `POST` | `/wagering/transactions` | Submits financial wager transaction (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`). Requires `Idempotency-Key` header. |
| `GET` | `/wagering/transactions/:id` | Retrieves transaction by internal UUID. |
| `GET` | `/providers/:providerId/wagering/transactions/:externalId` | Retrieves transaction by external provider ID. |
| `GET` | `/health/live` | Liveness probe (`/health/liveness` alias supported). |
| `GET` | `/health/ready` | Readiness probe verifying PostgreSQL and SQS connectivity. Returns 503 if down. |
| `GET` | `/metrics` | Prometheus metrics exposition format. |
| `GET` | `/metrics/json` | JSON snapshot of system metrics. |

---

## 5. Load Test Report (`yarn test:load` / `bun run test:load`)

- **Environment**: Localhost (Node v24.0.1, PostgreSQL 16-alpine in Docker, LocalStack 3.7 SQS FIFO in Docker).
- **Methodology**: 100 operations (`BET` and `WIN` of `5.00 BRL`) dispatched in concurrent batches of 10 against a single hot wallet (`100000.00 BRL` initial balance) to stress per-wallet `SELECT ... FOR UPDATE` serialization, followed by full ledger reconciliation.
- **Measured Results**:
  - **Throughput**: ~130 req/sec (single hot wallet serialized at DB row level)
  - **Latency**: p50 = `47ms`, p95 = `85ms`, p99 = `121ms`
  - **Error Rate**: `0.0%` (`100.0%` success rate)
  - **Lock Conflicts / Deadlocks**: `0`
  - **Outbox Lag**: `< 1s`
  - **Final Reconciliation**: `CONSISTENT` (`99840.00 BRL` stored == `99840.00 BRL` reconstructed from 101 ledger entries, `0.00 BRL` difference).
