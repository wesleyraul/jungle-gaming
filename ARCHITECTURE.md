# Distributed Wagering Processor — Architecture & Design Decisions

This document details the architectural decisions, concurrency mechanics, transactional integrity guarantees, and trade-offs of the Distributed Wagering Processor.

---

## 1. Architectural Style & Layering

The codebase follows **Domain-Driven Design (DDD)** and Clean Architecture principles, organized into cohesive NestJS feature modules:

```
src/
├── common/             # Cross-cutting primitives (Money VO, Enums, Filters, Guards, Metrics, Logger)
│   ├── database/       # TypeORM DataSource & reversible SQL migrations
│   ├── domain/         # DomainError hierarchy and Money Value Object
│   ├── functions/      # Canonical JSON hash and opaque cursor codecs
│   └── metrics/        # In-memory metrics aggregator & Prometheus exporter
└── app/
    ├── wallet/         # Wallet aggregate, immutable ledger entries, reconciliation
    ├── wagering/       # WagerTransaction aggregate, financial state machine, API controllers
    ├── messaging/      # SQS FIFO client, Outbox/Inbox entities, background pollers
    └── health/         # Liveness/readiness probes and Prometheus metrics endpoints
```

### Separation of Concerns
1. **Domain Layer**: Pure TypeScript aggregates (`Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `OutboxMessage`, `InboxMessage`) and Value Objects (`Money`). Absolutely zero database or framework imports inside domain entities.
2. **Infrastructure / Persistence Layer**: TypeORM entities (`*Entity`), repositories with explicit `EntityManager` transactional support, and SQL migration files.
3. **Application Layer**: NestJS services orchestrating transactions, acquiring pessimistic row locks, and maintaining metrics.
4. **Presentation Layer**: NestJS controllers with strict validation pipes (`.form.ts`), API responses (`.response.ts`), and Swagger annotations.

---

## 2. Concurrency & Locking Strategy

### Per-Wallet Serialization via Pessimistic Row Locking
Financial correctness mandates that concurrent debits and credits on a single wallet must never read stale balances. 
- **Implementation**: When processing any wager transaction, the service executes:
  ```sql
  SELECT * FROM wallets WHERE id = :walletId FOR UPDATE
  ```
- **Throughput Isolation**: The lock is scoped strictly to the specific `walletId`. Transactions across distinct wallets execute in parallel with zero lock contention.
- **Deadlock Avoidance**: Locks are always acquired in a deterministic order: the `wallets` row is locked first, before any secondary table lookups or insertions.
- **Optimistic Versioning**: Each mutation increments `wallet.version`. If an out-of-band write occurs, the version check detects the divergence immediately.

### Double-Reversal Prevention (`uq_wager_tx_processed_reversal`)
To guarantee that a transaction cannot be reversed twice (e.g. duplicate concurrent refunds or simultaneous refund and rollback):
1. **Application-level check**: Checks for existing `PROCESSED` reversals for the target reference before mutating balances.
2. **Database-level constraint**: A partial unique index guarantees absolute prevention at the PostgreSQL storage engine level:
   ```sql
   CREATE UNIQUE INDEX uq_wager_tx_processed_reversal
   ON wager_transactions (reference_transaction_id, kind)
   WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
   ```

---

## 3. Transactional Outbox & Inbox Deduplication

To avoid the Dual-Write problem (updating the database but failing to publish to SQS, or vice versa), the system uses the **Transactional Outbox Pattern**:

```
Client Request
      │
      ▼
┌────────────────────────────────────────────────────────┐
│               PostgreSQL ACID Transaction              │
│                                                        │
│  1. SELECT wallets FOR UPDATE                          │
│  2. INSERT wager_transactions                          │
│  3. INSERT wallet_ledger_entries (immutable)           │
│  4. UPDATE wallets SET balance = ..., version = +1     │
│  5. INSERT outbox_messages                             │
│  6. INSERT inbox_messages (if triggered via SQS)       │
└────────────────────────────────────────────────────────┘
      │
      ▼
Outbox Publisher (FOR UPDATE SKIP LOCKED)
      │
      ▼
AWS SQS FIFO Queue (integration-events.fifo)
```

### Worker Concurrency via `FOR UPDATE SKIP LOCKED`
When the background worker polls unpublished outbox messages, multiple worker instances can run in parallel without competing:
```sql
SELECT * FROM outbox_messages
WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
ORDER BY occurred_at ASC
LIMIT :batchSize
FOR UPDATE SKIP LOCKED;
```
Each worker claims a unique set of messages, sends them to SQS FIFO, and updates `published_at` inside its transaction.

---

## 4. Technical Trade-Offs

### MikroORM vs TypeORM
- **Why TypeORM**:
  - Direct, fine-grained control over raw transactional query builders (`setLock('pessimistic_write')` and `setOnLocked('skip_locked')`).
  - Native integration with NestJS without additional adapter layers.
  - Battle-tested migration runner and explicit entity schema mapping.
  - Simpler mental model for transactional boundary propagation (`dataSource.transaction(async manager => ...)`).

### Pessimistic vs Optimistic Locking
- **Choice**: Row-level pessimistic locking (`SELECT ... FOR UPDATE`) per wallet.
- **Trade-off**: Under high write contention on the *same* wallet (e.g. rapid-fire slot spins), optimistic locking leads to frequent serialization failures and expensive retry loops. Pessimistic locking queues operations deterministically at the PostgreSQL engine level, achieving higher net throughput and zero lost updates.

---

## 5. Security & OIDC Extension Point

Authentication is architected around a pluggable guard pattern:
- **`ProviderAuthGuard`**: Applied to all `/wallets` and `/wager-transactions` endpoints.
- **Production Roadmap**:
  - Integrate OpenID Connect (OIDC) JWT validation using `jose` or `jwks-rsa`.
  - Support mTLS (Mutual TLS) client certificates at the ingress controller.
  - Token caching with asymmetric RSA/ECDSA signature verification.

---

## 6. Observability & Metrics

- **Structured JSON Logging**: Standard JSON format with Section 12 mandatory fields: `timestamp`, `level`, `message`, `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`.
- **Prometheus Metrics (`/metrics`)**:
  - `transactions_by_status_total{status="..."}`: Counter of processed, rejected, pending_reference, and failed transactions.
  - `duplicates_detected_total`: Counter of idempotent duplicates.
  - `retries_total`: Counter of outbox and reference resolution retry attempts.
  - `sqs_dead_letter_total`: Counter of malformed or poison messages routed to DLQ.
  - `lock_conflicts_total`: Counter of lock contention instances.
  - `outbox_lag_seconds`: Gauge measuring publication latency.
  - `reconciliation_divergences_total`: Counter of ledger inconsistencies (strictly zero in production).
  - `processing_latency_ms`: Summary of latency distribution (count, avg, min, max).
