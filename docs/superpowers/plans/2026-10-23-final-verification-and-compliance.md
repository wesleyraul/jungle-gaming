# Final Compliance & Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all remaining spec gaps discovered against the verbatim `junglegaming/backend-challenge` specification (Sections 1–14) and verify 100% compliance across unit, real-container integration, 8-scenario concurrency, and load test suites.

**Architecture:** Introduce a `@Global()` `ObservabilityModule` so `MetricsService` and `AppLogger` are true singletons across all NestJS modules; align HTTP routes (`/health/live`, `/health/ready`) and SQS message envelopes (`WagerTransactionRequested` wrapper + DLQ routing for permanent vs transient errors) with Sections 9–11; enforce reference validation on optional `WIN`/`LOSS` references; and expand integration and concurrency suites to cover all mandatory scenarios (50x parallel replay, Section 8 2x80 BRL race, >=3 simultaneous app instances, crash after commit before SQS ack, dual outbox publishers, LocalStack DLQ verification, and restart recovery).

**Tech Stack:** Node 24 / Bun-compatible scripts, NestJS 11, TypeORM 0.3, PostgreSQL 16, AWS SQS FIFO (LocalStack), `decimal.js`, Jest, Supertest.

**Spec:** `docs/superpowers/specs/2026-10-22-distributed-wagering-processor-spec.md` & `https://github.com/junglegaming/backend-challenge`

## Global Constraints

- Never use `number`, `float`, or `double` for monetary values.
- Single quotes (`'`), strict TypeScript (`strict: true`, zero `as any`), no code comments.
- Never log sensitive data or financial amounts/balances in structured logs.
- Every test suite must verify the global invariant `wallet.balance == sum(ledger.credits) - sum(ledger.debits)`.

---

### Task 1: Global Observability Singleton & Log Hygiene

**Files:**
- Create: `src/common/observability/observability.module.ts`
- Modify: `src/app.module.ts`
- Modify: `src/app/wallet/wallet.module.ts`
- Modify: `src/app/messaging/messaging.module.ts`
- Modify: `src/app/wagering/wagering.module.ts`
- Modify: `src/app/health/health.module.ts`
- Modify: `src/app/wallet/services/wallet.service.ts`

**Interfaces:**
- Produces: `@Global() ObservabilityModule` exporting singleton `MetricsService` and `AppLogger`.

- [x] **Step 1: Create `src/common/observability/observability.module.ts`**
- [x] **Step 2: Remove duplicate `MetricsService` and `AppLogger` providers from feature modules and import `ObservabilityModule` in `AppModule`**
- [x] **Step 3: Remove financial balance strings from `WalletService.reconcileWallet` error log metadata**

---

### Task 2: Health Routes, Outbox Envelope & Reference Resolution Edge Cases

**Files:**
- Modify: `src/app/health/controllers/health.controller.ts`
- Modify: `src/app/messaging/services/outbox.service.ts`
- Modify: `src/app/wagering/services/wagering.service.ts`

**Interfaces:**
- Produces: `GET /health/live`, `GET /health/ready`, `GET /health/liveness`, `GET /health/readiness`; direct `IntegrationEvent.toJSON()` outbox payload publishing; complete `WIN`/`LOSS` optional reference validation and `inbox_messages` transactional persistence on all SQS deliveries.

- [x] **Step 1: Add `live` and `ready` route aliases to `HealthController`**
- [x] **Step 2: Publish `msg.payload` directly in `OutboxService.publishDueOutboxBatch`**
- [x] **Step 3: Ensure `WageringService` persists `inbox_messages` even when transaction was previously created via HTTP, avoids double-incrementing `duplicatesDetected`, handles `WIN`/`LOSS` missing/invalid references, and increments status metrics inside `retryPendingReferences`**

---

### Task 3: SQS Consumer Envelope & Error Classification (Business vs Transient vs Permanent/DLQ)

**Files:**
- Modify: `src/app/wagering/consumers/wager-transaction.consumer.ts`
- Modify: `src/app/wagering/consumers/wager-transaction.consumer.spec.ts`

**Interfaces:**
- Consumes: Section 10 SQS message envelope `{ messageId, type: 'WagerTransactionRequested', occurredAt, data: { ... } }` as well as flat payloads.
- Produces: Deterministic classification of permanent errors (`InvalidMoneyError`, `IdempotencyConflictError`, missing fields, `FORBIDDEN_TRANSACTION_KIND`) routed immediately to DLQ vs transient infrastructure errors left for SQS retry.

- [x] **Step 1: Update `WagerTransactionConsumer` to unwrap Section 10 `data` envelope and classify errors**
- [x] **Step 2: Update `wager-transaction.consumer.spec.ts` to test Section 10 envelope, permanent domain errors routing to DLQ, and transient DB errors**

---

### Task 4: Full Section 13 Integration & Concurrency Test Suites

**Files:**
- Modify: `test/integration/wagering.integration.spec.ts`
- Modify: `test/concurrency/wagering.concurrency.spec.ts`

**Interfaces:**
- Verifies all mandatory Section 8 and Section 13 integration and concurrency scenarios against real PostgreSQL and LocalStack SQS containers.

- [x] **Step 1: Expand `test/integration/wagering.integration.spec.ts` with SQS inbox redelivery, concurrent outbox publishers, LocalStack DLQ verification, `/health/live`, `/health/ready`, `/metrics`, and restart recovery**
- [x] **Step 2: Expand `test/concurrency/wagering.concurrency.spec.ts` with 50x parallel identical bet storm, Section 8 mandatory 2x80.00 BRL race on 100.00 BRL wallet, >=3 simultaneous NestJS application instances, worker crash after commit before SQS ack, dual outbox publishers, `REFUND` and `ROLLBACK` out-of-order resolution, and service restart consistency**
- [x] **Step 3: Run `yarn lint`, `yarn test:all`, `yarn test:load`, and `yarn build`**
