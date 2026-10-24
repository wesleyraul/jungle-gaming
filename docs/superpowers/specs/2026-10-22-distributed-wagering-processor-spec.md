# Technical Challenge — Distributed Wagering Processor (uTrip Architecture & Conventions)

## 1. Visão geral

Serviço financeiro distribuído em NestJS que processa transações de apostas recebidas de múltiplos provedores de jogos, estruturado rigorosamente nos padrões de código e organização dos microsserviços **uTrip** (`src/app/<domínio>/{controllers,services,repositories,entities,domain,forms,responses,dtos,cron,consumers}` e `src/common/...`).

Avaliação centrada em:
- correção financeira (`Money` com `decimal.js`, escala 2, sem `number`/`float`);
- concorrência entre múltiplas instâncias (`SELECT ... FOR UPDATE` por `walletId` + `FOR UPDATE SKIP LOCKED` em workers);
- idempotência persistente (`idempotency_key` + `payload_hash` SHA-256 canônico e `inbox_messages`);
- consistência entre saldo materializado e ledger imutável (validado por constraints e triggers no PostgreSQL);
- processamento assíncrono via AWS SQS (LocalStack), Transactional Outbox e recuperação após falhas;
- padrões uTrip: aspas simples (`'`), sem comentários no código, sem `as any`, `.form.ts` para inputs HTTP, `.response.ts` para outputs HTTP, `.dto.ts` para tráfego interno, módulos exportando apenas `*Service`, testes com Jest.

---

## 2. Stack & Convenções uTrip

| Item | Escolha |
|---|---|
| Runtime / package manager | **Bun 1.x** |
| Test runner | **Jest** (`jest` / `ts-jest`, compatível com `bun run test`) |
| Linguagem | **TypeScript** em modo estrito (`strict: true`, zero `as any`) |
| Framework | **NestJS 11** |
| ORM | **TypeORM 0.3** (`DataSource`, `QueryRunner`, `pessimistic_write`, migrations reversíveis) |
| Banco | **PostgreSQL 16** |
| Mensageria | **AWS SQS** via **LocalStack** (`wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `integration-events.fifo`) |
| Orquestração local | **Docker Compose** |
| Estilo uTrip | Aspas simples (`'`), zero comentários em código, `.form.ts` / `.response.ts` / `.dto.ts`, `EnvProvider`, `AppLogger`, módulos exportam apenas `*Service` |

---

## 3. Restrições invioláveis

1. Não usar `number`, `float` ou `double` para dinheiro.
2. Não usar cache em memória como garantia de idempotência.
3. Não confiar apenas em SQS FIFO para garantir consistência.
4. Não publicar eventos antes do commit da transação financeira.
5. Não sobrescrever nem excluir lançamentos do ledger (garantido também por trigger no PostgreSQL).
6. Não usar lock global compartilhado por todas as wallets.
7. Não implementar saldo como `read -> calculate -> update` sem controle de concorrência (`SELECT ... FOR UPDATE` na linha da `wallet`).
8. A solução deve estar correta com **múltiplas instâncias** da aplicação.
9. As garantias de unicidade, imutabilidade e não-negatividade devem ser aplicadas **no schema do banco** (constraints `CHECK`, `UNIQUE`, `FOREIGN KEY`, partial unique indexes e triggers).

---

## 4. Modelo de Domínio Obrigatório

- Construtores `private` ou `protected` + factories estáticas (`create`, `open`, `from`, `zero`, `receive`, `enqueue`, `rehydrate`).
- `Money`: `from`, `zero`, `add`, `subtract`, `negate`, `isZero`, `isPositive`, `isNegative`, `isLessThan`, `equals`, `toJSON`, `toString`, `assertSameCurrency`.
- `Wallet`: `open`, `rehydrate`, `debit`, `credit`, `balance`, `version`, `updatedAt`.
- `WagerTransaction`: `WagerTransactionKind` (`OPENING`, `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), `WagerTransactionStatus` (`PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED`, `FAILED`), `create`, `rehydrate`, `markProcessed`, `markPendingReference`, `reject`, `fail`, `isTerminal`, `affectsBalance`, `requiresReference`, `matchesPayload`, `ledgerDirectionFor`.
- `WalletLedgerEntry`: `LedgerDirection` (`DEBIT`, `CREDIT`), `create`, `rehydrate`, `isBalanced`.
- `InboxMessage`: `receive`, `rehydrate`, `isProcessed`, `markProcessed`.
- `OutboxMessage`: `enqueue`, `rehydrate`, `isPending`, `isDue`, `markPublished`, `scheduleRetry`.
- `IntegrationEvent<T>` (abstrata) e subclasses concretas:
  - `WagerTransactionProcessed`
  - `WagerTransactionRejected`
  - `WalletBalanceChanged`
  - `WagerTransactionPendingReference`

---

## 5. Endpoints HTTP, SQS, Outbox, Observabilidade e Testes

- `POST /wallets`
- `GET /wallets/:walletId`
- `GET /wallets/:walletId/ledger?cursor=...&limit=50`
- `POST /wallets/:walletId/reconciliation`
- `POST /wagering/transactions` (com header `Idempotency-Key`)
- `GET /wagering/transactions/:transactionId`
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId`
- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`
- Consumidor SQS `wager-transactions.fifo` + DLQ `wager-transactions-dlq.fifo`
- Worker Outbox (`FOR UPDATE SKIP LOCKED`)
- Worker `PENDING_REFERENCE` (`FOR UPDATE SKIP LOCKED` com backoff exponencial e expiração para `REJECTED` com `REFERENCE_NOT_FOUND`)
- Testes de Unidade, Integração, Concorrência (8 cenários reais em paralelo) e Carga (`bun run test:load`)
