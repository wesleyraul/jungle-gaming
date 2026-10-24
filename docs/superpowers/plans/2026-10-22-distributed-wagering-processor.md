# Distributed Wagering Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a distributed, multi-instance financial wagering processor in NestJS following the Jungle Gaming backend challenge specification and uTrip codebase organization and conventions.

**Architecture:** Modular NestJS microservice organized under `src/app/<domain>/{controllers,services,repositories,entities,domain,forms,responses,dtos,cron,consumers}` and `src/common/`. Financial consistency is enforced via immutable `Money` (`decimal.js`, scale 2), row-level pessimistic locking (`SELECT ... FOR UPDATE`) per `walletId` inside single SQL transactions covering Wallet, immutable Ledger, persistent Inbox, and Transactional Outbox, backed by PostgreSQL schema constraints/triggers and AWS SQS FIFO via LocalStack.

**Tech Stack:** Bun 1.x, TypeScript 5.x (`strict: true`), NestJS 11, TypeORM 0.3, PostgreSQL 16, AWS SDK v3 SQS (`@aws-sdk/client-sqs`), LocalStack, `decimal.js`, `class-validator`, `class-transformer`, `@nestjs/swagger`, Jest (`ts-jest`), Supertest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-22-distributed-wagering-processor-spec.md`

## Global Constraints

- **Money Representation:** Never use `number`, `float`, or `double` for monetary values; use `Decimal` (`decimal.js`) internally, `numeric(20, 2)` in PostgreSQL, and `{ amount: '25.00', currency: 'BRL' }` in JSON contracts.
- **uTrip Code Style:** Exclusive use of single quotes (`'`), zero explanatory comments or docstrings in code files, strict TypeScript (`strict: true`, zero `as any`), lean refactored functions.
- **uTrip Naming & Encapsulation:** HTTP inputs use `.form.ts` (`*Form`), HTTP outputs use `.response.ts` (`*Response`), internal service/repository payloads use `.dto.ts` (`*Dto`). NestJS modules export ONLY `*Service` classes, never `*Repository`.
- **Database Guarantees:** Uniqueness, non-negativity (`balance_amount >= 0`), ledger arithmetic balance, and ledger immutability (via PostgreSQL trigger blocking `UPDATE`/`DELETE`) must be enforced in PostgreSQL schema migrations.
- **Concurrency & Atomicity:** Concurrency unit is `walletId` (`SELECT ... FOR UPDATE` per wallet, never a global lock). Wallet update, Ledger entry, WagerTransaction state, Inbox record, and Outbox events execute in a single SQL transaction. Events are published only after commit via `FOR UPDATE SKIP LOCKED` workers.
- **Testing:** Jest (`jest` + `ts-jest`) for unit, integration (real PostgreSQL + LocalStack containers), and parallel concurrency tests (`wallet.balance == ledger reconstructed balance`).

---

## File Structure

```text
e:/projetos/jungle-gaming/
├── docker-compose.yml
├── Dockerfile
├── localstack/
│   └── init-aws.sh
├── package.json
├── tsconfig.json
├── tsconfig.build.json
├── eslint.config.mjs
├── .prettierrc
├── .gitignore
├── jest.config.ts
├── README.md
├── ARCHITECTURE.md
├── src/
│   ├── main.ts
│   ├── app.module.ts
│   ├── common/
│   │   ├── constants/
│   │   │   └── queues.constant.ts
│   │   ├── database/
│   │   │   ├── data-source.ts
│   │   │   └── migrations/
│   │   │       └── 1761130000000-InitialWageringSchema.ts
│   │   ├── domain/
│   │   │   ├── domain.error.ts
│   │   │   ├── money.domain.ts
│   │   │   └── money.domain.spec.ts
│   │   ├── enums/
│   │   │   ├── failure-code.enum.ts
│   │   │   ├── ledger-direction.enum.ts
│   │   │   ├── wager-transaction-kind.enum.ts
│   │   │   └── wager-transaction-status.enum.ts
│   │   ├── filters/
│   │   │   └── domain-exception.filter.ts
│   │   ├── functions/
│   │   │   ├── canonical-hash.function.ts
│   │   │   ├── canonical-hash.function.spec.ts
│   │   │   └── cursor.function.ts
│   │   ├── guards/
│   │   │   └── provider-auth.guard.ts
│   │   ├── interceptors/
│   │   │   └── request.interceptor.ts
│   │   ├── logger/
│   │   │   └── app.logger.ts
│   │   ├── metrics/
│   │   │   └── metrics.service.ts
│   │   └── providers/
│   │       └── env.provider.ts
│   └── app/
│       ├── wallet/
│       │   ├── wallet.module.ts
│       │   ├── controllers/
│       │   │   └── wallet.controller.ts
│       │   ├── domain/
│       │   │   ├── wallet.domain.ts
│       │   │   ├── wallet.domain.spec.ts
│       │   │   ├── wallet-ledger-entry.domain.ts
│       │   │   └── wallet-ledger-entry.domain.spec.ts
│       │   ├── dtos/
│       │   │   └── create-wallet.dto.ts
│       │   ├── entities/
│       │   │   ├── wallet.entity.ts
│       │   │   └── wallet-ledger-entry.entity.ts
│       │   ├── forms/
│       │   │   ├── create-wallet.form.ts
│       │   │   ├── get-wallet-ledger.form.ts
│       │   │   └── money.form.ts
│       │   ├── repositories/
│       │   │   ├── wallet.repository.ts
│       │   │   └── wallet-ledger.repository.ts
│       │   ├── responses/
│       │   │   ├── money.response.ts
│       │   │   ├── wallet.response.ts
│       │   │   ├── wallet-ledger.response.ts
│       │   │   └── wallet-reconciliation.response.ts
│       │   └── services/
│       │       └── wallet.service.ts
│       ├── messaging/
│       │   ├── messaging.module.ts
│       │   ├── cron/
│       │   │   └── outbox-publisher.cron.ts
│       │   ├── domain/
│       │   │   ├── inbox-message.domain.ts
│       │   │   ├── outbox-message.domain.ts
│       │   │   ├── inbox-outbox.domain.spec.ts
│       │   │   └── events/
│       │   │       ├── integration-event.domain.ts
│       │   │       ├── wallet-balance-changed.event.ts
│       │   │       ├── wager-transaction-processed.event.ts
│       │   │       ├── wager-transaction-rejected.event.ts
│       │   │       └── wager-transaction-pending-reference.event.ts
│       │   ├── dtos/
│       │   │   └── sqs-wager-message.dto.ts
│       │   ├── entities/
│       │   │   ├── inbox-message.entity.ts
│       │   │   └── outbox-message.entity.ts
│       │   ├── repositories/
│       │   │   ├── inbox.repository.ts
│       │   │   └── outbox.repository.ts
│       │   └── services/
│       │       ├── outbox.service.ts
│       │       └── sqs.service.ts
│       ├── wagering/
│       │   ├── wagering.module.ts
│       │   ├── consumers/
│       │   │   └── wager-transaction.consumer.ts
│       │   ├── controllers/
│       │   │   ├── provider-wagering.controller.ts
│       │   │   └── wagering.controller.ts
│       │   ├── cron/
│       │   │   └── pending-reference.cron.ts
│       │   ├── domain/
│       │   │   ├── wager-transaction.domain.ts
│       │   │   └── wager-transaction.domain.spec.ts
│       │   ├── dtos/
│       │   │   └── process-wager-transaction.dto.ts
│       │   ├── entities/
│       │   │   └── wager-transaction.entity.ts
│       │   ├── forms/
│       │   │   └── submit-wager-transaction.form.ts
│       │   ├── repositories/
│       │   │   └── wager-transaction.repository.ts
│       │   ├── responses/
│       │   │   ├── submit-wager-transaction.response.ts
│       │   │   └── wager-transaction.response.ts
│       │   └── services/
│       │       ├── wagering.service.ts
│       │       └── wagering.service.spec.ts
│       └── health/
│           ├── health.module.ts
│           ├── controllers/
│           │   ├── health.controller.ts
│           │   └── metrics.controller.ts
│           ├── responses/
│           │   └── health.response.ts
│           └── services/
│               └── health.service.ts
└── test/
    ├── helpers/
    │   └── test-app.helper.ts
    ├── integration/
    │   └── wagering.integration.spec.ts
    ├── concurrency/
    │   └── wagering.concurrency.spec.ts
    └── load/
        └── load-test.ts
```

---

### Task 1: Project Scaffolding, Docker Compose, `EnvProvider` & Observability Primitives

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `tsconfig.build.json`
- Create: `.prettierrc`
- Create: `eslint.config.mjs`
- Create: `.gitignore`
- Create: `jest.config.ts`
- Create: `docker-compose.yml`
- Create: `Dockerfile`
- Create: `localstack/init-aws.sh`
- Create: `src/common/providers/env.provider.ts`
- Create: `src/common/constants/queues.constant.ts`
- Create: `src/common/logger/app.logger.ts`
- Create: `src/common/metrics/metrics.service.ts`
- Create: `src/common/interceptors/request.interceptor.ts`
- Create: `src/common/guards/provider-auth.guard.ts`
- Test: `src/common/metrics/metrics.service.spec.ts`

**Interfaces:**
- Consumes: Node/Bun `process.env`
- Produces: `EnvProvider`, `QUEUES`, `AppLogger`, `MetricsService`, `RequestInterceptor`, `ProviderAuthGuard`, `ProviderIdentityPort`

- [ ] **Step 1: Create project configuration files and Docker infrastructure**

Create `package.json`:
```json
{
  "name": "jungle-wagering-processor",
  "version": "1.0.0",
  "private": true,
  "license": "UNLICENSED",
  "scripts": {
    "build": "nest build",
    "format": "prettier --write \"src/**/*.ts\" \"test/**/*.ts\"",
    "start": "bun run src/main.ts",
    "start:dev": "bun --watch run src/main.ts",
    "start:prod": "bun run dist/src/main.js",
    "lint": "eslint \"{src,test}/**/*.ts\"",
    "migration:run": "bun run typeorm migration:run -d src/common/database/data-source.ts",
    "migration:revert": "bun run typeorm migration:revert -d src/common/database/data-source.ts",
    "test": "jest --selectProjects unit",
    "test:unit": "jest --selectProjects unit",
    "test:integration": "jest --selectProjects integration --runInBand",
    "test:concurrency": "jest --selectProjects concurrency --runInBand",
    "test:all": "jest --runInBand",
    "test:load": "bun run test/load/load-test.ts"
  },
  "dependencies": {
    "@aws-sdk/client-sqs": "^3.750.0",
    "@nestjs/common": "^11.1.0",
    "@nestjs/core": "^11.1.0",
    "@nestjs/platform-express": "^11.1.0",
    "@nestjs/schedule": "^6.0.0",
    "@nestjs/swagger": "^11.1.0",
    "@nestjs/typeorm": "^11.0.0",
    "class-transformer": "^0.5.1",
    "class-validator": "^0.14.1",
    "decimal.js": "^10.6.0",
    "dotenv": "^16.4.7",
    "pg": "^8.13.3",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "typeorm": "^0.3.20",
    "uuid": "^11.1.0"
  },
  "devDependencies": {
    "@eslint/js": "^9.20.0",
    "@nestjs/cli": "^11.0.0",
    "@nestjs/testing": "^11.1.0",
    "@types/express": "^5.0.0",
    "@types/jest": "^29.5.14",
    "@types/node": "^22.13.4",
    "@types/supertest": "^6.0.2",
    "eslint": "^9.20.0",
    "eslint-config-prettier": "^10.0.1",
    "eslint-plugin-prettier": "^5.2.3",
    "globals": "^15.14.0",
    "jest": "^29.7.0",
    "prettier": "^3.5.1",
    "supertest": "^7.0.0",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "tsconfig-paths": "^4.2.0",
    "typescript": "^5.7.3",
    "typescript-eslint": "^8.24.0"
  }
}
```

Create `tsconfig.json`:
```json
{
  "compilerOptions": {
    "module": "commonjs",
    "declaration": true,
    "removeComments": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "allowSyntheticDefaultImports": true,
    "target": "ES2022",
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": "./",
    "rootDir": "./",
    "strict": true,
    "strictPropertyInitialization": false,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "esModuleInterop": true
  },
  "include": ["src/**/*", "test/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

Create `tsconfig.build.json`:
```json
{
  "extends": "./tsconfig.json",
  "include": ["src/**/*"],
  "exclude": ["node_modules", "test", "dist", "**/*spec.ts"]
}
```

Create `.prettierrc`:
```json
{
  "singleQuote": true,
  "trailingComma": "all"
}
```

Create `eslint.config.mjs`:
```javascript
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettierConfig from 'eslint-config-prettier';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettierConfig,
  {
    files: ['src/**/*.ts', 'test/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      quotes: ['error', 'single', { avoidEscape: true }],
    },
  },
);
```

Create `.gitignore`:
```text
node_modules/
dist/
coverage/
.env
.agents/
graphify-out/
```

Create `jest.config.ts`:
```typescript
import type { Config } from 'jest';

const baseProject = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  transform: {
    '^.+\\.(t|j)s$': ['ts-jest', { tsconfig: 'tsconfig.json' }],
  },
  testEnvironment: 'node',
};

const config: Config = {
  projects: [
    {
      ...baseProject,
      displayName: 'unit',
      rootDir: 'src',
      testRegex: '.*\\.spec\\.ts$',
    },
    {
      ...baseProject,
      displayName: 'integration',
      rootDir: 'test/integration',
      testRegex: '.*\\.integration\\.spec\\.ts$',
    },
    {
      ...baseProject,
      displayName: 'concurrency',
      rootDir: 'test/concurrency',
      testRegex: '.*\\.concurrency\\.spec\\.ts$',
    },
  ],
};

export default config;
```

Create `docker-compose.yml`:
```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: jungle-postgres
    environment:
      POSTGRES_USER: jungle
      POSTGRES_PASSWORD: jungle_password
      POSTGRES_DB: jungle_wagering
    ports:
      - '5432:5432'
    healthcheck:
      test: ['CMD-SHELL', 'pg_isready -U jungle -d jungle_wagering']
      interval: 5s
      timeout: 5s
      retries: 10

  localstack:
    image: localstack/localstack:3.7
    container_name: jungle-localstack
    environment:
      SERVICES: sqs
      AWS_DEFAULT_REGION: us-east-1
    ports:
      - '4566:4566'
    volumes:
      - './localstack/init-aws.sh:/etc/localstack/init/ready.d/init-aws.sh'
    healthcheck:
      test: ['CMD-SHELL', 'awslocal sqs list-queues']
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build: .
    environment:
      NODE_ENV: production
      PORT: 3000
      DB_HOST: postgres
      DB_PORT: 5432
      DB_USER: jungle
      DB_PASS: jungle_password
      DB_NAME: jungle_wagering
      AWS_REGION: us-east-1
      AWS_ENDPOINT: http://localstack:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      SQS_WAGER_QUEUE_URL: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo
      SQS_WAGER_DLQ_URL: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions-dlq.fifo
      SQS_EVENTS_QUEUE_URL: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/integration-events.fifo
    depends_on:
      postgres:
        condition: service_healthy
      localstack:
        condition: service_healthy
    ports:
      - '3000:3000'
```

Create `Dockerfile`:
```dockerfile
FROM oven/bun:1.2-alpine
WORKDIR /app
COPY package.json ./
RUN bun install
COPY . .
RUN bun run build
EXPOSE 3000
CMD ["bun", "run", "start:prod"]
```

Create `localstack/init-aws.sh`:
```bash
#!/bin/bash
set -e

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions-dlq.fifo \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

awslocal sqs create-queue \
  --queue-name integration-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false
```

- [ ] **Step 2: Write the failing test for `MetricsService` and `EnvProvider`**

Create `src/common/metrics/metrics.service.spec.ts`:
```typescript
import { MetricsService } from './metrics.service';

describe('MetricsService', () => {
  let service: MetricsService;

  beforeEach(() => {
    service = new MetricsService();
  });

  it('tracks transactions by status, duplicates, retries, dlq, lock conflicts, outbox lag, and latency', () => {
    service.incrementTransactionStatus('PROCESSED');
    service.incrementTransactionStatus('PROCESSED');
    service.incrementTransactionStatus('REJECTED');
    service.incrementDuplicatesDetected();
    service.incrementRetries();
    service.incrementDlqMessages();
    service.incrementLockConflicts();
    service.setOutboxLagMs(150);
    service.recordProcessingLatencyMs(12);
    service.recordProcessingLatencyMs(28);
    service.incrementReconciliationDivergence();

    const snapshot = service.getSnapshot();

    expect(snapshot.transactionsByStatus['PROCESSED']).toBe(2);
    expect(snapshot.transactionsByStatus['REJECTED']).toBe(1);
    expect(snapshot.duplicatesDetected).toBe(1);
    expect(snapshot.retries).toBe(1);
    expect(snapshot.dlqMessages).toBe(1);
    expect(snapshot.lockConflicts).toBe(1);
    expect(snapshot.outboxLagMs).toBe(150);
    expect(snapshot.reconciliationDivergences).toBe(1);
    expect(snapshot.processingLatencyMs.count).toBe(2);
    expect(snapshot.processingLatencyMs.avgMs).toBe(20);
  });
});
```

- [ ] **Step 3: Install dependencies and run the test to verify it fails**

Run: `bun install && bun run test:unit src/common/metrics/metrics.service.spec.ts`
Expected: FAIL with `Cannot find module './metrics.service'`

- [ ] **Step 4: Implement `EnvProvider`, `QUEUES`, `AppLogger`, `MetricsService`, `RequestInterceptor`, and `ProviderAuthGuard`**

Create `src/common/providers/env.provider.ts`:
```typescript
import * as dotenv from 'dotenv';

dotenv.config();

export class EnvProvider {
  static NODE_ENV = process.env.NODE_ENV || 'development';
  static PORT = Number(process.env.PORT || '3000');
  static DB_HOST = process.env.DB_HOST || 'localhost';
  static DB_PORT = Number(process.env.DB_PORT || '5432');
  static DB_USER = process.env.DB_USER || 'jungle';
  static DB_PASS = process.env.DB_PASS || 'jungle_password';
  static DB_NAME = process.env.DB_NAME || 'jungle_wagering';
  static AWS_REGION = process.env.AWS_REGION || 'us-east-1';
  static AWS_ENDPOINT = process.env.AWS_ENDPOINT || 'http://localhost:4566';
  static AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'test';
  static AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || 'test';
  static SQS_WAGER_QUEUE_URL =
    process.env.SQS_WAGER_QUEUE_URL ||
    'http://localhost:4566/000000000000/wager-transactions.fifo';
  static SQS_WAGER_DLQ_URL =
    process.env.SQS_WAGER_DLQ_URL ||
    'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
  static SQS_EVENTS_QUEUE_URL =
    process.env.SQS_EVENTS_QUEUE_URL ||
    'http://localhost:4566/000000000000/integration-events.fifo';
  static SQS_CONSUMER_ENABLED =
    (process.env.SQS_CONSUMER_ENABLED || 'true') === 'true';
  static WORKERS_ENABLED = (process.env.WORKERS_ENABLED || 'true') === 'true';
  static PENDING_REF_MAX_ATTEMPTS = Number(
    process.env.PENDING_REF_MAX_ATTEMPTS || '5',
  );
  static PENDING_REF_TTL_MS = Number(
    process.env.PENDING_REF_TTL_MS || '60000',
  );
  static SQS_MAX_RECEIVE_COUNT = Number(
    process.env.SQS_MAX_RECEIVE_COUNT || '3',
  );

  static initialize(): void {
    EnvProvider.NODE_ENV = process.env.NODE_ENV || 'development';
    EnvProvider.PORT = Number(process.env.PORT || '3000');
    EnvProvider.DB_HOST = process.env.DB_HOST || 'localhost';
    EnvProvider.DB_PORT = Number(process.env.DB_PORT || '5432');
    EnvProvider.DB_USER = process.env.DB_USER || 'jungle';
    EnvProvider.DB_PASS = process.env.DB_PASS || 'jungle_password';
    EnvProvider.DB_NAME = process.env.DB_NAME || 'jungle_wagering';
    EnvProvider.AWS_REGION = process.env.AWS_REGION || 'us-east-1';
    EnvProvider.AWS_ENDPOINT =
      process.env.AWS_ENDPOINT || 'http://localhost:4566';
    EnvProvider.SQS_WAGER_QUEUE_URL =
      process.env.SQS_WAGER_QUEUE_URL ||
      'http://localhost:4566/000000000000/wager-transactions.fifo';
    EnvProvider.SQS_WAGER_DLQ_URL =
      process.env.SQS_WAGER_DLQ_URL ||
      'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
    EnvProvider.SQS_EVENTS_QUEUE_URL =
      process.env.SQS_EVENTS_QUEUE_URL ||
      'http://localhost:4566/000000000000/integration-events.fifo';
    EnvProvider.SQS_CONSUMER_ENABLED =
      (process.env.SQS_CONSUMER_ENABLED || 'true') === 'true';
    EnvProvider.WORKERS_ENABLED =
      (process.env.WORKERS_ENABLED || 'true') === 'true';
  }
}
```

Create `src/common/constants/queues.constant.ts`:
```typescript
export const QUEUES = {
  WAGER_TRANSACTIONS_FIFO: 'wager-transactions.fifo',
  WAGER_TRANSACTIONS_DLQ_FIFO: 'wager-transactions-dlq.fifo',
  INTEGRATION_EVENTS_FIFO: 'integration-events.fifo',
  WAGER_CONSUMER_NAME: 'wager-transaction-consumer',
} as const;
```

Create `src/common/logger/app.logger.ts`:
```typescript
import { Injectable, LoggerService } from '@nestjs/common';

export interface StructuredLogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  context?: string;
  [key: string]: string | number | boolean | undefined;
}

@Injectable()
export class AppLogger implements LoggerService {
  log(message: string, meta?: StructuredLogContext | string): void {
    this.emit('INFO', message, meta);
  }

  error(
    message: string,
    trace?: string,
    meta?: StructuredLogContext | string,
  ): void {
    const contextObj = typeof meta === 'string' ? { context: meta } : meta;
    this.emit('ERROR', message, { ...contextObj, trace });
  }

  warn(message: string, meta?: StructuredLogContext | string): void {
    this.emit('WARN', message, meta);
  }

  private emit(
    level: string,
    message: string,
    meta?: StructuredLogContext | string,
  ): void {
    if (process.env.NODE_ENV === 'test' && process.env.ENABLE_TEST_LOGS !== 'true') {
      return;
    }
    const base = typeof meta === 'string' ? { context: meta } : (meta ?? {});
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...base,
    };
    process.stdout.write(`${JSON.stringify(entry)}\n`);
  }
}
```

Create `src/common/metrics/metrics.service.ts`:
```typescript
import { Injectable } from '@nestjs/common';

export interface LatencyMetrics {
  count: number;
  totalMs: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
}

export interface MetricsSnapshot {
  transactionsByStatus: Record<string, number>;
  duplicatesDetected: number;
  retries: number;
  dlqMessages: number;
  lockConflicts: number;
  outboxLagMs: number;
  reconciliationDivergences: number;
  processingLatencyMs: LatencyMetrics;
}

@Injectable()
export class MetricsService {
  private readonly transactionsByStatus: Record<string, number> = {
    PENDING: 0,
    PENDING_REFERENCE: 0,
    PROCESSED: 0,
    REJECTED: 0,
    FAILED: 0,
  };
  private duplicatesDetected = 0;
  private retries = 0;
  private dlqMessages = 0;
  private lockConflicts = 0;
  private outboxLagMs = 0;
  private reconciliationDivergences = 0;
  private latencyCount = 0;
  private latencyTotalMs = 0;
  private latencyMinMs = 0;
  private latencyMaxMs = 0;

  incrementTransactionStatus(status: string): void {
    this.transactionsByStatus[status] =
      (this.transactionsByStatus[status] ?? 0) + 1;
  }

  incrementDuplicatesDetected(): void {
    this.duplicatesDetected += 1;
  }

  incrementRetries(): void {
    this.retries += 1;
  }

  incrementDlqMessages(): void {
    this.dlqMessages += 1;
  }

  incrementLockConflicts(): void {
    this.lockConflicts += 1;
  }

  setOutboxLagMs(lagMs: number): void {
    this.outboxLagMs = Math.max(0, Math.round(lagMs));
  }

  incrementReconciliationDivergence(): void {
    this.reconciliationDivergences += 1;
  }

  recordProcessingLatencyMs(durationMs: number): void {
    const safeDuration = Math.max(0, durationMs);
    this.latencyCount += 1;
    this.latencyTotalMs += safeDuration;
    this.latencyMinMs =
      this.latencyCount === 1
        ? safeDuration
        : Math.min(this.latencyMinMs, safeDuration);
    this.latencyMaxMs = Math.max(this.latencyMaxMs, safeDuration);
  }

  getSnapshot(): MetricsSnapshot {
    return {
      transactionsByStatus: { ...this.transactionsByStatus },
      duplicatesDetected: this.duplicatesDetected,
      retries: this.retries,
      dlqMessages: this.dlqMessages,
      lockConflicts: this.lockConflicts,
      outboxLagMs: this.outboxLagMs,
      reconciliationDivergences: this.reconciliationDivergences,
      processingLatencyMs: {
        count: this.latencyCount,
        totalMs: this.latencyTotalMs,
        minMs: this.latencyMinMs,
        maxMs: this.latencyMaxMs,
        avgMs:
          this.latencyCount > 0
            ? Math.round((this.latencyTotalMs / this.latencyCount) * 100) / 100
            : 0,
      },
    };
  }
}
```

Create `src/common/interceptors/request.interceptor.ts`:
```typescript
import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';
import { Request, Response } from 'express';

export interface CorrelatedRequest extends Request {
  correlationId?: string;
}

@Injectable()
export class RequestInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    if (context.getType() === 'http') {
      const req = context.switchToHttp().getRequest<CorrelatedRequest>();
      const res = context.switchToHttp().getResponse<Response>();
      const headerId = req.headers['x-correlation-id'];
      const correlationId =
        typeof headerId === 'string' && headerId.trim().length > 0
          ? headerId.trim()
          : uuidv4();
      req.correlationId = correlationId;
      res.setHeader('x-correlation-id', correlationId);
    }
    return next.handle();
  }
}
```

Create `src/common/guards/provider-auth.guard.ts`:
```typescript
import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';

export interface ProviderIdentity {
  providerId?: string;
  authenticated: boolean;
}

export interface ProviderIdentityPort {
  verifyRequest(headers: Record<string, string | string[] | undefined>): Promise<ProviderIdentity>;
}

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  async canActivate(_context: ExecutionContext): Promise<boolean> {
    return true;
  }
}
```

- [ ] **Step 5: Run the unit test to verify it passes**

Run: `bun run test:unit src/common/metrics/metrics.service.spec.ts`
Expected: PASS

---

### Task 2: Core Domain Value Objects (`Money`), Enums, Errors & Canonical Hash / Cursor Functions

**Files:**
- Create: `src/common/enums/failure-code.enum.ts`
- Create: `src/common/enums/ledger-direction.enum.ts`
- Create: `src/common/enums/wager-transaction-kind.enum.ts`
- Create: `src/common/enums/wager-transaction-status.enum.ts`
- Create: `src/common/domain/domain.error.ts`
- Create: `src/common/domain/money.domain.ts`
- Create: `src/common/functions/canonical-hash.function.ts`
- Create: `src/common/functions/cursor.function.ts`
- Test: `src/common/domain/money.domain.spec.ts`
- Test: `src/common/functions/canonical-hash.function.spec.ts`

**Interfaces:**
- Consumes: `decimal.js`, Node `crypto`
- Produces: `Money`, `MoneyProps`, `FailureCode`, `LedgerDirection`, `WagerTransactionKind`, `WagerTransactionStatus`, `DomainError`, `CurrencyMismatchError`, `InvalidMoneyError`, `InvalidTransactionStateError`, `IdempotencyConflictError`, `InsufficientFundsError`, `DuplicateWalletError`, `computeCanonicalPayloadHash`, `encodeLedgerCursor`, `decodeLedgerCursor`

- [ ] **Step 1: Write the failing unit tests for `Money` and canonical payload hashing**

Create `src/common/domain/money.domain.spec.ts`:
```typescript
import { Money } from './money.domain';
import { CurrencyMismatchError, InvalidMoneyError } from './domain.error';

describe('Money Domain Value Object', () => {
  it('creates Money from valid decimal string with 2 decimal places', () => {
    const m = Money.from({ amount: '25.00', currency: 'BRL' });
    expect(m.toString()).toBe('25.00');
    expect(m.currency).toBe('BRL');
    expect(m.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  it('creates zero Money', () => {
    const zero = Money.zero('BRL');
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
    expect(zero.isNegative()).toBe(false);
    expect(zero.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
  });

  it('performs immutable add, subtract, negate, and comparisons', () => {
    const a = Money.from({ amount: '100.00', currency: 'BRL' });
    const b = Money.from({ amount: '80.00', currency: 'BRL' });

    const sum = a.add(b);
    const diff = a.subtract(b);
    const neg = b.negate();

    expect(a.toString()).toBe('100.00');
    expect(sum.toString()).toBe('180.00');
    expect(diff.toString()).toBe('20.00');
    expect(neg.isNegative()).toBe(true);
    expect(b.isLessThan(a)).toBe(true);
    expect(a.equals(Money.from({ amount: '100.00', currency: 'BRL' }))).toBe(true);
  });

  it('throws CurrencyMismatchError when operating on different currencies', () => {
    const brl = Money.from({ amount: '10.00', currency: 'BRL' });
    const usd = Money.from({ amount: '10.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
  });

  it.each([
    ['', 'BRL'],
    ['NaN', 'BRL'],
    ['Infinity', 'BRL'],
    ['-Infinity', 'BRL'],
    ['1e2', 'BRL'],
    ['1E-2', 'BRL'],
    ['25.123', 'BRL'],
    ['25.1', 'BRL'],
    ['25', 'BRL'],
    ['-10.00', 'BRL'],
    ['abc', 'BRL'],
    ['10.00', ''],
    ['10.00', 'br'],
    ['10.00', 'BRLX'],
  ])('rejects invalid input amount=%s currency=%s', (amount, currency) => {
    expect(() => Money.from({ amount, currency })).toThrow(InvalidMoneyError);
  });
});
```

Create `src/common/functions/canonical-hash.function.spec.ts`:
```typescript
import { computeCanonicalPayloadHash } from './canonical-hash.function';
import { encodeLedgerCursor, decodeLedgerCursor } from './cursor.function';

describe('Canonical Payload Hash & Ledger Cursor', () => {
  it('produces identical SHA-256 hash regardless of object key order', () => {
    const payloadA = {
      providerId: 'provider-a',
      externalTransactionId: 'tx-1',
      playerId: 'p-1',
      walletId: 'w-1',
      roundId: 'r-1',
      gameId: 'g-1',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const payloadB = {
      kind: 'BET',
      money: { currency: 'BRL', amount: '25.00' },
      gameId: 'g-1',
      roundId: 'r-1',
      walletId: 'w-1',
      playerId: 'p-1',
      externalTransactionId: 'tx-1',
      providerId: 'provider-a',
    };

    expect(computeCanonicalPayloadHash(payloadA)).toBe(
      computeCanonicalPayloadHash(payloadB),
    );
  });

  it('produces different hash when any business field changes', () => {
    const base = {
      providerId: 'provider-a',
      externalTransactionId: 'tx-1',
      playerId: 'p-1',
      walletId: 'w-1',
      roundId: 'r-1',
      gameId: 'g-1',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const altered = {
      ...base,
      money: { amount: '30.00', currency: 'BRL' },
    };

    expect(computeCanonicalPayloadHash(base)).not.toBe(
      computeCanonicalPayloadHash(altered),
    );
  });

  it('encodes and decodes opaque ledger cursor symmetrically', () => {
    const date = new Date('2026-10-22T12:00:00.000Z');
    const id = '0192f291-27dd-7d3f-8071-5f8685deef37';
    const cursor = encodeLedgerCursor({ createdAt: date, id });
    const decoded = decodeLedgerCursor(cursor);

    expect(decoded.createdAt.toISOString()).toBe(date.toISOString());
    expect(decoded.id).toBe(id);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bun run test:unit src/common/domain/money.domain.spec.ts src/common/functions/canonical-hash.function.spec.ts`
Expected: FAIL with `Cannot find module './money.domain'`

- [ ] **Step 3: Implement enums, domain errors, `Money`, `computeCanonicalPayloadHash`, and cursor helpers**

Create `src/common/enums/failure-code.enum.ts`:
```typescript
export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalInsufficientFunds = 'REVERSAL_INSUFFICIENT_FUNDS',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  DuplicateReversal = 'DUPLICATE_REVERSAL',
  AmountMismatch = 'AMOUNT_MISMATCH',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  WalletPlayerMismatch = 'WALLET_PLAYER_MISMATCH',
  InfrastructureFailure = 'INFRASTRUCTURE_FAILURE',
}
```

Create `src/common/enums/ledger-direction.enum.ts`:
```typescript
export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}
```

Create `src/common/enums/wager-transaction-kind.enum.ts`:
```typescript
export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}
```

Create `src/common/enums/wager-transaction-status.enum.ts`:
```typescript
export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}
```

Create `src/common/domain/domain.error.ts`:
```typescript
import { FailureCode } from '../enums/failure-code.enum';

export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string,
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(message, 'INVALID_MONEY');
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, actual: string) {
    super(
      `Currency mismatch: expected ${expected}, got ${actual}`,
      FailureCode.CurrencyMismatch,
    );
  }
}

export class InsufficientFundsError extends DomainError {
  constructor(
    message = 'Insufficient wallet balance',
    code: FailureCode = FailureCode.InsufficientFunds,
  ) {
    super(message, code);
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(currentStatus: string, targetAction: string) {
    super(
      `Cannot transition transaction from ${currentStatus} via ${targetAction}`,
      'INVALID_TRANSACTION_STATE',
    );
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(idempotencyKey: string) {
    super(
      `Idempotency key '${idempotencyKey}' already used with a different payload`,
      'IDEMPOTENCY_CONFLICT',
    );
  }
}

export class DuplicateWalletError extends DomainError {
  constructor(playerId: string, currency: string) {
    super(
      `Wallet already exists for player ${playerId} and currency ${currency}`,
      'DUPLICATE_WALLET',
    );
  }
}

export class EntityNotFoundError extends DomainError {
  constructor(entity: string, identifier: string) {
    super(`${entity} '${identifier}' not found`, 'ENTITY_NOT_FOUND');
  }
}
```

Create `src/common/domain/money.domain.ts`:
```typescript
import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './domain.error';

export interface MoneyProps {
  amount: string;
  currency: string;
}

const DECIMAL_AMOUNT_REGEX = /^\d+\.\d{2}$/;
const SIGNED_DECIMAL_AMOUNT_REGEX = /^-?\d+\.\d{2}$/;
const CURRENCY_REGEX = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || typeof props.currency !== 'string') {
      throw new InvalidMoneyError('Money requires string amount and currency');
    }
    if (!CURRENCY_REGEX.test(props.currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency: ${props.currency}`);
    }
    if (!DECIMAL_AMOUNT_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(
        `Invalid money amount '${props.amount}': must be non-negative fixed 2-decimal string`,
      );
    }
    const decimalValue = new Decimal(props.amount);
    if (!decimalValue.isFinite() || decimalValue.isNegative()) {
      throw new InvalidMoneyError(`Invalid decimal value: ${props.amount}`);
    }
    return new Money(decimalValue, props.currency);
  }

  static rehydrate(props: MoneyProps): Money {
    if (!CURRENCY_REGEX.test(props.currency) || !SIGNED_DECIMAL_AMOUNT_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(`Cannot rehydrate invalid money: ${JSON.stringify(props)}`);
    }
    return new Money(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    if (!CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency: ${currency}`);
    }
    return new Money(new Decimal('0.00'), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  toString(): string {
    return this.value.toFixed(2);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
```

Create `src/common/functions/canonical-hash.function.ts`:
```typescript
import { createHash } from 'crypto';
import { MoneyProps } from '../domain/money.domain';

export interface CanonicalWagerPayloadInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export function computeCanonicalPayloadHash(
  input: CanonicalWagerPayloadInput,
): string {
  const canonicalObject = {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    referenceExternalTransactionId:
      input.referenceExternalTransactionId ?? null,
    roundId: input.roundId,
    walletId: input.walletId,
  };
  return createHash('sha256')
    .update(JSON.stringify(canonicalObject))
    .digest('hex');
}
```

Create `src/common/functions/cursor.function.ts`:
```typescript
import { DomainError } from '../domain/domain.error';

export interface LedgerCursorPayload {
  createdAt: Date;
  id: string;
}

export function encodeLedgerCursor(payload: LedgerCursorPayload): string {
  const raw = JSON.stringify({
    c: payload.createdAt.toISOString(),
    i: payload.id,
  });
  return Buffer.from(raw, 'utf8').toString('base64url');
}

export function decodeLedgerCursor(cursor: string): LedgerCursorPayload {
  try {
    const json = Buffer.from(cursor, 'base64url').toString('utf8');
    const parsed = JSON.parse(json) as { c?: string; i?: string };
    if (!parsed.c || !parsed.i) {
      throw new Error('Missing cursor fields');
    }
    const createdAt = new Date(parsed.c);
    if (Number.isNaN(createdAt.getTime())) {
      throw new Error('Invalid cursor date');
    }
    return { createdAt, id: parsed.i };
  } catch {
    throw new DomainError('Invalid pagination cursor', 'INVALID_CURSOR');
  }
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `bun run test:unit src/common/domain/money.domain.spec.ts src/common/functions/canonical-hash.function.spec.ts`
Expected: PASS


---

### Task 3: Domain Aggregates (`Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `InboxMessage`, `OutboxMessage`) & `IntegrationEvent` Hierarchy

**Files:**
- Create: `src/app/wallet/domain/wallet-ledger-entry.domain.ts`
- Create: `src/app/wallet/domain/wallet.domain.ts`
- Create: `src/app/wagering/domain/wager-transaction.domain.ts`
- Create: `src/app/messaging/domain/events/integration-event.domain.ts`
- Create: `src/app/messaging/domain/events/wallet-balance-changed.event.ts`
- Create: `src/app/messaging/domain/events/wager-transaction-processed.event.ts`
- Create: `src/app/messaging/domain/events/wager-transaction-rejected.event.ts`
- Create: `src/app/messaging/domain/events/wager-transaction-pending-reference.event.ts`
- Create: `src/app/messaging/domain/inbox-message.domain.ts`
- Create: `src/app/messaging/domain/outbox-message.domain.ts`
- Test: `src/app/wallet/domain/wallet.domain.spec.ts`
- Test: `src/app/wallet/domain/wallet-ledger-entry.domain.spec.ts`
- Test: `src/app/wagering/domain/wager-transaction.domain.spec.ts`
- Test: `src/app/messaging/domain/inbox-outbox.domain.spec.ts`

**Interfaces:**
- Consumes: `Money`, `MoneyProps`, `FailureCode`, `LedgerDirection`, `WagerTransactionKind`, `WagerTransactionStatus`, `DomainError`
- Produces: `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `InboxMessage`, `OutboxMessage`, `IntegrationEvent`, `WalletBalanceChanged`, `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`

- [ ] **Step 1: Write the failing unit tests for `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `InboxMessage`, `OutboxMessage`, and `IntegrationEvent`**

Create `src/app/wallet/domain/wallet-ledger-entry.domain.spec.ts`:
```typescript
import { WalletLedgerEntry } from './wallet-ledger-entry.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { Money } from '../../../common/domain/money.domain';
import { DomainError } from '../../../common/domain/domain.error';

describe('WalletLedgerEntry Domain', () => {
  it('creates balanced CREDIT and DEBIT entries', () => {
    const credit = WalletLedgerEntry.create({
      id: 'entry-1',
      walletId: 'wallet-1',
      transactionId: 'tx-1',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '50.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '150.00', currency: 'BRL' }),
      createdAt: new Date(),
    });
    expect(credit.isBalanced()).toBe(true);

    const debit = WalletLedgerEntry.create({
      id: 'entry-2',
      walletId: 'wallet-1',
      transactionId: 'tx-2',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '30.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '150.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '120.00', currency: 'BRL' }),
      createdAt: new Date(),
    });
    expect(debit.isBalanced()).toBe(true);
  });

  it('rejects unbalanced ledger entry or zero money', () => {
    expect(() =>
      WalletLedgerEntry.create({
        id: 'entry-bad',
        walletId: 'wallet-1',
        transactionId: 'tx-1',
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '30.00', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '80.00', currency: 'BRL' }),
      }),
    ).toThrow(DomainError);
  });
});
```

Create `src/app/wallet/domain/wallet.domain.spec.ts`:
```typescript
import { Wallet } from './wallet.domain';
import { Money } from '../../../common/domain/money.domain';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
} from '../../../common/domain/domain.error';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';

describe('Wallet Aggregate Root', () => {
  it('opens wallet at version 1 and increments version on credit/debit while producing balanced ledger entries', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    expect(wallet.version).toBe(1);
    expect(wallet.balance.toString()).toBe('100.00');

    const debitEntry = wallet.debit({
      entryId: 'le-1',
      transactionId: 'tx-1',
      amount: Money.from({ amount: '40.00', currency: 'BRL' }),
    });

    expect(wallet.balance.toString()).toBe('60.00');
    expect(wallet.version).toBe(2);
    expect(debitEntry.direction).toBe(LedgerDirection.Debit);
    expect(debitEntry.balanceBefore.toString()).toBe('100.00');
    expect(debitEntry.balanceAfter.toString()).toBe('60.00');

    const creditEntry = wallet.credit({
      entryId: 'le-2',
      transactionId: 'tx-2',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    expect(wallet.balance.toString()).toBe('85.00');
    expect(wallet.version).toBe(3);
    expect(creditEntry.direction).toBe(LedgerDirection.Credit);
    expect(creditEntry.balanceBefore.toString()).toBe('60.00');
    expect(creditEntry.balanceAfter.toString()).toBe('85.00');
  });

  it('rejects debit when balance is insufficient and leaves balance/version unchanged', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    expect(() =>
      wallet.debit({
        entryId: 'le-1',
        transactionId: 'tx-1',
        amount: Money.from({ amount: '80.00', currency: 'BRL' }),
      }),
    ).toThrow(InsufficientFundsError);

    expect(wallet.balance.toString()).toBe('50.00');
    expect(wallet.version).toBe(1);
  });

  it('rejects operations with mismatched currency', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    expect(() =>
      wallet.credit({
        entryId: 'le-1',
        transactionId: 'tx-1',
        amount: Money.from({ amount: '10.00', currency: 'USD' }),
      }),
    ).toThrow(CurrencyMismatchError);
  });
});
```

Create `src/app/wagering/domain/wager-transaction.domain.spec.ts`:
```typescript
import { WagerTransaction } from './wager-transaction.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { Money } from '../../../common/domain/money.domain';
import {
  DomainError,
  InvalidTransactionStateError,
} from '../../../common/domain/domain.error';

describe('WagerTransaction Domain', () => {
  const baseProps = {
    id: 'tx-1',
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    idempotencyKey: 'provider-a:ext-1',
    payloadHash: 'hash-1',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: Money.from({ amount: '25.00', currency: 'BRL' }),
  };

  it('starts in PENDING and transitions to PROCESSED, blocking further transitions', () => {
    const tx = WagerTransaction.create(baseProps);
    expect(tx.status).toBe(WagerTransactionStatus.Pending);
    expect(tx.isTerminal()).toBe(false);
    expect(tx.affectsBalance()).toBe(true);
    expect(tx.requiresReference()).toBe(false);
    expect(tx.ledgerDirectionFor()).toBe(LedgerDirection.Debit);

    const now = new Date();
    tx.markProcessed(undefined, now, Money.from({ amount: '75.00', currency: 'BRL' }));
    expect(tx.status).toBe(WagerTransactionStatus.Processed);
    expect(tx.isTerminal()).toBe(true);
    expect(tx.observedBalance?.toString()).toBe('75.00');

    expect(() => tx.reject(FailureCode.InsufficientFunds)).toThrow(
      InvalidTransactionStateError,
    );
  });

  it('enforces referenceExternalTransactionId for REFUND and ROLLBACK', () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Refund,
      }),
    ).toThrow(DomainError);

    const refund = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'ext-bet-1',
    });
    expect(refund.requiresReference()).toBe(true);
  });

  it('computes inverse ledger direction for ROLLBACK based on referenced transaction', () => {
    const betTx = WagerTransaction.create(baseProps);
    const winTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-win',
      kind: WagerTransactionKind.Win,
    });
    const rollbackTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-rb',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-1',
    });

    expect(rollbackTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    expect(rollbackTx.ledgerDirectionFor(winTx)).toBe(LedgerDirection.Debit);
  });

  it('marks LOSS as not affecting balance', () => {
    const lossTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Loss,
      money: Money.zero('BRL'),
    });
    expect(lossTx.affectsBalance()).toBe(false);
  });
});
```

Create `src/app/messaging/domain/inbox-outbox.domain.spec.ts`:
```typescript
import { InboxMessage } from './inbox-message.domain';
import { OutboxMessage } from './outbox-message.domain';
import { WalletBalanceChanged } from './events/wallet-balance-changed.event';
import { Wallet } from '../../wallet/domain/wallet.domain';
import { Money } from '../../../common/domain/money.domain';

describe('InboxMessage, OutboxMessage & IntegrationEvent', () => {
  it('tracks InboxMessage processing state', () => {
    const inbox = InboxMessage.receive({
      messageId: 'msg-1',
      consumerName: 'wager-consumer',
      payloadHash: 'hash-1',
    });
    expect(inbox.isProcessed()).toBe(false);

    const now = new Date();
    inbox.markProcessed(now);
    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.processedAt).toEqual(now);
  });

  it('enqueues IntegrationEvent into OutboxMessage and schedules exponential backoff retries', () => {
    const wallet = Wallet.open({
      id: 'w-1',
      playerId: 'p-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });
    const entry = wallet.debit({
      entryId: 'le-1',
      transactionId: 'tx-1',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    const event = WalletBalanceChanged.from(wallet, entry, {
      eventId: 'ev-1',
      correlationId: 'corr-1',
      occurredAt: new Date('2026-10-22T12:00:00.000Z'),
    });

    const outbox = OutboxMessage.enqueue(event);
    expect(outbox.isPending()).toBe(true);
    expect(outbox.eventType).toBe('WalletBalanceChanged');
    expect(outbox.payload.version).toBe(1);

    const retryBase = new Date('2026-10-22T12:00:00.000Z');
    outbox.scheduleRetry(retryBase);
    expect(outbox.attempts).toBe(1);
    expect(outbox.nextAttemptAt?.getTime()).toBeGreaterThan(retryBase.getTime());
    expect(outbox.isDue(retryBase)).toBe(false);

    const publishAt = new Date('2026-10-22T12:01:00.000Z');
    outbox.markPublished(publishAt);
    expect(outbox.isPending()).toBe(false);
    expect(outbox.publishedAt).toEqual(publishAt);
  });
});
```

- [ ] **Step 2: Run the unit tests to verify they fail**

Run: `bun run test:unit src/app/wallet/domain/ src/app/wagering/domain/ src/app/messaging/domain/`
Expected: FAIL with module not found errors

- [ ] **Step 3: Implement `WalletLedgerEntry`, `Wallet`, `WagerTransaction`, `IntegrationEvent` subclasses, `InboxMessage`, and `OutboxMessage`**

Create `src/app/wallet/domain/wallet-ledger-entry.domain.ts`:
```typescript
import { DomainError } from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.money.isPositive()) {
      throw new DomainError(
        'Ledger entry money must be strictly positive',
        'INVALID_LEDGER_AMOUNT',
      );
    }
    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ?? new Date(),
    );
    if (!entry.isBalanced()) {
      throw new DomainError(
        'Unbalanced ledger entry arithmetic',
        'UNBALANCED_LEDGER_ENTRY',
      );
    }
    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  isBalanced(): boolean {
    if (this.balanceBefore.isNegative() || this.balanceAfter.isNegative()) {
      return false;
    }
    const expectedAfter =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expectedAfter.equals(this.balanceAfter);
  }
}
```

Create `src/app/wallet/domain/wallet.domain.ts`:
```typescript
import {
  CurrencyMismatchError,
  InsufficientFundsError,
} from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WalletLedgerEntry } from './wallet-ledger-entry.domain';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementProps {
  entryId: string;
  transactionId: string;
  amount: Money;
  at?: Date;
  insufficientFundsCode?: FailureCode;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
    createdAt?: Date;
  }): Wallet {
    const now = props.createdAt ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.amount);
    if (this._balance.isLessThan(props.amount)) {
      throw new InsufficientFundsError(
        'Insufficient balance for debit',
        props.insufficientFundsCode ?? FailureCode.InsufficientFunds,
      );
    }
    const before = this._balance;
    const after = before.subtract(props.amount);
    const now = props.at ?? new Date();
    const entry = WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Debit,
      money: props.amount,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: now,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = now;
    return entry;
  }

  credit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.amount);
    const before = this._balance;
    const after = before.add(props.amount);
    const now = props.at ?? new Date();
    const entry = WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Credit,
      money: props.amount,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: now,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = now;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
```

Create `src/app/wagering/domain/wager-transaction.domain.ts`:
```typescript
import {
  DomainError,
  InvalidTransactionStateError,
} from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt?: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  observedBalance?: Money;
  retryAttempts?: number;
  nextRetryAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _observedBalance?: Money,
    private _retryAttempts = 0,
    private _nextRetryAt?: Date,
  ) {}

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    const needsRef =
      props.kind === WagerTransactionKind.Refund ||
      props.kind === WagerTransactionKind.Rollback;
    if (needsRef && !props.referenceExternalTransactionId) {
      throw new DomainError(
        `${props.kind} requires referenceExternalTransactionId`,
        'MISSING_REFERENCE_EXTERNAL_ID',
      );
    }
    if (
      props.kind !== WagerTransactionKind.Loss &&
      !props.money.isPositive()
    ) {
      throw new DomainError(
        `${props.kind} requires strictly positive money amount`,
        'INVALID_TRANSACTION_AMOUNT',
      );
    }
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt ?? new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.observedBalance,
      state.retryAttempts ?? 0,
      state.nextRetryAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }

  get retryAttempts(): number {
    return this._retryAttempts;
  }

  get nextRetryAt(): Date | undefined {
    return this._nextRetryAt;
  }

  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
    observedBalance?: Money,
  ): void {
    this.assertNotTerminal('markProcessed');
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._observedBalance = observedBalance;
    this._nextRetryAt = undefined;
  }

  markPendingReference(now: Date = new Date()): void {
    this.assertNotTerminal('markPendingReference');
    this._status = WagerTransactionStatus.PendingReference;
    this._retryAttempts += 1;
    const delayMs = Math.min(30000, 500 * Math.pow(2, this._retryAttempts - 1));
    this._nextRetryAt = new Date(now.getTime() + delayMs);
  }

  reject(code: FailureCode, observedBalance?: Money, at: Date = new Date()): void {
    this.assertNotTerminal('reject');
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._processedAt = at;
    this._observedBalance = observedBalance;
    this._nextRetryAt = undefined;
  }

  fail(code: FailureCode, at: Date = new Date()): void {
    this.assertNotTerminal('fail');
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    this._nextRetryAt = undefined;
  }

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new DomainError(
            'ROLLBACK requires reference transaction to determine ledger direction',
            FailureCode.InvalidReferenceKind,
          );
        }
        const refDirection = reference.ledgerDirectionFor();
        return refDirection === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new DomainError(
          'LOSS does not produce ledger entries',
          'LOSS_NO_LEDGER',
        );
    }
  }

  private assertNotTerminal(action: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this._status, action);
    }
  }
}
```

Create `src/app/messaging/domain/events/integration-event.domain.ts`:
```typescript
export interface EventContext {
  eventId: string;
  correlationId: string;
  causationId?: string;
  occurredAt?: Date;
}

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export interface SerializedIntegrationEvent<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze({ ...props.data });
  }

  toJSON(): SerializedIntegrationEvent<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}
```

Create `src/app/messaging/domain/events/wallet-balance-changed.event.ts`:
```typescript
import { MoneyProps } from '../../../../common/domain/money.domain';
import { LedgerDirection } from '../../../../common/enums/ledger-direction.enum';
import { WalletLedgerEntry } from '../../../wallet/domain/wallet-ledger-entry.domain';
import { Wallet } from '../../../wallet/domain/wallet.domain';
import { EventContext, IntegrationEvent } from './integration-event.domain';

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: ctx.eventId,
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
```

Create `src/app/messaging/domain/events/wager-transaction-processed.event.ts`:
```typescript
import { MoneyProps } from '../../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../../common/enums/wager-transaction-kind.enum';
import { WagerTransaction } from '../../../wagering/domain/wager-transaction.domain';
import { EventContext, IntegrationEvent } from './integration-event.domain';

export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  balance: MoneyProps;
  referenceTransactionId?: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(
    tx: WagerTransaction,
    balance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    return new WagerTransactionProcessed({
      eventId: ctx.eventId,
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        balance,
        referenceTransactionId: tx.referenceTransactionId,
      },
    });
  }
}
```

Create `src/app/messaging/domain/events/wager-transaction-rejected.event.ts`:
```typescript
import { MoneyProps } from '../../../../common/domain/money.domain';
import { FailureCode } from '../../../../common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../../../common/enums/wager-transaction-kind.enum';
import { WagerTransaction } from '../../../wagering/domain/wager-transaction.domain';
import { EventContext, IntegrationEvent } from './integration-event.domain';

export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  failureCode: FailureCode;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(
    tx: WagerTransaction,
    failureCode: FailureCode,
    ctx: EventContext,
  ): WagerTransactionRejected {
    return new WagerTransactionRejected({
      eventId: ctx.eventId,
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        failureCode,
      },
    });
  }
}
```

Create `src/app/messaging/domain/events/wager-transaction-pending-reference.event.ts`:
```typescript
import { MoneyProps } from '../../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../../common/enums/wager-transaction-kind.enum';
import { WagerTransaction } from '../../../wagering/domain/wager-transaction.domain';
import { EventContext, IntegrationEvent } from './integration-event.domain';

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  referenceExternalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(
    tx: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({
      eventId: ctx.eventId,
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        referenceExternalTransactionId:
          tx.referenceExternalTransactionId ?? '',
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        kind: tx.kind,
        money: tx.money.toJSON(),
      },
    });
  }
}
```

Create `src/app/messaging/domain/inbox-message.domain.ts`:
```typescript
export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      props.receivedAt ?? new Date(),
    );
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined && this._processedAt !== null;
  }

  markProcessed(at: Date): void {
    this._processedAt = at;
  }
}
```

Create `src/app/messaging/domain/outbox-message.domain.ts`:
```typescript
import { IntegrationEvent } from './events/integration-event.domain';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const serialized = event.toJSON();
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze({ ...serialized }),
      event.occurredAt,
      0,
      event.occurredAt,
      undefined,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze({ ...state.payload }),
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined || this._publishedAt === null;
  }

  isDue(now: Date): boolean {
    if (!this.isPending()) {
      return false;
    }
    if (!this._nextAttemptAt) {
      return true;
    }
    return this._nextAttemptAt.getTime() <= now.getTime();
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
  }

  scheduleRetry(now: Date): void {
    this._attempts += 1;
    const backoffMs = Math.min(60000, 500 * Math.pow(2, this._attempts - 1));
    this._nextAttemptAt = new Date(now.getTime() + backoffMs);
  }
}
```

- [ ] **Step 4: Run the domain unit tests to verify they pass**

Run: `bun run test:unit src/app/wallet/domain/ src/app/wagering/domain/ src/app/messaging/domain/`
Expected: PASS


---

### Task 4: TypeORM Entities, DataSource & Versioned Reversible Migration (Schema Constraints & Immutable Ledger Trigger)

**Files:**
- Create: `src/app/wallet/entities/wallet.entity.ts`
- Create: `src/app/wallet/entities/wallet-ledger-entry.entity.ts`
- Create: `src/app/wagering/entities/wager-transaction.entity.ts`
- Create: `src/app/messaging/entities/inbox-message.entity.ts`
- Create: `src/app/messaging/entities/outbox-message.entity.ts`
- Create: `src/common/database/data-source.ts`
- Create: `src/common/database/migrations/1761130000000-InitialWageringSchema.ts`
- Test: `src/common/database/entities-mapping.spec.ts`

**Interfaces:**
- Consumes: `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `InboxMessage`, `OutboxMessage`, `Money`, `EnvProvider`
- Produces: `WalletEntity`, `WalletLedgerEntryEntity`, `WagerTransactionEntity`, `InboxMessageEntity`, `OutboxMessageEntity`, `AppDataSource`, `InitialWageringSchema1761130000000`

- [ ] **Step 1: Write the failing unit test for Entity <-> Domain `rehydrate` and persistence mapping**

Create `src/common/database/entities-mapping.spec.ts`:
```typescript
import { WalletEntity } from '../../app/wallet/entities/wallet.entity';
import { WalletLedgerEntryEntity } from '../../app/wallet/entities/wallet-ledger-entry.entity';
import { WagerTransactionEntity } from '../../app/wagering/entities/wager-transaction.entity';
import { Wallet } from '../../app/wallet/domain/wallet.domain';
import { Money } from '../domain/money.domain';
import { WagerTransaction } from '../../app/wagering/domain/wager-transaction.domain';
import { WagerTransactionKind } from '../enums/wager-transaction-kind.enum';

describe('Persistence Entities <-> Domain Mapping', () => {
  it('converts Wallet and WalletLedgerEntry between domain and entity preserving exact 2-decimal strings', () => {
    const wallet = Wallet.open({
      id: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const entry = wallet.debit({
      entryId: '0192f291-27dd-7d3f-8071-5f8685deef38',
      transactionId: '0192f291-27dd-7d3f-8071-5f8685deef39',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    const walletEntity = WalletEntity.fromDomain(wallet);
    const rehydratedWallet = walletEntity.toDomain();
    expect(rehydratedWallet.balance.toJSON()).toEqual({
      amount: '75.00',
      currency: 'BRL',
    });
    expect(rehydratedWallet.version).toBe(2);

    const entryEntity = WalletLedgerEntryEntity.fromDomain(entry);
    const rehydratedEntry = entryEntity.toDomain();
    expect(rehydratedEntry.isBalanced()).toBe(true);
    expect(rehydratedEntry.balanceAfter.toString()).toBe('75.00');
  });

  it('converts WagerTransaction between domain and entity preserving observedBalance', () => {
    const tx = WagerTransaction.create({
      id: '0192f298-345e-7e38-af88-e43f851a819d',
      providerId: 'provider-a',
      externalTransactionId: 'ext-1',
      idempotencyKey: 'provider-a:ext-1',
      payloadHash: 'hash-1',
      walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    });
    tx.markProcessed(
      undefined,
      new Date('2026-10-22T12:00:00.000Z'),
      Money.from({ amount: '975.00', currency: 'BRL' }),
    );

    const entity = WagerTransactionEntity.fromDomain(tx);
    const rehydrated = entity.toDomain();

    expect(rehydrated.status).toBe(tx.status);
    expect(rehydrated.observedBalance?.toJSON()).toEqual({
      amount: '975.00',
      currency: 'BRL',
    });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/common/database/entities-mapping.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement TypeORM entities, `AppDataSource`, and `1761130000000-InitialWageringSchema.ts`**

Create `src/app/wallet/entities/wallet.entity.ts`:
```typescript
import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { Wallet } from '../domain/wallet.domain';

@Entity({ name: 'wallets' })
export class WalletEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'player_id', type: 'varchar', length: 128 })
  playerId: string;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ name: 'balance_amount', type: 'numeric', precision: 20, scale: 2 })
  balanceAmount: string;

  @Column({ type: 'integer', default: 1 })
  version: number;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  static fromDomain(wallet: Wallet): WalletEntity {
    const entity = new WalletEntity();
    entity.id = wallet.id;
    entity.playerId = wallet.playerId;
    entity.currency = wallet.currency;
    entity.balanceAmount = wallet.balance.toString();
    entity.version = wallet.version;
    entity.createdAt = wallet.createdAt;
    entity.updatedAt = wallet.updatedAt;
    return entity;
  }

  toDomain(): Wallet {
    return Wallet.rehydrate({
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balance: Money.rehydrate({
        amount: Number.isNaN(Number(this.balanceAmount))
          ? this.balanceAmount
          : Number(this.balanceAmount).toFixed(2),
        currency: this.currency,
      }),
      version: Number(this.version),
      createdAt: new Date(this.createdAt),
      updatedAt: new Date(this.updatedAt),
    });
  }
}
```

Create `src/app/wallet/entities/wallet-ledger-entry.entity.ts`:
```typescript
import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';

@Entity({ name: 'wallet_ledger_entries' })
export class WalletLedgerEntryEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'wallet_id', type: 'uuid' })
  walletId: string;

  @Column({ name: 'transaction_id', type: 'uuid' })
  transactionId: string;

  @Column({ type: 'varchar', length: 16 })
  direction: LedgerDirection;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ type: 'numeric', precision: 20, scale: 2 })
  amount: string;

  @Column({ name: 'balance_before', type: 'numeric', precision: 20, scale: 2 })
  balanceBefore: string;

  @Column({ name: 'balance_after', type: 'numeric', precision: 20, scale: 2 })
  balanceAfter: string;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  static fromDomain(entry: WalletLedgerEntry): WalletLedgerEntryEntity {
    const entity = new WalletLedgerEntryEntity();
    entity.id = entry.id;
    entity.walletId = entry.walletId;
    entity.transactionId = entry.transactionId;
    entity.direction = entry.direction;
    entity.currency = entry.money.currency;
    entity.amount = entry.money.toString();
    entity.balanceBefore = entry.balanceBefore.toString();
    entity.balanceAfter = entry.balanceAfter.toString();
    entity.createdAt = entry.createdAt;
    return entity;
  }

  toDomain(): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: this.id,
      walletId: this.walletId,
      transactionId: this.transactionId,
      direction: this.direction,
      money: Money.rehydrate({
        amount: Number(this.amount).toFixed(2),
        currency: this.currency,
      }),
      balanceBefore: Money.rehydrate({
        amount: Number(this.balanceBefore).toFixed(2),
        currency: this.currency,
      }),
      balanceAfter: Money.rehydrate({
        amount: Number(this.balanceAfter).toFixed(2),
        currency: this.currency,
      }),
      createdAt: new Date(this.createdAt),
    });
  }
}
```

Create `src/app/wagering/entities/wager-transaction.entity.ts`:
```typescript
import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { WagerTransaction } from '../domain/wager-transaction.domain';

@Entity({ name: 'wager_transactions' })
export class WagerTransactionEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'provider_id', type: 'varchar', length: 128 })
  providerId: string;

  @Column({ name: 'external_transaction_id', type: 'varchar', length: 128 })
  externalTransactionId: string;

  @Column({ name: 'idempotency_key', type: 'varchar', length: 255 })
  idempotencyKey: string;

  @Column({ name: 'payload_hash', type: 'varchar', length: 64 })
  payloadHash: string;

  @Column({ name: 'wallet_id', type: 'uuid' })
  walletId: string;

  @Column({ name: 'player_id', type: 'varchar', length: 128 })
  playerId: string;

  @Column({ name: 'round_id', type: 'varchar', length: 128 })
  roundId: string;

  @Column({ name: 'game_id', type: 'varchar', length: 128 })
  gameId: string;

  @Column({ type: 'varchar', length: 32 })
  kind: WagerTransactionKind;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ type: 'numeric', precision: 20, scale: 2 })
  amount: string;

  @Column({
    name: 'reference_external_transaction_id',
    type: 'varchar',
    length: 128,
    nullable: true,
  })
  referenceExternalTransactionId: string | null;

  @Column({
    name: 'reference_transaction_id',
    type: 'uuid',
    nullable: true,
  })
  referenceTransactionId: string | null;

  @Column({ type: 'varchar', length: 32 })
  status: WagerTransactionStatus;

  @Column({ name: 'failure_code', type: 'varchar', length: 64, nullable: true })
  failureCode: FailureCode | null;

  @Column({
    name: 'observed_balance_amount',
    type: 'numeric',
    precision: 20,
    scale: 2,
    nullable: true,
  })
  observedBalanceAmount: string | null;

  @Column({
    name: 'observed_balance_currency',
    type: 'varchar',
    length: 3,
    nullable: true,
  })
  observedBalanceCurrency: string | null;

  @Column({ name: 'retry_attempts', type: 'integer', default: 0 })
  retryAttempts: number;

  @Column({ name: 'next_retry_at', type: 'timestamptz', nullable: true })
  nextRetryAt: Date | null;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({ name: 'processed_at', type: 'timestamptz', nullable: true })
  processedAt: Date | null;

  static fromDomain(tx: WagerTransaction): WagerTransactionEntity {
    const entity = new WagerTransactionEntity();
    entity.id = tx.id;
    entity.providerId = tx.providerId;
    entity.externalTransactionId = tx.externalTransactionId;
    entity.idempotencyKey = tx.idempotencyKey;
    entity.payloadHash = tx.payloadHash;
    entity.walletId = tx.walletId;
    entity.playerId = tx.playerId;
    entity.roundId = tx.roundId;
    entity.gameId = tx.gameId;
    entity.kind = tx.kind;
    entity.currency = tx.money.currency;
    entity.amount = tx.money.toString();
    entity.referenceExternalTransactionId =
      tx.referenceExternalTransactionId ?? null;
    entity.referenceTransactionId = tx.referenceTransactionId ?? null;
    entity.status = tx.status;
    entity.failureCode = tx.failureCode ?? null;
    entity.observedBalanceAmount = tx.observedBalance
      ? tx.observedBalance.toString()
      : null;
    entity.observedBalanceCurrency = tx.observedBalance
      ? tx.observedBalance.currency
      : null;
    entity.retryAttempts = tx.retryAttempts;
    entity.nextRetryAt = tx.nextRetryAt ?? null;
    entity.createdAt = tx.createdAt;
    entity.processedAt = tx.processedAt ?? null;
    return entity;
  }

  toDomain(): WagerTransaction {
    const observedBalance =
      this.observedBalanceAmount !== null &&
      this.observedBalanceAmount !== undefined &&
      this.observedBalanceCurrency
        ? Money.rehydrate({
            amount: Number(this.observedBalanceAmount).toFixed(2),
            currency: this.observedBalanceCurrency,
          })
        : undefined;

    return WagerTransaction.rehydrate({
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      money: Money.rehydrate({
        amount: Number(this.amount).toFixed(2),
        currency: this.currency,
      }),
      referenceExternalTransactionId:
        this.referenceExternalTransactionId ?? undefined,
      createdAt: new Date(this.createdAt),
      status: this.status,
      referenceTransactionId: this.referenceTransactionId ?? undefined,
      failureCode: this.failureCode ?? undefined,
      processedAt: this.processedAt ? new Date(this.processedAt) : undefined,
      observedBalance,
      retryAttempts: Number(this.retryAttempts ?? 0),
      nextRetryAt: this.nextRetryAt ? new Date(this.nextRetryAt) : undefined,
    });
  }
}
```

Create `src/app/messaging/entities/inbox-message.entity.ts`:
```typescript
import { Column, Entity, PrimaryColumn } from 'typeorm';
import { InboxMessage } from '../domain/inbox-message.domain';

@Entity({ name: 'inbox_messages' })
export class InboxMessageEntity {
  @PrimaryColumn({ name: 'consumer_name', type: 'varchar', length: 128 })
  consumerName: string;

  @PrimaryColumn({ name: 'message_id', type: 'varchar', length: 128 })
  messageId: string;

  @Column({ name: 'payload_hash', type: 'varchar', length: 64 })
  payloadHash: string;

  @Column({ name: 'received_at', type: 'timestamptz' })
  receivedAt: Date;

  @Column({ name: 'processed_at', type: 'timestamptz', nullable: true })
  processedAt: Date | null;

  static fromDomain(inbox: InboxMessage): InboxMessageEntity {
    const entity = new InboxMessageEntity();
    entity.consumerName = inbox.consumerName;
    entity.messageId = inbox.messageId;
    entity.payloadHash = inbox.payloadHash;
    entity.receivedAt = inbox.receivedAt;
    entity.processedAt = inbox.processedAt ?? null;
    return entity;
  }

  toDomain(): InboxMessage {
    return InboxMessage.rehydrate({
      consumerName: this.consumerName,
      messageId: this.messageId,
      payloadHash: this.payloadHash,
      receivedAt: new Date(this.receivedAt),
      processedAt: this.processedAt ? new Date(this.processedAt) : undefined,
    });
  }
}
```

Create `src/app/messaging/entities/outbox-message.entity.ts`:
```typescript
import { Column, Entity, PrimaryColumn } from 'typeorm';
import { OutboxMessage } from '../domain/outbox-message.domain';

@Entity({ name: 'outbox_messages' })
export class OutboxMessageEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'aggregate_id', type: 'varchar', length: 128 })
  aggregateId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 128 })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: Record<string, unknown>;

  @Column({ name: 'occurred_at', type: 'timestamptz' })
  occurredAt: Date;

  @Column({ type: 'integer', default: 0 })
  attempts: number;

  @Column({ name: 'next_attempt_at', type: 'timestamptz', nullable: true })
  nextAttemptAt: Date | null;

  @Column({ name: 'published_at', type: 'timestamptz', nullable: true })
  publishedAt: Date | null;

  static fromDomain(outbox: OutboxMessage): OutboxMessageEntity {
    const entity = new OutboxMessageEntity();
    entity.id = outbox.id;
    entity.aggregateId = outbox.aggregateId;
    entity.eventType = outbox.eventType;
    entity.payload = { ...outbox.payload };
    entity.occurredAt = outbox.occurredAt;
    entity.attempts = outbox.attempts;
    entity.nextAttemptAt = outbox.nextAttemptAt ?? null;
    entity.publishedAt = outbox.publishedAt ?? null;
    return entity;
  }

  toDomain(): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload: this.payload,
      occurredAt: new Date(this.occurredAt),
      attempts: Number(this.attempts),
      nextAttemptAt: this.nextAttemptAt
        ? new Date(this.nextAttemptAt)
        : undefined,
      publishedAt: this.publishedAt ? new Date(this.publishedAt) : undefined,
    });
  }
}
```

Create `src/common/database/data-source.ts`:
```typescript
import { DataSource, DataSourceOptions } from 'typeorm';
import { EnvProvider } from '../providers/env.provider';
import { WalletEntity } from '../../app/wallet/entities/wallet.entity';
import { WalletLedgerEntryEntity } from '../../app/wallet/entities/wallet-ledger-entry.entity';
import { WagerTransactionEntity } from '../../app/wagering/entities/wager-transaction.entity';
import { InboxMessageEntity } from '../../app/messaging/entities/inbox-message.entity';
import { OutboxMessageEntity } from '../../app/messaging/entities/outbox-message.entity';
import { InitialWageringSchema1761130000000 } from './migrations/1761130000000-InitialWageringSchema';

EnvProvider.initialize();

export const dataSourceOptions: DataSourceOptions = {
  type: 'postgres',
  host: EnvProvider.DB_HOST,
  port: EnvProvider.DB_PORT,
  username: EnvProvider.DB_USER,
  password: EnvProvider.DB_PASS,
  database: EnvProvider.DB_NAME,
  entities: [
    WalletEntity,
    WalletLedgerEntryEntity,
    WagerTransactionEntity,
    InboxMessageEntity,
    OutboxMessageEntity,
  ],
  migrations: [InitialWageringSchema1761130000000],
  synchronize: false,
  logging: false,
};

export const AppDataSource = new DataSource(dataSourceOptions);
```

Create `src/common/database/migrations/1761130000000-InitialWageringSchema.ts`:
```typescript
import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialWageringSchema1761130000000 implements MigrationInterface {
  name = 'InitialWageringSchema1761130000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY,
        player_id varchar(128) NOT NULL,
        currency varchar(3) NOT NULL,
        balance_amount numeric(20, 2) NOT NULL,
        version integer NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
        CONSTRAINT chk_wallets_non_negative_balance CHECK (balance_amount >= 0),
        CONSTRAINT chk_wallets_positive_version CHECK (version >= 1)
      );
    `);

    await queryRunner.query(`
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY,
        provider_id varchar(128) NOT NULL,
        external_transaction_id varchar(128) NOT NULL,
        idempotency_key varchar(255) NOT NULL,
        payload_hash varchar(64) NOT NULL,
        wallet_id uuid NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        player_id varchar(128) NOT NULL,
        round_id varchar(128) NOT NULL,
        game_id varchar(128) NOT NULL,
        kind varchar(32) NOT NULL,
        currency varchar(3) NOT NULL,
        amount numeric(20, 2) NOT NULL,
        reference_external_transaction_id varchar(128),
        reference_transaction_id uuid REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        status varchar(32) NOT NULL,
        failure_code varchar(64),
        observed_balance_amount numeric(20, 2),
        observed_balance_currency varchar(3),
        retry_attempts integer NOT NULL DEFAULT 0,
        next_retry_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz,
        CONSTRAINT uq_wager_tx_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT chk_wager_tx_non_negative_amount CHECK (amount >= 0),
        CONSTRAINT chk_wager_tx_positive_unless_loss CHECK (kind = 'LOSS' OR amount > 0),
        CONSTRAINT chk_wager_tx_reference_required CHECK (
          (kind NOT IN ('REFUND', 'ROLLBACK')) OR (reference_external_transaction_id IS NOT NULL)
        )
      );
    `);

    await queryRunner.query(`
      CREATE UNIQUE INDEX uq_wager_tx_processed_reversal
      ON wager_transactions (reference_transaction_id, kind)
      WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
    `);

    await queryRunner.query(`
      CREATE INDEX idx_wager_tx_pending_reference
      ON wager_transactions (status, next_retry_at)
      WHERE status = 'PENDING_REFERENCE';
    `);

    await queryRunner.query(`
      CREATE TABLE wallet_ledger_entries (
        id uuid PRIMARY KEY,
        wallet_id uuid NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        direction varchar(16) NOT NULL,
        currency varchar(3) NOT NULL,
        amount numeric(20, 2) NOT NULL,
        balance_before numeric(20, 2) NOT NULL,
        balance_after numeric(20, 2) NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_ledger_wallet_transaction UNIQUE (wallet_id, transaction_id),
        CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
        CONSTRAINT chk_ledger_positive_amount CHECK (amount > 0),
        CONSTRAINT chk_ledger_non_negative_before CHECK (balance_before >= 0),
        CONSTRAINT chk_ledger_non_negative_after CHECK (balance_after >= 0),
        CONSTRAINT chk_ledger_arithmetic_balance CHECK (
          (direction = 'DEBIT' AND balance_before - amount = balance_after)
          OR
          (direction = 'CREDIT' AND balance_before + amount = balance_after)
        )
      );
    `);

    await queryRunner.query(`
      CREATE INDEX idx_ledger_wallet_cursor
      ON wallet_ledger_entries (wallet_id, created_at ASC, id ASC);
    `);

    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
      RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is immutable: % operation is forbidden', TG_OP;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await queryRunner.query(`
      CREATE TRIGGER trg_immutable_wallet_ledger_entries
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();
    `);

    await queryRunner.query(`
      CREATE TABLE inbox_messages (
        consumer_name varchar(128) NOT NULL,
        message_id varchar(128) NOT NULL,
        payload_hash varchar(64) NOT NULL,
        received_at timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    await queryRunner.query(`
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY,
        aggregate_id varchar(128) NOT NULL,
        event_type varchar(128) NOT NULL,
        payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL DEFAULT now(),
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        published_at timestamptz
      );
    `);

    await queryRunner.query(`
      CREATE INDEX idx_outbox_pending_due
      ON outbox_messages (next_attempt_at ASC, occurred_at ASC)
      WHERE published_at IS NULL;
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS idx_outbox_pending_due;`);
    await queryRunner.query(`DROP TABLE IF EXISTS outbox_messages;`);
    await queryRunner.query(`DROP TABLE IF EXISTS inbox_messages;`);
    await queryRunner.query(
      `DROP TRIGGER IF EXISTS trg_immutable_wallet_ledger_entries ON wallet_ledger_entries;`,
    );
    await queryRunner.query(`DROP FUNCTION IF EXISTS prevent_ledger_mutation();`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_ledger_wallet_cursor;`);
    await queryRunner.query(`DROP TABLE IF EXISTS wallet_ledger_entries;`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_wager_tx_pending_reference;`);
    await queryRunner.query(`DROP INDEX IF EXISTS uq_wager_tx_processed_reversal;`);
    await queryRunner.query(`DROP TABLE IF EXISTS wager_transactions;`);
    await queryRunner.query(`DROP TABLE IF EXISTS wallets;`);
  }
}
```

- [ ] **Step 4: Run the entity mapping unit test to verify it passes**

Run: `bun run test:unit src/common/database/entities-mapping.spec.ts`
Expected: PASS


---

### Task 5: Wallet Module (`WalletRepository`, `WalletLedgerRepository`, `WalletService`, `WalletController`, `.form.ts`, `.response.ts`, `.dto.ts`)

**Files:**
- Create: `src/app/wallet/forms/money.form.ts`
- Create: `src/app/wallet/forms/create-wallet.form.ts`
- Create: `src/app/wallet/forms/get-wallet-ledger.form.ts`
- Create: `src/app/wallet/responses/money.response.ts`
- Create: `src/app/wallet/responses/wallet.response.ts`
- Create: `src/app/wallet/responses/wallet-ledger.response.ts`
- Create: `src/app/wallet/responses/wallet-reconciliation.response.ts`
- Create: `src/app/wallet/dtos/create-wallet.dto.ts`
- Create: `src/app/wallet/repositories/wallet.repository.ts`
- Create: `src/app/wallet/repositories/wallet-ledger.repository.ts`
- Create: `src/app/wallet/services/wallet.service.ts`
- Create: `src/app/wallet/controllers/wallet.controller.ts`
- Create: `src/app/wallet/wallet.module.ts`
- Test: `src/app/wallet/services/wallet.service.spec.ts`

**Interfaces:**
- Consumes: `DataSource`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `Money`, `MetricsService`, `AppLogger`
- Produces: `WalletModule` (exports `WalletService` only), `WalletService.createWallet`, `WalletService.getWalletById`, `WalletService.getWalletLedger`, `WalletService.reconcileWallet`

- [ ] **Step 1: Write the failing unit test for `WalletService` reconciliation and opening logic**

Create `src/app/wallet/services/wallet.service.spec.ts`:
```typescript
import { WalletService } from './wallet.service';
import { WalletRepository } from '../repositories/wallet.repository';
import { WalletLedgerRepository } from '../repositories/wallet-ledger.repository';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { AppLogger } from '../../../common/logger/app.logger';
import { Wallet } from '../domain/wallet.domain';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { DataSource } from 'typeorm';

describe('WalletService Unit Tests', () => {
  let walletService: WalletService;
  let walletRepository: jest.Mocked<WalletRepository>;
  let walletLedgerRepository: jest.Mocked<WalletLedgerRepository>;
  let metricsService: MetricsService;
  let logger: AppLogger;
  let dataSource: Partial<DataSource>;

  beforeEach(() => {
    walletRepository = {
      findById: jest.fn(),
      findByIdForUpdate: jest.fn(),
      save: jest.fn(),
    } as unknown as jest.Mocked<WalletRepository>;

    walletLedgerRepository = {
      findPaginatedByWalletId: jest.fn(),
      findAllByWalletIdOrdered: jest.fn(),
      save: jest.fn(),
    } as unknown as jest.Mocked<WalletLedgerRepository>;

    metricsService = new MetricsService();
    logger = new AppLogger();
    dataSource = {};

    walletService = new WalletService(
      dataSource as DataSource,
      walletRepository,
      walletLedgerRepository,
      metricsService,
      logger,
    );
  });

  it('reconciles consistent wallet balance against ledger entries', async () => {
    const wallet = Wallet.rehydrate({
      id: 'w-1',
      playerId: 'p-1',
      currency: 'BRL',
      balance: Money.from({ amount: '975.00', currency: 'BRL' }),
      version: 2,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const openingEntry = WalletLedgerEntry.create({
      id: 'le-1',
      walletId: 'w-1',
      transactionId: 'tx-open',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '1000.00', currency: 'BRL' }),
      balanceBefore: Money.zero('BRL'),
      balanceAfter: Money.from({ amount: '1000.00', currency: 'BRL' }),
    });
    const betEntry = WalletLedgerEntry.create({
      id: 'le-2',
      walletId: 'w-1',
      transactionId: 'tx-bet',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '1000.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '975.00', currency: 'BRL' }),
    });

    walletRepository.findById.mockResolvedValue(wallet);
    walletLedgerRepository.findAllByWalletIdOrdered.mockResolvedValue([
      openingEntry,
      betEntry,
    ]);

    const result = await walletService.reconcileWallet('w-1');

    expect(result.consistent).toBe(true);
    expect(result.storedBalance).toEqual({ amount: '975.00', currency: 'BRL' });
    expect(result.calculatedBalance).toEqual({
      amount: '975.00',
      currency: 'BRL',
    });
    expect(result.difference).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(result.checkedEntries).toBe(2);
  });

  it('detects and records metric when stored balance diverges from ledger', async () => {
    const wallet = Wallet.rehydrate({
      id: 'w-1',
      playerId: 'p-1',
      currency: 'BRL',
      balance: Money.from({ amount: '500.00', currency: 'BRL' }),
      version: 2,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const openingEntry = WalletLedgerEntry.create({
      id: 'le-1',
      walletId: 'w-1',
      transactionId: 'tx-open',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceBefore: Money.zero('BRL'),
      balanceAfter: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    walletRepository.findById.mockResolvedValue(wallet);
    walletLedgerRepository.findAllByWalletIdOrdered.mockResolvedValue([
      openingEntry,
    ]);

    const result = await walletService.reconcileWallet('w-1');

    expect(result.consistent).toBe(false);
    expect(result.difference).toEqual({ amount: '400.00', currency: 'BRL' });
    expect(metricsService.getSnapshot().reconciliationDivergences).toBe(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/app/wallet/services/wallet.service.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement Wallet forms, responses, dtos, repositories, service, controller, and module**

Create `src/app/wallet/forms/money.form.ts`:
```typescript
import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, Matches } from 'class-validator';

export class MoneyForm {
  @ApiProperty({ example: '1000.00' })
  @IsString()
  @IsNotEmpty()
  @Matches(/^\d+\.\d{2}$/, {
    message: 'amount must be a non-negative decimal string with scale 2',
  })
  amount: string;

  @ApiProperty({ example: 'BRL' })
  @IsString()
  @IsNotEmpty()
  @Matches(/^[A-Z]{3}$/, {
    message: 'currency must be a 3-letter ISO-4217 code',
  })
  currency: string;
}
```

Create `src/app/wallet/forms/create-wallet.form.ts`:
```typescript
import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsNotEmpty, IsString, ValidateNested } from 'class-validator';
import { MoneyForm } from './money.form';

export class CreateWalletForm {
  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  @IsString()
  @IsNotEmpty()
  playerId: string;

  @ApiProperty({ type: MoneyForm })
  @ValidateNested()
  @Type(() => MoneyForm)
  @IsNotEmpty()
  initialBalance: MoneyForm;
}
```

Create `src/app/wallet/forms/get-wallet-ledger.form.ts`:
```typescript
import { ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class GetWalletLedgerForm {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  cursor?: string;

  @ApiPropertyOptional({ default: 50 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit?: number = 50;
}
```

Create `src/app/wallet/responses/money.response.ts`:
```typescript
import { ApiProperty } from '@nestjs/swagger';

export class MoneyResponse {
  @ApiProperty({ example: '1000.00' })
  amount: string;

  @ApiProperty({ example: 'BRL' })
  currency: string;
}
```

Create `src/app/wallet/responses/wallet.response.ts`:
```typescript
import { ApiProperty } from '@nestjs/swagger';
import { MoneyResponse } from './money.response';

export class WalletResponse {
  @ApiProperty({ example: '0192f291-27dd-7d3f-8071-5f8685deef37' })
  id: string;

  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  playerId: string;

  @ApiProperty({ type: MoneyResponse })
  balance: MoneyResponse;

  @ApiProperty({ example: 1 })
  version: number;
}
```

Create `src/app/wallet/responses/wallet-ledger.response.ts`:
```typescript
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { MoneyResponse } from './money.response';

export class WalletLedgerEntryItemResponse {
  @ApiProperty()
  id: string;

  @ApiProperty()
  walletId: string;

  @ApiProperty()
  transactionId: string;

  @ApiProperty({ enum: LedgerDirection })
  direction: LedgerDirection;

  @ApiProperty({ type: MoneyResponse })
  money: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  balanceBefore: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  balanceAfter: MoneyResponse;

  @ApiProperty()
  createdAt: string;
}

export class WalletLedgerResponse {
  @ApiProperty({ type: [WalletLedgerEntryItemResponse] })
  items: WalletLedgerEntryItemResponse[];

  @ApiPropertyOptional({ nullable: true })
  nextCursor: string | null;
}
```

Create `src/app/wallet/responses/wallet-reconciliation.response.ts`:
```typescript
import { ApiProperty } from '@nestjs/swagger';
import { MoneyResponse } from './money.response';

export class WalletReconciliationResponse {
  @ApiProperty()
  walletId: string;

  @ApiProperty({ type: MoneyResponse })
  storedBalance: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  calculatedBalance: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  difference: MoneyResponse;

  @ApiProperty()
  consistent: boolean;

  @ApiProperty()
  checkedEntries: number;
}
```

Create `src/app/wallet/dtos/create-wallet.dto.ts`:
```typescript
import { MoneyProps } from '../../../common/domain/money.domain';

export interface CreateWalletDto {
  playerId: string;
  initialBalance: MoneyProps;
}
```

Create `src/app/wallet/repositories/wallet.repository.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { Wallet } from '../domain/wallet.domain';
import { WalletEntity } from '../entities/wallet.entity';

@Injectable()
export class WalletRepository {
  constructor(private readonly dataSource: DataSource) {}

  async findById(
    id: string,
    manager?: EntityManager,
  ): Promise<Wallet | undefined> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletEntity,
    );
    const entity = await repo.findOne({ where: { id } });
    return entity ? entity.toDomain() : undefined;
  }

  async findByIdForUpdate(
    id: string,
    manager: EntityManager,
  ): Promise<Wallet | undefined> {
    const entity = await manager
      .getRepository(WalletEntity)
      .createQueryBuilder('wallet')
      .setLock('pessimistic_write')
      .where('wallet.id = :id', { id })
      .getOne();
    return entity ? entity.toDomain() : undefined;
  }

  async save(wallet: Wallet, manager?: EntityManager): Promise<void> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletEntity,
    );
    await repo.save(WalletEntity.fromDomain(wallet));
  }
}
```

Create `src/app/wallet/repositories/wallet-ledger.repository.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';
import { WalletLedgerEntryEntity } from '../entities/wallet-ledger-entry.entity';
import { LedgerCursorPayload } from '../../../common/functions/cursor.function';

@Injectable()
export class WalletLedgerRepository {
  constructor(private readonly dataSource: DataSource) {}

  async save(
    entry: WalletLedgerEntry,
    manager?: EntityManager,
  ): Promise<void> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletLedgerEntryEntity,
    );
    await repo.insert(WalletLedgerEntryEntity.fromDomain(entry));
  }

  async findPaginatedByWalletId(
    walletId: string,
    limit: number,
    cursor?: LedgerCursorPayload,
  ): Promise<WalletLedgerEntry[]> {
    const qb = this.dataSource.manager
      .getRepository(WalletLedgerEntryEntity)
      .createQueryBuilder('le')
      .where('le.walletId = :walletId', { walletId });

    if (cursor) {
      qb.andWhere(
        '(le.createdAt > :cursorDate OR (le.createdAt = :cursorDate AND le.id > :cursorId))',
        {
          cursorDate: cursor.createdAt,
          cursorId: cursor.id,
        },
      );
    }

    const entities = await qb
      .orderBy('le.createdAt', 'ASC')
      .addOrderBy('le.id', 'ASC')
      .take(limit + 1)
      .getMany();

    return entities.map((e) => e.toDomain());
  }

  async findAllByWalletIdOrdered(
    walletId: string,
    manager?: EntityManager,
  ): Promise<WalletLedgerEntry[]> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletLedgerEntryEntity,
    );
    const entities = await repo.find({
      where: { walletId },
      order: { createdAt: 'ASC', id: 'ASC' },
    });
    return entities.map((e) => e.toDomain());
  }
}
```

Create `src/app/wallet/services/wallet.service.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import {
  DuplicateWalletError,
  EntityNotFoundError,
} from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { computeCanonicalPayloadHash } from '../../../common/functions/canonical-hash.function';
import {
  decodeLedgerCursor,
  encodeLedgerCursor,
} from '../../../common/functions/cursor.function';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { WagerTransaction } from '../../wagering/domain/wager-transaction.domain';
import { WagerTransactionEntity } from '../../wagering/entities/wager-transaction.entity';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';
import { Wallet } from '../domain/wallet.domain';
import { CreateWalletDto } from '../dtos/create-wallet.dto';
import { GetWalletLedgerForm } from '../forms/get-wallet-ledger.form';
import { WalletLedgerRepository } from '../repositories/wallet-ledger.repository';
import { WalletRepository } from '../repositories/wallet.repository';
import { WalletLedgerResponse } from '../responses/wallet-ledger.response';
import { WalletReconciliationResponse } from '../responses/wallet-reconciliation.response';
import { WalletResponse } from '../responses/wallet.response';

@Injectable()
export class WalletService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly walletRepository: WalletRepository,
    private readonly walletLedgerRepository: WalletLedgerRepository,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  async createWallet(dto: CreateWalletDto): Promise<WalletResponse> {
    const initialMoney = Money.from(dto.initialBalance);
    const walletId = uuidv4();
    const now = new Date();
    const wallet = Wallet.open({
      id: walletId,
      playerId: dto.playerId,
      initialBalance: initialMoney,
      createdAt: now,
    });

    try {
      await this.dataSource.transaction(async (manager) => {
        await this.walletRepository.save(wallet, manager);

        if (initialMoney.isPositive()) {
          const txId = uuidv4();
          const idempotencyKey = `system:opening:${walletId}`;
          const payloadHash = computeCanonicalPayloadHash({
            providerId: 'system',
            externalTransactionId: `opening:${walletId}`,
            playerId: dto.playerId,
            walletId,
            roundId: `opening:${walletId}`,
            gameId: 'system-opening',
            kind: WagerTransactionKind.Opening,
            money: initialMoney.toJSON(),
          });

          const openingTx = WagerTransaction.create({
            id: txId,
            providerId: 'system',
            externalTransactionId: `opening:${walletId}`,
            idempotencyKey,
            payloadHash,
            walletId,
            playerId: dto.playerId,
            roundId: `opening:${walletId}`,
            gameId: 'system-opening',
            kind: WagerTransactionKind.Opening,
            money: initialMoney,
            createdAt: now,
          });
          openingTx.markProcessed(undefined, now, initialMoney);

          await manager
            .getRepository(WagerTransactionEntity)
            .insert(WagerTransactionEntity.fromDomain(openingTx));

          const openingEntry = WalletLedgerEntry.create({
            id: uuidv4(),
            walletId,
            transactionId: txId,
            direction: LedgerDirection.Credit,
            money: initialMoney,
            balanceBefore: Money.zero(initialMoney.currency),
            balanceAfter: initialMoney,
            createdAt: now,
          });

          await this.walletLedgerRepository.save(openingEntry, manager);
        }
      });
    } catch (error: unknown) {
      const pgCode = (error as { code?: string })?.code;
      if (pgCode === '23505') {
        throw new DuplicateWalletError(dto.playerId, initialMoney.currency);
      }
      throw error;
    }

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  async getWalletById(walletId: string): Promise<WalletResponse> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new EntityNotFoundError('Wallet', walletId);
    }
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  async getWalletLedger(
    walletId: string,
    query: GetWalletLedgerForm,
  ): Promise<WalletLedgerResponse> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new EntityNotFoundError('Wallet', walletId);
    }

    const limit = query.limit ?? 50;
    const decodedCursor = query.cursor
      ? decodeLedgerCursor(query.cursor)
      : undefined;

    const entries = await this.walletLedgerRepository.findPaginatedByWalletId(
      walletId,
      limit,
      decodedCursor,
    );

    const hasMore = entries.length > limit;
    const pageItems = hasMore ? entries.slice(0, limit) : entries;
    const lastItem = pageItems[pageItems.length - 1];
    const nextCursor =
      hasMore && lastItem
        ? encodeLedgerCursor({
            createdAt: lastItem.createdAt,
            id: lastItem.id,
          })
        : null;

    return {
      items: pageItems.map((entry) => ({
        id: entry.id,
        walletId: entry.walletId,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        createdAt: entry.createdAt.toISOString(),
      })),
      nextCursor,
    };
  }

  async reconcileWallet(
    walletId: string,
  ): Promise<WalletReconciliationResponse> {
    const wallet = await this.walletRepository.findById(walletId);
    if (!wallet) {
      throw new EntityNotFoundError('Wallet', walletId);
    }

    const entries =
      await this.walletLedgerRepository.findAllByWalletIdOrdered(walletId);

    let calculated = Money.zero(wallet.currency);
    for (const entry of entries) {
      calculated =
        entry.direction === LedgerDirection.Credit
          ? calculated.add(entry.money)
          : calculated.subtract(entry.money);
    }

    const stored = wallet.balance;
    const consistent = stored.equals(calculated);
    const rawDiff = stored.subtract(calculated);
    const difference = rawDiff.isNegative() ? rawDiff.negate() : rawDiff;

    if (!consistent) {
      this.metricsService.incrementReconciliationDivergence();
      this.logger.error('Wallet reconciliation divergence detected', undefined, {
        walletId,
        storedBalance: stored.toString(),
        calculatedBalance: calculated.toString(),
      });
    }

    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: entries.length,
    };
  }
}
```

Create `src/app/wallet/controllers/wallet.controller.ts`:
```typescript
import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ApiCreatedResponse, ApiResponse, ApiTags } from '@nestjs/swagger';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { CreateWalletForm } from '../forms/create-wallet.form';
import { GetWalletLedgerForm } from '../forms/get-wallet-ledger.form';
import { WalletLedgerResponse } from '../responses/wallet-ledger.response';
import { WalletReconciliationResponse } from '../responses/wallet-reconciliation.response';
import { WalletResponse } from '../responses/wallet.response';
import { WalletService } from '../services/wallet.service';

@ApiTags('Wallets')
@UseGuards(ProviderAuthGuard)
@Controller('wallets')
export class WalletController {
  constructor(private readonly walletService: WalletService) {}

  @ApiCreatedResponse({ type: WalletResponse })
  @Post()
  async createWallet(
    @Body() createWalletForm: CreateWalletForm,
  ): Promise<WalletResponse> {
    return this.walletService.createWallet(createWalletForm);
  }

  @ApiResponse({ status: 200, type: WalletResponse })
  @Get(':walletId')
  async getWallet(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
  ): Promise<WalletResponse> {
    return this.walletService.getWalletById(walletId);
  }

  @ApiResponse({ status: 200, type: WalletLedgerResponse })
  @Get(':walletId/ledger')
  async getWalletLedger(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
    @Query() query: GetWalletLedgerForm,
  ): Promise<WalletLedgerResponse> {
    return this.walletService.getWalletLedger(walletId, query);
  }

  @ApiResponse({ status: 200, type: WalletReconciliationResponse })
  @HttpCode(HttpStatus.OK)
  @Post(':walletId/reconciliation')
  async reconcileWallet(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
  ): Promise<WalletReconciliationResponse> {
    return this.walletService.reconcileWallet(walletId);
  }
}
```

Create `src/app/wallet/wallet.module.ts`:
```typescript
import { Module } from '@nestjs/common';
import { WalletController } from './controllers/wallet.controller';
import { WalletLedgerRepository } from './repositories/wallet-ledger.repository';
import { WalletRepository } from './repositories/wallet.repository';
import { WalletService } from './services/wallet.service';
import { MetricsService } from '../../common/metrics/metrics.service';
import { AppLogger } from '../../common/logger/app.logger';

@Module({
  controllers: [WalletController],
  providers: [
    WalletService,
    WalletRepository,
    WalletLedgerRepository,
    MetricsService,
    AppLogger,
  ],
  exports: [WalletService],
})
export class WalletModule {}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `bun run test:unit src/app/wallet/services/wallet.service.spec.ts`
Expected: PASS


---

### Task 6: Wagering Core Use Case, Outbox Persistence & HTTP Endpoints (`WageringService`, `WageringController`, `ProviderWageringController`)

**Files:**
- Create: `src/app/messaging/repositories/outbox.repository.ts`
- Create: `src/app/messaging/repositories/inbox.repository.ts`
- Create: `src/app/wagering/repositories/wager-transaction.repository.ts`
- Create: `src/app/wagering/forms/submit-wager-transaction.form.ts`
- Create: `src/app/wagering/responses/submit-wager-transaction.response.ts`
- Create: `src/app/wagering/responses/wager-transaction.response.ts`
- Create: `src/app/wagering/dtos/process-wager-transaction.dto.ts`
- Create: `src/app/wagering/services/wagering.service.ts`
- Create: `src/app/wagering/controllers/wagering.controller.ts`
- Create: `src/app/wagering/controllers/provider-wagering.controller.ts`
- Create: `src/common/filters/domain-exception.filter.ts`
- Test: `src/app/wagering/services/wagering.service.spec.ts`

**Interfaces:**
- Consumes: `DataSource`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `OutboxMessage`, `InboxMessage`, `IntegrationEvent` subclasses, `MetricsService`, `AppLogger`
- Produces: `WageringService.submitTransaction`, `WageringService.retryPendingReferences`, `WageringService.getTransactionById`, `WageringService.getTransactionByProviderAndExternalId`, `DomainExceptionFilter`

- [ ] **Step 1: Write the failing unit tests for `WageringService` covering `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`, `PENDING_REFERENCE`, currency conflict, and idempotency conflict**

Create `src/app/wagering/services/wagering.service.spec.ts`:
```typescript
import { DataSource, EntityManager } from 'typeorm';
import { WageringService } from './wagering.service';
import { WagerTransactionRepository } from '../repositories/wager-transaction.repository';
import { OutboxRepository } from '../../messaging/repositories/outbox.repository';
import { InboxRepository } from '../../messaging/repositories/inbox.repository';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { AppLogger } from '../../../common/logger/app.logger';
import { Wallet } from '../../wallet/domain/wallet.domain';
import { Money } from '../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { IdempotencyConflictError } from '../../../common/domain/domain.error';
import { WagerTransaction } from '../domain/wager-transaction.domain';
import { computeCanonicalPayloadHash } from '../../../common/functions/canonical-hash.function';

describe('WageringService Unit Tests', () => {
  let service: WageringService;
  let wagerRepo: jest.Mocked<WagerTransactionRepository>;
  let outboxRepo: jest.Mocked<OutboxRepository>;
  let inboxRepo: jest.Mocked<InboxRepository>;
  let metrics: MetricsService;
  let logger: AppLogger;
  let mockManager: Partial<EntityManager>;
  let mockWallet: Wallet;

  beforeEach(() => {
    mockWallet = Wallet.open({
      id: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    wagerRepo = {
      findByIdempotencyKey: jest.fn().mockResolvedValue(undefined),
      findByProviderAndExternalId: jest.fn().mockResolvedValue(undefined),
      findProcessedReversalForReference: jest.fn().mockResolvedValue(undefined),
      save: jest.fn().mockResolvedValue(undefined),
      findById: jest.fn(),
      findDuePendingReferencesForUpdate: jest.fn().mockResolvedValue([]),
    } as unknown as jest.Mocked<WagerTransactionRepository>;

    outboxRepo = {
      saveMany: jest.fn().mockResolvedValue(undefined),
    } as unknown as jest.Mocked<OutboxRepository>;

    inboxRepo = {
      findByKeyForUpdate: jest.fn().mockResolvedValue(undefined),
      save: jest.fn().mockResolvedValue(undefined),
    } as unknown as jest.Mocked<InboxRepository>;

    mockManager = {
      getRepository: jest.fn().mockReturnValue({
        createQueryBuilder: jest.fn().mockReturnValue({
          setLock: jest.fn().mockReturnThis(),
          where: jest.fn().mockReturnThis(),
          getOne: jest.fn().mockImplementation(async () => ({
            toDomain: () => mockWallet,
          })),
        }),
        save: jest.fn().mockResolvedValue(undefined),
        insert: jest.fn().mockResolvedValue(undefined),
      }),
    };

    const dataSource = {
      transaction: jest
        .fn()
        .mockImplementation(async (cb: (em: EntityManager) => unknown) =>
          cb(mockManager as EntityManager),
        ),
    } as unknown as DataSource;

    metrics = new MetricsService();
    logger = new AppLogger();

    service = new WageringService(
      dataSource,
      wagerRepo,
      outboxRepo,
      inboxRepo,
      metrics,
      logger,
    );
  });

  const baseDto = {
    idempotencyKey: 'provider-a:tx-1',
    providerId: 'provider-a',
    externalTransactionId: 'tx-1',
    playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
    walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: WagerTransactionKind.Bet,
    money: { amount: '25.00', currency: 'BRL' },
    correlationId: 'corr-1',
  };

  it('processes BET debiting wallet and enqueueing WagerTransactionProcessed + WalletBalanceChanged', async () => {
    const res = await service.submitTransaction(baseDto);

    expect(res.status).toBe(WagerTransactionStatus.Processed);
    expect(res.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(res.idempotentReplay).toBe(false);
    expect(outboxRepo.saveMany).toHaveBeenCalledTimes(1);
    const enqueuedEvents = outboxRepo.saveMany.mock.calls[0][0];
    expect(enqueuedEvents.map((e) => e.eventType)).toEqual([
      'WalletBalanceChanged',
      'WagerTransactionProcessed',
    ]);
  });

  it('rejects BET when balance is insufficient without altering balance or creating WalletBalanceChanged', async () => {
    const res = await service.submitTransaction({
      ...baseDto,
      money: { amount: '250.00', currency: 'BRL' },
    });

    expect(res.status).toBe(WagerTransactionStatus.Rejected);
    expect(res.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(res.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    const enqueuedEvents = outboxRepo.saveMany.mock.calls[0][0];
    expect(enqueuedEvents.map((e) => e.eventType)).toEqual([
      'WagerTransactionRejected',
    ]);
  });

  it('processes LOSS without changing balance and emits only WagerTransactionProcessed', async () => {
    const res = await service.submitTransaction({
      ...baseDto,
      kind: WagerTransactionKind.Loss,
      money: { amount: '0.00', currency: 'BRL' },
    });

    expect(res.status).toBe(WagerTransactionStatus.Processed);
    expect(res.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    const enqueuedEvents = outboxRepo.saveMany.mock.calls[0][0];
    expect(enqueuedEvents.map((e) => e.eventType)).toEqual([
      'WagerTransactionProcessed',
    ]);
  });

  it('marks REFUND as PENDING_REFERENCE when referenced BET has not arrived yet', async () => {
    const res = await service.submitTransaction({
      ...baseDto,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'missing-bet',
    });

    expect(res.status).toBe(WagerTransactionStatus.PendingReference);
    const enqueuedEvents = outboxRepo.saveMany.mock.calls[0][0];
    expect(enqueuedEvents.map((e) => e.eventType)).toEqual([
      'WagerTransactionPendingReference',
    ]);
  });

  it('rejects ROLLBACK of WIN with REVERSAL_INSUFFICIENT_FUNDS when wallet balance would become negative', async () => {
    const refWin = WagerTransaction.create({
      id: 'tx-win-1',
      providerId: 'provider-a',
      externalTransactionId: 'win-1',
      idempotencyKey: 'provider-a:win-1',
      payloadHash: 'hash-win',
      walletId: mockWallet.id,
      playerId: mockWallet.playerId,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Win,
      money: Money.from({ amount: '500.00', currency: 'BRL' }),
    });
    refWin.markProcessed(
      undefined,
      new Date(),
      Money.from({ amount: '600.00', currency: 'BRL' }),
    );
    wagerRepo.findByProviderAndExternalId.mockResolvedValueOnce(refWin);

    const res = await service.submitTransaction({
      ...baseDto,
      externalTransactionId: 'rb-1',
      idempotencyKey: 'provider-a:rb-1',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'win-1',
      money: { amount: '500.00', currency: 'BRL' },
    });

    expect(res.status).toBe(WagerTransactionStatus.Rejected);
    expect(res.failureCode).toBe(FailureCode.ReversalInsufficientFunds);
  });

  it('throws IdempotencyConflictError when same idempotencyKey is submitted with different payload', async () => {
    const existingHash = computeCanonicalPayloadHash({
      ...baseDto,
      money: { amount: '25.00', currency: 'BRL' },
    });
    const existingTx = WagerTransaction.create({
      id: 'tx-existing',
      providerId: baseDto.providerId,
      externalTransactionId: baseDto.externalTransactionId,
      idempotencyKey: baseDto.idempotencyKey,
      payloadHash: existingHash,
      walletId: baseDto.walletId,
      playerId: baseDto.playerId,
      roundId: baseDto.roundId,
      gameId: baseDto.gameId,
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    });
    existingTx.markProcessed(
      undefined,
      new Date(),
      Money.from({ amount: '75.00', currency: 'BRL' }),
    );
    wagerRepo.findByIdempotencyKey.mockResolvedValueOnce(existingTx);

    await expect(
      service.submitTransaction({
        ...baseDto,
        money: { amount: '99.00', currency: 'BRL' },
      }),
    ).rejects.toThrow(IdempotencyConflictError);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/app/wagering/services/wagering.service.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement repositories, forms, responses, dtos, `WageringService`, controllers, and `DomainExceptionFilter`**

Create `src/app/messaging/repositories/outbox.repository.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { OutboxMessage } from '../domain/outbox-message.domain';
import { OutboxMessageEntity } from '../entities/outbox-message.entity';

@Injectable()
export class OutboxRepository {
  constructor(private readonly dataSource: DataSource) {}

  async saveMany(
    messages: OutboxMessage[],
    manager?: EntityManager,
  ): Promise<void> {
    if (messages.length === 0) {
      return;
    }
    const repo = (manager ?? this.dataSource.manager).getRepository(
      OutboxMessageEntity,
    );
    await repo.insert(messages.map((m) => OutboxMessageEntity.fromDomain(m)));
  }

  async save(message: OutboxMessage, manager?: EntityManager): Promise<void> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      OutboxMessageEntity,
    );
    await repo.save(OutboxMessageEntity.fromDomain(message));
  }

  async claimDueBatchForUpdate(
    batchSize: number,
    now: Date,
    manager: EntityManager,
  ): Promise<OutboxMessage[]> {
    const entities = await manager
      .getRepository(OutboxMessageEntity)
      .createQueryBuilder('outbox')
      .setLock('pessimistic_write')
      .setOnLocked('skip_locked')
      .where('outbox.publishedAt IS NULL')
      .andWhere('(outbox.nextAttemptAt IS NULL OR outbox.nextAttemptAt <= :now)', {
        now,
      })
      .orderBy('outbox.occurredAt', 'ASC')
      .take(batchSize)
      .getMany();

    return entities.map((e) => e.toDomain());
  }

  async getOldestUnpublishedOccurredAt(): Promise<Date | undefined> {
    const oldest = await this.dataSource.manager
      .getRepository(OutboxMessageEntity)
      .createQueryBuilder('outbox')
      .where('outbox.publishedAt IS NULL')
      .orderBy('outbox.occurredAt', 'ASC')
      .getOne();
    return oldest ? new Date(oldest.occurredAt) : undefined;
  }
}
```

Create `src/app/messaging/repositories/inbox.repository.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { InboxMessage } from '../domain/inbox-message.domain';
import { InboxMessageEntity } from '../entities/inbox-message.entity';

@Injectable()
export class InboxRepository {
  constructor(private readonly dataSource: DataSource) {}

  async findByKeyForUpdate(
    consumerName: string,
    messageId: string,
    manager: EntityManager,
  ): Promise<InboxMessage | undefined> {
    const entity = await manager
      .getRepository(InboxMessageEntity)
      .createQueryBuilder('inbox')
      .setLock('pessimistic_write')
      .where('inbox.consumerName = :consumerName', { consumerName })
      .andWhere('inbox.messageId = :messageId', { messageId })
      .getOne();
    return entity ? entity.toDomain() : undefined;
  }

  async save(inbox: InboxMessage, manager: EntityManager): Promise<void> {
    await manager
      .getRepository(InboxMessageEntity)
      .save(InboxMessageEntity.fromDomain(inbox));
  }
}
```

Create `src/app/wagering/repositories/wager-transaction.repository.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { WagerTransaction } from '../domain/wager-transaction.domain';
import { WagerTransactionEntity } from '../entities/wager-transaction.entity';

@Injectable()
export class WagerTransactionRepository {
  constructor(private readonly dataSource: DataSource) {}

  async findById(
    id: string,
    manager?: EntityManager,
  ): Promise<WagerTransaction | undefined> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WagerTransactionEntity,
    );
    const entity = await repo.findOne({ where: { id } });
    return entity ? entity.toDomain() : undefined;
  }

  async findByIdempotencyKey(
    idempotencyKey: string,
    manager?: EntityManager,
  ): Promise<WagerTransaction | undefined> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WagerTransactionEntity,
    );
    const entity = await repo.findOne({ where: { idempotencyKey } });
    return entity ? entity.toDomain() : undefined;
  }

  async findByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
    manager?: EntityManager,
  ): Promise<WagerTransaction | undefined> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WagerTransactionEntity,
    );
    const entity = await repo.findOne({
      where: { providerId, externalTransactionId },
    });
    return entity ? entity.toDomain() : undefined;
  }

  async findProcessedReversalForReference(
    referenceTransactionId: string,
    kind: WagerTransactionKind,
    manager: EntityManager,
  ): Promise<WagerTransaction | undefined> {
    const entity = await manager
      .getRepository(WagerTransactionEntity)
      .findOne({
        where: {
          referenceTransactionId,
          kind,
          status: WagerTransactionStatus.Processed,
        },
      });
    return entity ? entity.toDomain() : undefined;
  }

  async findDuePendingReferencesForUpdate(
    batchSize: number,
    now: Date,
    manager: EntityManager,
  ): Promise<WagerTransaction[]> {
    const entities = await manager
      .getRepository(WagerTransactionEntity)
      .createQueryBuilder('tx')
      .setLock('pessimistic_write')
      .setOnLocked('skip_locked')
      .where('tx.status = :status', {
        status: WagerTransactionStatus.PendingReference,
      })
      .andWhere('(tx.nextRetryAt IS NULL OR tx.nextRetryAt <= :now)', { now })
      .orderBy('tx.createdAt', 'ASC')
      .take(batchSize)
      .getMany();

    return entities.map((e) => e.toDomain());
  }

  async save(tx: WagerTransaction, manager?: EntityManager): Promise<void> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WagerTransactionEntity,
    );
    await repo.save(WagerTransactionEntity.fromDomain(tx));
  }
}
```

Create `src/app/wagering/forms/submit-wager-transaction.form.ts`:
```typescript
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsIn,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  ValidateNested,
} from 'class-validator';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { MoneyForm } from '../../wallet/forms/money.form';

const EXTERNAL_ALLOWED_KINDS = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
] as const;

export class SubmitWagerTransactionForm {
  @ApiProperty({ example: 'provider-a' })
  @IsString()
  @IsNotEmpty()
  providerId: string;

  @ApiProperty({ example: 'transaction-123' })
  @IsString()
  @IsNotEmpty()
  externalTransactionId: string;

  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  @IsString()
  @IsNotEmpty()
  playerId: string;

  @ApiProperty({ example: '0192f291-27dd-7d3f-8071-5f8685deef37' })
  @IsUUID()
  @IsNotEmpty()
  walletId: string;

  @ApiProperty({ example: 'round-987' })
  @IsString()
  @IsNotEmpty()
  roundId: string;

  @ApiProperty({ example: 'fortune-chimp' })
  @IsString()
  @IsNotEmpty()
  gameId: string;

  @ApiProperty({ enum: EXTERNAL_ALLOWED_KINDS, example: WagerTransactionKind.Bet })
  @IsIn(EXTERNAL_ALLOWED_KINDS)
  kind: WagerTransactionKind;

  @ApiProperty({ type: MoneyForm })
  @ValidateNested()
  @Type(() => MoneyForm)
  @IsNotEmpty()
  money: MoneyForm;

  @ApiPropertyOptional({ example: 'transaction-100' })
  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}
```

Create `src/app/wagering/responses/submit-wager-transaction.response.ts`:
```typescript
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { MoneyResponse } from '../../wallet/responses/money.response';

export class SubmitWagerTransactionResponse {
  @ApiProperty()
  transactionId: string;

  @ApiProperty({ enum: WagerTransactionStatus })
  status: WagerTransactionStatus;

  @ApiProperty({ type: MoneyResponse })
  balance: MoneyResponse;

  @ApiProperty()
  idempotentReplay: boolean;

  @ApiPropertyOptional({ enum: FailureCode })
  failureCode?: FailureCode;
}
```

Create `src/app/wagering/responses/wager-transaction.response.ts`:
```typescript
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { MoneyResponse } from '../../wallet/responses/money.response';

export class WagerTransactionResponse {
  @ApiProperty()
  id: string;

  @ApiProperty()
  providerId: string;

  @ApiProperty()
  externalTransactionId: string;

  @ApiProperty()
  idempotencyKey: string;

  @ApiProperty()
  walletId: string;

  @ApiProperty()
  playerId: string;

  @ApiProperty()
  roundId: string;

  @ApiProperty()
  gameId: string;

  @ApiProperty({ enum: WagerTransactionKind })
  kind: WagerTransactionKind;

  @ApiProperty({ type: MoneyResponse })
  money: MoneyResponse;

  @ApiProperty({ enum: WagerTransactionStatus })
  status: WagerTransactionStatus;

  @ApiPropertyOptional()
  referenceExternalTransactionId?: string;

  @ApiPropertyOptional()
  referenceTransactionId?: string;

  @ApiPropertyOptional({ enum: FailureCode })
  failureCode?: FailureCode;

  @ApiPropertyOptional({ type: MoneyResponse })
  balance?: MoneyResponse;

  @ApiProperty()
  createdAt: string;

  @ApiPropertyOptional()
  processedAt?: string;
}
```

Create `src/app/wagering/dtos/process-wager-transaction.dto.ts`:
```typescript
import { MoneyProps } from '../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';

export interface ProcessWagerTransactionDto {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  inboxContext?: {
    consumerName: string;
    messageId: string;
    payloadHash: string;
  };
}
```

Create `src/app/wagering/services/wagering.service.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import {
  DomainError,
  EntityNotFoundError,
  IdempotencyConflictError,
} from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { computeCanonicalPayloadHash } from '../../../common/functions/canonical-hash.function';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { IntegrationEvent } from '../../messaging/domain/events/integration-event.domain';
import { WagerTransactionPendingReference } from '../../messaging/domain/events/wager-transaction-pending-reference.event';
import { WagerTransactionProcessed } from '../../messaging/domain/events/wager-transaction-processed.event';
import { WagerTransactionRejected } from '../../messaging/domain/events/wager-transaction-rejected.event';
import { WalletBalanceChanged } from '../../messaging/domain/events/wallet-balance-changed.event';
import { InboxMessage } from '../../messaging/domain/inbox-message.domain';
import { OutboxMessage } from '../../messaging/domain/outbox-message.domain';
import { InboxRepository } from '../../messaging/repositories/inbox.repository';
import { OutboxRepository } from '../../messaging/repositories/outbox.repository';
import { WalletLedgerEntry } from '../../wallet/domain/wallet-ledger-entry.domain';
import { Wallet } from '../../wallet/domain/wallet.domain';
import { WalletLedgerEntryEntity } from '../../wallet/entities/wallet-ledger-entry.entity';
import { WalletEntity } from '../../wallet/entities/wallet.entity';
import { WagerTransaction } from '../domain/wager-transaction.domain';
import { ProcessWagerTransactionDto } from '../dtos/process-wager-transaction.dto';
import { WagerTransactionRepository } from '../repositories/wager-transaction.repository';
import { SubmitWagerTransactionResponse } from '../responses/submit-wager-transaction.response';
import { WagerTransactionResponse } from '../responses/wager-transaction.response';

@Injectable()
export class WageringService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly wagerTransactionRepository: WagerTransactionRepository,
    private readonly outboxRepository: OutboxRepository,
    private readonly inboxRepository: InboxRepository,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  async submitTransaction(
    dto: ProcessWagerTransactionDto,
  ): Promise<SubmitWagerTransactionResponse> {
    const startTime = Date.now();
    if (dto.kind === WagerTransactionKind.Opening) {
      throw new DomainError(
        'OPENING transaction kind is strictly internal',
        'FORBIDDEN_TRANSACTION_KIND',
      );
    }

    const money = Money.from(dto.money);
    const payloadHash = computeCanonicalPayloadHash({
      providerId: dto.providerId,
      externalTransactionId: dto.externalTransactionId,
      playerId: dto.playerId,
      walletId: dto.walletId,
      roundId: dto.roundId,
      gameId: dto.gameId,
      kind: dto.kind,
      money: money.toJSON(),
      referenceExternalTransactionId: dto.referenceExternalTransactionId,
    });

    const preExisting =
      await this.wagerTransactionRepository.findByIdempotencyKey(
        dto.idempotencyKey,
      );
    if (preExisting) {
      return this.handleExistingTransaction(preExisting, payloadHash);
    }

    try {
      const response = await this.dataSource.transaction(async (manager) => {
        if (dto.inboxContext) {
          const existingInbox = await this.inboxRepository.findByKeyForUpdate(
            dto.inboxContext.consumerName,
            dto.inboxContext.messageId,
            manager,
          );
          if (existingInbox && existingInbox.isProcessed()) {
            this.metricsService.incrementDuplicatesDetected();
            const existingTx =
              await this.wagerTransactionRepository.findByIdempotencyKey(
                dto.idempotencyKey,
                manager,
              );
            if (existingTx) {
              return this.handleExistingTransaction(existingTx, payloadHash, manager);
            }
          }
          const inboxMsg =
            existingInbox ??
            InboxMessage.receive({
              consumerName: dto.inboxContext.consumerName,
              messageId: dto.inboxContext.messageId,
              payloadHash: dto.inboxContext.payloadHash,
            });
          inboxMsg.markProcessed(new Date());
          await this.inboxRepository.save(inboxMsg, manager);
        }

        const walletEntity = await manager
          .getRepository(WalletEntity)
          .createQueryBuilder('wallet')
          .setLock('pessimistic_write')
          .where('wallet.id = :id', { id: dto.walletId })
          .getOne();

        if (!walletEntity) {
          throw new EntityNotFoundError('Wallet', dto.walletId);
        }
        const wallet = walletEntity.toDomain();

        const txInsideLock =
          await this.wagerTransactionRepository.findByIdempotencyKey(
            dto.idempotencyKey,
            manager,
          );
        if (txInsideLock) {
          return this.handleExistingTransaction(
            txInsideLock,
            payloadHash,
            manager,
          );
        }

        const providerDup =
          await this.wagerTransactionRepository.findByProviderAndExternalId(
            dto.providerId,
            dto.externalTransactionId,
            manager,
          );
        if (providerDup) {
          return this.handleExistingTransaction(
            providerDup,
            payloadHash,
            manager,
          );
        }

        const tx = WagerTransaction.create({
          id: uuidv4(),
          providerId: dto.providerId,
          externalTransactionId: dto.externalTransactionId,
          idempotencyKey: dto.idempotencyKey,
          payloadHash,
          walletId: dto.walletId,
          playerId: dto.playerId,
          roundId: dto.roundId,
          gameId: dto.gameId,
          kind: dto.kind,
          money,
          referenceExternalTransactionId: dto.referenceExternalTransactionId,
        });

        const result = await this.applyTransactionOnLockedWallet(
          tx,
          wallet,
          dto.correlationId ?? uuidv4(),
          manager,
        );

        this.metricsService.incrementTransactionStatus(result.status);
        return result;
      });

      this.metricsService.recordProcessingLatencyMs(Date.now() - startTime);
      return response;
    } catch (error: unknown) {
      const pgCode = (error as { code?: string })?.code;
      if (pgCode === '23505') {
        const racedTx =
          await this.wagerTransactionRepository.findByIdempotencyKey(
            dto.idempotencyKey,
          );
        if (racedTx) {
          return this.handleExistingTransaction(racedTx, payloadHash);
        }
      }
      if (pgCode === '40001' || pgCode === '40P01' || pgCode === '55P03') {
        this.metricsService.incrementLockConflicts();
      }
      throw error;
    }
  }

  private async handleExistingTransaction(
    existing: WagerTransaction,
    payloadHash: string,
    manager?: EntityManager,
  ): Promise<SubmitWagerTransactionResponse> {
    if (!existing.matchesPayload(payloadHash)) {
      throw new IdempotencyConflictError(existing.idempotencyKey);
    }
    this.metricsService.incrementDuplicatesDetected();
    let balance = existing.observedBalance;
    if (!balance) {
      const repo = (manager ?? this.dataSource.manager).getRepository(
        WalletEntity,
      );
      const w = await repo.findOne({ where: { id: existing.walletId } });
      balance = w ? w.toDomain().balance : Money.zero(existing.money.currency);
    }
    return {
      transactionId: existing.id,
      status: existing.status,
      balance: balance.toJSON(),
      idempotentReplay: true,
      failureCode: existing.failureCode,
    };
  }

  private async applyTransactionOnLockedWallet(
    tx: WagerTransaction,
    wallet: Wallet,
    correlationId: string,
    manager: EntityManager,
  ): Promise<SubmitWagerTransactionResponse> {
    const now = new Date();
    const events: IntegrationEvent<unknown>[] = [];

    if (wallet.playerId !== tx.playerId) {
      return this.rejectAndPersist(
        tx,
        wallet,
        FailureCode.WalletPlayerMismatch,
        correlationId,
        now,
        manager,
      );
    }

    if (wallet.currency !== tx.money.currency) {
      return this.rejectAndPersist(
        tx,
        wallet,
        FailureCode.CurrencyMismatch,
        correlationId,
        now,
        manager,
      );
    }

    let referenceTx: WagerTransaction | undefined;

    if (tx.requiresReference() || tx.referenceExternalTransactionId) {
      referenceTx =
        await this.wagerTransactionRepository.findByProviderAndExternalId(
          tx.providerId,
          tx.referenceExternalTransactionId!,
          manager,
        );

      if (!referenceTx && tx.requiresReference()) {
        tx.markPendingReference(now);
        await this.wagerTransactionRepository.save(tx, manager);

        events.push(
          WagerTransactionPendingReference.from(tx, {
            eventId: uuidv4(),
            correlationId,
            occurredAt: now,
          }),
        );
        await this.outboxRepository.saveMany(
          events.map((ev) => OutboxMessage.enqueue(ev)),
          manager,
        );

        this.logger.log('Wager transaction marked PENDING_REFERENCE', {
          correlationId,
          transactionId: tx.id,
          walletId: wallet.id,
          providerId: tx.providerId,
        });

        return {
          transactionId: tx.id,
          status: tx.status,
          balance: wallet.balance.toJSON(),
          idempotentReplay: false,
        };
      }

      if (referenceTx) {
        const refValidationFailure = await this.validateReferenceTransaction(
          tx,
          referenceTx,
          manager,
        );
        if (refValidationFailure) {
          return this.rejectAndPersist(
            tx,
            wallet,
            refValidationFailure,
            correlationId,
            now,
            manager,
          );
        }
      }
    }

    let ledgerEntry: WalletLedgerEntry | undefined;

    if (tx.affectsBalance()) {
      const direction = tx.ledgerDirectionFor(referenceTx);
      if (direction === LedgerDirection.Debit) {
        if (wallet.balance.isLessThan(tx.money)) {
          const code =
            tx.kind === WagerTransactionKind.Rollback
              ? FailureCode.ReversalInsufficientFunds
              : FailureCode.InsufficientFunds;
          return this.rejectAndPersist(
            tx,
            wallet,
            code,
            correlationId,
            now,
            manager,
          );
        }
        ledgerEntry = wallet.debit({
          entryId: uuidv4(),
          transactionId: tx.id,
          amount: tx.money,
          at: now,
          insufficientFundsCode:
            tx.kind === WagerTransactionKind.Rollback
              ? FailureCode.ReversalInsufficientFunds
              : FailureCode.InsufficientFunds,
        });
      } else {
        ledgerEntry = wallet.credit({
          entryId: uuidv4(),
          transactionId: tx.id,
          amount: tx.money,
          at: now,
        });
      }
    }

    tx.markProcessed(referenceTx?.id, now, wallet.balance);
    await this.wagerTransactionRepository.save(tx, manager);

    if (ledgerEntry) {
      await manager
        .getRepository(WalletEntity)
        .save(WalletEntity.fromDomain(wallet));
      await manager
        .getRepository(WalletLedgerEntryEntity)
        .insert(WalletLedgerEntryEntity.fromDomain(ledgerEntry));

      events.push(
        WalletBalanceChanged.from(wallet, ledgerEntry, {
          eventId: uuidv4(),
          correlationId,
          occurredAt: now,
        }),
      );
    }

    events.push(
      WagerTransactionProcessed.from(tx, wallet.balance.toJSON(), {
        eventId: uuidv4(),
        correlationId,
        occurredAt: now,
      }),
    );

    await this.outboxRepository.saveMany(
      events.map((ev) => OutboxMessage.enqueue(ev)),
      manager,
    );

    this.logger.log('Wager transaction processed', {
      correlationId,
      transactionId: tx.id,
      walletId: wallet.id,
      providerId: tx.providerId,
    });

    return {
      transactionId: tx.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
    };
  }

  private async validateReferenceTransaction(
    tx: WagerTransaction,
    referenceTx: WagerTransaction,
    manager: EntityManager,
  ): Promise<FailureCode | undefined> {
    if (
      referenceTx.providerId !== tx.providerId ||
      referenceTx.playerId !== tx.playerId ||
      referenceTx.walletId !== tx.walletId ||
      referenceTx.money.currency !== tx.money.currency ||
      referenceTx.roundId !== tx.roundId
    ) {
      return FailureCode.ReferenceMismatch;
    }

    if (referenceTx.status !== WagerTransactionStatus.Processed) {
      return FailureCode.ReferenceNotProcessed;
    }

    if (tx.kind === WagerTransactionKind.Win) {
      if (referenceTx.kind !== WagerTransactionKind.Bet) {
        return FailureCode.InvalidReferenceKind;
      }
      return undefined;
    }

    if (tx.kind === WagerTransactionKind.Refund) {
      if (referenceTx.kind !== WagerTransactionKind.Bet) {
        return FailureCode.InvalidReferenceKind;
      }
    }

    if (tx.kind === WagerTransactionKind.Rollback) {
      const allowedKinds = [
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
      ];
      if (!allowedKinds.includes(referenceTx.kind)) {
        return FailureCode.InvalidReferenceKind;
      }
    }

    if (!tx.money.equals(referenceTx.money)) {
      return FailureCode.AmountMismatch;
    }

    const existingReversal =
      await this.wagerTransactionRepository.findProcessedReversalForReference(
        referenceTx.id,
        tx.kind,
        manager,
      );
    if (existingReversal && existingReversal.id !== tx.id) {
      return FailureCode.DuplicateReversal;
    }

    return undefined;
  }

  private async rejectAndPersist(
    tx: WagerTransaction,
    wallet: Wallet,
    failureCode: FailureCode,
    correlationId: string,
    now: Date,
    manager: EntityManager,
  ): Promise<SubmitWagerTransactionResponse> {
    tx.reject(failureCode, wallet.balance, now);
    await this.wagerTransactionRepository.save(tx, manager);

    const rejectedEvent = WagerTransactionRejected.from(tx, failureCode, {
      eventId: uuidv4(),
      correlationId,
      occurredAt: now,
    });
    await this.outboxRepository.saveMany(
      [OutboxMessage.enqueue(rejectedEvent)],
      manager,
    );

    this.logger.warn('Wager transaction rejected', {
      correlationId,
      transactionId: tx.id,
      walletId: wallet.id,
      providerId: tx.providerId,
      failureCode,
    });

    return {
      transactionId: tx.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
      failureCode,
    };
  }

  async retryPendingReferences(batchSize = 25): Promise<number> {
    const now = new Date();
    return this.dataSource.transaction(async (manager) => {
      const pendingList =
        await this.wagerTransactionRepository.findDuePendingReferencesForUpdate(
          batchSize,
          now,
          manager,
        );

      for (const tx of pendingList) {
        this.metricsService.incrementRetries();
        const walletEntity = await manager
          .getRepository(WalletEntity)
          .createQueryBuilder('wallet')
          .setLock('pessimistic_write')
          .where('wallet.id = :id', { id: tx.walletId })
          .getOne();

        if (!walletEntity) {
          continue;
        }
        const wallet = walletEntity.toDomain();
        const refTx = tx.referenceExternalTransactionId
          ? await this.wagerTransactionRepository.findByProviderAndExternalId(
              tx.providerId,
              tx.referenceExternalTransactionId,
              manager,
            )
          : undefined;

        if (refTx) {
          await this.applyTransactionOnLockedWallet(
            tx,
            wallet,
            uuidv4(),
            manager,
          );
          continue;
        }

        const ageMs = now.getTime() - tx.createdAt.getTime();
        if (
          tx.retryAttempts >= EnvProvider.PENDING_REF_MAX_ATTEMPTS ||
          ageMs >= EnvProvider.PENDING_REF_TTL_MS
        ) {
          await this.rejectAndPersist(
            tx,
            wallet,
            FailureCode.ReferenceNotFound,
            uuidv4(),
            now,
            manager,
          );
          this.metricsService.incrementTransactionStatus(
            WagerTransactionStatus.Rejected,
          );
        } else {
          tx.markPendingReference(now);
          await this.wagerTransactionRepository.save(tx, manager);
        }
      }

      return pendingList.length;
    });
  }

  async getTransactionById(
    transactionId: string,
  ): Promise<WagerTransactionResponse> {
    const tx = await this.wagerTransactionRepository.findById(transactionId);
    if (!tx) {
      throw new EntityNotFoundError('WagerTransaction', transactionId);
    }
    return this.mapToResponse(tx);
  }

  async getTransactionByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransactionResponse> {
    const tx =
      await this.wagerTransactionRepository.findByProviderAndExternalId(
        providerId,
        externalTransactionId,
      );
    if (!tx) {
      throw new EntityNotFoundError(
        'WagerTransaction',
        `${providerId}:${externalTransactionId}`,
      );
    }
    return this.mapToResponse(tx);
  }

  private mapToResponse(tx: WagerTransaction): WagerTransactionResponse {
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      status: tx.status,
      referenceExternalTransactionId: tx.referenceExternalTransactionId,
      referenceTransactionId: tx.referenceTransactionId,
      failureCode: tx.failureCode,
      balance: tx.observedBalance ? tx.observedBalance.toJSON() : undefined,
      createdAt: tx.createdAt.toISOString(),
      processedAt: tx.processedAt ? tx.processedAt.toISOString() : undefined,
    };
  }
}
```

Create `src/app/wagering/controllers/wagering.controller.ts`:
```typescript
import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ApiHeader, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { CorrelatedRequest } from '../../../common/interceptors/request.interceptor';
import { SubmitWagerTransactionForm } from '../forms/submit-wager-transaction.form';
import { SubmitWagerTransactionResponse } from '../responses/submit-wager-transaction.response';
import { WagerTransactionResponse } from '../responses/wager-transaction.response';
import { WageringService } from '../services/wagering.service';

@ApiTags('Wagering')
@UseGuards(ProviderAuthGuard)
@Controller('wagering/transactions')
export class WageringController {
  constructor(private readonly wageringService: WageringService) {}

  @ApiHeader({
    name: 'Idempotency-Key',
    required: true,
    description: 'Mandatory idempotency key (e.g. provider-a:transaction-123)',
  })
  @ApiResponse({ status: 200, type: SubmitWagerTransactionResponse })
  @ApiResponse({ status: 202, type: SubmitWagerTransactionResponse })
  @ApiResponse({ status: 422, type: SubmitWagerTransactionResponse })
  @Post()
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() form: SubmitWagerTransactionForm,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ): Promise<SubmitWagerTransactionResponse> {
    if (!idempotencyKey || idempotencyKey.trim().length === 0) {
      throw new BadRequestException('Idempotency-Key header is required');
    }

    const result = await this.wageringService.submitTransaction({
      idempotencyKey: idempotencyKey.trim(),
      providerId: form.providerId,
      externalTransactionId: form.externalTransactionId,
      playerId: form.playerId,
      walletId: form.walletId,
      roundId: form.roundId,
      gameId: form.gameId,
      kind: form.kind,
      money: form.money,
      referenceExternalTransactionId: form.referenceExternalTransactionId,
      correlationId: req.correlationId,
    });

    if (result.status === WagerTransactionStatus.PendingReference) {
      res.status(HttpStatus.ACCEPTED);
    } else if (result.status === WagerTransactionStatus.Rejected) {
      res.status(HttpStatus.UNPROCESSABLE_ENTITY);
    } else {
      res.status(HttpStatus.OK);
    }

    return result;
  }

  @ApiResponse({ status: 200, type: WagerTransactionResponse })
  @Get(':transactionId')
  async getTransactionById(
    @Param('transactionId', new ParseUUIDPipe()) transactionId: string,
  ): Promise<WagerTransactionResponse> {
    return this.wageringService.getTransactionById(transactionId);
  }
}
```

Create `src/app/wagering/controllers/provider-wagering.controller.ts`:
```typescript
import { Controller, Get, Param, UseGuards } from '@nestjs/common';
import { ApiResponse, ApiTags } from '@nestjs/swagger';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { WagerTransactionResponse } from '../responses/wager-transaction.response';
import { WageringService } from '../services/wagering.service';

@ApiTags('Provider Wagering')
@UseGuards(ProviderAuthGuard)
@Controller('providers/:providerId/wagering/transactions')
export class ProviderWageringController {
  constructor(private readonly wageringService: WageringService) {}

  @ApiResponse({ status: 200, type: WagerTransactionResponse })
  @Get(':externalTransactionId')
  async getByProviderAndExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionResponse> {
    return this.wageringService.getTransactionByProviderAndExternalId(
      providerId,
      externalTransactionId,
    );
  }
}
```

Create `src/common/filters/domain-exception.filter.ts`:
```typescript
import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { Response } from 'express';
import {
  CurrencyMismatchError,
  DomainError,
  DuplicateWalletError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidMoneyError,
} from '../domain/domain.error';

@Catch()
export class DomainExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    if (exception instanceof HttpException) {
      response
        .status(exception.getStatus())
        .json(exception.getResponse());
      return;
    }

    if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof DuplicateWalletError
    ) {
      response.status(HttpStatus.CONFLICT).json({
        statusCode: HttpStatus.CONFLICT,
        error: 'Conflict',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (exception instanceof EntityNotFoundError) {
      response.status(HttpStatus.NOT_FOUND).json({
        statusCode: HttpStatus.NOT_FOUND,
        error: 'Not Found',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (exception instanceof InvalidMoneyError) {
      response.status(HttpStatus.BAD_REQUEST).json({
        statusCode: HttpStatus.BAD_REQUEST,
        error: 'Bad Request',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (
      exception instanceof CurrencyMismatchError ||
      exception instanceof InsufficientFundsError ||
      exception instanceof DomainError
    ) {
      const status =
        exception.code === 'INVALID_CURSOR' ||
        exception.code === 'FORBIDDEN_TRANSACTION_KIND' ||
        exception.code === 'MISSING_REFERENCE_EXTERNAL_ID' ||
        exception.code === 'INVALID_TRANSACTION_AMOUNT'
          ? HttpStatus.BAD_REQUEST
          : HttpStatus.UNPROCESSABLE_ENTITY;

      response.status(status).json({
        statusCode: status,
        error:
          status === HttpStatus.BAD_REQUEST
            ? 'Bad Request'
            : 'Unprocessable Entity',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    response.status(HttpStatus.SERVICE_UNAVAILABLE).json({
      statusCode: HttpStatus.SERVICE_UNAVAILABLE,
      error: 'Service Unavailable',
      code: 'TRANSIENT_INFRASTRUCTURE_ERROR',
      message:
        exception instanceof Error
          ? exception.message
          : 'Unexpected infrastructure error',
    });
  }
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `bun run test:unit src/app/wagering/services/wagering.service.spec.ts`
Expected: PASS


---

### Task 7: Messaging Module & SQS Consumer (`SqsService`, `OutboxService`, `WagerTransactionConsumer`)

**Files:**
- Create: `src/app/messaging/dtos/sqs-wager-message.dto.ts`
- Create: `src/app/messaging/services/sqs.service.ts`
- Create: `src/app/messaging/services/outbox.service.ts`
- Create: `src/app/wagering/consumers/wager-transaction.consumer.ts`
- Test: `src/app/wagering/consumers/wager-transaction.consumer.spec.ts`

**Interfaces:**
- Consumes: `@aws-sdk/client-sqs`, `WageringService`, `OutboxRepository`, `MetricsService`, `AppLogger`, `EnvProvider`
- Produces: `SqsService`, `OutboxService`, `WagerTransactionConsumer`

- [ ] **Step 1: Write the failing unit tests for `WagerTransactionConsumer` and `OutboxService`**

Create `src/app/wagering/consumers/wager-transaction.consumer.spec.ts`:
```typescript
import { WagerTransactionConsumer } from './wager-transaction.consumer';
import { WageringService } from '../services/wagering.service';
import { SqsService } from '../../messaging/services/sqs.service';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { AppLogger } from '../../../common/logger/app.logger';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { IdempotencyConflictError } from '../../../common/domain/domain.error';

describe('WagerTransactionConsumer Unit Tests', () => {
  let consumer: WagerTransactionConsumer;
  let wageringService: jest.Mocked<WageringService>;
  let sqsService: jest.Mocked<SqsService>;
  let metricsService: MetricsService;
  let logger: AppLogger;

  beforeEach(() => {
    wageringService = {
      submitTransaction: jest.fn(),
    } as unknown as jest.Mocked<WageringService>;

    sqsService = {
      receiveMessages: jest.fn().mockResolvedValue([]),
      deleteMessage: jest.fn().mockResolvedValue(undefined),
      changeVisibility: jest.fn().mockResolvedValue(undefined),
      sendToDlq: jest.fn().mockResolvedValue(undefined),
    } as unknown as jest.Mocked<SqsService>;

    metricsService = new MetricsService();
    logger = new AppLogger();

    consumer = new WagerTransactionConsumer(
      wageringService,
      sqsService,
      metricsService,
      logger,
    );
  });

  const validSqsBody = JSON.stringify({
    messageId: 'msg-123',
    type: 'WagerTransactionRequested',
    occurredAt: '2026-07-29T15:00:00.000Z',
    data: {
      providerId: 'provider-a',
      externalTransactionId: 'transaction-123',
      idempotencyKey: 'provider-a:transaction-123',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
      roundId: 'round-987',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    },
  });

  it('acks SQS message only after WageringService commits successfully', async () => {
    wageringService.submitTransaction.mockResolvedValueOnce({
      transactionId: 'tx-1',
      status: WagerTransactionStatus.Processed,
      balance: { amount: '975.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    await consumer.handleRawSqsMessage({
      MessageId: 'sqs-id-1',
      ReceiptHandle: 'receipt-1',
      Body: validSqsBody,
      Attributes: { ApproximateReceiveCount: '1' },
    });

    expect(wageringService.submitTransaction).toHaveBeenCalledTimes(1);
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      expect.any(String),
      'receipt-1',
    );
  });

  it('acks SQS message when business rule rejects transaction or idempotency conflicts (terminal error)', async () => {
    wageringService.submitTransaction.mockRejectedValueOnce(
      new IdempotencyConflictError('provider-a:transaction-123'),
    );

    await consumer.handleRawSqsMessage({
      MessageId: 'sqs-id-2',
      ReceiptHandle: 'receipt-2',
      Body: validSqsBody,
      Attributes: { ApproximateReceiveCount: '1' },
    });

    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      expect.any(String),
      'receipt-2',
    );
  });

  it('routes permanently malformed message or exhausted retries to DLQ and acks original queue', async () => {
    await consumer.handleRawSqsMessage({
      MessageId: 'sqs-bad',
      ReceiptHandle: 'receipt-bad',
      Body: '{bad-json',
      Attributes: { ApproximateReceiveCount: '1' },
    });

    expect(sqsService.sendToDlq).toHaveBeenCalledTimes(1);
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      expect.any(String),
      'receipt-bad',
    );
    expect(metricsService.getSnapshot().dlqMessages).toBe(1);
  });

  it('applies exponential visibility timeout backoff on transient error below max receive count', async () => {
    wageringService.submitTransaction.mockRejectedValueOnce(
      new Error('Connection terminated unexpectedly'),
    );

    await consumer.handleRawSqsMessage({
      MessageId: 'sqs-transient',
      ReceiptHandle: 'receipt-transient',
      Body: validSqsBody,
      Attributes: { ApproximateReceiveCount: '1' },
    });

    expect(sqsService.changeVisibility).toHaveBeenCalledWith(
      expect.any(String),
      'receipt-transient',
      expect.any(Number),
    );
    expect(sqsService.deleteMessage).not.toHaveBeenCalled();
    expect(metricsService.getSnapshot().retries).toBe(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/app/wagering/consumers/wager-transaction.consumer.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement `SqsWagerMessageDto`, `SqsService`, `OutboxService`, and `WagerTransactionConsumer`**

Create `src/app/messaging/dtos/sqs-wager-message.dto.ts`:
```typescript
import { MoneyProps } from '../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';

export interface SqsWagerMessageDataDto {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey?: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface SqsWagerMessageEnvelopeDto {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  correlationId?: string;
  data: SqsWagerMessageDataDto;
}
```

Create `src/app/messaging/services/sqs.service.ts`:
```typescript
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { Injectable } from '@nestjs/common';
import { EnvProvider } from '../../../common/providers/env.provider';
import { QUEUES } from '../../../common/constants/queues.constant';

@Injectable()
export class SqsService {
  private readonly client: SQSClient;

  constructor() {
    this.client = new SQSClient({
      region: EnvProvider.AWS_REGION,
      endpoint: EnvProvider.AWS_ENDPOINT,
      credentials: {
        accessKeyId: EnvProvider.AWS_ACCESS_KEY_ID,
        secretAccessKey: EnvProvider.AWS_SECRET_ACCESS_KEY,
      },
    });
  }

  async ensureQueuesExist(): Promise<void> {
    const dlqUrl = await this.ensureFifoQueue(
      QUEUES.WAGER_TRANSACTIONS_DLQ_FIFO,
    );
    const dlqAttrs = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ['QueueArn'],
      }),
    );
    const dlqArn = dlqAttrs.Attributes?.QueueArn;

    const mainAttrs: Record<string, string> = {
      FifoQueue: 'true',
      ContentBasedDeduplication: 'false',
      VisibilityTimeout: '30',
    };
    if (dlqArn) {
      mainAttrs.RedrivePolicy = JSON.stringify({
        deadLetterTargetArn: dlqArn,
        maxReceiveCount: String(EnvProvider.SQS_MAX_RECEIVE_COUNT),
      });
    }

    const mainUrl = await this.ensureFifoQueue(
      QUEUES.WAGER_TRANSACTIONS_FIFO,
      mainAttrs,
    );
    const eventsUrl = await this.ensureFifoQueue(
      QUEUES.INTEGRATION_EVENTS_FIFO,
    );

    EnvProvider.SQS_WAGER_DLQ_URL = dlqUrl;
    EnvProvider.SQS_WAGER_QUEUE_URL = mainUrl;
    EnvProvider.SQS_EVENTS_QUEUE_URL = eventsUrl;
  }

  private async ensureFifoQueue(
    queueName: string,
    attributes: Record<string, string> = {
      FifoQueue: 'true',
      ContentBasedDeduplication: 'false',
    },
  ): Promise<string> {
    try {
      const existing = await this.client.send(
        new GetQueueUrlCommand({ QueueName: queueName }),
      );
      if (existing.QueueUrl) {
        return existing.QueueUrl;
      }
    } catch {
      const created = await this.client.send(
        new CreateQueueCommand({
          QueueName: queueName,
          Attributes: attributes,
        }),
      );
      if (created.QueueUrl) {
        return created.QueueUrl;
      }
    }
    throw new Error(`Failed to resolve SQS queue ${queueName}`);
  }

  async checkReadiness(): Promise<boolean> {
    await this.client.send(new ListQueuesCommand({ MaxResults: 1 }));
    return true;
  }

  async receiveMessages(
    queueUrl: string,
    maxMessages = 10,
    waitTimeSeconds = 1,
  ): Promise<Message[]> {
    const response = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        AttributeNames: ['All'],
      }),
    );
    return response.Messages ?? [];
  }

  async deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeVisibility(
    queueUrl: string,
    receiptHandle: string,
    visibilityTimeoutSeconds: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeoutSeconds,
      }),
    );
  }

  async publishFifoMessage(params: {
    queueUrl: string;
    body: string;
    messageGroupId: string;
    deduplicationId: string;
  }): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: params.queueUrl,
        MessageBody: params.body,
        MessageGroupId: params.messageGroupId,
        MessageDeduplicationId: params.deduplicationId,
      }),
    );
  }

  async sendToDlq(params: {
    body: string;
    messageGroupId: string;
    deduplicationId: string;
    reason: string;
  }): Promise<void> {
    const dlqPayload = JSON.stringify({
      dlqReason: params.reason,
      failedAt: new Date().toISOString(),
      originalBody: params.body,
    });
    await this.publishFifoMessage({
      queueUrl: EnvProvider.SQS_WAGER_DLQ_URL,
      body: dlqPayload,
      messageGroupId: params.messageGroupId,
      deduplicationId: params.deduplicationId,
    });
  }
}
```

Create `src/app/messaging/services/outbox.service.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { OutboxRepository } from '../repositories/outbox.repository';
import { SqsService } from './sqs.service';

@Injectable()
export class OutboxService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly outboxRepository: OutboxRepository,
    private readonly sqsService: SqsService,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  async publishPendingBatch(batchSize = 50): Promise<number> {
    const now = new Date();
    let publishedCount = 0;

    await this.dataSource.transaction(async (manager) => {
      const dueMessages = await this.outboxRepository.claimDueBatchForUpdate(
        batchSize,
        now,
        manager,
      );

      for (const message of dueMessages) {
        try {
          await this.sqsService.publishFifoMessage({
            queueUrl: EnvProvider.SQS_EVENTS_QUEUE_URL,
            body: JSON.stringify(message.payload),
            messageGroupId: message.aggregateId,
            deduplicationId: message.id,
          });
          message.markPublished(new Date());
          await this.outboxRepository.save(message, manager);
          publishedCount += 1;
        } catch (error: unknown) {
          message.scheduleRetry(new Date());
          await this.outboxRepository.save(message, manager);
          this.metricsService.incrementRetries();
          this.logger.warn('Failed to publish outbox event, scheduled retry', {
            eventType: message.eventType,
            aggregateId: message.aggregateId,
            attempts: message.attempts,
            error: error instanceof Error ? error.message : 'Unknown error',
          });
        }
      }
    });

    const oldestUnpublished =
      await this.outboxRepository.getOldestUnpublishedOccurredAt();
    if (oldestUnpublished) {
      this.metricsService.setOutboxLagMs(
        Date.now() - oldestUnpublished.getTime(),
      );
    } else {
      this.metricsService.setOutboxLagMs(0);
    }

    return publishedCount;
  }
}
```

Create `src/app/wagering/consumers/wager-transaction.consumer.ts`:
```typescript
import { Message } from '@aws-sdk/client-sqs';
import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { createHash } from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { QUEUES } from '../../../common/constants/queues.constant';
import {
  DomainError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InvalidMoneyError,
} from '../../../common/domain/domain.error';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { SqsWagerMessageEnvelopeDto } from '../../messaging/dtos/sqs-wager-message.dto';
import { SqsService } from '../../messaging/services/sqs.service';
import { WageringService } from '../services/wagering.service';

@Injectable()
export class WagerTransactionConsumer
  implements OnModuleInit, OnApplicationShutdown
{
  private isShuttingDown = false;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private activeMessages = new Map<
    string,
    { queueUrl: string; receiptHandle: string }
  >();
  private inFlightPromises = new Set<Promise<void>>();

  constructor(
    private readonly wageringService: WageringService,
    private readonly sqsService: SqsService,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  async onModuleInit(): Promise<void> {
    if (!EnvProvider.SQS_CONSUMER_ENABLED) {
      return;
    }
    try {
      await this.sqsService.ensureQueuesExist();
    } catch (error: unknown) {
      this.logger.warn('SQS queue initialization skipped or deferred', {
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
    this.scheduleNextPoll(100);
  }

  async onApplicationShutdown(): Promise<void> {
    this.isShuttingDown = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
    }

    await Promise.race([
      Promise.allSettled(Array.from(this.inFlightPromises)),
      new Promise((resolve) => setTimeout(resolve, 2000)),
    ]);

    for (const [, active] of this.activeMessages) {
      try {
        await this.sqsService.changeVisibility(
          active.queueUrl,
          active.receiptHandle,
          0,
        );
      } catch {
        void 0;
      }
    }
    this.activeMessages.clear();
  }

  private scheduleNextPoll(delayMs: number): void {
    if (this.isShuttingDown || !EnvProvider.SQS_CONSUMER_ENABLED) {
      return;
    }
    this.pollTimer = setTimeout(async () => {
      await this.pollOnce();
      this.scheduleNextPoll(150);
    }, delayMs);
  }

  async pollOnce(): Promise<number> {
    if (this.isShuttingDown) {
      return 0;
    }
    try {
      const messages = await this.sqsService.receiveMessages(
        EnvProvider.SQS_WAGER_QUEUE_URL,
        10,
        0,
      );
      const tasks = messages.map((msg) => {
        const p = this.handleRawSqsMessage(msg).finally(() => {
          this.inFlightPromises.delete(p);
        });
        this.inFlightPromises.add(p);
        return p;
      });
      await Promise.all(tasks);
      return messages.length;
    } catch {
      return 0;
    }
  }

  async handleRawSqsMessage(msg: Message): Promise<void> {
    const queueUrl = EnvProvider.SQS_WAGER_QUEUE_URL;
    const receiptHandle = msg.ReceiptHandle;
    const sqsMessageId = msg.MessageId ?? uuidv4();
    if (!receiptHandle) {
      return;
    }

    this.activeMessages.set(sqsMessageId, { queueUrl, receiptHandle });

    try {
      const rawBody = msg.Body ?? '';
      let parsed: SqsWagerMessageEnvelopeDto;
      try {
        parsed = JSON.parse(rawBody) as SqsWagerMessageEnvelopeDto;
        if (
          !parsed ||
          parsed.type !== 'WagerTransactionRequested' ||
          !parsed.messageId ||
          !parsed.data ||
          !parsed.data.providerId ||
          !parsed.data.externalTransactionId ||
          !parsed.data.walletId ||
          !parsed.data.playerId ||
          !parsed.data.kind ||
          !parsed.data.money
        ) {
          throw new Error('Invalid WagerTransactionRequested envelope');
        }
      } catch (parseErr: unknown) {
        await this.routeToDlqAndAck(
          queueUrl,
          receiptHandle,
          rawBody,
          sqsMessageId,
          parseErr instanceof Error ? parseErr.message : 'Malformed message',
        );
        return;
      }

      const idempotencyKey =
        parsed.data.idempotencyKey ||
        `${parsed.data.providerId}:${parsed.data.externalTransactionId}`;
      const bodyHash = createHash('sha256').update(rawBody).digest('hex');

      await this.wageringService.submitTransaction({
        idempotencyKey,
        providerId: parsed.data.providerId,
        externalTransactionId: parsed.data.externalTransactionId,
        playerId: parsed.data.playerId,
        walletId: parsed.data.walletId,
        roundId: parsed.data.roundId,
        gameId: parsed.data.gameId,
        kind: parsed.data.kind,
        money: parsed.data.money,
        referenceExternalTransactionId:
          parsed.data.referenceExternalTransactionId,
        correlationId: parsed.correlationId ?? parsed.messageId,
        inboxContext: {
          consumerName: QUEUES.WAGER_CONSUMER_NAME,
          messageId: parsed.messageId,
          payloadHash: bodyHash,
        },
      });

      await this.sqsService.deleteMessage(queueUrl, receiptHandle);
    } catch (error: unknown) {
      await this.handleProcessingError(msg, queueUrl, receiptHandle, error);
    } finally {
      this.activeMessages.delete(sqsMessageId);
    }
  }

  private async handleProcessingError(
    msg: Message,
    queueUrl: string,
    receiptHandle: string,
    error: unknown,
  ): Promise<void> {
    if (
      error instanceof IdempotencyConflictError ||
      error instanceof InvalidMoneyError ||
      error instanceof DomainError
    ) {
      if (!(error instanceof EntityNotFoundError)) {
        await this.sqsService.deleteMessage(queueUrl, receiptHandle);
        return;
      }
    }

    const receiveCount = Number(
      msg.Attributes?.ApproximateReceiveCount ?? '1',
    );
    const maxReceives = EnvProvider.SQS_MAX_RECEIVE_COUNT;

    if (receiveCount >= maxReceives) {
      await this.routeToDlqAndAck(
        queueUrl,
        receiptHandle,
        msg.Body ?? '',
        msg.MessageId ?? uuidv4(),
        error instanceof Error ? error.message : 'Max receive count exceeded',
      );
      return;
    }

    this.metricsService.incrementRetries();
    const backoffSeconds = Math.min(30, Math.pow(2, receiveCount));
    await this.sqsService.changeVisibility(
      queueUrl,
      receiptHandle,
      backoffSeconds,
    );
  }

  private async routeToDlqAndAck(
    queueUrl: string,
    receiptHandle: string,
    rawBody: string,
    dedupId: string,
    reason: string,
  ): Promise<void> {
    await this.sqsService.sendToDlq({
      body: rawBody,
      messageGroupId: 'dlq-group',
      deduplicationId: `${dedupId}-${Date.now()}`,
      reason,
    });
    await this.sqsService.deleteMessage(queueUrl, receiptHandle);
    this.metricsService.incrementDlqMessages();
    this.logger.error('Message routed to DLQ', undefined, {
      messageId: dedupId,
      reason,
    });
  }
}
```

- [ ] **Step 4: Run the consumer unit tests to verify they pass**

Run: `bun run test:unit src/app/wagering/consumers/wager-transaction.consumer.spec.ts`
Expected: PASS


---

### Task 8: Scheduled Workers (`OutboxPublisherCron`, `PendingReferenceCron`) & Feature Modules (`MessagingModule`, `WageringModule`)

**Files:**
- Create: `src/app/messaging/cron/outbox-publisher.cron.ts`
- Create: `src/app/wagering/cron/pending-reference.cron.ts`
- Create: `src/app/messaging/messaging.module.ts`
- Create: `src/app/wagering/wagering.module.ts`
- Test: `src/app/messaging/cron/workers.cron.spec.ts`

**Interfaces:**
- Consumes: `OutboxService`, `WageringService`, `EnvProvider`
- Produces: `OutboxPublisherCron`, `PendingReferenceCron`, `MessagingModule` (exports `OutboxService`, `SqsService`), `WageringModule` (exports `WageringService`)

- [ ] **Step 1: Write the failing unit test for `OutboxPublisherCron` and `PendingReferenceCron`**

Create `src/app/messaging/cron/workers.cron.spec.ts`:
```typescript
import { OutboxPublisherCron } from './outbox-publisher.cron';
import { PendingReferenceCron } from '../../wagering/cron/pending-reference.cron';
import { OutboxService } from '../services/outbox.service';
import { WageringService } from '../../wagering/services/wagering.service';
import { EnvProvider } from '../../../common/providers/env.provider';

describe('Scheduled Workers (OutboxPublisherCron & PendingReferenceCron)', () => {
  beforeEach(() => {
    EnvProvider.WORKERS_ENABLED = true;
  });

  it('triggers OutboxService.publishPendingBatch when WORKERS_ENABLED is true', async () => {
    const outboxService = {
      publishPendingBatch: jest.fn().mockResolvedValue(3),
    } as unknown as jest.Mocked<OutboxService>;

    const cron = new OutboxPublisherCron(outboxService);
    await cron.handleOutboxPublish();

    expect(outboxService.publishPendingBatch).toHaveBeenCalledTimes(1);
  });

  it('triggers WageringService.retryPendingReferences when WORKERS_ENABLED is true', async () => {
    const wageringService = {
      retryPendingReferences: jest.fn().mockResolvedValue(2),
    } as unknown as jest.Mocked<WageringService>;

    const cron = new PendingReferenceCron(wageringService);
    await cron.handlePendingReferences();

    expect(wageringService.retryPendingReferences).toHaveBeenCalledTimes(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/app/messaging/cron/workers.cron.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement `OutboxPublisherCron`, `PendingReferenceCron`, `MessagingModule`, and `WageringModule`**

Create `src/app/messaging/cron/outbox-publisher.cron.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { EnvProvider } from '../../../common/providers/env.provider';
import { OutboxService } from '../services/outbox.service';

@Injectable()
export class OutboxPublisherCron {
  private isRunning = false;

  constructor(private readonly outboxService: OutboxService) {}

  @Interval(500)
  async handleOutboxPublish(): Promise<void> {
    if (!EnvProvider.WORKERS_ENABLED || this.isRunning) {
      return;
    }
    this.isRunning = true;
    try {
      await this.outboxService.publishPendingBatch(50);
    } finally {
      this.isRunning = false;
    }
  }
}
```

Create `src/app/wagering/cron/pending-reference.cron.ts`:
```typescript
import { Injectable } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { EnvProvider } from '../../../common/providers/env.provider';
import { WageringService } from '../services/wagering.service';

@Injectable()
export class PendingReferenceCron {
  private isRunning = false;

  constructor(private readonly wageringService: WageringService) {}

  @Interval(500)
  async handlePendingReferences(): Promise<void> {
    if (!EnvProvider.WORKERS_ENABLED || this.isRunning) {
      return;
    }
    this.isRunning = true;
    try {
      await this.wageringService.retryPendingReferences(25);
    } finally {
      this.isRunning = false;
    }
  }
}
```

Create `src/app/messaging/messaging.module.ts`:
```typescript
import { Module } from '@nestjs/common';
import { AppLogger } from '../../common/logger/app.logger';
import { MetricsService } from '../../common/metrics/metrics.service';
import { OutboxPublisherCron } from './cron/outbox-publisher.cron';
import { InboxRepository } from './repositories/inbox.repository';
import { OutboxRepository } from './repositories/outbox.repository';
import { OutboxService } from './services/outbox.service';
import { SqsService } from './services/sqs.service';

@Module({
  providers: [
    SqsService,
    OutboxService,
    OutboxRepository,
    InboxRepository,
    OutboxPublisherCron,
    MetricsService,
    AppLogger,
  ],
  exports: [OutboxService, SqsService],
})
export class MessagingModule {}
```

Create `src/app/wagering/wagering.module.ts`:
```typescript
import { Module } from '@nestjs/common';
import { AppLogger } from '../../common/logger/app.logger';
import { MetricsService } from '../../common/metrics/metrics.service';
import { MessagingModule } from '../messaging/messaging.module';
import { InboxRepository } from '../messaging/repositories/inbox.repository';
import { OutboxRepository } from '../messaging/repositories/outbox.repository';
import { WagerTransactionConsumer } from './consumers/wager-transaction.consumer';
import { ProviderWageringController } from './controllers/provider-wagering.controller';
import { WageringController } from './controllers/wagering.controller';
import { PendingReferenceCron } from './cron/pending-reference.cron';
import { WagerTransactionRepository } from './repositories/wager-transaction.repository';
import { WageringService } from './services/wagering.service';

@Module({
  imports: [MessagingModule],
  controllers: [WageringController, ProviderWageringController],
  providers: [
    WageringService,
    WagerTransactionRepository,
    OutboxRepository,
    InboxRepository,
    WagerTransactionConsumer,
    PendingReferenceCron,
    MetricsService,
    AppLogger,
  ],
  exports: [WageringService],
})
export class WageringModule {}
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `bun run test:unit src/app/messaging/cron/workers.cron.spec.ts`
Expected: PASS


---

### Task 9: Health Checks (`GET /health/live`, `GET /health/ready`), Metrics Endpoint (`GET /metrics`), `AppModule` & `main.ts`

**Files:**
- Create: `src/app/health/responses/health.response.ts`
- Create: `src/app/health/services/health.service.ts`
- Create: `src/app/health/controllers/health.controller.ts`
- Create: `src/app/health/controllers/metrics.controller.ts`
- Create: `src/app/health/health.module.ts`
- Create: `src/app.module.ts`
- Create: `src/main.ts`
- Test: `src/app/health/services/health.service.spec.ts`

**Interfaces:**
- Consumes: `DataSource`, `SqsService`, `MetricsService`, `WalletModule`, `WageringModule`, `MessagingModule`
- Produces: `HealthModule` (exports `HealthService`), `AppModule`, `main.ts`

- [ ] **Step 1: Write the failing unit test for `HealthService`**

Create `src/app/health/services/health.service.spec.ts`:
```typescript
import { DataSource } from 'typeorm';
import { HealthService } from './health.service';
import { SqsService } from '../../messaging/services/sqs.service';
import { ServiceUnavailableException } from '@nestjs/common';

describe('HealthService Unit Tests', () => {
  it('returns live status unconditionally and ready status when Postgres and SQS are reachable', async () => {
    const dataSource = {
      query: jest.fn().mockResolvedValue([{ '?column?': 1 }]),
    } as unknown as DataSource;
    const sqsService = {
      checkReadiness: jest.fn().mockResolvedValue(true),
    } as unknown as SqsService;

    const service = new HealthService(dataSource, sqsService);

    expect(service.checkLiveness().status).toBe('ok');
    const readiness = await service.checkReadiness();
    expect(readiness.status).toBe('ok');
    expect(readiness.postgres).toBe('up');
    expect(readiness.sqs).toBe('up');
  });

  it('throws ServiceUnavailableException on readiness check when Postgres or SQS fails', async () => {
    const dataSource = {
      query: jest.fn().mockRejectedValue(new Error('DB down')),
    } as unknown as DataSource;
    const sqsService = {
      checkReadiness: jest.fn().mockResolvedValue(true),
    } as unknown as SqsService;

    const service = new HealthService(dataSource, sqsService);

    await expect(service.checkReadiness()).rejects.toThrow(
      ServiceUnavailableException,
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bun run test:unit src/app/health/services/health.service.spec.ts`
Expected: FAIL with module not found error

- [ ] **Step 3: Implement `HealthModule`, `AppModule`, and `main.ts`**

Create `src/app/health/responses/health.response.ts`:
```typescript
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class HealthResponse {
  @ApiProperty({ example: 'ok' })
  status: string;

  @ApiPropertyOptional({ example: 'up' })
  postgres?: string;

  @ApiPropertyOptional({ example: 'up' })
  sqs?: string;
}
```

Create `src/app/health/services/health.service.ts`:
```typescript
import { Injectable, ServiceUnavailableException } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SqsService } from '../../messaging/services/sqs.service';
import { HealthResponse } from '../responses/health.response';

@Injectable()
export class HealthService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly sqsService: SqsService,
  ) {}

  checkLiveness(): HealthResponse {
    return { status: 'ok' };
  }

  async checkReadiness(): Promise<HealthResponse> {
    let postgresStatus = 'down';
    let sqsStatus = 'down';

    try {
      await this.dataSource.query('SELECT 1');
      postgresStatus = 'up';
    } catch {
      postgresStatus = 'down';
    }

    try {
      await this.sqsService.checkReadiness();
      sqsStatus = 'up';
    } catch {
      sqsStatus = 'down';
    }

    if (postgresStatus !== 'up' || sqsStatus !== 'up') {
      throw new ServiceUnavailableException({
        status: 'error',
        postgres: postgresStatus,
        sqs: sqsStatus,
      });
    }

    return {
      status: 'ok',
      postgres: postgresStatus,
      sqs: sqsStatus,
    };
  }
}
```

Create `src/app/health/controllers/health.controller.ts`:
```typescript
import { Controller, Get } from '@nestjs/common';
import { ApiResponse, ApiTags } from '@nestjs/swagger';
import { HealthResponse } from '../responses/health.response';
import { HealthService } from '../services/health.service';

@ApiTags('Health')
@Controller('health')
export class HealthController {
  constructor(private readonly healthService: HealthService) {}

  @ApiResponse({ status: 200, type: HealthResponse })
  @Get('live')
  getLiveness(): HealthResponse {
    return this.healthService.checkLiveness();
  }

  @ApiResponse({ status: 200, type: HealthResponse })
  @Get('ready')
  async getReadiness(): Promise<HealthResponse> {
    return this.healthService.checkReadiness();
  }
}
```

Create `src/app/health/controllers/metrics.controller.ts`:
```typescript
import { Controller, Get } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import {
  MetricsService,
  MetricsSnapshot,
} from '../../../common/metrics/metrics.service';

@ApiTags('Observability')
@Controller('metrics')
export class MetricsController {
  constructor(private readonly metricsService: MetricsService) {}

  @Get()
  getMetrics(): MetricsSnapshot {
    return this.metricsService.getSnapshot();
  }
}
```

Create `src/app/health/health.module.ts`:
```typescript
import { Module } from '@nestjs/common';
import { MetricsService } from '../../common/metrics/metrics.service';
import { MessagingModule } from '../messaging/messaging.module';
import { HealthController } from './controllers/health.controller';
import { MetricsController } from './controllers/metrics.controller';
import { HealthService } from './services/health.service';

@Module({
  imports: [MessagingModule],
  controllers: [HealthController, MetricsController],
  providers: [HealthService, MetricsService],
  exports: [HealthService],
})
export class HealthModule {}
```

Create `src/app.module.ts`:
```typescript
import { Global, Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HealthModule } from './app/health/health.module';
import { MessagingModule } from './app/messaging/messaging.module';
import { WageringModule } from './app/wagering/wagering.module';
import { WalletModule } from './app/wallet/wallet.module';
import { dataSourceOptions } from './common/database/data-source';
import { AppLogger } from './common/logger/app.logger';
import { MetricsService } from './common/metrics/metrics.service';

@Global()
@Module({
  imports: [
    TypeOrmModule.forRoot(dataSourceOptions),
    ScheduleModule.forRoot(),
    WalletModule,
    MessagingModule,
    WageringModule,
    HealthModule,
  ],
  providers: [MetricsService, AppLogger],
  exports: [MetricsService, AppLogger],
})
export class AppModule {}
```

Create `src/main.ts`:
```typescript
import { EnvProvider } from './common/providers/env.provider';

EnvProvider.initialize();

import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { DataSource } from 'typeorm';
import { AppModule } from './app.module';
import { DomainExceptionFilter } from './common/filters/domain-exception.filter';
import { RequestInterceptor } from './common/interceptors/request.interceptor';
import { AppLogger } from './common/logger/app.logger';

async function bootstrap() {
  const logger = new AppLogger();
  const app = await NestFactory.create(AppModule, { logger });

  const dataSource = app.get(DataSource);
  await dataSource.runMigrations();

  const swaggerConfig = new DocumentBuilder()
    .setTitle('Jungle Gaming — Distributed Wagering Processor')
    .setVersion('1.0.0')
    .build();
  const document = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup('docs', app, document);

  app.enableCors();
  app.enableShutdownHooks();
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );
  app.useGlobalInterceptors(new RequestInterceptor());
  app.useGlobalFilters(new DomainExceptionFilter());

  await app.listen(EnvProvider.PORT);

  const shutdown = async () => {
    await app.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

bootstrap();
```

- [ ] **Step 4: Run all unit tests to verify they pass**

Run: `bun run test:unit`
Expected: PASS (all unit test suites passing)


---

### Task 10: Real Container Integration Tests (PostgreSQL + LocalStack SQS)

**Files:**
- Create: `test/helpers/test-app.helper.ts`
- Create: `test/integration/wagering.integration.spec.ts`

**Interfaces:**
- Consumes: `AppModule`, `DataSource`, `SqsService`, `OutboxService`, `WagerTransactionConsumer`, `WageringService`, `WalletService`, PostgreSQL container, LocalStack container
- Produces: `createTestInstance`, `resetDatabase`, `purgeSqsQueues`, full integration test suite verifying Section 13 Integration requirements

- [ ] **Step 1: Create `test/helpers/test-app.helper.ts` and `test/integration/wagering.integration.spec.ts`**

Create `test/helpers/test-app.helper.ts`:
```typescript
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { PurgeQueueCommand, SQSClient } from '@aws-sdk/client-sqs';
import { DataSource } from 'typeorm';
import { AppModule } from '../../src/app.module';
import { DomainExceptionFilter } from '../../src/common/filters/domain-exception.filter';
import { RequestInterceptor } from '../../src/common/interceptors/request.interceptor';
import { EnvProvider } from '../../src/common/providers/env.provider';
import { SqsService } from '../../src/app/messaging/services/sqs.service';

export interface TestInstance {
  app: INestApplication;
  dataSource: DataSource;
  sqsService: SqsService;
}

export async function createTestInstance(options?: {
  enableConsumer?: boolean;
  enableWorkers?: boolean;
}): Promise<TestInstance> {
  process.env.NODE_ENV = 'test';
  EnvProvider.SQS_CONSUMER_ENABLED = options?.enableConsumer ?? false;
  EnvProvider.WORKERS_ENABLED = options?.enableWorkers ?? false;

  const moduleRef = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();

  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );
  app.useGlobalInterceptors(new RequestInterceptor());
  app.useGlobalFilters(new DomainExceptionFilter());

  await app.init();

  const dataSource = app.get(DataSource);
  await dataSource.runMigrations();

  const sqsService = app.get(SqsService);
  await sqsService.ensureQueuesExist();

  return { app, dataSource, sqsService };
}

export async function resetDatabase(dataSource: DataSource): Promise<void> {
  await dataSource.query(`
    TRUNCATE TABLE
      outbox_messages,
      inbox_messages,
      wallet_ledger_entries,
      wager_transactions,
      wallets
    RESTART IDENTITY CASCADE;
  `);
}

export async function purgeSqsQueues(): Promise<void> {
  const client = new SQSClient({
    region: EnvProvider.AWS_REGION,
    endpoint: EnvProvider.AWS_ENDPOINT,
    credentials: {
      accessKeyId: EnvProvider.AWS_ACCESS_KEY_ID,
      secretAccessKey: EnvProvider.AWS_SECRET_ACCESS_KEY,
    },
  });

  for (const url of [
    EnvProvider.SQS_WAGER_QUEUE_URL,
    EnvProvider.SQS_WAGER_DLQ_URL,
    EnvProvider.SQS_EVENTS_QUEUE_URL,
  ]) {
    try {
      await client.send(new PurgeQueueCommand({ QueueUrl: url }));
    } catch {
      void 0;
    }
  }
}
```

Create `test/integration/wagering.integration.spec.ts`:
```typescript
import request from 'supertest';
import { v4 as uuidv4 } from 'uuid';
import {
  createTestInstance,
  purgeSqsQueues,
  resetDatabase,
  TestInstance,
} from '../helpers/test-app.helper';
import { OutboxService } from '../../src/app/messaging/services/outbox.service';
import { WagerTransactionConsumer } from '../../src/app/wagering/consumers/wager-transaction.consumer';
import { WalletService } from '../../src/app/wallet/services/wallet.service';
import { EnvProvider } from '../../src/common/providers/env.provider';

describe('Wagering Processor Integration Tests (Real PostgreSQL + LocalStack SQS)', () => {
  let instance: TestInstance;

  beforeAll(async () => {
    instance = await createTestInstance({
      enableConsumer: false,
      enableWorkers: false,
    });
  });

  beforeEach(async () => {
    await resetDatabase(instance.dataSource);
    await purgeSqsQueues();
  });

  afterAll(async () => {
    await instance.app.close();
  });

  it('enforces PostgreSQL schema constraints: non-negative balance, unique wallet, and immutable ledger trigger', async () => {
    const playerId = uuidv4();
    const createRes = await request(instance.app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = createRes.body.id as string;

    await request(instance.app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '50.00', currency: 'BRL' },
      })
      .expect(409);

    await expect(
      instance.dataSource.query(
        `UPDATE wallets SET balance_amount = -1.00 WHERE id = $1`,
        [walletId],
      ),
    ).rejects.toThrow();

    await expect(
      instance.dataSource.query(
        `UPDATE wallet_ledger_entries SET amount = 999.00 WHERE wallet_id = $1`,
        [walletId],
      ),
    ).rejects.toThrow(/immutable/);

    await expect(
      instance.dataSource.query(
        `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`,
        [walletId],
      ),
    ).rejects.toThrow(/immutable/);
  });

  it('guarantees atomic wallet, ledger, inbox, and outbox persistence on SQS message processing and deduplicates redelivery', async () => {
    const playerId = uuidv4();
    const walletRes = await request(instance.app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '200.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = walletRes.body.id as string;
    const consumer = instance.app.get(WagerTransactionConsumer);

    const sqsPayload = JSON.stringify({
      messageId: 'msg-integration-1',
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-a',
        externalTransactionId: 'ext-sqs-1',
        idempotencyKey: 'provider-a:ext-sqs-1',
        playerId,
        walletId,
        roundId: 'round-sqs-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '50.00', currency: 'BRL' },
      },
    });

    await instance.sqsService.publishFifoMessage({
      queueUrl: EnvProvider.SQS_WAGER_QUEUE_URL,
      body: sqsPayload,
      messageGroupId: walletId,
      deduplicationId: 'dedup-1',
    });

    const processedCount = await consumer.pollOnce();
    expect(processedCount).toBe(1);

    await instance.sqsService.publishFifoMessage({
      queueUrl: EnvProvider.SQS_WAGER_QUEUE_URL,
      body: sqsPayload,
      messageGroupId: walletId,
      deduplicationId: 'dedup-2-redelivery',
    });
    await consumer.pollOnce();

    const walletService = instance.app.get(WalletService);
    const reconciliation = await walletService.reconcileWallet(walletId);
    expect(reconciliation.consistent).toBe(true);
    expect(reconciliation.storedBalance).toEqual({
      amount: '150.00',
      currency: 'BRL',
    });
    expect(reconciliation.checkedEntries).toBe(2);

    const inboxRows = await instance.dataSource.query(
      `SELECT * FROM inbox_messages WHERE message_id = 'msg-integration-1'`,
    );
    expect(inboxRows).toHaveLength(1);
    expect(inboxRows[0].processed_at).not.toBeNull();

    const outboxRows = await instance.dataSource.query(
      `SELECT * FROM outbox_messages WHERE aggregate_id = $1`,
      [walletId],
    );
    expect(outboxRows).toHaveLength(2);
  });

  it('publishes outbox events safely with concurrent publishers without duplicating or losing events', async () => {
    const playerId = uuidv4();
    const walletRes = await request(instance.app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '500.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = walletRes.body.id as string;

    for (let i = 1; i <= 5; i++) {
      await request(instance.app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', `provider-a:outbox-tx-${i}`)
        .send({
          providerId: 'provider-a',
          externalTransactionId: `outbox-tx-${i}`,
          playerId,
          walletId,
          roundId: `round-${i}`,
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '10.00', currency: 'BRL' },
        })
        .expect(200);
    }

    const outboxService = instance.app.get(OutboxService);
    const [pubA, pubB] = await Promise.all([
      outboxService.publishPendingBatch(50),
      outboxService.publishPendingBatch(50),
    ]);

    expect(pubA + pubB).toBe(10);

    const unpublished = await instance.dataSource.query(
      `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
    );
    expect(unpublished[0].cnt).toBe(0);
  });

  it('routes malformed SQS messages to DLQ and recovers state cleanly across instance restart', async () => {
    const consumer = instance.app.get(WagerTransactionConsumer);

    await instance.sqsService.publishFifoMessage({
      queueUrl: EnvProvider.SQS_WAGER_QUEUE_URL,
      body: JSON.stringify({ invalid: true }),
      messageGroupId: 'bad-group',
      deduplicationId: `bad-${Date.now()}`,
    });

    await consumer.pollOnce();

    const dlqMessages = await instance.sqsService.receiveMessages(
      EnvProvider.SQS_WAGER_DLQ_URL,
      10,
      1,
    );
    expect(dlqMessages.length).toBeGreaterThanOrEqual(1);
  });
});
```

- [ ] **Step 2: Start Docker containers, run migrations, and execute integration tests**

Run:
```bash
docker compose up -d postgres localstack
bun run test:integration
```
Expected: PASS


---

### Task 11: Real Parallel Concurrency Suite (All 8 Mandatory Scenarios + Ledger Invariant) & Load Test Script

**Files:**
- Create: `test/concurrency/wagering.concurrency.spec.ts`
- Create: `test/load/load-test.ts`

**Interfaces:**
- Consumes: `createTestInstance`, `WageringService`, `WalletService`, `OutboxService`, `WagerTransactionConsumer`, `SqsService`
- Produces: Concurrency verification suite covering all 8 mandatory scenarios from Section 13 + `test/load/load-test.ts` (`bun run test:load`)

- [ ] **Step 1: Write `test/concurrency/wagering.concurrency.spec.ts` covering all 8 mandatory concurrency scenarios and `test/load/load-test.ts`**

Create `test/concurrency/wagering.concurrency.spec.ts`:
```typescript
import request from 'supertest';
import { v4 as uuidv4 } from 'uuid';
import {
  createTestInstance,
  purgeSqsQueues,
  resetDatabase,
  TestInstance,
} from '../helpers/test-app.helper';
import { WalletService } from '../../src/app/wallet/services/wallet.service';
import { WageringService } from '../../src/app/wagering/services/wagering.service';
import { OutboxService } from '../../src/app/messaging/services/outbox.service';
import { WagerTransactionConsumer } from '../../src/app/wagering/consumers/wager-transaction.consumer';
import { EnvProvider } from '../../src/common/providers/env.provider';
import { WagerTransactionStatus } from '../../src/common/enums/wager-transaction-status.enum';
import { FailureCode } from '../../src/common/enums/failure-code.enum';

describe('Wagering Processor Concurrency Suite (8 Mandatory Scenarios)', () => {
  let instances: TestInstance[] = [];

  beforeAll(async () => {
    const inst1 = await createTestInstance();
    const inst2 = await createTestInstance();
    const inst3 = await createTestInstance();
    instances = [inst1, inst2, inst3];
  });

  beforeEach(async () => {
    await resetDatabase(instances[0].dataSource);
    await purgeSqsQueues();
  });

  afterAll(async () => {
    await Promise.all(instances.map((i) => i.app.close()));
  });

  async function assertGlobalLedgerInvariant(): Promise<void> {
    const walletRows = await instances[0].dataSource.query(
      `SELECT id FROM wallets`,
    );
    const walletService = instances[0].app.get(WalletService);
    for (const row of walletRows) {
      const rec = await walletService.reconcileWallet(row.id as string);
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance).toEqual(rec.calculatedBalance);
      expect(rec.difference.amount).toBe('0.00');
    }
  }

  it('Scenario 1: Same bet sent 50 times in parallel produces exactly 1 debit', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = createRes.body.id as string;
    const payload = {
      providerId: 'provider-a',
      externalTransactionId: 'bet-50x',
      playerId,
      walletId,
      roundId: 'round-50x',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };

    const responses = await Promise.all(
      Array.from({ length: 50 }, (_, idx) => {
        const targetApp = instances[idx % instances.length].app.getHttpServer();
        return request(targetApp)
          .post('/wagering/transactions')
          .set('Idempotency-Key', 'provider-a:bet-50x')
          .send(payload);
      }),
    );

    for (const res of responses) {
      expect(res.status).toBe(200);
      expect(res.body.status).toBe(WagerTransactionStatus.Processed);
      expect(res.body.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    }

    const ledgerRes = await request(instances[0].app.getHttpServer())
      .get(`/wallets/${walletId}/ledger`)
      .expect(200);

    const debitEntries = ledgerRes.body.items.filter(
      (i: { direction: string }) => i.direction === 'DEBIT',
    );
    expect(debitEntries).toHaveLength(1);

    await assertGlobalLedgerInvariant();
  });

  it('Scenario 2: Mandatory Section 8 scenario - Initial 100.00 BRL, two simultaneous 80.00 BRL bets -> 1 PROCESSED, 1 REJECTED, final 20.00 BRL', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = createRes.body.id as string;

    const [resA, resB] = await Promise.all([
      request(instances[0].app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', 'provider-a:bet-80-a')
        .send({
          providerId: 'provider-a',
          externalTransactionId: 'bet-80-a',
          playerId,
          walletId,
          roundId: 'round-80',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '80.00', currency: 'BRL' },
        }),
      request(instances[1].app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', 'provider-a:bet-80-b')
        .send({
          providerId: 'provider-a',
          externalTransactionId: 'bet-80-b',
          playerId,
          walletId,
          roundId: 'round-80',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '80.00', currency: 'BRL' },
        }),
    ]);

    const statuses = [resA.body.status, resB.body.status].sort();
    expect(statuses).toEqual([
      WagerTransactionStatus.Processed,
      WagerTransactionStatus.Rejected,
    ]);

    const rejected =
      resA.body.status === WagerTransactionStatus.Rejected ? resA : resB;
    expect(rejected.status).toBe(422);
    expect(rejected.body.failureCode).toBe(FailureCode.InsufficientFunds);

    const walletRes = await request(instances[2].app.getHttpServer())
      .get(`/wallets/${walletId}`)
      .expect(200);
    expect(walletRes.body.balance).toEqual({
      amount: '20.00',
      currency: 'BRL',
    });

    const ledgerRes = await request(instances[0].app.getHttpServer())
      .get(`/wallets/${walletId}/ledger`)
      .expect(200);
    const debitEntries = ledgerRes.body.items.filter(
      (i: { direction: string }) => i.direction === 'DEBIT',
    );
    expect(debitEntries).toHaveLength(1);

    await assertGlobalLedgerInvariant();
  });

  it('Scenario 3 & 4: Distinct wallets and >= 3 simultaneous instances processing mixed concurrent workloads', async () => {
    const wallets = await Promise.all(
      Array.from({ length: 6 }, async (_, idx) => {
        const res = await request(instances[idx % 3].app.getHttpServer())
          .post('/wallets')
          .send({
            playerId: `player-${idx}-${uuidv4()}`,
            initialBalance: { amount: '500.00', currency: 'BRL' },
          })
          .expect(201);
        return res.body as { id: string; playerId: string };
      }),
    );

    await Promise.all(
      wallets.flatMap((w, wIdx) =>
        Array.from({ length: 10 }, (_, tIdx) => {
          const appServer =
            instances[(wIdx + tIdx) % instances.length].app.getHttpServer();
          return request(appServer)
            .post('/wagering/transactions')
            .set('Idempotency-Key', `provider-a:w-${wIdx}-tx-${tIdx}`)
            .send({
              providerId: 'provider-a',
              externalTransactionId: `w-${wIdx}-tx-${tIdx}`,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `round-${wIdx}-${tIdx}`,
              gameId: 'fortune-chimp',
              kind: tIdx % 2 === 0 ? 'BET' : 'WIN',
              money: { amount: '20.00', currency: 'BRL' },
            });
        }),
      ),
    );

    for (const w of wallets) {
      const walletRes = await request(instances[0].app.getHttpServer())
        .get(`/wallets/${w.id}`)
        .expect(200);
      expect(walletRes.body.balance).toEqual({
        amount: '500.00',
        currency: 'BRL',
      });
    }

    await assertGlobalLedgerInvariant();
  });

  it('Scenario 5: Worker killed after SQL commit and before SQS ack does not duplicate effects on redelivery', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);

    const walletId = createRes.body.id as string;
    const wageringService = instances[0].app.get(WageringService);

    await wageringService.submitTransaction({
      idempotencyKey: 'provider-a:crash-tx-1',
      providerId: 'provider-a',
      externalTransactionId: 'crash-tx-1',
      playerId,
      walletId,
      roundId: 'round-crash',
      gameId: 'fortune-chimp',
      kind: 'BET' as never,
      money: { amount: '30.00', currency: 'BRL' },
      inboxContext: {
        consumerName: 'wager-transaction-consumer',
        messageId: 'msg-crash-1',
        payloadHash: 'hash-crash-1',
      },
    });

    await instances[1].sqsService.publishFifoMessage({
      queueUrl: EnvProvider.SQS_WAGER_QUEUE_URL,
      body: JSON.stringify({
        messageId: 'msg-crash-1',
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: {
          providerId: 'provider-a',
          externalTransactionId: 'crash-tx-1',
          idempotencyKey: 'provider-a:crash-tx-1',
          playerId,
          walletId,
          roundId: 'round-crash',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '30.00', currency: 'BRL' },
        },
      }),
      messageGroupId: walletId,
      deduplicationId: 'dedup-crash-redelivery',
    });

    const consumer2 = instances[1].app.get(WagerTransactionConsumer);
    await consumer2.pollOnce();

    const walletRes = await request(instances[2].app.getHttpServer())
      .get(`/wallets/${walletId}`)
      .expect(200);
    expect(walletRes.body.balance).toEqual({
      amount: '70.00',
      currency: 'BRL',
    });

    await assertGlobalLedgerInvariant();
  });

  it('Scenario 6: Two publishers running concurrently over the same outbox', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '300.00', currency: 'BRL' },
      })
      .expect(201);
    const walletId = createRes.body.id as string;

    await Promise.all(
      Array.from({ length: 8 }, (_, idx) =>
        request(instances[idx % 3].app.getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', `provider-a:ob-conc-${idx}`)
          .send({
            providerId: 'provider-a',
            externalTransactionId: `ob-conc-${idx}`,
            playerId,
            walletId,
            roundId: `round-ob-${idx}`,
            gameId: 'fortune-chimp',
            kind: 'BET',
            money: { amount: '10.00', currency: 'BRL' },
          }),
      ),
    );

    const outbox1 = instances[0].app.get(OutboxService);
    const outbox2 = instances[1].app.get(OutboxService);

    const [count1, count2] = await Promise.all([
      outbox1.publishPendingBatch(10),
      outbox2.publishPendingBatch(10),
    ]);

    expect(count1 + count2).toBe(16);
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 7: ROLLBACK or REFUND delivered before the referenced BET is queued as PENDING_REFERENCE and reconciled once BET arrives', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);
    const walletId = createRes.body.id as string;

    const refundEarlyRes = await request(instances[0].app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'provider-a:refund-before-bet')
      .send({
        providerId: 'provider-a',
        externalTransactionId: 'refund-before-bet',
        referenceExternalTransactionId: 'late-bet-1',
        playerId,
        walletId,
        roundId: 'round-ooo',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '40.00', currency: 'BRL' },
      })
      .expect(202);

    expect(refundEarlyRes.body.status).toBe(
      WagerTransactionStatus.PendingReference,
    );

    await request(instances[1].app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'provider-a:late-bet-1')
      .send({
        providerId: 'provider-a',
        externalTransactionId: 'late-bet-1',
        playerId,
        walletId,
        roundId: 'round-ooo',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '40.00', currency: 'BRL' },
      })
      .expect(200);

    await instances[2].dataSource.query(
      `UPDATE wager_transactions SET next_retry_at = now() - interval '1 second' WHERE external_transaction_id = 'refund-before-bet'`,
    );

    const wageringService = instances[2].app.get(WageringService);
    await wageringService.retryPendingReferences(10);

    const refundCheck = await request(instances[0].app.getHttpServer())
      .get('/providers/provider-a/wagering/transactions/refund-before-bet')
      .expect(200);

    expect(refundCheck.body.status).toBe(WagerTransactionStatus.Processed);

    const walletRes = await request(instances[0].app.getHttpServer())
      .get(`/wallets/${walletId}`)
      .expect(200);
    expect(walletRes.body.balance).toEqual({
      amount: '100.00',
      currency: 'BRL',
    });

    await assertGlobalLedgerInvariant();
  });

  it('Scenario 8: Service restart preserves full consistency across wallet, ledger, and outbox', async () => {
    const playerId = uuidv4();
    const createRes = await request(instances[0].app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '250.00', currency: 'BRL' },
      })
      .expect(201);
    const walletId = createRes.body.id as string;

    await request(instances[0].app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'provider-a:restart-bet-1')
      .send({
        providerId: 'provider-a',
        externalTransactionId: 'restart-bet-1',
        playerId,
        walletId,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '50.00', currency: 'BRL' },
      })
      .expect(200);

    await instances[0].app.close();
    instances[0] = await createTestInstance();

    const replayRes = await request(instances[0].app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'provider-a:restart-bet-1')
      .send({
        providerId: 'provider-a',
        externalTransactionId: 'restart-bet-1',
        playerId,
        walletId,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '50.00', currency: 'BRL' },
      })
      .expect(200);

    expect(replayRes.body.idempotentReplay).toBe(true);
    expect(replayRes.body.balance).toEqual({
      amount: '200.00',
      currency: 'BRL',
    });

    await assertGlobalLedgerInvariant();
  });
});
```

Create `test/load/load-test.ts`:
```typescript
import { v4 as uuidv4 } from 'uuid';
import { createTestInstance, resetDatabase } from '../helpers/test-app.helper';
import { WageringService } from '../../src/app/wagering/services/wagering.service';
import { WalletService } from '../../src/app/wallet/services/wallet.service';
import { WagerTransactionKind } from '../../src/common/enums/wager-transaction-kind.enum';

async function runLoadTest() {
  const instance = await createTestInstance({
    enableConsumer: false,
    enableWorkers: true,
  });
  await resetDatabase(instance.dataSource);

  const walletService = instance.app.get(WalletService);
  const wageringService = instance.app.get(WageringService);

  const walletCount = 10;
  const opsPerWallet = 50;
  const wallets = await Promise.all(
    Array.from({ length: walletCount }, (_, i) =>
      walletService.createWallet({
        playerId: `load-player-${i}-${uuidv4()}`,
        initialBalance: { amount: '10000.00', currency: 'BRL' },
      }),
    ),
  );

  const latencies: number[] = [];
  let errors = 0;
  const startWall = performance.now();

  await Promise.all(
    wallets.flatMap((wallet, wIdx) =>
      Array.from({ length: opsPerWallet }, async (_, opIdx) => {
        const t0 = performance.now();
        try {
          await wageringService.submitTransaction({
            idempotencyKey: `load:${wIdx}:${opIdx}`,
            providerId: 'load-provider',
            externalTransactionId: `ext-${wIdx}-${opIdx}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `round-${wIdx}-${opIdx}`,
            gameId: 'fortune-chimp',
            kind:
              opIdx % 3 === 0
                ? WagerTransactionKind.Win
                : WagerTransactionKind.Bet,
            money: { amount: '10.00', currency: 'BRL' },
          });
          latencies.push(performance.now() - t0);
        } catch {
          errors += 1;
        }
      }),
    ),
  );

  const totalTimeSec = (performance.now() - startWall) / 1000;
  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)] ?? 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] ?? 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)] ?? 0;

  for (const w of wallets) {
    const rec = await walletService.reconcileWallet(w.id);
    if (!rec.consistent) {
      throw new Error(`Wallet ${w.id} inconsistent after load test!`);
    }
  }

  process.stdout.write(
    `${JSON.stringify(
      {
        totalOperations: walletCount * opsPerWallet,
        durationSeconds: Number(totalTimeSec.toFixed(2)),
        throughputOpsPerSec: Number(
          ((walletCount * opsPerWallet) / totalTimeSec).toFixed(2),
        ),
        p50Ms: Number(p50.toFixed(2)),
        p95Ms: Number(p95.toFixed(2)),
        p99Ms: Number(p99.toFixed(2)),
        errors,
        allWalletsReconciled: true,
      },
      null,
      2,
    )}\n`,
  );

  await instance.app.close();
}

runLoadTest();
```

- [ ] **Step 2: Run the concurrency test suite and load test**

Run:
```bash
bun run test:concurrency
bun run test:load
```
Expected: PASS (all 8 concurrency scenarios pass and `allWalletsReconciled: true`)


---

### Task 12: Documentation (`README.md` & `ARCHITECTURE.md`) & Final Lint / Test Verification

**Files:**
- Create: `README.md`
- Create: `ARCHITECTURE.md`

**Interfaces:**
- Consumes: All modules, schema design, concurrency strategy, failure taxonomy, load test metrics
- Produces: Complete `README.md` and `ARCHITECTURE.md` required by Section 14

- [ ] **Step 1: Create `README.md` and `ARCHITECTURE.md`**

Create `README.md`:
```markdown
# Jungle Gaming — Distributed Wagering Processor

Serviço financeiro distribuído para processamento concorrente e idempotente de transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), construído com **Bun 1.x**, **NestJS 11**, **TypeORM**, **PostgreSQL 16** e **AWS SQS (LocalStack)** seguindo a organização modular de microsserviços **uTrip**.

## Setup & Execução Rápida

### 1. Subir infraestrutura (PostgreSQL + LocalStack SQS)

```bash
docker compose up -d postgres localstack
```

### 2. Instalar dependências e rodar migrations

```bash
bun install
bun run migration:run
```

### 3. Iniciar a aplicação

```bash
bun run start:dev
```

Swagger UI disponível em `http://localhost:3000/docs`.

### 4. Executar suítes de teste

```bash
# Testes de unidade (Jest)
bun run test:unit

# Testes de integração com PostgreSQL e LocalStack reais
bun run test:integration

# Testes de concorrência (8 cenários reais em paralelo + invariante de reconciliação)
bun run test:concurrency

# Suíte completa
bun run test:all

# Teste de carga (diferencial)
bun run test:load

# Lint
bun run lint
```

## Endpoints Principais

- `POST /wallets` — Criação de carteira com transação atômica `OPENING` e lançamento `CREDIT` no ledger.
- `GET /wallets/:walletId` — Consulta de saldo materializado e versão.
- `GET /wallets/:walletId/ledger?cursor=...&limit=50` — Histórico paginado por cursor opaco estável.
- `POST /wallets/:walletId/reconciliation` — Auditoria entre saldo materializado e soma reconstruída do ledger.
- `POST /wagering/transactions` — Submissão idempotente (`Idempotency-Key` obrigatório).
- `GET /wagering/transactions/:transactionId` — Consulta por ID interno.
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId` — Consulta por chave do provedor.
- `GET /health/live` & `GET /health/ready` — Liveness e Readiness probes.
- `GET /metrics` — Métricas operacionais em JSON.
```

Create `ARCHITECTURE.md`:
```markdown
# Architecture & Engineering Decisions

## 1. Organização Modular (Padrão uTrip + DDD)

O projeto combina o isolamento rico de domínio exigido pelo desafio com a estrutura padronizada dos microsserviços uTrip:

- `src/app/<domínio>/{controllers,services,repositories,entities,domain,forms,responses,dtos,cron,consumers}`
- **Contratos separados por responsabilidade:**
  - `.form.ts` (`*Form`): validação de entrada HTTP com `class-validator`.
  - `.response.ts` (`*Response`): contratos de saída HTTP documentados com `@nestjs/swagger`.
  - `.dto.ts` (`*Dto`): tráfego estritamente interno entre serviços, consumidores e repositórios.
- **Encapsulamento de módulos:** Módulos NestJS exportam exclusivamente `*Service`, nunca `*Repository`.
- **Domínio Puro:** `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `InboxMessage`, `OutboxMessage` e `IntegrationEvent<T>` possuem construtores `private`/`protected` e factories estáticas (`create`/`open`/`from`/`receive`/`enqueue` e `rehydrate`), sem dependência de decorators do ORM ou NestJS.

## 2. Escolha do ORM (TypeORM) e Mapeamento de `Money`

- **TypeORM** foi adotado por alinhamento com a stack padrão uTrip e suporte explícito a controle manual de `QueryRunner`/`EntityManager`, locks pessimistas (`pessimistic_write` / `FOR UPDATE`) e `FOR UPDATE SKIP LOCKED`.
- **`Money`:** Internamente usa `Decimal` (`decimal.js`) com escala fixa de 2 casas decimais. No PostgreSQL, valores monetários são armazenados como `numeric(20, 2)` (retornados pelo driver `pg` como `string` exata) + `varchar(3)` para moeda ISO-4217, eliminando qualquer risco de imprecisão IEEE 754 (`number`/`float`).

## 3. Concorrência, Ordenação e Isolamento por `walletId`

- **Unidade de Concorrência:** `walletId`.
- **Estratégia:** Dentro de uma única transação SQL (`READ COMMITTED`), a carteira alvo é carregada com `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE` (`pessimistic_write`).
  - Operações na **mesma wallet** são serializadas na linha da carteira no PostgreSQL, impedindo *lost updates* e *double spending* mesmo com 3+ instâncias simultâneas.
  - Operações em **wallets distintas** executam em total paralelismo sem contenção global.
  - A coluna `version` na `Wallet` inicia em `1` e é incrementada exclusivamente quando o saldo sofre mutação.

## 4. Garantias no Schema do Banco de Dados

Aplicadas na migration `1761130000000-InitialWageringSchema`:
1. `wallets`:
   - `UNIQUE (player_id, currency)`
   - `CHECK (balance_amount >= 0)`
   - `CHECK (version >= 1)`
2. `wallet_ledger_entries`:
   - `UNIQUE (wallet_id, transaction_id)` (no máximo 1 lançamento por transação e carteira)
   - `CHECK (amount > 0)`, `CHECK (balance_before >= 0)`, `CHECK (balance_after >= 0)`
   - `CHECK ((direction = 'DEBIT' AND balance_before - amount = balance_after) OR (direction = 'CREDIT' AND balance_before + amount = balance_after))`
   - **Imutabilidade estrutural no banco:** Trigger `trg_immutable_wallet_ledger_entries` que bloqueia qualquer `UPDATE` ou `DELETE` na tabela `wallet_ledger_entries`.
3. `wager_transactions`:
   - `UNIQUE (idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)`
   - Índice único parcial `uq_wager_tx_processed_reversal ON wager_transactions (reference_transaction_id, kind) WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK')`, impedindo dupla reversão no nível do banco.

## 5. Idempotência e Hash Canônico (`payloadHash`)

- **Algoritmo:** SHA-256 sobre JSON canônico com chaves ordenadas alfabeticamente (`externalTransactionId`, `gameId`, `kind`, `money.amount`, `money.currency`, `playerId`, `providerId`, `referenceExternalTransactionId`, `roundId`, `walletId`).
- **Replay vs Conflito:**
  - Mesma `Idempotency-Key` + mesmo `payloadHash` -> retorna o resultado original (`idempotentReplay: true`) com o saldo observado no momento da execução (`observed_balance_amount`).
  - Mesma `Idempotency-Key` + `payloadHash` divergente -> `409 Conflict` (`IDEMPOTENCY_CONFLICT`).

## 6. Taxonomia de Códigos de Falha (`FailureCode`) e Status HTTP

| Situação | HTTP Status | `FailureCode` / `code` | Ação do Provedor |
|---|---|---|---|
| Transação aplicada (`PROCESSED`) | `200 OK` | - | Concluído |
| Aguardando referência (`PENDING_REFERENCE`) | `202 Accepted` | - | Aguardar evento assíncrono |
| Payload inválido / `Money` inválido | `400 Bad Request` | `INVALID_MONEY` | Corrigir contrato |
| Carteira ou transação não encontrada | `404 Not Found` | `ENTITY_NOT_FOUND` | Verificar IDs |
| Conflito de idempotência ou carteira duplicada | `409 Conflict` | `IDEMPOTENCY_CONFLICT` / `DUPLICATE_WALLET` | Não reenviar payload divergente |
| Aposta sem saldo suficiente | `422 Unprocessable Entity` | `INSUFFICIENT_FUNDS` | Não reenviar |
| Reversão (`ROLLBACK` de `WIN`) geraria saldo negativo | `422 Unprocessable Entity` | `REVERSAL_INSUFFICIENT_FUNDS` | Escalar / auditar |
| Moeda divergente da carteira | `422 Unprocessable Entity` | `CURRENCY_MISMATCH` | Corrigir moeda |
| Tipo de referência inválido | `422 Unprocessable Entity` | `INVALID_REFERENCE_KIND` | Não reenviar |
| Divergência de player/wallet/round na referência | `422 Unprocessable Entity` | `REFERENCE_MISMATCH` | Não reenviar |
| Valor de `REFUND`/`ROLLBACK` diferente da referência | `422 Unprocessable Entity` | `AMOUNT_MISMATCH` | Corrigir valor |
| Referência já revertida anteriormente | `422 Unprocessable Entity` | `DUPLICATE_REVERSAL` | Ignorar |
| Referência nunca chegou após TTL/tentativas | `422 Unprocessable Entity` | `REFERENCE_NOT_FOUND` | Transação expirada |
| Falha transitória de banco/infra | `503 Service Unavailable` | `TRANSIENT_INFRASTRUCTURE_ERROR` | Reenviar com backoff |

## 7. Referências Fora de Ordem, Transactional Outbox e SQS

- **`PENDING_REFERENCE`:** Quando `REFUND` ou `ROLLBACK` chega antes da transação referenciada, é gravada como `PENDING_REFERENCE` com evento `WagerTransactionPendingReference` na Outbox. O worker `PendingReferenceCron` utiliza `FOR UPDATE SKIP LOCKED` com backoff exponencial (`500ms * 2^(attempts-1)`). Ao atingir `PENDING_REF_MAX_ATTEMPTS` (default 5) ou `PENDING_REF_TTL_MS` (default 60s), transiciona para `REJECTED` com `REFERENCE_NOT_FOUND` e publica `WagerTransactionRejected`.
- **Transactional Outbox:** Eventos de integração (`WalletBalanceChanged`, `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`) são gravados na tabela `outbox_messages` na mesma transação SQL da mudança financeira. `OutboxPublisherCron` consome lotes com `FOR UPDATE SKIP LOCKED`, garantindo segurança com múltiplos publishers simultâneos.
- **SQS Consumer & Inbox:** Deduplica por `(consumer_name, message_id)` na tabela `inbox_messages` dentro da mesma transação SQL, executando `DeleteMessage` (ack) exclusivamente após o commit. No `SIGTERM`, aguarda mensagens em voo ou reseta `VisibilityTimeout = 0`.

## 8. Autenticação (Decisão Documentada — Seção 2)

Conforme permitido pela Seção 2 do desafio, a autenticação foi modelada como ponto de extensão explícito via `ProviderAuthGuard` e porta `ProviderIdentityPort` (`src/common/guards/provider-auth.guard.ts`). Em produção, um adaptador OIDC/JWT (Keycloak ou Zitadel via JWKS) implementa `ProviderIdentityPort` validando `client_id` = `providerId`, mantendo `/health/live` e `/health/ready` abertos.
```

- [ ] **Step 2: Run linter and full test suite to verify zero errors**

Run:
```bash
bun run lint
bun run test:all
```
Expected: PASS with zero lint errors/warnings and all unit, integration, and concurrency tests green.

