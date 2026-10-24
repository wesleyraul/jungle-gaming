import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { OutboxService } from '../../src/app/messaging/services/outbox.service';
import { SqsService } from '../../src/app/messaging/services/sqs.service';
import { WagerTransactionConsumer } from '../../src/app/wagering/consumers/wager-transaction.consumer';
import { FailureCode } from '../../src/common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../src/common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../src/common/enums/wager-transaction-status.enum';
import { EnvProvider } from '../../src/common/providers/env.provider';
import { createTestApp, TestAppContext } from '../helpers/test-app.helper';

describe('Distributed Wagering Processor Integration Tests', () => {
  let context: TestAppContext;
  let app: INestApplication;
  let dataSource: DataSource;
  let outboxService: OutboxService;
  let sqsService: SqsService;
  let wagerConsumer: WagerTransactionConsumer;

  beforeAll(async () => {
    context = await createTestApp();
    app = context.app;
    dataSource = context.dataSource;
    outboxService = context.module.get(OutboxService);
    sqsService = context.module.get(SqsService);
    wagerConsumer = context.module.get(WagerTransactionConsumer);
  }, 60000);

  afterAll(async () => {
    await context?.close();
  });

  beforeEach(async () => {
    await context.cleanDatabase();
  });

  it('verifies DB trigger enforces immutability of wallet_ledger_entries and unique player+currency wallet constraint', async () => {
    const playerId = uuidv4();
    const createWalletRes = await request(app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);

    await request(app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '50.00', currency: 'BRL' },
      })
      .expect(409);

    const walletId = createWalletRes.body.id;
    const entries = await dataSource.query(
      'SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1',
      [walletId],
    );
    expect(entries.length).toBe(1);
    const entryId = entries[0].id;

    await expect(
      dataSource.query(
        'UPDATE wallet_ledger_entries SET amount = 999.00 WHERE id = $1',
        [entryId],
      ),
    ).rejects.toThrow(/immutable/i);

    await expect(
      dataSource.query('DELETE FROM wallet_ledger_entries WHERE id = $1', [
        entryId,
      ]),
    ).rejects.toThrow(/immutable/i);
  });

  it('completes full lifecycle: BET, LOSS, Idempotent replay, WIN, ROLLBACK, Reversal Conflict, Queries, and Ledger Reconciliation', async () => {
    const playerId = uuidv4();
    const walletRes = await request(app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);
    const walletId = walletRes.body.id;

    const betPayload = {
      providerId: 'prov-a',
      externalTransactionId: 'bet-101',
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'game-slot',
      kind: WagerTransactionKind.Bet,
      money: { amount: '30.00', currency: 'BRL' },
    };

    const betRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:bet-101')
      .send(betPayload)
      .expect(200);

    expect(betRes.body.transactionId).toBeDefined();
    expect(betRes.body.status).toBe(WagerTransactionStatus.Processed);
    expect(betRes.body.balance.amount).toBe('70.00');
    expect(betRes.body.idempotentReplay).toBe(false);

    const duplicateRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:bet-101')
      .send(betPayload)
      .expect(200);

    expect(duplicateRes.body.transactionId).toBe(betRes.body.transactionId);
    expect(duplicateRes.body.status).toBe(WagerTransactionStatus.Processed);
    expect(duplicateRes.body.balance.amount).toBe('70.00');
    expect(duplicateRes.body.idempotentReplay).toBe(true);

    await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:bet-101')
      .send({
        ...betPayload,
        money: { amount: '45.00', currency: 'BRL' },
      })
      .expect(409);

    const lossRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:loss-101')
      .send({
        providerId: 'prov-a',
        externalTransactionId: 'loss-101',
        playerId,
        walletId,
        roundId: 'round-1',
        gameId: 'game-slot',
        kind: WagerTransactionKind.Loss,
        referenceExternalTransactionId: 'bet-101',
        money: { amount: '0.00', currency: 'BRL' },
      })
      .expect(200);

    expect(lossRes.body.status).toBe(WagerTransactionStatus.Processed);
    expect(lossRes.body.balance.amount).toBe('70.00');

    const winPayload = {
      providerId: 'prov-a',
      externalTransactionId: 'win-101',
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'game-slot',
      kind: WagerTransactionKind.Win,
      referenceExternalTransactionId: 'bet-101',
      money: { amount: '50.00', currency: 'BRL' },
    };

    const winRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:win-101')
      .send(winPayload)
      .expect(200);

    expect(winRes.body.status).toBe(WagerTransactionStatus.Processed);
    expect(winRes.body.balance.amount).toBe('120.00');

    const rollbackPayload = {
      providerId: 'prov-a',
      externalTransactionId: 'rb-101',
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'game-slot',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'win-101',
      money: { amount: '50.00', currency: 'BRL' },
    };

    const rollbackRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:rb-101')
      .send(rollbackPayload)
      .expect(200);

    expect(rollbackRes.body.status).toBe(WagerTransactionStatus.Processed);
    expect(rollbackRes.body.balance.amount).toBe('70.00');

    const secondRollbackPayload = {
      providerId: 'prov-a',
      externalTransactionId: 'rb-102',
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'game-slot',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'win-101',
      money: { amount: '50.00', currency: 'BRL' },
    };

    const conflictRollbackRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'prov-a:rb-102')
      .send(secondRollbackPayload)
      .expect(422);

    expect(conflictRollbackRes.body.status).toBe(WagerTransactionStatus.Rejected);
    expect(conflictRollbackRes.body.failureCode).toBe(
      FailureCode.DuplicateReversal,
    );

    const txByIdRes = await request(app.getHttpServer())
      .get(`/wagering/transactions/${betRes.body.transactionId}`)
      .expect(200);
    expect(txByIdRes.body.id).toBe(betRes.body.transactionId);

    const txByProviderRes = await request(app.getHttpServer())
      .get('/providers/prov-a/wagering/transactions/bet-101')
      .expect(200);
    expect(txByProviderRes.body.id).toBe(betRes.body.transactionId);

    const reconRes = await request(app.getHttpServer())
      .post(`/wallets/${walletId}/reconciliation`)
      .expect(200);

    expect(reconRes.body.consistent).toBe(true);
    expect(reconRes.body.storedBalance.amount).toBe('70.00');
    expect(reconRes.body.calculatedBalance.amount).toBe('70.00');
    expect(reconRes.body.checkedEntries).toBe(4);

    const page1 = await request(app.getHttpServer())
      .get(`/wallets/${walletId}/ledger?limit=2`)
      .expect(200);

    expect(page1.body.items.length).toBe(2);
    expect(page1.body.nextCursor).toBeTruthy();

    const page2 = await request(app.getHttpServer())
      .get(`/wallets/${walletId}/ledger?limit=2&cursor=${page1.body.nextCursor}`)
      .expect(200);
    expect(page2.body.items.length).toBe(2);
    expect(page2.body.nextCursor).toBeNull();

    const publishedCount = await outboxService.publishDueOutboxBatch(50);
    expect(publishedCount).toBeGreaterThan(0);
  });

  it('processes SQS Section 10 WagerTransactionRequested envelope, deduplicates redeliveries via inbox_messages, and routes poison messages to LocalStack DLQ', async () => {
    const playerId = uuidv4();
    const walletRes = await request(app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      })
      .expect(201);
    const walletId = walletRes.body.id;

    const sqsEnvelope = {
      messageId: `msg-sqs-${uuidv4()}`,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-sqs',
        externalTransactionId: 'sqs-bet-1',
        idempotencyKey: 'provider-sqs:sqs-bet-1',
        playerId,
        walletId,
        roundId: 'round-sqs-1',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '25.00', currency: 'BRL' },
      },
    };

    await sqsService.sendMessage(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      JSON.stringify(sqsEnvelope),
      walletId,
      sqsEnvelope.messageId,
    );

    const polled = await wagerConsumer.pollOnce();
    expect(polled).toBeGreaterThanOrEqual(1);

    const inboxRows = await dataSource.query(
      'SELECT * FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2',
      ['WagerTransactionConsumer', sqsEnvelope.messageId],
    );
    expect(inboxRows.length).toBe(1);
    expect(inboxRows[0].processed_at).toBeTruthy();

    await sqsService.sendMessage(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      JSON.stringify(sqsEnvelope),
      walletId,
      `redelivery-${uuidv4()}`,
    );
    await wagerConsumer.pollOnce();

    const walletAfterRedelivery = await request(app.getHttpServer())
      .get(`/wallets/${walletId}`)
      .expect(200);
    expect(walletAfterRedelivery.body.balance.amount).toBe('75.00');

    const poisonMsgId = `poison-${uuidv4()}`;
    await sqsService.sendMessage(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      '{malformed-json',
      walletId,
      poisonMsgId,
    );
    await wagerConsumer.pollOnce();

    const dlqMessages = await sqsService.receiveMessages(
      EnvProvider.SQS_WAGER_DLQ_URL,
      10,
      1,
    );
    expect(dlqMessages.some((m) => m.Body === '{malformed-json')).toBe(true);
    for (const m of dlqMessages) {
      if (m.ReceiptHandle) {
        await sqsService.deleteMessage(
          EnvProvider.SQS_WAGER_DLQ_URL,
          m.ReceiptHandle,
        );
      }
    }
  });

  it('exposes /health/live, /health/ready, and /metrics with singleton counters', async () => {
    const liveRes = await request(app.getHttpServer())
      .get('/health/live')
      .expect(200);
    expect(liveRes.body.status).toBe('UP');

    const readyRes = await request(app.getHttpServer())
      .get('/health/ready')
      .expect(200);
    expect(readyRes.body.status).toBe('UP');
    expect(readyRes.body.checks.database).toBe('UP');
    expect(readyRes.body.checks.sqs).toBe('UP');

    const metricsRes = await request(app.getHttpServer())
      .get('/metrics')
      .expect(200);
    expect(metricsRes.text).toContain('transactions_by_status_total');
    expect(metricsRes.text).toContain('duplicates_detected_total');
    expect(metricsRes.text).toContain('sqs_dead_letter_total');
    expect(metricsRes.text).toContain('outbox_lag_seconds');
  });
});
