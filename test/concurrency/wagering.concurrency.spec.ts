import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { OutboxService } from '../../src/app/messaging/services/outbox.service';
import { SqsService } from '../../src/app/messaging/services/sqs.service';
import { WagerTransactionConsumer } from '../../src/app/wagering/consumers/wager-transaction.consumer';
import { WageringService } from '../../src/app/wagering/services/wagering.service';
import { WalletService } from '../../src/app/wallet/services/wallet.service';
import { Money } from '../../src/common/domain/money.domain';
import { FailureCode } from '../../src/common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../src/common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../src/common/enums/wager-transaction-status.enum';
import { createTestApp, TestAppContext } from '../helpers/test-app.helper';

describe('Distributed Wagering Processor Concurrency Suite (Section 8 & 13)', () => {
  let context: TestAppContext;
  let app: INestApplication;
  let dataSource: DataSource;
  let wageringService: WageringService;
  let walletService: WalletService;
  let outboxService: OutboxService;

  beforeAll(async () => {
    context = await createTestApp();
    app = context.app;
    dataSource = context.dataSource;
    wageringService = context.module.get(WageringService);
    walletService = context.module.get(WalletService);
    outboxService = context.module.get(OutboxService);
  }, 60000);

  afterAll(async () => {
    await context?.close();
  });

  beforeEach(async () => {
    await context.cleanDatabase();
  });

  async function createWallet(
    initialAmount = '100.00',
    playerId = uuidv4(),
  ): Promise<{ id: string; playerId: string }> {
    const res = await request(app.getHttpServer())
      .post('/wallets')
      .send({
        playerId,
        initialBalance: { amount: initialAmount, currency: 'BRL' },
      })
      .expect(201);
    return { id: res.body.id, playerId };
  }

  async function assertGlobalLedgerInvariant(): Promise<void> {
    const rows = await dataSource.query(`
      SELECT 
        w.id AS wallet_id,
        w.currency AS currency,
        w.balance_amount::text AS stored_balance,
        COALESCE(SUM(CASE WHEN e.direction = 'CREDIT' THEN e.amount ELSE -e.amount END), 0)::numeric(20,2)::text AS calculated_balance
      FROM wallets w
      LEFT JOIN wallet_ledger_entries e ON e.wallet_id = w.id
      GROUP BY w.id, w.currency, w.balance_amount
    `);

    for (const row of rows) {
      const stored = Money.from({
        amount: row.stored_balance,
        currency: row.currency,
      });
      const calculated = Money.from({
        amount: row.calculated_balance,
        currency: row.currency,
      });
      expect(stored.equals(calculated)).toBe(true);
    }
  }

  it('Scenario 1: Same bet sent 50 times in parallel produces a single debit', async () => {
    const { id: walletId, playerId } = await createWallet('100.00');
    const extId = `bet-storm-50-${uuidv4().substring(0, 8)}`;
    const idempKey = `provider-a:${extId}`;

    const payload = {
      providerId: 'provider-a',
      externalTransactionId: extId,
      playerId,
      walletId,
      roundId: 'round-storm-50',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: { amount: '25.00', currency: 'BRL' },
    };

    const promises = Array.from({ length: 50 }).map(() =>
      request(app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', idempKey)
        .send(payload),
    );

    const responses = await Promise.all(promises);
    for (const r of responses) {
      expect(r.status).toBe(200);
      expect(r.body.transactionId).toBeDefined();
      expect(r.body.status).toBe(WagerTransactionStatus.Processed);
      expect(r.body.balance.amount).toBe('75.00');
    }

    const distinctTxIds = new Set(responses.map((r) => r.body.transactionId));
    expect(distinctTxIds.size).toBe(1);

    const recon = await walletService.reconcileWallet(walletId);
    expect(recon.consistent).toBe(true);
    expect(recon.checkedEntries).toBe(2);
    expect(recon.storedBalance.amount).toBe('75.00');
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 2: Concurrent operations disputing same wallet balance (Section 8 mandatory 2x80.00 BRL on 100.00 BRL + 10x15.00 BRL)', async () => {
    const { id: walletId80, playerId: playerId80 } = await createWallet('100.00');

    const [bet80A, bet80B] = await Promise.all([
      request(app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', 'provider-a:bet-80-a')
        .send({
          providerId: 'provider-a',
          externalTransactionId: 'bet-80-a',
          playerId: playerId80,
          walletId: walletId80,
          roundId: 'round-80-a',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        }),
      request(app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', 'provider-a:bet-80-b')
        .send({
          providerId: 'provider-a',
          externalTransactionId: 'bet-80-b',
          playerId: playerId80,
          walletId: walletId80,
          roundId: 'round-80-b',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        }),
    ]);

    const pair80 = [bet80A, bet80B];
    const processed80 = pair80.filter(
      (r) => r.status === 200 && r.body.status === WagerTransactionStatus.Processed,
    );
    const rejected80 = pair80.filter(
      (r) => r.status === 422 && r.body.status === WagerTransactionStatus.Rejected,
    );

    expect(processed80.length).toBe(1);
    expect(rejected80.length).toBe(1);
    expect(rejected80[0].body.failureCode).toBe(FailureCode.InsufficientFunds);

    await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', 'provider-a:bet-80-a')
      .send({
        providerId: 'provider-a',
        externalTransactionId: 'bet-80-a',
        playerId: playerId80,
        walletId: walletId80,
        roundId: 'round-80-a',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      });

    const recon80 = await walletService.reconcileWallet(walletId80);
    expect(recon80.consistent).toBe(true);
    expect(recon80.storedBalance.amount).toBe('20.00');
    expect(recon80.checkedEntries).toBe(2);

    const { id: walletId15, playerId: playerId15 } = await createWallet('100.00');
    const promises15 = Array.from({ length: 10 }).map((_, idx) => {
      const extId = `bet-15-${idx}`;
      return request(app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', `provider-a:${extId}`)
        .send({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: playerId15,
          walletId: walletId15,
          roundId: `round-15-${idx}`,
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '15.00', currency: 'BRL' },
        });
    });

    const responses15 = await Promise.all(promises15);
    expect(
      responses15.filter(
        (r) => r.status === 200 && r.body.status === WagerTransactionStatus.Processed,
      ).length,
    ).toBe(6);
    expect(
      responses15.filter(
        (r) => r.status === 422 && r.body.status === WagerTransactionStatus.Rejected,
      ).length,
    ).toBe(4);

    const recon15 = await walletService.reconcileWallet(walletId15);
    expect(recon15.consistent).toBe(true);
    expect(recon15.storedBalance.amount).toBe('10.00');
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 3: Distinct wallets processed in parallel without cross-wallet contention', async () => {
    const wallets = await Promise.all([
      createWallet('500.00'),
      createWallet('500.00'),
      createWallet('500.00'),
      createWallet('500.00'),
      createWallet('500.00'),
    ]);

    const operations: Promise<unknown>[] = [];
    for (let i = 0; i < 50; i++) {
      const targetWallet = wallets[i % wallets.length];
      const isBet = i % 2 === 0;
      const extId = `multi-w-${i}`;
      operations.push(
        request(app.getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', `provider-a:${extId}`)
          .send({
            providerId: 'provider-a',
            externalTransactionId: extId,
            playerId: targetWallet.playerId,
            walletId: targetWallet.id,
            roundId: `round-multi-${i}`,
            gameId: 'fortune-chimp',
            kind: isBet ? WagerTransactionKind.Bet : WagerTransactionKind.Win,
            money: { amount: '10.00', currency: 'BRL' },
          }),
      );
    }

    await Promise.all(operations);

    for (const w of wallets) {
      const recon = await walletService.reconcileWallet(w.id);
      expect(recon.consistent).toBe(true);
      expect(recon.difference.amount).toBe('0.00');
    }
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 4: >= 3 simultaneous application instances competing on the same wallet and reversals', async () => {
    const instance2 = await createTestApp();
    const instance3 = await createTestApp();

    try {
      const apps = [app, instance2.app, instance3.app];
      const { id: walletId, playerId } = await createWallet('100.00');

      const betRequests = Array.from({ length: 9 }).map((_, idx) => {
        const targetApp = apps[idx % apps.length];
        const extId = `multi-inst-bet-${idx}`;
        return request(targetApp.getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', `provider-a:${extId}`)
          .send({
            providerId: 'provider-a',
            externalTransactionId: extId,
            playerId,
            walletId,
            roundId: 'round-multi-inst',
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Bet,
            money: { amount: '20.00', currency: 'BRL' },
          });
      });

      const betResults = await Promise.all(betRequests);
      expect(betResults.filter((r) => r.status === 200).length).toBe(5);
      expect(betResults.filter((r) => r.status === 422).length).toBe(4);

      const processedExtId = 'multi-inst-bet-0';
      const [rev1, rev2, rev3] = await Promise.all([
        request(apps[0].getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', 'provider-a:rev-inst-1')
          .send({
            providerId: 'provider-a',
            externalTransactionId: 'rev-inst-1',
            playerId,
            walletId,
            roundId: 'round-multi-inst',
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Refund,
            referenceExternalTransactionId: processedExtId,
            money: { amount: '20.00', currency: 'BRL' },
          }),
        request(apps[1].getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', 'provider-a:rev-inst-2')
          .send({
            providerId: 'provider-a',
            externalTransactionId: 'rev-inst-2',
            playerId,
            walletId,
            roundId: 'round-multi-inst',
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Refund,
            referenceExternalTransactionId: processedExtId,
            money: { amount: '20.00', currency: 'BRL' },
          }),
        request(apps[2].getHttpServer())
          .post('/wagering/transactions')
          .set('Idempotency-Key', 'provider-a:rev-inst-3')
          .send({
            providerId: 'provider-a',
            externalTransactionId: 'rev-inst-3',
            playerId,
            walletId,
            roundId: 'round-multi-inst',
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Refund,
            referenceExternalTransactionId: processedExtId,
            money: { amount: '20.00', currency: 'BRL' },
          }),
      ]);

      const revList = [rev1, rev2, rev3];
      expect(revList.filter((r) => r.status === 200).length).toBe(1);
      expect(revList.filter((r) => r.status === 422).length).toBe(2);

      const recon = await walletService.reconcileWallet(walletId);
      expect(recon.consistent).toBe(true);
      expect(recon.storedBalance.amount).toBe('20.00');
      await assertGlobalLedgerInvariant();
    } finally {
      await instance2.close();
      await instance3.close();
    }
  });

  it('Scenario 5: Worker crash after DB commit and before SQS ack (redelivery safety via inbox_messages)', async () => {
    const { id: walletId, playerId } = await createWallet('100.00');
    const sqsService = context.module.get(SqsService);
    const consumer = context.module.get(WagerTransactionConsumer);

    const envelope = {
      messageId: `msg-crash-${uuidv4()}`,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-a',
        externalTransactionId: 'bet-crash-1',
        idempotencyKey: 'provider-a:bet-crash-1',
        playerId,
        walletId,
        roundId: 'round-crash',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '35.00', currency: 'BRL' },
      },
    };

    const deleteSpy = jest
      .spyOn(sqsService, 'deleteMessage')
      .mockRejectedValueOnce(new Error('Simulated process kill before SQS ACK'));

    await consumer.processMessage({
      MessageId: 'sqs-delivery-1',
      ReceiptHandle: 'receipt-crash-1',
      Body: JSON.stringify(envelope),
      Attributes: { ApproximateReceiveCount: '1' },
    });

    deleteSpy.mockRestore();

    const deleteOkSpy = jest
      .spyOn(sqsService, 'deleteMessage')
      .mockResolvedValueOnce(undefined);

    await consumer.processMessage({
      MessageId: 'sqs-delivery-2',
      ReceiptHandle: 'receipt-crash-2',
      Body: JSON.stringify(envelope),
      Attributes: { ApproximateReceiveCount: '2' },
    });

    expect(deleteOkSpy).toHaveBeenCalled();
    deleteOkSpy.mockRestore();

    const recon = await walletService.reconcileWallet(walletId);
    expect(recon.consistent).toBe(true);
    expect(recon.storedBalance.amount).toBe('65.00');
    expect(recon.checkedEntries).toBe(2);
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 6: Two concurrent outbox publishers over the same outbox (FOR UPDATE SKIP LOCKED)', async () => {
    const { id: walletId, playerId } = await createWallet('200.00');

    for (let i = 0; i < 6; i++) {
      await request(app.getHttpServer())
        .post('/wagering/transactions')
        .set('Idempotency-Key', `provider-a:outbox-race-${i}`)
        .send({
          providerId: 'provider-a',
          externalTransactionId: `outbox-race-${i}`,
          playerId,
          walletId,
          roundId: `round-ob-${i}`,
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '10.00', currency: 'BRL' },
        })
        .expect(200);
    }

    const sqsService = context.module.get(SqsService);
    const publishedIds: string[] = [];
    const sendSpy = jest
      .spyOn(sqsService, 'sendMessage')
      .mockImplementation(async (_url, _body, _group, dedupId) => {
        await new Promise((r) => setTimeout(r, 10));
        if (dedupId) {
          publishedIds.push(dedupId);
        }
        return dedupId;
      });

    const [count1, count2] = await Promise.all([
      outboxService.publishDueOutboxBatch(4),
      outboxService.publishDueOutboxBatch(4),
    ]);
    const remaining = await outboxService.publishDueOutboxBatch(20);

    sendSpy.mockRestore();

    expect(count1 + count2 + remaining).toBe(12);
    expect(new Set(publishedIds).size).toBe(12);
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 7: Out-of-order execution (ROLLBACK and REFUND arrive before referenced WIN/BET)', async () => {
    const { id: walletId, playerId } = await createWallet('100.00');
    const futureWinExtId = `future-win-${uuidv4().substring(0, 8)}`;
    const rbExtId = `rb-ooo-${uuidv4().substring(0, 8)}`;

    const rbRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', `provider-a:${rbExtId}`)
      .send({
        providerId: 'provider-a',
        externalTransactionId: rbExtId,
        playerId,
        walletId,
        roundId: 'round-ooo',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: futureWinExtId,
        money: { amount: '25.00', currency: 'BRL' },
      })
      .expect(202);

    expect(rbRes.body.status).toBe(WagerTransactionStatus.PendingReference);

    await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', `provider-a:${futureWinExtId}`)
      .send({
        providerId: 'provider-a',
        externalTransactionId: futureWinExtId,
        playerId,
        walletId,
        roundId: 'round-ooo',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Win,
        money: { amount: '25.00', currency: 'BRL' },
      })
      .expect(200);

    const resolvedCount = await wageringService.retryPendingReferences(
      10,
      new Date(Date.now() + 2000),
    );
    expect(resolvedCount).toBe(1);

    const updatedRb = await wageringService.getTransactionById(
      rbRes.body.transactionId,
    );
    expect(updatedRb.status).toBe(WagerTransactionStatus.Processed);

    const recon = await walletService.reconcileWallet(walletId);
    expect(recon.consistent).toBe(true);
    expect(recon.storedBalance.amount).toBe('100.00');
    await assertGlobalLedgerInvariant();
  });

  it('Scenario 8: Service restart recovery with pending reference resolution and final ledger consistency', async () => {
    const { id: walletId, playerId } = await createWallet('100.00');
    const betExtId = `bet-restart-${uuidv4().substring(0, 8)}`;
    const refundExtId = `refund-restart-${uuidv4().substring(0, 8)}`;

    const refundRes = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', `provider-a:${refundExtId}`)
      .send({
        providerId: 'provider-a',
        externalTransactionId: refundExtId,
        playerId,
        walletId,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: betExtId,
        money: { amount: '40.00', currency: 'BRL' },
      })
      .expect(202);

    await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', `provider-a:${betExtId}`)
      .send({
        providerId: 'provider-a',
        externalTransactionId: betExtId,
        playerId,
        walletId,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '40.00', currency: 'BRL' },
      })
      .expect(200);

    const restartedContext = await createTestApp();
    try {
      const restartedWagering = restartedContext.module.get(WageringService);
      const restartedOutbox = restartedContext.module.get(OutboxService);
      const restartedWallet = restartedContext.module.get(WalletService);

      await restartedWagering.retryPendingReferences(
        10,
        new Date(Date.now() + 2000),
      );
      const published = await restartedOutbox.publishDueOutboxBatch(50);
      expect(published).toBeGreaterThan(0);

      const resolvedRefund = await restartedWagering.getTransactionById(
        refundRes.body.transactionId,
      );
      expect(resolvedRefund.status).toBe(WagerTransactionStatus.Processed);

      const recon = await restartedWallet.reconcileWallet(walletId);
      expect(recon.consistent).toBe(true);
      expect(recon.storedBalance.amount).toBe('100.00');
      expect(recon.checkedEntries).toBe(3);
      await assertGlobalLedgerInvariant();
    } finally {
      await restartedContext.close();
    }
  });
});
