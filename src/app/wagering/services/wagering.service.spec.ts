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
    wagerRepo.findByProviderAndExternalId.mockImplementation(
      async (_providerId, extId) => (extId === 'win-1' ? refWin : undefined),
    );

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
