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

    if (!dto.inboxContext) {
      const preExisting =
        await this.wagerTransactionRepository.findByIdempotencyKey(
          dto.idempotencyKey,
        );
      if (preExisting) {
        return this.handleExistingTransaction(preExisting, payloadHash);
      }
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

      if (!referenceTx) {
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

    if (
      tx.kind === WagerTransactionKind.Win ||
      tx.kind === WagerTransactionKind.Loss
    ) {
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

  async retryPendingReferences(
    batchSize = 25,
    now: Date = new Date(),
  ): Promise<number> {
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

        if (refTx && refTx.isTerminal()) {
          const result = await this.applyTransactionOnLockedWallet(
            tx,
            wallet,
            uuidv4(),
            manager,
          );
          this.metricsService.incrementTransactionStatus(result.status);
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
