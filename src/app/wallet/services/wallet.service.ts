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
