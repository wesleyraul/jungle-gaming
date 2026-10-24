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
