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
    if (!id) {
      return undefined;
    }
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
    if (!idempotencyKey) {
      return undefined;
    }
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
    if (!providerId || !externalTransactionId) {
      return undefined;
    }
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
