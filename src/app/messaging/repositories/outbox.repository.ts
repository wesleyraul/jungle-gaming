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
    await repo.save(messages.map((m) => OutboxMessageEntity.fromDomain(m)));
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
