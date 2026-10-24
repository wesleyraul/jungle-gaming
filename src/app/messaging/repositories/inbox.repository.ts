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
