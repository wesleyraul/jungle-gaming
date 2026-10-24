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
