import { Column, Entity, PrimaryColumn } from 'typeorm';
import { OutboxMessage } from '../domain/outbox-message.domain';

@Entity({ name: 'outbox_messages' })
export class OutboxMessageEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'aggregate_id', type: 'varchar', length: 128 })
  aggregateId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 128 })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: Record<string, unknown>;

  @Column({ name: 'occurred_at', type: 'timestamptz' })
  occurredAt: Date;

  @Column({ type: 'integer', default: 0 })
  attempts: number;

  @Column({ name: 'next_attempt_at', type: 'timestamptz', nullable: true })
  nextAttemptAt: Date | null;

  @Column({ name: 'published_at', type: 'timestamptz', nullable: true })
  publishedAt: Date | null;

  static fromDomain(outbox: OutboxMessage): OutboxMessageEntity {
    const entity = new OutboxMessageEntity();
    entity.id = outbox.id;
    entity.aggregateId = outbox.aggregateId;
    entity.eventType = outbox.eventType;
    entity.payload = { ...outbox.payload };
    entity.occurredAt = outbox.occurredAt;
    entity.attempts = outbox.attempts;
    entity.nextAttemptAt = outbox.nextAttemptAt ?? null;
    entity.publishedAt = outbox.publishedAt ?? null;
    return entity;
  }

  toDomain(): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload: this.payload,
      occurredAt: new Date(this.occurredAt),
      attempts: Number(this.attempts),
      nextAttemptAt: this.nextAttemptAt
        ? new Date(this.nextAttemptAt)
        : undefined,
      publishedAt: this.publishedAt ? new Date(this.publishedAt) : undefined,
    });
  }
}
