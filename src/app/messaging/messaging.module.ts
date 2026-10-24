import { Module } from '@nestjs/common';
import { OutboxPublisherCron } from './cron/outbox-publisher.cron';
import { InboxRepository } from './repositories/inbox.repository';
import { OutboxRepository } from './repositories/outbox.repository';
import { OutboxService } from './services/outbox.service';
import { SqsService } from './services/sqs.service';

@Module({
  providers: [
    OutboxService,
    SqsService,
    OutboxRepository,
    InboxRepository,
    OutboxPublisherCron,
  ],
  exports: [OutboxService, SqsService],
})
export class MessagingModule {}
