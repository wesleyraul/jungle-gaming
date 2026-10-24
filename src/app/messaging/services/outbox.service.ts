import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { OutboxRepository } from '../repositories/outbox.repository';
import { SqsService } from './sqs.service';

@Injectable()
export class OutboxService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly outboxRepository: OutboxRepository,
    private readonly sqsService: SqsService,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  async publishDueOutboxBatch(batchSize = 50): Promise<number> {
    const now = new Date();
    let publishedCount = 0;

    await this.dataSource.transaction(async (manager) => {
      const messages = await this.outboxRepository.claimDueBatchForUpdate(
        batchSize,
        now,
        manager,
      );

      for (const msg of messages) {
        try {
          const body = JSON.stringify(msg.payload);

          await this.sqsService.sendMessage(
            EnvProvider.SQS_EVENTS_QUEUE_URL,
            body,
            msg.aggregateId,
            msg.id,
          );

          msg.markPublished(new Date());
          publishedCount++;
        } catch {
          msg.scheduleRetry(new Date());
          this.metricsService.incrementRetries();
          this.logger.warn('Failed to publish outbox message', {
            messageId: msg.id,
          });
        }
        await this.outboxRepository.save(msg, manager);
      }
    });

    const oldest = await this.outboxRepository.getOldestUnpublishedOccurredAt();
    if (oldest) {
      const lagMs = Math.max(0, Date.now() - oldest.getTime());
      this.metricsService.setOutboxLagMs(lagMs);
    } else {
      this.metricsService.setOutboxLagMs(0);
    }

    return publishedCount;
  }
}
