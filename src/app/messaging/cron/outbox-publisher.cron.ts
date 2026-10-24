import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { AppLogger } from '../../../common/logger/app.logger';
import { EnvProvider } from '../../../common/providers/env.provider';
import { OutboxService } from '../services/outbox.service';

@Injectable()
export class OutboxPublisherCron {
  private isRunning = false;

  constructor(
    private readonly outboxService: OutboxService,
    private readonly logger: AppLogger,
  ) {}

  @Cron(CronExpression.EVERY_SECOND)
  async handleCron(): Promise<void> {
    if (!EnvProvider.WORKERS_ENABLED || this.isRunning) {
      return;
    }

    this.isRunning = true;
    try {
      await this.outboxService.publishDueOutboxBatch(50);
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      this.logger.error('Error during outbox publisher cron cycle', errMsg);
    } finally {
      this.isRunning = false;
    }
  }
}
