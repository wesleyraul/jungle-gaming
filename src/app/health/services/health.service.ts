import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { EnvProvider } from '../../../common/providers/env.provider';
import { SqsService } from '../../messaging/services/sqs.service';
import { HealthStatusResponse } from '../responses/health.response';

@Injectable()
export class HealthService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly sqsService: SqsService,
  ) {}

  checkLiveness(): HealthStatusResponse {
    return {
      status: 'UP',
      timestamp: new Date().toISOString(),
    };
  }

  async checkReadiness(): Promise<HealthStatusResponse> {
    let dbStatus: 'UP' | 'DOWN' = 'UP';
    let sqsStatus: 'UP' | 'DOWN' = 'UP';

    try {
      await this.dataSource.query('SELECT 1');
    } catch {
      dbStatus = 'DOWN';
    }

    try {
      await this.sqsService.receiveMessages(
        EnvProvider.SQS_WAGER_QUEUE_URL,
        1,
        0,
      );
    } catch {
      sqsStatus = 'DOWN';
    }

    const overall = dbStatus === 'UP' && sqsStatus === 'UP' ? 'UP' : 'DOWN';

    return {
      status: overall,
      timestamp: new Date().toISOString(),
      checks: {
        database: dbStatus,
        sqs: sqsStatus,
      },
    };
  }
}
