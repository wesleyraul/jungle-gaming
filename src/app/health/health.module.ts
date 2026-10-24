import { Module } from '@nestjs/common';
import { MessagingModule } from '../messaging/messaging.module';
import { HealthController } from './controllers/health.controller';
import { MetricsController } from './controllers/metrics.controller';
import { HealthService } from './services/health.service';

@Module({
  imports: [MessagingModule],
  controllers: [HealthController, MetricsController],
  providers: [HealthService],
  exports: [HealthService],
})
export class HealthModule {}
