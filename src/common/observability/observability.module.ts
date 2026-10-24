import { Global, Module } from '@nestjs/common';
import { AppLogger } from '../logger/app.logger';
import { MetricsService } from '../metrics/metrics.service';

@Global()
@Module({
  providers: [MetricsService, AppLogger],
  exports: [MetricsService, AppLogger],
})
export class ObservabilityModule {}
