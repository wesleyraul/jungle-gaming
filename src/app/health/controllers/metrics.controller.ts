import { Controller, Get, Header } from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { MetricsService, MetricsSnapshot } from '../../../common/metrics/metrics.service';

@ApiTags('Metrics')
@Controller('metrics')
export class MetricsController {
  constructor(private readonly metricsService: MetricsService) {}

  @Get()
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  @ApiOperation({ summary: 'Prometheus metrics exposition' })
  @ApiResponse({ status: 200, description: 'Prometheus metrics output' })
  getPrometheusMetrics(): string {
    return this.metricsService.toPrometheusFormat();
  }

  @Get('json')
  @ApiOperation({ summary: 'Structured JSON metrics snapshot' })
  @ApiResponse({ status: 200, description: 'Metrics snapshot' })
  getJsonMetrics(): MetricsSnapshot {
    return this.metricsService.getSnapshot();
  }
}
