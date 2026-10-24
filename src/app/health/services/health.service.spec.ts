import { DataSource } from 'typeorm';
import { SqsService } from '../../messaging/services/sqs.service';
import { HealthService } from './health.service';

describe('HealthService Unit Tests', () => {
  let healthService: HealthService;
  let dataSource: jest.Mocked<DataSource>;
  let sqsService: jest.Mocked<SqsService>;

  beforeEach(() => {
    dataSource = {
      query: jest.fn().mockResolvedValue([{ '?column?': 1 }]),
    } as unknown as jest.Mocked<DataSource>;

    sqsService = {
      receiveMessages: jest.fn().mockResolvedValue([]),
    } as unknown as jest.Mocked<SqsService>;

    healthService = new HealthService(dataSource, sqsService);
  });

  it('reports liveness UP', () => {
    const res = healthService.checkLiveness();
    expect(res.status).toBe('UP');
    expect(res.timestamp).toBeDefined();
  });

  it('reports readiness UP when all dependencies are healthy', async () => {
    const res = await healthService.checkReadiness();
    expect(res.status).toBe('UP');
    expect(res.checks?.database).toBe('UP');
    expect(res.checks?.sqs).toBe('UP');
  });

  it('reports readiness DOWN when database fails', async () => {
    dataSource.query.mockRejectedValueOnce(new Error('Connection refused'));
    const res = await healthService.checkReadiness();
    expect(res.status).toBe('DOWN');
    expect(res.checks?.database).toBe('DOWN');
    expect(res.checks?.sqs).toBe('UP');
  });

  it('reports readiness DOWN when SQS fails', async () => {
    sqsService.receiveMessages.mockRejectedValueOnce(new Error('SQS unreachable'));
    const res = await healthService.checkReadiness();
    expect(res.status).toBe('DOWN');
    expect(res.checks?.database).toBe('UP');
    expect(res.checks?.sqs).toBe('DOWN');
  });
});
