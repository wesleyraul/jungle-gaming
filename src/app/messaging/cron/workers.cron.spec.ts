import { AppLogger } from '../../../common/logger/app.logger';
import { EnvProvider } from '../../../common/providers/env.provider';
import { PendingReferenceCron } from '../../wagering/cron/pending-reference.cron';
import { WageringService } from '../../wagering/services/wagering.service';
import { OutboxService } from '../services/outbox.service';
import { OutboxPublisherCron } from './outbox-publisher.cron';

describe('Cron Workers Unit Tests', () => {
  let outboxService: jest.Mocked<OutboxService>;
  let wageringService: jest.Mocked<WageringService>;
  let logger: AppLogger;

  beforeEach(() => {
    EnvProvider.WORKERS_ENABLED = true;
    outboxService = {
      publishDueOutboxBatch: jest.fn().mockResolvedValue(2),
    } as unknown as jest.Mocked<OutboxService>;

    wageringService = {
      retryPendingReferences: jest.fn().mockResolvedValue(1),
    } as unknown as jest.Mocked<WageringService>;

    logger = new AppLogger();
  });

  describe('OutboxPublisherCron', () => {
    it('calls publishDueOutboxBatch when workers are enabled', async () => {
      const cron = new OutboxPublisherCron(outboxService, logger);
      await cron.handleCron();
      expect(outboxService.publishDueOutboxBatch).toHaveBeenCalledWith(50);
    });

    it('skips execution when WORKERS_ENABLED is false', async () => {
      EnvProvider.WORKERS_ENABLED = false;
      const cron = new OutboxPublisherCron(outboxService, logger);
      await cron.handleCron();
      expect(outboxService.publishDueOutboxBatch).not.toHaveBeenCalled();
    });

    it('catches and logs error without throwing', async () => {
      outboxService.publishDueOutboxBatch.mockRejectedValueOnce(
        new Error('Network error'),
      );
      const cron = new OutboxPublisherCron(outboxService, logger);
      await expect(cron.handleCron()).resolves.not.toThrow();
    });
  });

  describe('PendingReferenceCron', () => {
    it('calls retryPendingReferences when workers are enabled', async () => {
      const cron = new PendingReferenceCron(wageringService, logger);
      await cron.handleCron();
      expect(wageringService.retryPendingReferences).toHaveBeenCalledWith(50);
    });

    it('skips execution when WORKERS_ENABLED is false', async () => {
      EnvProvider.WORKERS_ENABLED = false;
      const cron = new PendingReferenceCron(wageringService, logger);
      await cron.handleCron();
      expect(wageringService.retryPendingReferences).not.toHaveBeenCalled();
    });

    it('catches and logs error without throwing', async () => {
      wageringService.retryPendingReferences.mockRejectedValueOnce(
        new Error('DB failure'),
      );
      const cron = new PendingReferenceCron(wageringService, logger);
      await expect(cron.handleCron()).resolves.not.toThrow();
    });
  });
});
