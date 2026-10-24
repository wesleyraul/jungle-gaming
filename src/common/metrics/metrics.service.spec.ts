import { MetricsService } from './metrics.service';

describe('MetricsService', () => {
  let service: MetricsService;

  beforeEach(() => {
    service = new MetricsService();
  });

  it('tracks transactions by status, duplicates, retries, dlq, lock conflicts, outbox lag, and latency', () => {
    service.incrementTransactionStatus('PROCESSED');
    service.incrementTransactionStatus('PROCESSED');
    service.incrementTransactionStatus('REJECTED');
    service.incrementDuplicatesDetected();
    service.incrementRetries();
    service.incrementDlqMessages();
    service.incrementLockConflicts();
    service.setOutboxLagMs(150);
    service.recordProcessingLatencyMs(12);
    service.recordProcessingLatencyMs(28);
    service.incrementReconciliationDivergence();

    const snapshot = service.getSnapshot();

    expect(snapshot.transactionsByStatus['PROCESSED']).toBe(2);
    expect(snapshot.transactionsByStatus['REJECTED']).toBe(1);
    expect(snapshot.duplicatesDetected).toBe(1);
    expect(snapshot.retries).toBe(1);
    expect(snapshot.dlqMessages).toBe(1);
    expect(snapshot.lockConflicts).toBe(1);
    expect(snapshot.outboxLagMs).toBe(150);
    expect(snapshot.reconciliationDivergences).toBe(1);
    expect(snapshot.processingLatencyMs.count).toBe(2);
    expect(snapshot.processingLatencyMs.avgMs).toBe(20);
  });
});
