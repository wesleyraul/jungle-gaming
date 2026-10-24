import { Injectable } from '@nestjs/common';

export interface LatencyMetrics {
  count: number;
  totalMs: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
}

export interface MetricsSnapshot {
  transactionsByStatus: Record<string, number>;
  duplicatesDetected: number;
  retries: number;
  dlqMessages: number;
  lockConflicts: number;
  outboxLagMs: number;
  reconciliationDivergences: number;
  processingLatencyMs: LatencyMetrics;
}

@Injectable()
export class MetricsService {
  private readonly transactionsByStatus: Record<string, number> = {
    PENDING: 0,
    PENDING_REFERENCE: 0,
    PROCESSED: 0,
    REJECTED: 0,
    FAILED: 0,
  };
  private duplicatesDetected = 0;
  private retries = 0;
  private dlqMessages = 0;
  private lockConflicts = 0;
  private outboxLagMs = 0;
  private reconciliationDivergences = 0;
  private latencyCount = 0;
  private latencyTotalMs = 0;
  private latencyMinMs = 0;
  private latencyMaxMs = 0;

  incrementTransactionStatus(status: string): void {
    this.transactionsByStatus[status] =
      (this.transactionsByStatus[status] ?? 0) + 1;
  }

  incrementDuplicatesDetected(): void {
    this.duplicatesDetected += 1;
  }

  incrementRetries(): void {
    this.retries += 1;
  }

  incrementDlqMessages(): void {
    this.dlqMessages += 1;
  }

  incrementLockConflicts(): void {
    this.lockConflicts += 1;
  }

  setOutboxLagMs(lagMs: number): void {
    this.outboxLagMs = Math.max(0, Math.round(lagMs));
  }

  incrementReconciliationDivergence(): void {
    this.reconciliationDivergences += 1;
  }

  recordProcessingLatencyMs(durationMs: number): void {
    const safeDuration = Math.max(0, durationMs);
    this.latencyCount += 1;
    this.latencyTotalMs += safeDuration;
    this.latencyMinMs =
      this.latencyCount === 1
        ? safeDuration
        : Math.min(this.latencyMinMs, safeDuration);
    this.latencyMaxMs = Math.max(this.latencyMaxMs, safeDuration);
  }

  getSnapshot(): MetricsSnapshot {
    return {
      transactionsByStatus: { ...this.transactionsByStatus },
      duplicatesDetected: this.duplicatesDetected,
      retries: this.retries,
      dlqMessages: this.dlqMessages,
      lockConflicts: this.lockConflicts,
      outboxLagMs: this.outboxLagMs,
      reconciliationDivergences: this.reconciliationDivergences,
      processingLatencyMs: {
        count: this.latencyCount,
        totalMs: this.latencyTotalMs,
        minMs: this.latencyMinMs,
        maxMs: this.latencyMaxMs,
        avgMs:
          this.latencyCount > 0
            ? Math.round((this.latencyTotalMs / this.latencyCount) * 100) / 100
            : 0,
      },
    };
  }

  toPrometheusFormat(): string {
    const s = this.getSnapshot();
    const lines: string[] = [];

    lines.push('# HELP transactions_by_status_total Total wager transactions by status');
    lines.push('# TYPE transactions_by_status_total counter');
    for (const [status, count] of Object.entries(s.transactionsByStatus)) {
      lines.push(`transactions_by_status_total{status="${status}"} ${count}`);
    }

    lines.push('# HELP duplicates_detected_total Total idempotent duplicate requests detected');
    lines.push('# TYPE duplicates_detected_total counter');
    lines.push(`duplicates_detected_total ${s.duplicatesDetected}`);

    lines.push('# HELP retries_total Total retry attempts executed');
    lines.push('# TYPE retries_total counter');
    lines.push(`retries_total ${s.retries}`);

    lines.push('# HELP sqs_dead_letter_total Messages routed to dead-letter queue');
    lines.push('# TYPE sqs_dead_letter_total counter');
    lines.push(`sqs_dead_letter_total ${s.dlqMessages}`);

    lines.push('# HELP lock_conflicts_total Lock contention conflicts');
    lines.push('# TYPE lock_conflicts_total counter');
    lines.push(`lock_conflicts_total ${s.lockConflicts}`);

    lines.push('# HELP outbox_lag_seconds Lag between outbox entry creation and publication');
    lines.push('# TYPE outbox_lag_seconds gauge');
    lines.push(`outbox_lag_seconds ${(s.outboxLagMs / 1000).toFixed(3)}`);

    lines.push('# HELP reconciliation_divergences_total Ledger reconciliation divergences detected');
    lines.push('# TYPE reconciliation_divergences_total counter');
    lines.push(`reconciliation_divergences_total ${s.reconciliationDivergences}`);

    lines.push('# HELP processing_latency_ms Transaction processing latency summary');
    lines.push('# TYPE processing_latency_ms summary');
    lines.push(`processing_latency_ms{stat="count"} ${s.processingLatencyMs.count}`);
    lines.push(`processing_latency_ms{stat="avg"} ${s.processingLatencyMs.avgMs}`);
    lines.push(`processing_latency_ms{stat="min"} ${s.processingLatencyMs.minMs}`);
    lines.push(`processing_latency_ms{stat="max"} ${s.processingLatencyMs.maxMs}`);

    return lines.join('\n') + '\n';
  }
}
