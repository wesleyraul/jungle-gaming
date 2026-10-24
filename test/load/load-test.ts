import { v4 as uuidv4 } from 'uuid';
import { WagerTransactionKind } from '../../src/common/enums/wager-transaction-kind.enum';
import { createTestApp } from '../helpers/test-app.helper';
import request from 'supertest';

async function runLoadTest(): Promise<void> {
  process.stdout.write('Starting Wagering Processor Load Test...\n');
  const context = await createTestApp();
  const app = context.app;

  await context.cleanDatabase();

  const playerId = uuidv4();
  const createWalletRes = await request(app.getHttpServer())
    .post('/wallets')
    .send({
      playerId,
      initialBalance: { amount: '100000.00', currency: 'BRL' },
    });

  const walletId = createWalletRes.body.id;
  process.stdout.write(`Initialized test wallet: ${walletId}\n`);

  const TOTAL_TRANSACTIONS = 100;
  const CONCURRENCY = 10;
  const latencies: number[] = [];

  const startTime = Date.now();
  let completed = 0;
  let successCount = 0;

  for (let i = 0; i < TOTAL_TRANSACTIONS; i += CONCURRENCY) {
    const batch = Array.from({ length: Math.min(CONCURRENCY, TOTAL_TRANSACTIONS - i) }).map(
      async (_, index) => {
        const opIndex = i + index;
        const extId = `load-tx-${opIndex}-${uuidv4().substring(0, 6)}`;
        const kind = opIndex % 3 === 0 ? WagerTransactionKind.Win : WagerTransactionKind.Bet;
        const opStart = Date.now();

        const res = await request(app.getHttpServer())
          .post('/wager-transactions')
          .set('Idempotency-Key', `load-test:${extId}`)
          .send({
            providerId: 'load-provider',
            externalTransactionId: extId,
            playerId,
            walletId,
            roundId: `round-${opIndex}`,
            gameId: 'load-slot',
            kind,
            money: { amount: '5.00', currency: 'BRL' },
          });

        const duration = Date.now() - opStart;
        latencies.push(duration);
        completed++;
        if (res.status === 200) {
          successCount++;
        }
      },
    );
    await Promise.all(batch);
  }

  const totalTimeSec = (Date.now() - startTime) / 1000;
  const rps = Math.round(completed / totalTimeSec);

  latencies.sort((a, b) => a - b);
  const min = latencies[0];
  const max = latencies[latencies.length - 1];
  const avg = Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  const reconRes = await request(app.getHttpServer()).get(
    `/wallets/${walletId}/reconciliation`,
  );
  const metricsRes = await request(app.getHttpServer()).get('/metrics/json');

  process.stdout.write('\n=== LOAD TEST RESULTS ===\n');
  process.stdout.write(`Total requests:      ${completed}\n`);
  process.stdout.write(`Success rate:        ${((successCount / completed) * 100).toFixed(1)}%\n`);
  process.stdout.write(`Error rate:          ${(((completed - successCount) / completed) * 100).toFixed(1)}%\n`);
  process.stdout.write(`Throughput (RPS):    ${rps} req/sec\n`);
  process.stdout.write(`Latency min / max:   ${min}ms / ${max}ms\n`);
  process.stdout.write(`Latency avg / p50:   ${avg}ms / ${p50}ms\n`);
  process.stdout.write(`Latency p95 / p99:   ${p95}ms / ${p99}ms\n`);
  process.stdout.write(`Lock conflicts:      ${metricsRes.body.lockConflicts}\n`);
  process.stdout.write(`Outbox lag:          ${metricsRes.body.outboxLagMs}ms\n`);
  process.stdout.write(`Reconciliation:      ${reconRes.body.consistent ? 'CONSISTENT (0 DIVERGENCE)' : 'DIVERGENCE DETECTED'}\n`);
  process.stdout.write(`Stored balance:      ${reconRes.body.storedBalance.amount} BRL\n`);
  process.stdout.write(`Calculated balance:  ${reconRes.body.calculatedBalance.amount} BRL\n`);
  process.stdout.write(`Checked entries:     ${reconRes.body.checkedEntries}\n`);
  process.stdout.write('=========================\n\n');

  await context.close();
}

void runLoadTest();
