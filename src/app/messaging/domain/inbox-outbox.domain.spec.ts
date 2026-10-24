import { InboxMessage } from './inbox-message.domain';
import { OutboxMessage } from './outbox-message.domain';
import { WalletBalanceChanged } from './events/wallet-balance-changed.event';
import { Wallet } from '../../wallet/domain/wallet.domain';
import { Money } from '../../../common/domain/money.domain';

describe('InboxMessage, OutboxMessage & IntegrationEvent', () => {
  it('tracks InboxMessage processing state', () => {
    const inbox = InboxMessage.receive({
      messageId: 'msg-1',
      consumerName: 'wager-consumer',
      payloadHash: 'hash-1',
    });
    expect(inbox.isProcessed()).toBe(false);

    const now = new Date();
    inbox.markProcessed(now);
    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.processedAt).toEqual(now);
  });

  it('enqueues IntegrationEvent into OutboxMessage and schedules exponential backoff retries', () => {
    const wallet = Wallet.open({
      id: 'w-1',
      playerId: 'p-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });
    const entry = wallet.debit({
      entryId: 'le-1',
      transactionId: 'tx-1',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    const event = WalletBalanceChanged.from(wallet, entry, {
      eventId: 'ev-1',
      correlationId: 'corr-1',
      occurredAt: new Date('2026-10-22T12:00:00.000Z'),
    });

    const outbox = OutboxMessage.enqueue(event);
    expect(outbox.isPending()).toBe(true);
    expect(outbox.eventType).toBe('WalletBalanceChanged');
    expect(outbox.payload.version).toBe(1);

    const retryBase = new Date('2026-10-22T12:00:00.000Z');
    outbox.scheduleRetry(retryBase);
    expect(outbox.attempts).toBe(1);
    expect(outbox.nextAttemptAt?.getTime()).toBeGreaterThan(retryBase.getTime());
    expect(outbox.isDue(retryBase)).toBe(false);

    const publishAt = new Date('2026-10-22T12:01:00.000Z');
    outbox.markPublished(publishAt);
    expect(outbox.isPending()).toBe(false);
    expect(outbox.publishedAt).toEqual(publishAt);
  });
});
