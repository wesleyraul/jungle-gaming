import { Message } from '@aws-sdk/client-sqs';
import { InvalidMoneyError } from '../../../common/domain/domain.error';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { SqsService } from '../../messaging/services/sqs.service';
import { WageringService } from '../services/wagering.service';
import { WagerTransactionConsumer } from './wager-transaction.consumer';

describe('WagerTransactionConsumer Unit Tests', () => {
  let consumer: WagerTransactionConsumer;
  let sqsService: jest.Mocked<SqsService>;
  let wageringService: jest.Mocked<WageringService>;
  let metricsService: MetricsService;
  let logger: AppLogger;

  beforeEach(() => {
    sqsService = {
      receiveMessages: jest.fn(),
      sendMessage: jest.fn(),
      deleteMessage: jest.fn().mockResolvedValue(undefined),
      sendToDlq: jest.fn().mockResolvedValue(undefined),
      onModuleDestroy: jest.fn(),
    } as unknown as jest.Mocked<SqsService>;

    wageringService = {
      submitTransaction: jest.fn().mockResolvedValue({ status: 'PROCESSED' }),
    } as unknown as jest.Mocked<WageringService>;

    metricsService = new MetricsService();
    logger = new AppLogger();

    consumer = new WagerTransactionConsumer(
      sqsService,
      wageringService,
      metricsService,
      logger,
    );
  });

  const validPayload = {
    idempotencyKey: 'provider-a:bet-100',
    providerId: 'provider-a',
    externalTransactionId: 'bet-100',
    playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
    walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
    roundId: 'round-1',
    gameId: 'fortune-tiger',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
  };

  it('processes valid flat message and deletes it from source queue', async () => {
    const msg: Message = {
      MessageId: 'msg-1',
      ReceiptHandle: 'receipt-1',
      Body: JSON.stringify(validPayload),
      Attributes: { ApproximateReceiveCount: '1' },
    };

    await consumer.processMessage(msg);

    expect(wageringService.submitTransaction).toHaveBeenCalledWith(
      expect.objectContaining({
        ...validPayload,
        inboxContext: {
          consumerName: 'WagerTransactionConsumer',
          messageId: 'msg-1',
          payloadHash: expect.any(String),
        },
      }),
    );
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      'receipt-1',
    );
    expect(sqsService.sendToDlq).not.toHaveBeenCalled();
  });

  it('unwraps Section 10 WagerTransactionRequested envelope and uses envelope messageId', async () => {
    const envelope = {
      messageId: 'msg-123',
      type: 'WagerTransactionRequested',
      occurredAt: '2026-07-29T15:00:00.000Z',
      data: validPayload,
    };
    const msg: Message = {
      MessageId: 'sqs-internal-id',
      ReceiptHandle: 'receipt-env-1',
      Body: JSON.stringify(envelope),
      Attributes: { ApproximateReceiveCount: '1' },
    };

    await consumer.processMessage(msg);

    expect(wageringService.submitTransaction).toHaveBeenCalledWith(
      expect.objectContaining({
        ...validPayload,
        inboxContext: {
          consumerName: 'WagerTransactionConsumer',
          messageId: 'msg-123',
          payloadHash: expect.any(String),
        },
      }),
    );
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      'receipt-env-1',
    );
  });

  it('routes malformed JSON to DLQ and deletes from source queue', async () => {
    const msg: Message = {
      MessageId: 'msg-bad',
      ReceiptHandle: 'receipt-bad',
      Body: 'invalid-json{',
      Attributes: { ApproximateReceiveCount: '1' },
    };

    await consumer.processMessage(msg);

    expect(wageringService.submitTransaction).not.toHaveBeenCalled();
    expect(sqsService.sendToDlq).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_DLQ_URL,
      'invalid-json{',
      'msg-bad',
      'msg-bad',
    );
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      'receipt-bad',
    );
    expect(metricsService.getSnapshot().dlqMessages).toBe(1);
  });

  it('routes permanent DomainError (e.g. InvalidMoneyError) directly to DLQ without retry', async () => {
    wageringService.submitTransaction.mockRejectedValueOnce(
      new InvalidMoneyError('Negative amount'),
    );

    const msg: Message = {
      MessageId: 'msg-domain-err',
      ReceiptHandle: 'receipt-domain-err',
      Body: JSON.stringify(validPayload),
      Attributes: { ApproximateReceiveCount: '1' },
    };

    await consumer.processMessage(msg);

    expect(sqsService.sendToDlq).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_DLQ_URL,
      JSON.stringify(validPayload),
      validPayload.walletId,
      'msg-domain-err',
    );
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      'receipt-domain-err',
    );
    expect(metricsService.getSnapshot().dlqMessages).toBe(1);
  });

  it('routes message exceeding max receive count to DLQ and deletes from source queue', async () => {
    const msg: Message = {
      MessageId: 'msg-retry-exhausted',
      ReceiptHandle: 'receipt-retry',
      Body: JSON.stringify(validPayload),
      Attributes: { ApproximateReceiveCount: '4' },
    };

    await consumer.processMessage(msg);

    expect(wageringService.submitTransaction).not.toHaveBeenCalled();
    expect(sqsService.sendToDlq).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_DLQ_URL,
      JSON.stringify(validPayload),
      validPayload.walletId,
      'msg-retry-exhausted',
    );
    expect(sqsService.deleteMessage).toHaveBeenCalledWith(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      'receipt-retry',
    );
  });

  it('leaves message in queue for retry when submitTransaction throws transient infrastructure error', async () => {
    wageringService.submitTransaction.mockRejectedValueOnce(
      new Error('DB connection lost'),
    );

    const msg: Message = {
      MessageId: 'msg-fail',
      ReceiptHandle: 'receipt-fail',
      Body: JSON.stringify(validPayload),
      Attributes: { ApproximateReceiveCount: '1' },
    };

    await consumer.processMessage(msg);

    expect(sqsService.deleteMessage).not.toHaveBeenCalled();
    expect(sqsService.sendToDlq).not.toHaveBeenCalled();
    expect(metricsService.getSnapshot().retries).toBe(1);
  });
});
