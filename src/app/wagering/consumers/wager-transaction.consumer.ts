import { Message } from '@aws-sdk/client-sqs';
import {
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { DomainError } from '../../../common/domain/domain.error';
import { computeCanonicalPayloadHash } from '../../../common/functions/canonical-hash.function';
import { AppLogger } from '../../../common/logger/app.logger';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { EnvProvider } from '../../../common/providers/env.provider';
import { SqsService } from '../../messaging/services/sqs.service';
import { ProcessWagerTransactionDto } from '../dtos/process-wager-transaction.dto';
import { WageringService } from '../services/wagering.service';

interface SqsWagerEnvelope {
  messageId?: string;
  type?: string;
  occurredAt?: string;
  correlationId?: string;
  data?: ProcessWagerTransactionDto;
}

@Injectable()
export class WagerTransactionConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private isRunning = false;
  private pollPromise: Promise<void> | null = null;

  constructor(
    private readonly sqsService: SqsService,
    private readonly wageringService: WageringService,
    private readonly metricsService: MetricsService,
    private readonly logger: AppLogger,
  ) {}

  onApplicationBootstrap(): void {
    if (!EnvProvider.SQS_CONSUMER_ENABLED) {
      return;
    }
    this.isRunning = true;
    this.pollPromise = this.loop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.isRunning = false;
    if (this.pollPromise) {
      await Promise.race([
        this.pollPromise,
        new Promise((resolve) => setTimeout(resolve, 3000)),
      ]);
    }
  }

  private async loop(): Promise<void> {
    while (this.isRunning) {
      try {
        await this.pollOnce();
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        this.logger.error('Error during SQS polling iteration', errMsg);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  async pollOnce(): Promise<number> {
    const messages = await this.sqsService.receiveMessages(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      10,
      2,
    );

    for (const msg of messages) {
      await this.processMessage(msg);
    }

    return messages.length;
  }

  async processMessage(msg: Message): Promise<void> {
    if (!msg.ReceiptHandle) {
      return;
    }

    const sqsMessageId = msg.MessageId ?? 'unknown';
    const rawBody = msg.Body ?? '';

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(rawBody) as Record<string, unknown>;
    } catch {
      await this.routeToDlqAndAck(
        rawBody,
        sqsMessageId,
        sqsMessageId,
        msg.ReceiptHandle,
        'Unparseable SQS wager message, routing to DLQ',
      );
      return;
    }

    const envelope = parsed as unknown as SqsWagerEnvelope;
    const isEnvelope =
      parsed !== null &&
      typeof parsed === 'object' &&
      'data' in parsed &&
      envelope.data !== null &&
      typeof envelope.data === 'object';

    const payload = (
      isEnvelope ? envelope.data : parsed
    ) as unknown as ProcessWagerTransactionDto;
    const messageId =
      (isEnvelope && typeof envelope.messageId === 'string'
        ? envelope.messageId
        : undefined) ?? sqsMessageId;

    if (!payload || typeof payload !== 'object') {
      await this.routeToDlqAndAck(
        rawBody,
        messageId,
        messageId,
        msg.ReceiptHandle,
        'Invalid SQS wager message structure, routing to DLQ',
      );
      return;
    }

    const idempotencyKey =
      payload.idempotencyKey ||
      (payload.providerId && payload.externalTransactionId
        ? `${payload.providerId}:${payload.externalTransactionId}`
        : '');

    if (
      !idempotencyKey ||
      !payload.providerId ||
      !payload.externalTransactionId ||
      !payload.playerId ||
      !payload.walletId ||
      !payload.roundId ||
      !payload.gameId ||
      !payload.kind ||
      !payload.money
    ) {
      await this.routeToDlqAndAck(
        rawBody,
        payload.walletId ?? messageId,
        messageId,
        msg.ReceiptHandle,
        'Missing required fields in SQS wager message, routing to DLQ',
      );
      return;
    }

    const receiveCount = Number(
      msg.Attributes?.ApproximateReceiveCount ?? '1',
    );
    if (receiveCount > EnvProvider.SQS_MAX_RECEIVE_COUNT) {
      await this.routeToDlqAndAck(
        rawBody,
        payload.walletId,
        messageId,
        msg.ReceiptHandle,
        'SQS message exceeded max receive count, routing to DLQ',
      );
      return;
    }

    try {
      const normalizedPayload: ProcessWagerTransactionDto = {
        ...payload,
        idempotencyKey,
        correlationId: payload.correlationId ?? envelope.correlationId,
      };
      const dto: ProcessWagerTransactionDto = {
        ...normalizedPayload,
        inboxContext: {
          consumerName: 'WagerTransactionConsumer',
          messageId,
          payloadHash: computeCanonicalPayloadHash({
            providerId: normalizedPayload.providerId,
            externalTransactionId: normalizedPayload.externalTransactionId,
            playerId: normalizedPayload.playerId,
            walletId: normalizedPayload.walletId,
            roundId: normalizedPayload.roundId,
            gameId: normalizedPayload.gameId,
            kind: normalizedPayload.kind,
            money: normalizedPayload.money,
            referenceExternalTransactionId:
              normalizedPayload.referenceExternalTransactionId,
          }),
        },
      };
      await this.wageringService.submitTransaction(dto);
      await this.sqsService.deleteMessage(
        EnvProvider.SQS_WAGER_QUEUE_URL,
        msg.ReceiptHandle,
      );
    } catch (error) {
      if (error instanceof DomainError) {
        await this.routeToDlqAndAck(
          rawBody,
          payload.walletId,
          messageId,
          msg.ReceiptHandle,
          `Permanent domain error (${error.code}) on SQS message, routing to DLQ`,
        );
        return;
      }

      this.metricsService.incrementRetries();
      const errMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(
        'Transient failure processing SQS wager message, releasing for retry',
        errMsg,
        { messageId, walletId: payload.walletId },
      );
    }
  }

  private async routeToDlqAndAck(
    rawBody: string,
    groupId: string,
    dedupId: string,
    receiptHandle: string,
    reason: string,
  ): Promise<void> {
    this.logger.error(reason, undefined, {
      messageId: dedupId,
      walletId: groupId,
    });
    this.metricsService.incrementDlqMessages();
    await this.sqsService.sendToDlq(
      EnvProvider.SQS_WAGER_DLQ_URL,
      rawBody,
      groupId,
      dedupId,
    );
    await this.sqsService.deleteMessage(
      EnvProvider.SQS_WAGER_QUEUE_URL,
      receiptHandle,
    );
  }
}
