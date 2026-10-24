import {
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { AppLogger } from '../../../common/logger/app.logger';
import { EnvProvider } from '../../../common/providers/env.provider';

@Injectable()
export class SqsService implements OnModuleDestroy {
  private readonly client: SQSClient;

  constructor(private readonly logger: AppLogger) {
    this.client = new SQSClient({
      region: EnvProvider.AWS_REGION,
      endpoint: EnvProvider.AWS_ENDPOINT,
      credentials: {
        accessKeyId: EnvProvider.AWS_ACCESS_KEY_ID,
        secretAccessKey: EnvProvider.AWS_SECRET_ACCESS_KEY,
      },
    });
  }

  async sendMessage(
    queueUrl: string,
    body: string,
    messageGroupId?: string,
    deduplicationId?: string,
  ): Promise<string | undefined> {
    const command = new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: body,
      MessageGroupId: messageGroupId,
      MessageDeduplicationId: deduplicationId,
    });
    const result = await this.client.send(command);
    return result.MessageId;
  }

  async receiveMessages(
    queueUrl: string,
    maxMessages = 10,
    waitTimeSeconds = 5,
  ): Promise<Message[]> {
    const command = new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: maxMessages,
      WaitTimeSeconds: waitTimeSeconds,
      AttributeNames: ['All'],
      MessageAttributeNames: ['All'],
    });
    const result = await this.client.send(command);
    return result.Messages ?? [];
  }

  async deleteMessage(
    queueUrl: string,
    receiptHandle: string,
  ): Promise<void> {
    const command = new DeleteMessageCommand({
      QueueUrl: queueUrl,
      ReceiptHandle: receiptHandle,
    });
    await this.client.send(command);
  }

  async sendToDlq(
    dlqUrl: string,
    body: string,
    messageGroupId?: string,
    deduplicationId?: string,
  ): Promise<void> {
    await this.sendMessage(dlqUrl, body, messageGroupId, deduplicationId);
  }

  onModuleDestroy(): void {
    this.client.destroy();
  }
}
