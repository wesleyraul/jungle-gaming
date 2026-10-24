import * as dotenv from 'dotenv';

dotenv.config();

export class EnvProvider {
  static NODE_ENV = process.env.NODE_ENV || 'development';
  static PORT = Number(process.env.PORT || '3000');
  static DB_HOST = process.env.DB_HOST || 'localhost';
  static DB_PORT = Number(process.env.DB_PORT || '5432');
  static DB_USER = process.env.DB_USER || 'jungle';
  static DB_PASS = process.env.DB_PASS || 'jungle_password';
  static DB_NAME = process.env.DB_NAME || 'jungle_wagering';
  static AWS_REGION = process.env.AWS_REGION || 'us-east-1';
  static AWS_ENDPOINT = process.env.AWS_ENDPOINT || 'http://localhost:4566';
  static AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'test';
  static AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || 'test';
  static SQS_WAGER_QUEUE_URL =
    process.env.SQS_WAGER_QUEUE_URL ||
    'http://localhost:4566/000000000000/wager-transactions.fifo';
  static SQS_WAGER_DLQ_URL =
    process.env.SQS_WAGER_DLQ_URL ||
    'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
  static SQS_EVENTS_QUEUE_URL =
    process.env.SQS_EVENTS_QUEUE_URL ||
    'http://localhost:4566/000000000000/integration-events.fifo';
  static SQS_CONSUMER_ENABLED =
    (process.env.SQS_CONSUMER_ENABLED || 'true') === 'true';
  static WORKERS_ENABLED = (process.env.WORKERS_ENABLED || 'true') === 'true';
  static PENDING_REF_MAX_ATTEMPTS = Number(
    process.env.PENDING_REF_MAX_ATTEMPTS || '5',
  );
  static PENDING_REF_TTL_MS = Number(
    process.env.PENDING_REF_TTL_MS || '60000',
  );
  static SQS_MAX_RECEIVE_COUNT = Number(
    process.env.SQS_MAX_RECEIVE_COUNT || '3',
  );

  static initialize(): void {
    EnvProvider.NODE_ENV = process.env.NODE_ENV || 'development';
    EnvProvider.PORT = Number(process.env.PORT || '3000');
    EnvProvider.DB_HOST = process.env.DB_HOST || 'localhost';
    EnvProvider.DB_PORT = Number(process.env.DB_PORT || '5432');
    EnvProvider.DB_USER = process.env.DB_USER || 'jungle';
    EnvProvider.DB_PASS = process.env.DB_PASS || 'jungle_password';
    EnvProvider.DB_NAME = process.env.DB_NAME || 'jungle_wagering';
    EnvProvider.AWS_REGION = process.env.AWS_REGION || 'us-east-1';
    EnvProvider.AWS_ENDPOINT =
      process.env.AWS_ENDPOINT || 'http://localhost:4566';
    EnvProvider.SQS_WAGER_QUEUE_URL =
      process.env.SQS_WAGER_QUEUE_URL ||
      'http://localhost:4566/000000000000/wager-transactions.fifo';
    EnvProvider.SQS_WAGER_DLQ_URL =
      process.env.SQS_WAGER_DLQ_URL ||
      'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
    EnvProvider.SQS_EVENTS_QUEUE_URL =
      process.env.SQS_EVENTS_QUEUE_URL ||
      'http://localhost:4566/000000000000/integration-events.fifo';
    EnvProvider.SQS_CONSUMER_ENABLED =
      (process.env.SQS_CONSUMER_ENABLED || 'true') === 'true';
    EnvProvider.WORKERS_ENABLED =
      (process.env.WORKERS_ENABLED || 'true') === 'true';
  }
}
