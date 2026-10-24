import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { DataSource } from 'typeorm';
import { AppModule } from '../../src/app.module';
import { DomainExceptionFilter } from '../../src/common/filters/domain-exception.filter';
import { RequestInterceptor } from '../../src/common/interceptors/request.interceptor';
import { EnvProvider } from '../../src/common/providers/env.provider';

export interface TestAppContext {
  app: INestApplication;
  module: TestingModule;
  dataSource: DataSource;
  cleanDatabase: () => Promise<void>;
  close: () => Promise<void>;
}

export async function createTestApp(): Promise<TestAppContext> {
  EnvProvider.initialize();
  EnvProvider.WORKERS_ENABLED = false;
  EnvProvider.SQS_CONSUMER_ENABLED = false;

  const moduleRef = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();

  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );
  app.useGlobalFilters(new DomainExceptionFilter());
  app.useGlobalInterceptors(new RequestInterceptor());

  await app.init();
  app.getHttpServer().setMaxListeners(0);

  const dataSource = app.get(DataSource);
  await dataSource.runMigrations();

  const cleanDatabase = async () => {
    await dataSource.query(
      'TRUNCATE TABLE outbox_messages, inbox_messages, wager_transactions, wallet_ledger_entries, wallets CASCADE',
    );
  };

  const close = async () => {
    await app.close();
  };

  return { app, module: moduleRef, dataSource, cleanDatabase, close };
}
