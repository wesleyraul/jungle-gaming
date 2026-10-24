import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';
import { DomainExceptionFilter } from './common/filters/domain-exception.filter';
import { RequestInterceptor } from './common/interceptors/request.interceptor';
import { AppLogger } from './common/logger/app.logger';
import { EnvProvider } from './common/providers/env.provider';

async function bootstrap() {
  EnvProvider.initialize();
  const logger = new AppLogger();

  const app = await NestFactory.create(AppModule, {
    logger,
    bufferLogs: true,
  });

  app.enableShutdownHooks();

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );

  app.useGlobalFilters(new DomainExceptionFilter());
  app.useGlobalInterceptors(new RequestInterceptor());

  const config = new DocumentBuilder()
    .setTitle('Distributed Wagering Processor')
    .setDescription(
      'High-throughput distributed wagering processor with pessimistic locking, transactional outbox, and idempotent ledger',
    )
    .setVersion('1.0.0')
    .addApiKey(
      { type: 'apiKey', name: 'x-provider-token', in: 'header' },
      'x-provider-token',
    )
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document);

  await app.listen(EnvProvider.PORT);
  logger.log(`Application is running on port ${EnvProvider.PORT}`);
}

void bootstrap();
