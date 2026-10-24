import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HealthModule } from './app/health/health.module';
import { MessagingModule } from './app/messaging/messaging.module';
import { WageringModule } from './app/wagering/wagering.module';
import { WalletModule } from './app/wallet/wallet.module';
import { dataSourceOptions } from './common/database/data-source';
import { ObservabilityModule } from './common/observability/observability.module';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      ...dataSourceOptions,
      autoLoadEntities: true,
    }),
    ScheduleModule.forRoot(),
    ObservabilityModule,
    WalletModule,
    MessagingModule,
    WageringModule,
    HealthModule,
  ],
})
export class AppModule {}
