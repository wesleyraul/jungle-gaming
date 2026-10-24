import { Module } from '@nestjs/common';
import { MessagingModule } from '../messaging/messaging.module';
import { InboxRepository } from '../messaging/repositories/inbox.repository';
import { OutboxRepository } from '../messaging/repositories/outbox.repository';
import { WalletModule } from '../wallet/wallet.module';
import { WagerTransactionConsumer } from './consumers/wager-transaction.consumer';
import { ProviderWageringController } from './controllers/provider-wagering.controller';
import { WageringController } from './controllers/wagering.controller';
import { PendingReferenceCron } from './cron/pending-reference.cron';
import { WagerTransactionRepository } from './repositories/wager-transaction.repository';
import { WageringService } from './services/wagering.service';

@Module({
  imports: [MessagingModule, WalletModule],
  controllers: [WageringController, ProviderWageringController],
  providers: [
    WageringService,
    WagerTransactionRepository,
    OutboxRepository,
    InboxRepository,
    WagerTransactionConsumer,
    PendingReferenceCron,
  ],
  exports: [WageringService],
})
export class WageringModule {}
