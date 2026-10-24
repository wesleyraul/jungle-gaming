import { Module } from '@nestjs/common';
import { WalletController } from './controllers/wallet.controller';
import { WalletLedgerRepository } from './repositories/wallet-ledger.repository';
import { WalletRepository } from './repositories/wallet.repository';
import { WalletService } from './services/wallet.service';

@Module({
  controllers: [WalletController],
  providers: [WalletService, WalletRepository, WalletLedgerRepository],
  exports: [WalletService],
})
export class WalletModule {}
