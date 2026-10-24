import { DataSource, DataSourceOptions } from 'typeorm';
import { EnvProvider } from '../providers/env.provider';
import { WalletEntity } from '../../app/wallet/entities/wallet.entity';
import { WalletLedgerEntryEntity } from '../../app/wallet/entities/wallet-ledger-entry.entity';
import { WagerTransactionEntity } from '../../app/wagering/entities/wager-transaction.entity';
import { InboxMessageEntity } from '../../app/messaging/entities/inbox-message.entity';
import { OutboxMessageEntity } from '../../app/messaging/entities/outbox-message.entity';
import { InitialWageringSchema1761130000000 } from './migrations/1761130000000-InitialWageringSchema';

EnvProvider.initialize();

export const dataSourceOptions: DataSourceOptions = {
  type: 'postgres',
  host: EnvProvider.DB_HOST,
  port: EnvProvider.DB_PORT,
  username: EnvProvider.DB_USER,
  password: EnvProvider.DB_PASS,
  database: EnvProvider.DB_NAME,
  entities: [
    WalletEntity,
    WalletLedgerEntryEntity,
    WagerTransactionEntity,
    InboxMessageEntity,
    OutboxMessageEntity,
  ],
  migrations: [InitialWageringSchema1761130000000],
  synchronize: false,
  logging: false,
};

export const AppDataSource = new DataSource(dataSourceOptions);
