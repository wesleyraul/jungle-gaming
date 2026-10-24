import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';

@Entity({ name: 'wallet_ledger_entries' })
export class WalletLedgerEntryEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'wallet_id', type: 'uuid' })
  walletId: string;

  @Column({ name: 'transaction_id', type: 'uuid' })
  transactionId: string;

  @Column({ type: 'varchar', length: 16 })
  direction: LedgerDirection;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ type: 'numeric', precision: 20, scale: 2 })
  amount: string;

  @Column({ name: 'balance_before', type: 'numeric', precision: 20, scale: 2 })
  balanceBefore: string;

  @Column({ name: 'balance_after', type: 'numeric', precision: 20, scale: 2 })
  balanceAfter: string;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  static fromDomain(entry: WalletLedgerEntry): WalletLedgerEntryEntity {
    const entity = new WalletLedgerEntryEntity();
    entity.id = entry.id;
    entity.walletId = entry.walletId;
    entity.transactionId = entry.transactionId;
    entity.direction = entry.direction;
    entity.currency = entry.money.currency;
    entity.amount = entry.money.toString();
    entity.balanceBefore = entry.balanceBefore.toString();
    entity.balanceAfter = entry.balanceAfter.toString();
    entity.createdAt = entry.createdAt;
    return entity;
  }

  toDomain(): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: this.id,
      walletId: this.walletId,
      transactionId: this.transactionId,
      direction: this.direction,
      money: Money.rehydrate({
        amount: Number(this.amount).toFixed(2),
        currency: this.currency,
      }),
      balanceBefore: Money.rehydrate({
        amount: Number(this.balanceBefore).toFixed(2),
        currency: this.currency,
      }),
      balanceAfter: Money.rehydrate({
        amount: Number(this.balanceAfter).toFixed(2),
        currency: this.currency,
      }),
      createdAt: new Date(this.createdAt),
    });
  }
}
