import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { Wallet } from '../domain/wallet.domain';

@Entity({ name: 'wallets' })
export class WalletEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'player_id', type: 'varchar', length: 128 })
  playerId: string;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ name: 'balance_amount', type: 'numeric', precision: 20, scale: 2 })
  balanceAmount: string;

  @Column({ type: 'integer', default: 1 })
  version: number;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  static fromDomain(wallet: Wallet): WalletEntity {
    const entity = new WalletEntity();
    entity.id = wallet.id;
    entity.playerId = wallet.playerId;
    entity.currency = wallet.currency;
    entity.balanceAmount = wallet.balance.toString();
    entity.version = wallet.version;
    entity.createdAt = wallet.createdAt;
    entity.updatedAt = wallet.updatedAt;
    return entity;
  }

  toDomain(): Wallet {
    return Wallet.rehydrate({
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balance: Money.rehydrate({
        amount: Number.isNaN(Number(this.balanceAmount))
          ? this.balanceAmount
          : Number(this.balanceAmount).toFixed(2),
        currency: this.currency,
      }),
      version: Number(this.version),
      createdAt: new Date(this.createdAt),
      updatedAt: new Date(this.updatedAt),
    });
  }
}
