import { Column, Entity, PrimaryColumn } from 'typeorm';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { WagerTransaction } from '../domain/wager-transaction.domain';

@Entity({ name: 'wager_transactions' })
export class WagerTransactionEntity {
  @PrimaryColumn({ type: 'uuid' })
  id: string;

  @Column({ name: 'provider_id', type: 'varchar', length: 128 })
  providerId: string;

  @Column({ name: 'external_transaction_id', type: 'varchar', length: 128 })
  externalTransactionId: string;

  @Column({ name: 'idempotency_key', type: 'varchar', length: 255 })
  idempotencyKey: string;

  @Column({ name: 'payload_hash', type: 'varchar', length: 64 })
  payloadHash: string;

  @Column({ name: 'wallet_id', type: 'uuid' })
  walletId: string;

  @Column({ name: 'player_id', type: 'varchar', length: 128 })
  playerId: string;

  @Column({ name: 'round_id', type: 'varchar', length: 128 })
  roundId: string;

  @Column({ name: 'game_id', type: 'varchar', length: 128 })
  gameId: string;

  @Column({ type: 'varchar', length: 32 })
  kind: WagerTransactionKind;

  @Column({ type: 'varchar', length: 3 })
  currency: string;

  @Column({ type: 'numeric', precision: 20, scale: 2 })
  amount: string;

  @Column({
    name: 'reference_external_transaction_id',
    type: 'varchar',
    length: 128,
    nullable: true,
  })
  referenceExternalTransactionId: string | null;

  @Column({
    name: 'reference_transaction_id',
    type: 'uuid',
    nullable: true,
  })
  referenceTransactionId: string | null;

  @Column({ type: 'varchar', length: 32 })
  status: WagerTransactionStatus;

  @Column({ name: 'failure_code', type: 'varchar', length: 64, nullable: true })
  failureCode: FailureCode | null;

  @Column({
    name: 'observed_balance_amount',
    type: 'numeric',
    precision: 20,
    scale: 2,
    nullable: true,
  })
  observedBalanceAmount: string | null;

  @Column({
    name: 'observed_balance_currency',
    type: 'varchar',
    length: 3,
    nullable: true,
  })
  observedBalanceCurrency: string | null;

  @Column({ name: 'retry_attempts', type: 'integer', default: 0 })
  retryAttempts: number;

  @Column({ name: 'next_retry_at', type: 'timestamptz', nullable: true })
  nextRetryAt: Date | null;

  @Column({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({ name: 'processed_at', type: 'timestamptz', nullable: true })
  processedAt: Date | null;

  static fromDomain(tx: WagerTransaction): WagerTransactionEntity {
    const entity = new WagerTransactionEntity();
    entity.id = tx.id;
    entity.providerId = tx.providerId;
    entity.externalTransactionId = tx.externalTransactionId;
    entity.idempotencyKey = tx.idempotencyKey;
    entity.payloadHash = tx.payloadHash;
    entity.walletId = tx.walletId;
    entity.playerId = tx.playerId;
    entity.roundId = tx.roundId;
    entity.gameId = tx.gameId;
    entity.kind = tx.kind;
    entity.currency = tx.money.currency;
    entity.amount = tx.money.toString();
    entity.referenceExternalTransactionId =
      tx.referenceExternalTransactionId ?? null;
    entity.referenceTransactionId = tx.referenceTransactionId ?? null;
    entity.status = tx.status;
    entity.failureCode = tx.failureCode ?? null;
    entity.observedBalanceAmount = tx.observedBalance
      ? tx.observedBalance.toString()
      : null;
    entity.observedBalanceCurrency = tx.observedBalance
      ? tx.observedBalance.currency
      : null;
    entity.retryAttempts = tx.retryAttempts;
    entity.nextRetryAt = tx.nextRetryAt ?? null;
    entity.createdAt = tx.createdAt;
    entity.processedAt = tx.processedAt ?? null;
    return entity;
  }

  toDomain(): WagerTransaction {
    const observedBalance =
      this.observedBalanceAmount !== null &&
      this.observedBalanceAmount !== undefined &&
      this.observedBalanceCurrency
        ? Money.rehydrate({
            amount: Number(this.observedBalanceAmount).toFixed(2),
            currency: this.observedBalanceCurrency,
          })
        : undefined;

    return WagerTransaction.rehydrate({
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      money: Money.rehydrate({
        amount: Number(this.amount).toFixed(2),
        currency: this.currency,
      }),
      referenceExternalTransactionId:
        this.referenceExternalTransactionId ?? undefined,
      createdAt: new Date(this.createdAt),
      status: this.status,
      referenceTransactionId: this.referenceTransactionId ?? undefined,
      failureCode: this.failureCode ?? undefined,
      processedAt: this.processedAt ? new Date(this.processedAt) : undefined,
      observedBalance,
      retryAttempts: Number(this.retryAttempts ?? 0),
      nextRetryAt: this.nextRetryAt ? new Date(this.nextRetryAt) : undefined,
    });
  }
}
