import {
  CurrencyMismatchError,
  InsufficientFundsError,
} from '../../../common/domain/domain.error';
import { Money } from '../../../common/domain/money.domain';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { WalletLedgerEntry } from './wallet-ledger-entry.domain';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementProps {
  entryId: string;
  transactionId: string;
  amount: Money;
  at?: Date;
  insufficientFundsCode?: FailureCode;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
    createdAt?: Date;
  }): Wallet {
    const now = props.createdAt ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.amount);
    if (this._balance.isLessThan(props.amount)) {
      throw new InsufficientFundsError(
        'Insufficient balance for debit',
        props.insufficientFundsCode ?? FailureCode.InsufficientFunds,
      );
    }
    const before = this._balance;
    const after = before.subtract(props.amount);
    const now = props.at ?? new Date();
    const entry = WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Debit,
      money: props.amount,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: now,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = now;
    return entry;
  }

  credit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.amount);
    const before = this._balance;
    const after = before.add(props.amount);
    const now = props.at ?? new Date();
    const entry = WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Credit,
      money: props.amount,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: now,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = now;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
