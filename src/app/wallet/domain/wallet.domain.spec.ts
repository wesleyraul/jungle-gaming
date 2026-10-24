import { Wallet } from './wallet.domain';
import { Money } from '../../../common/domain/money.domain';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
} from '../../../common/domain/domain.error';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';

describe('Wallet Aggregate Root', () => {
  it('opens wallet at version 1 and increments version on credit/debit while producing balanced ledger entries', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    expect(wallet.version).toBe(1);
    expect(wallet.balance.toString()).toBe('100.00');

    const debitEntry = wallet.debit({
      entryId: 'le-1',
      transactionId: 'tx-1',
      amount: Money.from({ amount: '40.00', currency: 'BRL' }),
    });

    expect(wallet.balance.toString()).toBe('60.00');
    expect(wallet.version).toBe(2);
    expect(debitEntry.direction).toBe(LedgerDirection.Debit);
    expect(debitEntry.balanceBefore.toString()).toBe('100.00');
    expect(debitEntry.balanceAfter.toString()).toBe('60.00');

    const creditEntry = wallet.credit({
      entryId: 'le-2',
      transactionId: 'tx-2',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    expect(wallet.balance.toString()).toBe('85.00');
    expect(wallet.version).toBe(3);
    expect(creditEntry.direction).toBe(LedgerDirection.Credit);
    expect(creditEntry.balanceBefore.toString()).toBe('60.00');
    expect(creditEntry.balanceAfter.toString()).toBe('85.00');
  });

  it('rejects debit when balance is insufficient and leaves balance/version unchanged', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    expect(() =>
      wallet.debit({
        entryId: 'le-1',
        transactionId: 'tx-1',
        amount: Money.from({ amount: '80.00', currency: 'BRL' }),
      }),
    ).toThrow(InsufficientFundsError);

    expect(wallet.balance.toString()).toBe('50.00');
    expect(wallet.version).toBe(1);
  });

  it('rejects operations with mismatched currency', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    expect(() =>
      wallet.credit({
        entryId: 'le-1',
        transactionId: 'tx-1',
        amount: Money.from({ amount: '10.00', currency: 'USD' }),
      }),
    ).toThrow(CurrencyMismatchError);
  });
});
