import { WalletLedgerEntry } from './wallet-ledger-entry.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { Money } from '../../../common/domain/money.domain';
import { DomainError } from '../../../common/domain/domain.error';

describe('WalletLedgerEntry Domain', () => {
  it('creates balanced CREDIT and DEBIT entries', () => {
    const credit = WalletLedgerEntry.create({
      id: 'entry-1',
      walletId: 'wallet-1',
      transactionId: 'tx-1',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '50.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '150.00', currency: 'BRL' }),
      createdAt: new Date(),
    });
    expect(credit.isBalanced()).toBe(true);

    const debit = WalletLedgerEntry.create({
      id: 'entry-2',
      walletId: 'wallet-1',
      transactionId: 'tx-2',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '30.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '150.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '120.00', currency: 'BRL' }),
      createdAt: new Date(),
    });
    expect(debit.isBalanced()).toBe(true);
  });

  it('rejects unbalanced ledger entry or zero money', () => {
    expect(() =>
      WalletLedgerEntry.create({
        id: 'entry-bad',
        walletId: 'wallet-1',
        transactionId: 'tx-1',
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '30.00', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '80.00', currency: 'BRL' }),
      }),
    ).toThrow(DomainError);
  });
});
