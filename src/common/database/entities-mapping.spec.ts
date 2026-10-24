import { WalletEntity } from '../../app/wallet/entities/wallet.entity';
import { WalletLedgerEntryEntity } from '../../app/wallet/entities/wallet-ledger-entry.entity';
import { WagerTransactionEntity } from '../../app/wagering/entities/wager-transaction.entity';
import { Wallet } from '../../app/wallet/domain/wallet.domain';
import { Money } from '../domain/money.domain';
import { WagerTransaction } from '../../app/wagering/domain/wager-transaction.domain';
import { WagerTransactionKind } from '../enums/wager-transaction-kind.enum';

describe('Persistence Entities <-> Domain Mapping', () => {
  it('converts Wallet and WalletLedgerEntry between domain and entity preserving exact 2-decimal strings', () => {
    const wallet = Wallet.open({
      id: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const entry = wallet.debit({
      entryId: '0192f291-27dd-7d3f-8071-5f8685deef38',
      transactionId: '0192f291-27dd-7d3f-8071-5f8685deef39',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    const walletEntity = WalletEntity.fromDomain(wallet);
    const rehydratedWallet = walletEntity.toDomain();
    expect(rehydratedWallet.balance.toJSON()).toEqual({
      amount: '75.00',
      currency: 'BRL',
    });
    expect(rehydratedWallet.version).toBe(2);

    const entryEntity = WalletLedgerEntryEntity.fromDomain(entry);
    const rehydratedEntry = entryEntity.toDomain();
    expect(rehydratedEntry.isBalanced()).toBe(true);
    expect(rehydratedEntry.balanceAfter.toString()).toBe('75.00');
  });

  it('converts WagerTransaction between domain and entity preserving observedBalance', () => {
    const tx = WagerTransaction.create({
      id: '0192f298-345e-7e38-af88-e43f851a819d',
      providerId: 'provider-a',
      externalTransactionId: 'ext-1',
      idempotencyKey: 'provider-a:ext-1',
      payloadHash: 'hash-1',
      walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    });
    tx.markProcessed(
      undefined,
      new Date('2026-10-22T12:00:00.000Z'),
      Money.from({ amount: '975.00', currency: 'BRL' }),
    );

    const entity = WagerTransactionEntity.fromDomain(tx);
    const rehydrated = entity.toDomain();

    expect(rehydrated.status).toBe(tx.status);
    expect(rehydrated.observedBalance?.toJSON()).toEqual({
      amount: '975.00',
      currency: 'BRL',
    });
  });
});
