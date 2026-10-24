import { WagerTransaction } from './wager-transaction.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { Money } from '../../../common/domain/money.domain';
import {
  DomainError,
  InvalidTransactionStateError,
} from '../../../common/domain/domain.error';

describe('WagerTransaction Domain', () => {
  const baseProps = {
    id: 'tx-1',
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    idempotencyKey: 'provider-a:ext-1',
    payloadHash: 'hash-1',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: Money.from({ amount: '25.00', currency: 'BRL' }),
  };

  it('starts in PENDING and transitions to PROCESSED, blocking further transitions', () => {
    const tx = WagerTransaction.create(baseProps);
    expect(tx.status).toBe(WagerTransactionStatus.Pending);
    expect(tx.isTerminal()).toBe(false);
    expect(tx.affectsBalance()).toBe(true);
    expect(tx.requiresReference()).toBe(false);
    expect(tx.ledgerDirectionFor()).toBe(LedgerDirection.Debit);

    const now = new Date();
    tx.markProcessed(undefined, now, Money.from({ amount: '75.00', currency: 'BRL' }));
    expect(tx.status).toBe(WagerTransactionStatus.Processed);
    expect(tx.isTerminal()).toBe(true);
    expect(tx.observedBalance?.toString()).toBe('75.00');

    expect(() => tx.reject(FailureCode.InsufficientFunds)).toThrow(
      InvalidTransactionStateError,
    );
  });

  it('enforces referenceExternalTransactionId for REFUND and ROLLBACK', () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Refund,
      }),
    ).toThrow(DomainError);

    const refund = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'ext-bet-1',
    });
    expect(refund.requiresReference()).toBe(true);
  });

  it('computes inverse ledger direction for ROLLBACK based on referenced transaction', () => {
    const betTx = WagerTransaction.create(baseProps);
    const winTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-win',
      kind: WagerTransactionKind.Win,
    });
    const rollbackTx = WagerTransaction.create({
      ...baseProps,
      id: 'tx-rb',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-1',
    });

    expect(rollbackTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    expect(rollbackTx.ledgerDirectionFor(winTx)).toBe(LedgerDirection.Debit);
  });

  it('marks LOSS as not affecting balance', () => {
    const lossTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Loss,
      money: Money.zero('BRL'),
    });
    expect(lossTx.affectsBalance()).toBe(false);
  });
});
