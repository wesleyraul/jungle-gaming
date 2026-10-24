import { createHash } from 'crypto';
import { MoneyProps } from '../domain/money.domain';

export interface CanonicalWagerPayloadInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export function computeCanonicalPayloadHash(
  input: CanonicalWagerPayloadInput,
): string {
  const canonicalObject = {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    referenceExternalTransactionId:
      input.referenceExternalTransactionId ?? null,
    roundId: input.roundId,
    walletId: input.walletId,
  };
  return createHash('sha256')
    .update(JSON.stringify(canonicalObject))
    .digest('hex');
}
