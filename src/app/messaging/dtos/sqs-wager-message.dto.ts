import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';

export interface SqsWagerMessageDto {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: {
    amount: string;
    currency: string;
  };
  referenceExternalTransactionId?: string;
  metadata?: Record<string, unknown>;
  correlationId?: string;
}
