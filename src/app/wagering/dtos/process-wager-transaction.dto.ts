import { MoneyProps } from '../../../common/domain/money.domain';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';

export interface ProcessWagerTransactionDto {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  inboxContext?: {
    consumerName: string;
    messageId: string;
    payloadHash: string;
  };
}
