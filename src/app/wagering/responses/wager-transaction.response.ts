import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { MoneyResponse } from '../../wallet/responses/money.response';

export class WagerTransactionResponse {
  @ApiProperty()
  id: string;

  @ApiProperty()
  providerId: string;

  @ApiProperty()
  externalTransactionId: string;

  @ApiProperty()
  idempotencyKey: string;

  @ApiProperty()
  walletId: string;

  @ApiProperty()
  playerId: string;

  @ApiProperty()
  roundId: string;

  @ApiProperty()
  gameId: string;

  @ApiProperty({ enum: WagerTransactionKind })
  kind: WagerTransactionKind;

  @ApiProperty({ type: MoneyResponse })
  money: MoneyResponse;

  @ApiProperty({ enum: WagerTransactionStatus })
  status: WagerTransactionStatus;

  @ApiPropertyOptional()
  referenceExternalTransactionId?: string;

  @ApiPropertyOptional()
  referenceTransactionId?: string;

  @ApiPropertyOptional({ enum: FailureCode })
  failureCode?: FailureCode;

  @ApiPropertyOptional({ type: MoneyResponse })
  balance?: MoneyResponse;

  @ApiProperty()
  createdAt: string;

  @ApiPropertyOptional()
  processedAt?: string;
}
