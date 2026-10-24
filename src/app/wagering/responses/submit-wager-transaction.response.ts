import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { FailureCode } from '../../../common/enums/failure-code.enum';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { MoneyResponse } from '../../wallet/responses/money.response';

export class SubmitWagerTransactionResponse {
  @ApiProperty()
  transactionId: string;

  @ApiProperty({ enum: WagerTransactionStatus })
  status: WagerTransactionStatus;

  @ApiProperty({ type: MoneyResponse })
  balance: MoneyResponse;

  @ApiProperty()
  idempotentReplay: boolean;

  @ApiPropertyOptional({ enum: FailureCode })
  failureCode?: FailureCode;
}
