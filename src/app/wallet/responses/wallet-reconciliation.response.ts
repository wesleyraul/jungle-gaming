import { ApiProperty } from '@nestjs/swagger';
import { MoneyResponse } from './money.response';

export class WalletReconciliationResponse {
  @ApiProperty()
  walletId: string;

  @ApiProperty({ type: MoneyResponse })
  storedBalance: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  calculatedBalance: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  difference: MoneyResponse;

  @ApiProperty()
  consistent: boolean;

  @ApiProperty()
  checkedEntries: number;
}
