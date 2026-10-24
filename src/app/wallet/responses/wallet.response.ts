import { ApiProperty } from '@nestjs/swagger';
import { MoneyResponse } from './money.response';

export class WalletResponse {
  @ApiProperty({ example: '0192f291-27dd-7d3f-8071-5f8685deef37' })
  id: string;

  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  playerId: string;

  @ApiProperty({ type: MoneyResponse })
  balance: MoneyResponse;

  @ApiProperty({ example: 1 })
  version: number;
}
