import { ApiProperty } from '@nestjs/swagger';

export class MoneyResponse {
  @ApiProperty({ example: '1000.00' })
  amount: string;

  @ApiProperty({ example: 'BRL' })
  currency: string;
}
