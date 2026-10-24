import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, Matches } from 'class-validator';

export class MoneyForm {
  @ApiProperty({ example: '1000.00' })
  @IsString()
  @IsNotEmpty()
  @Matches(/^\d+\.\d{2}$/, {
    message: 'amount must be a non-negative decimal string with scale 2',
  })
  amount: string;

  @ApiProperty({ example: 'BRL' })
  @IsString()
  @IsNotEmpty()
  @Matches(/^[A-Z]{3}$/, {
    message: 'currency must be a 3-letter ISO-4217 code',
  })
  currency: string;
}
