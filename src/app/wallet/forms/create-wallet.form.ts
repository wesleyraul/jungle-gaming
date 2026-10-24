import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsNotEmpty, IsString, ValidateNested } from 'class-validator';
import { MoneyForm } from './money.form';

export class CreateWalletForm {
  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  @IsString()
  @IsNotEmpty()
  playerId: string;

  @ApiProperty({ type: MoneyForm })
  @ValidateNested()
  @Type(() => MoneyForm)
  @IsNotEmpty()
  initialBalance: MoneyForm;
}
