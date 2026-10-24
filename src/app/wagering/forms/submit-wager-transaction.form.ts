import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsIn,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  ValidateNested,
} from 'class-validator';
import { WagerTransactionKind } from '../../../common/enums/wager-transaction-kind.enum';
import { MoneyForm } from '../../wallet/forms/money.form';

const EXTERNAL_ALLOWED_KINDS = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
] as const;

export class SubmitWagerTransactionForm {
  @ApiPropertyOptional({ example: 'provider-a:tx-123' })
  @IsOptional()
  @IsString()
  idempotencyKey?: string;

  @ApiProperty({ example: 'provider-a' })
  @IsString()
  @IsNotEmpty()
  providerId: string;

  @ApiProperty({ example: 'transaction-123' })
  @IsString()
  @IsNotEmpty()
  externalTransactionId: string;

  @ApiProperty({ example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' })
  @IsString()
  @IsNotEmpty()
  playerId: string;

  @ApiProperty({ example: '0192f291-27dd-7d3f-8071-5f8685deef37' })
  @IsUUID()
  @IsNotEmpty()
  walletId: string;

  @ApiProperty({ example: 'round-987' })
  @IsString()
  @IsNotEmpty()
  roundId: string;

  @ApiProperty({ example: 'fortune-chimp' })
  @IsString()
  @IsNotEmpty()
  gameId: string;

  @ApiProperty({ enum: EXTERNAL_ALLOWED_KINDS, example: WagerTransactionKind.Bet })
  @IsIn(EXTERNAL_ALLOWED_KINDS)
  kind: WagerTransactionKind;

  @ApiProperty({ type: MoneyForm })
  @ValidateNested()
  @Type(() => MoneyForm)
  @IsNotEmpty()
  money: MoneyForm;

  @ApiPropertyOptional({ example: 'transaction-100' })
  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}
