import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { MoneyResponse } from './money.response';

export class WalletLedgerEntryItemResponse {
  @ApiProperty()
  id: string;

  @ApiProperty()
  walletId: string;

  @ApiProperty()
  transactionId: string;

  @ApiProperty({ enum: LedgerDirection })
  direction: LedgerDirection;

  @ApiProperty({ type: MoneyResponse })
  money: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  balanceBefore: MoneyResponse;

  @ApiProperty({ type: MoneyResponse })
  balanceAfter: MoneyResponse;

  @ApiProperty()
  createdAt: string;
}

export class WalletLedgerResponse {
  @ApiProperty({ type: [WalletLedgerEntryItemResponse] })
  items: WalletLedgerEntryItemResponse[];

  @ApiPropertyOptional({ nullable: true })
  nextCursor: string | null;
}
