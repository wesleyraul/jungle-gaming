import { MoneyProps } from '../../../common/domain/money.domain';

export interface CreateWalletDto {
  playerId: string;
  initialBalance: MoneyProps;
}
