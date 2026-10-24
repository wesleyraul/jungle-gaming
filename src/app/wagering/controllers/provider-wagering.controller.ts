import { Controller, Get, Param, UseGuards } from '@nestjs/common';
import { ApiResponse, ApiTags } from '@nestjs/swagger';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { WagerTransactionResponse } from '../responses/wager-transaction.response';
import { WageringService } from '../services/wagering.service';

@ApiTags('Provider Wagering')
@UseGuards(ProviderAuthGuard)
@Controller('providers/:providerId/wagering/transactions')
export class ProviderWageringController {
  constructor(private readonly wageringService: WageringService) {}

  @ApiResponse({ status: 200, type: WagerTransactionResponse })
  @Get(':externalTransactionId')
  async getByProviderAndExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionResponse> {
    return this.wageringService.getTransactionByProviderAndExternalId(
      providerId,
      externalTransactionId,
    );
  }
}
