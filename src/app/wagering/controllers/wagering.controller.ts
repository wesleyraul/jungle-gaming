import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ApiHeader, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { WagerTransactionStatus } from '../../../common/enums/wager-transaction-status.enum';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { CorrelatedRequest } from '../../../common/interceptors/request.interceptor';
import { SubmitWagerTransactionForm } from '../forms/submit-wager-transaction.form';
import { SubmitWagerTransactionResponse } from '../responses/submit-wager-transaction.response';
import { WagerTransactionResponse } from '../responses/wager-transaction.response';
import { WageringService } from '../services/wagering.service';

@ApiTags('Wagering')
@UseGuards(ProviderAuthGuard)
@Controller(['wager-transactions', 'wagering/transactions'])
export class WageringController {
  constructor(private readonly wageringService: WageringService) {}

  @ApiHeader({
    name: 'Idempotency-Key',
    required: true,
    description: 'Mandatory idempotency key (e.g. provider-a:transaction-123)',
  })
  @ApiResponse({ status: 200, type: SubmitWagerTransactionResponse })
  @ApiResponse({ status: 202, type: SubmitWagerTransactionResponse })
  @ApiResponse({ status: 422, type: SubmitWagerTransactionResponse })
  @Post()
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() form: SubmitWagerTransactionForm,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ): Promise<SubmitWagerTransactionResponse> {
    if (!idempotencyKey || idempotencyKey.trim().length === 0) {
      throw new BadRequestException('Idempotency-Key header is required');
    }

    const result = await this.wageringService.submitTransaction({
      idempotencyKey: idempotencyKey.trim(),
      providerId: form.providerId,
      externalTransactionId: form.externalTransactionId,
      playerId: form.playerId,
      walletId: form.walletId,
      roundId: form.roundId,
      gameId: form.gameId,
      kind: form.kind,
      money: form.money,
      referenceExternalTransactionId: form.referenceExternalTransactionId,
      correlationId: req.correlationId,
    });

    if (result.status === WagerTransactionStatus.PendingReference) {
      res.status(HttpStatus.ACCEPTED);
    } else if (result.status === WagerTransactionStatus.Rejected) {
      res.status(HttpStatus.UNPROCESSABLE_ENTITY);
    } else {
      res.status(HttpStatus.OK);
    }

    return result;
  }

  @ApiResponse({ status: 200, type: WagerTransactionResponse })
  @Get(':transactionId')
  async getTransactionById(
    @Param('transactionId', new ParseUUIDPipe()) transactionId: string,
  ): Promise<WagerTransactionResponse> {
    return this.wageringService.getTransactionById(transactionId);
  }
}
