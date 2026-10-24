import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ApiCreatedResponse, ApiResponse, ApiTags } from '@nestjs/swagger';
import { ProviderAuthGuard } from '../../../common/guards/provider-auth.guard';
import { CreateWalletForm } from '../forms/create-wallet.form';
import { GetWalletLedgerForm } from '../forms/get-wallet-ledger.form';
import { WalletLedgerResponse } from '../responses/wallet-ledger.response';
import { WalletReconciliationResponse } from '../responses/wallet-reconciliation.response';
import { WalletResponse } from '../responses/wallet.response';
import { WalletService } from '../services/wallet.service';

@ApiTags('Wallets')
@UseGuards(ProviderAuthGuard)
@Controller('wallets')
export class WalletController {
  constructor(private readonly walletService: WalletService) {}

  @ApiCreatedResponse({ type: WalletResponse })
  @Post()
  async createWallet(
    @Body() createWalletForm: CreateWalletForm,
  ): Promise<WalletResponse> {
    return this.walletService.createWallet(createWalletForm);
  }

  @ApiResponse({ status: 200, type: WalletResponse })
  @Get(':walletId')
  async getWallet(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
  ): Promise<WalletResponse> {
    return this.walletService.getWalletById(walletId);
  }

  @ApiResponse({ status: 200, type: WalletLedgerResponse })
  @Get(':walletId/ledger')
  async getWalletLedger(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
    @Query() query: GetWalletLedgerForm,
  ): Promise<WalletLedgerResponse> {
    return this.walletService.getWalletLedger(walletId, query);
  }

  @ApiResponse({ status: 200, type: WalletReconciliationResponse })
  @HttpCode(HttpStatus.OK)
  @Get(':walletId/reconciliation')
  async getReconcileWallet(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
  ): Promise<WalletReconciliationResponse> {
    return this.walletService.reconcileWallet(walletId);
  }

  @ApiResponse({ status: 200, type: WalletReconciliationResponse })
  @HttpCode(HttpStatus.OK)
  @Post(':walletId/reconciliation')
  async reconcileWallet(
    @Param('walletId', new ParseUUIDPipe()) walletId: string,
  ): Promise<WalletReconciliationResponse> {
    return this.walletService.reconcileWallet(walletId);
  }
}
