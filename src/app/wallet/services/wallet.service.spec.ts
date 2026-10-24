import { WalletService } from './wallet.service';
import { WalletRepository } from '../repositories/wallet.repository';
import { WalletLedgerRepository } from '../repositories/wallet-ledger.repository';
import { MetricsService } from '../../../common/metrics/metrics.service';
import { AppLogger } from '../../../common/logger/app.logger';
import { Wallet } from '../domain/wallet.domain';
import { WalletLedgerEntry } from '../domain/wallet-ledger-entry.domain';
import { Money } from '../../../common/domain/money.domain';
import { LedgerDirection } from '../../../common/enums/ledger-direction.enum';
import { DataSource } from 'typeorm';

describe('WalletService Unit Tests', () => {
  let walletService: WalletService;
  let walletRepository: jest.Mocked<WalletRepository>;
  let walletLedgerRepository: jest.Mocked<WalletLedgerRepository>;
  let metricsService: MetricsService;
  let logger: AppLogger;
  let dataSource: Partial<DataSource>;

  beforeEach(() => {
    walletRepository = {
      findById: jest.fn(),
      findByIdForUpdate: jest.fn(),
      save: jest.fn(),
    } as unknown as jest.Mocked<WalletRepository>;

    walletLedgerRepository = {
      findPaginatedByWalletId: jest.fn(),
      findAllByWalletIdOrdered: jest.fn(),
      save: jest.fn(),
    } as unknown as jest.Mocked<WalletLedgerRepository>;

    metricsService = new MetricsService();
    logger = new AppLogger();
    dataSource = {};

    walletService = new WalletService(
      dataSource as DataSource,
      walletRepository,
      walletLedgerRepository,
      metricsService,
      logger,
    );
  });

  it('reconciles consistent wallet balance against ledger entries', async () => {
    const wallet = Wallet.rehydrate({
      id: 'w-1',
      playerId: 'p-1',
      currency: 'BRL',
      balance: Money.from({ amount: '975.00', currency: 'BRL' }),
      version: 2,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const openingEntry = WalletLedgerEntry.create({
      id: 'le-1',
      walletId: 'w-1',
      transactionId: 'tx-open',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '1000.00', currency: 'BRL' }),
      balanceBefore: Money.zero('BRL'),
      balanceAfter: Money.from({ amount: '1000.00', currency: 'BRL' }),
    });
    const betEntry = WalletLedgerEntry.create({
      id: 'le-2',
      walletId: 'w-1',
      transactionId: 'tx-bet',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '1000.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '975.00', currency: 'BRL' }),
    });

    walletRepository.findById.mockResolvedValue(wallet);
    walletLedgerRepository.findAllByWalletIdOrdered.mockResolvedValue([
      openingEntry,
      betEntry,
    ]);

    const result = await walletService.reconcileWallet('w-1');

    expect(result.consistent).toBe(true);
    expect(result.storedBalance).toEqual({ amount: '975.00', currency: 'BRL' });
    expect(result.calculatedBalance).toEqual({
      amount: '975.00',
      currency: 'BRL',
    });
    expect(result.difference).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(result.checkedEntries).toBe(2);
  });

  it('detects and records metric when stored balance diverges from ledger', async () => {
    const wallet = Wallet.rehydrate({
      id: 'w-1',
      playerId: 'p-1',
      currency: 'BRL',
      balance: Money.from({ amount: '500.00', currency: 'BRL' }),
      version: 2,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    const openingEntry = WalletLedgerEntry.create({
      id: 'le-1',
      walletId: 'w-1',
      transactionId: 'tx-open',
      direction: LedgerDirection.Credit,
      money: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceBefore: Money.zero('BRL'),
      balanceAfter: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    walletRepository.findById.mockResolvedValue(wallet);
    walletLedgerRepository.findAllByWalletIdOrdered.mockResolvedValue([
      openingEntry,
    ]);

    const result = await walletService.reconcileWallet('w-1');

    expect(result.consistent).toBe(false);
    expect(result.difference).toEqual({ amount: '400.00', currency: 'BRL' });
    expect(metricsService.getSnapshot().reconciliationDivergences).toBe(1);
  });
});
