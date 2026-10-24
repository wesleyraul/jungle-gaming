import { Injectable } from '@nestjs/common';
import { DataSource, EntityManager } from 'typeorm';
import { Wallet } from '../domain/wallet.domain';
import { WalletEntity } from '../entities/wallet.entity';

@Injectable()
export class WalletRepository {
  constructor(private readonly dataSource: DataSource) {}

  async findById(
    id: string,
    manager?: EntityManager,
  ): Promise<Wallet | undefined> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletEntity,
    );
    const entity = await repo.findOne({ where: { id } });
    return entity ? entity.toDomain() : undefined;
  }

  async findByIdForUpdate(
    id: string,
    manager: EntityManager,
  ): Promise<Wallet | undefined> {
    const entity = await manager
      .getRepository(WalletEntity)
      .createQueryBuilder('wallet')
      .setLock('pessimistic_write')
      .where('wallet.id = :id', { id })
      .getOne();
    return entity ? entity.toDomain() : undefined;
  }

  async save(wallet: Wallet, manager?: EntityManager): Promise<void> {
    const repo = (manager ?? this.dataSource.manager).getRepository(
      WalletEntity,
    );
    await repo.save(WalletEntity.fromDomain(wallet));
  }
}
