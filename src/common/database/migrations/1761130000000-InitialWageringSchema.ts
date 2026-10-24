import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialWageringSchema1761130000000 implements MigrationInterface {
  name = 'InitialWageringSchema1761130000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY,
        player_id varchar(128) NOT NULL,
        currency varchar(3) NOT NULL,
        balance_amount numeric(20, 2) NOT NULL,
        version integer NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
        CONSTRAINT chk_wallets_non_negative_balance CHECK (balance_amount >= 0),
        CONSTRAINT chk_wallets_positive_version CHECK (version >= 1)
      );
    `);

    await queryRunner.query(`
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY,
        provider_id varchar(128) NOT NULL,
        external_transaction_id varchar(128) NOT NULL,
        idempotency_key varchar(255) NOT NULL,
        payload_hash varchar(64) NOT NULL,
        wallet_id uuid NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        player_id varchar(128) NOT NULL,
        round_id varchar(128) NOT NULL,
        game_id varchar(128) NOT NULL,
        kind varchar(32) NOT NULL,
        currency varchar(3) NOT NULL,
        amount numeric(20, 2) NOT NULL,
        reference_external_transaction_id varchar(128),
        reference_transaction_id uuid REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        status varchar(32) NOT NULL,
        failure_code varchar(64),
        observed_balance_amount numeric(20, 2),
        observed_balance_currency varchar(3),
        retry_attempts integer NOT NULL DEFAULT 0,
        next_retry_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz,
        CONSTRAINT uq_wager_tx_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT chk_wager_tx_non_negative_amount CHECK (amount >= 0),
        CONSTRAINT chk_wager_tx_positive_unless_loss CHECK (kind = 'LOSS' OR amount > 0),
        CONSTRAINT chk_wager_tx_reference_required CHECK (
          (kind NOT IN ('REFUND', 'ROLLBACK')) OR (reference_external_transaction_id IS NOT NULL)
        )
      );
    `);

    await queryRunner.query(`
      CREATE UNIQUE INDEX uq_wager_tx_processed_reversal
      ON wager_transactions (reference_transaction_id, kind)
      WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
    `);

    await queryRunner.query(`
      CREATE INDEX idx_wager_tx_pending_reference
      ON wager_transactions (status, next_retry_at)
      WHERE status = 'PENDING_REFERENCE';
    `);

    await queryRunner.query(`
      CREATE TABLE wallet_ledger_entries (
        id uuid PRIMARY KEY,
        wallet_id uuid NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        direction varchar(16) NOT NULL,
        currency varchar(3) NOT NULL,
        amount numeric(20, 2) NOT NULL,
        balance_before numeric(20, 2) NOT NULL,
        balance_after numeric(20, 2) NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_ledger_wallet_transaction UNIQUE (wallet_id, transaction_id),
        CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
        CONSTRAINT chk_ledger_positive_amount CHECK (amount > 0),
        CONSTRAINT chk_ledger_non_negative_before CHECK (balance_before >= 0),
        CONSTRAINT chk_ledger_non_negative_after CHECK (balance_after >= 0),
        CONSTRAINT chk_ledger_arithmetic_balance CHECK (
          (direction = 'DEBIT' AND balance_before - amount = balance_after)
          OR
          (direction = 'CREDIT' AND balance_before + amount = balance_after)
        )
      );
    `);

    await queryRunner.query(`
      CREATE INDEX idx_ledger_wallet_cursor
      ON wallet_ledger_entries (wallet_id, created_at ASC, id ASC);
    `);

    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
      RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is immutable: % operation is forbidden', TG_OP;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await queryRunner.query(`
      CREATE TRIGGER trg_immutable_wallet_ledger_entries
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();
    `);

    await queryRunner.query(`
      CREATE TABLE inbox_messages (
        consumer_name varchar(128) NOT NULL,
        message_id varchar(128) NOT NULL,
        payload_hash varchar(64) NOT NULL,
        received_at timestamptz NOT NULL DEFAULT now(),
        processed_at timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    await queryRunner.query(`
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY,
        aggregate_id varchar(128) NOT NULL,
        event_type varchar(128) NOT NULL,
        payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL DEFAULT now(),
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        published_at timestamptz
      );
    `);

    await queryRunner.query(`
      CREATE INDEX idx_outbox_pending_due
      ON outbox_messages (next_attempt_at ASC, occurred_at ASC)
      WHERE published_at IS NULL;
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP INDEX IF EXISTS idx_outbox_pending_due;');
    await queryRunner.query('DROP TABLE IF EXISTS outbox_messages;');
    await queryRunner.query('DROP TABLE IF EXISTS inbox_messages;');
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_immutable_wallet_ledger_entries ON wallet_ledger_entries;',
    );
    await queryRunner.query('DROP FUNCTION IF EXISTS prevent_ledger_mutation();');
    await queryRunner.query('DROP INDEX IF EXISTS idx_ledger_wallet_cursor;');
    await queryRunner.query('DROP TABLE IF EXISTS wallet_ledger_entries;');
    await queryRunner.query('DROP INDEX IF EXISTS idx_wager_tx_pending_reference;');
    await queryRunner.query('DROP INDEX IF EXISTS uq_wager_tx_processed_reversal;');
    await queryRunner.query('DROP TABLE IF EXISTS wager_transactions;');
    await queryRunner.query('DROP TABLE IF EXISTS wallets;');
  }
}
