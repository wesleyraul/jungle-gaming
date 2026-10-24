import { FailureCode } from '../enums/failure-code.enum';

export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string,
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(message, 'INVALID_MONEY');
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, actual: string) {
    super(
      `Currency mismatch: expected ${expected}, got ${actual}`,
      FailureCode.CurrencyMismatch,
    );
  }
}

export class InsufficientFundsError extends DomainError {
  constructor(
    message = 'Insufficient wallet balance',
    code: FailureCode = FailureCode.InsufficientFunds,
  ) {
    super(message, code);
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(currentStatus: string, targetAction: string) {
    super(
      `Cannot transition transaction from ${currentStatus} via ${targetAction}`,
      'INVALID_TRANSACTION_STATE',
    );
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(idempotencyKey: string) {
    super(
      `Idempotency key '${idempotencyKey}' already used with a different payload`,
      'IDEMPOTENCY_CONFLICT',
    );
  }
}

export class DuplicateWalletError extends DomainError {
  constructor(playerId: string, currency: string) {
    super(
      `Wallet already exists for player ${playerId} and currency ${currency}`,
      'DUPLICATE_WALLET',
    );
  }
}

export class EntityNotFoundError extends DomainError {
  constructor(entity: string, identifier: string) {
    super(`${entity} '${identifier}' not found`, 'ENTITY_NOT_FOUND');
  }
}
