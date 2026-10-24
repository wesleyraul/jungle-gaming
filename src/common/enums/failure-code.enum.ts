export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalInsufficientFunds = 'REVERSAL_INSUFFICIENT_FUNDS',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  DuplicateReversal = 'DUPLICATE_REVERSAL',
  AmountMismatch = 'AMOUNT_MISMATCH',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  WalletPlayerMismatch = 'WALLET_PLAYER_MISMATCH',
  InfrastructureFailure = 'INFRASTRUCTURE_FAILURE',
}
