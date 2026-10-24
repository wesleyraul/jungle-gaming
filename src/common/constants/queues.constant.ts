export const QUEUES = {
  WAGER_TRANSACTIONS_FIFO: 'wager-transactions.fifo',
  WAGER_TRANSACTIONS_DLQ_FIFO: 'wager-transactions-dlq.fifo',
  INTEGRATION_EVENTS_FIFO: 'integration-events.fifo',
  WAGER_CONSUMER_NAME: 'wager-transaction-consumer',
} as const;
