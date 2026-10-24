export interface HealthStatusResponse {
  status: 'UP' | 'DOWN';
  timestamp: string;
  checks?: {
    database: 'UP' | 'DOWN';
    sqs?: 'UP' | 'DOWN';
  };
}
