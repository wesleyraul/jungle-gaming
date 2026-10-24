import { Injectable, LoggerService } from '@nestjs/common';

export interface StructuredLogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  context?: string;
  [key: string]: string | number | boolean | undefined;
}

@Injectable()
export class AppLogger implements LoggerService {
  log(message: string, meta?: StructuredLogContext | string): void {
    this.emit('INFO', message, meta);
  }

  error(
    message: string,
    trace?: string,
    meta?: StructuredLogContext | string,
  ): void {
    const contextObj = typeof meta === 'string' ? { context: meta } : meta;
    this.emit('ERROR', message, { ...contextObj, trace });
  }

  warn(message: string, meta?: StructuredLogContext | string): void {
    this.emit('WARN', message, meta);
  }

  private emit(
    level: string,
    message: string,
    meta?: StructuredLogContext | string,
  ): void {
    if (process.env.NODE_ENV === 'test' && process.env.ENABLE_TEST_LOGS !== 'true') {
      return;
    }
    const base = typeof meta === 'string' ? { context: meta } : (meta ?? {});
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...base,
    };
    process.stdout.write(`${JSON.stringify(entry)}\n`);
  }
}
