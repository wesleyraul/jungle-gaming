import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { Response } from 'express';
import {
  CurrencyMismatchError,
  DomainError,
  DuplicateWalletError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidMoneyError,
} from '../domain/domain.error';

@Catch()
export class DomainExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    if (exception instanceof HttpException) {
      response
        .status(exception.getStatus())
        .json(exception.getResponse());
      return;
    }

    if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof DuplicateWalletError
    ) {
      response.status(HttpStatus.CONFLICT).json({
        statusCode: HttpStatus.CONFLICT,
        error: 'Conflict',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (exception instanceof EntityNotFoundError) {
      response.status(HttpStatus.NOT_FOUND).json({
        statusCode: HttpStatus.NOT_FOUND,
        error: 'Not Found',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (exception instanceof InvalidMoneyError) {
      response.status(HttpStatus.BAD_REQUEST).json({
        statusCode: HttpStatus.BAD_REQUEST,
        error: 'Bad Request',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    if (
      exception instanceof CurrencyMismatchError ||
      exception instanceof InsufficientFundsError ||
      exception instanceof DomainError
    ) {
      const status =
        exception.code === 'INVALID_CURSOR' ||
        exception.code === 'FORBIDDEN_TRANSACTION_KIND' ||
        exception.code === 'MISSING_REFERENCE_EXTERNAL_ID' ||
        exception.code === 'INVALID_TRANSACTION_AMOUNT'
          ? HttpStatus.BAD_REQUEST
          : HttpStatus.UNPROCESSABLE_ENTITY;

      response.status(status).json({
        statusCode: status,
        error:
          status === HttpStatus.BAD_REQUEST
            ? 'Bad Request'
            : 'Unprocessable Entity',
        code: exception.code,
        message: exception.message,
      });
      return;
    }

    response.status(HttpStatus.SERVICE_UNAVAILABLE).json({
      statusCode: HttpStatus.SERVICE_UNAVAILABLE,
      error: 'Service Unavailable',
      code: 'TRANSIENT_INFRASTRUCTURE_ERROR',
      message:
        exception instanceof Error
          ? exception.message
          : 'Unexpected infrastructure error',
    });
  }
}
