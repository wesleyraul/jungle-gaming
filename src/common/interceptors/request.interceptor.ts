import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';
import { Request, Response } from 'express';

export interface CorrelatedRequest extends Request {
  correlationId?: string;
}

@Injectable()
export class RequestInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    if (context.getType() === 'http') {
      const req = context.switchToHttp().getRequest<CorrelatedRequest>();
      const res = context.switchToHttp().getResponse<Response>();
      const headerId = req.headers['x-correlation-id'];
      const correlationId =
        typeof headerId === 'string' && headerId.trim().length > 0
          ? headerId.trim()
          : uuidv4();
      req.correlationId = correlationId;
      res.setHeader('x-correlation-id', correlationId);
    }
    return next.handle();
  }
}
