import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';

export interface ProviderIdentity {
  providerId?: string;
  authenticated: boolean;
}

export interface ProviderIdentityPort {
  verifyRequest(headers: Record<string, string | string[] | undefined>): Promise<ProviderIdentity>;
}

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  async canActivate(_context: ExecutionContext): Promise<boolean> {
    return true;
  }
}
