import { DomainError } from '../domain/domain.error';

export interface LedgerCursorPayload {
  createdAt: Date;
  id: string;
}

export function encodeLedgerCursor(payload: LedgerCursorPayload): string {
  const raw = JSON.stringify({
    c: payload.createdAt.toISOString(),
    i: payload.id,
  });
  return Buffer.from(raw, 'utf8').toString('base64url');
}

export function decodeLedgerCursor(cursor: string): LedgerCursorPayload {
  try {
    const json = Buffer.from(cursor, 'base64url').toString('utf8');
    const parsed = JSON.parse(json) as { c?: string; i?: string };
    if (!parsed.c || !parsed.i) {
      throw new Error('Missing cursor fields');
    }
    const createdAt = new Date(parsed.c);
    if (Number.isNaN(createdAt.getTime())) {
      throw new Error('Invalid cursor date');
    }
    return { createdAt, id: parsed.i };
  } catch {
    throw new DomainError('Invalid pagination cursor', 'INVALID_CURSOR');
  }
}
