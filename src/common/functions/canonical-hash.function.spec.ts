import { computeCanonicalPayloadHash } from './canonical-hash.function';
import { encodeLedgerCursor, decodeLedgerCursor } from './cursor.function';

describe('Canonical Payload Hash & Ledger Cursor', () => {
  it('produces identical SHA-256 hash regardless of object key order', () => {
    const payloadA = {
      providerId: 'provider-a',
      externalTransactionId: 'tx-1',
      playerId: 'p-1',
      walletId: 'w-1',
      roundId: 'r-1',
      gameId: 'g-1',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const payloadB = {
      kind: 'BET',
      money: { currency: 'BRL', amount: '25.00' },
      gameId: 'g-1',
      roundId: 'r-1',
      walletId: 'w-1',
      playerId: 'p-1',
      externalTransactionId: 'tx-1',
      providerId: 'provider-a',
    };

    expect(computeCanonicalPayloadHash(payloadA)).toBe(
      computeCanonicalPayloadHash(payloadB),
    );
  });

  it('produces different hash when any business field changes', () => {
    const base = {
      providerId: 'provider-a',
      externalTransactionId: 'tx-1',
      playerId: 'p-1',
      walletId: 'w-1',
      roundId: 'r-1',
      gameId: 'g-1',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const altered = {
      ...base,
      money: { amount: '30.00', currency: 'BRL' },
    };

    expect(computeCanonicalPayloadHash(base)).not.toBe(
      computeCanonicalPayloadHash(altered),
    );
  });

  it('encodes and decodes opaque ledger cursor symmetrically', () => {
    const date = new Date('2026-10-22T12:00:00.000Z');
    const id = '0192f291-27dd-7d3f-8071-5f8685deef37';
    const cursor = encodeLedgerCursor({ createdAt: date, id });
    const decoded = decodeLedgerCursor(cursor);

    expect(decoded.createdAt.toISOString()).toBe(date.toISOString());
    expect(decoded.id).toBe(id);
  });
});
