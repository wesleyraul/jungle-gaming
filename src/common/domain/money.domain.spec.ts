import { Money } from './money.domain';
import { CurrencyMismatchError, InvalidMoneyError } from './domain.error';

describe('Money Domain Value Object', () => {
  it('creates Money from valid decimal string with 2 decimal places', () => {
    const m = Money.from({ amount: '25.00', currency: 'BRL' });
    expect(m.toString()).toBe('25.00');
    expect(m.currency).toBe('BRL');
    expect(m.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  it('creates zero Money', () => {
    const zero = Money.zero('BRL');
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
    expect(zero.isNegative()).toBe(false);
    expect(zero.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
  });

  it('performs immutable add, subtract, negate, and comparisons', () => {
    const a = Money.from({ amount: '100.00', currency: 'BRL' });
    const b = Money.from({ amount: '80.00', currency: 'BRL' });

    const sum = a.add(b);
    const diff = a.subtract(b);
    const neg = b.negate();

    expect(a.toString()).toBe('100.00');
    expect(sum.toString()).toBe('180.00');
    expect(diff.toString()).toBe('20.00');
    expect(neg.isNegative()).toBe(true);
    expect(b.isLessThan(a)).toBe(true);
    expect(a.equals(Money.from({ amount: '100.00', currency: 'BRL' }))).toBe(true);
  });

  it('throws CurrencyMismatchError when operating on different currencies', () => {
    const brl = Money.from({ amount: '10.00', currency: 'BRL' });
    const usd = Money.from({ amount: '10.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
  });

  it.each([
    ['', 'BRL'],
    ['NaN', 'BRL'],
    ['Infinity', 'BRL'],
    ['-Infinity', 'BRL'],
    ['1e2', 'BRL'],
    ['1E-2', 'BRL'],
    ['25.123', 'BRL'],
    ['25.1', 'BRL'],
    ['25', 'BRL'],
    ['-10.00', 'BRL'],
    ['abc', 'BRL'],
    ['10.00', ''],
    ['10.00', 'br'],
    ['10.00', 'BRLX'],
  ])('rejects invalid input amount=%s currency=%s', (amount, currency) => {
    expect(() => Money.from({ amount, currency })).toThrow(InvalidMoneyError);
  });
});
