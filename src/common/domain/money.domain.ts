import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './domain.error';

export interface MoneyProps {
  amount: string;
  currency: string;
}

const DECIMAL_AMOUNT_REGEX = /^\d+\.\d{2}$/;
const SIGNED_DECIMAL_AMOUNT_REGEX = /^-?\d+\.\d{2}$/;
const CURRENCY_REGEX = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || typeof props.currency !== 'string') {
      throw new InvalidMoneyError('Money requires string amount and currency');
    }
    if (!CURRENCY_REGEX.test(props.currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency: ${props.currency}`);
    }
    if (!DECIMAL_AMOUNT_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(
        `Invalid money amount '${props.amount}': must be non-negative fixed 2-decimal string`,
      );
    }
    const decimalValue = new Decimal(props.amount);
    if (!decimalValue.isFinite() || decimalValue.isNegative()) {
      throw new InvalidMoneyError(`Invalid decimal value: ${props.amount}`);
    }
    return new Money(decimalValue, props.currency);
  }

  static rehydrate(props: MoneyProps): Money {
    if (!CURRENCY_REGEX.test(props.currency) || !SIGNED_DECIMAL_AMOUNT_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(`Cannot rehydrate invalid money: ${JSON.stringify(props)}`);
    }
    return new Money(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    if (!CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency: ${currency}`);
    }
    return new Money(new Decimal('0.00'), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  toString(): string {
    return this.value.toFixed(2);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
