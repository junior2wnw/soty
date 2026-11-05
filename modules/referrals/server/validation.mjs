import { createHash } from 'node:crypto';
import { snapshot } from '../../app-contract/json.mjs';

export class ReferralsError extends Error {
  constructor(code, status = 400) { super(code); this.name = 'ReferralsError'; this.code = code; this.status = status; }
}
export function check(value, code = 'referrals_invalid_arguments', status) { if (!value) throw new ReferralsError(code, status); }
export function keys(value, allowed, required = allowed) {
  check(value && typeof value === 'object' && !Array.isArray(value));
  check(Object.keys(value).every(key => allowed.includes(key)) && required.every(key => Object.hasOwn(value, key)));
}
export function id(value) {
  check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(value) && !value.includes('..') && !value.includes('//'));
  return value;
}
export function integer(value, min = 0, max = Number.MAX_SAFE_INTEGER) { check(Number.isSafeInteger(value) && !Object.is(value, -0) && value >= min && value <= max); return value; }
export function currency(value) { check(typeof value === 'string' && /^[A-Z]{3}$/u.test(value)); return value; }
export function rates(value) {
  check(Array.isArray(value) && value.length >= 1 && value.length <= 10);
  value.forEach(rate => integer(rate, 0, 10000)); check(value.reduce((sum, rate) => sum + rate, 0) <= 10000);
  return [...value];
}
export const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
export const proportionalFloor = (amount, numerator, denominator) => Number(BigInt(amount) * BigInt(numerator) / BigInt(denominator));
export function trustedEvent(value) {
  value = snapshot(value);
  check(value && typeof value === 'object', 'referrals_trusted_event_required', 403);
  const common = ['kind', 'appId', 'eventId'];
  const fields = { attribution: ['accountId', 'parentAccountId'], payment: ['payerAccountId', 'currency', 'eligibleAmountMinor'], refund: ['paymentId', 'refundAmountMinor'] };
  check(Object.hasOwn(fields, value.kind), 'referrals_trusted_event_invalid'); keys(value, [...common, ...fields[value.kind]]);
  const event = { kind: value.kind, appId: id(value.appId), eventId: id(value.eventId) };
  if (value.kind === 'attribution') return { ...event, accountId: id(value.accountId), parentAccountId: value.parentAccountId === null ? null : id(value.parentAccountId) };
  if (value.kind === 'payment') return { ...event, payerAccountId: id(value.payerAccountId), currency: currency(value.currency), eligibleAmountMinor: integer(value.eligibleAmountMinor) };
  return { ...event, paymentId: id(value.paymentId), refundAmountMinor: integer(value.refundAmountMinor, 1) };
}
