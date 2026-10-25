import { createHash, randomBytes } from 'node:crypto';

export class SourceAppError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export const check = (condition, code = 'source_app_input_invalid', status = 400) => {
  if (!condition) throw new SourceAppError(code, status);
};
export function fields(input, required, optional = []) {
  check(input && typeof input === 'object' && !Array.isArray(input) && [Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const descriptors = Object.getOwnPropertyDescriptors(input);
  check(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && [...required, ...optional].includes(key)
    && descriptors[key].enumerable && Object.hasOwn(descriptors[key], 'value')) && required.every(key => Object.hasOwn(descriptors, key)));
  return Object.fromEntries(Object.entries(descriptors).map(([key, field]) => [key, field.value]));
}
export function jsonCopy(input, { bytes = 65536, nodes = 4096, depth = 18 } = {}) {
  let count = 0;
  function visit(value, level = 0) {
    check(++count <= nodes && level <= depth, 'source_app_payload_limit', 413);
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'string') { check(value.isWellFormed() && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)); return value; }
    if (typeof value === 'number') { check(Number.isFinite(value)); return value; }
    if (Array.isArray(value)) {
      const descriptors = Object.getOwnPropertyDescriptors(value);
      check(value.length <= 128 && Reflect.ownKeys(descriptors).length === value.length + 1);
      return Array.from({ length: value.length }, (_, index) => { check(descriptors[index] && 'value' in descriptors[index]); return visit(descriptors[index].value, level + 1); });
    }
    const object = fields(value, [], Object.keys(value ?? {}));
    return Object.fromEntries(Object.entries(object).map(([key, child]) => [key, visit(child, level + 1)]));
  }
  const value = visit(input); check(Buffer.byteLength(JSON.stringify(value)) <= bytes, 'source_app_payload_limit', 413); return value;
}
export function canonical(input) {
  if (Array.isArray(input)) return '[' + input.map(canonical).join(',') + ']';
  if (input && typeof input === 'object') return '{' + Object.keys(input).sort().map(key => JSON.stringify(key) + ':' + canonical(input[key])).join(',') + '}';
  return JSON.stringify(input);
}
export const digest = value => createHash('sha256').update(typeof value === 'string' ? value : canonical(value)).digest('hex');
export const nonce = () => randomBytes(32).toString('base64url');
export const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
export const requestId = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{16,128}$/u.test(value);
export const syncResult = value => { check(value === null || !['object', 'function'].includes(typeof value) || typeof value.then !== 'function', 'source_app_authority_invalid', 503); return value; };
export const deepFreeze = value => { if (value && typeof value === 'object') { Object.values(value).forEach(deepFreeze); Object.freeze(value); } return value; };
