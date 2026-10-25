import { createHash, randomUUID } from 'node:crypto';

export class AccessError extends Error {
  constructor(code = 'invalid_input') {
    super(code);
    this.name = 'AccessError';
    this.code = code;
  }
}

export function assert(condition, code = 'invalid_input') {
  if (!condition) throw new AccessError(code);
}

export function record(value, code = 'invalid_input') {
  assert(value && typeof value === 'object' && !Array.isArray(value), code);
  const prototype = Object.getPrototypeOf(value);
  assert(prototype === Object.prototype || prototype === null, code);
  return value;
}

export function exact(value, keys, code = 'invalid_input') {
  record(value, code);
  assert(Object.keys(value).every(key => keys.includes(key)), code);
  return value;
}

export function text(value, { min = 1, max = 160, code = 'invalid_input' } = {}) {
  assert(typeof value === 'string' && value.length >= min && value.length <= max, code);
  assert(!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value), code);
  return value;
}

export function identifier(value, code = 'invalid_input') {
  text(value, { max: 160, code });
  assert(/^[A-Za-z0-9][A-Za-z0-9_.:@/-]*$/u.test(value) && !value.includes('..'), code);
  return value;
}

export function integer(value, min, max, code = 'invalid_input') {
  assert(Number.isSafeInteger(value) && value >= min && value <= max, code);
  return value;
}

export function stringSet(value, { max = 32, empty = true, code = 'invalid_input' } = {}) {
  assert(Array.isArray(value) && value.length <= max && (empty || value.length > 0), code);
  const normalized = value.map(item => identifier(item, code));
  assert(new Set(normalized).size === normalized.length, code);
  return normalized.sort();
}

export function canonicalJson(value, { maxBytes = 262144, maxDepth = 20 } = {}) {
  let nodes = 0;
  function visit(item, depth) {
    assert(++nodes <= 10000 && depth <= maxDepth, 'payload_too_large');
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number') {
      assert(Number.isFinite(item), 'invalid_input');
      return JSON.stringify(item);
    }
    if (Array.isArray(item)) {
      assert(item.every((_, i) => Object.hasOwn(item, i)) && Object.keys(item).length === item.length);
      return `[${item.map(child => visit(child, depth + 1)).join(',')}]`;
    }
    record(item);
    const keys = Object.keys(item).sort();
    assert(keys.every(key => !['__proto__', 'prototype', 'constructor'].includes(key)));
    return `{${keys.map(key => `${JSON.stringify(key)}:${visit(item[key], depth + 1)}`).join(',')}}`;
  }
  const result = visit(value, 0);
  assert(Buffer.byteLength(result, 'utf8') <= maxBytes, 'payload_too_large');
  return result;
}

export function canonicalHash(value) {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

export function freezeDeep(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freezeDeep(child);
    Object.freeze(value);
  }
  return value;
}

export function newId(prefix) {
  return `${prefix}_${randomUUID()}`;
}

export function now(clock) {
  return integer(clock(), 0, Number.MAX_SAFE_INTEGER, 'clock_invalid');
}
