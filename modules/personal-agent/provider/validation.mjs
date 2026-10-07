import { canonicalJson } from '../../capabilities/server/validation.mjs';
import { createHash } from 'node:crypto';

export const GLM_MODEL = 'zai-org/GLM-5.3-Flash';
export class ProviderError extends Error {
  constructor(code, accounting) { super(code); this.name = 'ProviderError'; this.code = code; if (accounting) this.accounting = accounting; }
}
export function check(condition, code = 'provider_invalid_input') { if (!condition) throw new ProviderError(code); }
export function fields(value, required, optional = []) {
  check(value !== null && typeof value === 'object' && !Array.isArray(value));
  check([Object.prototype, null].includes(Object.getPrototypeOf(value)));
  const descriptors = Object.getOwnPropertyDescriptors(value);
  check(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && [...required, ...optional].includes(key)
    && Object.hasOwn(descriptors[key], 'value')) && required.every(key => Object.hasOwn(descriptors, key)));
  return value;
}
export function string(value, maxBytes, { empty = false } = {}) {
  check(typeof value === 'string' && value.isWellFormed() && !value.includes('\0')
    && Buffer.byteLength(value) <= maxBytes && (empty || value.trim().length > 0)); return value;
}
export function identifier(value, maximum = 180) {
  check(typeof value === 'string' && new RegExp(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,${maximum - 1}}$`, 'u').test(value)); return value;
}
export function responseIdentifier(value) {
  const output = identifier(value); check(!/^(sk-|obk-|gm_|gc_|gk_)/iu.test(output), 'provider_invalid_chunk'); return output;
}
export function integer(value, min = 0, max = Number.MAX_SAFE_INTEGER) {
  check(Number.isSafeInteger(value) && value >= min && value <= max); return value;
}
export const HARD_LIMITS = Object.freeze({ requestBytes: 262144, streamBytes: 8388608, eventBytes: 262144,
  textBytes: 262144, reasoningBytes: 524288, toolArgumentBytes: 65536,
  toolCalls: 12, messages: 128, messageBytes: 65536, jsonDepth: 16, jsonNodes: 10000,
  events: 65536, maxOutputTokens: 32768, timeoutMs: 1200000, concurrency: 8 });
export function boundedPolicy(value) {
  fields(value ?? {}, [], [...Object.keys(HARD_LIMITS), 'returnReasoning']);
  const output = { ...HARD_LIMITS, maxOutputTokens: 8192, timeoutMs: 120000, concurrency: 4, returnReasoning: false };
  for (const key of Object.keys(value ?? {})) {
    if (key === 'returnReasoning') { check(typeof value[key] === 'boolean'); output[key] = value[key]; }
    else output[key] = integer(value[key], 1, HARD_LIMITS[key]);
  }
  return Object.freeze(output);
}
export function canonical(value, maxBytes, maxDepth = 16) {
  try { return canonicalJson(value, { maxBytes, maxDepth }); } catch { throw new ProviderError('provider_invalid_input'); }
}
export function hash(value) { return createHash('sha256').update(value).digest('hex'); }
export function freeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child); Object.freeze(value);
  }
  return value;
}

/** Bounded JSON parser with decoded duplicate-key detection. It executes no schema or reference. */
export function parseJson(input, { maxBytes, maxDepth, maxNodes }, code = 'provider_invalid_json') {
  check(typeof input === 'string' && Buffer.byteLength(input) <= maxBytes, code);
  let index = 0, nodes = 0;
  const fail = () => { throw new ProviderError(code); };
  const white = () => { while (/[\t\r\n ]/u.test(input[index] ?? '\0')) index++; };
  function quoted() {
    if (input[index] !== '"') fail();
    const start = index++;
    while (index < input.length) {
      const character = input[index++];
      if (character === '\\') { index++; continue; }
      if (character === '"') {
        let result; try { result = JSON.parse(input.slice(start, index)); } catch { fail(); }
        if (!result.isWellFormed() || result.includes('\0')) fail(); return result;
      }
      if (character.charCodeAt(0) < 32) fail();
    }
    fail();
  }
  function value(depth) {
    if (++nodes > maxNodes || depth > maxDepth) fail();
    white(); const character = input[index];
    if (character === '"') return quoted();
    if (character === '{') {
      index++; white(); const result = Object.create(null), seen = new Set();
      if (input[index] === '}') { index++; return result; }
      while (index < input.length) {
        white(); const key = quoted();
        if (++nodes > maxNodes || seen.has(key) || ['__proto__', 'prototype', 'constructor'].includes(key)) fail(); seen.add(key);
        white(); if (input[index++] !== ':') fail(); result[key] = value(depth + 1); white();
        const end = input[index++]; if (end === '}') return result; if (end !== ',') fail();
      }
      fail();
    }
    if (character === '[') {
      index++; white(); const result = [];
      if (input[index] === ']') { index++; return result; }
      while (index < input.length) {
        result.push(value(depth + 1)); white(); const end = input[index++];
        if (end === ']') return result; if (end !== ',') fail();
      }
      fail();
    }
    for (const [token, output] of [['true', true], ['false', false], ['null', null]]) {
      if (input.slice(index, index + token.length) === token) { index += token.length; return output; }
    }
    const number = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u.exec(input.slice(index));
    if (number) { index += number[0].length; const result = Number(number[0]); if (!Number.isFinite(result)) fail(); return result; }
    fail();
  }
  const result = value(0); white(); if (index !== input.length) fail(); return result;
}
