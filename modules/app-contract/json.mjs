import { createHash } from 'node:crypto';
import { canonicalJson } from '../capabilities/server/validation.mjs';

export const LIMITS = Object.freeze({
  bytes: 65536, depth: 18, nodes: 4096, capabilities: 32, refs: 32,
  registryEntries: 128, registryHistory: 4096, admissionRequests: 128
});
export class ContractError extends Error {
  constructor(code) { super(code); this.name = 'ContractError'; this.code = code; }
}
export const check = (ok, code = 'invalid_descriptor') => { if (!ok) throw new ContractError(code); };
const safeKey = key => /^[A-Za-z_$][A-Za-z0-9_.$-]{0,95}$/.test(key) && !['__proto__', 'prototype', 'constructor'].includes(key);
const scalar = value => typeof value === 'string' && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value);
const secret = value => /(?:\bBearer\s+\S+|-----BEGIN[\w ]*PRIVATE KEY-----|\bsk-[A-Za-z0-9_-]{16,}|\b(?:password|api[_-]?key|auth[_-]?token)\s*[:=]\s*\S+)/i.test(value);

/** Snapshot data without executing accessors or reading arbitrary prototypes. */
export function snapshot(value) {
  let nodes = 0, bytes = 0;
  const budget = count => { bytes += count; check(bytes <= LIMITS.bytes, 'input_limit'); };
  function visit(item, depth) {
    check(++nodes <= LIMITS.nodes && depth <= LIMITS.depth, 'input_limit');
    if (item === null || typeof item === 'boolean') { budget(5); return item; }
    if (typeof item === 'number') { check(Number.isSafeInteger(item) && !Object.is(item, -0), 'unsafe_number'); budget(17); return item; }
    if (typeof item === 'string') {
      budget(Buffer.byteLength(item) + 2);
      check(scalar(item) && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(item), 'invalid_string');
      check(!secret(item), 'secret_not_allowed'); return item;
    }
    check(item && typeof item === 'object', 'invalid_json');
    check(Object.getOwnPropertySymbols(item).length === 0, 'invalid_json');
    const entries = Object.getOwnPropertyDescriptors(item);
    if (Array.isArray(item)) {
      check(item.length <= 128 && Object.keys(entries).length === item.length + 1, 'input_limit');
      const out = [];
      for (let i = 0; i < item.length; i++) {
        check(Object.hasOwn(entries, i) && 'value' in entries[i], 'invalid_json');
        out.push(visit(entries[i].value, depth + 1));
      }
      return out;
    }
    check([Object.prototype, null].includes(Object.getPrototypeOf(item)), 'invalid_json');
    const out = {};
    for (const key of Object.keys(entries).sort()) {
      check(safeKey(key) && entries[key].enumerable && 'value' in entries[key], 'invalid_json');
      budget(key.length + 3);
      out[key] = visit(entries[key].value, depth + 1);
    }
    return out;
  }
  const copy = visit(value, 0);
  check(Buffer.byteLength(canonicalJson(copy)) <= LIMITS.bytes, 'input_limit');
  return copy;
}

/** Bounded UTF-8 JSON with duplicate decoded keys rejected before JSON.parse can erase them. */
export function parseContractJson(input) {
  check(typeof input === 'string' || input instanceof Uint8Array, 'invalid_json');
  let text;
  if (typeof input === 'string') { check(scalar(input), 'invalid_string'); text = input; }
  else {
    check(input.byteLength <= LIMITS.bytes, 'input_limit');
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(input); } catch { throw new ContractError('invalid_utf8'); }
  }
  check(Buffer.byteLength(text) <= LIMITS.bytes, 'input_limit');
  let pos = 0, nodes = 0;
  const ws = () => { while (pos < text.length && /[ \r\n\t]/.test(text[pos])) pos++; };
  function string() {
    const start = pos++;
    while (pos < text.length) {
      const char = text[pos++];
      if (char === '\\') { pos++; continue; }
      if (char === '"') {
        try { return JSON.parse(text.slice(start, pos)); } catch { throw new ContractError('invalid_json'); }
      }
    }
    throw new ContractError('invalid_json');
  }
  function value(depth) {
    check(++nodes <= LIMITS.nodes && depth <= LIMITS.depth, 'input_limit'); ws();
    if (text[pos] === '"') return string();
    if (text[pos] === '{') {
      pos++; ws(); const result = {}, keys = new Set();
      if (text[pos] === '}') { pos++; return result; }
      while (true) {
        check(text[pos] === '"', 'invalid_json'); const key = string();
        check(!keys.has(key), 'duplicate_key'); check(safeKey(key), 'invalid_json'); keys.add(key);
        ws(); check(text[pos++] === ':', 'invalid_json'); result[key] = value(depth + 1); ws();
        const next = text[pos++]; if (next === '}') return result;
        check(next === ',', 'invalid_json'); ws();
      }
    }
    if (text[pos] === '[') {
      pos++; ws(); const out = [];
      if (text[pos] === ']') { pos++; return out; }
      while (true) {
        check(out.length < 128, 'input_limit'); out.push(value(depth + 1)); ws();
        const next = text[pos++]; if (next === ']') return out; check(next === ',', 'invalid_json');
      }
    }
    for (const [literal, result] of [['true', true], ['false', false], ['null', null]]) {
      if (text.startsWith(literal, pos)) { pos += literal.length; return result; }
    }
    const match = /^-?(?:0|[1-9][0-9]*)/.exec(text.slice(pos));
    check(match, 'invalid_json'); pos += match[0].length;
    const number = Number(match[0]); check(Number.isSafeInteger(number) && !Object.is(number, -0), 'unsafe_number');
    return number;
  }
  const result = value(0); ws(); check(pos === text.length, 'invalid_json'); return snapshot(result);
}
export function canonicalContractJson(value) { return canonicalJson(snapshot(value)); }
export function contractDigest(value) { return createHash('sha256').update(canonicalContractJson(value), 'utf8').digest('hex'); }
