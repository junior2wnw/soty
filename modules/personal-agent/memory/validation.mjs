import { createHash } from 'node:crypto';

export class MemoryError extends Error {
  constructor(code) { super(code); this.name = 'MemoryError'; this.code = code; }
}
export function check(condition, code = 'memory_invalid_arguments') {
  if (!condition) throw new MemoryError(code);
}
export function fields(value, required, optional = []) {
  check(value !== null && typeof value === 'object' && !Array.isArray(value));
  const prototype = Object.getPrototypeOf(value);
  check(prototype === Object.prototype || prototype === null);
  const descriptors = Object.getOwnPropertyDescriptors(value), keys = Reflect.ownKeys(descriptors);
  check(keys.every(key => typeof key === 'string' && [...required, ...optional].includes(key)
    && Object.hasOwn(descriptors[key], 'value')) && required.every(key => Object.hasOwn(descriptors, key)));
  return value;
}
export function text(value, maximum, { empty = false } = {}) {
  check(typeof value === 'string' && value.length <= maximum && (empty || value.trim().length > 0)
    && value.isWellFormed() && !value.includes('\0'));
  return value;
}
export function id(value) {
  check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,179}$/u.test(value)); return value;
}
export function integer(value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  check(Number.isSafeInteger(value) && value >= minimum && value <= maximum); return value;
}
export function canonicalScope(value) {
  fields(value, ['issuer', 'accountId', 'audienceKind', 'audienceId', 'projectId']);
  const scope = { issuer: text(value.issuer, 512), accountId: id(value.accountId),
    audienceKind: value.audienceKind, audienceId: id(value.audienceId),
    projectId: value.projectId === null ? null : id(value.projectId) };
  check(['personal', 'project', 'community'].includes(scope.audienceKind));
  check(scope.audienceKind !== 'personal' || scope.audienceId === scope.accountId);
  check(scope.audienceKind !== 'project' || scope.projectId === scope.audienceId);
  return Object.freeze(scope);
}
export const DEFAULT_LIMITS = Object.freeze({ records: 1000, contentBytes: 16 * 1024 * 1024,
  recordBytes: 64 * 1024, receipts: 10000, tombstones: 10000, recallLimit: 10,
  candidates: 64, embeddingConcurrency: 4, embeddingTimeoutMs: 10000,
  retentionMs: 365 * 24 * 60 * 60 * 1000 });
export function limits(value = {}) {
  fields(value, [], Object.keys(DEFAULT_LIMITS));
  const result = { ...DEFAULT_LIMITS };
  for (const key of Object.keys(value)) result[key] = integer(value[key], 1, DEFAULT_LIMITS[key]);
  return Object.freeze(result);
}
export function record(value, bounds, now) {
  fields(value, ['id', 'type', 'text', 'source', 'freshUntil', 'retainUntil'], ['importance', 'confidence']);
  const result = { id: id(value.id), type: value.type, text: text(value.text, bounds.recordBytes),
    source: text(value.source, 4000), freshUntil: integer(value.freshUntil), retainUntil: integer(value.retainUntil),
    importance: value.importance ?? 0.7, confidence: value.confidence ?? 0.8 };
  check(['fact', 'decision', 'preference', 'error'].includes(result.type));
  check([result.importance, result.confidence].every(x => typeof x === 'number' && Number.isFinite(x) && x >= 0 && x <= 1));
  // Historical exact retries are allowed after retention expiry; first effects check active retention separately.
  check(result.retainUntil > 0 && result.retainUntil <= now + bounds.retentionMs, 'memory_retention_invalid');
  check(Buffer.byteLength(JSON.stringify(result)) <= bounds.recordBytes, 'memory_record_too_large');
  return Object.freeze(result);
}
export function recordRef(value) {
  fields(value, ['id', 'expectedRevision']);
  return Object.freeze({ id: id(value.id), expectedRevision: integer(value.expectedRevision, 1, Number.MAX_SAFE_INTEGER - 1) });
}
export function digest(value) { return createHash('sha256').update(JSON.stringify(value)).digest('hex'); }
export function queryTerms(value) {
  return [...new Set(value.normalize('NFKC').match(/[\p{L}\p{N}_-]{2,}/gu) ?? [])].slice(0, 16)
    .map(term => `"${term}"`).join(' OR ');
}
