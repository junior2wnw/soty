import { randomBytes } from 'node:crypto';

export class WorldError extends Error {
  constructor(code) { super(code); this.name = 'WorldError'; this.code = code; }
}
export function assert(value, code) { if (!value) throw new WorldError(code); }
export const id = (prefix) => `${prefix}_${randomBytes(18).toString('base64url')}`;
export function identifier(value) {
  assert(typeof value === 'string' && /^[A-Za-z0-9_-]{3,160}$/u.test(value), 'invalid_identifier');
  return value;
}
export function exact(args, required = [], optional = []) {
  assert(args && typeof args === 'object' && !Array.isArray(args), 'invalid_arguments');
  const allowed = new Set([...required, ...optional]);
  assert(required.every(key => Object.hasOwn(args, key)) && Object.keys(args).every(key => allowed.has(key)), 'invalid_arguments');
}
export function text(value, max, { empty = true, multiline = false } = {}) {
  assert(typeof value === 'string', 'invalid_text');
  const result = value.normalize('NFC').trim();
  assert((empty || result.length > 0) && result.length <= max
    && !(multiline ? /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u : /[\u0000-\u001f\u007f]/u).test(result), 'invalid_text');
  return result;
}
export function tags(value) {
  assert(Array.isArray(value) && value.length <= 8, 'invalid_topics');
  return [...new Set(value.map(item => text(item, 32, { empty: false }).toLocaleLowerCase('ru')))];
}
export function choice(value, options) { assert(options.includes(value), 'invalid_choice'); return value; }
export function boolean(value) { assert(typeof value === 'boolean', 'invalid_boolean'); return value ? 1 : 0; }
export function integer(value, min = 0, max = Number.MAX_SAFE_INTEGER) {
  assert(Number.isSafeInteger(value) && value >= min && value <= max, 'invalid_number'); return value;
}
export const limit = value => value === undefined ? 30 : integer(value, 1, 60);
export function revision(args, row) {
  integer(args.expectedRevision, 1);
  assert(args.expectedRevision === row.revision, 'revision_conflict');
}
export function searchText(value) { return text(value ?? '', 100).toLocaleLowerCase('ru'); }
export const folded = value => value.normalize('NFC').toLocaleLowerCase('ru');
export function cursor(value, scope) {
  if (value === undefined || value === null) return 0;
  assert(typeof value === 'string' && value.length < 1024, 'invalid_cursor');
  try {
    const data = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    assert(data.scope === scope, 'invalid_cursor');
    return integer(data.offset, 0, 100_000);
  } catch { throw new WorldError('invalid_cursor'); }
}
export const nextCursor = (offset, scope) => Buffer.from(JSON.stringify({ offset, scope })).toString('base64url');
