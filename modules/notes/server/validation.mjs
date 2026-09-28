import { createHash } from 'node:crypto';

export class NotesError extends Error {
  constructor(code) { super(code); this.name = 'NotesError'; this.code = code; }
}
export function check(value, code = 'notes_invalid_arguments') { if (!value) throw new NotesError(code); }
export const STATES = Object.freeze(['active', 'archived', 'trashed']);
export const COLORS = Object.freeze(['plain', 'honey', 'sage', 'lilac', 'blue', 'coral']);
export const DEFAULT_LIMITS = Object.freeze({ notes: 1000, identities: 10000, accountBytes: 16 * 1024 * 1024, noteBytes: 256 * 1024, receiptsPerNote: 32 });
export function keys(args, allowed) {
  check(args && typeof args === 'object' && !Array.isArray(args));
  check(Object.keys(args).every(key => allowed.includes(key)));
}
export function id(value) { check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_-]{7,95}$/u.test(value)); return value; }
export function revision(value) { check(Number.isSafeInteger(value) && value >= 0); return value; }
function text(value, max) { check(typeof value === 'string' && value.length <= max && !value.includes('\0')); return value; }
export function document(args, limits) {
  const title = text(args.title, 160); const body = text(args.body, 100000);
  check(Array.isArray(args.items) && args.items.length <= 200);
  const seen = new Set();
  const items = args.items.map(item => {
    keys(item, ['id', 'text', 'done']); id(item.id); check(!seen.has(item.id)); seen.add(item.id);
    check(typeof item.done === 'boolean'); return { id: item.id, text: text(item.text, 1000), done: item.done };
  });
  check(COLORS.includes(args.color) && STATES.includes(args.state) && typeof args.pinned === 'boolean');
  const doc = { title, body, items, color: args.color, pinned: args.pinned, state: args.state };
  const bytes = Buffer.byteLength(JSON.stringify(doc), 'utf8'); check(bytes <= limits.noteBytes, 'notes_note_too_large');
  return { ...doc, bytes };
}
export const hash = value => createHash('sha256').update(value).digest('hex');
export function queryTerms(value = '') {
  check(typeof value === 'string' && value.length <= 160);
  return [...new Set(value.normalize('NFKC').toLocaleLowerCase('ru').match(/[\p{L}\p{N}]+/gu) || [])].slice(0, 8);
}
export function encodeCursor(value) { return Buffer.from(JSON.stringify(value)).toString('base64url'); }
export function decodeCursor(value, scope) {
  if (value === undefined || value === null || value === '') return null;
  check(typeof value === 'string' && /^[A-Za-z0-9_-]{1,600}$/u.test(value), 'notes_invalid_cursor');
  let cursor; try { cursor = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')); } catch { throw new NotesError('notes_invalid_cursor'); }
  check(cursor && cursor.scope === scope && [0, 1].includes(cursor.pin) && Number.isSafeInteger(cursor.time) && cursor.time >= 0 && typeof cursor.id === 'string' && /^[A-Za-z0-9][A-Za-z0-9_-]{7,95}$/u.test(cursor.id), 'notes_invalid_cursor');
  return cursor;
}
