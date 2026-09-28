import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { check, keys, id, revision, document, hash, queryTerms, encodeCursor, decodeCursor, STATES, DEFAULT_LIMITS, NotesError } from './validation.mjs';
import { migrateNotes, SCHEMA_VERSION } from './schema.mjs';
export { NotesError, SCHEMA_VERSION, DEFAULT_LIMITS };
export const NOTES_OPERATIONS = Object.freeze(['notes.list', 'notes.get', 'notes.put', 'notes.purge']);
const moduleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
function physicalPath(file) { try { return realpathSync(file); } catch (error) { if (error.code !== 'ENOENT') throw error; return resolve(physicalPath(dirname(file)), basename(file)); } }
function outsideModule(file) {
  const fold = value => process.platform === 'win32' ? value.toLowerCase() : value;
  const root = fold(physicalPath(moduleRoot));
  return ![file, physicalPath(file)].some(value => fold(value) === root || fold(value).startsWith(root + sep));
}
const metadata = row => ({ noteId: row.id, title: row.title, preview: row.preview, color: row.color, pinned: Boolean(row.pinned), state: row.state,
  revision: row.revision, createdAt: row.created_at, updatedAt: row.updated_at });
const fullNote = row => ({ ...metadata(row), body: row.body, items: JSON.parse(row.items) });

/** Trusted Connect extension; actor MUST come from an authenticated, non-revoked signed installation. */
export function createNotesService({ databasePath, projectId, clock = Date.now, limits: overrides = {} } = {}) {
  check(typeof databasePath === 'string' && databasePath.length > 0, 'notes_database_path_required');
  check(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'notes_project_id_required');
  check(typeof clock === 'function');
  keys(overrides, Object.keys(DEFAULT_LIMITS)); const limits = { ...DEFAULT_LIMITS, ...overrides };
  for (const [key, value] of Object.entries(limits)) check(Number.isSafeInteger(value) && value > 0 && value <= DEFAULT_LIMITS[key]);
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') { check(outsideModule(file), 'notes_database_must_be_outside_module'); mkdirSync(dirname(file), { recursive: true, mode: 0o700 }); }
  const db = new DatabaseSync(file); let closed = false;
  try { migrateNotes(db, projectId); } catch (error) { db.close(); throw error; }
  const statements = new Map(); const prepare = sql => { if (!statements.has(sql)) statements.set(sql, db.prepare(sql)); return statements.get(sql); };
  const get = (sql, ...params) => prepare(sql).get(...params);
  const all = (sql, ...params) => prepare(sql).all(...params);
  const run = (sql, ...params) => prepare(sql).run(...params);
  const accountUsage = accountId => {
    const row = get('SELECT bytes,active,archived,trashed FROM note_accounts WHERE account_id=?', accountId) || { bytes: 0, active: 0, archived: 0, trashed: 0 };
    return { bytes: row.bytes, maxBytes: limits.accountBytes, maxNotes: limits.notes, counts: { active: row.active, archived: row.archived, trashed: row.trashed } };
  };
  function receipt(accountId, noteId, mutationId, digest) {
    const row = get('SELECT digest,result FROM note_receipts WHERE account_id=? AND note_id=? AND mutation_id=?', accountId, noteId, mutationId);
    if (!row) return null; check(row.digest === digest, 'notes_mutation_reused'); return { ...JSON.parse(row.result), replayed: true };
  }
  function record(accountId, noteId, mutationId, digest, result) {
    run('INSERT INTO note_receipts(account_id,note_id,mutation_id,digest,result,revision) VALUES(?,?,?,?,?,?)', accountId, noteId, mutationId, digest, JSON.stringify(result), result.revision);
    run(`DELETE FROM note_receipts WHERE account_id=? AND note_id=? AND mutation_id IN
      (SELECT mutation_id FROM note_receipts WHERE account_id=? AND note_id=? ORDER BY revision DESC LIMIT -1 OFFSET ?)`, accountId, noteId, accountId, noteId, limits.receiptsPerNote);
  }
  function list(args, accountId) {
    keys(args, ['expectedAccountId', 'bucket', 'query', 'cursor', 'limit']);
    const bucket = args.bucket ?? 'active'; check(STATES.includes(bucket));
    const limit = args.limit ?? 30; check(Number.isInteger(limit) && limit >= 1 && limit <= 40);
    const terms = queryTerms(args.query); const scope = hash(JSON.stringify([accountId, bucket, terms]));
    const cursor = decodeCursor(args.cursor, scope);
    let from = 'notes n'; const clauses = ['n.account_id=?', 'n.state=?']; const params = [accountId, bucket];
    if (terms.length) {
      from += ' JOIN notes_fts f ON f.rowid=n.rowid'; clauses.push('notes_fts MATCH ?');
      params.push(`scope:"${hash(accountId)}" AND {title body items}:(${terms.map(term => `"${term}"*`).join(' AND ')})`);
    } else if ((args.query || '').trim()) clauses.push('0');
    if (cursor) { clauses.push('(n.pinned<? OR (n.pinned=? AND n.updated_at<?) OR (n.pinned=? AND n.updated_at=? AND n.id>?))'); params.push(cursor.pin, cursor.pin, cursor.time, cursor.pin, cursor.time, cursor.id); }
    const rows = all(`SELECT n.id,n.title,n.preview,n.color,n.pinned,n.state,n.revision,n.created_at,n.updated_at FROM ${from} WHERE ${clauses.join(' AND ')} ORDER BY n.pinned DESC,n.updated_at DESC,n.id ASC LIMIT ?`, ...params, limit + 1);
    const more = rows.length > limit; const page = rows.slice(0, limit); const last = page.at(-1);
    return { notes: page.map(metadata), nextCursor: more ? encodeCursor({ scope, pin: last.pinned, time: last.updated_at, id: last.id }) : null, usage: accountUsage(accountId) };
  }
  function put(args, accountId, now) {
    keys(args, ['expectedAccountId', 'noteId', 'mutationId', 'expectedRevision', 'title', 'body', 'items', 'color', 'pinned', 'state']);
    const noteId = id(args.noteId); const mutationId = id(args.mutationId); const expected = revision(args.expectedRevision);
    const doc = document(args, limits); const digest = hash(JSON.stringify([expected, doc]));
    const previous = get('SELECT * FROM notes WHERE account_id=? AND id=?', accountId, noteId);
    check(previous?.state !== 'deleted', 'notes_note_deleted');
    const replay = receipt(accountId, noteId, mutationId, digest); if (replay) return replay;
    if (!previous) check(expected === 0, 'notes_note_not_found');
    else check(previous.revision === expected, 'notes_revision_conflict');
    run('INSERT OR IGNORE INTO note_accounts(account_id) VALUES(?)', accountId);
    const usage = get('SELECT * FROM note_accounts WHERE account_id=?', accountId);
    check(previous || usage.active + usage.archived + usage.trashed < limits.notes, 'notes_count_quota');
    check(previous || usage.identities < limits.identities, 'notes_identity_quota');
    check(usage.bytes - (previous?.bytes || 0) + doc.bytes <= limits.accountBytes, 'notes_storage_quota');
    const nextRevision = (previous?.revision || 0) + 1;
    const updatedAt = Math.max(now, (previous?.updated_at || 0) + 1);
    const preview = (doc.body.trim() || doc.items.map(item => `${item.done ? '✓ ' : '□ '}${item.text}`).join(' · ')).replace(/\s+/gu, ' ').slice(0, 180);
    const values = [doc.title, doc.body, JSON.stringify(doc.items), preview, doc.color, Number(doc.pinned), doc.state, nextRevision, doc.bytes, updatedAt];
    let rowid;
    if (previous) {
      run('UPDATE notes SET title=?,body=?,items=?,preview=?,color=?,pinned=?,state=?,revision=?,bytes=?,updated_at=? WHERE rowid=?', ...values, previous.rowid); rowid = previous.rowid;
      run('DELETE FROM notes_fts WHERE rowid=?', rowid);
    } else rowid = Number(run('INSERT INTO notes(title,body,items,preview,color,pinned,state,revision,bytes,updated_at,account_id,id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)', ...values, accountId, noteId, now).lastInsertRowid);
    run('INSERT INTO notes_fts(rowid,scope,title,body,items) VALUES(?,?,?,?,?)', rowid, hash(accountId), doc.title, doc.body, doc.items.map(item => item.text).join('\n'));
    const counts = Object.fromEntries(STATES.map(state => [state, Number(doc.state === state) - Number(previous?.state === state)]));
    run('UPDATE note_accounts SET bytes=bytes+?,identities=identities+?,active=active+?,archived=archived+?,trashed=trashed+? WHERE account_id=?', doc.bytes - (previous?.bytes || 0), previous ? 0 : 1, counts.active, counts.archived, counts.trashed, accountId);
    const result = { noteId, revision: nextRevision, updatedAt }; record(accountId, noteId, mutationId, digest, result); return result;
  }
  function purge(args, accountId, now) {
    keys(args, ['expectedAccountId', 'noteId', 'mutationId', 'expectedRevision']);
    const noteId = id(args.noteId); const mutationId = id(args.mutationId); const expected = revision(args.expectedRevision);
    const digest = hash(JSON.stringify(['purge', expected])); const replay = receipt(accountId, noteId, mutationId, digest); if (replay) return replay;
    const previous = get('SELECT * FROM notes WHERE account_id=? AND id=?', accountId, noteId);
    check(previous && previous.state !== 'deleted', 'notes_note_not_found');
    check(previous.revision === expected, 'notes_revision_conflict'); check(previous.state === 'trashed', 'notes_trash_required');
    const updatedAt = Math.max(now, previous.updated_at + 1);
    run("UPDATE notes SET title='',body='',items='[]',preview='',color='plain',pinned=0,state='deleted',bytes=0,revision=revision+1,updated_at=? WHERE rowid=?", updatedAt, previous.rowid);
    run('DELETE FROM notes_fts WHERE rowid=?', previous.rowid);
    run('UPDATE note_accounts SET bytes=bytes-?,trashed=trashed-1 WHERE account_id=?', previous.bytes, accountId);
    const result = { noteId, revision: previous.revision + 1, updatedAt, deleted: true }; record(accountId, noteId, mutationId, digest, result); return result;
  }
  const operations = new Set(NOTES_OPERATIONS);
  return {
    projectId, schemaVersion: SCHEMA_VERSION, operations,
    execute({ op, args = {}, actor } = {}) {
      check(!closed, 'notes_service_closed'); check(operations.has(op), 'unsupported_operation');
      check(actor && typeof actor.accountId === 'string' && typeof actor.deviceId === 'string', 'authentication_required');
      const accountId = id(actor.accountId); id(actor.deviceId);
      check(args && args.expectedAccountId === accountId, 'notes_account_changed');
      const now = clock(); check(Number.isSafeInteger(now) && now >= 0);
      db.exec(op === 'notes.get' || op === 'notes.list' ? 'BEGIN' : 'BEGIN IMMEDIATE');
      try {
        let result;
        if (op === 'notes.list') result = list(args, accountId);
        else if (op === 'notes.put') result = put(args, accountId, now);
        else if (op === 'notes.purge') result = purge(args, accountId, now);
        else {
          keys(args, ['expectedAccountId', 'noteId']); const noteId = id(args.noteId);
          const row = get("SELECT * FROM notes WHERE account_id=? AND id=? AND state!='deleted'", accountId, noteId);
          check(row, 'notes_note_not_found'); result = { note: fullNote(row) };
        }
        db.exec('COMMIT'); return result;
      } catch (error) { db.exec('ROLLBACK'); throw error; }
    },
    close() { if (!closed) { closed = true; statements.clear(); db.close(); } },
  };
}
