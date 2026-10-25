import { DatabaseSync } from 'node:sqlite';
import { AppsError, assertApps, appId, textId } from './protocol.mjs';
import { createLaunchPath } from './launch-path.mjs';

const SCHEMA = 'soty.app-discussion-reads.v1';
const DDL = `CREATE TABLE discussion_read_meta (id INTEGER PRIMARY KEY CHECK(id=1),schema TEXT NOT NULL);
  CREATE TABLE discussion_reads (account_id TEXT NOT NULL,app_id TEXT NOT NULL,conversation_id TEXT NOT NULL,message_id TEXT NOT NULL,
    domain_id TEXT NOT NULL,path TEXT NOT NULL,
    read_seq INTEGER NOT NULL CHECK(read_seq BETWEEN 0 AND 9007199254740991),
    updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),PRIMARY KEY(account_id,app_id));`;

// Separate optional metadata leaves the strict app registry and its rollback readable.
// It stores one current generation per account/app; messages and authority stay in Apps.
export function createDiscussionReadStore({ filename = ':memory:', now = Date.now, maxRows = 1_000_000, maxPerAccount = 8192 } = {}) {
  assertApps(typeof filename === 'string' && filename.length > 0 && typeof now === 'function'
    && Number.isSafeInteger(maxRows) && maxRows > 0 && maxRows <= 1_000_000
    && Number.isSafeInteger(maxPerAccount) && maxPerAccount > 0 && maxPerAccount <= 8192,
  'apps_discussion_read_configuration_invalid', 500);
  const db = new DatabaseSync(filename);
  let closed = false;
  try {
    const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row => row.name);
    if (!tables.length) {
      db.exec('BEGIN IMMEDIATE');
      try { db.exec(DDL); db.prepare('INSERT INTO discussion_read_meta VALUES (1,?)').run(SCHEMA); db.exec('COMMIT'); }
      catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    } else {
      assertApps(tables.join(',') === 'discussion_read_meta,discussion_reads'
        && db.prepare('SELECT schema FROM discussion_read_meta WHERE id=1').get()?.schema === SCHEMA,
      'apps_discussion_read_schema_unsupported', 503);
      for (const [table, columns] of [['discussion_read_meta', 'id,schema'], ['discussion_reads', 'account_id,app_id,conversation_id,message_id,domain_id,path,read_seq,updated_at']]) {
        assertApps(db.prepare(`PRAGMA table_info(${table})`).all().map(row => row.name).join(',') === columns,
          'apps_discussion_read_schema_unsupported', 503);
      }
      assertApps(!db.prepare('SELECT 1 FROM discussion_reads WHERE typeof(read_seq)<>\'integer\' OR read_seq<0 OR read_seq>9007199254740991 OR typeof(updated_at)<>\'integer\' OR updated_at<0 OR updated_at>9007199254740991 LIMIT 1').get(),
        'apps_discussion_read_store_corrupt', 503);
    }
    db.exec('PRAGMA busy_timeout=100; PRAGMA journal_mode=WAL;');
  } catch (error) { db.close(); throw error; }
  function identifiers(accountId, id, conversationId) {
    assertApps(!closed, 'apps_discussion_read_closed', 503); textId(accountId); appId(id);
    assertApps(typeof conversationId === 'string' && /^conv_[a-f0-9]{32}$/u.test(conversationId), 'invalid_discussion_conv');
  }
  function save(accountId, entry, conversationId, sequence, messageId, resetInvalid = false) {
    const id = appId(entry?.appId); identifiers(accountId, id, conversationId);
    assertApps(typeof entry.domainId === 'string' && /^dom_[a-f0-9]{32}$/u.test(entry.domainId), 'invalid_discussion_dom');
    const path = createLaunchPath(entry.path).entryPath;
    assertApps(Number.isSafeInteger(sequence) && sequence >= 0 && typeof resetInvalid === 'boolean'
      && (sequence === 0 ? messageId === '' : typeof messageId === 'string' && /^msg_[a-f0-9]{32}$/u.test(messageId)), 'invalid_discussion_read_sequence');
    const at = now(); assertApps(Number.isSafeInteger(at) && at >= 0, 'apps_discussion_clock_invalid', 500);
    try {
      db.exec('BEGIN IMMEDIATE');
      const previous = db.prepare('SELECT conversation_id,read_seq,message_id,updated_at FROM discussion_reads WHERE account_id=? AND app_id=?').get(accountId, id);
      if (!previous) {
        assertApps(db.prepare('SELECT count(*) AS n FROM discussion_reads WHERE account_id=?').get(accountId).n < maxPerAccount
          && db.prepare('SELECT count(*) AS n FROM discussion_reads').get().n < maxRows, 'apps_discussion_read_capacity', 409);
      }
      const keep = previous?.conversation_id === conversationId && (sequence === 0 || !resetInvalid && previous.read_seq >= sequence);
      const readSequence = keep ? previous.read_seq : sequence, readMessageId = keep ? previous.message_id : messageId;
      const readAt = Math.max(previous?.updated_at ?? 0, at);
      db.prepare(`INSERT INTO discussion_reads VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(account_id,app_id) DO UPDATE SET
        conversation_id=excluded.conversation_id,message_id=excluded.message_id,domain_id=excluded.domain_id,path=excluded.path,
        read_seq=excluded.read_seq,updated_at=excluded.updated_at`).run(accountId, id, conversationId, readMessageId, entry.domainId, path, readSequence, readAt);
      db.exec('COMMIT'); return { readSequence, readAt };
    } catch (error) {
      if (db.isTransaction) db.exec('ROLLBACK');
      if (Number.isInteger(error?.errcode) && [5, 6].includes(error.errcode & 255)) throw new AppsError('apps_discussion_read_busy', 503);
      throw error;
    }
  }
  return Object.freeze({
    get(accountId, id, conversationId) {
      identifiers(accountId, id, conversationId);
      const row = db.prepare('SELECT read_seq,message_id FROM discussion_reads WHERE account_id=? AND app_id=? AND conversation_id=?').get(accountId, id, conversationId);
      return row?.read_seq > 0 ? { sequence: row.read_seq, messageId: row.message_id } : null;
    },
    mark(accountId, entry, conversationId, sequence, messageId, { resetInvalid = false } = {}) {
      assertApps(Number.isSafeInteger(sequence) && sequence >= 1, 'invalid_discussion_read_sequence');
      return save(accountId, entry, conversationId, sequence, messageId, resetInvalid);
    },
    remember(accountId, entry, conversationId) { save(accountId, entry, conversationId, 0, ''); },
    entries(accountId) {
      assertApps(!closed, 'apps_discussion_read_closed', 503); textId(accountId);
      return db.prepare('SELECT app_id,domain_id,path,updated_at FROM discussion_reads WHERE account_id=? ORDER BY updated_at DESC,app_id LIMIT 8192').all(accountId)
        .map(row => ({ appId: row.app_id, domainId: row.domain_id, path: row.path, at: row.updated_at }));
    },
    close() { if (!closed) { closed = true; db.close(); } }
  });
}
