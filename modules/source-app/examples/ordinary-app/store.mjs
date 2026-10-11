import { createSqliteSourceStorage } from '../../server/sqlite-source-storage.mjs';
import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { existsSync } from 'node:fs';
import { ORDINARY_SCHEMA, ORDINARY_SCHEMA_2, ORDINARY_META_2, ORDINARY_LOGIN_PROOF_DDL } from './schema.mjs';
import { verifyOrdinaryReader1 } from './reader.mjs';
import { verifyOrdinaryReader2 } from './reader2.mjs';
import { verifyOrdinaryReader3 } from './reader3.mjs';
import { ORDINARY_SCHEMA_3, ORDINARY_META_3, ORDINARY_JOB_DDL } from './job-schema.mjs';
import { check, fields, digest, jsonCopy, nonce, opaque, syncResult } from '../../server/wire.mjs';

/** The example has one actual Native SQL authority. Only host configuration
 * creates it; no Root/body owner flag or RP shadow creates Native permissions. */
export function createOrdinaryAppStore(options) {
  const value = fields(options, ['databasePath', 'realmId', 'key', 'keyId'], ['initialize', 'clock', 'format', 'allowLoginProofMigration','allowFeedbackJobsMigration']);
  check(typeof value.databasePath === 'string' && typeof value.realmId === 'string' && /^[a-z][a-z0-9.-]{0,63}$/u.test(value.realmId)
    && Buffer.isBuffer(value.key) && value.key.length === 32 && /^[a-zA-Z0-9_.-]{1,64}$/u.test(value.keyId));
  check(existsSync(value.databasePath) || value.initialize === true, 'ordinary_source_storage_not_ready', 503);
  const requestedFormat = value.format ?? 1; check([1,2,3].includes(requestedFormat) && (value.allowLoginProofMigration === undefined || typeof value.allowLoginProofMigration === 'boolean')
    &&(value.allowFeedbackJobsMigration===undefined||typeof value.allowFeedbackJobsMigration==='boolean'));
  const db = new DatabaseSync(value.databasePath), clock = value.clock ?? Date.now, key = Buffer.from(value.key);
  let reader;
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
    const objects=db.prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all();
    if(objects.length===0){
      check(value.initialize===true,'ordinary_source_storage_not_ready',503);
      db.exec('BEGIN IMMEDIATE');
      db.exec(requestedFormat===3?ORDINARY_SCHEMA_3:requestedFormat===2?ORDINARY_SCHEMA_2:ORDINARY_SCHEMA);
      db.prepare('INSERT INTO native_meta VALUES(?,?)').run(requestedFormat,value.realmId);
      // Verify independent format/FK oracle before the initialization commits.
      verifyOrdinaryReader3(db,value.realmId);db.exec('COMMIT');
    }
    reader = verifyOrdinaryReader3(db, value.realmId);
    if (requestedFormat === 2 && reader.format === 1) {
      check(value.allowLoginProofMigration === true, 'ordinary_source_migration_required', 503);
      verifyOrdinaryReader1(db, value.realmId); db.exec('BEGIN IMMEDIATE');
      // native_meta has no inbound FK. Preserve every other Native/BFF row
      // verbatim; the only replaced row is this explicitly versioned marker.
      db.exec('DROP TABLE native_meta; ' + ORDINARY_META_2 + ';'); db.prepare('INSERT INTO native_meta VALUES(2,?)').run(value.realmId);
      db.exec(ORDINARY_LOGIN_PROOF_DDL); verifyOrdinaryReader2(db, value.realmId); db.exec('COMMIT');
      reader = verifyOrdinaryReader2(db, value.realmId);
    }
    if(requestedFormat===3&&reader.format<3){
      check(reader.format===2&&value.allowFeedbackJobsMigration===true,'ordinary_source_jobs_migration_required',503);
      verifyOrdinaryReader2(db,value.realmId);db.exec('BEGIN IMMEDIATE');
      db.exec('DROP TABLE native_meta; '+ORDINARY_META_3+';');db.prepare('INSERT INTO native_meta VALUES(3,?)').run(value.realmId);
      db.exec(ORDINARY_JOB_DDL);verifyOrdinaryReader3(db,value.realmId);db.exec('COMMIT');
      reader=verifyOrdinaryReader3(db,value.realmId);
    }
    db.exec('PRAGMA journal_mode=WAL;');
  }catch(error){
    // Cleanup cannot replace the primary rejection or leave a live key when
    // BEGIN/ROLLBACK/reader/WAL itself fails. close rolls back any remaining TX.
    try{if(db.isTransaction)db.exec('ROLLBACK');}catch{}
    try{db.close();}catch{}
    finally{key.fill(0);}
    throw error;
  }
  let closed = false, transaction = false;
  function current() { check(!closed, 'ordinary_source_closed', 503); }
  function tx(action) {
    current(); check(!transaction, 'ordinary_source_transaction_invalid', 503); transaction = true; db.exec('BEGIN IMMEDIATE');
    try { const result = syncResult(action()); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; } finally { transaction = false; }
  }
  const aad = (model, id, revision) => JSON.stringify(['soty.ordinary-source.v1', value.realmId, model, id, revision, value.keyId]);
  function encrypt(model, id, revision, input) {
    current(); const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv); cipher.setAAD(Buffer.from(aad(model, id, revision)));
    const bytes = Buffer.concat([cipher.update(JSON.stringify(jsonCopy(input)), 'utf8'), cipher.final()]);
    return Buffer.concat([iv, cipher.getAuthTag(), bytes]).toString('base64');
  }
  function decrypt(model, id, revision, cipher, keyId) {
    current(); check(keyId === value.keyId, 'ordinary_source_storage_key_unavailable', 503);
    try { const bytes = Buffer.from(cipher, 'base64'); check(bytes.length > 28);
      const decipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12)); decipher.setAAD(Buffer.from(aad(model, id, revision))); decipher.setAuthTag(bytes.subarray(12, 28));
      return jsonCopy(JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString('utf8')));
    } catch { throw Object.assign(new Error('ordinary_source_storage_corrupt'), { code: 'ordinary_source_storage_corrupt', status: 503 }); }
  }
  const storage = createSqliteSourceStorage({ database: db, current, transaction: tx, encrypt, decrypt, clock, keyId: value.keyId });
  return Object.freeze({ db, storage, clock, realmId: value.realmId, format:reader.format, tx, inTransaction: () => transaction, encrypt, decrypt,
    /** Trusted Native provisioning only. These functions are never HTTP/RPC. */
    createResource({ id, incarnationId, title, guestEmpty = false }) { tx(() => { db.prepare('INSERT INTO native_resources VALUES(?,?,?,?,?)').run(id, incarnationId, value.realmId, title, guestEmpty ? 1 : 0); }); },
    createPrincipal(id) { tx(() => { db.prepare('INSERT INTO native_principals VALUES(?,?)').run(id, value.realmId); }); },
    grant(resourceId, principalId, role) { tx(() => { db.prepare('INSERT INTO native_memberships VALUES(?,?,?,1,1) ON CONFLICT(resource_id,principal_id) DO UPDATE SET role=excluded.role,active=1,revision=revision+1').run(resourceId, principalId, role); }); },
    createNativeSession(principalId) { const token = nonce(); tx(() => { db.prepare('INSERT INTO native_sessions VALUES(?,?,1,1,?)').run(digest(token), principalId, clock() + 86400000); }); return token; },
    revokeNativeSession(token) { check(opaque(token)); tx(() => { db.prepare('UPDATE native_sessions SET active=0,generation=generation+1 WHERE id_hash=?').run(digest(token)); }); },
    revokeMembership(resourceId, principalId) { tx(() => { db.prepare('UPDATE native_memberships SET active=0,revision=revision+1 WHERE resource_id=? AND principal_id=?').run(resourceId, principalId); }); },
    close() { if (closed) return; closed = true; key.fill(0); db.close(); },
  });
}
