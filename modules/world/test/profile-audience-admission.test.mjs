import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { migrateWorld } from '../server/schema.mjs';
import { createWorldService } from '../server/index.mjs';
import { migrateWorld as migrateV3 } from './support/legacy-world-schema-v3.mjs';

const PROJECT = 'world-audience-admission';
const actor = Object.freeze({ accountId: 'admission_owner', deviceId: 'admission_owner_device', label: 'Автор' });
function oldStore(databasePath) {
  const db = new DatabaseSync(databasePath);
  try {
    migrateV3(db, PROJECT);
    db.prepare('INSERT INTO profiles(account_id,display_name,bio,search_text,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?)')
      .run(actor.accountId, actor.label, 'Существующий профиль', 'автор', 17, 11, 22);
    db.exec('PRAGMA journal_mode=DELETE');
    return snapshot(db);
  } finally { db.close(); }
}
function snapshot(db) {
  return { version: db.prepare('PRAGMA user_version').get().user_version,
    journal: db.prepare('PRAGMA journal_mode').get().journal_mode,
    schema: db.prepare('SELECT name,type,sql FROM sqlite_schema ORDER BY name').all(),
    meta: db.prepare('SELECT * FROM world_meta ORDER BY key').all(),
    profiles: db.prepare('SELECT * FROM profiles ORDER BY account_id').all() };
}

test('default audience admission rereads format under the actual writer lock: initial0 then another real connection creates3 remains unchanged', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-audience-admission-race-')), databasePath = join(directory, 'world.sqlite');
  const db = new DatabaseSync(databasePath);
  t.after(() => { db.close(); rmSync(directory, { recursive: true, force: true }); });
  let interleaved = false, before;
  // Only the public prepare/get boundary is wrapped. Both reads, all DDL and
  // the competing initialization use actual DatabaseSync connections.
  const boundary = {
    get isTransaction() { return db.isTransaction; },
    exec(sql) { return db.exec(sql); },
    prepare(sql) {
      const statement = db.prepare(sql);
      if (sql !== 'PRAGMA user_version' || interleaved) return statement;
      return { get(...args) {
        const observed = statement.get(...args);
        assert.equal(observed.user_version, 0);
        interleaved = true;
        before = oldStore(databasePath);
        return observed;
      } };
    },
  };
  assert.throws(() => migrateWorld(boundary, PROJECT), /world_audience_migration_required/u);
  assert.equal(interleaved, true);
  assert.deepEqual(snapshot(db), before);
  assert.equal(db.prepare("SELECT 1 FROM pragma_table_info('profiles') WHERE name='audience'").get(), undefined);
});

test('post-COMMIT WAL setup failure is an honest startup error with admitted6 preserved, followed by a healthy normal6 reopen', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-audience-admission-wal-')), databasePath = join(directory, 'world.sqlite');
  oldStore(databasePath);
  let db = new DatabaseSync(databasePath), service, rollbackCalls = 0, walAttempts = 0;
  t.after(() => { service?.close(); db?.close(); rmSync(directory, { recursive: true, force: true }); });
  const boundary = {
    get isTransaction() { return db.isTransaction; },
    prepare(sql) { return db.prepare(sql); },
    exec(sql) {
      if (sql.includes('PRAGMA journal_mode=WAL')) { walAttempts++; throw new Error('owned_wal_setup_failure'); }
      if (sql === 'ROLLBACK') rollbackCalls++;
      return db.exec(sql);
    },
  };
  assert.throws(() => migrateWorld(boundary, PROJECT, { allowAudienceMigration: true, allowWorld6Migration: true }), /owned_wal_setup_failure/u);
  assert.equal(walAttempts, 1); assert.equal(rollbackCalls, 0);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 6);
  assert.equal(db.prepare("SELECT value FROM world_meta WHERE key='schema_version'").get().value, '6');
  assert.equal(db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
  assert.deepEqual({ ...db.prepare('SELECT audience,revision,bio FROM profiles WHERE account_id=?').get(actor.accountId) },
    { audience: 'private', revision: 17, bio: 'Существующий профиль' });
  db.close(); db = null;
  service = createWorldService({ databasePath, projectId: PROJECT });
  const profile = service.execute({ actor, op: 'world.profile.get' }).profile;
  assert.equal(service.schemaVersion, 6); assert.equal(profile.revision, 17); assert.equal(profile.audience, 'private');
  const observer = new DatabaseSync(databasePath, { readOnly: true });
  try { assert.equal(observer.prepare('PRAGMA journal_mode').get().journal_mode, 'wal'); } finally { observer.close(); }
});
