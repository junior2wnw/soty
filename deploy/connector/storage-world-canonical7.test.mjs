import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { migrateWorld } from '../../modules/world/server/schema.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { migrateWorld as frozenWorld6 } from '../../modules/world/test/support/world6-schema-before.mjs';
import { migrateField } from '../../modules/world/server/field.mjs';
import { readStorageFormat, readWorldFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, guardStorageStart, requireStorageStartReceipt, storageReaderLabel } from './storage-guard.mjs';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const reader6 = '{"version":7,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8,9,10,11,12,13,14],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4],"world":[6]}}';
const imageId = 'sha256:' + 'a'.repeat(64);
const image = label => ({ Id: imageId, Config: { Labels: { [storageReaderLabel]: label } } });
function fixture(t, { version = 7, field = true, wal = false } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'soty-canonical-world7-'));
  fs.mkdirSync(path.join(root, 'world'));
  const file = path.join(root, 'world', 'world.sqlite'), db = new DatabaseSync(file);
  t.after(() => { db.close(); fs.rmSync(root, { recursive: true, force: true }); });
  assert.equal(migrateWorld(db, 'soty', { allowWorld7Migration: version === 7 }), version);
  if (field) migrateField(db);
  db.exec(wal ? 'PRAGMA wal_autocheckpoint=0' : 'PRAGMA journal_mode=DELETE');
  return { root, file, db };
}
function bytes(f) {
  return Object.fromEntries(['', '-wal'].filter(suffix => fs.existsSync(f.file + suffix))
    .map(suffix => [suffix, sha(fs.readFileSync(f.file + suffix))]));
}

for (const field of [false, true]) test(`canonical World7 field${+field} is recognized from an exact independent layout without migration`, async t => {
  const f = fixture(t, { field }), before = bytes(f);
  assert.equal(await readWorldFormat(f.root), 7);
  const format = await readStorageFormat(f.root);
  assert.equal(assertStorageCompatible(image(currentStorageReaders), format).world, 7);
  assert.deepEqual(bytes(f), before);
  assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 7);
});

test('reader7 default boot and probe retain physical6 with attachment capability OFF', async t => {
  const f = fixture(t, { version: 6 });
  const service = createWorldService({ databasePath: f.file, projectId: 'soty' });
  try {
    assert.equal(service.schemaVersion, 6);
    const actor = { accountId: 'synthetic_owner', deviceId: 'synthetic_device', label: 'Synthetic' };
    assert.equal(service.execute({ op: 'world.profile.get', actor }).profile.chatAttachmentsVersion, undefined);
    const before = bytes(f);
    assert.equal(await readWorldFormat(f.root), 6);
    assert.equal(assertStorageCompatible(image(currentStorageReaders), await readStorageFormat(f.root)).world, 6);
    assert.equal(f.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name GLOB 'world_chat_attach*'").get().n, 0);
    assert.deepEqual(bytes(f), before);
  } finally { service.close(); }
});

test('exact previous reader6 refuses physical7 before START and frozen6 cannot rewrite it', async t => {
  const f = fixture(t), before = bytes(f), id = 'b'.repeat(64);
  const runtime = { Id: id, Image: imageId, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'] },
    Mounts: [{ Type: 'volume', Name: 'synthetic-canonical7', Source: '/volumes/synthetic-canonical7', Destination: '/data', RW: true }] };
  let starts = 0;
  const engine = { async inspect() { return structuredClone(runtime); }, async image() { return image(reader6); },
    async request(_method, target) { if (target === '/containers/json') return []; if (target === '/volumes/synthetic-canonical7') return { Name: 'synthetic-canonical7', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source }; throw Error('unexpected synthetic request'); },
    async start() { starts++; } };
  await assert.rejects(async () => { await guardStorageStart({ engine, probe: () => readStorageFormat(f.root) }, runtime); await engine.start(id); }, { code: 'storage_reader_incompatible' });
  assert.equal(starts, 0);
  assert.throws(() => frozenWorld6(f.db, 'soty'), /world_schema_unsupported/u);
  assert.deepEqual(bytes(f), before);
  engine.image = async () => image(currentStorageReaders);
  const receipt = await guardStorageStart({ engine, probe: () => readStorageFormat(f.root) }, runtime);
  requireStorageStartReceipt(receipt, id); assert.equal(receipt.world, 7);
  await engine.start(id); assert.equal(starts, 1); assert.deepEqual(bytes(f), before);
});

for (const [name, sql, code] of [
  ['wrong lineage', "UPDATE world_meta SET value='foreign.world' WHERE key='lineage'", 'storage_format_unknown'],
  ['mismatched metadata', "UPDATE world_meta SET value='6' WHERE key='schema_version'", 'storage_format_unknown'],
  ['missing attachment index', 'DROP INDEX world_chat_attachments_expiry', 'storage_format_unreadable'],
  ['foreign attachment trigger', 'CREATE TRIGGER foreign_chunk_effect AFTER INSERT ON world_chat_attachment_chunks BEGIN SELECT 1; END', 'storage_format_unreadable'],
  ['mismatched cover accounting', 'UPDATE profile_avatar_usage SET reserved_bytes=1', 'storage_format_unreadable'],
]) test(`canonical7 ${name} refuses without repairing original bytes`, async t => {
  const f = fixture(t); f.db.exec(sql); const before = bytes(f);
  await assert.rejects(readWorldFormat(f.root), { code }); assert.deepEqual(bytes(f), before);
});

test('partial7 on a physical6 marker and forged7 on an exact6 layout are both refused unchanged', async t => {
  for (const sql of ['CREATE TABLE world_chat_attachments(id TEXT PRIMARY KEY)', "PRAGMA user_version=7;UPDATE world_meta SET value='7' WHERE key='schema_version'"]) {
    const f = fixture(t, { version: 6 }); f.db.exec(sql); const before = bytes(f);
    await assert.rejects(readWorldFormat(f.root), { code: 'storage_format_unreadable' }); assert.deepEqual(bytes(f), before);
  }
});

test('populated canonical7 committed WAL is recognized with the original database and WAL unchanged', async t => {
  const f = fixture(t, { wal: true }), chunk = Buffer.from([1, 2, 3]);
  f.db.prepare('INSERT INTO profiles(account_id,display_name,search_text,created_at,updated_at) VALUES(?,?,?,?,?)').run('synthetic_owner', 'Synthetic', 'synthetic', 1, 1);
  f.db.prepare("INSERT INTO world_chat_attachments(id,scope_kind,conversation_id,author_id,name,media_type,size,state,created_at,expires_at) VALUES(?,'room',?,?,?,?,?,'pending',?,?)")
    .run('synthetic_file', 'synthetic_room', 'synthetic_owner', 'fixture.bin', 'application/octet-stream', chunk.length, 1, 100);
  f.db.prepare('INSERT INTO world_chat_attachment_chunks VALUES(?,?,?,?,?)').run('synthetic_file', 0, chunk.length, sha(chunk), chunk);
  const before = bytes(f); assert.ok(before['-wal']);
  assert.equal(await readWorldFormat(f.root), 7); assert.deepEqual(bytes(f), before);
  assert.deepEqual(Buffer.from(f.db.prepare('SELECT data FROM world_chat_attachment_chunks').get().data), chunk);
});
