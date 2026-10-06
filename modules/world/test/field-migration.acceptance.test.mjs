import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService as legacyWorld } from './support/legacy-world-v3.mjs';
import { createWorldService } from '../server/index.mjs';

const actor = { accountId: 'legacy_field_account', deviceId: 'legacy_field_device', label: 'Existing World actor' };
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
function snapshot(db) {
  const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").all();
  return Object.fromEntries(tables.map(({ name }) => [name, db.prepare(`SELECT * FROM "${name.replaceAll('"', '""')}"`).all()
    .map(row => JSON.stringify(Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value instanceof Uint8Array ? Buffer.from(value).toString('hex') : value])))).sort()]));
}

test('actual frozen 5166b3b v3 reader prepares baseline; current boot performs only the declared additive migration; old boot preserves new layout', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-field-old-reader-')), databasePath = join(directory, 'world.sqlite'); let instance;
  const options = { databasePath, projectId: 'field-legacy-acceptance', clock: () => 1_800_000_000_000 };
  t.after(() => { instance?.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-field-old-reader-/u); rmSync(directory, { recursive: true, force: true }); });
  instance = legacyWorld(options);
  instance.execute({ actor, op: 'world.profile.get' });
  instance.execute({ actor, op: 'world.community.create', args: { requestId: 'legacy_group_request', name: 'Existing community', joinPolicy: 'open' } });
  assert.equal(instance.operations.has('world.field.put'), false); instance.close(); instance = null;
  const beforeDb = new DatabaseSync(databasePath), before = snapshot(beforeDb);
  assert.equal(beforeDb.prepare('PRAGMA user_version').get().user_version, 3);
  assert.equal(beforeDb.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get(), undefined); beforeDb.close();
  instance = createWorldService(options);
  const afterDb = new DatabaseSync(databasePath), after = snapshot(afterDb);
  assert.equal(afterDb.prepare('PRAGMA user_version').get().user_version, 3);
  assert.deepEqual(Object.keys(after).filter(name => !Object.hasOwn(before, name)).sort(), ['world_field_documents', 'world_field_receipts']);
  assert.equal(after.world_field_documents.length, 0); assert.equal(after.world_field_receipts.length, 0);
  assert.deepEqual(after.world_meta.filter(row => JSON.parse(row).key !== 'field_schema_version'), before.world_meta);
  assert.deepEqual(after.world_meta.filter(row => JSON.parse(row).key === 'field_schema_version').map(row => JSON.parse(row)), [{ key: 'field_schema_version', value: '1' }]);
  for (const [name, rows] of Object.entries(before)) if (name !== 'world_meta') assert.equal(hash(after[name]), hash(rows), name);
  assert.equal(afterDb.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'world_field_%'").get().n, 6); afterDb.close();
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'personal', title: 'Known new positions', x: 48, y: 26 }], shortcuts: [] };
  instance.execute({ actor, op: 'world.field.put', args: { expectedAccountId: actor.accountId, expectedRevision: 0, requestId: 'new_field_after_migration', document } });
  instance.close(); instance = legacyWorld(options); instance.execute({ actor, op: 'world.profile.get' }); instance.close(); instance = null;
  const preservedDb = new DatabaseSync(databasePath);
  assert.equal(preservedDb.prepare('SELECT document_json FROM world_field_documents').get().document_json, JSON.stringify(document));
  assert.equal(preservedDb.prepare('SELECT accepted_revision FROM world_field_receipts').get().accepted_revision, 1);
  assert.equal(preservedDb.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value, '1'); preservedDb.close();
  instance = createWorldService(options);
  assert.deepEqual(instance.execute({ actor, op: 'world.field.get', args: { expectedAccountId: actor.accountId } }).document, document);
});
