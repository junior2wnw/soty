import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, checkedStorageFormat, guardStorageStart, requireStorageStartReceipt,
  storageReaderLabel, storageReaders } from './storage-guard.mjs';

// Independently pinned historical code, not the current migrator or the
// author's literal fixture/seed. The SHA is verified before dynamic import.
const history = {
  notes: { url: new URL('./fixtures/notes-v1/schema.mjs', import.meta.url),
    sha: 'da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa' },
  capabilities: { url: new URL('./fixtures/capabilities-v1/schema.mjs', import.meta.url),
    sha: '959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad' },
};
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const hashId = '1'.repeat(64), imageId = `sha256:${'2'.repeat(64)}`;
const manifest = () => ({ version: 3, readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6], notes: [1], capabilities: [1] } });
const image = (value = manifest()) => ({ Id: imageId, Config: { Labels: { [storageReaderLabel]: JSON.stringify(value) } } });
const format = (notes = 'empty', capabilities = 'empty') => ({ ok: true, schema: 'soty.storage-format.v3',
  rooms: 'empty', apps: 'empty', notes, capabilities });
const json = value => JSON.parse(JSON.stringify(value));
const fault = code => error => error?.code === code;

async function historical(store) {
  assert.equal(digest(await readFile(history[store].url)), history[store].sha, `${store}: immutable historical source`);
  return import(history[store].url.href);
}

async function fixture(t) {
  const parent = await realpath(os.tmpdir());
  const root = await mkdtemp(path.join(parent, 'soty-storage-bridge-independent-'));
  const owner = randomBytes(24).toString('hex'), marker = path.join(root, '.independent-owner');
  await writeFile(marker, owner);
  const handles = new Set();
  t.after(async () => {
    for (const db of handles) db.close();
    handles.clear();
    // Only this invocation's canonical, marked synthetic directory is removed.
    // No glob, legacy test directory or computed ancestor is a cleanup target.
    assert.equal(await realpath(root), root);
    assert.equal(path.dirname(root), parent);
    assert.match(path.basename(root), /^soty-storage-bridge-independent-[A-Za-z0-9_-]+$/u);
    assert.equal(await readFile(marker, 'utf8'), owner);
    await rm(root, { recursive: true, force: true });
  });
  const filenames = Object.fromEntries(['notes', 'capabilities'].map(store => [store, path.join(root, store, `${store}.sqlite`)]));
  async function open(store) {
    await mkdir(path.dirname(filenames[store]), { recursive: true });
    const old = await historical(store), db = new DatabaseSync(filenames[store]);
    handles.add(db);
    if (store === 'notes') old.migrateNotes(db, 'soty');
    else old.initializeCapabilitiesSchema(db);
    db.exec('PRAGMA wal_autocheckpoint=0');
    return db;
  }
  function close(db) { db.close(); handles.delete(db); }
  return { root, filenames, open, close };
}

function seedNotes(db) {
  // Small own synthetic seed: one live body and one tombstone. This is storage
  // preservation evidence, not a claim that domain effect admission ran.
  const bytes = Buffer.byteLength(JSON.stringify({ title: 'PRIVATE NOTES TITLE', body: 'PRIVATE NOTES BODY 😀',
    items: [], color: 'plain', pinned: false, state: 'active' }));
  db.prepare('INSERT INTO note_accounts VALUES (?,?,?,?,?,?)').run('account-independent', bytes, 2, 1, 0, 0);
  const put = db.prepare('INSERT INTO notes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
  put.run(1, 'account-independent', 'note-active', 'PRIVATE NOTES TITLE', 'PRIVATE NOTES BODY 😀', '[]',
    'PRIVATE NOTES BODY 😀', 'plain', 0, 'active', 9, bytes, 1, 9);
  put.run(2, 'account-independent', 'note-deleted', '', '', '[]', '', 'plain', 0, 'deleted', 12, 0, 1, 12);
  db.prepare('INSERT INTO notes_fts(rowid,scope,title,body,items) VALUES(1,?,?,?,?)')
    .run('account-independent', 'PRIVATE NOTES TITLE', 'PRIVATE NOTES BODY 😀', '');
  db.prepare('INSERT INTO note_receipts VALUES (?,?,?,?,?,?)').run('account-independent', 'note-active',
    'old-mutation', 'a'.repeat(64), '{"id":"note-active","revision":9}', 9);
}

function seedCapabilities(db) {
  db.prepare('INSERT INTO cap_contracts VALUES (?,?,?)').run('notes.createDraft', 1,
    '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204');
  db.prepare('INSERT INTO cap_clients VALUES (?,?,?,?,?,?,?)')
    .run('client-independent', 'account-independent', 'PRIVATE CAPABILITY CLIENT', 'active', 7, 1, null);
  db.prepare('INSERT INTO cap_audit VALUES (?,?,?,?,?,?,?,?)')
    .run('audit-independent', 'account-independent', 'synthetic', 'client', 'client-independent', 'owner', 'device-independent', 2);
}

async function persistentBytes(f) {
  const result = {};
  for (const [store, filename] of Object.entries(f.filenames)) for (const suffix of ['', '-wal']) {
    try { result[store + suffix] = digest(await readFile(filename + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; result[store + suffix] = null; }
  }
  // SHM read marks are not claimed immutable. Main/WAL contain the durable data.
  return result;
}

async function inventory(root) {
  const entries = [];
  for (const dir of ['', 'notes', 'capabilities']) {
    try { for (const entry of await readdir(path.join(root, dir), { withFileTypes: true }))
      entries.push(`${dir}/${entry.name}:${entry.isDirectory() ? 'directory' : 'file'}`); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return entries.sort();
}

function guardFixture(root) {
  const volume = { Name: 'independent-store', Mountpoint: '/var/lib/docker/volumes/independent-store/_data',
    Driver: 'local', Scope: 'local', Options: null };
  const runtime = { Id: hashId, Image: imageId, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'] },
    HostConfig: { Mounts: [] }, Mounts: [{ Type: 'volume', Name: volume.Name, Source: volume.Mountpoint, Destination: '/data', RW: true }] };
  let probes = 0;
  const engine = {
    async inspect(id) { assert.equal(id, hashId); return json(runtime); },
    async image(id) { assert.equal(id, imageId); return image(); },
    async request(method, endpoint) {
      assert.equal(method, 'GET');
      if (endpoint === '/containers/json') return [];
      assert.equal(endpoint, '/volumes/independent-store'); return json(volume);
    },
  };
  return { runtime, context: { engine, probe: async () => { probes++; return readStorageFormat(root); } }, probes: () => probes };
}

test('B1 independent: exact historical populated stores survive RO projection without exporting private content', async t => {
  const f = await fixture(t), notes = await f.open('notes'), caps = await f.open('capabilities');
  seedNotes(notes); seedCapabilities(caps);
  const before = await persistentBytes(f), files = await inventory(f.root);
  const observed = await readStorageFormat(f.root);
  assert.deepEqual(observed, format(1, 1));
  assert.doesNotMatch(JSON.stringify(observed), /PRIVATE|account-independent|client-independent|note-active/u);
  assert.deepEqual(await persistentBytes(f), before, 'read-only success leaves main/WAL bytes unchanged');
  assert.deepEqual(await inventory(f.root), files, 'an existing WAL view needs no new files');
  assert.deepEqual(notes.prepare('SELECT id,state,revision FROM notes ORDER BY rowid').all().map(row => ({ ...row })),
    [{ id: 'note-active', state: 'active', revision: 9 }, { id: 'note-deleted', state: 'deleted', revision: 12 }]);
  assert.equal(notes.prepare("SELECT count(*) AS n FROM notes_fts WHERE notes_fts MATCH 'BODY'").get().n, 1);
  assert.equal(caps.prepare('SELECT policy_epoch FROM cap_clients').get().policy_epoch, 7);
});

test('B1 independent: absent and empty are accepted, orphan evidence in either store is never initialized or ignored', async t => {
  const f = await fixture(t);
  assert.deepEqual(await readStorageFormat(f.root), format());
  for (const store of ['notes', 'capabilities']) await mkdir(path.dirname(f.filenames[store]));
  assert.deepEqual(await readStorageFormat(f.root), format());
  for (const store of ['notes', 'capabilities']) {
    const g = await fixture(t);
    await mkdir(path.dirname(g.filenames[store]));
    const orphan = g.filenames[store] + '-wal';
    await writeFile(orphan, Buffer.from('synthetic orphan; not a database'));
    const before = await persistentBytes(g), files = await inventory(g.root);
    await assert.rejects(readStorageFormat(g.root), fault('storage_format_unreadable'));
    assert.deepEqual(await persistentBytes(g), before);
    assert.deepEqual(await inventory(g.root), files, 'refusal must not create a missing main or auxiliary file');
  }
});

for (const changed of ['notes', 'capabilities']) test(`B1 independent: fresh guard rejects committed ${changed} future WAL after a prior v3 receipt`, async t => {
  const f = await fixture(t), notes = await f.open('notes'), caps = await f.open('capabilities');
  seedNotes(notes); seedCapabilities(caps);
  notes.exec('PRAGMA wal_checkpoint(TRUNCATE)'); caps.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  const g = guardFixture(f.root), receipt = await guardStorageStart(g.context, g.runtime);
  requireStorageStartReceipt(receipt, hashId);
  assert.equal(receipt.notes, 1); assert.equal(receipt.capabilities, 1);
  const db = changed === 'notes' ? notes : caps;
  db.exec('BEGIN IMMEDIATE; PRAGMA user_version=2');
  if (changed === 'notes') db.prepare("UPDATE notes_meta SET value=? WHERE key='lineage'").run('soty.notes.sqlite.v2');
  else db.prepare("UPDATE cap_metadata SET value=? WHERE key='lineage'").run('soty.capabilities.sqlite.v2');
  db.exec('COMMIT');
  // A true committed tail with a historical1 main, not a mocked format enum.
  assert.equal((await readFile(f.filenames[changed])).readUInt32BE(60), 1);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 2);
  const before = await persistentBytes(f), files = await inventory(f.root);
  requireStorageStartReceipt(receipt, hashId); // Receipt shape is not present authority.
  await assert.rejects(guardStorageStart(g.context, g.runtime), fault('storage_format_unknown'));
  assert.equal(g.probes(), 2, 'the old valid receipt did not replace a fresh probe');
  assert.deepEqual(await persistentBytes(f), before);
  assert.deepEqual(await inventory(f.root), files);
});

test('B1 independent: wrong Notes identity and unreadable Cap projection fail without content in the error or file changes', async t => {
  for (const variant of ['wrong-project', 'duplicate-lineage', 'renamed-cap-column']) {
    const f = await fixture(t), notes = await f.open('notes'), caps = await f.open('capabilities');
    seedNotes(notes); seedCapabilities(caps);
    if (variant === 'wrong-project') notes.prepare("UPDATE notes_meta SET value=? WHERE key='project_id'").run('PRIVATE OTHER PROJECT');
    if (variant === 'duplicate-lineage') {
      notes.exec('ALTER TABLE notes_meta RENAME TO old_meta; CREATE TABLE notes_meta(key TEXT,value TEXT NOT NULL); INSERT INTO notes_meta SELECT * FROM old_meta; DROP TABLE old_meta');
      notes.prepare('INSERT INTO notes_meta VALUES (?,?)').run('lineage', 'soty.notes.sqlite.v1');
    }
    if (variant === 'renamed-cap-column') caps.exec('ALTER TABLE cap_clients RENAME COLUMN label TO private_client_label');
    const before = await persistentBytes(f);
    await assert.rejects(readStorageFormat(f.root), error => {
      assert.ok(['storage_format_unknown', 'storage_format_unreadable'].includes(error.code));
      assert.doesNotMatch(String(error), /PRIVATE|private_client_label|account-independent/u);
      return true;
    });
    assert.deepEqual(await persistentBytes(f), before);
  }
});

test('B1 independent: JSON manifests and format DTOs deny coercion, unknown stores and own prototype-named keys', () => {
  const base = manifest();
  assert.deepEqual(storageReaders(image()), base.readers);
  const badManifests = [
    { ...base, version: 2 }, { ...base, version: '3' }, { ...base, ignored: true },
    { ...base, readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6] } },
    ...['notes', 'capabilities'].flatMap(store => [[3], ['1'], [true], [1, 1], { 0: 1, length: 1 }, null]
      .map(value => ({ ...base, readers: { ...base.readers, [store]: value } }))),
    JSON.parse('{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1],"capabilities":[1]},"__proto__":{}}'),
  ];
  for (const candidate of badManifests) assert.throws(() => storageReaders(image(candidate)), fault('storage_reader_unknown'));
  assert.throws(() => storageReaders({ ...image(), Id: [imageId] }), fault('storage_image_identity_invalid'));
  const badFormats = [
    { ...format(), schema: 'soty.storage-format.v2' }, { ...format(), ok: 1 },
    { ...format(), futureStore: 'empty' },
    ...['notes', 'capabilities'].flatMap(store => ['1', true, [1], { value: 1 }, 0, 3, null]
      .map(value => ({ ...format(1, 1), [store]: value }))),
    JSON.parse('{"ok":true,"schema":"soty.storage-format.v3","rooms":"empty","apps":"empty","notes":1,"capabilities":1,"__proto__":{}}'),
  ];
  for (const candidate of badFormats) assert.throws(() => checkedStorageFormat(json(candidate)), fault('storage_probe_invalid'));
  assert.deepEqual(assertStorageCompatible(image(), format(1, 1)), format(1, 1));
  // B1b knows real version2; the frozen reader1 declaration still cannot read it.
  const nativeReaders = { ...base.readers, notes: [1, 2], capabilities: [1, 2] };
  const nativeImage = image({ ...base, readers: nativeReaders });
  assert.deepEqual(storageReaders(nativeImage), nativeReaders);
  assert.deepEqual(checkedStorageFormat(json(format(2, 2))), format(2, 2));
  assert.deepEqual(assertStorageCompatible(nativeImage, format(2, 2)), format(2, 2));
  for (const store of ['notes', 'capabilities']) {
    assert.throws(() => assertStorageCompatible(image(), { ...format(1, 1), [store]: 2 }), fault('storage_reader_incompatible'));
  }
  const oldAppsReader = image({ ...base, readers: { ...base.readers, apps: [1] } });
  assert.throws(() => assertStorageCompatible(oldAppsReader, { ...format(1, 1), apps: 6 }), fault('storage_reader_incompatible'));
});

test('B1 independent: persisted START receipts require every exact store, image and container identity', () => {
  const good = { schema: 'soty.storage-start.v3', containerId: hashId, image: imageId,
    mountSha256: '3'.repeat(64), rooms: 'empty', apps: 6, notes: 1, capabilities: 1 };
  requireStorageStartReceipt(good, hashId);
  requireStorageStartReceipt({ ...good, notes: 2, capabilities: 2 }, hashId);
  const { notes, ...withoutNotes } = good;
  const bad = [withoutNotes, { ...good, schema: 'soty.storage-start.v2' }, { ...good, notes: '1' },
    { ...good, capabilities: 3 }, { ...good, containerId: '4'.repeat(64) },
    { ...good, image: [imageId] }, { ...good, mountSha256: ['3'.repeat(64)] },
    JSON.parse(JSON.stringify(good).replace(/}$/, ',"constructor":{}}'))];
  for (const receipt of bad) assert.throws(() => requireStorageStartReceipt(receipt, hashId), fault('storage_start_guard_missing'));
});
