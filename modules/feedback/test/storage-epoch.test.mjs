import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createFeedbackService } from '../server/index.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../server/profile.mjs';
import { readStorageFormat } from '../../../deploy/connector/storage-probe.mjs';
import { assertStorageReaderManifestCompatible, currentStorageReaders } from '../../../deploy/connector/storage-guard.mjs';

const code = expected => error => error?.code === expected;
const oldReaders = JSON.parse(currentStorageReaders); oldReaders.readers.feedback = [1];
const legacyReader = JSON.stringify(oldReaders);
const video = () => ({ kind: 'video', name: 'selected.webm', mimeType: 'video/webm',
  dataBase64: readFileSync(new URL('../../materials/test/fixtures/video.webm', import.meta.url)).toString('base64') });

function fixture(t, hook) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-feedback-epoch2-'));
  const databasePath = join(directory, 'feedback', 'feedback.sqlite');
  const actor = { accountId: 'reporter', deviceId: 'device' };
  const options = { databasePath, registryId: 'fixture', environmentId: 'test', actorActive: who => who?.accountId === actor.accountId,
    withAppAuthority({ appId }, run) { return run({ appId, accountId: actor.accountId, ownerId: 'owner', canManage: false, title: 'Fixture' }); },
    ...(hook ? { withTransaction: hook } : {}) };
  let service;
  const close = () => { service?.close(); service = undefined; };
  t.after(close);
  const open = (videoEnabled = true, extra = {}) => { close(); service = createFeedbackService({ ...options, videoEnabled, ...extra }); return service; };
  const call = (operation, args) => service.execute({ op: 'apps.feedback.' + operation, actor, args });
  open();
  const { appId, installationId } = call('context', { appId: 'app_fixture' }).context;
  const input = attachments => ({ appId, installationId, requestId: 'one-request', body: 'Synthetic issue', attachments });
  const inspect = action => {
    const db = new DatabaseSync(databasePath); try { return action(db); } finally { db.close(); }
  };
  const state = () => inspect(db => {
    const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
    const rows = Object.fromEntries(tables.map(({ name }) => [name, db.prepare('SELECT * FROM ' + name + ' ORDER BY rowid').all()]));
    return { epoch: db.prepare('PRAGMA user_version').get().user_version,
      data: createHash('sha256').update(JSON.stringify(rows)).digest('hex'),
      tickets: rows.feedback_tickets.length, attachments: rows.feedback_attachments.length, receipts: rows.feedback_receipts.length };
  });
  return { directory, open, close, call, input, inspect, state };
}

test('fresh and nonvideo stores retain epoch1, default Source admission and exact replay', async t => {
  const f = fixture(t), input = f.input([]), first = f.call('submit', input);
  assert.equal(f.state().epoch, 1);
  assert.equal((await readStorageFormat(f.directory)).feedback, 1);
  assertStorageReaderManifestCompatible(legacyReader, await readStorageFormat(f.directory));
  assert.equal(f.open(false).schemaVersion, 1);
  assert.equal(f.call('submit', input).replayed, true);
  assert.deepEqual(f.call('submit', input).receipt, first.receipt);
  assert.equal(DEFAULT_FEEDBACK_PROFILE.digest, '716516e51e0cab9261c737d13a0f494799c516d383a3b494d04ba606fe729a34');
  assert.equal(DEFAULT_FEEDBACK_PROFILE.feedback.captureProfile.digest, '94db961b2287b4231aedf0f8a954df7d9557432a8c052a30c29ec599f2c0f080');
  assert.throws(() => f.call('submit', { ...input, requestId: 'unsupported-video', attachments: [video()] }), code('feedback_media_unsupported'));
  assert.equal(f.state().epoch, 1);
});

test('the first video commits epoch2 with its ticket/receipt; old readers and disabled Source stay closed', async t => {
  const f = fixture(t), input = f.input([video()]), first = f.call('submit', input), before = f.state();
  assert.deepEqual({ epoch: before.epoch, tickets: before.tickets, attachments: before.attachments, receipts: before.receipts },
    { epoch: 2, tickets: 1, attachments: 1, receipts: 1 });
  const format = await readStorageFormat(f.directory);
  assert.equal(format.feedback, 2);
  assert.throws(() => assertStorageReaderManifestCompatible(legacyReader, format), code('storage_reader_incompatible'));
  assertStorageReaderManifestCompatible(currentStorageReaders, format);
  f.close();
  assert.throws(() => f.open(false), code('feedback_storage_unknown'));
  assert.deepEqual(f.state(), before);
  assert.equal(f.open().schemaVersion, 1);
  const replay = f.call('submit', input);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt);
  assert.deepEqual(f.state(), before);
});

for (const annotate of [false, true]) test(`a failed video transaction rolls back epoch, media, ticket and receipt together (annotate=${annotate})`, t => {
  let fail = false, before;
  const f = fixture(t, (run, addProof) => {
    const result = run();
    if (fail) {
      assert.deepEqual(f.state(), before, 'another connection sees neither the new epoch nor its uncommitted effect');
      if (annotate) addProof({ synthetic: true }); throw Error('synthetic_commit_failure');
    }
    return result;
  });
  before = f.state(); const input = f.input([video()]); fail = true;
  assert.throws(() => f.call('submit', input), /synthetic_commit_failure/u);
  assert.deepEqual(f.state(), before);
  fail = false;
  assert.equal(f.call('submit', input).replayed, false);
  assert.equal(f.state().epoch, 2);
});

test('legacy epoch1 plus stored video is logical2 before normalization; only enabled Root normalizes without changing data', async t => {
  const f = fixture(t), input = f.input([video()]), first = f.call('submit', input);
  f.close(); f.inspect(db => db.exec('PRAGMA user_version=1'));
  const legacy = f.state(), format = await readStorageFormat(f.directory);
  assert.equal(format.feedback, 2); assert.equal(f.state().epoch, 1);
  assert.throws(() => assertStorageReaderManifestCompatible(legacyReader, format), code('storage_reader_incompatible'));
  assert.throws(() => f.open(false), code('feedback_storage_unknown'));
  assert.deepEqual(f.state(), legacy);
  f.open();
  assert.deepEqual(f.state(), { ...legacy, epoch: 2 });
  const replay = f.call('submit', input);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt);
  assert.deepEqual(f.state(), { ...legacy, epoch: 2 });
});

test('a persisted epoch2 stays2 even without video rows, and unknown epochs remain closed', async t => {
  const f = fixture(t); f.close(); f.inspect(db => db.exec('PRAGMA user_version=2'));
  assert.equal((await readStorageFormat(f.directory)).feedback, 2);
  assert.throws(() => f.open(false), code('feedback_storage_unknown'));
  f.open(); assert.equal(f.state().epoch, 2); f.close();
  f.inspect(db => db.exec('PRAGMA user_version=3'));
  assert.throws(() => f.open(), code('feedback_storage_unknown'));
  await assert.rejects(readStorageFormat(f.directory), code('storage_format_unknown'));
  assert.equal(f.state().epoch, 3);
});

test('normalization cannot adopt a foreign identity or unrecognized layout', t => {
  const f = fixture(t); f.call('submit', f.input([video()])); f.close(); f.inspect(db => db.exec('PRAGMA user_version=1'));
  const before = f.state();
  assert.throws(() => f.open(true, { registryId: 'foreign_registry' }), code('feedback_storage_unknown'));
  assert.deepEqual(f.state(), before);
  f.inspect(db => db.exec('CREATE TABLE unrecognized(value TEXT) STRICT'));
  const damaged = f.state();
  assert.throws(() => f.open(), code('feedback_storage_unknown'));
  assert.deepEqual(f.state(), damaged);
});
