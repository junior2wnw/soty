import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { connectorKey } from '../../modules/apps/server/protocol.mjs';

test('actual signed Root maintenance denies the first video and all new feedback effects, preserving reads and lost-ACK replay through restart', { timeout: 20000 }, async t => {
  const f = await nativeHttpFixture(t, { appOrigins: true }), owner = nativeIdentity('Synthetic owner'), reporter = nativeIdentity('Synthetic reporter');
  const ownerId = (await f.bootstrap(owner)).accountId, reporterId = (await f.bootstrap(reporter)).accountId;
  const identity = { linkId: 'maintenance-link', hostDeviceId: 'maintenance-host', connectorId: 'maintenance-connector' };
  const appsDb = new DatabaseSync(join(f.directory, 'apps', 'registry.sqlite'));
  try { appsDb.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(connectorKey(identity), ownerId, JSON.stringify(identity), 'Synthetic device', Date.now()); }
  finally { appsDb.close(); }
  const appId = good(await f.call(owner, 'apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
    name: 'Synthetic maintenance app', port: 42002, grants: { accountIds: [reporterId], communityIds: [] } })).app.id;
  const { installationId } = good(await f.call(reporter, 'apps.feedback.context', { appId })).context, scope = { appId, installationId };
  const textInput = { ...scope, requestId: 'text-before-maintenance', body: 'Existing synthetic text', attachments: [] };
  const text = good(await f.call(reporter, 'apps.feedback.submit', textInput)), selected = { ...scope, ticketId: text.receipt.ticketId };
  const statusInput = { ...selected, requestId: 'ready-before-maintenance', expectedRevision: 1, status: 'ready_to_check' };
  const status = good(await f.call(owner, 'apps.feedback.status', statusInput));
  const videoInput = { ...scope, requestId: 'first-video-maintenance-once', body: 'Synthetic first video', attachments: [{ kind: 'video', name: 'selected.webm',
    mimeType: 'video/webm', dataBase64: readFileSync(new URL('../../modules/materials/test/fixtures/video.webm', import.meta.url)).toString('base64') }] };
  const file = join(f.directory, 'feedback', 'feedback.sqlite'), marker = join(f.directory, 'connector-maintenance.json');
  const state = () => f.sql(file, db => {
    const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
    const rows = Object.fromEntries(tables.map(({ name }) => [name, db.prepare('SELECT * FROM ' + name + ' ORDER BY rowid').all()]));
    return { epoch: db.prepare('PRAGMA user_version').get().user_version, tickets: rows.feedback_tickets.length, attachments: rows.feedback_attachments.length,
      receipts: rows.feedback_receipts.length, dataDigest: createHash('sha256').update(JSON.stringify(rows)).digest('hex') };
  });
  const wire = async (actor, op, args) => f.http('/api/connect/rpc', { method: 'POST', headers: { origin: f.origin },
    body: JSON.stringify({ protocol: 1, ...await f.proof(actor, op, args) }) });
  const before = state(); assert.equal(before.epoch, 1); assert.equal(before.attachments, 0);
  writeFileSync(marker, '{}');
  const readiness = await f.http('/api/connectors/storage-ready'); assert.equal(readiness.body.maintenance, true);
  const refused = await wire(reporter, 'apps.feedback.submit', videoInput);
  t.diagnostic(JSON.stringify({ phase: 'first-video-under-maintenance', http: refused.status, code: refused.body.error?.code ?? null,
    before: { epoch: before.epoch, tickets: before.tickets, attachments: before.attachments }, after: state() }));
  assert.equal(refused.status, 503); assert.equal(refused.body.ok, false); assert.equal(refused.body.error.code, 'feedback_busy');
  assert.deepEqual(state(), before);
  for (const [actor, operation, args] of [[owner, 'reply', { ...selected, requestId: 'new-reply', expectedRevision: 2, body: 'Synthetic reply' }],
    [owner, 'status', { ...selected, requestId: 'new-status', expectedRevision: 2, status: 'in_progress' }],
    [reporter, 'accept', { ...selected, requestId: 'new-accept', expectedRevision: 2 }]]) {
    const value = await wire(actor, 'apps.feedback.' + operation, args); assert.equal(value.status, 503); assert.equal(value.body.error.code, 'feedback_busy');
  }
  assert.equal(good(await f.call(reporter, 'apps.feedback.context', { appId })).context.canSubmit, true);
  assert.equal(good(await f.call(owner, 'apps.feedback.list', scope)).tickets.length, 1);
  assert.equal(good(await f.call(owner, 'apps.feedback.get', selected)).ticket.status, 'ready_to_check');
  assert.deepEqual(good(await f.call(reporter, 'apps.feedback.submit', textInput)).receipt, text.receipt);
  assert.deepEqual(good(await f.call(owner, 'apps.feedback.status', statusInput)).receipt, status.receipt);
  assert.deepEqual(state(), before);
  unlinkSync(marker);
  let loseAck = true, receipt;
  f.setFront((req, res) => {
    let input = '', operation = ''; req.on('data', value => { if (input.length < 2200000) input += value.toString(); });
    req.on('end', () => { try { const value = JSON.parse(input); operation = value.op; } catch {} input = ''; });
    const end = res.end.bind(res); res.end = function (chunk, ...args) {
      if (loseAck && operation === 'apps.feedback.submit') {
        let value; try { value = JSON.parse(String(chunk)); } catch {}
        if (value?.ok === true && value.receipt?.ticketId) { loseAck = false; receipt = value.receipt; res.destroy(); return res; }
      }
      return end(chunk, ...args);
    }; f.app(req, res);
  });
  await assert.rejects(wire(reporter, 'apps.feedback.submit', videoInput), error => error.code === 'ECONNRESET');
  assert.ok(receipt); const committed = state(); assert.equal(committed.epoch, 2); assert.equal(committed.tickets, 2); assert.equal(committed.attachments, 1);
  writeFileSync(marker, '{}');
  const replay = good(await f.call(reporter, 'apps.feedback.submit', videoInput)); assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, receipt);
  assert.deepEqual(state(), committed);
  await f.restart({ enabled: true });
  assert.equal((await f.http('/api/connectors/storage-ready')).body.maintenance, true);
  const reopened = good(await f.call(reporter, 'apps.feedback.submit', videoInput)); assert.equal(reopened.replayed, true); assert.deepEqual(reopened.receipt, receipt);
  assert.equal(good(await f.call(owner, 'apps.feedback.get', { ...scope, ticketId: receipt.ticketId })).ticket.attachments[0].dataBase64, videoInput.attachments[0].dataBase64);
  assert.equal((await wire(reporter, 'apps.feedback.submit', { ...videoInput, requestId: 'new-after-restart' })).status, 503);
  assert.deepEqual(state(), committed);
  t.diagnostic(JSON.stringify({ phase: 'maintenance-leave-same-id-lost-ack-restart', sameRequestIdAcceptedAfterLeave: true,
    persistedEpoch: committed.epoch, videoTicketsAdded: committed.tickets - before.tickets, exactReplayDuringMaintenance: true, replayAfterRestart: true }));
});
