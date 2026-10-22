import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, access } from 'node:fs/promises';
import { join, dirname, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { coreFeedbackHttpFixture as fixture } from './support/core-feedback-http.mjs';
import { createHttpApp } from '../http-app.js';
import { createCoreFeedback, coreFeedbackConfigurationFromEnvironment } from '../core-feedback.js';
import { appId as checkedAppId } from '../../modules/apps/server/protocol.mjs';
import { readStorageFormat } from '../../deploy/connector/storage-probe.mjs';
import { snapshotStorage } from '../../deploy/connector/storage-snapshot.mjs';

const rpc = (who, action, args = {}) => who.client.extension(`core.feedback.${action}`, args);
const fails = code => error => error.code === code;
async function submit(f, requestId = 'core-first') {
  const { context } = await rpc(f.reporter, 'context');
  const args = { installationId: context.installationId, requestId, body: 'Private problem in Soty', attachments: [] };
  return { context, args, result: await rpc(f.reporter, 'submit', args) };
}

test('core support is explicitly OFF, opens no storage and cannot select a recipient from the actor', async t => {
  const folder = await mkdtemp(join(tmpdir(), 'soty-core-feedback-off-'));
  t.after(async () => { assert.equal(dirname(resolve(folder)), resolve(tmpdir())); await rm(folder, { recursive: true, force: true }); });
  const file = join(folder, 'feedback', 'feedback.sqlite');
  const off = createCoreFeedback({ databasePath: file, actorActive: () => true });
  assert.deepEqual(off.execute({ op: 'core.feedback.context', actor: {}, args: {} }), { readiness: 'unavailable', context: null });
  assert.throws(() => off.execute({ op: 'core.feedback.submit', actor: {}, args: {} }), fails('core_feedback_unavailable'));
  await assert.rejects(access(file), { code: 'ENOENT' }); off.close();
  assert.deepEqual(coreFeedbackConfigurationFromEnvironment({}), { enabled: false, supportOwnerAccountId: null });
  for (const configuration of [{ enabled: true }, { enabled: true, supportOwnerAccountId: 'fixture' }, { enabled: true, owner: 'invented' }])
    assert.throws(() => createCoreFeedback({ configuration, actorActive: () => true }), fails('core_feedback_configuration_invalid'));
  assert.throws(() => coreFeedbackConfigurationFromEnvironment({ SOTY_CORE_FEEDBACK_ENABLED: '1' }), fails('core_feedback_configuration_invalid'));
  const invalidHostData = join(folder, 'invalid-host');
  assert.throws(() => createHttpApp(folder, { env: {}, dataDir: invalidHostData, coreFeedback: { enabled: true } }), fails('core_feedback_configuration_invalid'));
  await assert.rejects(access(invalidHostData), { code: 'ENOENT' });
});

test('actual signed reporter lost ACK replays after restart, owner replies, reporter reads and accepts privately', async t => {
  const f = await fixture(t);
  assert.deepEqual(await rpc(f.reporter, 'context'), { ok: true, readiness: 'unavailable', context: null });
  await f.restart();
  const { context } = await rpc(f.reporter, 'context');
  assert.equal(context.scope, 'core'); assert.equal(context.recipientLabel, 'Поддержка Сот');
  assert.equal(Object.hasOwn(context, 'appId'), false); assert.equal(Object.hasOwn(context, 'entry'), false);
  const args = { installationId: context.installationId, requestId: 'lost-core-ack', body: 'Private problem in Soty', attachments: [] };
  f.loseNextSubmit(); await assert.rejects(rpc(f.reporter, 'submit', args));
  await f.restart();
  const repeated = await rpc(f.reporter, 'submit', args); assert.equal(repeated.replayed, true);
  const base = { installationId: context.installationId, ticketId: repeated.receipt.ticketId };
  assert.equal((await rpc(f.reporter, 'list', { installationId: context.installationId })).tickets.length, 1);
  assert.equal((await rpc(f.foreign, 'list', { installationId: context.installationId })).tickets.length, 0);
  await assert.rejects(rpc(f.foreign, 'get', base), fails('feedback_ticket_unavailable'));
  await assert.rejects(rpc(f.foreign, 'reply', { ...base, requestId: 'foreign-reply', expectedRevision: 1, body: 'No access' }), fails('feedback_ticket_unavailable'));
  const queue = await rpc(f.owner, 'list', { installationId: context.installationId }); assert.equal(queue.tickets[0].id, base.ticketId);
  const answer = await rpc(f.owner, 'reply', { ...base, requestId: 'support-reply', expectedRevision: 1, body: 'Here are the steps' });
  assert.equal(answer.ticket.messages[0].kind, 'support'); assert.equal(answer.ticket.status, 'in_progress');
  await assert.rejects(rpc(f.reporter, 'status', { ...base, requestId: 'not-support', expectedRevision: 2, status: 'ready_to_check' }), fails('feedback_support_required'));
  const ready = await rpc(f.owner, 'status', { ...base, requestId: 'ready', expectedRevision: 2, status: 'ready_to_check' });
  assert.equal((await rpc(f.reporter, 'get', base)).ticket.messages[0].body, 'Here are the steps');
  await assert.rejects(rpc(f.owner, 'accept', { ...base, requestId: 'not-reporter', expectedRevision: ready.ticket.revision }), fails('feedback_acceptance_required'));
  const accepted = await rpc(f.reporter, 'accept', { ...base, requestId: 'accepted', expectedRevision: ready.ticket.revision });
  assert.equal(accepted.ticket.status, 'resolved');
  await f.restart(); assert.equal((await rpc(f.reporter, 'get', base)).ticket.status, 'resolved');
  const unsigned = await fetch(f.origin + '/api/connect/rpc', { method: 'POST', headers: { 'content-type': 'application/json', origin: f.origin },
    body: JSON.stringify({ protocol: 1, op: 'core.feedback.get', args: base }) });
  assert.equal((await unsigned.json()).error.code, 'proof_required');
});

test('client-selected app/recipient/owner never alters core authority, and ordinary Apps cannot access its reserved namespace', async t => {
  const f = await fixture(t); await f.restart(); const { context, result } = await submit(f);
  const reserved = 'soty.core.feedback'; assert.throws(() => checkedAppId(reserved), fails('invalid_app_id'));
  for (const key of ['appId', 'owner', 'ownerId', 'recipient', 'supportOwnerAccountId', 'domainId', 'path'])
    await assert.rejects(rpc(f.reporter, 'context', { [key]: 'forged' }), fails('core_feedback_invalid_arguments'));
  for (const who of [f.reporter, f.owner]) {
    await assert.rejects(who.client.extension('apps.feedback.context', { appId: reserved }), fails('invalid_app_id'));
    await assert.rejects(who.client.extension('apps.feedback.get', { appId: reserved, installationId: context.installationId, ticketId: result.receipt.ticketId }), fails('invalid_app_id'));
    const ordinary = await who.client.extension('apps.list'); assert.deepEqual(ordinary.apps, []);
  }
  assert.equal((await fetch(f.origin + '/api/apps/catalog').then(value => value.json())).apps.length, 0);
});

test('existing and newly competing owners cannot silently replace the first core queue recipient', async t => {
  const f = await fixture(t); await f.restart();
  const contender = createCoreFeedback({ databasePath: join(f.dataDir, 'feedback', 'feedback.sqlite'), connectDatabasePath: join(f.dataDir, 'connect', 'accounts.sqlite'),
    configuration: { enabled: true, supportOwnerAccountId: f.foreign.account.accountId }, actorActive: actor => f.app.locals.connectService.isActorActive(actor) });
  try {
    const first = await submit(f);
    assert.throws(() => contender.execute({ op: 'core.feedback.context', actor: f.reporter.account, args: {} }), fails('core_feedback_owner_change_requires_migration'));
    await assert.rejects(f.restart({ coreFeedback: { enabled: true, supportOwnerAccountId: f.foreign.account.accountId } }), fails('core_feedback_owner_change_requires_migration'));
    await f.restart();
    assert.equal((await rpc(f.reporter, 'get', { installationId: first.context.installationId, ticketId: first.result.receipt.ticketId })).ticket.body, 'Private problem in Soty');
  } finally { contender.close(); }
  await assert.rejects(f.restart({ coreFeedback: { enabled: true, supportOwnerAccountId: `acct_${'x'.repeat(32)}` } }), fails('core_feedback_owner_unavailable'));
});

test('revoked device cannot read or reply to a previously admitted ticket', async t => {
  const f = await fixture(t); await f.restart(); const { context, result } = await submit(f);
  const extra = await f.client('Additional reporter device', false);
  const enrollment = await extra.client.startEnrollment('Additional reporter device');
  await f.reporter.client.approveEnrollment(enrollment.requestId, f.reporter.account.accountId);
  const preview = await extra.client.previewEnrollment(enrollment.requestId);
  await extra.client.finishEnrollment(enrollment.requestId, preview.account.accountId);
  await extra.client.revokeDevice(f.reporter.account.deviceId);
  await assert.rejects(rpc(f.reporter, 'get', { installationId: context.installationId, ticketId: result.receipt.ticketId }), error => ['device_revoked', 'NO_LOCAL_PROFILE'].includes(error.code));
  await assert.rejects(rpc(f.reporter, 'reply', { installationId: context.installationId, ticketId: result.receipt.ticketId,
    requestId: 'revoked-reply', expectedRevision: 1, body: 'Must not be saved' }), error => ['device_revoked', 'NO_LOCAL_PROFILE'].includes(error.code));
});

test('canonical independent storage reader and offline snapshot retain core tickets without new objects or reader allowances', async t => {
  const f = await fixture(t); await f.restart(); const { context, result } = await submit(f);
  await f.stop();
  const observation = await readStorageFormat(f.dataDir); assert.equal(observation.feedback, 1);
  const restored = join(f.folder, 'restored'); await snapshotStorage(f.dataDir, restored);
  assert.deepEqual(await readStorageFormat(restored), observation);
  const db = new DatabaseSync(join(restored, 'feedback', 'feedback.sqlite'), { readOnly: true });
  try { assert.equal(db.prepare('PRAGMA user_version').get().user_version, 1); assert.equal(db.prepare('SELECT count(*) AS n FROM feedback_tickets').get().n, 1); }
  finally { db.close(); }
  await f.restart({ dataDir: restored, coreFeedback: f.configuration });
  assert.equal((await rpc(f.reporter, 'get', { installationId: context.installationId, ticketId: result.receipt.ticketId })).ticket.body, 'Private problem in Soty');
});

test('core support works with universal Apps feedback disabled and still uses the canonical recognised feedback1 file', async t => {
  const f = await fixture(t, { universalAppsEnabled: false }); await f.restart();
  const { context, result } = await submit(f);
  assert.equal((await rpc(f.owner, 'get', { installationId: context.installationId, ticketId: result.receipt.ticketId })).ticket.canManage, true);
  await assert.rejects(f.owner.client.extension('apps.feedback.context', { appId: 'soty.core.feedback' }), fails('unsupported_operation'));
  await f.stop(); const observed = await readStorageFormat(f.dataDir);
  assert.equal(observed.feedback, 1); assert.equal(observed.appRegistration, 'empty');
});
