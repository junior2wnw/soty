import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join, resolve, dirname, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { createFeedbackService } from '../server/index.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../server/profile.mjs';
import { contractDigest } from '../../app-contract/json.mjs';

function fixture(t, filename = ':memory:', limits = {}) {
  const active = new Set(['owner', 'one', 'two']);
  const access = new Set(['owner', 'one', 'two']);
  const actor = name => ({ accountId: name, deviceId: 'device_' + name });
  const scope = (who, appId) => ({ appId, ownerId: 'owner', accountId: who.accountId, canManage: who.accountId === 'owner',
    title: 'Fixture', entry: { appId, domainId: 'domain', origin: 'http://localhost', path: '/' } });
  const options = { databasePath: filename, registryId: 'fixture', environmentId: 'test', limits, actorActive: who => active.has(who.accountId),
    withAppAuthority({ actor: who, appId }, callback) {
      if (!access.has(who.accountId)) throw Object.assign(new Error('denied'), { code: 'apps_access_denied' });
      return callback(scope(who, appId));
    } };
  let service = createFeedbackService(options);
  t.after(() => service.close());
  const exec = (name, op, args) => service.execute({ op: 'apps.feedback.' + op, actor: actor(name), args });
  const context = (name = 'one', appId = 'app_fixture') => exec(name, 'context', { appId }).context;
  return { exec, context, active, access, actor, service: () => service,
    restart() { service.close(); service = createFeedbackService(options); } };
}
const fails = (fn, code) => assert.throws(fn, error => error.code === code);

test('durable text receipt survives restart, exact lost-ACK replay and source access checks', t => {
  const folder = mkdtempSync(join(tmpdir(), 'soty-feedback-test-'));
  const f = fixture(t, join(folder, 'feedback.sqlite')), c = f.context();
  t.after(() => { assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-feedback-test-/); rmSync(folder, { recursive: true, force: true }); });
  const args = { appId: c.appId, installationId: c.installationId, requestId: 'submission', body: 'Нужна помощь', attachments: [] };
  const first = f.exec('one', 'submit', args); f.restart();
  const repeat = f.exec('one', 'submit', args);
  assert.equal(repeat.replayed, true); assert.deepEqual(repeat.receipt, first.receipt);
  assert.equal(f.exec('one', 'list', { appId: c.appId, installationId: c.installationId }).tickets.length, 1);
  fails(() => f.exec('one', 'submit', { ...args, body: 'Другое намерение' }), 'feedback_request_conflict');
  f.active.delete('one'); fails(() => f.exec('one', 'submit', args), 'feedback_authentication_required');
});

test('two reporters and two apps cannot read each other through list, cursor, ticket or installation', t => {
  const f = fixture(t), c = f.context();
  const one = f.exec('one', 'submit', { appId: c.appId, installationId: c.installationId, requestId: 'one', body: 'Частная проблема', attachments: [] });
  f.exec('two', 'submit', { appId: c.appId, installationId: c.installationId, requestId: 'two', body: 'Другая проблема', attachments: [] });
  assert.equal(f.exec('one', 'list', { appId: c.appId, installationId: c.installationId }).tickets.length, 1);
  assert.equal(f.exec('owner', 'list', { appId: c.appId, installationId: c.installationId }).tickets.length, 2);
  fails(() => f.exec('two', 'get', { appId: c.appId, installationId: c.installationId, ticketId: one.receipt.ticketId }), 'feedback_ticket_unavailable');
  fails(() => f.exec('one', 'list', { appId: 'app_else', installationId: c.installationId }), 'feedback_installation_mismatch');
  const page = f.exec('owner', 'list', { appId: c.appId, installationId: c.installationId, limit: 1 });
  fails(() => f.exec('one', 'list', { appId: c.appId, installationId: c.installationId, cursor: page.nextCursor }), 'feedback_invalid_cursor');
  f.access.delete('one'); fails(() => f.exec('one', 'get', { appId: c.appId, installationId: c.installationId, ticketId: one.receipt.ticketId }), 'apps_access_denied');
});

test('support reply is revision guarded and does not mark a problem resolved; reporter accepts separately', t => {
  const f = fixture(t), c = f.context();
  const sent = f.exec('one', 'submit', { appId: c.appId, installationId: c.installationId, requestId: 'submit', body: 'Как открыть?', attachments: [] });
  const reply = { appId: c.appId, installationId: c.installationId, ticketId: sent.receipt.ticketId, requestId: 'reply', expectedRevision: 1, body: 'Вот шаги' };
  const answer = f.exec('owner', 'reply', reply);
  assert.equal(answer.ticket.status, 'in_progress'); assert.equal(answer.ticket.messages[0].kind, 'support');
  assert.equal(f.exec('owner', 'reply', reply).replayed, true);
  fails(() => f.exec('owner', 'reply', { ...reply, requestId: 'stale' }), 'feedback_revision_conflict');
  fails(() => f.exec('one', 'status', { ...reply, body: undefined, status: 'ready_to_check' }), 'feedback_invalid_arguments');
  const base = { appId: c.appId, installationId: c.installationId, ticketId: sent.receipt.ticketId };
  const ready = f.exec('owner', 'status', { ...base, requestId: 'ready', expectedRevision: 2, status: 'ready_to_check' });
  fails(() => f.exec('owner', 'accept', { ...base, requestId: 'accept_owner', expectedRevision: ready.ticket.revision }), 'feedback_acceptance_required');
  const accepted = f.exec('one', 'accept', { ...base, requestId: 'accept', expectedRevision: ready.ticket.revision });
  assert.equal(accepted.ticket.status, 'resolved');
});

test('real provider commits exact generation receipts; copied proof changes do not match', t => {
  const f = fixture(t);
  const scope = { registryId: 'fixture', tenantId: 'owner', appId: 'app_fixture', environmentId: 'test' };
  const request = { scope, source: { id: 'apps:app_fixture/target', revision: 1, digest: 'a'.repeat(64) },
    profile: DEFAULT_FEEDBACK_PROFILE, provisioningKey: contractDigest({ scope, providerId: DEFAULT_FEEDBACK_PROFILE.feedback.provider.id }),
    intentDigest: 'b'.repeat(64), authorityDigest: 'c'.repeat(64), generation: 1 };
  assert.equal(f.service().provider.inspectInstallation(request), null);
  const proof = f.service().provider.ensureInstallation(request);
  assert.deepEqual(f.service().provider.inspectInstallation(request), proof);
  assert.deepEqual(f.service().provider.ensureInstallation(request), proof);
  assert.equal(f.service().provider.inspectInstallation({ ...request, generation: 2 }), null);
  fails(() => f.service().provider.ensureInstallation({ ...request, provisioningKey: 'f'.repeat(64) }), 'feedback_provisioning_mismatch');
});

test('source authority callbacks cannot be skipped, retained, repeated or return a foreign app/account context', t => {
  const actor = { accountId: 'one', deviceId: 'device_one' };
  const context = { appId: 'app_fixture', accountId: 'one', ownerId: 'owner', canManage: false, title: 'Fixture', entry: null };
  let retained, mode = 'skip';
  const service = createFeedbackService({ databasePath: ':memory:', registryId: 'fixture', environmentId: 'test', actorActive: () => true,
    withAppAuthority(_request, callback) {
      retained = callback;
      if (mode === 'skip') return { readiness: 'forged' };
      if (mode === 'double') { callback(context); return callback(context); }
      if (mode === 'foreign') return callback({ ...context, accountId: 'elsewhere' });
      return callback(context);
    } });
  t.after(() => service.close());
  const call = () => service.execute({ op: 'apps.feedback.context', actor, args: { appId: context.appId } });
  fails(call, 'feedback_authority_fence_invalid');
  fails(() => retained(context), 'feedback_authority_fence_invalid');
  mode = 'foreign'; fails(call, 'feedback_authority_context_invalid');
  mode = 'double'; fails(call, 'feedback_authority_fence_invalid');
  mode = 'normal'; assert.equal(call().readiness, 'ready');
  fails(() => retained(context), 'feedback_authority_fence_invalid');
});

test('a host cannot change the verified reporter by mutating its actor object inside the fence', t => {
  let original;
  const service = createFeedbackService({ databasePath: ':memory:', registryId: 'fixture', environmentId: 'test', actorActive: () => true,
    withAppAuthority({ actor, appId }, callback) {
      assert(Object.isFrozen(actor));
      if (original) original.accountId = 'other';
      return callback({ appId, accountId: actor.accountId, ownerId: 'owner', canManage: actor.accountId === 'owner', title: 'Fixture', entry: null });
    } });
  t.after(() => service.close());
  const installationId = service.execute({ op: 'apps.feedback.context', actor: { accountId: 'one', deviceId: 'device_one' }, args: { appId: 'app_fixture' } }).context.installationId;
  original = { accountId: 'one', deviceId: 'device_one' };
  const sent = service.execute({ op: 'apps.feedback.submit', actor: original, args: { appId: 'app_fixture', installationId, requestId: 'frozen-reporter', body: 'Synthetic body', attachments: [] } });
  original = null;
  assert.equal(service.execute({ op: 'apps.feedback.list', actor: { accountId: 'one', deviceId: 'device_one' }, args: { appId: 'app_fixture', installationId } }).tickets[0].id, sent.receipt.ticketId);
  assert.equal(service.execute({ op: 'apps.feedback.list', actor: { accountId: 'other', deviceId: 'device_other' }, args: { appId: 'app_fixture', installationId } }).tickets.length, 0);
});

test('a full pilot store holds new writes without losing readable tickets or exact receipts', t => {
  const folder = mkdtempSync(join(tmpdir(), 'soty-feedback-test-'));
  const f = fixture(t, join(folder, 'feedback.sqlite'), { maxDatabaseBytes: 256 * 1024 });
  t.after(() => { assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-feedback-test-/); rmSync(folder, { recursive: true, force: true }); });
  const c = f.context(), base = { appId: c.appId, installationId: c.installationId }, accepted = [];
  let refused = false;
  for (let n = 0; n < 128; n++) {
    const args = { ...base, requestId: 'quota-' + n, body: 'x'.repeat(8000), attachments: [] };
    try { accepted.push({ args, receipt: f.exec('one', 'submit', args).receipt }); }
    catch (error) { assert.equal(error.code, 'feedback_storage_capacity'); assert.equal(error.status, 503); refused = true; break; }
  }
  assert(refused && accepted.length > 0);
  f.restart();
  let count = 0, cursor = null;
  do { const page = f.exec('one', 'list', { ...base, ...(cursor ? { cursor } : {}) }); count += page.tickets.length; cursor = page.nextCursor; } while (cursor);
  assert.equal(count, accepted.length);
  const replay = f.exec('one', 'submit', accepted[0].args);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted[0].receipt);
  assert.equal(f.exec('one', 'get', { ...base, ticketId: accepted[0].receipt.ticketId }).ticket.body.length, 8000);
});
