import test from 'node:test';
import assert from 'node:assert/strict';
import { composeConnectExtensions } from '../connect-extensions.js';
import { createConnectService, MAX_ARGS_BYTES } from '../../modules/connect/server/index.mjs';
import { signedFixture } from '../../modules/connect/test/support/direct-signed-fixture.mjs';
import { createTransitAccess } from '../transit-access.js';
import { createAppsService } from '../../modules/apps/server/index.mjs';

const sync = (op, extra = {}) => ({ operations: new Set([op]), execute() { return { value: true }; }, ...extra });
const asyncService = (op, extra = {}) => ({ operations: new Set([op]), async executeAsync() { return { value: true }; }, ...extra });
const connectOptions = extensions => ({ databasePath: ':memory:', projectId: 'composition-test', allowedOrigins: ['https://composition.test'], extensions });

test('composition keeps Core32 limit and routes both original async services with their original actor', async t => {
  let f;
  const apps = createAppsService({ databasePath: ':memory:', shellOrigins: ['https://direct-contract.test'],
    actorActive(actor) { f.service.withActorAuthorityFence(actor, () => true); return true; } });
  t.after(() => apps.close());
  const transit = createTransitAccess({ configuration: null, getConnect: () => f.service });
  t.after(() => transit.close());
  const filler = Array.from({ length: 31 }, (_, i) => sync(`fixture.operation${i}`));
  assert.throws(() => createConnectService(connectOptions([...filler, apps.endpointRegistrationExtension, transit])), { code: 'invalid_extensions' });
  const grouped = composeConnectExtensions([apps.endpointRegistrationExtension, transit]);
  assert.equal(grouped.maxResultBytes, undefined, 'the original Core default is preserved for endpoints');
  assert.equal(grouped.challengeStorage, undefined, 'both branches retain persistent challenges');
  assert.equal(typeof grouped.executeAsync, 'function'); assert.equal(grouped.execute, undefined);
  f = signedFixture(t, [...filler, grouped]);
  const actor = await f.person('Composition owner');
  const status = await f.call(actor, 'transit.access.status', { expectedAccountId: actor.accountId });
  assert.equal(status.enabled, false); assert.equal(status.appId, null);
  const endpoint = await f.raw(actor, 'apps.registerEndpoint', {});
  assert.equal(endpoint.ok, false); assert.equal(endpoint.error.code, 'apps_endpoint_connector_upgrade_required',
    'the real endpoint service, rather than a permissive fixture, applies its disabled-target policy');
});

test('original this/request identity, operation snapshot and challenge policy survive composition', async () => {
  const actor = Object.freeze({ accountId: 'own' }), request = { op: 'first.read', actor, args: {} };
  const first = asyncService('first.read', { maxResultBytes: 2048, challengeStorage: 'memory',
    async executeAsync(value) { assert.equal(this, first); assert.equal(value, request); assert.equal(value.actor, actor); return { first: true }; } });
  const second = asyncService('second.read', { maxResultBytes: 4096, challengeStorage: 'memory' });
  const combined = composeConnectExtensions([first, second]);
  first.operations.clear(); first.executeAsync = () => { throw Error('late replacement'); }; first.maxResultBytes = 8 * 1024 * 1024;
  assert.equal(combined.challengeStorage, 'memory'); assert.equal(combined.maxResultBytes, 4096);
  assert.deepEqual(await combined.executeAsync(request), { first: true });
  await assert.rejects(combined.executeAsync({ op: 'late.read', actor, args: {} }), { code: 'invalid_extension_operation' });
});

test('undefined result limit retains responses above64KiB, with the original Core ceiling', async t => {
  const original = asyncService('wide.read', { async executeAsync() { return { text: 'x'.repeat(96 * 1024) }; } });
  const limited = asyncService('small.read', { maxResultBytes: 1024 });
  const combined = composeConnectExtensions([original, limited]);
  assert.equal(Object.hasOwn(combined, 'maxResultBytes'), false);
  const direct = signedFixture(t, [original]), merged = signedFixture(t, [combined]);
  const a = await direct.person('Before'), b = await merged.person('After');
  assert.equal((await direct.call(a, 'wide.read')).text.length, 96 * 1024);
  assert.equal((await merged.call(b, 'wide.read')).text.length, 96 * 1024);
  const oversized = composeConnectExtensions([asyncService('wide.read', { async executeAsync() { return { text: 'x'.repeat(MAX_ARGS_BYTES) }; } }), limited]);
  await assert.rejects(oversized.executeAsync({ op: 'wide.read' }), { code: 'extension_result_too_large' });
  const biggerSibling = composeConnectExtensions([
    asyncService('wide.read', { async executeAsync() { return { text: 'x'.repeat(MAX_ARGS_BYTES) }; } }),
    asyncService('larger.read', { maxResultBytes: 4 * 1024 * 1024 }),
  ]);
  assert.equal(biggerSibling.maxResultBytes, 4 * 1024 * 1024);
  await assert.rejects(biggerSibling.executeAsync({ op: 'wide.read' }), { code: 'extension_result_too_large' },
    'a wider sibling cannot increase the original default for this operation');
});

test('neither a Transit limit nor an explicit endpoint limit is widened to the merged ceiling', async () => {
  const endpoint = asyncService('apps.registerEndpoint', { maxResultBytes: 2048,
    async executeAsync() { return { text: 'e'.repeat(2048) }; } });
  const transit = asyncService('transit.access.profile', { maxResultBytes: 256000,
    async executeAsync() { return { text: 't'.repeat(256000) }; } });
  const merged = composeConnectExtensions([endpoint, transit]);
  assert.equal(merged.maxResultBytes, 256000);
  for (const op of ['apps.registerEndpoint', 'transit.access.profile']) {
    await assert.rejects(merged.executeAsync({ op }), { code: 'extension_result_too_large' });
  }
  const defaultMerged = composeConnectExtensions([asyncService('unlimited.read'), transit]);
  assert.equal(defaultMerged.maxResultBytes, undefined);
  await assert.rejects(defaultMerged.executeAsync({ op: 'transit.access.profile' }), { code: 'extension_result_too_large' });
});

test('invalid, mixed-lane, mixed-policy and duplicate branches fail before registration', () => {
  for (const services of [[], [sync('first.read'), asyncService('second.read')],
    [sync('first.read'), sync('first.read')], [sync('first.read'), sync('second.read', { challengeStorage: 'memory' })],
    [sync('first.read', { maxResultBytes: 1 })], [sync('first.read', { challengeStorage: 'future' })],
    [sync('first.read', { operations: new Set() })]]) assert.throws(() => composeConnectExtensions(services));
});

test('sync lane and invalid result envelopes retain original Core semantics', () => {
  const first = sync('first.read'), second = sync('second.read', { maxResultBytes: 1024 });
  const merged = composeConnectExtensions([first, second]);
  assert.equal(merged.executeAsync, undefined); assert.deepEqual(merged.execute({ op: 'first.read' }), { value: true });
  for (const value of [null, [], { ok: true }, { error: 'secret' }, Promise.resolve({ value: true })]) {
    const invalid = composeConnectExtensions([sync('first.read', { execute() { return value; } })]);
    assert.throws(() => invalid.execute({ op: 'first.read' }), { code: 'invalid_extension_result' });
  }
});

test('real Root combined slot serves signed Transit status and a successful connector endpoint registration', { timeout: 45000 }, async t => {
  const { rootSelfIpFixture } = await import('./support/self-ip-root-fixture.mjs');
  const f = await rootSelfIpFixture(t);
  const status = await f.ownerCall('transit.access.status');
  assert.equal(status.ok, true, status.error?.code);
  assert.equal(status.enabled, false, 'the synthetic Root has no issuer configuration');
  const registered = await f.ownerCall('apps.registerEndpoint', await f.args());
  assert.equal(registered.ok, true, registered.error?.code);
  assert.match(registered.app.id, /^app-[a-f0-9]{32}$/u);
  assert.equal(registered.registrationReceipt.committed, true);
  assert.equal(registered.app.name, 'Self-IP marker app');
});
