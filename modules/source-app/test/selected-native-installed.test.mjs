import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../server/standard-profile.mjs';
import { captureExternalApplications } from '../../../server/external-applications.js';
import { selectedNativeApplications } from './support/selected-native-contracts.mjs';



test('exact selected Native bridge uses real installed HTTP/WS + SQL effects/proofs, windows, reload and revoke', { timeout: 180000 }, async t => {
  let entries;
  const f = await createOrdinaryInstalledFixture(t, { sourceProfile: STANDARD_SELECTED_SOURCE_V3, libraryNativeOwner: true,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    externalApplications(realms) { const rows = realms.flatMap(realm => selectedNativeApplications(realm.profile)); entries = captureExternalApplications(rows); return rows; } });
  const caps = f.root().locals.capabilitiesService, [board, library] = f.realms;
  // Separate explicit finite read/write principals keep this multi-scenario
  // fixture within the unchanged per-principal10/min admission rate.
  const bindings = {};
  for (const kind of ['list', 'create']) {
    const approved = entries.filter(row => row.catalog.capabilityId.endsWith(':items.' + kind));
    const principal = (await f.reader.client.extension('access.principals.create', { expectedAccountId: f.reader.account.accountId, label: 'Synthetic selected Native ' + kind })).principal;
    const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId, principalId: principal.id,
      capabilities: approved.map(row => ({ capabilityId: row.contract.capabilityId, version: 1 })), resources: [...new Set(approved.flatMap(row => row.catalog.resources))],
      effects: kind === 'create' ? ['create'] : [], recipients: [...new Set(approved.flatMap(row => row.catalog.recipients))], expiresAt: Date.now() + 600000,
      budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.reader.client.extension('access.credentials.issue', { expectedAccountId: f.reader.account.accountId, grantId: grant.id, audience: f.origin });
    bindings[kind] = { grant, credential };
  }
  const actor = (kind = 'create') => caps.authenticateCredential({ token: bindings[kind].credential.token, audience: f.origin });
  const reference = (realm, op) => entries.find(row => row.appId === realm.appId && row.catalog.capabilityId.endsWith(':items.' + op)).contract;
  let sequence = 0;
  const list = (realm, handle) => caps.external.query({ actor: actor('list'), reference: reference(realm, 'list'),
    idempotencyKey: 'selected_items_read_' + (++sequence), input: { handle, input: { operation: 'items.list' } } });
  const create = (realm, handle, key, title) => caps.external.invoke({ actor: actor(), reference: reference(realm, 'create'),
    idempotencyKey: key, input: { handle, input: { operation: 'items.create', title } } });
  const count = realm => realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n;
  const denied = async action => { try { const result = await action(); assert.notEqual(result.invocation.status, 'succeeded'); }
    catch (error) { assert.equal(typeof error.code, 'string'); } };
  async function ready(realm) { await realm.launch(); await f.login(realm);
    const result = await f.wire(realm.embedded + '/api/embed/session-continue', { body: { requestId: 'selected-native-ready-' + (++sequence) } });
    assert.equal(result.status, 200); assert.equal(result.value.ready, true); return realm.lastLaunch.scopedCloseHandle; }
  const first = await ready(board), other = await ready(library);
  await denied(() => create(library, first, 'selected_foreign_window_001', 'Denied wrong app'));
  assert.equal(count(board), 0); assert.equal(count(library), 0);
  const created = await create(board, first, 'selected_real_create_001', 'Synthetic selected Native item');
  assert.equal(created.invocation.status, 'succeeded'); assert.equal(created.invocation.effectState, 'committed');
  const item = created.invocation.effects[0].resourceId;
  assert.equal((await list(board, first)).result.items[0].id, item); assert.deepEqual((await list(library, other)).result.items, []);
  assert.equal(count(board), 1); assert.equal(count(library), 0);
  await caps.external.reconcile({ reference: reference(board, 'create'), invocationId: created.invocation.invocationId });
  assert.equal(caps.external.get({ actor: actor(), invocationId: created.invocation.invocationId }).invocation.status, 'succeeded'); assert.equal(count(board), 1);
  const second = await ready(board); assert.notEqual(second, first);
  await assert.rejects(caps.external.reconcile({ reference: reference(board, 'create'), invocationId: created.invocation.invocationId,
    expectedInput: { handle: second, input: { operation: 'items.create', title: 'Synthetic selected Native item' } } }), { code: 'external_proof_mismatch' });
  await assert.rejects(caps.external.reconcile({ reference: reference(library, 'create'), invocationId: created.invocation.invocationId,
    expectedInput: { handle: first, input: { operation: 'items.create', title: 'Synthetic selected Native item' } } }), { code: 'external_proof_mismatch' });
  assert.equal((await list(board, first)).result.items[0].id, item); assert.equal((await list(board, second)).result.items[0].id, item);
  await f.reader.client.extension('apps.scoped.close', { appId: board.appId, handle: first });
  await denied(() => create(board, first, 'selected_closed_window_001', 'Denied closed first window'));
  assert.equal((await list(board, second)).result.items.length, 1); assert.equal(count(board), 1);
  await board.restart(); assert.equal((await list(board, second)).result.items[0].id, item);
  board.dropPath = '/api/embed/invoke';
  const lost = await create(board, second, 'selected_lost_ack_001', 'Synthetic lost ACK item');
  assert.equal(lost.invocation.status, 'succeeded'); assert.equal(lost.invocation.effectState, 'committed'); assert.equal(count(board), 2);
  await caps.external.reconcile({ reference: reference(board, 'create'), invocationId: lost.invocation.invocationId }); assert.equal(count(board), 2);
  let entered, release; const barrier = new Promise(done => { entered = done; });
  board.instance.server.prependListener('request', (req, res) => { if (req.url !== '/api/embed/invoke') return;
    const end = res.end.bind(res); res.end = (...args) => { release = () => end(...args); entered(); return res; }; });
  t.after(() => release?.());
  const pending = create(board, second, 'selected_late_ack_001', 'Synthetic late ACK item'), failed = denied(() => pending);
  await Promise.race([barrier, pending.then(() => { throw Error('expected actual Native hold'); })]);
  assert.equal(count(board), 3);
  await f.reader.client.extension('apps.scoped.close', { appId: board.appId, handle: second }); release(); await failed;
  await caps.external.reconcile({ reference: reference(board, 'create'), invocationId: lost.invocation.invocationId });
  assert.equal(count(board), 3); assert.deepEqual((await list(library, other)).result.items, []);
  library.instance.store.revokeMembership('selected', 'native-owner');
  await denied(() => create(library, other, 'selected_native_revoke_001', 'Denied revoked Native member')); assert.equal(count(library), 0);
  await f.reader.client.extension('access.grants.revoke', { expectedAccountId: f.reader.account.accountId, grantId: bindings.create.grant.id });
  await denied(() => create(board, second, 'selected_root_revoke_001', 'Denied Root grant')); assert.equal(count(board), 3);
});
