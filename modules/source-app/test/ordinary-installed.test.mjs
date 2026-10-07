import test from 'node:test';
import assert from 'node:assert/strict';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';

test('standard2: actual signed Root Apps + installed HTTP/WS connector connects two Native realms with real embed OIDC, source receipts and current ACL', { timeout: 120000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t);
  for (const realm of f.realms) {
    await realm.launch(); await f.login(realm);
    const context = await f.wire(realm.embedded + '/api/embed/context'); assert.equal(context.status, 200); assert.equal(context.value.ready, true);
    const args = { requestId: 'installed-create-' + realm.realmId, input: { operation: 'items.create', title: 'Synthetic retained ' + realm.realmId } };
    const created = await f.wire(realm.embedded + '/api/embed/invoke', { body: args }); assert.equal(created.status, 200);
    await realm.restart(); const replayed = await f.wire(realm.embedded + '/api/embed/invoke', { body: args }); assert.equal(replayed.status, 200);
    assert.deepEqual(replayed.value.data.receipt, created.value.data.receipt); assert.equal(replayed.value.data.replayed, true);
    assert.equal(replayed.value.data.outcome, 'committed'); assert.equal(replayed.value.data.inputDigest, created.value.data.inputDigest);
    const read = await f.wire(realm.embedded + '/api/embed/query', { body: { requestId: 'installed-read-' + realm.realmId, input: { operation: 'items.list' } } });
    assert.equal(read.status, 200); assert.equal(read.value.data.items.length, 1);
    const feedback = await f.wire(realm.embedded + '/api/embed/feedback/context'); assert.equal(feedback.status, 200); assert.equal(feedback.value.data.canSubmit, true);
    const sent = await f.wire(realm.embedded + '/api/embed/feedback', { body: { requestId: 'installed-feedback-' + realm.realmId, body: 'Synthetic private issue', attachments: [] } });
    assert.equal(sent.status, 200); assert.equal(sent.value.data.ticket.canManage, false);
    const managed = await f.wire(realm.embedded + '/api/embed/feedback/status', { body: { requestId: 'installed-denied-' + realm.realmId,
      ticketId: sent.value.data.ticket.id, expectedRevision: sent.value.data.ticket.revision, status: 'ready_to_check' } }); assert.equal(managed.status, 403);
    assert.equal((await f.wire(realm.embedded + '/api/embed/private-admin')).status, 403);
    realm.instance.store.revokeMembership('selected', 'native-participant'); assert.equal((await f.wire(realm.embedded + '/api/embed/context')).status, 403);
    assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 1);
  }
});

test('installed channel: callback COMMIT/lost ACK consumes Root map; explicit exact-H receipt recovery after Source restart never repeats exchange/link/grant; foreign/revoke/Root restart deny', { timeout: 120000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t), [realm] = f.realms;
  const launch = await realm.launch(), flow = await f.authorize(realm), originalH = flow.callbackUrl.searchParams.get('state');
  realm.dropPath = flow.callbackUrl.pathname + flow.callbackUrl.search;
  const lost = await f.wire(flow.callbackUrl); assert.ok([502, 503].includes(lost.status), 'wire loss is unknown, never reported as success');
  const counts = () => Object.fromEntries(['source_interactions', 'source_sessions', 'native_links', 'native_consents'].map(name =>
    [name, realm.instance.store.db.prepare('SELECT count(*) AS n FROM ' + name).get().n]));
  assert.deepEqual(counts(), { source_interactions: 1, source_sessions: 1, native_links: 1, native_consents: 1 });
  assert.equal((await f.wire(flow.callbackUrl)).status, 403, 'one-use Root auth map already consumed');
  await assert.rejects(f.foreign.client.extension('apps.scoped.close', { appId: realm.appId, handle: launch.scopedCloseHandle }), error => error.code === 'app_scoped_close_unavailable');
  await realm.restart();
  const recovery = await f.wire(realm.embedded + '/api/embed/login', { body: {} }); assert.equal(recovery.status, 200);
  const url = new URL(recovery.value.nativeUrl); assert.equal(url.searchParams.get('intent') === originalH, true);
  const native = await f.wire(url); assert.equal(native.status, 303); assert.equal(native.location.searchParams.has('code'), false);
  await f.complete(native.location); assert.deepEqual(counts(), { source_interactions: 1, source_sessions: 1, native_links: 1, native_consents: 1 });
  assert.equal((await f.wire(realm.embedded + '/api/embed/context')).status, 200);
  realm.instance.store.revokeNativeSession(realm.nativeToken);
  assert.equal((await f.wire(realm.embedded + '/api/embed/login', { body: {} })).status, 403, 'Native revoke denies receipt recovery');
  await f.restartRoot();
  assert.equal((await f.wire(realm.embedded + '/api/embed/login', { body: {} })).status, 403, 'cold Root RAM map/slot cannot be recreated from Source receipt');
});
