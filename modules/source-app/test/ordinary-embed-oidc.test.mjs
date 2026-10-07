import test from 'node:test';
import assert from 'node:assert/strict';
import { createOrdinaryHttpFixture } from './support/ordinary-http.mjs';
import { STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2 } from '../server/standard-profile.mjs';

test('standard2 uses existing embed callback/Hstate with actual maintained RP49, two durable Native realms; native proof precedes OIDC and survives restart', { timeout: 20000 }, async t => {
  assert.equal(STANDARD_SELECTED_SOURCE.digest, 'b646578a1cbace022bdcc44147e0ad56b4e9d6239250726c4bb2decc4e4de721');
  assert.notEqual(STANDARD_SELECTED_SOURCE_V2.digest, STANDARD_SELECTED_SOURCE.digest);
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true });
  for (const realm of f.realms) {
    const native = await realm.beginNative(), authorized = await realm.authorizeNative(native), url = new URL(authorized.location);
    assert.equal(url.searchParams.get('state') === native.form.intent, true);
    assert.equal(url.searchParams.get('redirect_uri'), realm.embedOrigin + '/api/embed/callback');
    assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_login_proofs').get().n, 1);
    assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n, 0);
    assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM source_sessions').get().n, 0);
    realm.restart(); const callback = (await realm.completeOidc(authorized)).callback; await realm.finishNative(callback);
    assert.equal((await realm.request('/api/embed/session-status')).value.ready, true);
    assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n, 1);
    assert.equal((await realm.request('/api/embed/session-continue', { method: 'POST', data: { requestId: 'embed-basic-proof-0001' } })).value.renewable, false);
  }
});

test('Native revoke after capture/before code exchange rejects callback without new link/session; JSON/mixed proof cannot supply authority', { timeout: 15000 }, async t => {
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true }), [realm] = f.realms;
  const authorized = await realm.authorizeNative(await realm.beginNative()), callback = (await realm.completeOidc(authorized)).callback;
  realm.store.revokeMembership('selected', 'native-participant');
  const denied = await realm.request(callback.pathname + callback.search); assert.equal(denied.status, 403);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n, 0);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM source_sessions').get().n, 0);
  const marker = realm.store.db.prepare('SELECT id_hash FROM native_login_proofs').get().id_hash;
  assert.equal((await realm.request('/api/embed/login', { method: 'POST', data: { nativeProof: marker, actor: 'owner' } })).status, 400);
});

test('Source callback COMMIT wire loss recovers only exact completion receipt via original private H; no code exchange/link/grant replay and foreign/closed/Native revoked scope denies', { timeout: 15000 }, async t => {
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true }), [realm] = f.realms;
  const native = await realm.beginNative(), callback = (await realm.completeOidc(await realm.authorizeNative(native))).callback;
  realm.dropPath = callback.pathname + callback.search;
  await assert.rejects(realm.request(callback.pathname + callback.search));
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n, 1);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM source_sessions').get().n, 1); realm.restart();
  const recovery = await realm.request('/api/embed/login', { method: 'POST', data: {} }); assert.equal(recovery.status, 200);
  const url = new URL(recovery.value.nativeUrl); assert.equal(url.searchParams.get('intent') === native.form.intent, true);
  const readback = await realm.request(url.pathname + url.search, { native: true }); assert.equal(readback.status, 303);
  const complete = new URL(readback.location); assert.equal(complete.searchParams.has('code'), false); await realm.finishNative(complete);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n, 1);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM source_sessions').get().n, 1);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_consents').get().n, 1);
  const reference = realm.context.reference; realm.context.reference = { ...reference, id: 'x'.repeat(43) };
  assert.equal((await realm.request('/api/embed/login', { method: 'POST', data: {} })).status, 403); realm.context.reference = reference;
  realm.store.revokeNativeSession(realm.nativeToken); assert.equal((await realm.request('/api/embed/login', { method: 'POST', data: {} })).status, 403);
});

test('Standard2 Native form alone permits exact reviewed issuer redirect; null/missing/foreign Origin and same-host wrong-port cannot use valid CSRF/body, unknown query cannot extend CSP', { timeout: 15000 }, async t => {
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true }), [realm] = f.realms, intent = await realm.beginNative();
  const rootOrigin = new URL(realm.profile.issuer).origin;
  assert.equal(intent.page.policy.referrer, 'origin');
  assert.equal(intent.page.policy.csp, "default-src 'none'; style-src 'self'; form-action 'self' " + rootOrigin + "; frame-ancestors 'none'");
  const request = headers => realm.request('/soty/authorize', { native: true, method: 'POST', form: intent.form, headers });
  for (const headers of [ { origin: 'null' }, { origin: 'null', referer: 'https://foreign.invalid/' },
    { origin: 'null', referer: realm.nativeOrigin + '/' }, { origin: null }, { origin: 'https://foreign.invalid' },
    { origin: realm.nativeOrigin === 'http://localhost:65534' ? 'http://localhost:65533' : 'http://localhost:65534' } ]) {
    const denied = await request(headers); assert.equal(denied.status, 403); assert.match(denied.policy.csp, /form-action 'self';/u);
    assert.equal(denied.policy.csp.includes(rootOrigin), false); assert.equal(denied.text.includes('source_app_'), false);
  }
  const extra = await realm.request('/soty/connect?intent=' + intent.form.intent + '&issuer=https%3A%2F%2Fforeign.invalid', { native: true });
  assert.equal(extra.status, 400); assert.equal(extra.policy.csp.includes(rootOrigin), false);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_login_proofs').get().n, 0);
  const authorized = await request({ origin: realm.nativeOrigin }); assert.equal(authorized.status, 303);
  assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_login_proofs').get().n, 1);
  const old = await createOrdinaryHttpFixture(t), legacy = await old.realms[0].beginNative();
  assert.equal(legacy.page.policy.referrer, 'no-referrer'); assert.match(legacy.page.policy.csp, /form-action 'self';/u);
});
