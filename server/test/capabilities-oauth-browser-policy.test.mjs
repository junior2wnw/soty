import test from 'node:test';
import assert from 'node:assert/strict';
import { oauthNativeFixture, nativeIdentity, digest } from './support/oauth-native-http.mjs';

// Real signed Connect decisions, Caps3/Notes2 and the production Provider.
// Headers and manual POSTs model the browser policy boundary; this is not a
// browser automation test. Codes, cookies, XSRF and tokens stay in memory.
const CAPS_TABLES = ['cap_oauth_connections', 'cap_oauth_interactions', 'cap_oauth_artifacts',
  'cap_oauth_credentials', 'cap_credentials', 'cap_grants', 'cap_invocations'];

function durableState(f) {
  const caps = f.sql(f.capsFile, db => CAPS_TABLES.map(table => [table,
    db.prepare(`SELECT * FROM ${table} ORDER BY ${table === 'cap_oauth_artifacts' ? 'model,id_hash' : '1'}`).all()]));
  const notes = f.sql(f.notesFile, db => [
    db.prepare('SELECT count(*) AS n FROM notes').get().n,
    db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n,
  ]);
  // Compare exact persisted rows without putting ciphertext, IDs or payloads
  // into an assertion diagnostic if this boundary regresses.
  return digest(JSON.stringify({ caps, notes }));
}

const modelCount = (f, model) => f.sql(f.capsFile, db => db.prepare(
  'SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model=?').get(model).n);

function noNotes(f) {
  const counts = f.sql(f.notesFile, db => [db.prepare('SELECT count(*) AS n FROM notes').get().n,
    db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n]);
  assert.deepEqual(counts, [0, 0], 'authorization alone creates no Note or native proof');
}

function defaultPolicy(response, purpose) {
  assert.equal(response.headers.get('referrer-policy'), 'no-referrer', purpose);
  assert.equal(response.headers.get('cache-control'), 'no-store');
}

function documentPolicy(response, callback, { providerScript = false } = {}) {
  assert.equal(response.status, 200);
  assert.ok(response.headers.get('content-type')?.startsWith('text/html'), 'actual HTML document');
  assert.equal(response.headers.get('cache-control'), 'no-store');
  const directives = Object.fromEntries((response.headers.get('content-security-policy') ?? '').split(';')
    .filter(Boolean).map(item => { const [name, ...values] = item.trim().split(/\s+/u); return [name, values]; }));
  assert.deepEqual(directives['form-action'], ["'self'", callback], 'only the checked native callback expands form-action');
  if (providerScript) {
    assert.ok(directives['script-src']?.some(value => /^'sha256-[A-Za-z0-9+/]+=*'$/u.test(value)),
      'the maintained Provider autoform keeps its actual inline script hash');
    assert.equal(directives['script-src'].includes("'unsafe-inline'"), false);
  }
  return response.headers.get('referrer-policy');
}

function callbackFlow(f, flow, response) {
  assert.ok([302, 303].includes(response.status), 'actual Provider returns a native callback redirect');
  defaultPolicy(response, 'callback redirect does not relax referrer policy');
  const callback = response.location;
  assert.ok(callback?.origin + callback?.pathname === flow.redirectUri, 'exact registered callback is not fetched');
  assert.ok(callback.searchParams.get('state') === flow.state && callback.searchParams.get('iss') === f.issuer,
    'callback retains the original state and issuer');
  assert.ok(callback.searchParams.has('code') && !callback.searchParams.has('error'), 'approved flow produced a real code');
  return { ...flow, code: callback.searchParams.get('code'), error: null };
}

function autoform(f, response) {
  const action = /<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
  assert.ok(action === f.origin + '/oauth/session/end/confirm', 'real Provider account-switch form destination');
  const fields = Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)]
    .map(match => [match[1], match[2]]));
  assert.deepEqual(Object.keys(fields).sort(), ['logout', 'xsrf']);
  assert.equal(fields.logout, 'yes');
  assert.ok(typeof fields.xsrf === 'string' && fields.xsrf.length > 10, 'use the actual session-bound XSRF without logging it');
  return { action, fields };
}

test('consent HTML preserves same-origin form Origin while null-origin signed completion has no durable effect', async t => {
  const f = await oauthNativeFixture(t), owner = nativeIdentity('Владелец политики браузера');
  const account = await f.bootstrap(owner), pending = await f.begin();
  const html = await pending.session.request(pending.pathname);
  const policy = documentPolicy(html, pending.redirectUri);
  const context = await pending.session.request(pending.pathname + '/context');
  assert.equal(context.status, 200); defaultPolicy(context, 'private context API keeps no-referrer');
  const approved = await f.decide(pending, owner, account.accountId);
  for (const model of ['Grant', 'AuthorizationCode', 'RefreshToken', 'AccessToken']) assert.equal(modelCount(f, model), 0);
  const before = durableState(f), fields = { expectedAccountId: account.accountId };

  // The literal header value is intentional. JavaScript null would omit the
  // header in this fixture and would not reproduce an opaque browser Origin.
  const denied = await approved.session.request(approved.pathname + '/complete', { fields, originHeader: 'null' });
  assert.equal(denied.status, 403); assert.equal(denied.location, null);
  defaultPolicy(denied, 'denied completion keeps no-referrer');
  assert.equal(durableState(f), before, 'null Origin changes no family, Grant, token, interaction or Note');
  noNotes(f);

  // Do not call f.complete: inspect the production redirect and manually
  // continue with the same valid signed decision, fields and browser cookies.
  const completed = await approved.session.request(approved.pathname + '/complete', { fields, originHeader: f.origin });
  assert.equal(completed.status, 303); defaultPolicy(completed, 'completion redirect keeps no-referrer');
  assert.ok(completed.location?.origin === f.origin, 'completion resumes on the same issuer');
  assert.equal(modelCount(f, 'Grant'), 1, 'only admitted completion binds a Provider Grant');
  const issued = callbackFlow(f, approved, await approved.session.request(completed.location));
  const exchanged = await f.exchange(issued); f.tokens(exchanged);
  defaultPolicy(exchanged, 'token API keeps no-referrer');
  assert.equal(modelCount(f, 'AccessToken'), 1); assert.equal(modelCount(f, 'RefreshToken'), 1);
  noNotes(f);

  // Keep the causal header assertion last: RED still exercises both refusal
  // and the valid same-origin continuation against the unmodified host.
  assert.equal(policy, 'same-origin', 'only the real consent HTML must preserve Origin on its same-origin form POST');
});

test('actual A-to-B resume autoform preserves Origin and null-origin valid XSRF cannot change Session or either family', async t => {
  const f = await oauthNativeFixture(t), alice = nativeIdentity('Алиса политики'), bob = nativeIdentity('Боб политики');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob), session = f.browser();
  const first = await f.connect(alice, a.accountId, { session });
  const second = await f.decide(await f.begin({ session }), bob, b.accountId);
  const completed = await session.request(second.pathname + '/complete', { fields: { expectedAccountId: b.accountId } });
  assert.equal(completed.status, 303); defaultPolicy(completed, 'account-switch completion redirect keeps no-referrer');
  assert.ok(completed.location?.origin === f.origin, 'same-issuer resume');
  const document = await session.request(completed.location);
  const policy = documentPolicy(document, second.redirectUri, { providerScript: true });
  const form = autoform(f, document);
  assert.ok(modelCount(f, 'Session') > 0, 'a real Provider Session exists before account confirmation');
  const before = durableState(f);

  const denied = await session.request(form.action, { fields: form.fields, originHeader: 'null' });
  assert.equal(denied.status, 403); assert.equal(denied.location, null);
  defaultPolicy(denied, 'denied confirmation keeps no-referrer');
  assert.equal(durableState(f), before, 'null Origin leaves exact Session, both families and all credentials unchanged');
  noNotes(f);

  const confirmed = await session.request(form.action, { fields: form.fields, originHeader: f.origin });
  assert.equal(confirmed.status, 303); defaultPolicy(confirmed, 'confirmation redirect keeps no-referrer');
  assert.ok(confirmed.location?.origin === f.origin
    && /^\/oauth\/authorize\/[A-Za-z0-9_-]{16,128}$/u.test(confirmed.location.pathname), 'same-issuer authorization resumes');
  const issued = callbackFlow(f, second, await session.request(confirmed.location));
  const secondTokens = f.tokens(await f.exchange(issued));
  assert.ok(secondTokens.access_token !== first.tokens.access_token, 'signed B consent issues a distinct bearer');
  f.tokens(await f.refresh(first.flow, first.tokens));
  const families = f.sql(f.capsFile, db => db.prepare('SELECT state FROM cap_oauth_connections ORDER BY id').all());
  assert.deepEqual(families.map(row => row.state), ['active', 'active'], 'valid account switch preserves both independent families');
  noNotes(f);

  assert.equal(policy, 'same-origin', 'the real Provider resume200 HTML must preserve Origin on its confirmation POST');
});

test('consumed signed consent reopens as a fixed safe HTML document while context remains JSON and durable state is unchanged', async t => {
  const f = await oauthNativeFixture(t), owner = nativeIdentity('Владелец завершённого подключения');
  const account = await f.bootstrap(owner);
  const flow = await f.complete(await f.decide(await f.begin(), owner, account.accountId));
  const issued = f.tokens(await f.exchange(flow));
  assert.equal(modelCount(f, 'AccessToken'), 1, 'the consent was actually completed and exchanged');
  const before = durableState(f);

  const document = await flow.session.request(flow.pathname);
  const context = await flow.session.request(flow.pathname + '/context');
  const reopened = await flow.session.request(flow.pathname);
  assert.ok(context.status >= 400 && context.status < 500, 'consumed context remains unavailable');
  assert.equal(document.status, context.status, 'document presentation does not turn refusal into a success');
  assert.equal(reopened.status, document.status);
  assert.equal(document.location, null); assert.equal(context.location, null); assert.equal(reopened.location, null);
  defaultPolicy(document, 'consumed consent HTML keeps no-referrer');
  defaultPolicy(context, 'consumed context JSON keeps no-referrer');
  defaultPolicy(reopened, 'repeated consumed HTML keeps no-referrer');
  assert.ok(context.headers.get('content-type')?.startsWith('application/json'), 'context is still the API surface');
  assert.deepEqual(Object.keys(context.body), ['error']);
  assert.ok(['invalid_request', 'interaction_expired'].includes(context.body.error), 'only a safe context error code');
  assert.equal(durableState(f), before, 'document/context rereads change no Session, family, credential, Invocation or Note');
  noNotes(f);
  for (const secret of [flow.pathname.split('/').at(-1), flow.code, flow.state, flow.context.browserNonce,
    flow.context.contextDigest, issued.access_token, issued.refresh_token]) {
    assert.ok(typeof secret === 'string' && secret.length > 0, 'real private flow values exist for the no-reflection check');
    assert.equal([document, context, reopened].some(response => response.text.includes(secret)), false,
      'neither fixed HTML nor JSON API reflects private flow values');
  }

  // Assert presentation after the authority/no-change checks so the initial
  // RED identifies raw JSON rather than substituting a fabricated expired UID.
  assert.ok(document.headers.get('content-type')?.startsWith('text/html'), 'a real consumed consent GET must return fixed HTML, not raw JSON');
  assert.equal(digest(document.text), digest(reopened.text), 'the error document is fixed across rereads');
  assert.ok(/<!doctype html>/iu.test(document.text) && /<main\b/u.test(document.text)
    && /<h1\b/u.test(document.text) && /<a href="\/">/u.test(document.text), 'standalone readable document retains a safe home link');
  assert.equal(/<(?:script|form|input)\b/iu.test(document.text), false, 'no executable or resubmittable consent state');
  const style = /<style>([\s\S]*?)<\/style>/u.exec(document.text)?.[1];
  assert.ok(typeof style === 'string', 'fixed inline style has a matching CSP hash');
  const expectedPolicy = `default-src 'none'; base-uri 'none'; object-src 'none'; script-src 'none'; style-src 'sha256-${Buffer.from(digest(style), 'hex').toString('base64')}'; frame-ancestors 'none'; form-action 'none'`;
  assert.equal(document.headers.get('content-security-policy'), expectedPolicy, 'fixed CSP permits only this stylesheet');
  assert.equal(reopened.headers.get('content-security-policy'), expectedPolicy);
});
