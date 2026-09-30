import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { nativeHttpFixture, nativeIdentity, good } from './native-capability-http.mjs';

export { nativeIdentity, good };
export const digest = value => createHash('sha256').update(value).digest('hex');
const SCOPE = 'notes.createDraft';
const signer = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(signer, { kid: 'full-host-fixture', use: 'sig', alg: 'RS256' });

/** Actual Connect + Notes2 + Caps3 + production HTTP/Provider. All consent is
 * signed and every code/AT/RT is issued over HTTP; no seeded identity/token or
 * replacement auth/storage port. Loopback callbacks are inspected, never fetched. */
export async function oauthNativeFixture(t) {
  let issuance = true;
  const privateKeys = { jwks: { keys: [signer] }, cookieKeys: [randomBytes(32).toString('base64url')],
    artifactKey: randomBytes(32), artifactKeyId: 'full-host-fixture' };
  const f = await nativeHttpFixture(t, { capabilitiesVersion: 3,
    oauth: origin => ({ enabled: issuance, issuer: origin + '/oauth', ...(issuance ? privateKeys : {}) }) });
  const issuer = f.origin + '/oauth';
  function browser() {
    const jar = new Map();
    function saveCookies(response, url) {
      for (const raw of response.headers.getSetCookie()) {
        const [pair, ...attributes] = raw.split(';'), equal = pair.indexOf('=');
        assert.ok(equal > 0, 'valid cookie pair');
        const attrs = Object.fromEntries(attributes.map(part => {
          const split = part.indexOf('='); return split < 0 ? [part.trim().toLowerCase(), true]
            : [part.slice(0, split).trim().toLowerCase(), part.slice(split + 1).trim()];
        }));
        assert.ok(attrs.domain === undefined, 'fixture only accepts the host-only provider cookies');
        const cookie = { name: pair.slice(0, equal), value: pair.slice(equal + 1), secure: attrs.secure === true,
          path: attrs.path ?? (url.pathname.slice(0, url.pathname.lastIndexOf('/')) || '/'),
          expires: attrs['max-age'] !== undefined ? Date.now() + Number(attrs['max-age']) * 1000
            : attrs.expires ? Date.parse(attrs.expires) : Infinity };
        const key = cookie.name + '\0' + cookie.path;
        if (!cookie.value || cookie.expires <= Date.now()) jar.delete(key); else jar.set(key, cookie);
      }
    }
    async function request(input, { fields, originHeader = f.origin, cookies = true } = {}) {
      const url = new URL(input, f.origin);
      assert.ok(url.origin === f.origin, 'never fetch a callback or a foreign origin');
      const cookie = [...jar.values()].filter(item => item.expires > Date.now()
        && (!item.secure || url.protocol === 'https:')
        && (url.pathname === item.path || url.pathname.startsWith(item.path.endsWith('/') ? item.path : item.path + '/')))
        .sort((a, b) => b.path.length - a.path.length).map(item => item.name + '=' + item.value).join('; ');
      const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(5000),
        ...(fields ? { method: 'POST', body: new URLSearchParams(fields) } : {}),
        headers: { ...(cookies && cookie ? { Cookie: cookie } : {}),
          ...(fields && originHeader ? { Origin: originHeader, 'Sec-Fetch-Site': 'same-origin' } : {}) } });
      if (cookies) saveCookies(response, url);
      const location = response.headers.get('location'), text = await response.text();
      assert.ok(Buffer.byteLength(text) <= 1048576, 'fixture response is bounded');
      return { status: response.status, headers: response.headers, text,
        location: location && new URL(location, url),
        ...(response.headers.get('content-type')?.includes('application/json') ? { body: JSON.parse(text) } : {}) };
    }
    return { request };
  }
  const wire = browser();
  const redirectFor = client => `http://127.0.0.1:19876${client === 'soty-opencode-cli' ? '/mcp/oauth/callback' : '/callback'}`;
  async function begin({ client = 'soty-codex-cli', resource = f.origin, session = browser() } = {}) {
    const redirectUri = redirectFor(client), verifier = randomBytes(32).toString('base64url'), state = randomBytes(24).toString('base64url');
    const query = new URLSearchParams({ client_id: client, redirect_uri: redirectUri, response_type: 'code', resource,
      scope: SCOPE, code_challenge_method: 'S256', code_challenge: createHash('sha256').update(verifier).digest('base64url'), state });
    const authorize = await session.request('/oauth/authorize?' + query);
    assert.ok([302, 303].includes(authorize.status) && authorize.location?.origin === f.origin, 'actual authorization reaches consent');
    const pathname = authorize.location.pathname;
    assert.ok(/^\/oauth\/interaction\/[A-Za-z0-9_-]{16,128}$/u.test(pathname), 'only interaction redirect is admitted');
    const html = await session.request(pathname); assert.equal(html.status, 200, 'actual consent document loads');
    const context = await session.request(pathname + '/context'); assert.equal(context.status, 200);
    assert.equal(context.body.decision, 'pending', 'every connection needs its own signed decision');
    return { client, resource, redirectUri, verifier, state, session, pathname, context: context.body };
  }
  async function decide(flow, actor, accountId, { approve = true } = {}) {
    const context = flow.context;
    const decision = good(await f.call(actor, `oauth.connections.${approve ? 'approve' : 'deny'}`, {
      expectedAccountId: accountId, interactionId: context.interactionId,
      browserNonce: context.browserNonce, contextDigest: context.contextDigest,
    }));
    return { ...flow, accountId, connectionId: decision.connectionId, decision };
  }
  async function complete(flow) {
    const result = await flow.session.request(flow.pathname + '/complete', { fields: { expectedAccountId: flow.accountId } });
    assert.equal(result.status, 303, 'signed decision produces the real Provider resume');
    assert.ok(result.location?.origin === f.origin, 'resume stays on issuer');
    let response = await flow.session.request(result.location);
    if (response.status === 200) {
      const action = /<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
      assert.ok(action === f.origin + '/oauth/session/end/confirm', 'actual Provider account-switch confirmation only');
      const fields = Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)]
        .map(match => [match[1], match[2]]));
      assert.deepEqual(Object.keys(fields).sort(), ['logout', 'xsrf']); assert.equal(fields.logout, 'yes');
      assert.ok(typeof fields.xsrf === 'string' && fields.xsrf.length > 10, 'real session-bound confirmation');
      response = await flow.session.request(action, { fields });
      assert.equal(response.status, 303, 'account switch returns to the authorization resume');
      assert.ok(response.location?.origin === f.origin
        && /^\/oauth\/authorize\/[A-Za-z0-9_-]{16,128}$/u.test(response.location.pathname),
      'account-switch resume remains on the exact issuer route');
      response = await flow.session.request(response.location);
    }
    assert.ok([302, 303].includes(response.status), 'authorization returns the callback redirect');
    const callback = response.location;
    assert.ok(callback?.origin + callback?.pathname === flow.redirectUri, 'exact registered callback, never followed');
    assert.ok(callback.searchParams.get('state') === flow.state && callback.searchParams.get('iss') === issuer, 'state and issuer match');
    return { ...flow, code: callback.searchParams.get('code'), error: callback.searchParams.get('error') };
  }
  const exchange = (flow, overrides = {}) => wire.request('/oauth/token', { cookies: false, originHeader: null,
    fields: { grant_type: 'authorization_code', client_id: flow.client, redirect_uri: flow.redirectUri,
      resource: flow.resource, code: flow.code, code_verifier: flow.verifier, ...overrides } });
  const refresh = (flow, tokens, overrides = {}) => wire.request('/oauth/token', { cookies: false, originHeader: null,
    fields: { grant_type: 'refresh_token', client_id: flow.client, resource: flow.resource,
      refresh_token: tokens.refresh_token, ...overrides } });
  function tokens(response) {
    const error = response.body?.error;
    assert.equal(response.status, 200, 'token exchange; safe error=' + (typeof error === 'string' && /^[a-z_]{1,40}$/u.test(error) ? error : 'none'));
    assert.ok(typeof response.body?.access_token === 'string' && typeof response.body?.refresh_token === 'string', 'actual tokens returned');
    assert.equal(response.body.scope, SCOPE); assert.equal(response.body.token_type, 'Bearer');
    return response.body;
  }
  async function connect(actor, accountId, options) {
    const flow = await complete(await decide(await begin(options), actor, accountId));
    assert.ok(typeof flow.code === 'string' && flow.error === null, 'approved flow issues code');
    return { flow, tokens: tokens(await exchange(flow)) };
  }
  return { ...f, get app() { return f.app; }, issuer, browser, wire, begin, decide, complete, exchange, refresh, tokens, connect,
    async keyless({ enabled = false } = {}) { issuance = false; await f.restart({ enabled }); } };
}
