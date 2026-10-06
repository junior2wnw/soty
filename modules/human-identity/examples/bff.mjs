import { createServer } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

// An isolated RP fixture, not a production RP deployment SDK. Reuse the maintained installed JOSE verifier.
const providerRequire = createRequire(import.meta.resolve('oidc-provider'));
const { jwtVerify, createLocalJWKSet } = await import(pathToFileURL(providerRequire.resolve('jose')).href);
const random = () => randomBytes(32).toString('base64url');
const sameOrigin = (value, expected) => new URL(value).origin === new URL(expected).origin;
function require(ok, code = 'bff_invalid_request') { if (!ok) throw Object.assign(new Error(code), { code }); }
async function fetchJson(url, options) {
  const response = await fetch(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(5000) });
  const text = await response.text(); require(Buffer.byteLength(text) <= 65536, 'bff_response_limit');
  let value; try { value = JSON.parse(text); } catch { throw new Error('bff_invalid_response'); }
  require(response.status === 200, 'bff_provider_rejected'); return value;
}
export async function verifyHumanIdToken({ token, jwks, issuer, clientId, nonce }) {
  const { payload } = await jwtVerify(token, createLocalJWKSet(jwks), { algorithms: ['RS256'], issuer, audience: clientId, clockTolerance: 0 });
  require(payload.aud === clientId && payload.nonce === nonce && typeof payload.sub === 'string' && payload.sub.length <= 128,
    'bff_invalid_id_token'); return payload;
}
function cookie(req, name) {
  const values = (req.headers.cookie || '').split(';').map(value => value.trim()).filter(value => value.startsWith(name + '='));
  require(values.length <= 1); return values[0]?.slice(name.length + 1);
}
/** Two independently created fixtures have separate pending state, sessions, account rows and link registries. */
export async function createHumanBffFixture({ clientId, clientSecret, name = clientId }) {
  const pending = new Map(), sessions = new Map(), legacySessions = new Map(), accounts = new Map(), links = new Map();
  const prefix = 'bff_' + clientId.replace(/[^A-Za-z0-9]/gu, '_'), sessionCookie = prefix + '_session', pendingCookie = prefix + '_pending', legacyCookie = prefix + '_legacy';
  const existing = { id: 'legacy-' + clientId, name: 'Same display name', marker: 'owned local row', balance: 17 }; accounts.set(existing.id, existing);
  let host, metadata, jwks, capturedToken = null, lastError = null;
  const server = createServer(async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    const send = (status, body) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
    try {
      require(host, 'bff_unconfigured'); const url = new URL(req.url, host.origin);
      if (url.pathname === '/login' && req.method === 'GET') {
        require(pending.size < 128, 'bff_capacity');
        const request = { state: random(), nonce: random(), verifier: random(), browser: random() };
        pending.set(request.browser, request);
        res.setHeader('Set-Cookie', `${pendingCookie}=${request.browser};Path=/;HttpOnly;SameSite=Lax`);
        const params = new URLSearchParams({ client_id: clientId, redirect_uri: host.redirectUri, response_type: 'code', scope: url.searchParams.get('scope') || 'openid profile',
          state: request.state, nonce: request.nonce, code_challenge_method: 'S256', code_challenge: createHash('sha256').update(request.verifier).digest('base64url') });
        res.writeHead(302, { location: metadata.authorization_endpoint + '?' + params }); res.end(); return;
      }
      if (url.pathname === '/oidc/callback' && req.method === 'GET') {
        const browser = cookie(req, pendingCookie), request = pending.get(browser);
        require(request && url.searchParams.getAll('state').length === 1 && url.searchParams.get('state') === request.state, 'bff_state_mismatch');
        require(url.searchParams.get('iss') === host.issuer && url.searchParams.getAll('code').length === 1 && !url.searchParams.has('error'), 'bff_issuer_mismatch');
        const tokens = await fetchJson(metadata.token_endpoint, { method: 'POST', headers: {
          authorization: 'Basic ' + Buffer.from(clientId + ':' + clientSecret).toString('base64'), 'content-type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ grant_type: 'authorization_code', redirect_uri: host.redirectUri, code: url.searchParams.get('code'), code_verifier: request.verifier }) });
        const claims = await verifyHumanIdToken({ token: tokens.id_token, jwks, issuer: host.issuer, clientId, nonce: request.nonce });
        const profile = await fetchJson(metadata.userinfo_endpoint, { headers: { authorization: 'Bearer ' + tokens.access_token } });
        require(profile.sub === claims.sub, 'bff_subject_mismatch'); pending.delete(browser);
        const identityKey = host.issuer + '\0' + claims.sub;
        let local = links.get(identityKey);
        if (!local) { local = 'oidc-' + random().slice(0, 16); accounts.set(local, { id: local, name: profile.name || 'Soty profile', marker: 'independent app row' }); links.set(identityKey, local); }
        const sid = random(); sessions.set(sid, { accessToken: tokens.access_token, claims, profile, localAccountId: local, csrf: random() });
        capturedToken = { idToken: tokens.id_token, accessToken: tokens.access_token, expectedNonce: request.nonce };
        res.setHeader('Set-Cookie', `${sessionCookie}=${sid};Path=/;HttpOnly;SameSite=Lax`); send(200, { ok: true, app: name }); return;
      }
      if (url.pathname === '/me' && req.method === 'GET') {
        const current = sessions.get(cookie(req, sessionCookie)); require(current, 'bff_session_required');
        const profile = await fetchJson(metadata.userinfo_endpoint, { headers: { authorization: 'Bearer ' + current.accessToken } });
        require(profile.sub === current.claims.sub, 'bff_subject_mismatch');
        send(200, { app: name, localAccountId: current.localAccountId, identity: { issuer: host.issuer, ...profile }, linkCsrf: current.csrf }); return;
      }
      if (url.pathname === '/oidc/link' && req.method === 'POST') {
        require(req.headers.origin === host.origin, 'bff_link_csrf');
        const current = sessions.get(cookie(req, sessionCookie)), legacy = legacySessions.get(cookie(req, legacyCookie));
        require(current && legacy && accounts.has(legacy), 'bff_two_sided_proof_required');
        const chunks = []; let bytes = 0;
        for await (const chunk of req) { bytes += chunk.length; require(bytes <= 1024, 'bff_request_limit'); chunks.push(chunk); }
        const input = new URLSearchParams(Buffer.concat(chunks).toString()); require(input.size === 1 && input.get('csrf') === current.csrf, 'bff_link_csrf');
        const profile = await fetchJson(metadata.userinfo_endpoint, { headers: { authorization: 'Bearer ' + current.accessToken } });
        require(profile.sub === current.claims.sub, 'bff_subject_mismatch');
        links.set(host.issuer + '\0' + profile.sub, legacy); current.localAccountId = legacy;
        send(200, { linked: true, localAccountId: legacy }); return;
      }
      send(404, { error: 'not_found' });
    } catch (error) {
      lastError = /^[a-z][a-z0-9_]{0,79}$/u.test(error?.code || error?.message || '') ? error.code || error.message : 'bff_authentication_failed';
      send(401, { error: lastError });
    }
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  return Object.freeze({ origin, redirectUri: origin + '/oidc/callback', clientId,
    async configure(issuer) {
      const discovered = await fetchJson(issuer + '/.well-known/openid-configuration'); require(discovered.issuer === issuer, 'bff_discovery_mismatch');
      for (const field of ['authorization_endpoint', 'token_endpoint', 'userinfo_endpoint', 'jwks_uri']) require(sameOrigin(discovered[field], issuer), 'bff_discovery_mismatch');
      host = { issuer, origin, redirectUri: origin + '/oidc/callback' }; metadata = discovered; jwks = await fetchJson(metadata.jwks_uri);
    },
    authenticateExistingLocalAccount() { const proof = random(); legacySessions.set(proof, existing.id); return `${legacyCookie}=${proof}`; },
    existingLocalRow() { return { ...existing }; },
    currentLink(subject) { return links.get(host.issuer + '\0' + subject); },
    // Test-only access is never an HTTP route, and no token/key is logged or written to configuration.
    verificationFixture() { return { ...capturedToken, jwks, issuer: host.issuer, clientId }; },
    pendingFixture() { return [...pending.values()].at(-1); },
    get lastError() { return lastError; },
    async close() { server.closeAllConnections(); await new Promise(done => server.close(done)); },
  });
}
