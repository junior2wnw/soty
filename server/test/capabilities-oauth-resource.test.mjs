import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import express from 'express';
import { attachCapabilitiesActions } from '../capabilities-actions.js';
import { AccessError } from '../../modules/capabilities/server/validation.mjs';

const ID = 'inv_' + 'a'.repeat(32), ROUTE = '/api/capabilities/v1/invocations/' + ID;
async function fixture(t, { secure = true, trustedProxy = false, configured = true } = {}) {
  const app = express(), calls = { legacy: 0, oauth: 0, read: 0 };
  if (trustedProxy) app.set('trust proxy', 'loopback');
  const audience = secure ? 'https://resource.soty.invalid' : 'http://127.0.0.1:49731';
  let fault;
  const authenticate = name => () => { calls[name]++; if (fault) throw new AccessError(fault); return Object.freeze({}); };
  attachCapabilitiesActions(app, { audience,
    resourceMetadata: configured ? `${audience}/.well-known/oauth-protected-resource` : null,
    service: { authenticateCredential: authenticate('legacy'), oauth: { authenticateBearer: authenticate('oauth') },
      nativeNotes: { readiness: () => ({ ready: false }), get() { calls.read++; return { invocation: {
        invocationId: ID, capabilityId: 'notes.createDraft', version: 1, status: 'cancelled', cancelRequested: true,
        effectState: 'none', effects: [], createdAt: 1, updatedAt: 2, completedAt: 2,
      } }; } } },
  });
  const server = createServer(app);
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => { server.closeAllConnections(); await new Promise(done => server.close(done)); });
  const http = (headers = {}) => new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port: server.address().port, path: ROUTE,
      headers: { host: new URL(audience).host, authorization: 'Bearer synthetic-oauth-transport', ...headers } }, res => {
      const chunks = []; res.on('data', bytes => chunks.push(bytes)); res.once('error', reject);
      res.once('end', () => done({ status: res.statusCode, headers: res.headers, body: JSON.parse(Buffer.concat(chunks)) }));
    });
    req.once('error', reject); req.end();
  });
  return { calls, audience, http, fail(code) { fault = code; } };
}

test('configured HTTPS resource rejects raw HTTP and forged transport before authentication or current read', async t => {
  const f = await fixture(t);
  for (const headers of [{}, { 'x-forwarded-proto': 'https' }, { forwarded: 'proto=https;host=resource.soty.invalid' }]) {
    const response = await f.http(headers);
    assert.equal(response.status, 403);
    assert.deepEqual(response.body, { error: { code: 'access_denied' } });
    assert.equal(response.headers['cache-control'], 'no-store');
  }
  assert.deepEqual(f.calls, { legacy: 0, oauth: 0, read: 0 });
});

test('explicitly trusted proxy is admitted, while missing or non-HTTPS proxy transport is denied', async t => {
  const f = await fixture(t, { trustedProxy: true });
  assert.equal((await f.http({ 'x-forwarded-proto': 'https' })).status, 200);
  assert.equal((await f.http()).status, 403);
  assert.equal((await f.http({ 'x-forwarded-proto': 'http' })).status, 403);
  assert.deepEqual(f.calls, { legacy: 0, oauth: 1, read: 1 });
});

test('loopback profile uses OAuth resolver without an execution-readiness guard; legacy auth never falls back', async t => {
  const f = await fixture(t, { secure: false });
  assert.equal((await f.http()).status, 200);
  f.fail('authorization_required');
  const denied = await f.http({ authorization: 'Bearer soty_cap_synthetic-invalid' });
  assert.equal(denied.status, 401);
  assert.equal(denied.headers['www-authenticate'], `Bearer realm="soty", resource_metadata="${f.audience}/.well-known/oauth-protected-resource"`);
  assert.deepEqual(f.calls, { legacy: 1, oauth: 1, read: 1 });
  for (const code of ['oauth_unavailable', 'oauth_storage_busy', 'oauth_storage_key_unavailable', 'capabilities_storage_corrupt']) {
    f.fail(code); const response = await f.http();
    assert.equal(response.status, 503); assert.deepEqual(response.body, { error: { code: 'service_unavailable' } });
    assert.equal(response.headers['retry-after'], '1');
  }
  assert.equal(f.calls.read, 1);
});

test('an unconfigured legacy resource preserves its existing transport policy and challenge', async t => {
  const f = await fixture(t, { configured: false });
  assert.equal((await f.http({ authorization: 'Bearer soty_cap_synthetic' })).status, 200);
  f.fail('authorization_required');
  assert.equal((await f.http()).headers['www-authenticate'], 'Bearer realm="soty"');
});
