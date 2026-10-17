import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHmac, generateKeyPairSync } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createFeedbackSource } from '../../source.mjs';
import { createProjectService } from '../../service.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../../source-app/server/index.mjs';
import { selectedResourceProfile } from '../../../apps/scoped-embed/resource-profile.mjs';
import { createResourceSourceProofSigner } from '../../../apps/scoped-embed/resource-proof.mjs';
import { digest } from '../../../source-app/server/wire.mjs';
const require = createRequire(import.meta.url), oidc = createRequire(require.resolve('openid-client'));
const { SignJWT, importJWK } = await import(pathToFileURL(oidc.resolve('jose')).href);
const random = () => randomBytes(32).toString('base64url');

// Synthetic issuer/transport keys live only in this private local fixture.
// rootAccountFor binds an actual signed Root test account to its witness.
export async function feedbackSourceFixture(t, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'feedback-source-')), transportKey = randomBytes(32), storageKey = randomBytes(32);
  let source, service, context, profile, token, forged = false, live = true, clock = Date.now();
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const signing = await importJWK({ ...pair.privateKey.export({ format: 'jwk' }), alg: 'RS256' }, 'RS256');
  const issuer = createServer((_req, res) => res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ keys: [publicJwk] })));
  const connector = createServer(async (req, res) => { try {
    if (!live) { res.writeHead(403).end(); return; }
    const parts = []; for await (const part of req) parts.push(part);
    const input = JSON.parse(Buffer.concat(parts));
    const now = clock;
    const identityWitness = await new SignJWT({ iss: profile.issuer, aud: 'urn:soty:source-authority', sub: context.humanPrincipal.subject,
      iat: Math.floor(now / 1000), exp: Math.floor(Math.min(now + 10000, context.expiresAt) / 1000),
      sourceChallenge: input.nonce, contextDigest: digest(context) })
      .setProtectedHeader({ alg: 'RS256', typ: 'soty-source-authority+jwt', kid: 'fixture' }).sign(signing);
    const body = JSON.stringify({ nonce: input.nonce, context, ...(forged ? {} : { identityWitness }) });
    res.writeHead(200, { 'content-type': 'application/json',
      'x-soty-source-mac': createHmac('sha256', transportKey).update('authority-result\0' + body).digest('base64url') }).end(body);
  } catch { res.writeHead(503).end(); } });
  const server = createServer((req, res) => void source.handleRequest(req, res));
  for (const s of [issuer, connector, server]) await new Promise(done => s.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`, issuerUrl = `http://127.0.0.1:${issuer.address().port}/human-identity`;
  const appId = 'app-' + 'a'.repeat(32);
  profile = selectedResourceProfile({ schema: 'soty.selected-human-embed.v2', appId,
    connector: { linkId: 'fixture', hostDeviceId: 'host', connectorId: 'connector' }, target: { revision: 1, digest: '1'.repeat(64) },
    sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', environmentId: 'fixture', tenantId: 'owner', appId, resourceId: 'feedback',
      selection: { kind: 'soty.workspace.v1', nativeId: 'feedback', incarnationId: 'one' } },
    issuer: issuerUrl, clientId: 'feedback-client', embedOrigin: origin, nativeOrigin: origin, parentOrigin: new URL(issuerUrl).origin });
  function newContext(subject = 'person-one') {
    token = undefined;
    context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: random(), version: 1, digest: randomBytes(32).toString('hex') },
      profileDigest: profile.digest, appId, sourceProfile: profile.sourceProfile, resource: profile.resource, instanceId: 'main',
      rootPrincipal: { accountId: options.rootAccountFor?.(subject) ?? 'root-' + subject, deviceId: 'device' },
      humanPrincipal: { issuer: issuerUrl, subject, clientId: profile.clientId, clientProfileDigest: '2'.repeat(64), clientGeneration: 1 },
      entry: { domainId: 'domain', origin }, target: profile.target, policyEpoch: 1, expiresAt: clock + 300000 };
  }
  function start() {
    service = createProjectService({ dataDirectory: dir, origin, now: () => clock });
    source = createFeedbackSource({ service, now: () => clock, config: { profile, transportKey, connectorPort: connector.address().port,
      databasePath: join(dir, 'source.sqlite'), storageKey, storageKeyId: 'fixture' } });
  }
  newContext(); start();
  const signer = createResourceSourceProofSigner({ profile, key: transportKey, clock: () => clock });
  async function request(path = '/api/embed/session-continue', input = { requestId: 'source-continue-0001' }, signed = true, deferred = false) {
    const body = Buffer.from(JSON.stringify(input)), cookie = token ? 'soty_rp_session=' + token : '';
    const headers = { origin, 'content-type': 'application/json', ...(cookie ? { cookie } : {}),
      ...(signed ? signer.headers({ context, method: 'POST', path, body, cookie }) : {}) };
    const send = async () => {
      const response = await fetch(origin + path, { method: 'POST', headers, body });
      const set = response.headers.get('set-cookie'); if (set) token = set.split(';')[0].split('=')[1];
      return { status: response.status, body: await response.json() };
    };
    return deferred ? send : send();
  }
  async function contextRef() {
    const ready = await request(); assert.equal(ready.status, 200, JSON.stringify(ready.body));
    const result = await request('/api/embed/query', { requestId: 'source-context-0001', input: { operation: 'soty.current_context' } });
    assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body.data.context.ref;
  }
  t.after(async () => { source?.close(); service?.close(); for (const s of [server, connector, issuer]) {
    s.closeAllConnections(); await new Promise(done => s.close(done));
  } await rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 40 }); });
  return { profile, origin, request, contextRef, newContext, forge: value => { forged = value; }, revoke: () => { live = false; },
    restart() { source.close(); service.close(); start(); }, service: () => service,
    ownActor: () => service.actor(service.identity(issuerUrl, context.humanPrincipal.subject)), advance: ms => { clock += ms; } };
}

