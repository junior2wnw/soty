import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import { setTimeout as pause } from 'node:timers/promises';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

const owner = { accountId: 'execution_owner', deviceId: 'execution_device' };
const secret = () => randomBytes(32).toString('base64url');
async function until(check) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) { if (await check()) return; await pause(10); }
  assert.fail('fixture binding did not become ready');
}
async function fixture(t, { stripCapability = false, corruptMetadata = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-execution-http-'));
  let service, runtime, sourcePolicy = "script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'", upstreamCalls = 0;
  const upstream = createServer((_req, res) => {
    upstreamCalls++;
    if (sourcePolicy !== undefined) res.setHeader('Content-Security-Policy', sourcePolicy);
    res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Execution fixture</title>');
  });
  const server = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    runtime?.stop(); service?.close();
    for (const item of [server, upstream]) { item.closeAllConnections(); await new Promise(done => item.close(done)); }
    assert.ok(resolve(directory).startsWith(resolve(tmpdir()) + sep));
    await rm(directory, { recursive: true, force: true });
  });
  await new Promise(done => upstream.listen(0, '127.0.0.1', done));
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, identity = { linkId: 'execution_link', hostDeviceId: 'execution_host', connectorId: 'execution_connector' }, token = secret();
  const config = { databasePath: join(directory, 'apps.sqlite'), shellOrigins: [`http://localhost:${port}`],
    namedAppZone: `http://named.localhost:${port}`, actorActive: actor => actor?.accountId === owner.accountId && actor.deviceId === owner.deviceId,
    authenticateConnector: async auth => auth.token === token && auth.linkId === identity.linkId && auth.connectorId === identity.connectorId && auth.deviceId === identity.hostDeviceId };
  service = createAppsService(config);
  const startRuntime = () => createLocalAppsRuntime({ randomSecret: secret, digest: value => createHash('sha256').update(value).digest('hex'),
    httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'),
    createWebSocket: url => {
      const socket = new WebSocket(url), send = socket.send.bind(socket);
      socket.send = data => {
        const frame = JSON.parse(data);
        if (stripCapability && frame.type === 'auth') delete frame.capabilities.relayResponseCsp;
        if (corruptMetadata && frame.type === 'head') frame.sourceCsp = typeof corruptMetadata === 'object'
          ? corruptMetadata : { schema: 'soty.relay-source-csp.v1', status: 'invalid' };
        return send(JSON.stringify(frame));
      };
      return socket;
    },
  }, { serverUrl: `http://127.0.0.1:${port}`, identity, token });
  runtime = startRuntime(); runtime.start(); await until(() => runtime.status().connected);
  const call = (op, args) => service.execute({ op, args, actor: owner });
  const claim = await runtime.claim(); call('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Execution fixture', port: upstream.address().port }).app;
  const domain = call('apps.domains.claim', { appId: app.id, slug: 'execution-test', requestId: 'execution-domain', expectedDomainsRevision: 0 }).receipt;
  await until(() => call('apps.list').apps[0].state === 'ready');
  const before = call('apps.publication.get', { appId: app.id });
  call('apps.publication.update', { appId: app.id, requestId: 'execution-public', expectedPolicyEpoch: before.policyEpoch,
    expectedTargetRevision: before.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [domain.domainId],
    exposureAck: { scope: 'whole-port', targetRevision: before.target.revision, targetDigest: before.target.digest, profile: before.target.profile } });
  const publication = call('apps.publication.get', { appId: app.id });
  const admission = { appId: app.id, policyEpoch: publication.policyEpoch,
    target: { revision: publication.target.revision, digest: publication.target.digest, profile: publication.target.profile },
    capabilities: { wasm: true, workers: 'self', connectSrc: ['https://tiles.openfreemap.org', 'https://s3.amazonaws.com/elevation-tiles-prod/'], imgSrc: ['https://tiles.maps.eox.at'] } };
  async function restart(admissions) {
    runtime.stop(); service.close();
    service = createAppsService({ ...config, runtimeExecutionPolicy: { schema: 'soty.app-execution-policy.v1', admissions } });
    runtime = startRuntime(); runtime.start();
    await until(() => runtime.status().connected && call('apps.list').apps[0].state === 'ready');
  }
  const get = () => new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path: '/', headers: { Host: new URL(domain.origin).host } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('error', reject);
      res.on('end', () => { const policies = []; for (let i = 0; i < res.rawHeaders.length; i += 2) if (res.rawHeaders[i].toLowerCase() === 'content-security-policy') policies.push(res.rawHeaders[i + 1]);
        done({ status: res.statusCode, policies, body: Buffer.concat(chunks).toString() }); });
    }); req.once('error', reject); req.setTimeout(5000, () => req.destroy(new Error('fixture_http_timeout'))); req.end();
  });
  return { get, restart, admission, setSourcePolicy: value => { sourcePolicy = value; }, upstreamCalls: () => upstreamCalls };
}

test('actual relay expands only the exact reviewed tuple and preserves the real upstream CSP independently', { timeout: 25_000 }, async t => {
  const f = await fixture(t);
  const baseline = await f.get(); assert.equal(baseline.status, 200); assert.equal(baseline.policies.length, 1);
  assert.doesNotMatch(baseline.policies[0], /wasm-unsafe-eval|tiles\.openfreemap|worker-src 'self'/u);
  await f.restart([f.admission]);
  const approved = await f.get(); assert.equal(approved.status, 200, approved.body); assert.equal(approved.policies.length, 2);
  assert.match(approved.policies[0], /script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval';/u);
  assert.match(approved.policies[0], /worker-src 'self';/u);
  assert.match(approved.policies[0], /https:\/\/s3\.amazonaws\.com\/elevation-tiles-prod\//u);
  assert.doesNotMatch(approved.policies[0], /'unsafe-eval'|script-src[^;]*blob:|worker-src[^;]*blob:/u);
  assert.equal(approved.policies[1], "script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'");
  f.setSourcePolicy("script-src 'self'; worker-src 'none'");
  const strict = await f.get(); assert.equal(strict.status, 200); assert.equal(strict.policies[1], "script-src 'self'; worker-src 'none'");
  await f.restart([{ ...f.admission, policyEpoch: f.admission.policyEpoch + 1 }]);
  const mismatch = await f.get(); assert.equal(mismatch.status, 200); assert.deepEqual(mismatch.policies, baseline.policies);
});

test('old connector remains available without opt-in but cannot silently drop CSP on an approved application', { timeout: 20_000 }, async t => {
  const f = await fixture(t, { stripCapability: true });
  assert.equal((await f.get()).status, 200);
  await f.restart([f.admission]);
  const count = f.upstreamCalls(), refused = await f.get();
  assert.equal(refused.status, 503); assert.match(refused.body, /app_execution_policy_unavailable/u);
  assert.equal(f.upstreamCalls(), count, 'no source dispatch before preservation capability');
});

test('invalid source CSP metadata fails closed before a permissive policy or source body is exposed', { timeout: 20_000 }, async t => {
  const f = await fixture(t, { corruptMetadata: true });
  const baseline = await f.get(); assert.equal(baseline.status, 200, 'default behavior ignores new optional metadata');
  await f.restart([f.admission]);
  const refused = await f.get(); assert.equal(refused.status, 502); assert.match(refused.body, /app_execution_source_csp_invalid/u);
  assert.ok(refused.policies.every(policy => !policy.includes('wasm-unsafe-eval')));
  assert.doesNotMatch(refused.body, /Execution fixture/u);
});

test('non-HTTP Source policy characters are refused before expanded headers are installed', { timeout: 20_000 }, async t => {
  const f = await fixture(t, { corruptMetadata: { schema: 'soty.relay-source-csp.v1', status: 'present', policy: 'script-src \u0100' } });
  await f.restart([f.admission]);
  const refused = await f.get();
  assert.equal(refused.status, 502); assert.match(refused.body, /app_execution_source_csp_invalid/u);
  assert.ok(refused.policies.every(policy => !policy.includes('wasm-unsafe-eval')));
});
