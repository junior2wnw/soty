import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm, mkdir, writeFile, realpath, stat, readFile, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative, isAbsolute, sep } from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime, readLocalAppProposal, normalizeLocalAppManifest } from '../../../scripts/agent-modules/local-apps.mjs';
import { createSampleApp } from '../examples/sample-app.mjs';

const owner = { accountId: 'acct_owner', deviceId: 'dev_owner' }, member = { accountId: 'acct_member', deviceId: 'dev_member' }, outsider = { accountId: 'acct_outsider', deviceId: 'dev_outsider' };
const runtimeDeps = { randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
  createWebSocket: url => new WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: text => Buffer.from(text, 'base64') };

async function setup({ secureOrigins = false } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-apps-test-'));
  const sample = await createSampleApp(); let service;
  const server = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); const port = server.address().port;
  const groupMembers = new Set([owner.accountId, member.accountId]); const groupAdmins = new Set([owner.accountId]); const revokedActors = new Set(); let membershipListener;
  const connectorAuth = { active: true };
  const authToken = randomBytes(32).toString('base64url');
  const options = { databasePath: join(dir, 'apps.sqlite'), appOriginTemplate: secureOrigins ? 'https://{appId}.apps.example.org' : `http://{appId}.localhost:${port}`, shellOrigins: [`http://localhost:${port}`],
    actorActive: actor => [owner, member, outsider].some(item => item.accountId === actor.accountId && item.deviceId === actor.deviceId) && !revokedActors.has(actor.deviceId),
    canAccessCommunity: (accountId, id) => id === 'community_family' && groupMembers.has(accountId), isGroupAdmin: (accountId, id) => id === 'community_family' && groupAdmins.has(accountId),
    activeCommunityIds: accountId => groupMembers.has(accountId) ? ['community_family'] : [],
    subscribeMembership: fn => { membershipListener = fn; return () => {}; },
    connectorAuthCheckMs: 250, authenticateConnector: async auth => connectorAuth.active && auth.token === authToken && auth.deviceId === 'host_test' && auth.connectorId === 'connector_test' && auth.linkId === 'test_link_1234567890' };
  service = createAppsService(options);
  let claimSecretsGenerated = 0;
  const runtime = createLocalAppsRuntime({ ...runtimeDeps, randomSecret: () => { claimSecretsGenerated += 1; return runtimeDeps.randomSecret(); } }, { serverUrl: `http://127.0.0.1:${port}`, identity: { linkId: 'test_link_1234567890', hostDeviceId: 'host_test', connectorId: 'connector_test', name: 'Test laptop' }, token: authToken });
  runtime.start(); await until(() => runtime.status().connected);
  assert.equal(claimSecretsGenerated, 0, 'connection must not generate a claim secret without an explicit action');
  const call = (actor, op, args = {}) => service.execute({ actor, op, args });
  const claim = await runtime.claim(); call(owner, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  assert.equal(claimSecretsGenerated, 1);
  assert.throws(() => call(outsider, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode }), /apps_claim_unavailable/u);
  const app = call(owner, 'apps.register', { hostDeviceId: 'host_test', connectorId: 'connector_test', name: 'Покупки', port: sample.port, grants: { communityIds: ['community_family'] } }).app;
  await until(() => call(owner, 'apps.list').apps[0].state === 'ready');
  const appHost = `${app.id}.localhost:${port}`, appOrigin = `http://${appHost}`;
  const launch = async actor => {
    const result = call(actor, 'apps.launch', { appId: app.id });
    const ticket = new URL(result.launchUrl).hash.slice(1);
    const response = await http(port, '/_soty/session', { host: appHost, origin: appOrigin, method: 'POST', body: JSON.stringify({ ticket }), headers: { 'content-type': 'application/json' } });
    assert.equal(response.status, 200, response.body.toString()); return response.headers['set-cookie'][0].split(';')[0];
  };
  return { dir, sample, server, port, service, options, runtime, call, app, appHost, appOrigin, launch, groupMembers, groupAdmins, revokedActors, connectorAuth,
    notify: () => membershipListener({ communityId: 'community_family', profileId: member.accountId, state: 'removed' }),
    close: async () => { runtime.stop(); service.close(); await sample.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await rm(dir, { recursive: true, force: true }); } };
}

test('TLS edge permits exact registered HTTPS addresses, including retained revoked status pages', async t => {
  const env = await setup({ secureOrigins: true }); t.after(env.close);
  const domain = `${env.app.id}.apps.example.org`;
  assert.equal(env.service.allowsTlsDomain(domain), true);
  for (const value of [undefined, [domain], `${domain}.evil.test`, `prefix.${domain}`, `${domain}:443`, `https://${domain}`, `app-${'0'.repeat(32)}.apps.example.org`]) assert.equal(env.service.allowsTlsDomain(value), false);
  env.call(owner, 'apps.revoke', { appId: env.app.id });
  assert.equal(env.service.allowsTlsDomain(domain), true, 'TLS ownership is separate from runtime access');
});

test('expected account guards every app operation before reads or mutation while legacy clients remain compatible', async t => {
  const env = await setup(); t.after(env.close);
  const expectedAccountId = owner.accountId;
  assert.equal(env.call(owner, 'apps.devices', { expectedAccountId }).devices.length, 1);
  assert.equal(env.call(owner, 'apps.list', { expectedAccountId }).apps[0].id, env.app.id);
  assert.equal(env.call(owner, 'apps.devices').devices.length, 1);
  assert.throws(() => env.call(owner, 'apps.devices', { expectedAccountId, extra: true }), /unexpected_argument/u);
  for (const op of ['apps.devices', 'apps.list', 'apps.register', 'apps.update', 'apps.revoke', 'apps.launch', 'apps.claim']) {
    assert.throws(() => env.call(outsider, op, { expectedAccountId, appId: env.app.id }), /authentication_required/u);
  }
  for (const invalid of ['', null, 42, {}, ['acct_owner']]) {
    assert.throws(() => env.call(owner, 'apps.devices', { expectedAccountId: invalid }), /authentication_required/u);
  }
  assert.equal(env.call(owner, 'apps.register', { expectedAccountId, hostDeviceId: 'host_test', connectorId: 'connector_test',
    name: 'Покупки', port: env.sample.port, grants: { communityIds: ['community_family'] } }).app.id, env.app.id);
  assert.equal(env.call(owner, 'apps.update', { expectedAccountId, appId: env.app.id, name: 'Список покупок' }).app.name, 'Список покупок');
  assert.equal(env.call(owner, 'apps.list', { expectedAccountId }).apps.length, 1);
});

test('real app HTTP/assets/POST and WebSocket work for two principals; third is denied; revoke closes current access', async t => {
  const env = await setup(); t.after(env.close);
  const ownerCookie = await env.launch(owner), memberCookie = await env.launch(member);
  assert.throws(() => env.call(outsider, 'apps.launch', { appId: env.app.id }), /apps_access_denied/u);
  const home = await http(env.port, '/', { host: env.appHost, cookie: memberCookie }); assert.equal(home.status, 200); assert.match(home.body.toString(), /Покупки/u);
  const asset = await http(env.port, '/app.js', { host: env.appHost, cookie: memberCookie }); assert.equal(asset.status, 200); assert.match(asset.headers['content-type'], /javascript/u);
  const post = await http(env.port, '/api/items', { host: env.appHost, cookie: ownerCookie, origin: env.appOrigin, method: 'POST', body: JSON.stringify({ text: 'Яблоки' }), headers: { 'content-type': 'application/json', authorization: 'Bearer must-not-pass' } });
  assert.equal(post.status, 200); assert.equal(post.headers['set-cookie'], undefined);
  const items = await http(env.port, '/api/items', { host: env.appHost, cookie: memberCookie }); assert.match(items.body.toString(), /Яблоки/u);
  const forwarded = env.sample.requests.findLast(item => item.path === '/api/items'); assert.equal(forwarded.headers.cookie, undefined); assert.equal(forwarded.headers.authorization, undefined);
  const ws = new WebSocket(`ws://127.0.0.1:${env.port}/live`, { headers: { Host: env.appHost, Origin: env.appOrigin, Cookie: memberCookie } });
  const first = await wsMessage(ws); assert.match(first.toString(), /Яблоки/u);
  const echo = wsMessage(ws); ws.send('hello over real tunnel'); assert.equal((await echo).toString(), 'hello over real tunnel');
  const closure = new Promise(resolve => ws.once('close', resolve)); env.groupMembers.delete(member.accountId); env.notify(); await closure;
  const denied = await http(env.port, '/api/items', { host: env.appHost, cookie: memberCookie }); assert.equal(denied.status, 403);
  const ownerStillWorks = await http(env.port, '/api/items', { host: env.appHost, cookie: ownerCookie }); assert.equal(ownerStillWorks.status, 200);
});

test('ticket reuse, wrong Origin, ownership forgery, redirect SSRF and forbidden port fail closed', async t => {
  const env = await setup(); t.after(env.close);
  assert.throws(() => env.call(outsider, 'apps.register', { hostDeviceId: 'host_test', connectorId: 'connector_test', name: 'Stolen', port: 8000 }), /apps_device_not_owned/u);
  assert.throws(() => env.call(owner, 'apps.register', { hostDeviceId: 'host_test', connectorId: 'connector_test', name: 'Admin', port: 49424 }), /invalid_app_port/u);
  const launched = env.call(owner, 'apps.launch', { appId: env.app.id }), ticket = new URL(launched.launchUrl).hash.slice(1);
  const fields = { host: env.appHost, origin: env.appOrigin, method: 'POST', body: JSON.stringify({ ticket }) };
  assert.equal((await http(env.port, '/_soty/session', fields)).status, 200);
  assert.equal((await http(env.port, '/_soty/session', fields)).status, 403);
  const cookie = await env.launch(owner);
  assert.equal((await http(env.port, '/api/items', { host: env.appHost, cookie, origin: 'https://evil.example' })).status, 403);
  assert.equal((await http(env.port, '/redirect', { host: env.appHost, cookie })).status, 502);
  assert.equal((await http(env.port, '//169.254.169.254/', { host: env.appHost, cookie })).status, 400);
  assert.ok(!env.sample.requests.some(item => item.path.includes('meta-data')));
});

test('bounded streaming sends large real assets and revoke terminates active HTTP', async t => {
  const env = await setup(); t.after(env.close); const cookie = await env.launch(member);
  const response = await http(env.port, '/large', { host: env.appHost, cookie }); assert.equal(response.status, 200); assert.equal(response.body.length, 4 * 1024 * 1024);
  for (let i = 0; i < 64; i++) assert.equal(response.body[i * 65536], i);
  const slow = request({ hostname: '127.0.0.1', port: env.port, path: '/slow', headers: { host: env.appHost, cookie } });
  const res = await new Promise(resolve => { slow.on('response', resolve); slow.end(); });
  await new Promise(resolve => res.once('data', resolve)); const closed = new Promise(resolve => res.once('close', resolve));
  env.call(owner, 'apps.revoke', { appId: env.app.id }); await closed;
  assert.equal((await http(env.port, '/', { host: env.appHost, cookie })).status, 403);
});

test('host credential revocation terminates live sessions and owner demotion withdraws community grants', async t => {
  const env = await setup(); t.after(env.close); const memberCookie = await env.launch(member);
  const firstSocket = new WebSocket(`ws://127.0.0.1:${env.port}/live`, { headers: { Host: env.appHost, Origin: env.appOrigin, Cookie: memberCookie } });
  await wsMessage(firstSocket); const demoted = new Promise(resolve => firstSocket.once('close', resolve));
  env.groupAdmins.delete(owner.accountId); env.notify(); await demoted;
  assert.equal((await http(env.port, '/', { host: env.appHost, cookie: memberCookie })).status, 403);
  const ownerCookie = await env.launch(owner);
  const secondSocket = new WebSocket(`ws://127.0.0.1:${env.port}/live`, { headers: { Host: env.appHost, Origin: env.appOrigin, Cookie: ownerCookie } });
  await wsMessage(secondSocket); const revoked = new Promise(resolve => secondSocket.once('close', resolve));
  env.connectorAuth.active = false; await revoked;
  assert.equal(env.call(owner, 'apps.list').apps[0].state, 'offline');
});

test('device offline is distinct from stopped service; registry survives reload', async t => {
  const env = await setup(); t.after(env.close);
  const cookie = await env.launch(owner); await env.sample.close();
  assert.equal((await http(env.port, '/', { host: env.appHost, cookie })).status, 502);
  await until(() => env.call(owner, 'apps.list').apps[0].state === 'stopped');
  env.runtime.stop(); await until(() => env.call(owner, 'apps.list').apps[0].state === 'offline');
  env.service.close();
  const reopened = createAppsService(env.options);
  assert.equal(reopened.execute({ actor: owner, op: 'apps.list' }).apps[0].id, env.app.id);
  assert.equal(reopened.execute({ actor: owner, op: 'apps.devices' }).devices.length, 1); reopened.close();
});

test('registration retry keeps the stable app id and cannot silently change parameters', async t => {
  const env = await setup(); t.after(env.close);
  const args = { hostDeviceId: 'host_test', connectorId: 'connector_test', name: 'Покупки', port: env.sample.port, grants: { communityIds: ['community_family'] } };
  assert.equal(env.call(owner, 'apps.register', args).app.id, env.app.id);
  assert.equal(env.call(owner, 'apps.list').apps.length, 1);
  assert.throws(() => env.call(owner, 'apps.register', { ...args, name: 'Other app' }), /app_port_already_registered/u);
  assert.equal(env.service.resolveOwnedDevice(owner, 'host_test').linkId, 'test_link_1234567890');
  assert.throws(() => env.service.resolveOwnedDevice(member, 'host_test'), /apps_device_not_owned/u);
});

test('OpenCode proposal validates actual allowed workspace, exact manifest and live app', async t => {
  const root = await mkdtemp(join(tmpdir(), 'soty-app-manifest-')); const sample = await createSampleApp();
  t.after(async () => { await sample.close(); await rm(root, { recursive: true, force: true }); });
  const workspace = join(root, 'workspace'); await mkdir(join(workspace, '.soty'), { recursive: true });
  const dependencies = { ...runtimeDeps, realpath, stat, readFile, join, isWithin: (base, target) => { const part = relative(base, target); return part === '' || (part !== '..' && !part.startsWith(`..${sep}`) && !isAbsolute(part)); } };
  const manifest = { schema: 'soty.local-app.v1', name: 'Покупки', port: sample.port, entryPath: '/' };
  await writeFile(join(workspace, '.soty', 'app.json'), JSON.stringify(manifest));
  const proposal = await readLocalAppProposal(dependencies, { workspace, allowedRoots: [root], jobId: 'job_test' }); assert.equal(proposal.sourceJobId, 'job_test'); assert.equal(proposal.port, sample.port);
  await assert.rejects(readLocalAppProposal(dependencies, { workspace, allowedRoots: [join(root, 'other')] }), /ENOENT/u);
  assert.throws(() => normalizeLocalAppManifest({ ...manifest, command: 'execute anything' }), /invalid_app_manifest/u);
  assert.throws(() => normalizeLocalAppManifest({ ...manifest, port: 49424 }), /invalid_app_port/u);
  await writeFile(join(workspace, '.soty', 'app.json'), JSON.stringify({ ...manifest, port: 1 }));
  await assert.rejects(readLocalAppProposal(dependencies, { workspace, allowedRoots: [root], jobId: 'job_test' }), /invalid_app_port/u);
  await writeFile(join(workspace, '.soty', 'app.json'), ' '.repeat(4097));
  await assert.rejects(readLocalAppProposal(dependencies, { workspace, allowedRoots: [root], jobId: 'job_test' }), /app_manifest_invalid_file/u);
  const external = join(root, 'outside-project'), linked = join(root, 'linked-project');
  await mkdir(external); await mkdir(linked); await writeFile(join(external, 'app.json'), JSON.stringify(manifest));
  await symlink(external, join(linked, '.soty'), process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(readLocalAppProposal(dependencies, { workspace: linked, allowedRoots: [root], jobId: 'job_test' }), /app_manifest_outside_workspace/u);
  await writeFile(join(workspace, '.soty', 'app.json'), JSON.stringify(manifest));
  await assert.rejects(readLocalAppProposal(dependencies, { workspace, allowedRoots: [root], jobId: 'job_test', completedAfter: Date.now() + 1000 }), /app_manifest_invalid_file/u);
});

async function until(check) { const end = Date.now() + 5000; while (!check()) { if (Date.now() > end) throw new Error('condition timeout'); await new Promise(resolve => setTimeout(resolve, 20)); } }
function wsMessage(ws) { return new Promise((resolve, reject) => { ws.once('message', resolve); ws.once('error', reject); }); }
function http(port, path, { host, cookie, origin, method = 'GET', body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { ...(host ? { Host: host } : {}), ...(cookie ? { Cookie: cookie } : {}), ...(origin ? { Origin: origin } : {}), ...headers } }, res => {
      const parts = []; res.on('data', chunk => parts.push(chunk)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(parts) })); res.on('error', reject);
    }); req.on('error', reject); if (body) req.write(body); req.end();
  });
}
