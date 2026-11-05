import { createServer, request } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import WebSocket from 'ws';
import { createHttpApp } from '../../../server/http-app.js';
import { createClientWithStorage } from '../../connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';
import { createSampleApp } from '../../apps/examples/sample-app.mjs';

const until = async check => { const deadline = Date.now() + 5000; while (!check()) { if (Date.now() > deadline) throw new Error('fixture_timeout'); await new Promise(done => setTimeout(done, 10)); } };
function identityStore() {
  let state = null;
  return { async read() { return structuredClone(state); }, async claim(value) { state ??= structuredClone(value); return structuredClone(state); },
    async compareAndSwap(revision, next) { if (state.localRevision !== revision) throw new Error('fixture_revision'); state = structuredClone(next); return structuredClone(state); } };
}
export async function createInvitationFixture({ port = 0, distDir, additionalOrigins = [], appScheme = 'http', scopedAppCleanup = false } = {}) {
  if (!['http', 'https'].includes(appScheme)) throw new Error('fixture_app_scheme_invalid');
  if(typeof scopedAppCleanup!=='boolean')throw new Error('fixture_cleanup_option_invalid');
  const parent = await realpath(tmpdir()), directory = await mkdtemp(join(parent, 'soty-invitation-http-'));
  const dataDir = join(directory, 'data'), clients = [], sockets = new Set();
  let app, runtime, sample, closed = false, handler;
  const server = createServer((req, res) => { if (handler?.(req, res)) return; return app ? app(req, res) : res.writeHead(503).end(); });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  async function close() {
    if (closed) return; closed = true; runtime?.stop(); clients.forEach(client => client.dispose());
    const stopped = new Promise(done => server.close(done)); sockets.forEach(socket => socket.destroy()); await stopped;
    await app?.locals.closeServices(); await sample?.close();
    const actual = await realpath(directory);
    if (dirname(actual) !== parent || !basename(actual).startsWith('soty-invitation-http-')) throw new Error('fixture_cleanup_scope');
    await rm(actual, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  }
  try {
    if (!distDir) { distDir = join(directory, 'dist'); await mkdir(distDir); await writeFile(join(distDir, 'index.html'), '<!doctype html><title>Invitation API fixture</title>'); }
    await new Promise(done => server.listen(port, '127.0.0.1', done));
    const origin = `http://127.0.0.1:${server.address().port}`;
    const openRoot = () => createHttpApp(distDir, { dataDir, connectOrigins: [origin, ...additionalOrigins], appOriginTemplate: `${appScheme}://{appId}.legacy.localhost:${server.address().port}`,
      namedAppZone: `${appScheme}://named.localhost:${server.address().port}`, allowAudienceMigration: true });
    app = openRoot();
    server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
    function clientFor(storage) {
      const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
        scopedAppCleanup,
        fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, storage);
      clients.push(client); return client;
    }
    async function principal(label) {
      const storage=identityStore(),client=clientFor(storage);
      return { storage,client, account: await client.bootstrap(label) };
    }
    const owner = await principal('Владелец'), member = await principal('Ирина'), director = await principal('Руководитель'), outsider = await principal('Посторонний');
    const call = (actor, op, args = {}) => actor.client.extension(op, {
      ...(/^(?:apps|invitations)\./u.test(op) ? { expectedAccountId: actor.account.accountId } : {}), ...args }, { expectedAccountId: actor.account.accountId });
    for (const actor of [owner, member, director, outsider]) await call(actor, 'world.profile.get');
    const identity = { linkId: 'invitation_fixture_link_01234567890123456789', hostDeviceId: 'invitation_host', connectorId: 'invitation_connector' }, token = randomBytes(32).toString('base64url');
    const registration = await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
    if (!(await registration.json()).ok) throw new Error('fixture_connector_registration');
    sample = await createSampleApp();
    runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
      createWebSocket: url => new WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
    { serverUrl: origin, identity: { ...identity, name: 'Тестовый компьютер' }, token });
    runtime.start(); await until(() => runtime.status().connected);
    const claim = await runtime.claim(); await call(owner, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
    const registered = (await call(owner, 'apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Команда проекта',
      port: sample.port, grants: { accountIds: [], communityIds: [] }, entryPath: '/' })).app;
    await until(() => app.locals.appsService.execute({ op: 'apps.list', actor: { accountId: owner.account.accountId, deviceId: owner.account.deviceId }, args: {} }).apps[0]?.state === 'ready');
    return { get app(){return app;}, server, origin, appId: registered.id, owner, member, director, outsider, call, close, sample, directory, setHandler(value) { handler = value; },
      reopenClient(person){person.client.dispose();person.client=clientFor(person.storage);},
      async restart(){await app.locals.closeServices();app=openRoot();await until(()=>runtime.status().connected&&app.locals.appsService.execute({op:'apps.list',actor:{accountId:owner.account.accountId,deviceId:owner.account.deviceId},args:{}}).apps[0]?.state==='ready');} };
  } catch (error) { await close(); throw error; }
}
