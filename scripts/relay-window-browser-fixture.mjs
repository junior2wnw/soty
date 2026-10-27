// Local synthetic-only browser fixture for the candidate window-bound relay.
import { createServer, request } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const tree = fileURLToPath(new URL('../', import.meta.url));
const get = name => import(pathToFileURL(join(tree, name)).href);
const { createAppsService } = await get('modules/apps/server/index.mjs');
const { createRelayIdentitySigner } = await get('modules/human-identity/source-authority.mjs');
const { createSqliteRelayIdentity } = await get('modules/source-app/server/sqlite-relay-identity.mjs');
const { createLocalAppsRuntime } = await get('scripts/agent-modules/local-apps.mjs');
const { createConnectService } = await get('modules/connect/server/index.mjs');
const { createClientWithStorage } = await get('modules/connect/browser/client.mjs');
const dir = await mkdtemp(join(tmpdir(), 'soty-switch-fixture-'));
const hash = value => createHash('sha256').update(value).digest('hex'), secret = () => randomBytes(32).toString('base64url');
const keys = generateKeyPairSync('rsa', { modulusLength: 2048 });
const jwk = { ...keys.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
const publicJwk = { ...keys.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
const issuer = 'https://soty.example/human-identity';
const signRelayIdentity = createRelayIdentitySigner({ profile: { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [jwk] } }) } });
const sdk = await readFile(join(tree, 'modules/source-app/browser/relay-identity-fetch.mjs'));
const bridgeSource = await readFile(join(tree, 'src/world/app-relay-identity-bridge.mjs'));
const tlsKey = process.env.SOTY_FIXTURE_TLS_KEY, tlsCertificate = process.env.SOTY_FIXTURE_TLS_CERT;
if (!!tlsKey !== !!tlsCertificate) throw Error('fixture_tls_configuration');
const shellHost = tlsKey ? 'soty.lvh.me' : '127.0.0.1';
const business = new DatabaseSync(join(dir, 'business.sqlite'));
business.exec('CREATE TABLE records (person TEXT PRIMARY KEY, value TEXT NOT NULL); CREATE TABLE receipts (person TEXT, request_id TEXT, PRIMARY KEY(person,request_id))');
let people = {}, clients = {}, service, connect, appId, verifier, runtime, protectedCalls = 0, writes = 0;
const source = createServer((req, res) => {
  if (req.method === 'HEAD') { res.writeHead(200).end(); return; }
  if (req.url === '/sdk-browser.mjs') { res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' }).end(sdk); return; }
  if (req.url === '/subject' || req.url === '/write') {
    protectedCalls++;
    void verifier.verify(req).then(person => {
      const name = person.subject === people.alice.accountId ? 'alice' : person.subject === people.bob.accountId ? 'bob' : 'unexpected';
      if (req.url === '/write') {
        const key = req.headers['idempotency-key']; if (typeof key !== 'string' || !key) return res.writeHead(400).end();
        business.exec('BEGIN IMMEDIATE');
        try { if (!business.prepare('SELECT 1 FROM receipts WHERE person=? AND request_id=?').get(name, key)) {
          business.prepare('UPDATE records SET value=? WHERE person=?').run(name.toUpperCase() + '_SAVED', name);
          business.prepare('INSERT INTO receipts VALUES (?,?)').run(name, key); writes++; }
          business.exec('COMMIT'); } catch (error) { business.exec('ROLLBACK'); throw error; }
      }
      return res.writeHead(200, { 'content-type': 'text/plain' }).end(name + ':' + business.prepare('SELECT value FROM records WHERE person=?').get(name)?.value);
    })
      .catch(error => res.writeHead(error.status || 401).end('refused')); return;
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }).end(`<!doctype html><html><body>
    <p>Синтетическое приложение</p><button id="read">Проверить субъект</button><button id="write">Записать</button><output id="subject">Пока нет запроса</output>
    <script type="module">import {createRelayIdentityFetch} from '/sdk-browser.mjs';const sotyFetch=createRelayIdentityFetch();async function show(path,options){try{const r=await sotyFetch(path,options);if(!r.ok)throw Error('unavailable');document.querySelector('#subject').textContent=await r.text()}catch{document.querySelector('#subject').textContent='Вход недоступен'}}document.querySelector('#read').onclick=()=>show('/subject');document.querySelector('#write').onclick=()=>show('/write',{method:'POST',headers:{'idempotency-key':'fixture-write-v1'}})</script>
  </body></html>`);
});
const handleRoot = (req, res) => {
  if (req.headers.host?.startsWith(shellHost + ':')) {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.pathname === '/fixture') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }).end(`<!doctype html><html><body><p>Только синтетическая проверка</p>
        <button id="alice">Открыть Alice</button><button id="bob">Открыть Bob</button>
        <iframe title="Alice" id="alice-frame" referrerpolicy="no-referrer"></iframe><iframe title="Bob" id="bob-frame" referrerpolicy="no-referrer"></iframe>
        <script type="module">import {createRelayIdentityBridge} from '/fixture/bridge.mjs';for(const who of ['alice','bob']){let meta=null;const frame=document.querySelector('#'+who+'-frame');
          const call=async(op,data)=>{const r=await fetch('/fixture/'+op+'?who='+who,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(data)});if(!r.ok)throw Error('unavailable');return r.json()};
          const bridge=createRelayIdentityBridge({view:window,isCurrent:()=>true,getFrame:()=>frame,getEntry:()=>meta?.entry??null,getBinding:()=>meta?.binding??null,request:data=>call('grant',data),close:data=>call('close',data)});
          document.querySelector('#'+who).onclick=async()=>{await bridge.clear();meta=await(await fetch('/fixture/launch?who='+who)).json();frame.src=meta.url;bridge.resume()}}
        </script>
      </body></html>`); return;
    }
    if (url.pathname === '/fixture/launch' && people[url.searchParams.get('who')]) {
      const who = url.searchParams.get('who');
      void clients[who].extension('apps.launch', { appId, expectedAccountId: people[who].accountId })
        .then(reply => res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }).end(JSON.stringify({ url: reply.launchUrl, entry: reply.entry, binding: reply.launchBinding })))
        .catch(() => res.writeHead(503).end()); return;
    }
    if (url.pathname === '/fixture/bridge.mjs') { res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' }).end(bridgeSource); return; }
    if (['/fixture/grant', '/fixture/close'].includes(url.pathname) && people[url.searchParams.get('who')] && req.method === 'POST') {
      const who = url.searchParams.get('who'), chunks = [];
      req.on('data', chunk => chunks.push(chunk)); req.on('end', () => {
        try { const args = JSON.parse(Buffer.concat(chunks).toString());
          void clients[who].extension(url.pathname.endsWith('grant') ? 'apps.relay.identity' : 'apps.relay.identity.close', { ...args, expectedAccountId: people[who].accountId })
            .then(reply => res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }).end(JSON.stringify(reply)))
            .catch(() => res.writeHead(403).end()); }
        catch { res.writeHead(400).end(); }
      }); return;
    }
    if (url.pathname === '/fixture/state') { res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ protectedCalls, writes })); return; }
    if (url.pathname === '/fixture/revoke-bob') { void clients.bob2.revokeDevice(people.bob.deviceId).then(() => res.writeHead(200).end('revoked')).catch(() => res.writeHead(409).end('not revoked')); return; }
    if (url.pathname === '/fixture/restart' && req.method === 'POST') {
      void (async () => { runtime.stop(); connect.close(); service.close(); service = makeApps(); connect = makeConnect(); runtime.start();
        await until(() => runtime.status().connected && runtime.status().apps === 1); res.writeHead(200).end('restarted'); })()
        .catch(() => res.writeHead(503).end('restart failed')); return;
    }
    res.writeHead(404).end(); return;
  }
  if (!service?.handleRequest(req, res)) res.writeHead(404).end();
};
const root = tlsKey ? createHttpsServer({ key: await readFile(tlsKey), cert: await readFile(tlsCertificate) }, handleRoot) : createServer(handleRoot);
root.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
await Promise.all([root, source].map(server => new Promise(done => server.listen(0, '127.0.0.1', done))));
const port = root.address().port, rootOrigin = `${tlsKey ? 'https' : 'http'}://${shellHost}:${port}`;
const makeApps = () => createAppsService({ databasePath: join(dir, 'apps.sqlite'), shellOrigins: [rootOrigin],
  appOriginTemplate: tlsKey ? `https://{appId}.soty.lvh.me:${port}` : `http://{appId}.localhost:${port}`, actorActive: actor => connect?.isActorActive(actor) === true,
  canAccessCommunity: () => false, isGroupAdmin: () => false, authenticateConnector: async () => true, signRelayIdentity });
const makeConnect = () => createConnectService({ databasePath: join(dir, 'connect.sqlite'), projectId: 'soty', allowedOrigins: [rootOrigin], extensions: [service] });
service = makeApps(); connect = makeConnect();
function memory() { let value; return { async read() { return structuredClone(value ?? null); }, async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
  async compareAndSwap(revision, candidate) { if (value.localRevision !== revision) throw Error('fixture_revision'); value = structuredClone(candidate); return structuredClone(value); } }; }
function client() { return createClientWithStorage({ projectId: 'soty', endpoint: rootOrigin + '/synthetic-rpc',
  fetch: async (_url, options) => { const result = await connect.handle({ ...JSON.parse(options.body), origin: rootOrigin }); return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400 }); } }, memory()); }
clients = { alice: client(), bob: client(), bob2: client() };
people = { alice: await clients.alice.bootstrap('Alice'), bob: await clients.bob.bootstrap('Bob') };
business.prepare('INSERT INTO records VALUES (?,?)').run('alice', 'ALICE_DATA'); business.prepare('INSERT INTO records VALUES (?,?)').run('bob', 'BOB_DATA');
if (people.alice.accountId === people.bob.accountId || people.alice.deviceId === people.bob.deviceId) throw Error('fixture_people_not_independent');
const enroll = await clients.bob2.startEnrollment('Bob second device'); await clients.bob.approveEnrollment(enroll.requestId);
const enrollment = await clients.bob2.previewEnrollment(enroll.requestId); await clients.bob2.finishEnrollment(enroll.requestId, enrollment.account.accountId);
const host = { linkId: 'fixture-link', hostDeviceId: 'fixture-host', connectorId: 'fixture-connector', name: 'fixture' };
runtime = createLocalAppsRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => new WebSocket(url), httpRequest: request,
  encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { serverUrl: rootOrigin, identity: host, token: 'synthetic-token' });
runtime.start();
async function until(fn) { const end = Date.now() + 8000; while (!fn()) { if (Date.now() > end) throw Error('fixture_timeout'); await new Promise(r => setTimeout(r, 20)); } }
await until(() => runtime.status().connected);
const claim = await runtime.claim();
await clients.alice.extension('apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode: claim.claimCode });
const app = (await clients.alice.extension('apps.register', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, name: 'Synthetic', port: source.address().port, entryPath: '/', grants: { accountIds: [people.bob.accountId] } })).app;
appId = app.id;
verifier = createSqliteRelayIdentity({ issuer, appId, databasePath: join(dir, 'nonce.sqlite'), fetch: async () => new Response(JSON.stringify({ keys: [publicJwk] })) });
await until(() => runtime.status().apps === 1);
const deadline = Date.now() + 8000; while (true) { try { await clients.alice.extension('apps.launch', { appId }); break; } catch { if (Date.now() > deadline) throw Error('fixture_binding_timeout'); await new Promise(r => setTimeout(r, 20)); } }
process.stdout.write(`READY ${rootOrigin}/fixture\n`);
async function close() {
  runtime.stop(); for (const client of Object.values(clients)) client.dispose(); connect.close(); service.close(); verifier.close(); business.close(); root.closeAllConnections(); source.closeAllConnections();
  await Promise.all([root, source].map(server => new Promise(done => server.close(done))));
  if (dirname(resolve(dir)) === resolve(tmpdir()) && basename(dir).startsWith('soty-switch-fixture-')) await rm(dir, { recursive: true, force: true, maxRetries: 5 });
}
process.on('SIGINT', () => { void close().finally(() => process.exit(0)); });
