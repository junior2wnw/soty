import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
const { createAppsService } = await import(process.env.SOTY_APPS_TEST_MODULE ?? '../server/index.mjs');
import { createRelayIdentitySigner } from '../../human-identity/source-authority.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

const installed = process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY;
const serverModule = installed ? pathToFileURL(join(installed, 'dist', 'server.mjs')).href : null;
const { createRelayIdentityVerifier } = await import(serverModule ?? '../../source-app/server/relay-identity.mjs');
const { createSqliteRelayNonceStore } = await import(serverModule ?? '../../source-app/server/sqlite-relay-nonces.mjs');
const { createSqliteRelayIdentity } = await import(serverModule ?? '../../source-app/server/sqlite-relay-identity.mjs');
const { createRelayIdentityFetch } = await import(installed ? pathToFileURL(join(installed, 'dist', 'browser.mjs')).href : '../../source-app/browser/relay-identity-fetch.mjs');
// Exercise the exact module shipped in the generated single-file connector,
// as well as source. The full bundle's process/update path has separate tests.
const bundle = await readFile(new URL('../../../public/agent/soty-connector.mjs', import.meta.url), 'utf8');
const moduleStart = bundle.indexOf('// bundled connector module: ./agent-modules/local-apps.mjs\n');
const moduleEnd = bundle.indexOf('// bundled connector module:', moduleStart + 1);
assert.ok(moduleStart >= 0 && moduleEnd > moduleStart, 'connector release module is missing');
const bundledModule = bundle.slice(moduleStart, moduleEnd) + '\nexport { createLocalAppsRuntime };\n';
const { createLocalAppsRuntime: bundledRuntime } = await import('data:text/javascript;base64,' + Buffer.from(bundledModule).toString('base64'));

const secret = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(value).digest('hex');
async function until(check) {
  const end = Date.now() + 5000;
  while (!check()) { if (Date.now() >= end) throw Error('fixture_timeout'); await delay(10); }
}
function http(port, path, { origin, cookie, method = 'GET', headers = {}, body } = {}) {
  const url = new URL(origin);
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { host: url.host,
      ...(cookie ? { cookie } : {}), ...(method === 'GET' ? {} : { origin }), ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode,
        headers: res.headers, text: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); req.end(body);
  });
}
const memberProof=value=>({frameId:value.frameId,lease:value.lease,revision:value.revision});
async function eventStream(port,origin,grant){
  const value={data:[],closed:false},req=request({hostname:'127.0.0.1',port,path:'/events',headers:{host:new URL(origin).host,'x-soty-identity-request':'1','x-soty-window-grant':grant}},res=>{
    value.status=res.statusCode;res.on('data',data=>value.data.push(data.toString()));res.on('error',()=>{});res.on('close',()=>value.closed=true);
  });req.on('error',()=>value.closed=true);req.end();value.close=()=>req.destroy();await until(()=>value.data.length>0||value.closed);assert.equal(value.status,200);return value;
}

for (const [mode, makeRuntime] of [['source', createLocalAppsRuntime], ['released bundle', bundledRuntime]])
test(`window-bound Root identity and idempotency stay isolated through ${mode}`, { timeout: 20000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-identity-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const privateJwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const issuer = 'https://soty.example/human-identity';
  const profile = { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [privateJwk] } }) };
  const actualSigner = createRelayIdentitySigner({ profile });
  let gate, failSigner = false, service, runtime, legacyWire = false, legacyAuthSeen = false;
  const root = createServer((req, res) => service?.handleRequest(req, res) || res.writeHead(404).end());
  root.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => root.listen(0, '127.0.0.1', done));
  const port = root.address().port, rootOrigin = `http://127.0.0.1:${port}`;
  const owner = { accountId: 'alice', deviceId: 'alice-device' }, bob = { accountId: 'bob', deviceId: 'bob-device' };
  const revoked = new Set(), runtimeWidgets = new Map(), lockedWidgets = new Set(); let clockOffset = 0;
  service = createAppsService({ databasePath: join(directory, 'apps.sqlite'), shellOrigins: [rootOrigin],
    appOriginTemplate: `http://{appId}.localhost:${port}`, namedAppZone: `http://apps.localhost:${port}`,
    actorActive: actor => [owner, bob].some(person => person.accountId === actor?.accountId && person.deviceId === actor?.deviceId) && !revoked.has(actor.deviceId), now: () => Date.now() + clockOffset,
    canAccessCommunity: () => false, isGroupAdmin: () => false, authenticateConnector: async () => true,
    identitySignTimeoutMs: 300,
    resolveWidgetRuntime: id => runtimeWidgets.get(id) ?? null,
    checkWidgetLaunch: ({ widgetId }) => { assert.ok(runtimeWidgets.has(widgetId)); return true; },
    checkWidgetRuntime: ({ widgetId }) => !lockedWidgets.has(widgetId),
    signRelayIdentity: async input => { if (failSigner) throw Object.assign(Error('synthetic'), { code: 'app_identity_unavailable' });
      const proof = await actualSigner(input); if (gate) { gate.enter(); await gate.wait; } return proof; } });
  const identity = { linkId: 'link', hostDeviceId: 'host', connectorId: 'connector', name: 'fixture' };
  runtime = makeRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => {
    const ws = new WebSocket(url), send = ws.send.bind(ws);
    ws.send = data => { const frame = JSON.parse(data); if (legacyWire && frame.type === 'auth') {
      delete frame.capabilities.relayIdempotencyKey; legacyAuthSeen = true; return send(JSON.stringify(frame));
    } return send(data); };
    return ws;
  }, httpRequest: request,
    encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
  { serverUrl: rootOrigin, identity, token: 'fixture' });
  const servers = [], stores = [], businessStores = [];
  t.after(async () => { gate?.release(); runtime.stop(); service.close(); root.closeAllConnections();
    await new Promise(done => root.close(done)); for (const server of servers) { server.closeAllConnections(); await new Promise(done => server.close(done)); }
    for (const store of stores) store.close(); for (const close of businessStores) close();
    await rm(directory, { recursive: true, force: true }); });
  runtime.start(); await until(() => runtime.status().connected);
  const claim = await runtime.claim();
  const call = (op, args, actor = owner) => service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode: claim.claimCode });
  const apps = [];
  for (let index = 0; index < 2; index++) {
    let verifier; const seen = [], businessPath = join(directory, `business-${index}.sqlite`);
    let business = new DatabaseSync(businessPath);
    business.exec('CREATE TABLE records (issuer TEXT NOT NULL, subject TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(issuer,subject))');
    business.exec('CREATE TABLE receipts (issuer TEXT NOT NULL, subject TEXT NOT NULL, request_id TEXT NOT NULL, input_hash TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(issuer,subject,request_id))');
    businessStores.push(() => business.close());
    const upstream = createServer((req, res) => {
      if (req.method === 'HEAD') { res.writeHead(200).end(); return; }
      seen.push({ method: req.method, path: req.url, token: req.headers['x-soty-identity'], windowGrant: req.headers['x-soty-window-grant'], key: req.headers['idempotency-key'], authorization: req.headers.authorization });
      void (async () => {
        try { const person = await verifier(req);
          if(req.url==='/events'){
            res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-store'});res.write('data: admitted\n\n');
            const timer=setInterval(()=>res.write('data: next\n\n'),40);res.on('close',()=>clearInterval(timer));return;
          }
          if (req.url === '/upstream-503') { res.writeHead(503, { 'x-soty-identity-window': 'not-dispatched' }).end('app failure'); return; }
          if (req.method === 'GET' && req.url.startsWith('/receipt?')) {
            const key = new URL(req.url, 'http://fixture.invalid').searchParams.get('key');
            const receipt = business.prepare('SELECT count FROM receipts WHERE issuer=? AND subject=? AND request_id=?').get(person.issuer, person.subject, key);
            if (!receipt) { res.writeHead(404).end(); return; }
            res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count: receipt.count })); return;
          }
          if (req.method === 'POST' && req.url === '/idempotent') {
            const key = req.headers['idempotency-key']; if (typeof key !== 'string' || !key) { res.writeHead(400).end(); return; }
            const chunks = []; for await (const chunk of req) chunks.push(chunk);
            const raw = Buffer.concat(chunks).toString('utf8'), inputHash = hash(raw);
            let count; business.exec('BEGIN IMMEDIATE');
            try {
              const prior = business.prepare('SELECT input_hash,count FROM receipts WHERE issuer=? AND subject=? AND request_id=?').get(person.issuer, person.subject, key);
              if (prior && prior.input_hash !== inputHash) { business.exec('ROLLBACK'); res.writeHead(409).end(); return; }
              if (prior) count = prior.count;
              else {
                business.prepare('INSERT INTO records VALUES(?,?,1) ON CONFLICT(issuer,subject) DO UPDATE SET count=count+1').run(person.issuer, person.subject);
                count = business.prepare('SELECT count FROM records WHERE issuer=? AND subject=?').get(person.issuer, person.subject).count;
                business.prepare('INSERT INTO receipts VALUES(?,?,?,?,?)').run(person.issuer, person.subject, key, inputHash, count);
              }
              business.exec('COMMIT');
            } catch (error) { business.exec('ROLLBACK'); throw error; }
            if (['lost-reply','host-renew-lost-reply'].includes(key) && !seen.some(item => item.key === key && item.lost)) { seen.at(-1).lost = true; res.destroy(); return; }
            res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count })); return;
          }
          if (req.method === 'POST') business.prepare('INSERT INTO records VALUES(?,?,1) ON CONFLICT(issuer,subject) DO UPDATE SET count=count+1').run(person.issuer, person.subject);
          const count = business.prepare('SELECT count FROM records WHERE issuer=? AND subject=?').get(person.issuer, person.subject)?.count ?? 0;
          res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count })); }
        catch (error) { res.writeHead(error.status === 503 ? 503 : 401).end('identity unavailable'); }
      })();
    });
    await new Promise(done => upstream.listen(0, '127.0.0.1', done)); servers.push(upstream);
    const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      name: 'fixture ' + index, port: upstream.address().port, entryPath: '/', grants: { accountIds: [bob.accountId] } }).app;
    const noncePath = join(directory, `nonce-${index}.sqlite`);
    const makeIdentity = () => createSqliteRelayIdentity({ issuer, appId: app.id, databasePath: noncePath, fetch: async url => {
      assert.equal(new URL(url).href, issuer + '/jwks'); return new Response(JSON.stringify({ keys: [publicJwk] }), { headers: { 'content-type': 'application/json' } });
    } });
    let store = makeIdentity(); stores.push(store); verifier = store.verify;
    apps.push({ app, seen, get verifier() { return verifier; }, restartStore() { business.close(); business = new DatabaseSync(businessPath); },
      restartVerifier() { store.close(); store = makeIdentity(); stores[index] = store; verifier = store.verify; } });
  }
  await until(() => runtime.status().apps === 2);
  async function session(app, actor) {
    const result = call('apps.launch', { appId: app.id }, actor), url = new URL(result.launchUrl);
    const boot = await http(port, '/_soty/session', { origin: url.origin, method: 'POST',
      headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(boot.status, 200); return { origin: url.origin, cookie: boot.headers['set-cookie'][0].split(';')[0],
      actor, appId: app.id, domainId: result.entry.domainId, binding: result.launchBinding };
  }
  let aliceSession, bobSession, otherSession;
  await until(() => { try { aliceSession = call('apps.launch', { appId: apps[0].app.id }, owner); return true; } catch { return false; } });
  aliceSession = await session(apps[0].app, owner); bobSession = await session(apps[0].app, bob); otherSession = await session(apps[1].app, owner);
  const config = await http(port, '/_soty/relay-identity-config', { origin: aliceSession.origin });
  assert.equal(config.status, 200); assert.deepEqual(JSON.parse(config.text), { schema: 'soty.relay-identity-config.v1', shellOrigins: [rootOrigin] });
  assert.equal(config.headers['cache-control'], 'no-store');
  assert.equal((await http(port, '/_soty/relay-identity-config?wrong=1', { origin: aliceSession.origin })).status, 403);
  assert.equal(apps[0].seen.length, 0, 'configuration is served by Root, not forwarded to the application');
  const issue = session => call('apps.relay.identity', { appId: session.appId, domainId: session.domainId, launchBinding: session.binding }, session.actor).grant;
  const fetchApp = (session, method = 'GET', headers = {}, path = '/items?x=1', body) => http(port, path, { ...session, method,
    headers: { ...headers, 'x-soty-identity-request': '1', ...(session.actor ? { 'x-soty-window-grant': issue(session) } : {}) }, body });
  assert.equal((await http(port, '/items?x=1', aliceSession)).status, 401);
  assert.equal(apps[0].seen.at(-1).token, undefined, 'identity is never sent on an ordinary request');
  const alice = await fetchApp(aliceSession, 'POST', { 'x-soty-identity': 'browser-forgery' });
  assert.equal(alice.status, 200, JSON.stringify({ text: alice.text, upstreamSawProof: !!apps[0].seen.at(-1)?.token })); assert.deepEqual(JSON.parse(alice.text), { subject: 'alice', count: 1 });
  assert.equal(apps[0].seen.at(-1).windowGrant, undefined, 'window grants never reach the upstream application');
  assert.equal(alice.text.includes(apps[0].seen.at(-1).token), false);
  apps[0].restartStore();
  assert.deepEqual(JSON.parse((await fetchApp(aliceSession)).text), { subject: 'alice', count: 1 }, 'personal data survives closing and reopening the app store');
  const oldWindowGrant = issue(aliceSession);
  assert.throws(() => call('apps.relay.identity.close', { appId: aliceSession.appId, grant: oldWindowGrant }, bob), /app_identity_window_unavailable/u);
  const wrongApp = await http(port, '/items?x=1', { ...otherSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': oldWindowGrant } });
  assert.equal(wrongApp.status, 503, 'a grant for another app cannot select an actor');
  const mismatchedCookie = await http(port, '/items?x=1', { ...bobSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': oldWindowGrant } });
  assert.deepEqual(JSON.parse(mismatchedCookie.text), { subject: 'alice', count: 1 }, 'window grant, not the later account cookie, selects the actor');
  assert.deepEqual(call('apps.relay.identity.close', { appId: aliceSession.appId, grant: oldWindowGrant }, owner), { closed: true });
  const beforeClosed = apps[0].seen.length;
  const closedReply = await http(port, '/items?x=1', { ...bobSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': oldWindowGrant } });
  assert.equal(closedReply.status, 503); assert.equal(closedReply.headers['x-soty-identity-window'], 'not-dispatched');
  assert.equal(apps[0].seen.length, beforeClosed, 'a closed window grant never reaches the app');
  const fakeMarker = await fetchApp(aliceSession, 'GET', {}, '/upstream-503');
  assert.equal(fakeMarker.status, 503); assert.equal(fakeMarker.headers['x-soty-identity-window'], undefined, 'the upstream cannot forge Root\'s pre-dispatch marker');
  assert.deepEqual(JSON.parse((await fetchApp(bobSession)).text), { subject: 'bob', count: 0 });
  const revokedGrant = issue(bobSession), bobHeaders = { 'x-soty-identity-request': '1', 'x-soty-window-grant': revokedGrant };
  assert.equal((await http(port, '/items?x=1', { ...bobSession, headers: bobHeaders })).status, 200);
  const beforeRevoked = apps[0].seen.length; revoked.add(bob.deviceId);
  assert.equal((await http(port, '/items?x=1', { ...bobSession, headers: bobHeaders })).status, 401);
  assert.equal(apps[0].seen.length, beforeRevoked, 'device revocation overrides an unexpired window grant before dispatch');
  revoked.delete(bob.deviceId);
  assert.deepEqual(JSON.parse((await fetchApp(otherSession)).text), { subject: 'alice', count: 0 });
  const operation = { 'idempotency-key': 'same-action', authorization: 'must-not-reach-app' }, body = JSON.stringify({ item: 'bread' });
  const firstWrite = await fetchApp(aliceSession, 'POST', operation, '/idempotent', body);
  const repeatedWrite = await fetchApp(aliceSession, 'POST', operation, '/idempotent', body);
  assert.equal(firstWrite.status, 200); assert.equal(repeatedWrite.status, 200);
  assert.deepEqual(JSON.parse(firstWrite.text), { subject: 'alice', count: 2 });
  assert.deepEqual(JSON.parse(repeatedWrite.text), JSON.parse(firstWrite.text), 'same operation must use the app receipt');
  assert.equal(apps[0].seen.at(-1).key, 'same-action'); assert.equal(apps[0].seen.at(-1).authorization, undefined, 'other forbidden headers remain filtered');
  assert.equal((await fetchApp(aliceSession, 'POST', operation, '/idempotent', JSON.stringify({ item: 'changed' }))).status, 409);
  const bobWrite = await fetchApp(bobSession, 'POST', operation, '/idempotent', body);
  assert.deepEqual(JSON.parse(bobWrite.text), { subject: 'bob', count: 1 }, 'same key must not merge people');
  const otherWrite = await fetchApp(otherSession, 'POST', operation, '/idempotent', body);
  assert.deepEqual(JSON.parse(otherWrite.text), { subject: 'alice', count: 1 }, 'same key must not merge applications');
  const lostBefore = apps[0].seen.filter(value => value.key === 'lost-reply').length;
  const lost = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'lost-reply' }, '/idempotent', body);
  assert.equal(lost.status, 502); await delay(100);
  assert.equal(apps[0].seen.filter(value => value.key === 'lost-reply').length, lostBefore + 1, 'transport must not retry a lost response');
  const readback = await fetchApp(aliceSession, 'GET', {}, '/receipt?key=lost-reply');
  assert.deepEqual(JSON.parse(readback.text), { subject: 'alice', count: 3 }, 'read-only app receipt confirms the original write');
  assert.equal(apps[0].seen.filter(value => value.key === 'lost-reply').length, lostBefore + 1, 'readback does not send the write again');
  const recovered = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'lost-reply' }, '/idempotent', body);
  assert.deepEqual(JSON.parse(recovered.text), { subject: 'alice', count: 3 }, 'app receipt returns the original result without a second write');
  const priorSeen = apps[0].seen.length;
  const duplicate = await http(port, '/idempotent', { ...aliceSession, method: 'POST', headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': issue(aliceSession), 'idempotency-key': ['one', 'two'] } });
  assert.equal(duplicate.status, 400); assert.equal(apps[0].seen.length, priorSeen, 'duplicate keys must fail before reaching the app');
  const normalized = await http(port, '/folder/../items?x=1', { ...aliceSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': issue(aliceSession) } });
  assert.equal(normalized.status, 200); assert.equal(apps[0].seen.at(-1).path, '/items?x=1');
  const old = apps[0].seen.find(value => value.method === 'POST').token;
  const replay = { headers: { 'x-soty-identity': old }, rawHeaders: ['x-soty-identity', old], method: 'POST', url: '/items?x=1' };
  await assert.rejects(apps[0].verifier(replay), { code: 'soty_identity_invalid' });
  apps[0].restartVerifier();
  await assert.rejects(apps[0].verifier(replay), { code: 'soty_identity_invalid' }, 'restarting the verifier cannot revive a consumed proof');
  await assert.rejects(apps[1].verifier(replay), { code: 'soty_identity_invalid' });
  const fresh = await actualSigner({ appId: apps[0].app.id, accountId: 'alice', method: 'GET', path: '/items?x=1', expiresAt: Date.now() + 30000 });
  const changed = { ...replay, method: 'POST', headers: { 'x-soty-identity': fresh }, rawHeaders: ['x-soty-identity', fresh] };
  await assert.rejects(apps[0].verifier(changed), { code: 'soty_identity_invalid' });
  await assert.rejects(apps[0].verifier({ ...changed, headers: { 'x-soty-identity': 'unsigned-json' } }), { code: 'soty_identity_invalid' });

  let unchanged = apps[0].seen.length; failSigner = true;
  assert.equal((await fetchApp(aliceSession)).status, 503); failSigner = false;
  assert.equal(apps[0].seen.length, unchanged, 'signer errors finish without dispatch');
  let enterTimeout, releaseTimeout;
  gate = { entered: new Promise(done => { enterTimeout = done; }), wait: new Promise(done => { releaseTimeout = done; }),
    enter: () => enterTimeout(), release: () => releaseTimeout() };
  const timed = fetchApp(aliceSession); await gate.entered; assert.equal((await timed).status, 503);
  gate.release(); gate = null; await delay(30);
  assert.equal(apps[0].seen.length, unchanged, 'late signer completion cannot dispatch a timed-out request');
  let enter, release; gate = { entered: new Promise(done => { enter = done; }), wait: new Promise(done => { release = done; }), enter: () => enter(), release: () => release() };
  const cachedBob = issue(bobSession), before = apps[0].seen.length, pending = fetchApp(bobSession, 'POST'); await gate.entered;
  call('apps.update', { appId: apps[0].app.id, grants: {} }); gate.release(); gate = null;
  const denied = await pending; assert.equal(denied.status, 403); assert.equal(apps[0].seen.length, before, 'revocation during signing never dispatches the write');
  const afterGrantRevoke = await http(port, '/items?x=1', { ...bobSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': cachedBob } });
  assert.ok([403, 503].includes(afterGrantRevoke.status)); assert.equal(apps[0].seen.length, before, 'revoked app access defeats a still-unexpired grant');

  const domains = call('apps.domains.get', { appId: apps[0].app.id });
  const aliasId = call('apps.domains.claim', { appId: apps[0].app.id, slug: 'relay-auth-fixture',
    expectedDomainsRevision: domains.revision, requestId: 'claim-relay-auth' }).receipt.domainId;
  const alias = call('apps.domains.get', { appId: apps[0].app.id }).domains.find(value => value.id === aliasId);
  const publication = call('apps.publication.get', { appId: apps[0].app.id });
  call('apps.publication.update', { appId: apps[0].app.id, requestId: 'publish-relay-auth', expectedPolicyEpoch: publication.policyEpoch,
    expectedTargetRevision: publication.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [alias.id],
    exposureAck: { scope: 'whole-port', targetRevision: publication.target.revision, targetDigest: publication.target.digest, profile: publication.target.profile } });
  const publicBefore = apps[0].seen.length;
  assert.equal((await fetchApp({ origin: alias.origin })).status, 503);
  assert.equal(apps[0].seen.length, publicBefore, 'anonymous access never receives an identity proof');

  {
  const managed=call('apps.launch',{appId:apps[0].app.id,path:'/deep-view',hostSession:true});
  const managedBoot=await http(port,'/_soty/session',{origin:managed.runtimeOrigin,method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ticket:new URL(managed.launchUrl).hash.slice(1)})});
  assert.equal(managedBoot.status,200);
  const managedCookie=managedBoot.headers['set-cookie'].map(value=>value.split(';')[0]).join('; ');
  const managedCheck=await http(port,'/_soty/session',{origin:managed.runtimeOrigin,cookie:managedCookie,headers:{'x-soty-boot-check':JSON.parse(managedBoot.text).sessionCheck}});assert.equal(managedCheck.status,200);
  const mainArgs={appId:apps[0].app.id,domainId:managed.entry.domainId,launchBinding:managed.launchBinding,instanceId:'main',runtimeOrigin:managed.runtimeOrigin,hostSession:memberProof(managed.hostSession)};
  const{hostSession:_mainMember,...withoutMember}=mainArgs;
  assert.throws(()=>call('apps.relay.identity',withoutMember),/app_identity_window_unavailable/u,'managed main cannot select an arbitrary physical member');
  assert.throws(()=>call('apps.relay.identity',{...mainArgs,hostSession:{...mainArgs.hostSession,unexpected:true}}),/app_identity_window_unavailable/u);
  const mainGrant=call('apps.relay.identity',mainArgs).grant,mainHeaders={'x-soty-identity-request':'1','x-soty-window-grant':mainGrant};
  const whoami=await http(port,'/items?x=1',{origin:managed.runtimeOrigin,headers:mainHeaders});assert.equal(whoami.status,200);assert.equal(JSON.parse(whoami.text).subject,owner.accountId);
  const mainEvents=await eventStream(port,managed.runtimeOrigin,mainGrant),beforeEvents=mainEvents.data.length;
  assert.equal((await http(port,'/items?x=1',{origin:managed.entry.origin,headers:mainHeaders})).status,503,'a main-runtime grant never authorizes canonical legacy origin');
  assert.throws(()=>call('apps.relay.identity',{...mainArgs,runtimeOrigin:managed.entry.origin}),/app_identity_window_unavailable/);
  const canonicalGrant=issue({...aliceSession,binding:managed.launchBinding});
  assert.equal((await http(port,'/items?x=1',{origin:managed.runtimeOrigin,headers:{'x-soty-identity-request':'1','x-soty-window-grant':canonicalGrant}})).status,503,'legacy canonical grant cannot select a managed main runtime');
  const lostKey='host-renew-lost-reply',lostManaged=await http(port,'/idempotent',{origin:managed.runtimeOrigin,method:'POST',headers:{...mainHeaders,'idempotency-key':lostKey},body});
  assert.equal(lostManaged.status,502);assert.equal(apps[0].seen.filter(value=>value.key===lostKey).length,1);
  const proof=managed.hostSession,renewalId='managed_main_transport_renew';
  const renewal=call('apps.session.renew',{appId:apps[0].app.id,frameId:proof.frameId,lease:proof.lease,revision:proof.revision,requestId:renewalId,parentOrigin:rootOrigin,currentEntryPath:managed.entry.path});
  const renewed=await http(port,'/_soty/session',{origin:managed.runtimeOrigin,cookie:managedCookie,method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ticket:new URL(renewal.launchUrl).hash.slice(1)})});assert.equal(renewed.status,200);
  const cookies=new Map(managedCookie.split('; ').map(value=>{const at=value.indexOf('=');return[value.slice(0,at),value.slice(at+1)];}));
  for(const field of renewed.headers['set-cookie']){const part=field.split(';')[0],at=part.indexOf('=');if(/Max-Age=0/.test(field))cookies.delete(part.slice(0,at));else cookies.set(part.slice(0,at),part.slice(at+1));}
  const renewedCookie=[...cookies].map(([name,value])=>name+'='+value).join('; ');
  assert.equal((await http(port,'/_soty/session',{origin:managed.runtimeOrigin,cookie:renewedCookie,headers:{'x-soty-boot-check':JSON.parse(renewed.text).sessionCheck}})).status,200);
  call('apps.session.confirm',{appId:apps[0].app.id,frameId:renewal.hostSession.frameId,lease:renewal.hostSession.lease,revision:renewal.hostSession.revision,requestId:renewalId});
  assert.throws(()=>call('apps.relay.identity',mainArgs),/app_host_session_conflict/u,'stale member proof cannot issue a new grant after CAS');
  await until(()=>mainEvents.data.length>beforeEvents);assert.equal(mainEvents.closed,false,'member CAS keeps the exact already-authorized identity SSE');
  assert.equal((await http(port,'/items?x=1',{origin:managed.runtimeOrigin,headers:mainHeaders})).status,200,'original bounded grant survives own member CAS');
  assert.equal(apps[0].seen.filter(value=>value.path==='/events').length,1,'CAS must not replay identity Source SSE');
  apps[0].restartStore();
  const currentMainArgs={...mainArgs,hostSession:memberProof(renewal.hostSession)},afterRenewGrant=call('apps.relay.identity',currentMainArgs).grant;
  const durableReceipt=await http(port,'/receipt?key='+lostKey,{origin:managed.runtimeOrigin,headers:{'x-soty-identity-request':'1','x-soty-window-grant':afterRenewGrant}});
  assert.equal(durableReceipt.status,200);assert.equal(JSON.parse(durableReceipt.text).subject,owner.accountId);
  assert.equal(apps[0].seen.filter(value=>value.key===lostKey).length,1,'transport renewal does not replay the uncertain Source POST');
  const duplicateManaged=call('apps.launch',{appId:apps[0].app.id,path:'/deep-view',hostSession:true}),duplicateBoot=await http(port,'/_soty/session',{
    origin:duplicateManaged.runtimeOrigin,method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ticket:new URL(duplicateManaged.launchUrl).hash.slice(1)})});
  assert.equal(duplicateBoot.status,200);const duplicateCookie=duplicateBoot.headers['set-cookie'].filter(value=>!value.includes('Max-Age=0')).map(value=>value.split(';')[0]).join('; ');
  assert.equal((await http(port,'/_soty/session',{origin:duplicateManaged.runtimeOrigin,cookie:duplicateCookie,headers:{'x-soty-boot-check':JSON.parse(duplicateBoot.text).sessionCheck}})).status,200);
  const duplicateGrant=call('apps.relay.identity',{...currentMainArgs,hostSession:memberProof(duplicateManaged.hostSession)}).grant;
  const duplicateEvents=await eventStream(port,duplicateManaged.runtimeOrigin,duplicateGrant),duplicateEventsBefore=duplicateEvents.data.length;
  assert.equal(call('apps.session.close',{appId:apps[0].app.id,...memberProof(renewal.hostSession)}).closed,true);
  assert.throws(()=>call('apps.relay.identity',currentMainArgs),/app_host_session_closed/u,'closed member proof cannot issue new grant despite another same-family member');
  await until(()=>mainEvents.closed);assert.equal(duplicateEvents.closed,false);
  const beforeClosed=apps[0].seen.length;
  assert.equal((await http(port,'/items?x=1',{origin:managed.runtimeOrigin,headers:mainHeaders})).status,503,'closing A invalidates its original grant');
  assert.equal((await http(port,'/items?x=1',{origin:managed.runtimeOrigin,headers:{'x-soty-identity-request':'1','x-soty-window-grant':afterRenewGrant}})).status,503,'closing A invalidates its later grant');
  assert.equal(apps[0].seen.length,beforeClosed,'closed member grants cannot dispatch Source requests');
  assert.equal((await http(port,'/items?x=1',{origin:duplicateManaged.runtimeOrigin,headers:{'x-soty-identity-request':'1','x-soty-window-grant':duplicateGrant}})).status,200,'B independent grant survives A close');
  await until(()=>duplicateEvents.data.length>duplicateEventsBefore);duplicateEvents.close();mainEvents.close();
  }

  // Real HTTP/signing/connector; routing metadata is an explicitly synthetic host-port fixture here.
  const current = call('apps.launch', { appId: apps[0].app.id }), logical = 'widget-' + randomUUID();
  runtimeWidgets.set(logical, { widgetId: logical, revision: 1, state: 'active', appId: apps[0].app.id,
    entry: { domainId: current.entry.domainId, path: current.entry.path },
    source: { revision: current.launchBinding.targetRevision, digest: current.launchBinding.targetDigest, profile: current.launchBinding.profile } });
  const widgetLaunch = call('apps.launch', { appId: apps[0].app.id, instanceId: logical });
  const identityArgs = { appId: apps[0].app.id, domainId: current.entry.domainId, launchBinding: widgetLaunch.launchBinding,
    instanceId: logical, runtimeOrigin: widgetLaunch.runtimeOrigin };
  const grant = call('apps.relay.identity', identityArgs).grant;
  const widgetRequest = { origin: widgetLaunch.runtimeOrigin, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': grant } };
  assert.equal((await http(port, '/items?x=1', widgetRequest)).status, 200);
  const beforeSwaps = apps[0].seen.length;
  assert.equal((await http(port, '/items?x=1', { ...widgetRequest, origin: current.entry.origin })).status, 503);
  assert.equal((await http(port, '/items?x=1', { origin: widgetLaunch.runtimeOrigin,
    headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': issue({ ...aliceSession, binding: current.launchBinding }) } })).status, 503);
  assert.equal(apps[0].seen.length, beforeSwaps);
  assert.throws(() => call('apps.relay.identity', { ...identityArgs, runtimeOrigin: current.entry.origin }), /app_widget_runtime_changed/u);
  const { runtimeOrigin: _runtimeOrigin, ...unpaired } = identityArgs;
  assert.throws(() => call('apps.relay.identity', unpaired), /app_identity_window_unavailable/u);
  lockedWidgets.add(logical); service.invalidateWidget({ widgetId: logical });
  assert.throws(() => call('apps.relay.identity', identityArgs), /apps_widget_runtime_denied/u);
  assert.equal((await http(port, '/items?x=1', widgetRequest)).status, 503);
  assert.equal(apps[0].seen.length, beforeSwaps, 'relocked widget window never reaches upstream');
  runtime.stop(); legacyWire = true; runtime.start();
  await until(() => legacyAuthSeen && runtime.status().connected && runtime.status().apps === 2);
  await until(() => { try { call('apps.launch', { appId: apps[0].app.id }); return true; } catch { return false; } });
  aliceSession = await session(apps[0].app, owner);
  const legacyBefore = apps[0].seen.length;
  const unsupported = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'old-connector' }, '/idempotent', body);
  assert.equal(unsupported.status, 503);
  assert.match(unsupported.text, /app_idempotency_key_unavailable/u); assert.match(unsupported.text, /Обновите коннектор/u);
  assert.equal(apps[0].seen.length, legacyBefore, 'a connector that does not advertise support must not receive the request');
  const unsupportedPage = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'old-connector', accept: 'text/html' }, '/idempotent', body);
  assert.equal(unsupportedPage.status, 503); assert.match(unsupportedPage.text, /Обновите коннектор/u);
  assert.equal(apps[0].seen.length, legacyBefore, 'HTML error must also refuse before delivery');
  assert.equal((await fetchApp(aliceSession)).status, 200, 'requests without an idempotency key still work on the older protocol');
  const expiring = issue(aliceSession), beforeExpiry = apps[0].seen.length; clockOffset += 31000;
  const expired = await http(port, '/items?x=1', { ...aliceSession, headers: { 'x-soty-identity-request': '1', 'x-soty-window-grant': expiring } });
  assert.ok([403, 503].includes(expired.status)); assert.equal(apps[0].seen.length, beforeExpiry, 'expired grants never reach the app');
});

test('browser helper cannot send the request flag to another origin or follow a redirect', async () => {
  let calls = 0, captured, receive;
  const parent = { postMessage(data, origin) { assert.equal(origin, 'https://shell.example'); queueMicrotask(() => receive({ source: parent, origin, data: { schema: data.schema, type: 'grant', requestId: data.requestId, grant: 'A'.repeat(43) } })); } };
  const view = { parent, crypto: { randomUUID }, addEventListener(_type, fn) { receive = fn; }, removeEventListener() {}, setTimeout, clearTimeout };
  const sotyFetch = createRelayIdentityFetch({ origin: 'https://app.example', parentOrigin: 'https://shell.example', view,
    fetch: async (path, options) => { calls++; captured = { path, options }; return new Response(); } });
  await sotyFetch('/items?x=1', { headers: { accept: 'application/json' } });
  assert.equal(captured.path, '/items?x=1'); assert.equal(captured.options.headers.get('x-soty-identity-request'), '1');
  assert.equal(captured.options.credentials, 'same-origin'); assert.equal(captured.options.redirect, 'error');
  assert.equal(captured.options.headers.get('x-soty-window-grant'), 'A'.repeat(43));
  assert.throws(() => sotyFetch('https://other.example/items'), /invalid_soty_request/u);
  assert.throws(() => sotyFetch('/items', { mode: 'no-cors' }), /invalid_soty_request/u);
  assert.throws(() => sotyFetch('/items', { redirect: 'follow' }), /invalid_soty_request/u); assert.equal(calls, 1);
});

test('verifier rejects expired, foreign-issuer and duplicate assertions', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-verifier-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 }), issuer = 'https://soty.example/human-identity';
  const jwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  let now = Date.now();
  const signer = createRelayIdentitySigner({ profile: { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [jwk] } }) }, clock: () => now });
  const appId = 'app-' + '1'.repeat(32), fetcher = async () => new Response(JSON.stringify({ keys: [publicJwk] }));
  const store = createSqliteRelayNonceStore({ databasePath: join(directory, 'nonces.sqlite'), clock: () => now });
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const verifier = createRelayIdentityVerifier({ issuer, appId, fetch: fetcher, clock: () => now, consume: store.consume });
  const foreign = createRelayIdentityVerifier({ issuer: 'https://other.example/human-identity', appId, fetch: fetcher, clock: () => now, consume: store.consume });
  assert.throws(() => createRelayIdentityVerifier({ issuer, appId, fetch: fetcher }), { code: 'soty_identity_store_unavailable' });
  const unavailable = createRelayIdentityVerifier({ issuer, appId, fetch: fetcher, clock: () => now, consume() { throw Error('disk failure'); } });
  const token = await signer({ appId, accountId: 'alice', method: 'GET', path: '/items', expiresAt: now + 30000 });
  const req = { method: 'GET', url: '/items', headers: { 'x-soty-identity': token }, rawHeaders: ['x-soty-identity', token] };
  await assert.rejects(foreign(req), { code: 'soty_identity_invalid' });
  await assert.rejects(unavailable(req), { code: 'soty_identity_store_unavailable', status: 503 });
  await assert.rejects(verifier({ ...req, rawHeaders: [...req.rawHeaders, 'X-Soty-Identity', token] }), { code: 'soty_identity_invalid' });
  now += 11000;
  await assert.rejects(verifier(req), { code: 'soty_identity_invalid' });
});
