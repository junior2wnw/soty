import { createServer, request } from 'node:http';
import { createHash, randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createServer as createViteServer } from 'vite';
import { createHttpApp } from '../../server/http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../agent-modules/local-apps.mjs';
import { createHumanBffFixture } from '../../modules/human-identity/examples/bff.mjs';

// Synthetic fixture only. Helpers deliberately use an actual signed owner; they
// are not production authentication and must never be installed on a live host.
const root = fileURLToPath(new URL('../../', import.meta.url));
const frontendOrigin = 'http://127.0.0.1:5237', backendOrigin = 'http://127.0.0.1:5397';
const folder = await mkdtemp(join(tmpdir(), 'soty-feedback-browser-')), dataDir = join(folder, 'data'), dist = join(folder, 'dist');
await mkdir(dist); await writeFile(join(dist, 'index.html'), await readFile(join(root, 'index.html'), 'utf8'));
const sockets = new Set(), clients = [], granted = new Set();
let app, runtime, vite, fixtureAppId, owner, lossArmed = false, generation = 0, stopping = false;
let reviewScenario = 'public', reviewRequests = 0, reviewCredentialRequests = 0;
let humanLossArmed = false, humanHoldArmed = false, releaseHumanAck = null, humanCompletePosts = 0;
const humanDecisionIntents = [], humanRps = [];
const reviewSubjects = [
  { kind: 'app', id: 'subject_'+'1'.repeat(20), type: 'product', title: 'Тестовое приложение', count: 2, average: 4.5 },
  { kind: 'project', id: 'subject_'+'2'.repeat(20), type: 'project', title: 'Тестовый проект', count: 4, average: 4.75 },
  { kind: 'person', id: 'subject_'+'3'.repeat(20), type: 'profile', title: 'Тестовый профиль', count: 1, average: 3 },
];
// Independent public API 1.1 network stub. All authors and UGC are synthetic.
const reviewsProvider = createServer((req, res) => void (async () => {
  reviewRequests++; if (req.headers.cookie || req.headers.authorization) reviewCredentialRequests++;
  res.setHeader('access-control-allow-origin', '*'); res.setHeader('cache-control', 'no-store'); res.setHeader('content-type', 'application/json');
  const url = new URL(req.url, 'http://127.0.0.1');
  const match = /^\/api\/public\/v1\/subjects\/(subject_[0-9a-f]{20})(\/reviews)?$/.exec(url.pathname), subject = reviewSubjects.find(value => value.id === match?.[1]);
  if (req.method !== 'GET' || !subject) { res.writeHead(404).end('{}'); return; }
  const scenario = reviewScenario;
  if (scenario === 'slow') await pause(1500);
  if (res.destroyed) return;
  if (scenario === 'unavailable' || scenario === 'partial' && match[2]) { res.writeHead(503).end('{}'); return; }
  if (scenario === 'redirect') { res.writeHead(302, { location: 'http://127.0.0.1:5397/__fixture/blocked-provider-redirect' }).end(); return; }
  if (scenario === 'large') { res.end(JSON.stringify({ padding: 'x'.repeat(530000) })); return; }
  const empty = scenario === 'empty', count = empty ? 0 : subject.count, stamp = '2026-10-06T12:00:00.000Z';
  if (!match[2]) {
    const distribution = subject.kind === 'person' ? { 1:0, 2:0, 3:1, 4:0, 5:0 } : { 1:0, 2:0, 3:0, 4:1, 5:1 };
    res.end(JSON.stringify({ apiVersion:'1.1', subject:{ id:scenario === 'mismatch' ? 'subject_'+'f'.repeat(20) : subject.id, entityType:subject.type, title:subject.title, publication:{status:'published'} },
      rating:{ average:empty?null:subject.average, count, distribution:empty?{1:0,2:0,3:0,4:0,5:0}:distribution, distributionCount:empty?0:subject.kind==='person'?1:2,
        distributionComplete:subject.kind!=='project', updatedAt:stamp, provenance:{kind:subject.kind==='project'?'imported':'native',source:'other'} },
      counts:{publishedItems:empty?0:7,publishedReviews:empty?0:2,discussionItems:empty?0:5}, links:{publicPage:`/subjects/${subject.id}`} })); return;
  }
  const second = url.searchParams.has('cursor');
  const item = { id:second?'review_second':'review_shared',type:'review',body:'Синтетический отзыв · '+subject.kind+' · <img src=x onerror=alert(1)>',
    author:{name:'Вымышленный автор',verified:subject.kind==='person'},rating:subject.kind==='person'?3:5,
    createdAt:stamp,updatedAt:stamp,provenance:{kind:subject.kind==='project'?'imported':'native',source:'other'},moderation:{status:'published',publishedAt:stamp},mediaUrls:[],reactions:{useful:0,notUseful:0},effectiveTags:[] };
  res.end(JSON.stringify({apiVersion:'1.1',subjectId:subject.id,items:empty?[]:[item],page:{count:empty?0:1,total:empty?0:2,hasMore:!empty&&!second,nextCursor:empty||second?null:'fixture.cursor_2'}}));
})().catch(() => { if (!res.headersSent) res.writeHead(500); res.end('{}'); }));
const pause = ms => new Promise(done => setTimeout(done, ms));
async function until(check) { const deadline = Date.now() + 8000; do { const value = await check(); if (value) return value; await pause(20); } while (Date.now() < deadline); throw new Error('fixture_readiness_timeout'); }
function memory() { let value; return { async read() { return structuredClone(value ?? null); }, async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); }, async compareAndSwap(revision, candidate) { if (value.localRevision !== revision) throw new Error('fixture_storage_conflict'); value = structuredClone(candidate); return structuredClone(value); } }; }
const source = createServer((_req, res) => { res.setHeader('content-type', 'text/html; charset=utf-8'); res.end('<!doctype html><html lang="ru"><meta charset="utf-8"><style>body{font:16px system-ui;padding:18px;background:#f7f8f4;color:#17212b}input{padding:10px;max-width:90%}</style><label>Состояние тестового приложения <input id="runtime-state" value="initial"></label><p>Настоящий HTTP runtime. Только вымышленные данные.</p></html>'); });
const backend = createServer((req, res) => {
  if (req.url?.startsWith('/__fixture/')) { void helper(req, res); return; }
  if (!app) { res.writeHead(503).end(); return; }
  if (req.url?.startsWith('/human-identity/interaction/') && req.url?.endsWith('/complete') && req.method === 'POST') humanCompletePosts++;
  if (req.url?.split('?')[0] === '/api/connect/rpc') {
    let incoming = '', operation = '';
    req.on('data', value => { if (incoming.length < 2200000) incoming += value.toString('utf8'); });
    req.on('end', () => { try { const parsed = JSON.parse(incoming); operation = parsed.op;
      if (operation === 'identity.human.approve') humanDecisionIntents.push({ request:createHash('sha256').update(String(parsed.args?.requestId)).digest('hex'),intent:createHash('sha256').update(JSON.stringify(parsed.args)).digest('hex') });
    } catch { operation = ''; } incoming = ''; });
    const end = res.end.bind(res);
    res.end = function(chunk, ...args) {
      if (operation === 'identity.human.approve' && (humanLossArmed || humanHoldArmed)) {
        let successful = false; try { const value = JSON.parse(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)); successful = value.ok === true; } catch { /* No payload output. */ }
        if (successful && humanLossArmed) { humanLossArmed = false; res.destroy(); return res; }
        if (successful && humanHoldArmed) { humanHoldArmed = false; releaseHumanAck = () => { releaseHumanAck = null; end(chunk, ...args); }; return res; }
      }
      if (lossArmed && operation === 'apps.feedback.submit') {
        let successful = false;
        try { const value = JSON.parse(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)); const result = value.result ?? value;
          successful = value.ok === true && !!result.receipt?.ticketId; } catch { /* Never log a response body. */ }
        if (successful) { lossArmed = false; res.destroy(); return res; }
      }
      return end(chunk, ...args);
    };
  }
  app(req, res);
});
for (const server of [source, backend, reviewsProvider]) server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
backend.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
const token = randomBytes(32).toString('base64url');
const identity = { linkId: 'feedback_browser_fixture_link_123456789012', hostDeviceId: 'feedback_browser_fixture_host', connectorId: 'feedback_browser_fixture_connector', name: 'Synthetic fixture device' };
const settings = { dataDir, connectOrigins: [frontendOrigin], appOriginTemplate: 'http://{appId}.localhost:5397' };
function makeRuntime() { return createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
  createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity, token, serverUrl: backendOrigin }); }
async function restart() { runtime?.stop(); app = undefined; await currentApp?.locals.closeServices(); app = createHttpApp(dist, settings); currentApp = app; runtime = makeRuntime(); runtime.start(); await until(() => runtime.status().connected); generation++; }
let currentApp;
function send(res, status, value) { res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(value)); }
async function body(req) { let value = ''; for await (const part of req) { value += part.toString('utf8'); if (Buffer.byteLength(value) > 10000) throw new Error('fixture_body_limit'); } return value ? JSON.parse(value) : {}; }
async function helper(req, res) {
  try {
    if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress) || req.headers.origin !== frontendOrigin) { send(res, 403, { code: 'fixture_origin_denied' }); return; }
    const pathname = new URL(req.url, backendOrigin).pathname;
    if (pathname === '/__fixture/info' && req.method === 'GET') { send(res, 200, { synthetic: true, appId: fixtureAppId, projectId: 'soty', namespace: basename(folder), generation,
      humanRps: humanRps.map(rp => ({ label: rp.clientId, loginUrl: rp.origin+'/login' })) }); return; }
    if (req.method !== 'POST') { send(res, 405, { code: 'fixture_method_denied' }); return; }
    const args = await body(req);
    if (pathname === '/__fixture/shutdown') { send(res, 200, { synthetic: true, stopping: true }); setTimeout(() => void cleanup().then(() => process.exit(0)), 50); return; }
    if (pathname === '/__fixture/lose-next-human-ack') { humanLossArmed = true; send(res, 200, { synthetic: true, armed: true }); return; }
    if (pathname === '/__fixture/hold-next-human-ack') { humanHoldArmed = true; send(res, 200, { synthetic: true, armed: true }); return; }
    if (pathname === '/__fixture/release-human-ack') { releaseHumanAck?.(); send(res, 200, { synthetic: true, released: true }); return; }
    if (pathname === '/__fixture/human-proof') {
      if (Object.keys(args).some(key => key !== 'expectedAccountId') || args.expectedAccountId !== undefined && typeof args.expectedAccountId !== 'string') throw new Error('fixture_invalid_account');
      const connectDb = new DatabaseSync(join(dataDir,'connect','accounts.sqlite'),{readOnly:true});
      const identityDb = new DatabaseSync(join(dataDir,'human-identity','identity.sqlite'),{readOnly:true});
      const identities = humanRps.map(rp => { const token = rp.verificationFixture().idToken; if (!token) return null; return JSON.parse(Buffer.from(token.split('.')[1],'base64url').toString('utf8')).sub; });
      const lastPair = humanDecisionIntents.slice(-2);
      try { send(res,200,{synthetic:true,connectAccounts:connectDb.prepare('SELECT count(*) AS n FROM accounts').get().n,
        decisions:identityDb.prepare('SELECT count(*) AS n FROM human_identity_decisions').get().n,
        completePosts:humanCompletePosts,ackHeld:!!releaseHumanAck,approvalPosts:humanDecisionIntents.length,
        exactDecisionReplay:lastPair.length===2&&lastPair[0].request===lastPair[1].request&&lastPair[0].intent===lastPair[1].intent,
        rpVerified:identities.map(Boolean),sameProfileInBoth:identities.every(Boolean)&&identities[0]===identities[1],
        expectedProfileMatches:args.expectedAccountId===undefined?null:identities.filter(Boolean).every(value=>value===args.expectedAccountId),
        independentAppRows:identities.every(Boolean)&&humanRps[0].currentLink(identities[0])!==humanRps[1].currentLink(identities[1]),
        priorLocalRowsPreserved:humanRps.every(rp=>rp.existingLocalRow().balance===17),bffErrors:humanRps.map(rp=>rp.lastError) }); }
      finally { connectDb.close();identityDb.close(); } return;
    }
    if (pathname === '/__fixture/reviews-scenario') {
      if (Object.keys(args).some(key => key !== 'scenario') || !['public','empty','unavailable','partial','mismatch','redirect','large','slow'].includes(args.scenario)) throw new Error('fixture_invalid_scenario');
      reviewScenario = args.scenario; send(res, 200, { synthetic: true, scenario: reviewScenario }); return;
    }
    if (pathname === '/__fixture/reviews-counts') { send(res, 200, { synthetic: true, publicGets: reviewRequests, credentialRequests: reviewCredentialRequests, scenario: reviewScenario }); return; }
    if (pathname === '/__fixture/grant') {
      if (Object.keys(args).some(key => key !== 'accountId') || typeof args.accountId !== 'string' || !/^[a-zA-Z0-9_:-]{8,160}$/.test(args.accountId)) throw new Error('fixture_invalid_account');
      granted.add(args.accountId);
      await owner.client.extension('apps.update', { appId: fixtureAppId, grants: { accountIds: [...granted], communityIds: [] } });
      send(res, 200, { granted: true }); return;
    }
    if (pathname === '/__fixture/restart') { await restart(); send(res, 200, { restarted: true, generation }); return; }
    if (pathname === '/__fixture/lose-next-submit-ack') { lossArmed = true; send(res, 200, { armed: true }); return; }
    if (pathname === '/__fixture/support') {
      if (Object.keys(args).some(key => key !== 'ticketId') || typeof args.ticketId !== 'string') throw new Error('fixture_invalid_ticket');
      const context = (await owner.client.extension('apps.feedback.context', { appId: fixtureAppId })).context;
      const base = { appId: fixtureAppId, installationId: context.installationId, ticketId: args.ticketId };
      const current = (await owner.client.extension('apps.feedback.get', base)).ticket;
      const replied = await owner.client.extension('apps.feedback.reply', { ...base, requestId: randomUUID(), expectedRevision: current.revision, body: 'Ответ тестовой поддержки: результат можно проверить.' });
      const ready = await owner.client.extension('apps.feedback.status', { ...base, requestId: randomUUID(), expectedRevision: replied.ticket.revision, status: 'ready_to_check' });
      send(res, 200, { ticketId: ready.ticket.id, status: ready.ticket.status, revision: ready.ticket.revision }); return;
    }
    if (pathname === '/__fixture/counts') {
      const databasePath = join(dataDir, 'feedback', 'feedback.sqlite');
      const db = new DatabaseSync(databasePath, { readOnly: true });
      try { send(res, 200, { tickets: db.prepare('SELECT count(*) AS n FROM feedback_tickets').get().n,
        attachments: db.prepare('SELECT count(*) AS n FROM feedback_attachments').get().n, receipts: db.prepare('SELECT count(*) AS n FROM feedback_receipts').get().n }); }
      finally { db.close(); } return;
    }
    send(res, 404, { code: 'fixture_route_unknown' });
  } catch (error) { send(res, 503, { code: /^[a-z0-9_]+$/.test(error.code ?? error.message ?? '') ? String(error.code ?? error.message) : 'fixture_failed' }); }
}

async function cleanup() {
  if (stopping) return; stopping = true;
  await vite?.close(); runtime?.stop(); clients.forEach(client => client.dispose()); await app?.locals.closeServices(); sockets.forEach(socket => socket.destroy());
  await Promise.all(humanRps.map(rp=>rp.close()));
  await Promise.all([source, backend, reviewsProvider].map(server => new Promise(done => server.listening ? server.close(done) : done())));
  if (dirname(resolve(folder)) !== resolve(tmpdir()) || !/^soty-feedback-browser-/.test(basename(folder))) throw new Error('fixture_cleanup_path');
  await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
try {
  await Promise.all([source, backend, reviewsProvider].map((server, index) => new Promise((done, reject) => { server.once('error', reject); server.listen(index === 1 ? 5397 : 0, '127.0.0.1', done); })));
  app = createHttpApp(dist, settings); currentApp = app;
  const registration = await fetch(backendOrigin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  if (!(await registration.json()).ok) throw new Error('fixture_connector_registration');
  runtime = makeRuntime(); runtime.start(); await until(() => runtime.status().connected);
  const client = createClientWithStorage({ projectId: 'soty', endpoint: backendOrigin + '/api/connect/rpc', fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin: frontendOrigin } }) }, memory());
  clients.push(client); owner = { client, account: await client.bootstrap('Тестовая поддержка') };
  const claim = await runtime.claim(); await client.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const created = await client.extension('apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, name: 'Синтетическое приложение проверки', port: source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  fixtureAppId = created.app.id;
  await until(async () => (await client.extension('apps.list')).apps.find(value => value.id === fixtureAppId)?.state === 'ready');
  const current = await client.extension('apps.universal.get', { appId: fixtureAppId, expectedAccountId: owner.account.accountId });
  await client.extension('apps.universal.admit', { appId: fixtureAppId, expectedAccountId: owner.account.accountId, requestId: randomUUID(), expectedRevision: current.registration.revision,
    proposal: { kind: 'author-draft', draft: { title: 'Синтетическое приложение проверки' } } });
  const providerRef = { id:'fixture:reviews',version:1,digest:createHash('sha256').update('synthetic-public-api-1.1').digest('hex') };
  settings.reviewsConfiguration = { providers:[{...providerRef,origin:`http://127.0.0.1:${reviewsProvider.address().port}`}],bindings:reviewSubjects.map(value => ({
    scope:{registryId:'soty',environmentId:'production',tenantId:owner.account.accountId,appId:fixtureAppId},localSubject:{kind:value.kind,id:value.kind==='app'?fixtureAppId:value.kind+'_fixture'},
    providerRef,subjectRef:{id:'fixture:subject-'+value.kind,version:1,digest:createHash('sha256').update('synthetic-'+value.kind).digest('hex')},
    providerSubjectId:value.id,providerEntityType:value.type,mode:'public-read' })) };
  settings.allowReviewsFixtureOrigins = true;
  const humanSecrets = [randomBytes(32).toString('base64url'),randomBytes(32).toString('base64url')];
  humanRps.push(...await Promise.all(['browser-alpha','browser-beta'].map((clientId,index)=>createHumanBffFixture({clientId,clientSecret:humanSecrets[index]}))));
  const key = generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'}); Object.assign(key,{kid:'browser-fixture',use:'sig',alg:'RS256'});
  settings.humanIdentity = {enabled:true,issuer:frontendOrigin+'/human-identity',registryId:'soty',environmentId:'production',
    clients:humanRps.map((rp,index)=>({id:rp.clientId,label:index?'Второе тестовое приложение':'Первое тестовое приложение',redirectUri:rp.redirectUri,clientSecret:humanSecrets[index]})),
    jwks:{keys:[key]},cookieKeys:[randomBytes(32).toString('base64url')],artifactKey:randomBytes(32),artifactKeyId:'browser-fixture'};
  await restart();
  vite = await createViteServer({ root, configFile: join(root, 'vite.config.ts'), server: { host: '127.0.0.1', port: 5237, strictPort: true,
    proxy: { '/api': { target: backendOrigin }, '/human-identity': { target:backendOrigin,changeOrigin:false }, '/__fixture': { target: backendOrigin, headers: { origin: frontendOrigin } } } },
    plugins: [{ name: 'feedback-loopback-fixture-guard', configureServer(server) { server.middlewares.use((req, res, next) => {
      if (!req.url?.startsWith('/__fixture/')) return next();
      if (req.headers.host !== '127.0.0.1:5237' || req.headers.origin && req.headers.origin !== frontendOrigin || req.method === 'POST' && req.headers.origin !== frontendOrigin) { res.writeHead(403).end(); return; }
      next();
    }); } }] });
  await vite.listen();
  await Promise.all(humanRps.map(rp=>rp.configure(settings.humanIdentity.issuer)));
  console.log(JSON.stringify({ state: 'ready', url: frontendOrigin + '/src/world/app-feedback-signed.test.html', synthetic: true, launchCredentialsLogged: false }));
  process.on('SIGINT', () => void cleanup().then(() => process.exit(0))); process.on('SIGTERM', () => void cleanup().then(() => process.exit(0)));
} catch (error) { await cleanup(); console.error(/^[a-z0-9_]+$/.test(error.code ?? error.message ?? '') ? String(error.code ?? error.message) : 'fixture_start_failed'); process.exitCode = 1; }
