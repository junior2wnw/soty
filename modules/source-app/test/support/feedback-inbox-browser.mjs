// Local synthetic UI/Native-SQL fixture. Does not implement or claim Root login.
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createOrdinaryAppStore } from '../../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../../examples/ordinary-app/native.mjs';
import { createNativeAuthorityRuntime } from '../../server/native-authority.mjs';
import { feedbackInput, feedbackOutput } from '../../shared/feedback-wire.mjs';

const bundle = resolve(process.argv[2]), directory = await mkdtemp(join(tmpdir(), 'soty-inbox-browser-'));
const store = createOrdinaryAppStore({ databasePath: join(directory, 'source.sqlite'), realmId: 'browser-fixture', key: randomBytes(32), keyId: 'synthetic', initialize: true });
store.createResource({ id: 'selected', incarnationId: 'one', title: 'Synthetic project' });
const runtime = createNativeAuthorityRuntime(createOrdinaryAppNativePort({ store, resourceId: 'selected', incarnationId: 'one' }));
const proofs = {};
for (const role of ['owner', 'participant']) {
  const principal = 'synthetic-' + role; store.createPrincipal(principal); store.grant('selected', principal, role);
  const token = store.createNativeSession(principal), subject = 'synthetic-oidc-' + role;
  const binding = { identity: { issuer: 'https://issuer.test/human-identity', subject }, rootPrincipal: { accountId: 'synthetic-root-' + role, deviceId: 'fixture-device' },
    humanPrincipal: { issuer: 'https://issuer.test/human-identity', subject, clientId: 'fixture' },
    resource: { selection: { kind: 'soty.resource.v1', nativeId: 'selected', incarnationId: 'one' } }, semanticDigest: 'a'.repeat(64), operation: 'link' };
  proofs[role] = await runtime.capture(binding, { headers: { cookie: 'ordinary_native_browser-fixture=' + token } });
  store.tx(() => runtime.commitIdentity(proofs[role], binding.identity));
}
const image = await readFile(new URL('../../../feedback/test/fixtures/chrome.png', import.meta.url));
const audio = await readFile(new URL('../../../feedback/test/fixtures/ffmpeg-opus.ogg', import.meta.url));
await runtime.feedback(proofs.participant, 'submit', { requestId: 'synthetic-browser-media-0001', body: 'Synthetic voice and screenshot issue', attachments: [
  { kind: 'image', name: 'Synthetic screenshot.png', mimeType: 'image/png', dataBase64: image.toString('base64') },
  { kind: 'audio', name: 'Synthetic voice.ogg', mimeType: 'audio/ogg', dataBase64: audio.toString('base64') }] });
for (let i = 0; i < 21; i++) await runtime.feedback(proofs.participant, 'submit', {
  requestId: 'synthetic-browser-list-' + String(i).padStart(4, '0'), body: 'Synthetic issue ' + String(i).padStart(2, '0'), attachments: [] });
const scripts = new Map([
  ['/assets/source-sdk.js', bundle],
  ['/assets/feedback-inbox.js', new URL('../../examples/ordinary-app/assets/feedback-inbox.js', import.meta.url)],
  ['/assets/app.css', new URL('../../examples/ordinary-app/assets/app.css', import.meta.url)],
]);
const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (req.method === 'GET' && url.pathname === '/') {
      const role = url.searchParams.get('role') === 'owner' ? 'owner' : 'participant';
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
      res.end(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><link rel="stylesheet" href="/assets/app.css"><main><h1>Проверка обращений</h1><p>Учебные данные · ${role === 'owner' ? 'Владелец' : 'Автор'} · проверка интерфейса, без общего входа Сот</p><div id="feedback-inbox"></div></main><script type="module">import{mountFeedbackInbox}from'/assets/feedback-inbox.js';import{createSourceFeedbackClient}from'/assets/source-sdk.js';const api=createSourceFeedbackClient({profileVersion:3,fetch:(url,options)=>fetch(url,{...options,headers:{...options.headers,'x-fixture-role':'${role}'}})});const inbox=mountFeedbackInbox({host:document.getElementById('feedback-inbox'),api,isCurrent:()=>true});window.fixtureInbox=inbox;await inbox.open();addEventListener('pagehide',()=>inbox.dispose(),{once:true});</script></html>`); return;
    }
    if (scripts.has(url.pathname) && req.method === 'GET') {
      res.writeHead(200, { 'content-type': url.pathname.endsWith('.css') ? 'text/css' : 'text/javascript' }); res.end(await readFile(scripts.get(url.pathname))); return;
    }
    if (!url.pathname.startsWith('/api/embed/feedback')) { res.writeHead(404).end(); return; }
    const role = req.headers['x-fixture-role']; if (!Object.hasOwn(proofs, role)) { res.writeHead(401).end(); return; }
    const suffix = url.pathname.slice('/api/embed/feedback'.length);
    const operation = req.method === 'GET' ? suffix === '/context' ? 'context' : suffix === '/ticket' ? 'get' : 'list'
      : suffix === '/reply' ? 'reply' : suffix === '/status' ? 'status' : suffix === '/accept' ? 'accept' : 'submit';
    let input = Object.fromEntries(url.searchParams);
    if (req.method === 'POST') { let raw = ''; for await (const part of req) { raw += part; if (raw.length > 1500000) throw new Error('bound'); } input = JSON.parse(raw); }
    const data = feedbackOutput(operation, await runtime.feedback(proofs[role], operation, feedbackInput(operation, input)));
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify({ ok: true, data }));
  } catch (error) {
    res.writeHead(error.status ?? 503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: { code: error.code ?? 'fixture_unavailable' } }));
  }
});
await new Promise(resolveListen => server.listen(0, '127.0.0.1', resolveListen));
console.log(JSON.stringify({ fixture: true, origin: 'http://127.0.0.1:' + server.address().port, seededTickets: 22, realRootLogin: false }));
async function close() { server.closeAllConnections(); await new Promise(done => server.close(done)); runtime.close(); store.close(); process.exit(0); }
process.stdin.resume(); process.stdin.once('data', close); process.once('SIGTERM', close);
