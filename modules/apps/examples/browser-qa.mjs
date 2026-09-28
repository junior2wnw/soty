import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';
import { createSampleApp } from './sample-app.mjs';

// Isolated, loopback-only acceptance fixture. These three invented principals
// deliberately do not share production Connect data or installed credentials.
const port = Number(process.argv[2] || 5306), origin = `http://localhost:${port}`;
const dir = await mkdtemp(join(tmpdir(), 'soty-apps-browser-'));
const sample = await createSampleApp();
const actors = { owner: { accountId: 'qa_owner', deviceId: 'qa_owner_device' }, member: { accountId: 'qa_member', deviceId: 'qa_member_device' }, outsider: { accountId: 'qa_outsider', deviceId: 'qa_outsider_device' } };
let apps, app, membershipListener; const members = new Set(['qa_owner', 'qa_member']);
const token = randomBytes(32).toString('base64url');
const server = createServer(async (req, res) => {
  if (apps?.handleRequest(req, res)) return;
  if (!['localhost', '127.0.0.1'].includes(String(req.headers.host).split(':')[0])) { res.writeHead(404); res.end(); return; }
  if (req.url === '/qa/open' && req.method === 'POST' && req.headers.origin === origin) {
    try { const chunks = []; for await (const chunk of req) chunks.push(chunk); const { actor } = JSON.parse(Buffer.concat(chunks));
      const value = apps.execute({ op: 'apps.launch', args: { appId: app.id }, actor: actors[actor] }); res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(value));
    } catch (error) { res.writeHead(error.status || 400, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: error.code })); } return;
  }
  if (req.url === '/qa/revoke' && req.method === 'POST' && req.headers.origin === origin) {
    members.delete('qa_member'); membershipListener({ communityId: 'qa_family', profileId: 'qa_member', state: 'removed' }); res.end('{}'); return;
  }
  if (req.url === '/qa/restore' && req.method === 'POST' && req.headers.origin === origin) { members.add('qa_member'); res.end('{}'); return; }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Соты — проверка живого приложения</title><style>body{font:15px system-ui;background:#252924;color:#eee;margin:0;padding:24px}nav{display:flex;gap:10px;flex-wrap:wrap}button{padding:12px;border:1px solid #887648;border-radius:8px;background:#353c34;color:#eee;cursor:pointer}iframe{display:block;border:0;border-radius:18px;width:min(650px,100%);height:660px;margin-top:20px}p{color:#c8b581}</style><h1>Живое приложение на другом устройстве</h1><nav><button data-actor="owner">Открыть: владелец</button><button data-actor="member">Открыть: участник</button><button data-actor="outsider">Открыть: посторонний</button><button id="revoke">Отозвать доступ участника</button><button id="restore">Вернуть участника</button></nav><p id="status" role="status">Отдельный локальный проект · HTTP + WebSocket · изолированный origin</p><iframe title="Покупки" sandbox="allow-scripts allow-forms allow-same-origin" referrerpolicy="no-referrer"></iframe><script>document.querySelectorAll('[data-actor]').forEach(button=>button.onclick=async()=>{const r=await fetch('/qa/open',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({actor:button.dataset.actor})});const v=await r.json();document.querySelector('#status').textContent=r.ok?'Проверяем вход: '+button.dataset.actor:'Доступ запрещён: '+v.error;if(r.ok)document.querySelector('iframe').src=v.launchUrl});document.querySelector('#revoke').onclick=()=>fetch('/qa/revoke',{method:'POST'});document.querySelector('#restore').onclick=()=>fetch('/qa/restore',{method:'POST'});</script></html>`);
});
server.on('upgrade', (req, socket, head) => { if (!apps?.handleUpgrade(req, socket, head)) socket.destroy(); });
await new Promise(resolve => server.listen(port, '127.0.0.1', resolve));
apps = createAppsService({ databasePath: join(dir, 'apps.sqlite'), appOriginTemplate: `http://{appId}.localhost:${port}`, shellOrigins: [origin],
  actorActive: actor => Object.values(actors).some(item => item.accountId === actor.accountId && item.deviceId === actor.deviceId),
  canAccessCommunity: (id, group) => group === 'qa_family' && members.has(id), isGroupAdmin: (id, group) => id === 'qa_owner' && group === 'qa_family',
  subscribeMembership: listener => { membershipListener = listener; return () => {}; },
  authenticateConnector: async auth => auth.token === token && auth.deviceId === 'qa_laptop' && auth.connectorId === 'qa_connector' });
const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
  createWebSocket: url => new WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
{ serverUrl: `http://127.0.0.1:${port}`, identity: { linkId: 'qa_link_1234567890', hostDeviceId: 'qa_laptop', connectorId: 'qa_connector', name: 'QA Laptop' }, token });
runtime.start();
while (!runtime.status().connected) await new Promise(resolve => setTimeout(resolve, 20));
const claim = await runtime.claim(); apps.execute({ op: 'apps.claim', actor: actors.owner, args: { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode } });
app = apps.execute({ op: 'apps.register', actor: actors.owner, args: { hostDeviceId: 'qa_laptop', connectorId: 'qa_connector', name: 'Покупки', port: sample.port, grants: { communityIds: ['qa_family'] } } }).app;
console.log(`Browser QA fixture ready: ${origin}`);
async function stop() { runtime.stop(); apps.close(); await sample.close(); server.closeAllConnections(); server.close(); }
process.on('SIGINT', () => void stop()); process.on('SIGTERM', () => void stop());
