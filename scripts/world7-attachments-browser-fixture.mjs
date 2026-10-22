import { createServer } from 'node:http';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { createServer as createViteServer } from 'vite';
import { createWorldService } from '../modules/world/server/index.mjs';
import { createConnectService, digestArgs } from '../modules/connect/server/index.mjs';
import { createConnectHandler } from '../modules/connect/server/http.mjs';

// Explicit disposable localhost acceptance only. Private signers never leave Node.
const origin = 'http://127.0.0.1:5356', projectId = 'world7-browser-owned', base = realpathSync(tmpdir());
const folder = mkdtempSync(join(base, 'soty-world7-browser-')); let connect;
const world = createWorldService({ databasePath: join(folder, 'world.sqlite'), projectId, allowWorld7Migration: true,
  withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback) });
connect = createConnectService({ databasePath: join(folder, 'connect.sqlite'), projectId, allowedOrigins: [origin], extensions: [world],
  canRequestContact: (left, right) => world.canRequestContact(left, right) });
const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
const peer = { key: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }), label: 'Борис · локально' };
async function call(op, args = {}) {
  const challenge = await connect.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
  if (!challenge.ok) throw Error(challenge.error.code);
  const response = await connect.handle({ origin, op, args, proof: { challengeId: challenge.challengeId, publicJwk: peer.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: peer.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  if (!response.ok) throw Error(response.error.code); return response;
}
Object.assign(peer, await call('bootstrap', { label: peer.label, encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) }));
await call('world.profile.get');
const community = (await call('world.community.create', { requestId: 'browser_files_room', name: 'Файлы · локально', joinPolicy: 'open' })).community;
const handler = createConnectHandler(connect);
const server = createServer(async (req, res) => {
  if (req.url?.startsWith('/api/connect/rpc')) { await handler(req, res); return; }
  const reply = value => { res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(value)); };
  if (req.url === '/fixture/info' && req.method === 'GET') { reply({ projectId, communityId: community.communityId, peerAccountId: peer.accountId }); return; }
  if (req.url === '/fixture/contact' && req.method === 'POST' && req.headers.origin === origin) {
    try {
      const chunks = []; let bytes = 0; for await (const part of req) { bytes += part.length; if (bytes > 4096) throw Error('fixture_input_limit'); chunks.push(part); }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8')); reply(await call('contacts.request', { cardId: body.cardId }));
    } catch { res.statusCode = 400; reply({ error: 'fixture_contact_failed' }); }
    return;
  }
  res.statusCode = 404; reply({ error: 'not_found' });
});
await new Promise(resolve => server.listen(5436, '127.0.0.1', resolve));
const vite = await createViteServer({ server: { host: '127.0.0.1', port: 5356, strictPort: true, proxy: {
  '/api/connect/rpc': 'http://127.0.0.1:5436', '/fixture/': 'http://127.0.0.1:5436',
} } }); await vite.listen();
console.log(JSON.stringify({ state: 'ready', url: origin + '/src/world/attachment-fixture.html', data: 'disposable', realConnect: true, realIndexedDB: true }));
let closing = false;
async function close() {
  if (closing) return; closing = true; await vite.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  connect.close(); world.close(); const actual = realpathSync(folder);
  if (dirname(actual).toLowerCase() !== base.toLowerCase() || !basename(actual).startsWith('soty-world7-browser-')) throw Error('fixture_cleanup_scope');
  rmSync(actual, { recursive: true }); process.exit(0);
}
process.on('SIGINT', close); process.on('SIGTERM', close);
