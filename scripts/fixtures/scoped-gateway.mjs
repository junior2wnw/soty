// Loopback-only, synthetic data, installed connector, real signed services.
// Helpers below are a test harness, never production authorization.
import { createServer as createTlsServer } from 'node:https';
import { execFile } from 'node:child_process';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { createServer as createViteServer } from 'vite';
import { createScopedGatewayFixture, freePort } from '../../modules/apps/test/support/scoped-gateway-fixture.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const frontPort = Number(process.env.SOTY_GATEWAY_FIXTURE_PORT || 5242);
if (!Number.isInteger(frontPort) || frontPort < 1024 || frontPort > 65535) throw new Error('fixture_port_invalid');
const origin = `http://127.0.0.1:${frontPort}`, backendPort = await freePort(), appPort = await freePort();
let fixture, closeFixture, vite, tls, closed = false, browserAccount;
const sockets = new Set();
const safeCode = error => /^[A-Za-z0-9_:-]{1,120}$/.test(error?.code || '') ? error.code : 'fixture_failed';
function send(res, status, value) { res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(value)); }
async function body(req) {
  const parts = []; let size = 0;
  for await (const part of req) { size += part.length; if (size > 2048) throw new Error('fixture_input_too_large'); parts.push(part); }
  return JSON.parse(Buffer.concat(parts).toString('utf8'));
}
async function helper(req, res) {
  if (!fixture) return send(res, 503, { synthetic: true, code: 'fixture_starting' });
  const path = req.url?.split('?')[0];
  if (path === '/__fixture/info' && req.method === 'GET') return send(res, 200, { synthetic: true, appId: fixture.appId, embeddedOrigin: fixture.embedded, nativeOrigin: fixture.native });
  if (path === '/__fixture/grant' && req.method === 'POST') {
    const input = await body(req);
    if (Object.keys(input).join(',') !== 'accountId' || typeof input.accountId !== 'string' || !/^acct_[A-Za-z0-9_-]{20,80}$/.test(input.accountId)) return send(res, 400, { code: 'fixture_account_invalid' });
    await fixture.grant(input.accountId); browserAccount = input.accountId;
    return send(res, 200, { synthetic: true, granted: true });
  }
  if (path === '/__fixture/proof' && req.method === 'GET') {
    const state = fixture.planner().store.read();
    return send(res, 200, { synthetic: true, selectedObjects: state.entities.filter(value => value.workspaceId === fixture.workspaceId).length,
      otherPrivateObjects: state.entities.filter(value => value.workspaceId === fixture.foreignWorkspaceId).length,
      sourceWorkspaces: state.workspaces.length, scopedProfile: true, browserGranted: !!browserAccount });
  }
  if (path === '/__fixture/restart-source' && req.method === 'POST') { await fixture.restartSource(); return send(res, 200, { synthetic: true, restarted: true }); }
  if (path === '/__fixture/revoke' && req.method === 'POST') {
    await fixture.owner.client.extension('apps.update', { appId: fixture.appId, grants: { accountIds: [], communityIds: [] } });
    return send(res, 200, { synthetic: true, revoked: true });
  }
  return send(res, 404, { code: 'fixture_route_not_found' });
}
async function cleanup() {
  if (closed) return; closed = true;
  sockets.forEach(socket => socket.destroy());
  await Promise.allSettled([vite?.close(), tls ? new Promise(done => tls.close(done)) : undefined]);
  await (fixture?.close() || closeFixture?.());
}
process.on('SIGINT', () => void cleanup().then(() => process.exit(0)));
process.on('SIGTERM', () => void cleanup().then(() => process.exit(0)));
try {
  await mkdir(join(root,'output','playwright'),{recursive:true});
  // A 2×2 synthetic raster exported by native Canvas, not personal media.
  await writeFile(join(root,'output','playwright','scoped-capture-selected.png'),Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC','base64'));
  vite = await createViteServer({ root, configFile: join(root, 'vite.config.ts'), server: { host: '127.0.0.1', port: frontPort, strictPort: true,
    proxy: { '/api': { target: `http://127.0.0.1:${backendPort}`, ws: true }, '/human-identity': { target: `http://127.0.0.1:${backendPort}`, changeOrigin: false } } },
    plugins: [{ name: 'scoped-loopback-test-harness', configureServer(server) { server.middlewares.use((req, res, next) => {
      if (!req.url?.startsWith('/__fixture/')) return next();
      if (req.headers.host !== `127.0.0.1:${frontPort}` || req.headers.origin && req.headers.origin !== origin || req.method === 'POST' && req.headers.origin !== origin) return send(res, 403, { code: 'fixture_origin_denied' });
      void helper(req, res).catch(error => send(res, 400, { code: safeCode(error) }));
    }); } }] });
  await vite.listen();
  fixture = await createScopedGatewayFixture({ frontPort, appPort, backendPort, distDir: root,renewal:process.env.SOTY_GATEWAY_RENEWAL_FIXTURE==='1', t: { after: callback => { closeFixture = callback; } } });
  // Ephemeral fixture certificate. It is not installed into any trust store and
  // never represents a production TLS validation result.
  const keyFile = join(fixture.directory, 'tls.key'), certFile = join(fixture.directory, 'tls.crt');
  const openssl = process.env.SOTY_FIXTURE_OPENSSL || (process.platform === 'win32' ? 'C:/Program Files/Git/usr/bin/openssl.exe' : 'openssl');
  await promisify(execFile)(openssl, ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyFile, '-out', certFile, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:*.localhost,DNS:localhost,IP:127.0.0.1'], { windowsHide: true, maxBuffer: 16384 });
  tls = createTlsServer({ key: await readFile(keyFile), cert: await readFile(certFile) }, (req, res) => {
    if (req.headers.host !== new URL(fixture.embedded).host) return res.writeHead(403).end();
    fixture.root()(req, res);
  });
  tls.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => tls.listen(appPort, '127.0.0.1', done));
  const browserConfig = join(fixture.directory, 'playwright-config.json');
  await writeFile(browserConfig, JSON.stringify({ browser: { browserName: 'chromium', launchOptions: { channel: 'chrome' }, contextOptions: { ignoreHTTPSErrors: true } } }));
  console.log(JSON.stringify({ synthetic: true, state: 'ready', url: origin + '/src/world/app-scoped-gateway.test.html', browserConfig,
    certificate: 'ephemeral-local-only', productionHttpsValidated: false, credentialsLogged: false }));
} catch (error) { await cleanup(); console.error(safeCode(error)); process.exitCode = 1; }
