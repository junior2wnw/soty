import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { join, dirname, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { createHttpApp, waitForRejectedHttpStart } from '../../http-app.js';
import { createClientWithStorage } from '../../../modules/connect/browser/client.mjs';

function memory() {
  let value;
  return { async read() { return structuredClone(value ?? null); },
    async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) { assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value); } };
}
export async function coreFeedbackHttpFixture(t, { universalAppsEnabled = true } = {}) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-core-feedback-')), dist = join(folder, 'dist'), dataDir = join(folder, 'data');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Disposable core support</title>');
  let app, loseSubmit = false; const clients = [], sockets = new Set();
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    clients.forEach(client => client.dispose());
    const closed = new Promise(done => server.close(done)); sockets.forEach(socket => socket.destroy());
    await app?.locals.closeServices(); await closed;
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const start = options => createHttpApp(dist, { env: {}, dataDir, connectOrigins: [origin], universalAppsEnabled,
    appOriginTemplate: `http://{appId}.localhost:${server.address().port}`, ...options });
  app = start();
  async function client(label, bootstrap = true) {
    const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      async fetch(url, options) {
        const op = JSON.parse(options.body).op;
        const response = await fetch(url, { ...options, headers: { ...options.headers, origin } });
        if (loseSubmit && op === 'core.feedback.submit') { loseSubmit = false; await response.text(); throw new TypeError('disposable_lost_ack'); }
        return response;
      } }, memory());
    clients.push(client); return { client, account: bootstrap ? await client.bootstrap(label) : null };
  }
  const owner = await client('Support fixture'), reporter = await client('Reporter fixture'), foreign = await client('Other fixture');
  const configuration = { enabled: true, supportOwnerAccountId: owner.account.accountId };
  return { folder, dataDir, origin, owner, reporter, foreign, configuration, client,
    get app() { return app; }, loseNextSubmit() { loseSubmit = true; },
    async stop() { await app?.locals.closeServices(); app = null; },
    async restart(options = { coreFeedback: configuration }) {
      await app?.locals.closeServices(); app = null;
      try { app = start(options); } catch (error) { await waitForRejectedHttpStart(error); throw error; }
    } };
}
