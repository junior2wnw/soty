import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { createHttpApp } from '../../../server/http-app.js';
import { leaseLoopbackPort, startScopedConnector } from './support/scoped-connector-startup.mjs';

const listen = server => new Promise((done, fail) => {
  server.once('error', fail); server.listen(0, '127.0.0.1', () => done(server.address().port));
});
const close = server => new Promise((done, fail) => server.close(error => error ? fail(error) : done()));
function claim(port, origin, timeoutMs) {
  return new Promise((done, fail) => {
    const bytes = '{}';
    const req = request({ hostname: '127.0.0.1', port, path: '/apps/claim', method: 'POST', agent: false,
      headers: { origin, 'content-type': 'application/json', 'content-length': bytes.length },
      signal: AbortSignal.timeout(timeoutMs),
    }, res => {
      const chunks = []; res.on('data', part => chunks.push(part)); res.on('error', fail);
      res.on('end', () => { let body; try { body = JSON.parse(Buffer.concat(chunks)); } catch {}
        done({ status: res.statusCode, body }); });
    });
    req.on('error', fail); req.end(bytes);
  });
}

async function runtime(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-scoped-startup-'));
  const runtimeDir = join(directory, 'runtime'), dist = join(directory, 'dist');
  let root, lease, child;
  const server = createServer((req, res) => root ? root(req, res) : res.writeHead(503).end());
  t.after(async () => {
    let childStopped = false;
    try { await child?.stop(); childStopped = true; }
    finally {
      await lease?.release(); await root?.locals.closeServices();
      server.closeAllConnections(); if (server.listening) await close(server);
      assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
      assert.match(basename(directory), /^soty-scoped-startup-/);
      if (childStopped) await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
  });
  await mkdir(runtimeDir); await mkdir(dist);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>Disposable startup proof</title>');
  await writeFile(join(runtimeDir, 'connector-config.json'), JSON.stringify({
    workspaceRoot: runtimeDir, allowedRoots: [runtimeDir], installId: 'fixture_startup_install',
  }));
  lease = await leaseLoopbackPort();
  const backendPort = await listen(server), origin = `http://127.0.0.1:${backendPort}`;
  assert.notEqual(backendPort, lease.port, 'backend cannot reclaim the held connector port');
  root = createHttpApp(dist, { dataDir: join(directory, 'root'), connectOrigins: [origin],
    appOriginTemplate: `http://{appId}.localhost:${backendPort}`, localConnectorPort: lease.port });
  server.on('upgrade', (req, socket, head) => {
    if (!root.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy();
  });
  return { port: lease.port, origin, release: () => lease.release(), start() {
    child = startScopedConnector({ port: lease.port, directory, runtimeDir, origin }); return child;
  } };
}

test('scoped startup lease holds the real port, closes unexpected sockets, and releases idempotently', async t => {
  const lease = await leaseLoopbackPort(); t.after(() => lease.release());
  const other = createServer();
  await assert.rejects(new Promise((done, fail) => {
    other.once('error', fail); other.listen(lease.port, '127.0.0.1', done);
  }), error => error.code === 'EADDRINUSE');
  // Even an accidental connection must not hold server.close open.
  const { connect } = await import('node:net');
  const socket = connect(lease.port, '127.0.0.1');
  await new Promise((done, fail) => { socket.once('close', done); socket.once('error', fail); });
  await lease.release(); await lease.release();
  const reuse = createServer(); t.after(async () => { if (reuse.listening) await close(reuse); });
  await new Promise((done, fail) => { reuse.once('error', fail); reuse.listen(lease.port, '127.0.0.1', done); });
});

test('actual installed CLI bind collision fails fast without accepting another process claim and fully stops', { timeout: 15000 }, async t => {
  const f = await runtime(t); await f.release();
  let requests = 0;
  const occupied = createServer((req, res) => {
    requests++; res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ hostDeviceId: 'fixture_private_canary', connectorId: 'unrelated', claimCode: 'unrelated' }));
  });
  t.after(async () => { occupied.closeAllConnections(); if (occupied.listening) await close(occupied); });
  await new Promise((done, fail) => { occupied.once('error', fail); occupied.listen(f.port, '127.0.0.1', done); });
  const child = f.start(), started = Date.now();
  await assert.rejects(child.waitForClaim(timeout => claim(f.port, f.origin, timeout)), error => {
    assert.equal(error.code, 'scoped_connector_http_bind_eaddrinuse');
    assert.equal(error.diagnostics.listening, false);
    assert.equal(error.diagnostics.attempts, 0);
    assert.equal(error.diagnostics.lastHttpStatus, null);
    assert.ok(!error.message.includes('fixture_private_canary'));
    assert.ok(!error.message.includes('fixture_scoped_link_'));
    return true;
  });
  assert.ok(Date.now() - started < 5000, 'bind failure must not wait the 60-second claim deadline');
  assert.equal(requests, 0, 'a foreign HTTP 200 is never a fixture readiness proof');
  assert.equal(child.diagnostics().closed, false, 'bind-failed maintained CLI remains alive without owned cleanup');
  await child.stop(); await child.stop(); assert.equal(child.diagnostics().closed, true);
});

test('actual installed CLI claims through the maintained Root after a held fresh port is released', { timeout: 70000 }, async t => {
  const f = await runtime(t); await f.release(); const child = f.start();
  const value = await child.waitForClaim(timeout => claim(f.port, f.origin, timeout));
  assert.equal(value.hostDeviceId, 'fixture_scoped_host');
  assert.equal(/^[A-Za-z0-9_-]{43}$/.test(value.claimCode), true, 'claim has the maintained format without logging its value');
  assert.ok(value.connectorId);
  assert.equal(child.diagnostics().listening, true);
  assert.equal(child.diagnostics().lastHttpStatus, 200);
  await child.stop(); assert.equal(child.diagnostics().closed, true);
});

test('owned CLI readiness preserves exact-Origin refusal and reports only the safe HTTP status', { timeout: 15000 }, async t => {
  const f = await runtime(t); await f.release(); const child = f.start();
  await assert.rejects(child.waitForClaim(timeout => claim(f.port, 'https://wrong-origin.invalid', timeout), 1200), error => {
    assert.equal(error.code, 'scoped_connector_claim_timeout');
    assert.equal(error.diagnostics.listening, true);
    assert.equal(error.diagnostics.lastHttpStatus, 403);
    assert.ok(!error.message.includes('wrong-origin.invalid'));
    assert.ok(!error.message.includes('claimCode'));
    return true;
  });
  await child.stop(); assert.equal(child.diagnostics().closed, true);
});

test('actual pending claim response cannot become readiness after owned child termination', { timeout: 70000 }, async t => {
  const f = await runtime(t); await f.release(); const child = f.start();
  let entered, release;
  const pending = new Promise(done => { entered = done; }), gate = new Promise(done => { release = done; });
  const result = child.waitForClaim(async timeout => {
    const value = await claim(f.port, f.origin, timeout);
    if(value.status!==200)return value;
    entered(); await gate; return value;
  });
  const rejected = assert.rejects(result, error => {
    assert.equal(error.code, 'scoped_connector_stopped');
    assert.equal(error.diagnostics.closed, true);
    assert.equal(error.diagnostics.stopping, true);
    return true;
  });
  await pending; await child.stop(); release(); await rejected;
});
