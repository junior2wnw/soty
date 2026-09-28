import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { attachConnectorApi } from '../connector-api.js';

const executable = process.env.SOTY_OPENCODE_E2E_PATH;

// Runs the real connector with the installed CLI for version detection only.
// There is no model provider and no job submission in this test.
test('a successful registration after the first poll denial immediately restores truthful connector health',
  { skip: !executable, timeout: 20_000 }, async t => {
    const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-registration-test-'));
    const runtime = join(root, 'runtime'); await mkdir(runtime);
    await writeFile(join(runtime, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtime, allowedRoots: [runtime] }));
    let releaseRegistration, retryObserved, firstPollStatus, polls = 0, registrations = 0, child, outputBytes = 0;
    const registrationGate = new Promise(done => { releaseRegistration = done; });
    const retried = new Promise(done => { retryObserved = done; });
    const app = express();
    app.use((req, res, next) => {
      if (req.path === '/api/connectors/register') {
        res.once('finish', () => { if (res.statusCode === 200) registrations += 1; });
        // Guarantee that the first real store poll occurs before registration.
        void registrationGate.then(next); return;
      }
      if (req.path === '/api/connectors/poll') {
        polls += 1;
        if (polls === 1) res.once('finish', () => { firstPollStatus = res.statusCode; releaseRegistration(); });
        else retryObserved();
      }
      next();
    });
    const { store } = attachConnectorApi(app, { dataDir: join(root, 'data'), gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
    const server = createServer(app); await listen(server);
    const origin = `http://127.0.0.1:${server.address().port}`, localPort = await unusedPort();
    t.after(async () => {
      releaseRegistration();
      if (child && child.exitCode === null && child.signalCode === null) {
        const exited = new Promise(done => child.once('exit', done)); child.kill(); await exited;
      }
      server.closeAllConnections(); await new Promise(done => server.close(done)); await store.close();
      assert.equal(dirname(resolve(root)), parent); assert.ok(resolve(root).startsWith(join(parent, 'soty-registration-test-')));
      await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    });
    const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR', 'COMSPEC'].includes(key.toUpperCase())));
    child = spawn(process.execPath, [fileURLToPath(new URL('../../scripts/soty-connector.mjs', import.meta.url))], {
      cwd: runtime, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
        TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
        SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtime,
        SOTY_CONNECTOR_PORT: String(localPort), SOTY_CONNECTOR_SERVER_URL: origin,
        SOTY_CONNECTOR_LINK_ID: 'registration_test_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'registration_test_device',
        SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0',
      },
    });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
    await retried;
    assert.equal(child.exitCode, null, `connector exited (${outputBytes} diagnostic bytes)`);
    assert.equal(firstPollStatus, 401, 'the regression must exercise a real pre-registration denial');
    assert.ok(registrations >= 1);
    const response = await fetch(`http://127.0.0.1:${localPort}/health`, { headers: { Origin: origin }, signal: AbortSignal.timeout(5000) });
    const health = await response.json();
    assert.equal(response.status, 200);
    assert.ok(health.registration.lastRegisteredAt);
    assert.equal(health.registration.connected, true, 'a recovered poll must not overwrite successful registration with the stale denial');
    assert.equal(health.registration.error, '');
  });

async function listen(server) { await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); }); }
async function unusedPort() { const server = createServer(); await listen(server); const port = server.address().port; await new Promise(done => server.close(done)); return port; }
