import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../http-app.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';

const executable = process.env.SOTY_OPENCODE_E2E_PATH;

// Opt-in: uses the real installed OpenCode executable with an explicitly controlled
// local inference fixture. No production inference, credentials or provider costs.
test('real OpenCode executes an account-owned app job past its cancellation watchdog and returns a validated proposal',
  { skip: !executable, timeout: 100_000 }, async t => {
    const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-owned-opencode-'));
    const runtimeDir = join(root, 'runtime'), dist = join(root, 'dist');
    await mkdir(runtimeDir); await mkdir(dist); await writeFile(join(dist, 'index.html'), '<title>Test shell</title>');
    await writeFile(join(runtimeDir, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtimeDir, allowedRoots: [runtimeDir] }));
    let jobId = '', toolIssued = false, mainRequests = 0, app, child;
    const sample = createServer((_req, res) => { res.setHeader('Content-Type', 'text/html'); res.end('<title>Real local project</title>Local app'); });
    await listen(sample); const samplePort = sample.address().port;
    const inference = createServer(async (req, res) => {
      try {
        const chunks = []; for await (const chunk of req) chunks.push(chunk);
        const body = JSON.parse(Buffer.concat(chunks));
        const writeTool = body.tools?.find(tool => tool.function?.name === 'write');
        let message = { role: 'assistant', content: 'Local app ready.' }, finish = 'stop';
        if (writeTool && !toolIssued && jobId) {
          toolIssued = true; mainRequests++;
          // The former shared-link status bug aborted this exact account-owned task
          // after one second. Keep the real CLI alive across two watchdog checks.
          await new Promise(done => setTimeout(done, 2300));
          message = { role: 'assistant', content: null, tool_calls: [{ id: 'call_write_manifest', type: 'function', function: {
            name: 'write', arguments: JSON.stringify({ filePath: join(runtimeDir, 'Soty Apps', jobId, '.soty', 'app.json'),
              content: JSON.stringify({ schema: 'soty.local-app.v1', name: 'Runtime test app', port: samplePort, entryPath: '/' }) }),
          } }] }; finish = 'tool_calls';
        }
        const base = { id: 'chatcmpl-local-owned-runtime-test', created: 1, model: body.model };
        if (body.stream === false) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ...base, object: 'chat.completion', choices: [{ index: 0, message, finish_reason: finish }] }));
        } else {
          res.writeHead(200, { 'Content-Type': 'text/event-stream' });
          const delta = { ...message, ...(message.tool_calls ? { tool_calls: message.tool_calls.map((tool, index) => ({ index, ...tool })) } : {}) };
          res.write(`data: ${JSON.stringify({ ...base, object: 'chat.completion.chunk', choices: [{ index: 0, delta, finish_reason: null }] })}\n\n`);
          res.write(`data: ${JSON.stringify({ ...base, object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: finish }] })}\n\n`);
          res.end('data: [DONE]\n\n');
        }
      } catch { res.writeHead(500); res.end(); }
    });
    await listen(inference);
    const localPort = await unusedPort();
    const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
    await listen(server); const origin = `http://127.0.0.1:${server.address().port}`;
    app = createHttpApp(dist, { dataDir: join(root, 'data'), connectOrigins: [origin], localConnectorPort: localPort,
      appOriginTemplate: `http://{appId}.localhost:${server.address().port}`,
      gonka: { baseUrl: `http://127.0.0.1:${inference.address().port}/v1`, apiKey: 'local-test-provider-key' } });
    server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
    t.after(async () => {
      if (child && child.exitCode === null) {
        const exited = new Promise(done => child.once('exit', done)); child.kill();
        let timer; await Promise.race([exited, new Promise(done => { timer = setTimeout(done, 4000); })]); clearTimeout(timer);
      }
      await app?.locals.closeServices();
      await Promise.all([sample, inference, server].map(item => { item.closeAllConnections(); return new Promise(done => item.close(done)); }));
      assert.equal(dirname(resolve(root)), parent); assert.ok(resolve(root).startsWith(join(parent, 'soty-owned-opencode-')));
      await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    });
    const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH','PATHEXT','SYSTEMROOT','WINDIR','COMSPEC'].includes(key.toUpperCase())));
    child = spawn(process.execPath, [fileURLToPath(new URL('../../scripts/soty-connector.mjs', import.meta.url))], {
      cwd: runtimeDir, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
        TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
        SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtimeDir,
        SOTY_CONNECTOR_PORT: String(localPort), SOTY_CONNECTOR_SERVER_URL: origin,
        SOTY_CONNECTOR_LINK_ID: 'owned_runtime_test_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'owned_runtime_host',
        SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0',
      },
    });
    // Count, but never print process content or credential-bearing config.
    let outputBytes = 0; for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
    const claim = await waitFor(async () => {
      assert.equal(child.exitCode, null, `connector exited (${outputBytes} diagnostic bytes)`);
      try { const result = await post(`http://127.0.0.1:${localPort}/apps/claim`, {}, origin); return result.ok ? result : null; } catch { return null; }
    });
    const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    const publicJwk = signing.publicKey.export({ format: 'jwk' });
    const rpc = async (op, args = {}) => {
      const endpoint = `${origin}/api/connect/rpc`;
      const challenge = await post(endpoint, { protocol: 1, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }, origin);
      assert.equal(challenge.ok, true, challenge.error?.code);
      const value = await post(endpoint, { protocol: 1, op, args, proof: { challengeId: challenge.challengeId, publicJwk,
        signature: sign('sha256', Buffer.from(challenge.message), { key: signing.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } }, origin);
      assert.equal(value.ok, true, value.error?.code); return value;
    };
    await rpc('bootstrap', { label: 'Owned OpenCode test', encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) });
    const ids = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId };
    await rpc('apps.claim', { ...ids, claimCode: claim.claimCode });
    const created = await rpc('apps.agent.create', { ...ids, requestId: 'owned-runtime-request', text: 'Create the requested local app manifest using the write tool.' });
    jobId = created.job.id;
    const completed = await waitFor(async () => { const value = await rpc('apps.agent.read', { ...ids, jobId }); return value.done ? value : null; }, 75_000);
    assert.equal(toolIssued, true, 'the actual OpenCode tool call must be requested by the local model fixture');
    assert.equal(mainRequests, 1);
    assert.equal(completed.job.status, 'succeeded', `job ended ${completed.job.status}; result=${completed.job.result?.text?.slice(-240)}`);
    assert.equal(completed.job.result.appProposal?.sourceJobId, jobId);
    assert.equal(completed.job.result.appProposal?.port, samplePort);
    const manifest = JSON.parse(await readFile(join(runtimeDir, 'Soty Apps', jobId, '.soty', 'app.json'), 'utf8'));
    assert.equal(manifest.name, 'Runtime test app');
    const registered = await rpc('apps.register', { ...ids, name: manifest.name, port: manifest.port, entryPath: manifest.entryPath, grants: {} });
    assert.equal((await rpc('apps.launch', { appId: registered.app.id })).launchUrl.includes(registered.app.id), true);
  });

async function listen(server) { await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); }); }
async function unusedPort() { const server = createServer(); await listen(server); const port = server.address().port; await new Promise(done => server.close(done)); return port; }
async function post(url, body, origin) { const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin }, body: JSON.stringify(body), signal: AbortSignal.timeout(8000) }); return response.json(); }
async function waitFor(check, timeout = 20_000) { const until = Date.now() + timeout; while (Date.now() < until) { const result = await check(); if (result) return result; await new Promise(done => setTimeout(done, 150)); } throw new Error('runtime integration timeout'); }
