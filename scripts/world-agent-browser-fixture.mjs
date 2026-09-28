import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { access, lstat, mkdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../server/http-app.js';
import { createSampleApp } from '../modules/apps/examples/sample-app.mjs';

// Explicit browser-acceptance fixture. A real installed OpenCode executes its
// write tool against a controlled local inference response. The shopping app is
// already running as a test fixture. No production provider is contacted, and
// this script never bootstraps an account, claims a device or submits a UI job.
const repository = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtureRoot = join(repository, 'output', 'world-validation-20260928', 'agent-fixture');
const runtime = join(fixtureRoot, 'runtime'), workspace = join(fixtureRoot, 'workspace');
const data = join(fixtureRoot, 'data'), stopPath = join(fixtureRoot, 'STOP');
const statusPath = join(fixtureRoot, 'status.json');
const origin = 'http://localhost:5310', connectorPort = 49430;
const deviceId = 'world-agent-fixture-device';
const executable = process.env.SOTY_OPENCODE_E2E_PATH || join(process.env.LOCALAPPDATA || '', 'soty-connector', 'opencode-runtime', '1.18.15', 'opencode.exe');

if (process.argv.includes('--stop')) {
  await mkdir(fixtureRoot, { recursive: true });
  await writeFile(stopPath, 'stop\n');
  console.log('Agent browser fixture: stop requested.');
} else {
  await run().catch(error => {
    // Child diagnostics and connector configuration can contain credentials.
    // Report only the fixed error category, never raw request/process content.
    console.error('Agent browser fixture failed:', safeCode(error));
    process.exitCode = 1;
  });
}

async function run() {
  if (!isAbsolute(executable)) throw new Error('installed_executable_required');
  await access(executable); await access(join(repository, 'dist', 'index.html'));
  await mkdir(runtime, { recursive: true }); await mkdir(workspace, { recursive: true });
  await rm(stopPath, { force: true });
  const configPath = join(runtime, 'connector-config.json');
  try {
    const stored = JSON.parse(await readFile(configPath, 'utf8'));
    if (resolve(stored.workspaceRoot || '') !== workspace || !Array.isArray(stored.allowedRoots)
      || stored.allowedRoots.length !== 1 || resolve(stored.allowedRoots[0]) !== workspace) throw new Error('fixture_workspace_mismatch');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    await writeFile(configPath, JSON.stringify({ workspaceRoot: workspace, allowedRoots: [workspace], linkId: randomBytes(32).toString('base64url') }), { mode: 0o600 });
  }

  let app, sample, inference, server, child, stopTimer, stopping = false, outputBytes = 0;
  const modelKey = randomBytes(32).toString('base64url');
  const issued = new Set();
  const progress = { state: 'starting', origin, connectorPort, productionInference: false, model: 'controlled-local-fixture',
    sampleAlreadyRunning: true, pid: process.pid, startedAt: new Date().toISOString(), issuedToolCalls: 0 };
  const saveStatus = () => writeFile(statusPath, JSON.stringify(progress, null, 2));
  async function stop() {
    if (stopping) return;
    stopping = true; clearInterval(stopTimer);
    if (child && child.exitCode === null && child.signalCode === null) {
      if (process.platform === 'win32') {
        await new Promise(done => {
          const killer = spawn('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' });
          killer.once('exit', done); killer.once('error', done);
        });
      } else child.kill('SIGTERM');
    }
    app?.locals.closeServices();
    await Promise.all([sample?.close(), close(inference), close(server)]);
    progress.state = 'stopped'; progress.stoppedAt = new Date().toISOString(); progress.connectorOutputBytes = outputBytes;
    await saveStatus();
    console.log('Agent browser fixture stopped.');
  }
  process.once('SIGINT', () => void stop()); process.once('SIGTERM', () => void stop());
  try {
    sample = await createSampleApp();
    progress.samplePort = sample.port;
    inference = createServer(async (req, res) => {
      if (req.method !== 'POST' || req.url !== '/v1/chat/completions' || req.headers.authorization !== `Bearer ${modelKey}`) {
        res.writeHead(401); res.end(); return;
      }
      try {
        const parts = []; let length = 0;
        for await (const chunk of req) { length += chunk.length; if (length > 4 * 1024 * 1024) throw new Error('fixture_request_too_large'); parts.push(chunk); }
        const body = JSON.parse(Buffer.concat(parts).toString('utf8'));
        const writeTool = body.tools?.find(tool => tool.function?.name === 'write');
        let message = { role: 'assistant', content: 'Локальное тестовое приложение готово. Манифест сохранён.' }, finish = 'stop';
        if (writeTool) {
          const job = await currentJob();
          const attempt = `${job.id}:${job.attempts}`;
          if (!issued.has(attempt)) {
            const folder = await realpath(join(workspace, 'Soty Apps', job.id));
            if (!within(await realpath(workspace), folder)) throw new Error('fixture_workspace_escape');
            const metadata = join(folder, '.soty');
            try {
              const info = await lstat(metadata);
              if (!info.isDirectory() || info.isSymbolicLink() || !within(folder, await realpath(metadata))) throw new Error('fixture_metadata_escape');
            } catch (error) { if (error.code !== 'ENOENT') throw error; }
            issued.add(attempt);
            progress.lastJobId = job.id; progress.issuedToolCalls += 1;
            progress.lastToolAt = new Date().toISOString(); await saveStatus();
            // Keep "pending" visible and cross two real cancellation-watch ticks.
            await new Promise(done => setTimeout(done, 2600));
            message = { role: 'assistant', content: null, tool_calls: [{ id: `fixture_write_${job.attempts}`, type: 'function', function: {
              name: 'write', arguments: JSON.stringify({ filePath: join(metadata, 'app.json'), content: JSON.stringify({
                schema: 'soty.local-app.v1', name: 'Покупки · тест ИИ', port: sample.port, entryPath: '/',
              }) }),
            } }] };
            finish = 'tool_calls';
          }
        }
        if (res.destroyed || stopping) return;
        const base = { id: 'chatcmpl-soty-browser-fixture', created: 1, model: body.model };
        if (body.stream === false) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
          res.end(JSON.stringify({ ...base, object: 'chat.completion', choices: [{ index: 0, message, finish_reason: finish }] }));
        } else {
          res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store' });
          const delta = { ...message, ...(message.tool_calls ? { tool_calls: message.tool_calls.map((tool, index) => ({ index, ...tool })) } : {}) };
          res.write(`data: ${JSON.stringify({ ...base, object: 'chat.completion.chunk', choices: [{ index: 0, delta, finish_reason: null }] })}\n\n`);
          res.write(`data: ${JSON.stringify({ ...base, object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: finish }] })}\n\n`);
          res.end('data: [DONE]\n\n');
        }
      } catch (error) {
        progress.lastError = safeCode(error); await saveStatus();
        if (!res.headersSent && !res.destroyed) { res.writeHead(500, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: { message: 'fixture_unavailable' } })); }
      }
    });
    await listen(inference, 0);
    server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
    await listen(server, 5310);
    app = createHttpApp(join(repository, 'dist'), { dataDir: data, connectOrigins: [origin], localConnectorPort: connectorPort,
      appOriginTemplate: 'http://{appId}.localhost:5310',
      gonka: { baseUrl: `http://127.0.0.1:${inference.address().port}/v1`, apiKey: modelKey } });
    server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });

    const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR', 'COMSPEC'].includes(key.toUpperCase())));
    child = spawn(process.execPath, [join(repository, 'scripts', 'soty-connector.mjs')], {
      cwd: workspace, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
        TEMP: fixtureRoot, TMP: fixtureRoot, USERPROFILE: fixtureRoot, APPDATA: join(fixtureRoot, 'appdata'), LOCALAPPDATA: join(fixtureRoot, 'localappdata'),
        SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtime,
        SOTY_CONNECTOR_PORT: String(connectorPort), SOTY_CONNECTOR_SERVER_URL: origin,
        SOTY_CONNECTOR_DEVICE_ID: deviceId, SOTY_CONNECTOR_DEVICE_NICK: 'Ноутбук · тест ИИ',
        SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0',
      },
    });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
    child.once('error', () => { progress.lastError = 'connector_start_failed'; void stop(); });
    child.once('exit', () => { if (!stopping) { progress.lastError = 'connector_exited'; void stop(); } });
    await waitFor(async () => {
      if (stopping) throw new Error('connector_start_failed');
      try {
        const response = await fetch(`http://127.0.0.1:${connectorPort}/health`, { headers: { Origin: origin }, signal: AbortSignal.timeout(1500) });
        const value = await response.json();
        // The first poll can precede registration and leave an old diagnostic
        // until the next heartbeat. A completed registration is the readiness
        // event; browser claim still checks the live app channel itself.
        return response.ok && value.ok && value.registration?.lastRegisteredAt && value.agent?.available;
      } catch { return false; }
    }, 30_000);
    progress.state = 'ready'; progress.connectorPid = child.pid; await saveStatus();
    stopTimer = setInterval(() => { void access(stopPath).then(stop).catch(() => undefined); }, 400);
    console.log(JSON.stringify({ state: 'ready', url: origin, connectorPort, samplePort: sample.port,
      productionInference: false, realOpenCode: true, stop: 'node scripts/world-agent-browser-fixture.mjs --stop' }));
  } catch (error) {
    progress.lastError = safeCode(error); await stop(); throw error;
  }

  async function currentJob() {
    const stored = JSON.parse(await readFile(join(data, 'connector-store.json'), 'utf8'));
    const jobs = stored.jobs?.filter(job => job.ownerAccountId && job.deviceId === deviceId && job.kind === 'agent'
      && job.input?.output === 'local-app' && !job.input?.cwd && ['leased', 'running'].includes(job.status)
      && /^job_[a-f0-9]{32}$/u.test(job.id));
    if (!Array.isArray(jobs) || jobs.length !== 1) throw new Error('fixture_active_job_ambiguous');
    return jobs[0];
  }
}

function within(base, target) { const difference = relative(base, target); return difference === '' || difference !== '..' && !difference.startsWith('..' + sep) && !isAbsolute(difference); }
function safeCode(error) { return typeof error?.code === 'string' && /^[A-Za-z0-9_]{1,80}$/u.test(error.code) ? error.code : /^[a-z_]{1,80}$/u.test(error?.message || '') ? error.message : 'fixture_error'; }
async function listen(server, port) { await new Promise((done, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', done); }); }
async function close(server) { if (!server?.listening) return; server.closeAllConnections(); await new Promise(done => server.close(done)); }
async function waitFor(check, timeout) { const deadline = Date.now() + timeout; while (Date.now() < deadline) { if (await check()) return; await new Promise(done => setTimeout(done, 200)); } throw new Error('fixture_start_timeout'); }
