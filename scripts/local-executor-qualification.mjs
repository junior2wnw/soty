#!/usr/bin/env node
// Actual installed OpenCode and connector; controlled loopback model/owner.
// This never connects, claims or modifies a production device.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, readFile, writeFile, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../server/http-app.js';
import { digestArgs } from '../modules/connect/server/index.mjs';
import { glmProxyModel } from '../server/gonka-proxy.js';

const executable = process.env.SOTY_OPENCODE_E2E_PATH;
assert.ok(executable && isAbsolute(executable), 'Specify the existing installed OpenCode CLI');
const sourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const root = await mkdtemp(join(tmpdir(), 'soty-local-executor-proof-'));
const workspace = join(root, 'workspace'), data = join(root, 'root-data'), connectorData = join(root, 'connector'), dist = join(root, 'dist');
const target = join(workspace, 'qa-proof.txt'), content = 'soty isolated executor proof\n';
const owner = identity(), phone = identity(), outsider = identity(), sockets = new Set(), children = [], ports = [];
let app, server, upstream, child, connectorPort, blocked = true, modelRequests = 0, toolWrites = 0, toolReads = 0, stage = 'setup';
let toolStep = 0;
let origin, options, source, callsAfterEffect;
const runtimeReasons = new Set();
const modelToolNames = new Set();
const delay = ms => new Promise(done => setTimeout(done, ms));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
async function wait(check, label, timeout = 30000) {
  const until = Date.now() + timeout;
  while (Date.now() < until) { const value = await check(); if (value) return value; await delay(80); }
  throw new Error(`qualification_timeout_${label}`);
}
async function listen(value) { await new Promise(done => value.listen(0, '127.0.0.1', done)); ports.push(value.address().port); return `http://127.0.0.1:${value.address().port}`; }
async function json(url, options = {}) { const response = await fetch(url, { ...options, signal: AbortSignal.timeout(6000) }); return response.json(); }
async function rpc(actor, op, args = {}) {
  const post = body => json(origin + '/api/connect/rpc', { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...body }) });
  const challenge = await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }); assert.equal(challenge.ok, true);
  return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
}
function identity() { const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }); return { key: keys.privateKey,
  publicJwk: keys.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).publicKey.export({ format: 'jwk' }) }; }
function protectDelivery() {
  const store = app.locals.connectorService.store, original = store.finishJob.bind(store);
  store.finishJob = (...args) => blocked ? Promise.resolve({ ok: false, error: 'synthetic-result-delivery-blocked' }) : original(...args);
}
async function launch() {
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => /^(path|systemroot|windir|temp|tmp|userprofile|localappdata|appdata|comspec|homedrive|homepath|pathext|psmodulepath)$/iu.test(name)));
  child = spawn(process.execPath, [join(sourceRoot, 'scripts/soty-connector.mjs')], { cwd: workspace, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...env, SOTY_CONNECTOR_DATA_DIR: connectorData, SOTY_CONNECTOR_PORT: String(connectorPort), SOTY_CONNECTOR_SERVER_URL: origin,
      SOTY_CONNECTOR_LINK_ID: 'q'.repeat(43), SOTY_CONNECTOR_DEVICE_ID: 'synthetic-local-executor', SOTY_CONNECTOR_SCOPE: 'CurrentUser',
      SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0', SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_MODEL: glmProxyModel,
      SOTY_CONNECTOR_DURABLE_RESULTS: '1', SOTY_SPREADEX_BASE_URL: origin, SOTY_CONNECTOR_UPDATE_URL: origin + '/agent/manifest.json' } });
  child.on('error', () => {}); child.stdout.on('data', () => {}); child.stderr.on('data', bytes => {
    const text = String(bytes); for (const code of ['local_receipt_capacity', 'local_receipt_directory_denied', 'local_receipt_file_denied',
      'local_receipt_invalid_job', 'local_job_admission_unavailable', 'local_job_admission_revoked', 'ENOENT', 'EACCES', 'EEXIST']) if (text.includes(code)) runtimeReasons.add(code);
  }); children.push(child);
  await wait(async () => { try { return (await json(`http://127.0.0.1:${connectorPort}/health`)).ok; } catch { return false; } }, 'connector_health');
  await wait(async () => { const state = await json(origin + '/api/connectors/status?linkId=' + 'q'.repeat(43)); return state.connected; }, 'connector_registration');
}
async function stopChild() {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const done = new Promise(resolveClose => child.once('close', resolveClose)); child.kill();
  await Promise.race([done, delay(8000).then(() => { throw new Error('owned_connector_not_closed'); })]);
}
try {
  for (const directory of [workspace, connectorData, dist]) await mkdir(directory);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>Isolated executor qualification</title>');
  await writeFile(join(connectorData, 'connector-config.json'), JSON.stringify({ workspaceRoot: workspace, allowedRoots: [workspace] }), { mode: 0o600 });
  upstream = createServer(async (req, res) => {
    try {
      const parts = []; for await (const bytes of req) parts.push(bytes); const body = JSON.parse(Buffer.concat(parts));
      assert.equal(req.url, '/v1/chat/completions'); assert.equal(req.headers.authorization, 'Bearer synthetic-executor-model-key');
      for (const tool of body.tools ?? []) if (/^[A-Za-z_]{1,60}$/u.test(tool.function?.name ?? '')) modelToolNames.add(tool.function.name);
      for (const forbidden of ['bash', 'task', 'webfetch', 'websearch']) assert.ok(!body.tools?.some(tool => tool.function?.name === forbidden), 'bounded_file_executor_must_not_offer_shell_or_subagent');
      modelRequests++; assert.ok(modelRequests <= 12); let delta, finish;
      if (!body.tools?.length) {
        // OpenCode's existing title/metadata request is independent of its task.
        delta = { role: 'assistant', content: 'Isolated file verification' }; finish = 'stop';
      } else if (++toolStep <= 2) {
        const name = toolStep === 1 ? (body.tools.some(tool => tool.function.name === 'write') ? 'write' : 'apply_patch') : 'read';
        assert.ok(body.tools.some(tool => tool.function.name === name));
        if (toolStep === 1) toolWrites++; else toolReads++;
        const input = name === 'write' ? { filePath: target, content } : name === 'apply_patch'
          ? { patchText: `*** Begin Patch\n*** Add File: ${target.replaceAll('\\', '/')}\n+${content.trim()}\n*** End Patch` } : { filePath: target };
        delta = { role: 'assistant', tool_calls: [{ index: 0, id: `call_qa_${name}`, type: 'function', function: { name,
          arguments: JSON.stringify(input) } }] }; finish = 'tool_calls';
      } else {
        assert.equal(toolStep, 3, 'A recovered receipt must not invoke the task model again');
        assert.ok(body.messages.some(message => message.role === 'tool' && JSON.stringify(message).includes('soty isolated executor proof')));
        delta = { role: 'assistant', content: 'QA file written and independently read by OpenCode.' }; finish = 'stop';
      }
      const packet = (change, reason) => ({ id: 'chatcmpl-synthetic-executor', object: 'chat.completion.chunk', created: 1, model: body.model,
        choices: [{ index: 0, delta: change, finish_reason: reason }], ...(reason ? { usage: { prompt_tokens: 3, completion_tokens: 10, total_tokens: 13 } } : {}) });
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: ' + JSON.stringify(packet(delta, null)) + '\n\n'); res.write('data: ' + JSON.stringify(packet({}, finish)) + '\n\n'); res.end('data: [DONE]\n\n');
    } catch { res.writeHead(500, { 'Content-Type': 'application/json' }); res.end('{"error":{"message":"synthetic_qualification_model_denied"}}'); }
  });
  const upstreamOrigin = await listen(upstream);
  server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  origin = await listen(server);
  options = { dataDir: data, connectOrigins: [origin], appOriginTemplate: 'http://{appId}.localhost:' + server.address().port,
    appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' }, universalAppsEnabled: false, localExecutorCurrent: true,
    gonka: { apiKey: 'synthetic-executor-model-key', baseUrl: upstreamOrigin + '/v1', applicationTokens: { 'synthetic-app': 'a'.repeat(43) } } };
  app = createHttpApp(dist, options); protectDelivery();
  source = await rpc(owner, 'bootstrap', { label: 'Synthetic executor owner', encryptionPublicJwk: owner.encryptionPublicJwk }); assert.equal(source.ok, true);
  assert.equal((await rpc(outsider, 'bootstrap', { label: 'Synthetic outsider', encryptionPublicJwk: outsider.encryptionPublicJwk })).ok, true);
  const portHolder = createServer(); await new Promise(done => portHolder.listen(0, '127.0.0.1', done)); connectorPort = portHolder.address().port;
  await new Promise(done => portHolder.close(done)); ports.push(connectorPort); await launch();
  stage = 'owner_claim';
  const claim = await wait(async () => { const value = await json(`http://127.0.0.1:${connectorPort}/apps/claim`, { method: 'POST', headers: { Origin: origin } }); return value.ok && value; }, 'claim');
  assert.equal((await rpc(owner, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode })).ok, true);
  const args = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, expectedAccountId: source.accountId,
    requestId: 'isolated-executor-once', conversationId: 'isolated-executor-once', text: 'Write and read the isolated qa-proof.txt only.', cwd: workspace };
  stage = 'actual_file_effect'; const sent = await rpc(owner, 'apps.assistant.send', args); assert.equal(sent.ok, true);
  const id = sent.job.id; assert.equal(sent.job.status, 'queued'); assert.equal(sent.job.result, null);
  assert.equal((await rpc(owner, 'apps.assistant.send', args)).job.id, id);
  const resultFile = join(connectorData, 'connector-jobs', 'native-results-v1', `${id}.result.json`);
  const native = await wait(async () => { try { return JSON.parse(await readFile(resultFile, 'utf8')); } catch { return false; } }, 'durable_native_result', 60000);
  assert.equal(native.result.ok, true); assert.equal(native.result.exitCode, 0);
  assert.equal(await readFile(target, 'utf8'), content); assert.equal(toolWrites, 1); assert.equal(toolReads, 1); callsAfterEffect = modelRequests;
  assert.equal((await rpc(outsider, 'apps.agent.read', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, jobId: id })).ok, false);
  await stopChild();
  const before = app.locals.connectorService.store.state.jobs.find(job => job.id === id); assert.equal(before.result, null); assert.equal(before.attempts, 1);
  const retainedConfig = JSON.parse(await readFile(join(connectorData, 'connector-config.json'), 'utf8'));
  const auth = { linkId: retainedConfig.linkId, deviceId: retainedConfig.deviceId, connectorId: retainedConfig.installId + ':currentuser', token: retainedConfig.connectorToken };
  app.locals.connectorService.store.now = () => Date.now() + 90000;
  assert.equal((await app.locals.connectorService.store.poll(auth, 0)).jobs.length, 0);
  assert.equal(app.locals.connectorService.store.state.jobs.find(job => job.id === id).executionUncertain, true);
  stage = 'reconnect_receipt'; await app.locals.closeServices(); app = createHttpApp(dist, options); blocked = false; protectDelivery(); await launch();
  const completed = await wait(async () => { const value = await rpc(owner, 'apps.agent.read', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, jobId: id }); return value.ok && value.job.status === 'succeeded' && value; }, 'reconciled_receipt');
  assert.equal(completed.job.executionUncertain, false); assert.equal(completed.job.result.exitCode, 0); assert.equal(completed.job.attempts, 1);
  assert.equal(modelRequests, callsAfterEffect); assert.equal(await readFile(target, 'utf8'), content);
  await wait(async () => { try { return Boolean(await readFile(join(connectorData, 'connector-jobs', 'native-results-v1', `${id}.ack.json`))); } catch { return false; } }, 'ack_persisted');
  await stopChild(); stage = 'revoke_before_execution';
  const second = await rpc(owner, 'apps.assistant.send', { ...args, requestId: 'isolated-executor-revoked', conversationId: 'isolated-executor-revoked' }); assert.equal(second.ok, true);
  const enrollment = await rpc(phone, 'enrollment.start', { label: 'Synthetic owner second device', encryptionPublicJwk: phone.encryptionPublicJwk }); assert.equal(enrollment.ok, true);
  assert.equal((await rpc(owner, 'enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'synthetic.encrypted', ciphertext: 'opaque' } })).ok, true);
  assert.equal((await rpc(phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: source.accountId })).ok, true);
  assert.equal((await rpc(phone, 'device.revoke', { deviceId: source.deviceId })).ok, true);
  await launch(); await wait(async () => app.locals.connectorService.store.state.jobs.find(job => job.id === second.job.id)?.status === 'cancelled', 'revoked_not_leased');
  assert.equal(app.locals.connectorService.store.state.jobs.find(job => job.id === second.job.id).attempts, 0);
  assert.equal(modelRequests, callsAfterEffect); assert.equal(await readFile(target, 'utf8'), content);
  const proof = { schema: 'soty.local-executor-qualification.v1', qualified: true, baseRoot: 'c56b37263e1b23b9a1b2c88ecd0ffa059e7bb96d',
    actualInstalledOpenCode: true, controlledLoopbackModel: true, signedConnectAndAppsOwnership: 'synthetic-real-protocol', liveOwnerQualified: false,
    effect: { fileCount: (await readdir(workspace)).filter(name => name === 'qa-proof.txt').length, fileSha256: hash(Buffer.from(content)),
      writeToolCalls: toolWrites, readToolCalls: toolReads, modelRequests, originalExecutionAttempts: completed.job.attempts },
    guards: { durableBeforePost: true, rootRestart: true, connectorRestart: true, resultDeliveryLostBeforeCommit: true,
      executionUncertainNotReoffered: true, originalResultReconciled: true, ownerDeviceRevokeBeforeLease: true, outsiderDenied: true,
      shellSubagentAndNetworkToolsNotAdvertised: true },
    prodRootChanged: false, prodConnectorChanged: false, arbitraryCommandHttpCreated: false, shellJobsUsed: false, machinePrivilegeUsed: false, externalModelCost: 0 };
  const output = process.env.SOTY_LOCAL_EXECUTOR_EVIDENCE_FILE;
  if (output) { assert.ok(isAbsolute(output)); await writeFile(output, JSON.stringify(proof, null, 2) + '\n', { flag: 'wx', mode: 0o600 }); }
  console.log(JSON.stringify({ qualified: true, actualInstalledOpenCode: true, modelRequests, writeToolCalls: toolWrites, readToolCalls: toolReads,
    executionAttempts: completed.job.attempts, liveOwnerQualified: false, productionChanged: false }));
} catch {
  let runtimeState;
  try { const state = await json(`http://127.0.0.1:${connectorPort}/health`);
    const reason = state.registration?.error || '';
    runtimeState = { activeJob: Boolean(state.activeJob), agentAvailable: state.agent?.available,
      registrationReason: /^[a-z][a-z_-]{2,100}$/u.test(reason) ? reason : reason ? 'unclassified_redacted' : 'none' };
    const names = await readdir(join(connectorData, 'connector-jobs', 'native-results-v1')).catch(() => []);
    runtimeState.startedFiles = names.filter(name => name.endsWith('.started.json')).length;
    runtimeState.nativeResultFiles = names.filter(name => name.endsWith('.result.json')).length;
  } catch { runtimeState = { available: false }; }
  console.error(JSON.stringify({ qualified: false, stage, modelRequests, toolWrites, toolReads, modelToolNames: [...modelToolNames], runtimeState,
  runtimeReasons: [...runtimeReasons], jobs: app?.locals.connectorService.store.state.jobs.map(row => ({ status: row.status, attempts: row.attempts,
    cancelRequested: row.cancelRequested, nativeResult: Boolean(row.result), sourceActorBound: Boolean(row.sourceActorDeviceId) })), privateValuesPrinted: false })); process.exitCode = 1; }
finally {
  await stopChild(); await app?.locals.closeServices();
  if (server) { for (const socket of sockets) socket.destroy(); server.closeAllConnections(); await new Promise(done => server.close(done)); }
  if (upstream) { upstream.closeAllConnections(); await new Promise(done => upstream.close(done)); }
  assert.ok(children.every(value => value.exitCode !== null || value.signalCode !== null));
  for (const port of ports) { const probe = createServer(); await new Promise((done, reject) => { probe.once('error', reject); probe.listen(port, '127.0.0.1', done); }); await new Promise(done => probe.close(done)); }
  assert.equal(dirname(root), resolve(tmpdir())); assert.ok(root.startsWith(join(resolve(tmpdir()), 'soty-local-executor-proof-')));
  await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
