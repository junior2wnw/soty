import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { createServer, request } from 'node:http';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import vm from 'node:vm';
import { EventEmitter, once as onceEvent } from 'node:events';
import { PassThrough } from 'node:stream';
import ts from 'typescript';

const sourcePath = process.argv[2] ? resolve(process.argv[2]) : fileURLToPath(new URL('./dev.mjs', import.meta.url));
const repo = process.argv[3] ? resolve(process.argv[3]) : resolve(dirname(sourcePath), '..');
const source = await readFile(sourcePath, 'utf8'), parsed = ts.createSourceFile('dev.mjs', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const named = name => parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
const factory = named('startDevelopment');
assert.ok(factory);
const lifecycleImport = parsed.statements.find(node => ts.isImportDeclaration(node) && node.moduleSpecifier.text === './dev-lifecycle.mjs');
const lifecycleModule = lifecycleImport ? await import(new URL('./dev-lifecycle.mjs', pathToFileURL(sourcePath))) : null;
const childFile = fileURLToPath(new URL('./test-support/dev-lifecycle-child.mjs', import.meta.url));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const outcome = promise => promise.then(value => ({ value }), error => ({ error }));
async function within(promise, ms = 1000) {
  let timer; try { return await Promise.race([promise.then(value => ({ settled: true, value })), new Promise(resolve => { timer = setTimeout(() => resolve({ settled: false }), ms); })]); }
  finally { clearTimeout(timer); }
}
async function phase(promise, label, ms = 3000) {
  const result = await within(outcome(Promise.resolve(promise)), ms);
  assert.equal(result.settled, true, 'Test fixture phase did not finish: ' + label);
  if ('error' in result.value) throw result.value.error;
  return result.value.value;
}
async function cleanup(primaryFailure, ...jobs) {
  const results = await Promise.allSettled(jobs.map((job, index) => phase(Promise.resolve().then(job), 'cleanup ' + (index + 1), 4000)));
  const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
  if (primaryFailure && errors.length) throw new AggregateError([primaryFailure, ...errors], primaryFailure.message, { cause: primaryFailure });
  if (errors.length === 1) throw errors[0];
  if (errors.length) throw new AggregateError(errors, 'Owned fixture cleanup failed');
}
// Match the production development runner's platform-only inheritance. No
// provider variables, NODE_OPTIONS, application credentials or env dump.
const platformKeys = ['PATH', 'Path', 'SystemRoot', 'SYSTEMROOT', 'WINDIR', 'HOME', 'USERPROFILE', 'TMPDIR', 'TEMP', 'TMP', 'COMSPEC', 'ComSpec', 'PATHEXT'];
const fixtureEnvironment = port => ({ ...Object.fromEntries(platformKeys.filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]])), PORT: String(port) });
function observeChild(api) {
  const once = (target, event, already) => already ? Promise.resolve() : new Promise(resolve => target.once(event, resolve));
  const exited = once(api, 'exit', api.exitCode !== null || api.signalCode !== null);
  const disconnected = once(api, 'disconnect', api.channel === undefined || api.channel === null);
  const pipes = [api.stdout, api.stderr].map(stream => once(stream, 'close', !stream || stream.closed));
  // Observe each real resource. No emitted close, private close counters, or
  // exit-only substitute for still-open pipes/IPC.
  const closed = Promise.all([exited, disconnected, ...pipes]).then(() => undefined);
  return { exited, closed };
}
function spawnChild(port = 0, options = {}) {
  const api = fork(childFile, [], { ...options, execArgv: [], env: fixtureEnvironment(port), windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let stderr = '', readyFinished = false;
  api.stdout.resume(); api.stderr.setEncoding('utf8');
  api.stderr.on('data', text => { stderr = (stderr + text).slice(-2048); });
  const physical = observeChild(api);
  const ready = new Promise((resolve, reject) => {
    const describe = reason => new Error('Owned public fixture ' + reason + (stderr ? '\n' + stderr : ''));
    function finish(error, message) {
      if (readyFinished) return; readyFinished = true;
      api.off('message', onMessage); api.off('exit', onExit);
      if (error) reject(error); else resolve(message);
    }
    const onMessage = message => { if (message?.type === 'soty:ready' && Number.isInteger(message.port)) finish(undefined, message); };
    const onExit = (code, signal) => finish(describe('exited before readiness (code=' + code + ', signal=' + signal + ')'));
    api.on('message', onMessage); api.once('exit', onExit);
    // Keep observing child errors after readiness too; the production code
    // still receives its own error event. Diagnostics contain this static
    // fixture's bounded stderr only, never its environment values.
    api.on('error', error => finish(describe('could not start (' + (error.code || error.name) + ')')));
  });
  void ready.catch(() => {});
  return { api, ...physical, ready };
}
async function childResource(port = 0) {
  const child = spawnChild(port);
  try { const ready = await phase(child.ready, 'owned child readiness'); return { ...child, port: ready.port }; }
  catch (error) {
    try { await rescue(child); } catch (cleanupError) { throw new AggregateError([error, cleanupError], error.message, { cause: error }); }
    throw error;
  }
}
async function rescue(child) {
  if (!child) return;
  if (child.api.exitCode === null && child.api.signalCode === null) { if (child.api.connected) child.api.disconnect(); else child.api.kill('SIGTERM'); }
  if (!(await within(child.closed, 1000)).settled) {
    child.api.kill('SIGKILL');
    assert.equal((await within(child.closed, 2000)).settled, true, 'Owned fixture did not close after SIGKILL');
  }
}
function lifecycle(child, frontend, signal) {
  if (lifecycleModule) return lifecycleModule.createDevelopmentLifecycle({ api: child.api, frontend: () => frontend, signal });
  // Baseline: execute the exact old nested close closure, without copying it.
  const original = factory.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'close');
  assert.ok(original, 'The actual baseline close exists');
  let settleDone, resolveExit;
  const done = new Promise(resolve => { settleDone = resolve; }), exited = new Promise(resolve => { resolveExit = resolve; });
  const port = vm.createContext({ exports: {}, vite: frontend, api: child.api, input: { signal }, onAbort() {}, closing: undefined,
    childExited: false, exited, settleDone, setTimeout, clearTimeout, Promise });
  child.api.once('exit', () => { port.childExited = true; resolveExit(); });
  vm.runInContext(print(original) + '\nexports.close = close;', port);
  return { close: port.exports.close, done };
}
function actualStart({ forkPort, vitePort }) {
  const declarations = ['portNumber', 'developmentEnvironment', 'startDevelopment'].map(name => { const node = named(name); assert.ok(node); return print(node); });
  const platform = parsed.statements.find(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(value => value.name.getText(parsed) === 'platformEnvironment'));
  const exports = {};
  const code = ts.transpileModule(print(platform) + '\n' + declarations.join('\n'), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
  vm.runInNewContext(code, { exports, rootDir: repo, process: { env: {} }, mkdir: async () => {}, join, resolve,
    fork: forkPort, createViteServer: vitePort, fetch, AbortController, AbortSignal, setTimeout, clearTimeout, console,
    ...(lifecycleModule || {}) });
  return exports.startDevelopment;
}

test('actual startup aborts before dev assignment and closes a later frontend instead of publishing it', { timeout: 10000 }, async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'soty-dev-start-cancel-')), gate = deferred(), entered = deferred(), controller = new AbortController();
  const reason = new Error('Synthetic cancellation before assignment');
  let child, pending, primaryFailure, frontendCloses = 0;
  const frontend = { async close() { frontendCloses++; }, async listen() { assert.fail('Cancelled startup must not publish a listener'); } };
  const holder = createServer();
  try {
    await phase(new Promise((resolve, reject) => { holder.once('error', reject); holder.listen(0, '127.0.0.1', resolve); }), 'reserve startup port');
    const port = holder.address().port; await phase(new Promise(resolve => holder.close(resolve)), 'release startup port');
    const start = actualStart({
      forkPort(_file, _args, options) { child = spawnChild(port, options); return child.api; },
      vitePort() { entered.resolve(); return gate.promise; },
    });
    pending = outcome(start({ port: port > 65533 ? port - 2 : port + 1, apiPort: port, connectorPort: port > 65533 ? port - 1 : port + 2, dataDir, quiet: true, signal: controller.signal }));
    const entry = await phase(Promise.race([
      entered.promise.then(() => ({ entered: true })),
      pending.then(result => ({ premature: result })),
    ]), 'actual startup reaches the frontend factory');
    if (entry.premature) {
      if ('error' in entry.premature) throw entry.premature.error;
      assert.fail('Actual startup returned before entering its frontend factory');
    }
    controller.abort(reason);
    const early = await within(pending);
    assert.equal(early.settled, true, 'Cancellation cannot wait for the held Vite factory');
    assert.equal(early.value.error, reason);
    assert.equal((await within(child.closed)).settled, true);
  } catch (error) { primaryFailure = error; throw error; } finally {
    controller.abort(reason); gate.resolve(frontend);
    holder.closeAllConnections();
    await cleanup(primaryFailure,
      () => rescue(child),
      () => pending,
      () => holder.listening ? new Promise(resolve => holder.close(resolve)) : undefined,
      async () => { assert.equal(dirname(resolve(dataDir)), resolve(tmpdir())); await rm(dataDir, { recursive: true, force: true }); },
    );
  }
  await phase(new Promise(resolve => setImmediate(resolve)), 'late frontend microtasks');
  assert.equal(frontendCloses, 1, 'The late owned frontend is closed exactly once');
});

for (const primary of [false, true]) {
  test('a rejected frontend close still closes the real child and settles done' + (primary ? ' with the primary failure retained' : ''), { timeout: 10000 }, async () => {
    const child = await childResource(), frontendError = new Error('Synthetic frontend close failure'), initialError = primary ? new Error('Synthetic original failure') : undefined;
    let primaryFailure;
    try {
      const running = lifecycle(child, { close() { return Promise.reject(frontendError); } });
      const result = await phase(outcome(running.close(initialError)), 'rejected frontend close settlement'), childClosed = await within(child.closed), done = await within(running.done);
      assert.equal(childClosed.settled, true); assert.equal(done.settled, true);
      if (primary) {
        assert.equal(result.error?.cause, initialError); assert.ok(result.error?.errors.includes(frontendError)); assert.equal(done.value, result.error);
      } else { assert.equal(result.error, frontendError); assert.equal(done.value, frontendError); }
    } catch (error) { primaryFailure = error; throw error; } finally { await cleanup(primaryFailure, () => rescue(child)); }
  });
}

test('frontend drain and backend stop run together over a real held HTTP proxy', { timeout: 10000 }, async () => {
  const child = await childResource(), upstreamSeen = deferred(), requests = new Set();
  let proxy, client, pending, primaryFailure;
  try {
    child.api.on('message', message => { if (message?.type === 'held-request') upstreamSeen.resolve(); });
    proxy = createServer((req, res) => {
      const upstream = request({ hostname: '127.0.0.1', port: child.port, path: '/held' }, upstreamResponse => upstreamResponse.pipe(res));
      requests.add(upstream); upstream.once('error', () => res.destroy()); upstream.once('close', () => requests.delete(upstream)); upstream.end();
    });
    await phase(new Promise((resolve, reject) => { proxy.once('error', reject); proxy.listen(0, '127.0.0.1', resolve); }), 'proxy readiness');
    const frontendPort = proxy.address().port; client = request({ hostname: '127.0.0.1', port: frontendPort, path: '/held' });
    client.on('error', () => {}); client.end(); await phase(upstreamSeen.promise, 'real upstream held request');
    let closes = 0; const frontend = { close() { closes++; return new Promise((resolve, reject) => proxy.close(error => error ? reject(error) : resolve())); } };
    const running = lifecycle(child, frontend); pending = outcome(running.close());
    const early = await within(pending);
    assert.equal(early.settled, true, 'The frontend drain cannot prevent stopping the backend it is awaiting');
    assert.equal(early.value.error, undefined); assert.equal(await phase(running.done, 'proxy done'), null); assert.equal(closes, 1);
    assert.equal(proxy.listening, false); assert.notEqual(child.api.exitCode, null);
  } catch (error) { primaryFailure = error; throw error; } finally {
    client?.destroy(); requests.forEach(req => req.destroy()); proxy?.closeAllConnections();
    await cleanup(primaryFailure,
      () => rescue(child),
      () => proxy?.listening ? new Promise(resolve => proxy.close(resolve)) : undefined,
      () => pending,
    );
  }
});

test('a held frontend close cannot retain the real API child or report successful done', { timeout: 10000 }, async () => {
  const child = await childResource(), gate = deferred();
  let pending, primaryFailure;
  try {
    const running = lifecycle(child, { close: () => gate.promise }); pending = outcome(running.close());
    const childClosed = await within(child.closed), result = await within(pending, 3000), done = await within(running.done);
    assert.equal(childClosed.settled, true); assert.equal(result.settled, true); assert.equal(done.settled, true);
    assert.equal(result.value.error?.code, 'DEV_UI_CLOSE_TIMEOUT'); assert.equal(done.value, result.value.error);
  } catch (error) { primaryFailure = error; throw error; } finally { gate.resolve(); await cleanup(primaryFailure, () => rescue(child), () => pending); }
});

test('normal close is idempotent and closes both resources with null done', { timeout: 10000 }, async () => {
  const child = await childResource(); let closes = 0, primaryFailure;
  try {
    const running = lifecycle(child, { async close() { closes++; } });
    const first = running.close(), second = running.close(); assert.equal(first, second);
    await phase(first, 'normal close'); assert.equal(await phase(running.done, 'normal done'), null); assert.equal(closes, 1); assert.equal((await within(child.closed)).settled, true);
  } catch (error) { primaryFailure = error; throw error; } finally { await cleanup(primaryFailure, () => rescue(child)); }
});

test('API exit and IPC disconnect cannot complete shutdown while either real owned stream remains open', { timeout: 10000 }, async () => {
  // Controlled ChildProcess lifecycle, actual independently closing Node
  // streams. Physical process/stdio behavior is covered by the first six
  // cases; OS grandchild inheritance is a separate observational probe.
  const api = Object.assign(new EventEmitter(), { exitCode: null, signalCode: null, connected: true, channel: {},
    stdout: new PassThrough(), stderr: new PassThrough() });
  const disconnectEntered = deferred();
  let pending, primaryFailure, kills = 0;
  api.disconnect = () => { api.connected = false; api.channel = null; api.emit('disconnect'); disconnectEntered.resolve(); };
  api.kill = () => { kills++; return false; };
  const child = { api, ...observeChild(api) };
  api.stdout.resume(); api.stderr.resume();
  const stdoutClosed = onceEvent(api.stdout, 'close'), stderrClosed = onceEvent(api.stderr, 'close');
  const complete = () => {
    api.stdout.destroy(); api.stderr.destroy();
    if (api.exitCode === null) { api.exitCode = 0; api.emit('exit', 0, null); }
    api.channel = null; api.connected = false; api.emit('disconnect');
  };
  try {
    const running = lifecycle(child, { async close() {} }); pending = outcome(running.close());
    await phase(disconnectEntered.promise, 'owned IPC disconnect');
    api.exitCode = 0; api.emit('exit', 0, null); await phase(child.exited, 'API exit observed');
    assert.equal(api.connected, false); assert.equal(api.channel, null);
    assert.equal(api.stdout.closed, false); assert.equal(api.stderr.closed, false);
    assert.equal((await within(pending, 150)).settled, false, 'Exit alone cannot complete shutdown with real streams still open');
    assert.equal((await within(running.done, 150)).settled, false, 'done must wait for the actual owned streams');
    api.stdout.end('owned stdout'); await phase(stdoutClosed, 'actual stdout end and close');
    assert.equal(api.stdout.closed, true); assert.equal(api.stderr.closed, false);
    assert.equal((await within(pending, 150)).settled, false, 'One closed stream cannot retire the other');
    assert.equal((await within(running.done, 150)).settled, false);
    api.stderr.end('owned stderr'); await phase(stderrClosed, 'actual stderr end and close');
    const result = await phase(pending, 'shutdown after both actual streams close');
    assert.equal(result.error, undefined); assert.equal(await phase(running.done, 'done after actual stream closure'), null);
    assert.equal(api.stdout.closed, true); assert.equal(api.stderr.closed, true); assert.equal(kills, 0);
    assert.equal((await within(child.closed)).settled, true);
  } catch (error) { primaryFailure = error; throw error; } finally {
    await cleanup(primaryFailure, () => complete(), () => stdoutClosed, () => stderrClosed, () => pending);
  }
});

test('connected=false with pending IPC cannot complete shutdown after exit and real stream closure', { timeout: 10000 }, async () => {
  // Only the ChildProcess lifecycle state is controlled here. The runtime
  // close method is genuine and both stream close events are real.
  const api = Object.assign(new EventEmitter(), { exitCode: null, signalCode: null, connected: true, channel: {},
    stdout: new PassThrough(), stderr: new PassThrough() });
  const disconnectEntered = deferred();
  let disconnects = 0, kills = 0, pending, primaryFailure;
  api.disconnect = () => { disconnects++; api.connected = false; disconnectEntered.resolve(); }; // queue not drained yet
  api.kill = () => { kills++; return false; };
  const child = { api, ...observeChild(api) };
  api.stdout.resume(); api.stderr.resume();
  const streamsClosed = Promise.all([onceEvent(api.stdout, 'close'), onceEvent(api.stderr, 'close')]);
  const complete = () => {
    api.stdout.destroy(); api.stderr.destroy();
    if (api.exitCode === null) { api.exitCode = 0; api.emit('exit', 0, null); }
    api.channel = null; api.connected = false; api.emit('disconnect');
  };
  try {
    const running = lifecycle(child, { async close() {} }); pending = outcome(running.close());
    await phase(disconnectEntered.promise, 'IPC disconnect requested with queue still pending');
    assert.equal(disconnects, 1); assert.equal(api.connected, false); assert.notEqual(api.channel, null);
    api.exitCode = 0; api.emit('exit', 0, null); api.stdout.destroy(); api.stderr.destroy();
    await phase(streamsClosed, 'real owned stream closure with IPC queue pending');
    assert.equal(api.stdout.closed, true); assert.equal(api.stderr.closed, true);
    assert.equal((await within(pending, 150)).settled, false, 'connected=false alone does not prove IPC has drained');
    assert.equal((await within(running.done, 150)).settled, false);
    api.channel = null; api.emit('disconnect');
    const result = await phase(pending, 'shutdown after actual disconnect');
    assert.equal(result.error, undefined); assert.equal(await phase(running.done, 'done after IPC disconnect'), null);
    assert.equal(kills, 0); assert.equal((await within(child.closed)).settled, true);
  } catch (error) { primaryFailure = error; throw error; } finally {
    await cleanup(primaryFailure, () => complete(), () => streamsClosed, () => pending);
  }
});
