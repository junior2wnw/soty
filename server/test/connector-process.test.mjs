import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { EventEmitter, getEventListeners } from 'node:events';
import vm from 'node:vm';
// Compile the EXACT existing shared production function, without importing the
// CLI's startup/config/network side effects. No copied runner implementation.
const source = await readFile(new URL('../../scripts/soty-connector.mjs', import.meta.url), 'utf8');
const start = source.indexOf('function runChild(command, args, options) {');
const end = source.indexOf('\nfunction createSpreadExJsonlWorker(', start);
assert.ok(start > 0 && end > start);
const actualFunctionSource = source.slice(start, end);
const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
function fixture(t) {
  const children = [], timers = new Set(); let kills = 0;
  const clock = { setTimeout(callback, wait) { const id = setTimeout(() => { timers.delete(id); callback(); }, wait); timers.add(id); return id; },
    clearTimeout(id) { timers.delete(id); clearTimeout(id); } };
  class Child extends EventEmitter {
    stdout = new EventEmitter(); stderr = new EventEmitter();
    stdin = { end: value => { this.input = value; }, destroy() {} };
    kill() {}
    constructor(command, args, options) { super(); this.command = command; this.args = args; this.options = options; }
    finish(code = 0) { this.emit('close', code, code === 0 ? null : 'SIGTERM'); }
  }
  const run = new vm.Script(actualFunctionSource + '\nrunChild', { filename: 'actual-soty-connector-runChild-source.mjs' }).runInNewContext({
    setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    spawn(command, args, options) { const child = new Child(command, args, options); children.push(child); return child; },
    childEnv: () => ({ SOTY_TEST_ONLY: 'synthetic' }), maxResultChars: 1000000,
    appendBounded: (value, extra, limit) => (value + extra).slice(-limit),
    killProcessTree(child) { kills++; queueMicrotask(() => child.finish(130)); },
  });
  t.after(() => { for (const id of timers) clearTimeout(id); });
  return { run, children, timers, kills: () => kills, options: signal => ({ signal, cwd: 'synthetic-workspace', timeoutMs: 5000, input: 'synthetic prompt' }) };
}
function cancelled(result) { assert.equal(result.exitCode, 130); assert.equal(result.stdout, ''); assert.equal(result.stderr, ''); }
test('already-aborted owner signal returns cancellation without any native spawn or timer', async t => {
  const f = fixture(t), owner = new AbortController(); owner.abort();
  const result = await f.run('synthetic-executable', ['synthetic'], f.options(owner.signal));
  cancelled(result); assert.equal(f.children.length, 0); assert.equal(f.kills(), 0); assert.equal(f.timers.size, 0);
  assert.equal(getEventListeners(owner.signal, 'abort').length, 0);
});
test('cancellation during awaited setup is rechecked at the actual shared spawn boundary', async t => {
  const f = fixture(t), owner = new AbortController(), setup = defer(), entered = defer();
  const pending = (async () => { entered.resolve(); await setup.promise;
    return f.run('synthetic-executable', ['synthetic'], f.options(owner.signal)); })();
  await entered.promise; owner.abort(); setup.resolve(); const result = await pending;
  cancelled(result); assert.equal(f.children.length, 0); assert.equal(f.kills(), 0); assert.equal(f.timers.size, 0);
});
test('normal completion and abort after spawn preserve existing result, tree cleanup and listener/timer release', async t => {
  const f = fixture(t), owner = new AbortController(), normal = f.run('synthetic-executable', ['one'], f.options(owner.signal));
  assert.equal(f.children.length, 1); const child = f.children[0]; assert.equal(child.options.windowsHide, true); assert.equal(child.input, 'synthetic prompt');
  child.stdout.emit('data', Buffer.from('observed stdout')); child.stderr.emit('data', Buffer.from('observed stderr')); child.finish();
  const completed = await normal; assert.equal(completed.exitCode, 0); assert.equal(completed.stdout, 'observed stdout'); assert.equal(completed.stderr, 'observed stderr');
  assert.equal(f.timers.size, 0); assert.equal(getEventListeners(owner.signal, 'abort').length, 0);
  const other = new AbortController(), active = f.run('synthetic-executable', ['two'], f.options(other.signal));
  assert.equal(f.children.length, 2); other.abort(); assert.equal((await active).exitCode, 130); assert.equal(f.kills(), 1);
  assert.equal(f.timers.size, 0); assert.equal(getEventListeners(other.signal, 'abort').length, 0);
});
