import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const run = promisify(execFile);

// Observe actual native Promise resources without retaining them. The relay
// stays open across collections: closing it would settle a lifetime cancel
// Promise and conceal retained reactions from already completed writes.
const memoryProbe = `
import { createHook } from 'node:async_hooks';
import { createWebSocketRelay } from ${JSON.stringify(new URL('../server/websocket-relay.mjs', import.meta.url).href)};
const live = new Set();
let created = 0, destroyed = 0, written = 0;
const hook = createHook({
  init(id, type) { if (type === 'PROMISE') { live.add(id); created++; } },
  destroy(id) { if (live.delete(id)) destroyed++; },
});
hook.enable();
const failures = [];
const relay = createWebSocketRelay({
  toClient: () => { written++; return Promise.resolve(); },
  toSource: () => { written++; return Promise.resolve(); },
  assertActive() {}, onFailure: error => failures.push(error.code),
  clock: { now: () => 0, setTimeout: () => 1, clearTimeout() {} },
});
relay.start();
const clientFrame = Buffer.from([0x82, 0x80, 0, 0, 0, 0]);
const sourceFrame = Buffer.from([0x82, 0]);
async function writes(count) {
  for (let i = 0; i < count; i++) {
    await relay.clientBytes(clientFrame);
    await relay.sourceBytes(sourceFrame);
  }
}
async function snapshot() {
  // Collection and async_hooks destroy callbacks need separate event-loop
  // turns. Do not retain async resource objects or sample before collection.
  for (let i = 0; i < 4; i++) {
    await new Promise(resolve => setImmediate(resolve));
    global.gc();
  }
  await new Promise(resolve => setImmediate(resolve));
  return { live: live.size, written, created, destroyed };
}
await writes(250);
const warm = await snapshot();
await writes(2000);
const first = await snapshot();
await writes(8000);
const second = await snapshot();
// The same relay is still usable after every measured collection.
await relay.clientBytes(clientFrame);
await relay.sourceBytes(sourceFrame);
relay.close();
hook.disable();
process.stdout.write(JSON.stringify({ warm, first, second, written, failures }));
`;

test('completed writes do not retain Promise resources in a still-open relay', { timeout: 20000 }, async t => {
  const { stdout } = await run(process.execPath, ['--expose-gc', '--input-type=module', '-e', memoryProbe], { timeout: 15000, maxBuffer: 16 * 1024 });
  const observed = JSON.parse(stdout);
  assert.deepEqual(observed.failures, []);
  assert.equal(observed.first.written - observed.warm.written, 4000);
  assert.equal(observed.second.written - observed.first.written, 16000);
  assert.equal(observed.written, 20502);
  assert.ok(observed.second.destroyed > 100000, 'the probe must observe real Promise collection');
  assert.ok(observed.first.live <= observed.warm.live + 64, `retained after 4000 completed writes: ${JSON.stringify(observed)}`);
  assert.ok(observed.second.live <= observed.first.live + 64, `retained after another 16000 completed writes: ${JSON.stringify(observed)}`);
  t.diagnostic(`live native Promises after GC at writes500/4500/20500: ${observed.warm.live}/${observed.first.live}/${observed.second.live}; collected=${observed.second.destroyed}`);
});
