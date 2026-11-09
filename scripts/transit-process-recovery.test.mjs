import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import { win32 } from 'node:path';
import { EventEmitter } from 'node:events';
import { transitClientWindowsRelease, transitOwnedWindowsCores } from './agent-modules/transit-client.mjs';

const source = (await readFile(new URL('./soty-connector.mjs', import.meta.url), 'utf8')).replace(/\r\n/gu, '\n');
const guard = source.slice(source.indexOf('function transitProcessGuard('), source.indexOf('\nasync function rememberTransitProcess('));
const recover = source.slice(source.indexOf('async function recoverTransitProcess('), source.indexOf('\nasync function transitPublicIp('));
assert.ok(guard.length > 100 && recover.length > 100);
function fixture(version) {
  const calls = [], timers = [];
  const birth = '2026-11-09T00:00:00.0000000Z';
  const known = transitOwnedWindowsCores.find(row => row.version === version);
  const context = vm.createContext({
    join: win32.join, transitRoot: 'C:\\private\\transit', transitClientWindowsRelease, transitOwnedWindowsCores,
    psQuote: value => "'" + value.replace(/'/gu, "''") + "'", process: { platform: 'win32' }, EventEmitter,
    readTransitProcess: async () => ({ pid: 777, created: birth }),
    transitPowerShell: async query => {
      calls.push(query);
      if (!known || !query.includes(known.executable.replaceAll('/', '\\'))) return '';
      if (query.includes('Get-FileHash')) return query.includes(known.binarySha256) ? 'yes' : '';
      return 'yes';
    },
    setInterval(fn) { timers.push(fn); return { unref() {} }; }, clearInterval() {},
  });
  new vm.Script(guard + '\n' + recover + '\nglobalThis.recover=recoverTransitProcess').runInContext(context);
  return { recover: context.recover, calls, timers, birth };
}

test('updating the core preserves recovery of exactly the old or current pinned owned process', async () => {
  for (const version of ['1.13.15', '1.14.4']) {
    const f = fixture(version), owned = await f.recover();
    assert.ok(owned); assert.equal(owned.pid, 777);
    assert.ok(f.calls.some(query => query.includes('Get-FileHash') && query.includes('-ErrorAction Stop')));
    assert.ok(f.calls.every(query => query.includes('ProcessId=777') && query.includes(f.birth)
      && query.includes('C:\\private\\transit\\run.json')));
    const count = f.calls.length; await f.timers[0]();
    assert.equal(f.calls.length, count + 1); assert.ok(!f.calls.at(-1).includes('Get-FileHash'));
    owned.kill(); await Promise.resolve();
    assert.ok(f.calls.at(-1).includes('Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop'));
    assert.ok(f.calls.at(-1).includes(version));
  }
});

test('an unknown executable, changed pin or reused ownership cannot be adopted or stopped', async () => {
  const f = fixture('unknown'); assert.equal(await f.recover(), null); assert.equal(f.timers.length, 0);
  assert.ok(!f.calls.some(query => query.includes('Stop-Process')));
});
