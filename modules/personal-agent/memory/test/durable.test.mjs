import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, mkdtempSync, rmSync } from 'node:fs';
import { fork } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { openMemoryPartition } from '../index.mjs';
import { openFloorFixture } from './durable-fixture.mjs';
import { personal, record, error } from './fixture.mjs';

function disk(t) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-memory-durable-'));
  t.after(() => {
    assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-memory-durable-')));
    rmSync(directory, { recursive: true, force: true });
  });
  return { directory, memory: join(directory, 'memory.sqlite'), floor: join(directory, 'external-floor.sqlite') };
}
function open(files, authority) {
  return openMemoryPartition({ databasePath: files.memory, scope: personal(), context: authority.context,
    verifyContext: authority.verifyContext, readRestoreFloor: authority.readRestoreFloor,
    advanceRestoreFloor: authority.advanceRestoreFloor, clock: () => 1000 });
}

test('external deletion floor and intent survive their own restart and reject pre-erase memory restore', async t => {
  const files = disk(t); let authority = openFloorFixture(files.floor), store = open(files, authority);
  try {
    await store.remember({ context: authority.context, mutationId: 'seed', record: record() });
    store.close(); const snapshot = join(files.directory, 'pre-delete.sqlite'); copyFileSync(files.memory, snapshot);
    store = open(files, authority);
    store.delete({ context: authority.context, mutationId: 'delete_a', record: { id: 'record_a', expectedRevision: 1 } });
    store.close(); authority.close();
    authority = openFloorFixture(files.floor); store = open(files, authority);
    assert.equal(authority.intents().length, 1);
    assert.equal(authority.intents()[0].ids, '["record_a"]');
    assert.deepEqual(await store.recall({ context: authority.context, query: 'lighthouse' }), { records: [] });
    assert.throws(() => open({ ...files, memory: snapshot }, authority), error('memory_restore_floor_mismatch'));
    assert.equal(store.export({ context: authority.context }).restoreFloor, 1);
  } finally { store.close(); authority.close(); }
});

test('two OS processes race through real SQLite and durable external floor; one replacement commits', { timeout: 10000 }, async t => {
  const files = disk(t), authority = openFloorFixture(files.floor), seeded = open(files, authority);
  try { await seeded.remember({ context: authority.context, mutationId: 'seed', record: record() }); }
  finally { seeded.close(); authority.close(); }
  const childFile = fileURLToPath(new URL('./race-child.mjs', import.meta.url)), children = [];
  const safeEnvironment = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(name => process.env[name]).map(name => [name, process.env[name]]));
  function worker(suffix) {
    const child = fork(childFile, [files.memory, files.floor, suffix], { execPath: process.execPath,
      execArgv: [], env: safeEnvironment, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
    children.push(child);
    let readyResolve, resultResolve, exitResolve;
    const ready = new Promise(resolveReady => { readyResolve = resolveReady; });
    const result = new Promise(resolveResult => { resultResolve = resolveResult; });
    const exited = new Promise(resolveExit => { exitResolve = resolveExit; });
    let errorOutput = '';
    child.stderr.on('data', data => { errorOutput += String(data).slice(0, 2000); });
    child.on('error', err => { readyResolve({ error: err.code }); resultResolve({ error: err.code }); });
    child.on('message', message => { if (message.ready || message.setupError) readyResolve(message); else resultResolve(message); });
    child.on('exit', code => { exitResolve(code); if (code !== 0) { readyResolve({ error: code, output: errorOutput }); resultResolve({ error: code }); } });
    return { child, ready, result, exited };
  }
  try {
    const workers = [worker('b'), worker('c')];
    const readiness = await Promise.all(workers.map(value => value.ready));
    readiness.forEach(value => assert.deepEqual(value, { ready: true }));
    workers.forEach(value => value.child.send('go'));
    const results = await Promise.all(workers.map(value => value.result));
    assert.equal(results.filter(value => value.committed).length, 1);
    const rejected = results.find(value => !value.committed);
    assert.ok(['memory_revision_conflict', 'memory_context_stale', 'memory_restore_floor_mismatch'].includes(rejected.code));
    assert.deepEqual(await Promise.all(workers.map(value => value.exited)), [0, 0]);
    const currentAuthority = openFloorFixture(files.floor), current = open(files, currentAuthority);
    try {
      const exported = current.export({ context: currentAuthority.context });
      assert.equal(exported.records.length, 1); assert.equal(exported.tombstones.length, 1);
      assert.equal(exported.restoreFloor, 1); assert.equal(currentAuthority.intents().length, 1);
    } finally { current.close(); currentAuthority.close(); }
  } finally { for (const child of children) if (child.exitCode === null) child.kill(); }
});
