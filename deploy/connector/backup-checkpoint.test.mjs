import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createReleaseJournal } from './journal.mjs';
import { SafeError } from './docker-api.mjs';
import { fixture, args } from './test-support/rollout-engine.mjs';

async function setup(t) {
  const directory = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-backup-checkpoint-'));
  t.after(async () => { assert.equal(path.dirname(path.resolve(directory)), await realpath(tmpdir()));
    assert.match(path.basename(directory), /^soty-backup-checkpoint-/u); await rm(directory, { recursive: true, force: true, maxRetries: 3 }); });
  return { ...fixture(), file: path.join(directory, 'journal.json') };
}

test('stopped-writer journal checkpoint waits for backup completion before maintenance or candidate start', async t => {
  const f = await setup(t); let entered, release;
  const reached = new Promise(done => { entered = done; }), gate = new Promise(done => { release = done; });
  f.run.record = createReleaseJournal(f.file,{ afterJournal: async state => {
    if (state.phase === 'original_stopped') {
      assert.equal(f.map.get(args.originalId).State.Running, false);
      assert.equal(JSON.parse(await readFile(f.file, 'utf8')).phase, 'original_stopped'); entered(); await gate;
    }
  }});
  await f.run.prepare(args); const promotion = f.run.promote(); await reached;
  assert.equal(f.events.includes('helper:enter'), false); assert.equal(f.events.includes('start-candidate'), false);
  release(); await promotion;
  assert.equal(f.map.get(f.run.candidate.Id).State.Running, true);
});

test('failed stopped-writer backup checkpoint restores the exact original without starting a candidate', async t => {
  const f = await setup(t);
  let calls=0;
  f.run.record = createReleaseJournal(f.file,{ afterJournal: async state => { if (state.phase === 'original_stopped') { calls++;throw new SafeError('backup_failed'); } }});
  await f.run.prepare(args); await assert.rejects(f.run.promote(), error => error.code === 'backup_failed');
  assert.equal(f.map.get(args.originalId).State.Running, true);
  assert.equal(calls,1);
  assert.equal(f.events.includes('start-candidate'), false); assert.equal(f.events.includes('helper:enter'), false);
  assert.equal(JSON.parse(await readFile(f.file, 'utf8')).phase, 'restored');
});
