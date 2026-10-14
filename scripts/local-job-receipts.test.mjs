import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, readdir, writeFile, rm, open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { createLocalJobReceipts } from './agent-modules/local-job-receipts.mjs';

const id = 'job_' + 'a'.repeat(32), binding = { linkId: 'synthetic-link', deviceId: 'synthetic-device', connectorId: 'synthetic-user', relayBaseUrl: 'https://synthetic.invalid' };
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'soty-local-receipts-'));
  t.after(async () => { assert.equal(dirname(root), tmpdir()); await rm(root, { recursive: true, force: true }); });
  const api = { join, digest: value => createHash('sha256').update(value).digest('hex'),
    mkdir: target => mkdir(target, { recursive: true }), list: target => readdir(target),
    readJson: async target => JSON.parse(await readFile(target, 'utf8')),
    writeExclusive: async (target, value) => { const file = await open(target, 'wx', 0o600);
      try { await file.writeFile(JSON.stringify(value)); await file.sync(); } finally { await file.close(); } } };
  return { root, reopen: (overrides = {}) => createLocalJobReceipts(api, { root, binding: { ...binding, ...overrides } }) };
}

test('restart/lost result ACK retains the original native receipt and never authorizes execution again', async t => {
  const f = await fixture(t), job = { id, kind: 'agent', input: { text: 'Own QA file' } }, one = f.reopen();
  assert.equal(await one.claim(job), true);
  assert.equal(await f.reopen().claim(job), false); assert.deepEqual(await f.reopen().pending(), []);
  const native = { ok: true, text: 'Verified QA file', exitCode: 0, sessionId: 'synthetic-opencode-session' };
  await one.finish(id, native);
  const two = f.reopen(); assert.deepEqual((await two.pending())[0].result, native);
  assert.equal(await two.claim(job), false);
  await two.finish(id, native); // Repeating the result is permitted; a changed outcome is not.
  await assert.rejects(two.finish(id, { ...native, ok: false }), /result_conflict/);
  await two.acknowledge(id); assert.deepEqual(await f.reopen().pending(), []);
  assert.equal(await f.reopen().claim(job), false);
});

test('private receipt binding mismatch and corruption stay unknown without being sent as completion', async t => {
  const f = await fixture(t), one = f.reopen(); await one.claim({ id, kind: 'agent' });
  await one.finish(id, { ok: true, text: 'Own only', exitCode: 0 });
  await assert.rejects(f.reopen({ deviceId: 'another-device' }).pending(), /binding_denied/);
  const target = join(f.root, `${id}.result.json`), value = JSON.parse(await readFile(target, 'utf8'));
  value.result.text = 'tampered'; await writeFile(target, JSON.stringify(value));
  await assert.rejects(one.pending(), /corrupt/);
  await assert.rejects(one.claim({ id: '../outside', kind: 'agent' }), /invalid_job/);
});
