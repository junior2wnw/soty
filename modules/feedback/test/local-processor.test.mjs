import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rmdir, chmod } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLocalFeedbackProcessor } from '../processors/local.mjs';

const fixture = name => readFileSync(new URL('./fixtures/' + name, import.meta.url));
const image = () => ({ kind: 'image', name: 'synthetic.png', mimeType: 'image/png', dataBase64: fixture('chrome.png').toString('base64') });
const deny = (value, code) => assert.rejects(value, error => error.code === code && error.message === code);
const turn = () => new Promise(resolve => setImmediate(resolve));
async function host(t, options = {}) {
  const scratchDirectory = await mkdtemp(join(tmpdir(), 'soty-processor-test-')); await chmod(scratchDirectory, 0o700);
  const processor = createLocalFeedbackProcessor({ scratchDirectory, ...options });
  t.after(async () => { processor.close(); assert.deepEqual(await readdir(scratchDirectory), []); await rmdir(scratchDirectory); });
  return { processor, scratchDirectory };
}

test('no installed engine fails closed and remote/body commands cannot enable it', async t => {
  const { processor } = await host(t);
  assert.deepEqual(processor.availability(), { speech: false, screenshot: false });
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => true,
    command: 'untrusted command', remoteUrl: 'https://invalid.example/' }), 'feedback_processor_unavailable');
});

test('native current authority is mandatory before any media work', async t => {
  const { processor } = await host(t);
  await deny(processor.derive({ attachment: image() }), 'feedback_processor_authority_required');
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => false }), 'feedback_processor_access_denied');
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => ({ allowed: true }) }), 'feedback_processor_access_denied');
});

test('private errors and attachment text are never reflected by the processor', async t => {
  const { processor } = await host(t);
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => { throw new Error('private-source-data'); } }), 'feedback_processor_failed');
  await deny(processor.derive({ attachment: { ...image(), dataBase64: 'invalid-private-data' }, currentAuthority: async () => true }), 'feedback_processor_failed');
});

test('deadline bounds ignored authority callback but holds the actual slot until settlement', async t => {
  const { processor } = await host(t, { timeoutMs: 100 });
  let release, started, captured;
  const began = new Promise(resolve => { started = resolve; });
  const pending = processor.derive({ attachment: image(), currentAuthority: signal => { captured = signal; started(); return new Promise(resolve => { release = resolve; }); } });
  await began; await deny(pending, 'feedback_processor_timeout'); assert.equal(captured.aborted, true);
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => true }), 'feedback_processor_busy');
  release(true); await turn();
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => true }), 'feedback_processor_unavailable');
});

test('caller cancellation poisons late work and does not release its occupied slot', async t => {
  const { processor } = await host(t);
  const controller = new AbortController(); let release, started;
  const began = new Promise(resolve => { started = resolve; });
  const pending = processor.derive({ attachment: image(), signal: controller.signal, currentAuthority: () => { started(); return new Promise(resolve => { release = resolve; }); } });
  await began; controller.abort(); await deny(pending, 'feedback_processor_cancelled');
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => true }), 'feedback_processor_busy');
  release(true); await turn();
});

test('close cancels actual work, rejects later calls and reports engines unavailable', async t => {
  const { processor } = await host(t); let release, started;
  const began = new Promise(resolve => { started = resolve; });
  const pending = processor.derive({ attachment: image(), currentAuthority: () => { started(); return new Promise(resolve => { release = resolve; }); } });
  await began; processor.close(); await deny(pending, 'feedback_processor_cancelled');
  assert.deepEqual(processor.availability(), { speech: false, screenshot: false });
  await deny(processor.derive({ attachment: image(), currentAuthority: async () => true }), 'feedback_processor_unavailable');
  release(true); await turn();
});

test('installed executables/models must be host absolute paths, never shell text', () => {
  for (const options of [{ pythonExecutable: 'python', whisperModelDirectory: 'https://invalid.example/' },
    { pythonExecutable: process.execPath, whisperModelDirectory: 'relative-model' }, { timeoutMs: 180001 }]) {
    assert.throws(() => createLocalFeedbackProcessor({ scratchDirectory: tmpdir(), ...options }), error => error.code === 'feedback_processor_configuration_invalid');
  }
});

test('failed actual child spawn cleans private input and frees its slot only after close', async t => {
  const { processor, scratchDirectory } = await host(t, { pythonExecutable: join(tmpdir(), 'soty-nonexistent-processor-executable'),
    whisperModelDirectory: tmpdir() });
  const attachment = { kind: 'audio', name: 'synthetic.webm', mimeType: 'audio/webm;codecs=opus',
    dataBase64: fixture('chrome-recorder.webm').toString('base64') };
  await deny(processor.derive({ attachment, currentAuthority: async () => true }), 'feedback_processor_unavailable');
  assert.deepEqual(await readdir(scratchDirectory), []);
  await deny(processor.derive({ attachment, currentAuthority: async () => false }), 'feedback_processor_access_denied');
});
