// Manual installed-engine gate. Only synthetic files supplied by the operator.
// No result text, model/provider secrets or private paths enter the receipt.
import assert from 'node:assert/strict';
import { readFile, readdir, mkdtemp, rmdir, chmod, writeFile } from 'node:fs/promises';
import { join, resolve, isAbsolute } from 'node:path';
import { createLocalFeedbackProcessor } from '../../modules/feedback/processors/local.mjs';

const [directoryArg, pythonExecutable, whisperModelDirectory, windowsPowerShellExecutable] = process.argv.slice(2);
assert.equal(process.platform, 'win32');
assert.ok([directoryArg, pythonExecutable, whisperModelDirectory, windowsPowerShellExecutable].every(value => isAbsolute(value)));
const directory = resolve(directoryArg);
const scratchDirectory = await mkdtemp(join(directory, 'processor-scratch-')); await chmod(scratchDirectory, 0o700);
const processor = createLocalFeedbackProcessor({ scratchDirectory, pythonExecutable, whisperModelDirectory, windowsPowerShellExecutable });
const attachment = async (filename, kind, mimeType) => ({ kind, name: filename, mimeType,
  dataBase64: (await readFile(join(directory, filename))).toString('base64') });
const evidence = { schema: 'soty.feedback.local-processing-gate.v1', syntheticOnly: true, remoteProviderCalls: 0, checks: [] };
const start = performance.now();
try {
  for (const [filename, kind, mimeType, engine] of [
    ['synthetic-feedback.ogg', 'audio', 'audio/ogg;codecs=opus', 'faster-whisper-local'],
    ['synthetic-feedback.png', 'image', 'image/png', 'windows-ocr-local'],
  ]) {
    let checks = 0; const then = performance.now();
    const result = await processor.derive({ attachment: await attachment(filename, kind, mimeType), currentAuthority: async () => { checks++; return true; } });
    assert.equal(result.engine, engine); assert.equal(result.trust, 'untrusted-content');
    assert.match(result.text, /сохранения/u); assert.match(result.text, /повторяется/u);
    assert.ok(checks >= 5); assert.deepEqual(await readdir(scratchDirectory), []);
    evidence.checks.push({ engine, passed: true, milliseconds: Math.round(performance.now() - then),
      expectedPhrasesMatched: true, currentAuthorityChecks: checks, privateScratchClean: true });
  }
  let currentChecks = 0;
  await assert.rejects(processor.derive({ attachment: await attachment('synthetic-feedback.png', 'image', 'image/png'),
    currentAuthority: async () => ++currentChecks < 4 }), error => error.code === 'feedback_processor_access_denied');
  assert.equal(currentChecks, 4); assert.deepEqual(await readdir(scratchDirectory), []);
  evidence.checks.push({ afterEngineAccessRevoke: true, privateTextReturned: false, privateScratchClean: true });
  const controller = new AbortController(); let cancelledChecks = 0;
  await assert.rejects(processor.derive({ attachment: await attachment('synthetic-feedback.png', 'image', 'image/png'), signal: controller.signal,
    currentAuthority: async () => { if (++cancelledChecks === 3) setTimeout(() => controller.abort(), 100); return true; } }),
  error => error.code === 'feedback_processor_cancelled');
  for (let i = 0; i < 100 && (await readdir(scratchDirectory)).length; i++) await new Promise(resolve => setTimeout(resolve, 20));
  assert.deepEqual(await readdir(scratchDirectory), []);
  evidence.checks.push({ actualChildCancellation: true, privateTextReturned: false, privateScratchClean: true });
  evidence.milliseconds = Math.round(performance.now() - start);
  await writeFile(join(directory, 'processor-public-evidence.json'), JSON.stringify(evidence, null, 2) + '\n', { flag: 'wx' });
  console.log(JSON.stringify(evidence));
} finally { processor.close(); assert.deepEqual(await readdir(scratchDirectory), []); await rmdir(scratchDirectory); }
