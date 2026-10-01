// Private, dry R1a admission only. No sender, extractor, output stream or CLI.
import { performance } from 'node:perf_hooks';
import { readEncryptedBackup, formatFailure, formatFailureCode } from './backup-format.mjs';

const HEX = /^[a-f0-9]{64}$/;
const LIMIT_KEYS = ['archiveBytes', 'plaintextBytes', 'fileBytes', 'extractedBytes', 'entries', 'headers',
  'pathBytes', 'pathDepth', 'externalFiles', 'externalBytes', 'wallMs', 'idleMs'];
const CODES = new Set(['restore_authentication_failed', 'restore_incomplete', 'restore_archive_invalid',
  'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed']);
const aborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;
function exact(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || required.some(key => !Object.hasOwn(value, key))
      || Reflect.ownKeys(value).some(key => !required.includes(key) && !optional.includes(key)))
    throw formatFailure('restore_archive_invalid');
}
function capture(input) {
  exact(input, ['file', 'privateKeyPem', 'expectedSha256', 'expectedManifestSha256', 'sourceWitness', 'limits'], ['signal']);
  const { file, privateKeyPem, expectedSha256, expectedManifestSha256, sourceWitness, limits } = input;
  const signal = Object.hasOwn(input, 'signal') ? input.signal : undefined;
  if (typeof file !== 'string' || !file || file.length > 4096 || !file.isWellFormed() || file.includes('\0') || Buffer.byteLength(file) > 4096
      || typeof privateKeyPem !== 'string' || !privateKeyPem || privateKeyPem.length > 64 * 1024 || Buffer.byteLength(privateKeyPem) > 64 * 1024
      || typeof expectedSha256 !== 'string' || !HEX.test(expectedSha256)
      || typeof expectedManifestSha256 !== 'string' || !HEX.test(expectedManifestSha256)) throw formatFailure();
  exact(sourceWitness, ['generationId', 'checkpointSha256', 'inventorySha256']);
  const witness = { generationId: sourceWitness.generationId, checkpointSha256: sourceWitness.checkpointSha256,
    inventorySha256: sourceWitness.inventorySha256 };
  if (typeof witness.generationId !== 'string' || !/^[a-f0-9]{32}$/.test(witness.generationId)
      || typeof witness.checkpointSha256 !== 'string' || !HEX.test(witness.checkpointSha256)
      || typeof witness.inventorySha256 !== 'string' || !HEX.test(witness.inventorySha256)) throw formatFailure();
  exact(limits, LIMIT_KEYS);
  const capturedLimits = {};
  for (const key of LIMIT_KEYS) {
    const value = limits[key];
    if (!Number.isSafeInteger(value) || value <= 0) throw formatFailure('restore_limit_exceeded');
    capturedLimits[key] = value;
  }
  // Invoke the native brand-checking getter, never a caller-defined aborted or
  // reason property. Abort reasons are not part of this port's diagnostics.
  if (signal !== undefined) aborted.call(signal);
  return { file, privateKeyPem, expectedSha256, expectedManifestSha256,
    sourceWitness: Object.freeze(witness), limits: Object.freeze(capturedLimits), signal };
}

export async function inspectRestorableBackup(input) {
  try {
    const started = performance.now(), options = capture(input);
    let lastProgress = started;
    const check = (progress = false) => {
      const now = performance.now();
      if (options.signal !== undefined && aborted.call(options.signal)) throw formatFailure('restore_io_failed');
      if (now - started > options.limits.wallMs || now - lastProgress > options.limits.idleMs)
        throw formatFailure('restore_timeout');
      if (progress) lastProgress = now;
    };
    check();
    const result = await readEncryptedBackup({ file: options.file, privateKeyPem: options.privateKeyPem,
      restore: { expectedSha256: options.expectedSha256, expectedManifestSha256: options.expectedManifestSha256,
        sourceWitness: options.sourceWitness, limits: options.limits, check } });
    check();
    return Object.freeze({ ...result, strictProfile: 'soty.restore-manifest.v1', inventoryMatched: true });
  } catch (error) {
    const observed = formatFailureCode(error), code = CODES.has(observed) ? observed : 'restore_io_failed';
    throw Object.assign(new Error(code), { code, stack: `Error: ${code}` });
  }
}
