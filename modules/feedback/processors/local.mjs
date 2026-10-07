import { spawn } from 'node:child_process';
import { mkdtemp, lstat, realpath, writeFile, readFile, unlink, rmdir } from 'node:fs/promises';
import { resolve, join, dirname, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateFeedbackAttachments } from '../server/media.mjs';

export class FeedbackProcessorError extends Error {
  constructor(code, status = 503) { super(code); this.name = 'FeedbackProcessorError'; this.code = code; this.status = status; }
}
const check = (condition, code, status) => { if (!condition) throw new FeedbackProcessorError(code, status); };
const scripts = dirname(fileURLToPath(import.meta.url));
async function trustedDirectory(directory) {
  const normalized = resolve(directory), stat = await lstat(normalized);
  check(stat.isDirectory() && !stat.isSymbolicLink() && await realpath(normalized) === normalized, 'feedback_processor_directory_invalid');
  if (process.platform !== 'win32') check(stat.uid === process.getuid() && (stat.mode & 0o077) === 0, 'feedback_processor_directory_invalid');
  return normalized;
}
async function run(executable, args, signal) {
  check(!signal?.aborted, 'feedback_processor_cancelled', 409);
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(executable, args, { shell: false, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let rejected = null, size = 0;
    const stop = (code, status = 503) => { if (rejected) return; rejected = new FeedbackProcessorError(code, status); child.kill('SIGKILL'); };
    const cancel = () => stop('feedback_processor_cancelled', 409);
    signal?.addEventListener('abort', cancel, { once: true });
    // Library logs are bounded and discarded; text exists only in private output.
    const discard = bytes => { size += bytes.length; if (size > 8192) stop('feedback_processor_failed'); };
    child.stdout.on('data', discard); child.stderr.on('data', discard);
    // Retain the single slot through actual close, including spawn errors.
    child.once('error', () => { rejected ||= new FeedbackProcessorError('feedback_processor_unavailable'); });
    child.once('close', code => { signal?.removeEventListener('abort', cancel);
      rejected || code !== 0 ? rejectRun(rejected || new FeedbackProcessorError('feedback_processor_failed')) : resolveRun(); });
    if (signal?.aborted) cancel();
  });
}

/** Installed host configuration, never feedback text/manifest commands. The
 * Source supplies its actual current permission callback. No remote provider,
 * persistent result cache, assessment commit or automatic code action exists. */
export function createLocalFeedbackProcessor({ scratchDirectory, pythonExecutable, whisperModelDirectory,
  windowsPowerShellExecutable, timeoutMs = 120000 } = {}) {
  check(typeof scratchDirectory === 'string' && Number.isSafeInteger(timeoutMs) && timeoutMs >= 100 && timeoutMs <= 180000,
    'feedback_processor_configuration_invalid');
  const path = value => typeof value === 'string' && isAbsolute(value) && value.length <= 4096 && !value.includes('\u0000');
  check(!pythonExecutable || path(pythonExecutable) && path(whisperModelDirectory), 'feedback_processor_configuration_invalid');
  check(!windowsPowerShellExecutable || process.platform === 'win32' && path(windowsPowerShellExecutable), 'feedback_processor_configuration_invalid');
  let active = false, closed = false, activeController = null;
  return Object.freeze({
    availability() { return Object.freeze({ speech: !closed && Boolean(pythonExecutable && whisperModelDirectory), screenshot: !closed && Boolean(windowsPowerShellExecutable) }); },
    async derive({ attachment, language = 'ru', currentAuthority, signal } = {}) {
      check(!closed, 'feedback_processor_unavailable');
      check(!active, 'feedback_processor_busy');
      check(typeof currentAuthority === 'function' && ['ru', 'en'].includes(language), 'feedback_processor_authority_required', 403);
      active = true;
      const controller = new AbortController(); activeController = controller;
      let folder, timer, failCaller;
      const deadline = new Promise((_resolve, reject) => { failCaller = reject; });
      const cancel = () => { failCaller(new FeedbackProcessorError('feedback_processor_cancelled', 409)); controller.abort(); };
      signal?.addEventListener('abort', cancel, { once: true });
      timer = setTimeout(() => { failCaller(new FeedbackProcessorError('feedback_processor_timeout', 504)); controller.abort(); }, timeoutMs);
      if (signal?.aborted) cancel();
      const current = async () => {
        check(!closed && !controller.signal.aborted, 'feedback_processor_cancelled', 409);
        check(await currentAuthority(controller.signal) === true, 'feedback_processor_access_denied', 403);
        check(!closed && !controller.signal.aborted, 'feedback_processor_cancelled', 409);
      };
      const operation = (async () => { try {
        await current();
        const media = validateFeedbackAttachments([attachment])[0];
        const speech = media.kind === 'audio';
        check(speech ? pythonExecutable && whisperModelDirectory : windowsPowerShellExecutable, 'feedback_processor_unavailable');
        const base = await trustedDirectory(scratchDirectory);
        await current();
        folder = await mkdtemp(join(base, 'job-'));
        const input = join(folder, speech ? 'input.webm' : 'input.png'), output = join(folder, 'derived-private.json');
        await writeFile(input, media.bytes, { flag: 'wx', mode: 0o600 });
        await current();
        await run(speech ? pythonExecutable : windowsPowerShellExecutable, speech
          ? [join(scripts, 'whisper-local.py'), '--input', input, '--output', output, '--model', whisperModelDirectory, '--language', language]
          : ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', join(scripts, 'ocr-windows.ps1'),
            '-InputPath', input, '-OutputPath', output, '-Language', language], controller.signal);
        await current();
        const stat = await lstat(output); check(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 65536, 'feedback_processor_output_invalid');
        const result = JSON.parse(await readFile(output, 'utf8'));
        const keys = speech ? ['schema', 'engine', 'language', 'text', 'durationSeconds'] : ['schema', 'engine', 'language', 'text', 'width', 'height'];
        check(result && Object.keys(result).length === keys.length && keys.every(key => Object.hasOwn(result, key))
          && result.schema === 'soty.feedback.derived-text.v1' && result.engine === (speech ? 'faster-whisper-local' : 'windows-ocr-local')
          && result.language === language && typeof result.text === 'string' && result.text.isWellFormed()
          && Buffer.byteLength(result.text, 'utf8') <= 32768, 'feedback_processor_output_invalid');
        if (speech) check(Number.isFinite(result.durationSeconds) && result.durationSeconds >= 0 && result.durationSeconds <= 120.1, 'feedback_processor_output_invalid');
        else check(Number.isInteger(result.width) && result.width > 0 && result.width <= 8192 && Number.isInteger(result.height)
          && result.height > 0 && result.height <= 8192 && result.width * result.height <= 16777216, 'feedback_processor_output_invalid');
        await current();
        return Object.freeze({ ...result, trust: 'untrusted-content' });
      } catch (error) {
        throw error instanceof FeedbackProcessorError ? error : new FeedbackProcessorError('feedback_processor_failed');
      } finally {
        try { if (folder) {
          const base = resolve(scratchDirectory); check(dirname(folder) === base && /^job-[A-Za-z0-9]+$/u.test(folder.slice(base.length + 1)), 'feedback_processor_directory_invalid');
          // Exact known generated files only. Never traverse/delete author paths.
          for (const name of ['input.webm', 'input.png', 'derived-private.json']) await unlink(join(folder, name)).catch(error => { if (error.code !== 'ENOENT') throw error; });
          await rmdir(folder);
        } } catch { throw new FeedbackProcessorError('feedback_processor_cleanup_failed'); }
        finally {
          active = false; activeController = null;
          clearTimeout(timer); signal?.removeEventListener('abort', cancel);
        }
      } })();
      // Deadline bounds the caller even if a trusted Source callback ignores
      // abort; its actual work still owns the one slot until settlement.
      operation.catch(() => {});
      const onClose = () => failCaller(new FeedbackProcessorError('feedback_processor_cancelled', 409));
      controller.signal.addEventListener('abort', onClose, { once: true });
      try { return await Promise.race([operation, deadline]); }
      finally { controller.signal.removeEventListener('abort', onClose); }
    },
    close() { closed = true; activeController?.abort(); },
  });
}
