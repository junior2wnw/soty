// SOURCE-ONLY Linux fixture entry. Requires actually inherited FD3/FD4; no fd fallback.
import { Socket } from 'node:net';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { runWarmReceiver } from './warm-receiver.mjs';
import { captureSpec, decodeCanonicalFrame, failureCode, fail, MAX_FRAME_BYTES } from './wire-protocol.mjs';

export async function runFixedLinuxFdEntry(argv = process.argv.slice(2)) {
  let control, announcements;
  try {
    if (process.platform !== 'linux') fail('warm_linux_required');
    if (argv.length !== 2 || argv[0] !== '--warm-spec-b64' || typeof argv[1] !== 'string'
      || argv[1].length > Math.ceil(MAX_FRAME_BYTES / 3) * 4 || !/^[A-Za-z0-9+/]+={0,2}$/u.test(argv[1])) fail('warm_entry_arguments_invalid');
    const bytes = Buffer.from(argv[1], 'base64');
    if (bytes.toString('base64') !== argv[1]) fail('warm_entry_arguments_invalid');
    const spec = captureSpec(decodeCanonicalFrame(bytes));
    // Native child stdio:['pipe','pipe','pipe','pipe','pipe'] supplies these
    // sockets. Direct legacy Docker Attach(0/1/2 only) must fail closed.
    control = new Socket({ fd: 3, readable: true, writable: false });
    announcements = new Socket({ fd: 4, readable: false, writable: true });
    const receipt = await runWarmReceiver({ spec, controlInput: control, payloadInput: process.stdin,
      announcementOutput: announcements, signal: undefined,
      emit(record) { process.stderr.write(JSON.stringify(record) + '\n'); } });
    process.stdout.write(JSON.stringify(receipt) + '\n');
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, code: failureCode(error) }) + '\n'); process.exitCode = 1;
  } finally { control?.destroy(); announcements?.destroy(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await runFixedLinuxFdEntry();
