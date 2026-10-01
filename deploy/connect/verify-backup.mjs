// Plaintext is inspected by the shared internal reader, never returned or saved.
import { pathToFileURL } from 'node:url';
import { readEncryptedBackup } from './backup-format.mjs';

const MAX_INPUT = 64 * 1024;
export async function verifyEncryptedBackup(input) {
  try {
    const { file, privateKeyPem } = input;
    return await readEncryptedBackup({ file, privateKeyPem });
  } catch {
    throw Object.assign(new Error('backup_verification_failed'), {
      code: 'backup_verification_failed', stack: 'Error: backup_verification_failed',
    });
  }
}

async function main() {
  try {
    const chunks = []; let length = 0;
    for await (const chunk of process.stdin) {
      length += chunk.length;
      if (length > MAX_INPUT) throw new Error('backup_verification_failed');
      chunks.push(chunk);
    }
    const input = Buffer.concat(chunks);
    const options = JSON.parse(input.toString('utf8').replace(/^\uFEFF/, ''));
    input.fill(0); chunks.forEach(chunk => chunk.fill(0));
    console.log(JSON.stringify(await verifyEncryptedBackup(options)));
  } catch { console.error(JSON.stringify({ ok: false, code: 'backup_verification_failed' })); process.exitCode = 1; }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
