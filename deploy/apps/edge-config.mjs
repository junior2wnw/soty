import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { createHash, createCipheriv, randomBytes, publicEncrypt } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { namedZoneIngress } from './release.mjs';

const marker = '# Soty isolated applications';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const fail = code => { throw Object.assign(new Error(code), { code }); };

export function extendCaddyfile(source) {
  if (typeof source !== 'string' || !/^\s*\{/.test(source) || source.includes(marker)
      || /\bon_demand_tls\b|\*\.soty\.pochinit\.online|https:\/\/\s*\{|\{\$/.test(source)) fail('edge_existing_configuration_requires_review');
  const global = source.indexOf('{') + 1;
  return source.slice(0, global) + '\n\ton_demand_tls {\n\t\task http://127.0.0.1:18182/api/apps/tls-allow\n\t}\n' + source.slice(global)
    + '\n' + marker + '\n' + namedZoneIngress({ origin: 'https://soty.pochinit.online' });
}

// Preparing a candidate never writes the serving configuration or reloads Caddy.
// Existing configuration is backed up only as authenticated ciphertext.
export async function prepareEdge({ currentPath, outputDirectory, publicKeyPath }) {
  const bytes = await readFile(currentPath), candidate = extendCaddyfile(bytes.toString('utf8'));
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  const candidatePath = join(outputDirectory, `Caddyfile-${hash(candidate).slice(0, 16)}.candidate`);
  const backupPath = join(outputDirectory, `Caddyfile-${hash(bytes).slice(0, 16)}.enc`);
  const key = randomBytes(32), nonce = randomBytes(12);
  const header = { schema: 'soty.edge-backup.v1', sha256: hash(bytes), nonce: nonce.toString('base64'),
    wrappedKey: publicEncrypt({ key: await readFile(publicKeyPath), oaepHash: 'sha256' }, key).toString('base64') };
  const cipher = createCipheriv('aes-256-gcm', key, nonce); cipher.setAAD(Buffer.from(JSON.stringify(header)));
  const encrypted = Buffer.concat([cipher.update(bytes), cipher.final()]); key.fill(0);
  await writeFile(backupPath, JSON.stringify({ header, tag: cipher.getAuthTag().toString('base64'), ciphertext: encrypted.toString('base64') }), { flag: 'wx', mode: 0o600 });
  await writeFile(candidatePath, candidate, { flag: 'wx', mode: 0o600 });
  const validation = spawnSync('caddy', ['adapt', '--config', candidatePath, '--adapter', 'caddyfile'], { stdio: 'ignore', timeout: 30_000 });
  if (validation.status !== 0) fail('edge_candidate_validation_failed');
  return { ok: true, beforeSha256: hash(bytes), afterSha256: hash(candidate), candidatePath, backupPath, encrypted: true };
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  try {
    if (process.argv.length !== 5) fail('edge_usage');
    console.log(JSON.stringify(await prepareEdge({ currentPath: resolve(process.argv[2]), outputDirectory: resolve(process.argv[3]), publicKeyPath: resolve(process.argv[4]) })));
  } catch (error) { console.error(JSON.stringify({ ok: false, code: /^edge_\w+$/.test(error.code || '') ? error.code : 'edge_prepare_failed' })); process.exitCode = 1; }
}
