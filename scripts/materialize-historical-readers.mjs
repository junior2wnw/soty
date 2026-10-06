// Operator-side packaging only. No config/history dump, network or production
// data; read the declared old source blobs and preserve their exact bytes.
import { execFileSync } from 'node:child_process';
import { readFileSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url)), pins = new Map();
function add(commit, source, expected) {
  if (!/^[a-f0-9]{40}$/u.test(commit) || !/^(?:modules\/(?:notes|capabilities)\/server|deploy\/connector)\/[a-z0-9-]+\.mjs$/u.test(source)) throw new Error('historical_source_pin_invalid');
  const key = `${commit}/${source}`; if (pins.has(key)) throw new Error('historical_source_duplicate');
  pins.set(key, { commit, source, expected });
}
for (const name of ['notes-v2', 'capabilities-v2', 'capabilities-v3']) {
  const p = JSON.parse(readFileSync(path.join(root, 'deploy/connector/fixtures', name, 'provenance.json'), 'utf8'));
  for (const [source, expected] of Object.entries(p.files)) add(p.commit, source, expected);
  if (p.oldHost) add(p.oldHost.commit, p.oldHost.sourcePath, p.oldHost);
}
const domain = JSON.parse(readFileSync(path.join(root, 'modules/capabilities/test/fixtures/capabilities-v2/provenance.json'), 'utf8'));
for (const expected of domain.files) add(domain.commit, expected.source, expected);
add('ae914f55e6d8d64628a7279189d2d55dfd05de45', 'deploy/connector/storage-probe.mjs', {
  sha256: 'd0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17',
});
for (const { commit, source, expected } of pins.values()) {
  const bytes = execFileSync('git', ['show', `${commit}:${source}`], { cwd: root, maxBuffer: 1024 * 1024, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  if (createHash('sha256').update(bytes).digest('hex') !== expected.sha256 || expected.bytes !== undefined && bytes.length !== expected.bytes) throw new Error('historical_source_hash_mismatch');
  const blob = createHash('sha1').update(`blob ${bytes.length}\0`).update(bytes).digest('hex');
  if (expected.gitBlob && blob !== expected.gitBlob) throw new Error('historical_git_blob_mismatch');
  const target = path.join(root, 'deploy/connector/fixtures/historical-source', commit, source);
  if (existsSync(target)) {
    if (!readFileSync(target).equals(bytes)) throw new Error('historical_source_existing_mismatch');
  } else { mkdirSync(path.dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: 'wx' }); }
}
process.stdout.write(JSON.stringify({ ok: true, materialized: pins.size }) + '\n');
