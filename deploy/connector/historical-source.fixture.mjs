// Test-only exact Git blobs, materialized before packaging. A source archive
// must run the same old-reader checks without repository history or remotes.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

export const gitBlobHash = bytes => createHash('sha1').update(`blob ${bytes.length}\0`).update(bytes).digest('hex');
export function historicalSourceBytes(commit, source) {
  if (!/^[a-f0-9]{40}$/u.test(commit) || !/^(?:modules\/(?:notes|capabilities)\/server|deploy\/connector)\/[a-z0-9-]+\.mjs$/u.test(source)) {
    throw new Error('historical_source_pin_invalid');
  }
  const bytes = readFileSync(new URL(`./fixtures/historical-source/${commit}/${source}`, import.meta.url));
  if (bytes.length > 1024 * 1024) throw new Error('historical_source_limit');
  return bytes;
}
