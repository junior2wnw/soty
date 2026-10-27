import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

// Exact Git blob IDs and SHA-256 hashes of the historical files under test.
// The list is closed: no arbitrary revisions or paths are read from disk.
const pins = new Map([
  ['e371d14be0aba67892a389f630a6c029f298ac77/server/connector-store.js', ['569509f6444b0fe3c7b751fd40b593777d20aa34','9b68c22594c96bf47a64ed0e382a6360b94e2a3e79e6ccec8c6105c236252fa9']],
  ['e371d14be0aba67892a389f630a6c029f298ac77/server/connector-persistence.js', ['5bee8f1e471a5226a2ad8cc61abfedbb4321d310','0eb53d9af38006c58f05cc78fa481b92711c372dbb625fd6cf6d08adc0432d25']],
  ['e371d14be0aba67892a389f630a6c029f298ac77/server/connector-model-authorization.js', ['ffec952ea240a883686e8bffc8c21729e7a63ee7','cf3cd7e0bd76b0cad63bfe3235ffe3c8bbac5bc2ce8bc7ab14a7510012ae44bb']],
  ['2b770d8c2c34ae513e83a1c5bd489f1414c36b3b/public/agent/soty-connector.mjs', ['2d4bed319b8922dd5f7cfdbac0b3b7bc4d167f14','79196c4f9b4a5d57bae34c0a8546270d03c7aeb1865ef84218652b993e140cd7']],
  ['2b770d8c2c34ae513e83a1c5bd489f1414c36b3b/public/agent/manifest.json', ['ad7e017e4a0bc3bfd01b3ba7c4ed5f5c91a5a547','2bf69e7cef7dc5b0acdfb7723304dca926cbbcc6936a2ab1b08d6ff1546b405c']],
  ['ff507100b3645f77e6a7fb84cbc0ad913ab3e34c/public/agent/soty-connector.mjs', ['7b943de8103f378a7814518df0012feb8bd4bcb0','ba6a7a1a94e18c4c4d67e6bb31c23bc77568a18eb98e4d3422c109a2c35ecec0']],
  ['ff507100b3645f77e6a7fb84cbc0ad913ab3e34c/public/agent/manifest.json', ['1e9ec401e8d797d6c1d5ab8efd3642e399fdba91','e92be4f6e235acde0ac910bed90bcb699179d96d612b34fd2437e02186fdf3e2']],
]);

function pin(commit, source) {
  const expected = typeof commit === 'string' && /^[a-f0-9]{40}$/u.test(commit) && typeof source === 'string' ? pins.get(`${commit}/${source}`) : undefined;
  if (!expected) throw new Error('historical_fixture_unknown');
  return expected;
}

export function verifyPinnedHistory(commit, source, bytes) {
  const expected = pin(commit, source);
  if (!Buffer.isBuffer(bytes)) throw new Error('historical_fixture_corrupt');
  const blob = createHash('sha1').update(`blob ${bytes.length}\0`).update(bytes).digest('hex');
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  if (blob !== expected[0] || sha256 !== expected[1]) throw new Error('historical_fixture_corrupt');
  return bytes;
}

export function pinnedHistory(commit, source) {
  pin(commit, source);
  const bytes = readFileSync(new URL(`../../../test-fixtures/pinned-history/${commit}/${source}`, import.meta.url));
  return verifyPinnedHistory(commit, source, bytes);
}
