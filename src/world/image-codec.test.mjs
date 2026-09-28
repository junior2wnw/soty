import assert from 'node:assert/strict';
import { test } from 'node:test';
import { canonicalWebP } from './image-codec.mjs';

const chunk = (type, body) => { const bytes = Buffer.alloc(8 + body.length + body.length % 2); bytes.write(type); bytes.writeUInt32LE(body.length, 4); Buffer.from(body).copy(bytes, 8); return bytes; };
const riff = parts => { const body = Buffer.concat(parts); const header = Buffer.alloc(12); header.write('RIFF'); header.writeUInt32LE(body.length + 4, 4); header.write('WEBP', 8); return Buffer.concat([header, body]); };
test('canvas ICC profile is stripped while encoded image bytes and padding remain intact', () => {
  const encoded = chunk('VP8 ', [0, 1, 2, 157, 1, 42, 32, 0, 32, 0, 11]);
  const input = riff([chunk('VP8X', [32, 0, 0, 0, 31, 0, 0, 31, 0, 0]), chunk('ICCP', [7, 8, 9]), encoded]);
  assert.deepEqual(Buffer.from(canonicalWebP(input)), riff([encoded]));
});
test('animation, alpha, duplicate image chunks and truncation fail closed', () => {
  const encoded = chunk('VP8 ', [0, 1, 2, 157, 1, 42, 32, 0, 32, 0]);
  for (const type of ['ALPH', 'ANIM', 'ANMF']) assert.throws(() => canonicalWebP(riff([chunk(type, [0]), encoded])), /Invalid canvas WebP/);
  assert.throws(() => canonicalWebP(riff([encoded, encoded])), /Invalid canvas WebP/);
  assert.throws(() => canonicalWebP(riff([encoded]).subarray(0, 22)), /Invalid canvas WebP/);
});
