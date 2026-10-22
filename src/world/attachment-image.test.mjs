import test from 'node:test';
import assert from 'node:assert/strict';
import { attachmentImage } from './attachment-image.mjs';
import { pngFixture } from '../../modules/world/test/avatar-fixtures.mjs';

test('raster preview reuses verified dimensions and preserves every original byte', async () => {
  const bytes = pngFixture(32, 24), original = new Blob([bytes], { type: 'application/octet-stream' });
  const preview = await attachmentImage(original);
  assert.equal(preview.width, 32); assert.equal(preview.height, 24); assert.equal(preview.blob.type, 'image/png');
  assert.deepEqual(Buffer.from(await preview.blob.arrayBuffer()), bytes); assert.equal(original.type, 'application/octet-stream');
});
test('active documents and unbounded raster headers never become inline image previews', async () => {
  for (const bytes of [Buffer.from('<svg onload="alert(1)"/>'), Buffer.from('<html><script>bad()</script></html>'), pngFixture(8193, 1)])
    await assert.rejects(attachmentImage(new Blob([bytes])), { code: 'attachment_image_unsupported' });
});
