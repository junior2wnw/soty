/** Canonical opaque canvas WebP: carry the encoded image, never browser-inserted ICC/EXIF metadata. */
export function canonicalWebP(bytes) {
  const fail = () => { throw Object.assign(new Error('Invalid canvas WebP'), { code: 'invalid_avatar_data' }); };
  if (!(bytes instanceof Uint8Array) || bytes.length < 26) fail();
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const tag = offset => String.fromCharCode(...bytes.subarray(offset, offset + 4));
  if (tag(0) !== 'RIFF' || tag(8) !== 'WEBP' || view.getUint32(4, true) + 8 !== bytes.length) fail();
  let image = null;
  for (let offset = 12; offset < bytes.length;) {
    if (offset + 8 > bytes.length) fail();
    const size = view.getUint32(offset + 4, true), end = offset + 8 + size + size % 2;
    if (end > bytes.length) fail();
    const type = tag(offset);
    if (type === 'ALPH' || type === 'ANIM' || type === 'ANMF') fail();
    if (type === 'VP8 ' || type === 'VP8L') { if (image) fail(); image = bytes.slice(offset, end); }
    offset = end;
  }
  if (!image) fail();
  const result = new Uint8Array(12 + image.length); result.set([82, 73, 70, 70], 0); result.set([87, 69, 66, 80], 8);
  new DataView(result.buffer).setUint32(4, result.length - 8, true); result.set(image, 12); return result;
}
