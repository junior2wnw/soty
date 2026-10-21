import { coverError, COVER_UPLOAD_LIMIT, bytesToBase64 } from './cover-api.mjs';
export const COVER_FILE_LIMIT = 8 * 1024 * 1024;
export const COVER_DECODE_PIXELS = 16_000_000;
const check = (value, code = 'cover_file_invalid') => { if (!value) throw coverError(code); };
const dimensions = (width, height) => { check(Number.isSafeInteger(width) && Number.isSafeInteger(height) && width > 0 && height > 0 && width * height <= COVER_DECODE_PIXELS, 'cover_file_dimensions'); return { width, height }; };
const ascii = (bytes, offset, length) => String.fromCharCode(...bytes.subarray(offset, offset + length));
/** Cheap declared-size/animation admission before allocating a native bitmap. The
 * native decoder still validates the entire file, and its output is checked too. */
export function inspectCoverRaster(bytes, mime) {
  check(bytes instanceof Uint8Array && bytes.length > 0 && bytes.length <= COVER_FILE_LIMIT, 'cover_file_limit');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (mime === 'image/png') {
    check(bytes.length >= 33 && ascii(bytes, 0, 8) === '\x89PNG\r\n\x1a\n' && view.getUint32(8) === 13 && ascii(bytes, 12, 4) === 'IHDR');
    const size = dimensions(view.getUint32(16), view.getUint32(20)); let end = false, n = 0;
    for (let at = 8; at + 12 <= bytes.length && n++ < 4096;) {
      const length = view.getUint32(at), type = ascii(bytes, at + 4, 4); check(length <= bytes.length - at - 12);
      check(type !== 'acTL' && type !== 'fcTL' && type !== 'fdAT', 'cover_animation_unsupported');
      at += length + 12; if (type === 'IEND') { check(length === 0 && at === bytes.length); end = true; break; }
    }
    check(end); return size;
  }
  if (mime === 'image/jpeg') {
    check(bytes.length >= 12 && bytes[0] === 255 && bytes[1] === 216); let at = 2;
    for (let n = 0; n < 4096 && at + 3 < bytes.length; n++) {
      check(bytes[at++] === 255); while (bytes[at] === 255) at++; const marker = bytes[at++];
      check(marker !== 0xda && marker !== 0xd9); if (marker === 0x01 || marker >= 0xd0 && marker <= 0xd7) continue;
      const length = view.getUint16(at); check(length >= 2 && at + length <= bytes.length);
      if ([0xc0, 0xc1, 0xc2].includes(marker)) { check(length >= 8); return dimensions(view.getUint16(at + 5), view.getUint16(at + 3)); }
      at += length;
    }
    throw coverError('cover_file_invalid');
  }
  if (mime === 'image/webp') {
    check(bytes.length >= 30 && ascii(bytes, 0, 4) === 'RIFF' && ascii(bytes, 8, 4) === 'WEBP' && view.getUint32(4, true) + 8 === bytes.length);
    let size = null, n = 0, at = 12;
    while (at + 8 <= bytes.length && n++ < 4096) {
      const type = ascii(bytes, at, 4), length = view.getUint32(at + 4, true), p = at + 8; check(length <= bytes.length - p);
      check(type !== 'ANIM' && type !== 'ANMF', 'cover_animation_unsupported');
      if (type === 'VP8X') { check(length === 10 && !(bytes[p] & 2), 'cover_animation_unsupported'); size = dimensions(1 + view.getUint32(p + 4, true) % 0x1000000, 1 + (view.getUint32(p + 6, true) >>> 8)); }
      if (type === 'VP8 ') { check(length >= 10 && bytes[p + 3] === 0x9d && bytes[p + 4] === 1 && bytes[p + 5] === 0x2a); size ??= dimensions(view.getUint16(p + 6, true) & 0x3fff, view.getUint16(p + 8, true) & 0x3fff); }
      if (type === 'VP8L') { check(length >= 5 && bytes[p] === 0x2f); const bits = view.getUint32(p + 1, true); size ??= dimensions((bits & 0x3fff) + 1, ((bits >>> 14) & 0x3fff) + 1); }
      at = p + length + (length & 1);
    }
    check(at === bytes.length && size); return size;
  }
  throw coverError('cover_file_type');
}
/** Exactly the server's square extraction, applied after EXIF orientation. */
export function coverCropRect(width, height, crop = { x: .5, y: .5, zoom: 1 }) {
  check(Number.isFinite(width) && width > 0 && Number.isFinite(height) && height > 0 && Number.isFinite(crop.x) && crop.x >= 0 && crop.x <= 1
    && Number.isFinite(crop.y) && crop.y >= 0 && crop.y <= 1 && Number.isFinite(crop.zoom) && crop.zoom >= 1 && crop.zoom <= 4, 'cover_crop_invalid');
  const size = Math.max(1, Math.floor(Math.min(width, height) / crop.zoom));
  return { left: Math.max(0, Math.min(width - size, Math.round(crop.x * width - size / 2))),
    top: Math.max(0, Math.min(height - size, Math.round(crop.y * height - size / 2))), size };
}
export function createCoverRasterPreparer(platform) {
  let decoding = false;
  return async function prepare(file, isCurrent, kind = 'app') {
    const current = () => check(isCurrent() === true, 'cover_context_changed'); current();
    check(kind === 'app' || kind === 'profile', 'cover_configuration_invalid');
    check(!decoding, 'cover_decode_busy');
    check(file && Number.isSafeInteger(file.size) && file.size > 0 && file.size <= COVER_FILE_LIMIT, 'cover_file_limit');
    check(['image/png', 'image/jpeg', 'image/webp'].includes(file.type) && typeof file.arrayBuffer === 'function', 'cover_file_type');
    decoding = true; let bitmap = null, owned = null;
    try {
      const bytes = new Uint8Array(await file.arrayBuffer()); current(); check(bytes.length === file.size, 'cover_file_invalid'); inspectCoverRaster(bytes, file.type);
      bitmap = await platform.decode(new Blob([bytes], { type: file.type })); current(); dimensions(bitmap.width, bitmap.height);
      const attempts = [[1024, .82], [1024, .62], [1024, .42], [768, .62], [512, .62], [256, .62]];
      let accepted = null, size = null;
      for (const [edge, quality] of attempts) {
        const scale = Math.min(1, edge / Math.max(bitmap.width, bitmap.height));
        size = { width: Math.max(1, Math.floor(bitmap.width * scale)), height: Math.max(1, Math.floor(bitmap.height * scale)) };
        const blob = await platform.encode(bitmap, size, quality, kind); current();
        check(blob && ['image/webp', 'image/png'].includes(blob.type), 'cover_encode_unavailable');
        if (blob.size > 0 && blob.size <= COVER_UPLOAD_LIMIT) { accepted = blob; break; }
      }
      check(accepted, 'cover_input_limit'); const encoded = new Uint8Array(await accepted.arrayBuffer()); current();
      check(encoded.length === accepted.size && encoded.length <= COVER_UPLOAD_LIMIT, 'cover_input_limit');
      const encodedSize = inspectCoverRaster(encoded, accepted.type);
      check(encodedSize.width === size.width && encodedSize.height === size.height && encodedSize.width * encodedSize.height <= 1_048_576, 'cover_encode_invalid');
      owned = platform.createUrl(accepted); current(); let disposed = false;
      const result = { width: size.width, height: size.height, mime: accepted.type, url: owned, byteCount: encoded.length,
        base64() { check(!disposed, 'cover_context_changed'); return bytesToBase64(encoded); },
        dispose() { if (disposed) return; disposed = true; encoded.fill(0); platform.revokeUrl(result.url); } };
      owned = null; return Object.freeze(result);
    } finally { if (bitmap) bitmap.close(); if (owned) platform.revokeUrl(owned); decoding = false; }
  };
}
const browserPlatform = {
  decode: blob => createImageBitmap(blob, { imageOrientation: 'from-image' }),
  encode: (bitmap, size, quality, kind) => new Promise((resolve, reject) => {
    const canvas = document.createElement('canvas'); canvas.width = size.width; canvas.height = size.height;
    const context = canvas.getContext('2d'); if (!context) { reject(coverError('cover_encode_unavailable')); return; }
    if (kind === 'profile') { context.fillStyle = '#f4efe4'; context.fillRect(0, 0, size.width, size.height); }
    context.drawImage(bitmap, 0, 0, size.width, size.height);
    canvas.toBlob(blob => { canvas.width = 1; canvas.height = 1; resolve(blob); }, 'image/webp', quality);
  }),
  createUrl: blob => URL.createObjectURL(blob), revokeUrl: url => URL.revokeObjectURL(url),
};
export const prepareCoverFile = createCoverRasterPreparer(browserPlatform);
