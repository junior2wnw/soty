import { downloadAttachment } from './attachment-outbox.mjs';
import { ATTACHMENT_LIMITS } from '../../modules/world/attachment-contract.mjs';

// Local rendering ceilings for this pilot, independent of upload admission.
// Nothing is persisted. A new visible view always makes new signed reads.
export const PREVIEW_LIMITS = Object.freeze({ reads: 2, queue: 24, liveUrls: 14,
  liveBytes: 6 * 1024 * 1024, pixels: 24_000_000, side: 12_000,
  thumbnailSide: 384, gallerySide: 1600, thumbnailBytes: 256 * 1024, galleryBytes: 2 * 1024 * 1024 });
const types = new Set(['image/jpeg', 'image/png', 'image/webp']);
const fail = code => { throw Object.assign(Error(code), { code }); };
const need = (condition, code) => { if (!condition) fail(code); };
// The existing wire intentionally reports octet-stream. A filename is only a
// lazy-selection hint: it never bypasses signed reads or the encoded-byte gate.
export const isRasterAttachment = attachment => !!attachment && (types.has(attachment.mediaType)
  || attachment.mediaType === 'application/octet-stream' && typeof attachment.name === 'string' && /\.(?:png|jpe?g|webp)$/iu.test(attachment.name))
  && Number.isSafeInteger(attachment.size) && attachment.size > 0 && attachment.size <= ATTACHMENT_LIMITS.fileBytes;

/** The name and declared MIME never make HTML/SVG into an image. Check the
 * encoded dimensions before invoking a browser decoder; re-encode its pixels. */
export function inspectRaster(bytes, declaredType) {
  need(bytes instanceof Uint8Array && bytes.length > 0 && bytes.length <= ATTACHMENT_LIMITS.fileBytes, 'attachment_preview_format');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let mime, width, height;
  const ascii = (offset, value) => offset + value.length <= bytes.length && [...value].every((c, i) => bytes[offset + i] === c.charCodeAt(0));
  if (bytes.length >= 33 && bytes[0] === 137 && ascii(1, 'PNG\r\n\x1a\n') && view.getUint32(8) === 13 && ascii(12, 'IHDR')) {
    mime = 'image/png'; width = view.getUint32(16); height = view.getUint32(20);
    // Animated containers stay a normal download, rather than an unbounded player.
    for (let p = 8; p + 12 <= bytes.length;) {
      const length = view.getUint32(p); need(length <= bytes.length - p - 12, 'attachment_preview_format');
      need(!ascii(p + 4, 'acTL'), 'attachment_preview_format');
      if (ascii(p + 4, 'IDAT') || ascii(p + 4, 'IEND')) break;
      p += length + 12;
    }
  } else if (bytes.length >= 4 && bytes[0] === 255 && bytes[1] === 216) {
    mime = 'image/jpeg'; let p = 2;
    for (let count = 0; p < bytes.length && count < 2048; count++) {
      need(bytes[p++] === 255, 'attachment_preview_format'); while (bytes[p] === 255) p++;
      const marker = bytes[p++]; need(marker !== undefined && marker !== 0 && marker !== 218 && marker !== 217, 'attachment_preview_format');
      if (marker === 1 || marker >= 208 && marker <= 215) continue;
      need(p + 2 <= bytes.length, 'attachment_preview_format'); const length = view.getUint16(p);
      need(length >= 2 && p + length <= bytes.length, 'attachment_preview_format');
      if ([192, 193, 194, 195, 197, 198, 199, 201, 202, 203, 205, 206, 207].includes(marker)) {
        need(length >= 8, 'attachment_preview_format'); height = view.getUint16(p + 3); width = view.getUint16(p + 5); break;
      }
      p += length;
    }
  } else if (bytes.length >= 25 && ascii(0, 'RIFF') && ascii(8, 'WEBP') && view.getUint32(4, true) + 8 === bytes.length) {
    mime = 'image/webp';
    const le24 = p => bytes[p] + bytes[p + 1] * 256 + bytes[p + 2] * 65536;
    if (ascii(12, 'VP8X') && bytes.length >= 30 && view.getUint32(16, true) >= 10) {
      need(!(bytes[20] & 2), 'attachment_preview_format'); width = le24(24) + 1; height = le24(27) + 1;
    } else if (ascii(12, 'VP8L') && bytes[20] === 47) {
      const bits = view.getUint32(21, true); width = (bits & 16383) + 1; height = (bits >>> 14 & 16383) + 1;
    } else if (ascii(12, 'VP8 ') && bytes.length >= 30 && ascii(23, '\x9d\x01\x2a')) {
      width = view.getUint16(26, true) & 16383; height = view.getUint16(28, true) & 16383;
    }
  }
  need((mime === declaredType || declaredType === 'application/octet-stream') && types.has(mime) && Number.isSafeInteger(width) && Number.isSafeInteger(height) && width > 0 && height > 0, 'attachment_preview_format');
  need(width <= PREVIEW_LIMITS.side && height <= PREVIEW_LIMITS.side && width * height <= PREVIEW_LIMITS.pixels, 'attachment_preview_dimensions');
  return Object.freeze({ mime, width, height });
}

/** No input URL, fetch, document insertion or original active bytes. Browser
 * codec resources are closed even when encoding/context validation fails. */
export async function rasterPreview(blob, info, kind) {
  need(typeof globalThis.createImageBitmap === 'function', 'attachment_preview_unsupported');
  const side = kind === 'gallery' ? PREVIEW_LIMITS.gallerySide : PREVIEW_LIMITS.thumbnailSide;
  let bitmap, canvas;
  try {
    // Native default orientation applies EXIF before we choose a canvas aspect.
    // Two resize dimensions from the encoded header can squeeze a rotated JPEG.
    bitmap = await createImageBitmap(blob);
    need(Number.isSafeInteger(bitmap.width) && Number.isSafeInteger(bitmap.height) && bitmap.width > 0 && bitmap.height > 0
      && bitmap.width <= PREVIEW_LIMITS.side && bitmap.height <= PREVIEW_LIMITS.side && bitmap.width * bitmap.height <= PREVIEW_LIMITS.pixels, 'attachment_preview_dimensions');
    const scale = Math.min(1, side / Math.max(bitmap.width, bitmap.height));
    const width = Math.max(1, Math.round(bitmap.width * scale)), height = Math.max(1, Math.round(bitmap.height * scale));
    canvas = typeof OffscreenCanvas === 'function' ? new OffscreenCanvas(width, height) : document.createElement('canvas');
    canvas.width = width; canvas.height = height;
    const context = canvas.getContext('2d'); need(context, 'attachment_preview_unsupported'); context.drawImage(bitmap, 0, 0, width, height);
    const output = typeof canvas.convertToBlob === 'function' ? await canvas.convertToBlob({ type: 'image/webp', quality: .86 })
      : await new Promise((resolve, reject) => canvas.toBlob(value => value ? resolve(value) : reject(Object.assign(Error('attachment_preview_unsupported'), { code: 'attachment_preview_unsupported' })), 'image/webp', .86));
    need(output instanceof Blob && ['image/webp', 'image/png'].includes(output.type), 'attachment_preview_format');
    return output;
  } finally { bitmap?.close(); if (canvas) canvas.width = canvas.height = 1; }
}

/** Bounded active-view RAM pool, not a bearer cache. No reuse across view
 * epochs: every thumbnail/gallery/download goes through signed status/read.
 * API has no cancellation port; stale reads stop between chunks and discard. */
export function createAttachmentPreviewPool({ api, accountId, communityId, current = () => true,
  transform = rasterPreview, urls = globalThis.URL }) {
  const queued = [], leases = new Set(); let running = 0, closed = false, generation = 0, decodeTail = Promise.resolve();
  const active = (guard, epoch) => !closed && epoch === generation && current() && guard();
  const releaseAll = () => { for (const lease of [...leases]) lease.release(); };
  function leaseFor(blob, name, kind, released) {
    // A clicked download keeps its URL for the existing one-second lifetime;
    // an active gallery must not disappear under download pressure. Plan the
    // thumbnail-only eviction before allocating any new URL or changing a view.
    let count = leases.size + 1, bytes = [...leases].reduce((n, v) => n + v.bytes, blob.size);
    const victims = [];
    for (const candidate of leases) {
      if (count <= PREVIEW_LIMITS.liveUrls && bytes <= PREVIEW_LIMITS.liveBytes) break;
      if (candidate.kind !== 'thumbnail') continue;
      victims.push(candidate); count--; bytes -= candidate.bytes;
    }
    need(count <= PREVIEW_LIMITS.liveUrls && bytes <= PREVIEW_LIMITS.liveBytes, 'attachment_preview_capacity');
    for (const victim of victims) victim.release();
    const lease = { url: urls.createObjectURL(blob), name, bytes: blob.size, kind, release() {
      if (!leases.delete(lease)) return; urls.revokeObjectURL(lease.url); released?.();
    } };
    leases.add(lease);
    return lease;
  }
  function pump() {
    while (running < PREVIEW_LIMITS.reads && queued.length) {
      const job = queued.shift(); running++;
      void (async () => {
        need(active(job.current, job.epoch), 'attachment_context_changed');
        const valid = () => active(job.current, job.epoch);
        const result = await downloadAttachment({ api, accountId, communityId, attachment: job.attachment, current: valid });
        need(valid(), 'attachment_context_changed'); let output = result.blob;
        if (job.kind !== 'download') {
          const bytes = new Uint8Array(await result.blob.arrayBuffer()); need(valid(), 'attachment_context_changed');
          const info = inspectRaster(bytes, job.attachment.mediaType);
          const decode = async () => { need(valid(), 'attachment_context_changed'); return transform(result.blob, info, job.kind); };
          // One potentially large native decoder at a time; at most two file Blobs.
          const pending = decodeTail.then(decode, decode); decodeTail = pending.then(() => undefined, () => undefined);
          output = await pending; need(valid(), 'attachment_context_changed');
          const bound = job.kind === 'gallery' ? PREVIEW_LIMITS.galleryBytes : PREVIEW_LIMITS.thumbnailBytes;
          need(output instanceof Blob && output.size > 0 && output.size <= bound && ['image/webp', 'image/png'].includes(output.type), 'attachment_preview_capacity');
        }
        need(valid(), 'attachment_context_changed'); return leaseFor(output, result.name, job.kind, job.released);
      })().then(job.resolve, job.reject).finally(() => { running--; pump(); });
    }
  }
  async function load(attachment, { kind = 'thumbnail', current: guard = () => true, released } = {}) {
    need(['thumbnail', 'gallery', 'download'].includes(kind), 'attachment_preview_format');
    need(!closed && current() && guard(), 'attachment_context_changed');
    need(kind === 'download' || isRasterAttachment(attachment), 'attachment_preview_format');
    need(queued.length < PREVIEW_LIMITS.queue, 'attachment_preview_busy');
    // Bind the exact message ref once; a mutable UI object cannot change it mid-read.
    const ref = Object.freeze({ attachmentId: attachment.attachmentId, name: attachment.name, size: attachment.size,
      chunks: attachment.chunks, mediaType: attachment.mediaType, manifestSha256: attachment.manifestSha256, fileSha256: attachment.fileSha256 });
    return new Promise((resolve, reject) => {
      const job = { attachment: ref, kind, current: guard, released, epoch: generation, resolve, reject };
      if (kind === 'gallery') queued.unshift(job); else queued.push(job); pump();
    });
  }
  return Object.freeze({ load, clear() {
    generation++; for (const job of queued.splice(0)) job.reject(Object.assign(Error('attachment_context_changed'), { code: 'attachment_context_changed' })); releaseAll();
  }, close() { closed = true; this.clear(); }, snapshot: () => ({ running, queued: queued.length, liveUrls: leases.size, liveBytes: [...leases].reduce((n, v) => n + v.bytes, 0), closed }) });
}
