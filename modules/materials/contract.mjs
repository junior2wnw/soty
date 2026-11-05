import { attachmentManifest, ATTACHMENT_LIMITS } from '../world/attachment-contract.mjs';
import { attachmentFileName } from '../world/attachment-file-policy.mjs';

/** Local product limits, not claims about browser/device capacity. */
export const MATERIAL_LIMITS = Object.freeze({ chunkBytes: ATTACHMENT_LIMITS.chunkBytes,
  fileBytes: ATTACHMENT_LIMITS.fileBytes, accountBytes: 256 * 1024 * 1024,
  storeBytes: 2 * 1024 * 1024 * 1024, databaseBytes: 3 * 1024 * 1024 * 1024,
  accountRecords: 2000, records: 20000, pending: 8, pendingMs: 24 * 60 * 60 * 1000,
  receipts: 32, imageSide: 8192, imagePixels: 16 * 1024 * 1024, audioSeconds: 600,
  videoSide: 4096, videoPixels: 4096 * 2160, videoSeconds: 600 });
export const MATERIAL_OPERATIONS = Object.freeze(['materials.list', 'materials.get', 'materials.begin', 'materials.chunk',
  'materials.commit', 'materials.status', 'materials.read', 'materials.trash', 'materials.restore', 'materials.cancel']);
export const MATERIAL_MEDIA_TYPES = Object.freeze({ image: ['image/png', 'image/jpeg', 'image/webp'],
  audio: ['audio/webm', 'audio/ogg', 'audio/wav'], video: ['video/webm'] });
export class MaterialsError extends Error { constructor(code = 'materials_invalid_arguments') { super(code); this.name = 'MaterialsError'; this.code = code; } }
export function materialCheck(condition, code) { if (!condition) throw new MaterialsError(code); }
export function materialKeys(value, allowed) {
  materialCheck(value && typeof value === 'object' && !Array.isArray(value)
    && [null, Object.prototype].includes(Object.getPrototypeOf(value))
    && Object.keys(value).every(key => allowed.includes(key)));
}
export function materialId(value) { materialCheck(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_-]{7,95}$/u.test(value)); return value; }
export function materialHash(value) { materialCheck(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value)); return value; }
export function materialMediaType(kind, value) {
  materialCheck(Object.hasOwn(MATERIAL_MEDIA_TYPES, kind) && typeof value === 'string');
  const match = /^(image\/(?:png|jpeg|webp)|audio\/(?:webm|ogg|wav)|video\/webm)(?:\s*;\s*codecs\s*=\s*(?:"([^";]+)"|([^";]+)))?$/iu.exec(value.trim());
  const codecs=(match?.[2]??match?.[3]??'').toLowerCase().split(',').map(value=>value.trim()).filter(Boolean);
  materialCheck(match && MATERIAL_MEDIA_TYPES[kind].includes(match[1].toLowerCase())
    && (codecs.length===0 || (['audio/webm','audio/ogg'].includes(match[1].toLowerCase())&&codecs.length===1&&codecs[0]==='opus')
      || (kind==='video'&&codecs.includes('vp8')&&codecs.every(value=>['vp8','opus'].includes(value))&&new Set(codecs).size===codecs.length)), 'materials_media_type');
  return match[1].toLowerCase();
}
export function materialFileName(value) { materialCheck(typeof value === 'string' && value.length <= 1024); return attachmentFileName(value); }
export function materialManifest(size, chunks) {
  // Reuse the established bounded chunk/hash validator, not the chat scope or its storage format.
  try { attachmentManifest(size, chunks); } catch { throw new MaterialsError('materials_manifest_invalid'); }
  materialCheck(size > 0, 'materials_file_empty');
  return JSON.stringify({ schema: 'soty.material.chunks.v1', size, chunkBytes: MATERIAL_LIMITS.chunkBytes, chunks });
}
