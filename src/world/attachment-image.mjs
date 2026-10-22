import { rasterFileDimensions } from './feedback-media.mjs';
import { ATTACHMENT_LIMITS } from '../../modules/world/attachment-contract.mjs';

/** Preview only: the original Blob stays intact and remains the download. */
export async function attachmentImage(blob) {
  const fail = () => { throw Object.assign(Error('attachment_image_unsupported'), { code: 'attachment_image_unsupported' }); };
  if (!(blob instanceof Blob) || blob.size > ATTACHMENT_LIMITS.fileBytes) fail();
  const header = new Uint8Array(await blob.slice(0, ATTACHMENT_LIMITS.chunkBytes).arrayBuffer());
  const mime = header[0] === 137 && header[1] === 80 ? 'image/png' : header[0] === 255 && header[1] === 216 ? 'image/jpeg'
    : new TextDecoder().decode(header.subarray(0, 4)) === 'RIFF' ? 'image/webp' : null;
  if (!mime) fail();
  let size; try { size = rasterFileDimensions(header, mime); } catch { fail(); }
  return { blob: new Blob([blob], { type: mime }), ...size };
}
