import { createHash } from 'node:crypto';
import sharp from 'sharp';

export const COVER_RECIPES = Object.freeze({
  app: Object.freeze([['s160',160,20_000],['s320',320,60_000],['s640',640,140_000],['s960',960,250_000]]),
  profile: Object.freeze([['image',512,98_304],['thumbnail',192,8_192]]),
});
const failure = code => Object.assign(new Error(code), { code });
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function rejectAnimation(bytes,format) {
  if(format==='png')for(let at=8;at+12<=bytes.length;){const length=bytes.readUInt32BE(at),type=bytes.toString('ascii',at+4,at+8);if(['acTL','fcTL','fdAT'].includes(type))throw failure('cover_image_invalid');if(length>bytes.length-at-12)break;at+=length+12;}
  if(format==='webp')for(let at=12;at+8<=bytes.length;){const type=bytes.toString('ascii',at,at+4),length=bytes.readUInt32LE(at+4);if(['ANIM','ANMF'].includes(type))throw failure('cover_image_invalid');if(length>bytes.length-at-8)break;at+=8+length+length%2;}
}
export function normalizedCoverCrop(value = { x: .5, y: .5, zoom: 1 }) {
  if (!value || Object.keys(value).sort().join(',') !== 'x,y,zoom' || !['x','y','zoom'].every(key => Number.isFinite(value[key]))
    || value.x < 0 || value.x > 1 || value.y < 0 || value.y > 1 || value.zoom < 1 || value.zoom > 4) throw failure('cover_crop_invalid');
  return { x: value.x, y: value.y, zoom: value.zoom };
}
export async function canonicalizeCover({ kind, bytes, mime, crop }) {
  const recipe = COVER_RECIPES[kind];
  if (!recipe || !Buffer.isBuffer(bytes) || !bytes.length || bytes.length > 320 * 1024) throw failure('cover_input_limit');
  const format = ({ 'image/png':'png','image/jpeg':'jpeg','image/webp':'webp' })[mime];
  if (!format) throw failure('cover_mime_invalid');
  const framing = normalizedCoverCrop(crop);
  try {
    rejectAnimation(bytes,format);
    const image = sharp(bytes, { limitInputPixels: 1_048_576, failOn: 'error' });
    const meta = await image.metadata();
    if (meta.format !== format || !meta.width || !meta.height || meta.width * meta.height > 1_048_576 || meta.pages > 1) throw failure('cover_image_invalid');
    const rotated = await image.rotate().png().toBuffer({ resolveWithObject: true });
    const size = Math.max(1, Math.floor(Math.min(rotated.info.width, rotated.info.height) / framing.zoom));
    const left = Math.max(0, Math.min(rotated.info.width - size, Math.round(framing.x * rotated.info.width - size / 2)));
    const top = Math.max(0, Math.min(rotated.info.height - size, Math.round(framing.y * rotated.info.height - size / 2)));
    const renditions = [];
    for (const [role, maximum, limit] of recipe) {
      let width = Math.min(maximum, size), accepted;
      while (!accepted) {
        for (const quality of [82,62,42,26]) {
          let output = sharp(rotated.data, { limitInputPixels: 1_048_576, failOn: 'error' }).extract({ left, top, width: size, height: size }).resize({ width, height: width, withoutEnlargement: true });
          if (kind === 'profile') output = output.flatten({ background: '#f4efe4' });
          const rendered = await output.webp({ quality, effort: 4 }).toBuffer({ resolveWithObject: true });
          if (rendered.data.length <= limit) { accepted = { role, width: rendered.info.width, height: rendered.info.height, mime: 'image/webp', sha256: hash(rendered.data), bytes: rendered.data }; break; }
        }
        if (!accepted) { if (width <= 32) throw failure('cover_output_limit'); width = Math.max(32, Math.floor(width * .75)); }
      }
      renditions.push(accepted);
    }
    return { recipe: `cover-${kind}-webp-v1`, crop: framing, renditions };
  } catch (error) { if (String(error?.code || '').startsWith('cover_')) throw error; throw failure('cover_image_invalid'); }
}

/** Share one instance across Apps/World in a configured worker process. A queue
 * timeout only removes unstarted work; a running native job retains its slot. */
export function createCoverDecoder({ concurrency = 2, queueLimit = 16, ownerQueueLimit = 2, queueMs = 30_000, decode = canonicalizeCover } = {}) {
  if (![concurrency,queueLimit,ownerQueueLimit,queueMs].every(value => Number.isSafeInteger(value) && value > 0) || typeof decode !== 'function') throw failure('cover_decoder_configuration_invalid');
  let active = 0; const owners = new Set(), queue = [];
  const pump = () => {
    while (active < concurrency) {
      const index = queue.findIndex(entry => !owners.has(entry.ownerId)); if (index < 0) break;
      const entry = queue.splice(index,1)[0]; clearTimeout(entry.timer); active++; owners.add(entry.ownerId);
      Promise.resolve().then(() => decode(entry.input)).then(entry.resolve, entry.reject).finally(() => { active--; owners.delete(entry.ownerId); pump(); });
    }
  };
  return Object.freeze({ decode(ownerId,input) {
    if (typeof ownerId !== 'string' || queue.length >= queueLimit || queue.filter(entry => entry.ownerId === ownerId).length >= ownerQueueLimit) return Promise.reject(failure('cover_decoder_busy'));
    return new Promise((resolve,reject) => {
      const entry = { ownerId,input,resolve,reject,timer:null };
      entry.timer = setTimeout(() => { const index = queue.indexOf(entry); if (index >= 0) { queue.splice(index,1); reject(failure('cover_decoder_queue_timeout')); } }, queueMs);
      queue.push(entry); pump();
    });
  }, stats() { return { active, queued: queue.length, activeOwners: owners.size }; } });
}
