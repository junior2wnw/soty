import manifest from './app-art-manifest.json' with { type: 'json' };
import { appArtBindingKey } from './app-art-identity.mjs';

const KEY = /^[a-z][a-z0-9-]{1,63}$/u;
const COLOR = /^#[0-9a-f]{6}$/iu;
const URL_PATH = /^\/app-art\/[a-z][a-z0-9-]{1,63}\/v\d{3,6}-[a-f0-9]{12}\/cover-\d{2,4}\.[a-f0-9]{12}\.webp$/u;
const DEFAULT_SIZES = '(max-width: 600px) 100vw, (max-width: 1000px) 50vw, 360px';
const FALLBACK_PALETTES = [
  { base: '#343029', accent: '#E8CFAB', ink: '#F5F0E9' },
  { base: '#29362F', accent: '#BAD3BA', ink: '#EFF5EF' },
  { base: '#392F44', accent: '#CCACEF', ink: '#F4EDF9' },
  { base: '#3D2E2B', accent: '#DEAC9C', ink: '#FBF1EA' },
  { base: '#2B3542', accent: '#ACCAE2', ink: '#EFF5FA' },
];
const own = (object, key) => object && Object.hasOwn(object, key) ? object[key] : undefined;
const safeKey = value => typeof value === 'string' && KEY.test(value) ? value : null;
const safeUrl = value => typeof value === 'string' && URL_PATH.test(value) ? value : null;
function fingerprint(value) { let hash = 2166136261; for (const char of value) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619); return hash >>> 0; }
function paletteFor(palette, seed) {
  const fallback = FALLBACK_PALETTES[fingerprint(seed) % FALLBACK_PALETTES.length];
  return Object.freeze(Object.fromEntries(['base', 'accent', 'ink'].map(key => [key, COLOR.test(palette?.[key] || '') ? palette[key] : fallback[key]])));
}
function focalPointFor(value, fallback = { x: 0.5, y: 0.5 }) {
  const coordinate = (number, defaultValue) => typeof number === 'number' && Number.isFinite(number) ? Math.max(0, Math.min(1, number)) : defaultValue;
  return Object.freeze({ x: coordinate(value?.x, fallback.x), y: coordinate(value?.y, fallback.y) });
}

/** Only explicit cover keys or stable app-id bindings select artwork. Names never do. */
export function createAppArtResolver(source) {
  return function resolve(input) {
    const record = input && typeof input === 'object' ? input : null;
    const id = typeof record?.appId === 'string' ? record.appId : typeof record?.id === 'string' ? record.id : typeof input === 'string' ? input : '';
    const requested = typeof input === 'string' ? safeKey(input) : safeKey(record?.coverKey);
    const key = requested && own(source?.assets, requested) ? requested : safeKey(own(source?.bindings, appArtBindingKey(id) || ''));
    const asset = key ? own(source?.assets, key) : null;
    const metadata = asset;
    const variants = Array.isArray(asset?.renditions) ? asset.renditions.filter(item => {
      const url = safeUrl(item?.url);
      return url && url.startsWith(`/app-art/${key}/`) && Number.isInteger(item.width) && item.width >= 160 && item.width <= 4096 && Number.isInteger(item.height) && item.height > 0 && item.height <= 4096;
    }).sort((a, b) => a.width - b.width) : [];
    const unique = variants.filter((item, index) => index === 0 || item.width !== variants[index - 1].width);
    const preferred = unique.find(item => item.width >= 640) || unique.at(-1);
    const alt = typeof metadata?.alt === 'string' && metadata.alt.length <= 300 ? metadata.alt : '';
    const focalPoint = focalPointFor(metadata?.focalPoint);
    return Object.freeze({
      key, status: preferred ? 'ready' : 'fallback', src: preferred?.url || null,
      srcset: unique.map(item => `${item.url} ${item.width}w`).join(', '), sizes: DEFAULT_SIZES,
      width: preferred?.width || 960, height: preferred?.height || 640,
      alt, palette: paletteFor(metadata?.palette, (key || id).slice(0, 160)), focalPoint, compactFocalPoint: focalPointFor(metadata?.compactFocalPoint, focalPoint),
      version: preferred && Number.isInteger(asset.version) ? asset.version : null,
    });
  };
}

export const resolveAppArt = createAppArtResolver(manifest);

/** Install before assigning src. A failed image exposes the card's native palette. */
export function installAppArtFallback(image, onFallback) {
  let active = true;
  const fail = () => {
    if (!active) return;
    active = false;
    image.removeEventListener('error', fail);
    image.hidden = true;
    image.dataset.artState = 'fallback';
    image.removeAttribute('srcset');
    image.removeAttribute('src');
    if (typeof onFallback === 'function') onFallback(image);
  };
  image.addEventListener('error', fail);
  queueMicrotask(() => { if (active && image.complete && image.hasAttribute('src') && image.naturalWidth === 0) fail(); });
  return () => { active = false; image.removeEventListener('error', fail); };
}
