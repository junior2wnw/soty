// Advisory display/download names only. Never use them as storage paths or IDs.
// Download authorization and byte/MIME validation belong to the serving adapter.
const encoder = new TextEncoder();
const forbidden = /[\p{Cc}\u00ad\u061c\u200b\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff]/gu;
const device = /^(?:con|prn|aux|nul|conin\$|conout\$|com[1-9¹²³]|lpt[1-9¹²³])(?:\.|$)/iu;
export const ATTACHMENT_NAME_MAX_BYTES = 240;

function wellFormed(value) {
  return Array.from(value, character => {
    const first = character.charCodeAt(0);
    return character.length === 1 && first >= 0xd800 && first <= 0xdfff ? '\ufffd' : character;
  }).join('');
}

// A small, deterministic filename rule: keep combining marks, emoji modifiers,
// flag pairs, emoji tags and ZWJ sequences together on every supported engine.
// This is not a general-purpose Unicode grapheme segmentation API.
function nameUnits(value) {
  const units = [];
  for (const character of value) {
    const last = units.at(-1);
    const extendsUnit = /[\p{Mark}\p{Emoji_Modifier}\u{e0020}-\u{e007f}]/u.test(character)
      || character === '\u200d' || last?.endsWith('\u200d')
      || /^[\u{1f1e6}-\u{1f1ff}]$/u.test(character) && /^[\u{1f1e6}-\u{1f1ff}]$/u.test(last ?? '');
    if (last !== undefined && extendsUnit) units[units.length - 1] += character;
    else units.push(character);
  }
  return units;
}

function bounded(value, budget) {
  let result = '', bytes = 0;
  for (const part of nameUnits(value)) {
    const size = encoder.encode(part).byteLength;
    if (bytes + size > budget) break;
    result += part; bytes += size;
  }
  return result;
}

/** One deterministic policy shared by server and composer. No content trust. */
export function attachmentFileName(input) {
  if (typeof input !== 'string' || input.length > 4096) throw new TypeError('attachment_filename_invalid');
  // Keep only the final supplied path component before normalizing its content.
  let name = wellFormed(input).split(/[\\/]/u).at(-1).normalize('NFC')
    .replace(forbidden, '').replace(/[<>:"|?*]/gu, ' ').replace(/\s+/gu, ' ').trim().replace(/[. ]+$/gu, '');
  if (!name || /^[.~ ]+$/u.test(name)) name = 'файл';
  if (device.test(name)) name = '_' + name;
  if (encoder.encode(name).byteLength > ATTACHMENT_NAME_MAX_BYTES) {
    // Preserve an ordinary short extension; it remains an untrusted name hint.
    const extension = /\.[A-Za-z0-9]{1,12}$/u.exec(name)?.[0] ?? '';
    const stem = extension ? name.slice(0, -extension.length) : name;
    const shortened = bounded(stem, ATTACHMENT_NAME_MAX_BYTES - encoder.encode(extension).byteLength).trimEnd();
    name = (shortened || 'файл') + extension;
    name = name.replace(/[. ]+$/gu, '') || 'файл';
  }
  // Truncation may turn a long name into a reserved device name.
  if (device.test(name)) name = '_' + bounded(name, ATTACHMENT_NAME_MAX_BYTES - 1);
  return name;
}

/** RFC6266 attachment disposition, UTF-8 plus conservative legacy fallback. */
export function attachmentDisposition(input) {
  const name = attachmentFileName(input);
  const extension = /\.[A-Za-z0-9]{1,12}$/u.exec(name)?.[0] ?? '';
  const fallback = /^[A-Za-z0-9 _().+-]+$/u.test(name) ? name : 'file' + extension;
  const encoded = encodeURIComponent(name).replace(/[!'()*]/gu,
    character => '%' + character.charCodeAt(0).toString(16).toUpperCase());
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encoded}`;
}

/** Arbitrary documents download as bytes. Raster previews use a separate port. */
export function attachmentDownloadHeaders(input) {
  return Object.freeze({
    'content-type': 'application/octet-stream',
    'content-disposition': attachmentDisposition(input),
    'x-content-type-options': 'nosniff',
    'content-security-policy': "sandbox; default-src 'none'",
    'cache-control': 'private, no-store',
  });
}
