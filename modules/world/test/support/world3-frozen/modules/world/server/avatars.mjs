import { assert, exact, identifier, revision, WorldError } from './validation.mjs';

export const AVATAR_OPERATIONS = ['world.profile.avatar.set', 'world.profile.avatar.read', 'world.profile.avatars'];
export const AVATAR_LIMITS = Object.freeze({ imageBytes: 96 * 1024, thumbnailBytes: 8 * 1024, imagePixels: 512, thumbnailPixels: 192, batch: 24 });
const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
const crcTable = Array.from({ length: 256 }, (_, value) => {
  let result = value; for (let i = 0; i < 8; i++) result = result & 1 ? 0xedb88320 ^ (result >>> 1) : result >>> 1;
  return result >>> 0;
});
function crc(bytes) { let value = 0xffffffff; for (const byte of bytes) value = crcTable[(value ^ byte) & 255] ^ (value >>> 8); return (value ^ 0xffffffff) >>> 0; }
const valid = condition => assert(condition, 'invalid_avatar_data');
function dimensions(width, height, maximum) {
  assert(Number.isInteger(width) && Number.isInteger(height) && width > 0 && height > 0 && width <= maximum && height <= maximum, 'avatar_dimensions');
  return { width, height };
}
function png(bytes, maximum) {
  valid(bytes.length >= 57 && bytes.subarray(0, 8).equals(signature));
  let offset = 8, size, ended = false, image = false;
  while (offset < bytes.length) {
    valid(offset + 12 <= bytes.length); const length = bytes.readUInt32BE(offset); const end = offset + 12 + length;
    valid(end <= bytes.length); const type = bytes.toString('latin1', offset + 4, offset + 8);
    valid(crc(bytes.subarray(offset + 4, end - 4)) === bytes.readUInt32BE(end - 4));
    // Only still raster content and small colour hints; no APNG, EXIF, text or opaque metadata.
    valid(['IHDR', 'PLTE', 'IDAT', 'IEND', 'tRNS', 'sRGB', 'gAMA', 'cHRM', 'pHYs'].includes(type));
    if (type === 'IHDR') {
      valid(offset === 8 && !size && length === 13);
      size = dimensions(bytes.readUInt32BE(offset + 8), bytes.readUInt32BE(offset + 12), maximum);
      const depth = bytes[offset + 16], color = bytes[offset + 17];
      valid(({ 0: [1, 2, 4, 8, 16], 2: [8, 16], 3: [1, 2, 4, 8], 4: [8, 16], 6: [8, 16] })[color]?.includes(depth));
      valid(bytes[offset + 18] === 0 && bytes[offset + 19] === 0 && bytes[offset + 20] <= 1);
    } else valid(Boolean(size));
    if (type === 'IDAT') { valid(length > 0); image = true; }
    if (type === 'IEND') { valid(length === 0 && image && end === bytes.length); ended = true; }
    offset = end;
  }
  valid(ended); return size;
}
function jpeg(bytes, maximum) {
  valid(bytes.length >= 20 && bytes[0] === 255 && bytes[1] === 216 && bytes[bytes.length - 2] === 255 && bytes[bytes.length - 1] === 217);
  let offset = 2, size;
  while (offset < bytes.length - 2) {
    valid(bytes[offset++] === 255); while (bytes[offset] === 255) offset++;
    const marker = bytes[offset++];
    valid(offset + 2 <= bytes.length && ![0, 216, 217].includes(marker));
    const length = bytes.readUInt16BE(offset); valid(length >= 2 && offset + length <= bytes.length - 2);
    // Browser canvas emits JFIF/quantization/Huffman/scan segments. Metadata including EXIF GPS
    // is rejected: clients must decode and rasterize a selected photo before uploading it.
    valid([192, 193, 194, 196, 218, 219, 221, 224].includes(marker));
    if ([192, 193, 194].includes(marker)) {
      valid(!size && length >= 8 && bytes[offset + 2] === 8);
      size = dimensions(bytes.readUInt16BE(offset + 5), bytes.readUInt16BE(offset + 3), maximum);
    }
    if (marker === 218) { valid(Boolean(size) && offset + length < bytes.length - 2); return size; }
    offset += length;
  }
  throw new WorldError('invalid_avatar_data');
}
function webp(bytes, maximum) {
  valid(bytes.length >= 26 && bytes.toString('latin1', 0, 4) === 'RIFF' && bytes.toString('latin1', 8, 12) === 'WEBP' && bytes.readUInt32LE(4) + 8 === bytes.length);
  let offset = 12, size, decoded, images = 0;
  while (offset < bytes.length) {
    valid(offset + 8 <= bytes.length); const type = bytes.toString('latin1', offset, offset + 4);
    const length = bytes.readUInt32LE(offset + 4), start = offset + 8, end = start + length;
    valid(end + (length % 2) <= bytes.length && ['VP8X', 'VP8 ', 'VP8L', 'ALPH'].includes(type));
    if (length % 2) valid(bytes[end] === 0);
    if (type === 'VP8X') {
      valid(offset === 12 && length === 10 && (bytes[start] & ~0x10) === 0 && bytes.readUIntBE(start + 1, 3) === 0);
      size = dimensions(bytes.readUIntLE(start + 4, 3) + 1, bytes.readUIntLE(start + 7, 3) + 1, maximum);
    }
    if (type === 'VP8 ') {
      valid(length >= 10 && (bytes[start] & 1) === 0 && bytes.subarray(start + 3, start + 6).equals(Buffer.from([157, 1, 42])));
      decoded = dimensions(bytes.readUInt16LE(start + 6) & 0x3fff, bytes.readUInt16LE(start + 8) & 0x3fff, maximum); images++;
    }
    if (type === 'VP8L') {
      valid(length >= 5 && bytes[start] === 47 && bytes[start + 4] >>> 5 === 0);
      decoded = dimensions((bytes[start + 1] | (bytes[start + 2] & 63) << 8) + 1,
        ((bytes[start + 2] >>> 6) | bytes[start + 3] << 2 | (bytes[start + 4] & 15) << 10) + 1, maximum); images++;
    }
    offset = end + (length % 2);
  }
  valid(images === 1 && decoded && (!size || size.width === decoded.width && size.height === decoded.height));
  return decoded;
}

/** A small raster/container validator, not an image decoder or malware scanner.
 * Stored bytes only leave through signed RPC and are displayed in an img element. */
export function validateAvatar(value, { maximumBytes, maximumPixels }) {
  assert(typeof value === 'string' && /^data:image\/(png|jpeg|webp);base64,/u.test(value), 'invalid_avatar_mime');
  assert(value.length <= Math.ceil(maximumBytes / 3) * 4 + 32, 'avatar_too_large');
  const match = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]+={0,2})$/u.exec(value);
  valid(match && match[2].length % 4 === 0);
  const bytes = Buffer.from(match[2], 'base64');
  valid(bytes.toString('base64') === match[2]); assert(bytes.length <= maximumBytes, 'avatar_too_large');
  const mime = match[1];
  const size = mime === 'image/png' ? png(bytes, maximumPixels) : mime === 'image/jpeg' ? jpeg(bytes, maximumPixels) : webp(bytes, maximumPixels);
  return { mime, bytes, ...size };
}
const dataUrl = (mime, bytes) => `data:${mime};base64,${Buffer.from(bytes).toString('base64')}`;

export function avatarOperation(m, op, args, actor, now) {
  if (op === 'world.profile.avatar.set') {
    exact(args, ['expectedRevision', 'avatarUrl', 'thumbnailUrl']);
    const profile = m.person(actor.accountId); revision(args, profile);
    if (args.avatarUrl === null || args.thumbnailUrl === null) {
      valid(args.avatarUrl === null && args.thumbnailUrl === null);
      m.run('DELETE FROM profile_avatars WHERE account_id=?', actor.accountId);
      m.run('UPDATE profiles SET avatar_revision=NULL,revision=revision+1,updated_at=? WHERE account_id=?', now, actor.accountId);
    } else {
      const image = validateAvatar(args.avatarUrl, { maximumBytes: AVATAR_LIMITS.imageBytes, maximumPixels: AVATAR_LIMITS.imagePixels });
      const thumb = validateAvatar(args.thumbnailUrl, { maximumBytes: AVATAR_LIMITS.thumbnailBytes, maximumPixels: AVATAR_LIMITS.thumbnailPixels });
      m.rate('avatar:' + actor.accountId, 12, 60_000, now);
      m.run(`INSERT INTO profile_avatars(account_id,mime,image,thumbnail_mime,thumbnail) VALUES (?,?,?,?,?)
        ON CONFLICT(account_id) DO UPDATE SET mime=excluded.mime,image=excluded.image,thumbnail_mime=excluded.thumbnail_mime,thumbnail=excluded.thumbnail`,
      actor.accountId, image.mime, image.bytes, thumb.mime, thumb.bytes);
      m.run('UPDATE profiles SET avatar_revision=revision+1,revision=revision+1,updated_at=? WHERE account_id=?', now, actor.accountId);
    }
    m.audit(actor.accountId, op, actor.accountId, now);
    return { profile: m.profile(m.person(actor.accountId), true) };
  }
  const batch = op === 'world.profile.avatars';
  exact(args, batch ? ['profileIds'] : ['profileId'], ['communityId']);
  if (args.communityId !== undefined) m.requireMember(identifier(args.communityId), actor.accountId);
  const allowed = profile => profile && (profile.account_id === actor.accountId || profile.discoverable || args.communityId && (
    m.membership(args.communityId, profile.account_id)?.state === 'active'
    || m.get('SELECT 1 FROM messages WHERE community_id=? AND author_id=? LIMIT 1', args.communityId, profile.account_id)));
  function image(profileId, thumbnail) {
    const profile = m.person(identifier(profileId));
    if (!allowed(profile)) return null;
    const stored = m.get(thumbnail ? 'SELECT thumbnail_mime AS mime,thumbnail AS image FROM profile_avatars WHERE account_id=?'
      : 'SELECT mime,image FROM profile_avatars WHERE account_id=?', profile.account_id);
    return { profileId: profile.account_id, avatarRevision: profile.avatar_revision ?? null,
      avatarUrl: stored ? dataUrl(stored.mime, stored.image) : null };
  }
  if (!batch) {
    const result = image(args.profileId, false); assert(result, 'profile_not_found'); return result;
  }
  assert(Array.isArray(args.profileIds) && args.profileIds.length >= 1 && args.profileIds.length <= AVATAR_LIMITS.batch, 'invalid_avatar_batch');
  const avatars = [...new Set(args.profileIds.map(identifier))].map(profileId => image(profileId, true)).filter(item => item?.avatarUrl);
  return { avatars };
}
