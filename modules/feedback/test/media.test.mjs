import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { deflateSync } from 'node:zlib';
import { validateFeedbackAttachments, FEEDBACK_MEDIA_LIMITS } from '../server/media.mjs';

const file = name => readFileSync(new URL('./fixtures/' + name, import.meta.url));
const attachment = (name, mimeType, kind = mimeType.startsWith('image/') ? 'image' : 'audio', bytes = file(name)) => ({ kind, name, mimeType, dataBase64: bytes.toString('base64') });
const code = expected => error => error?.code === expected && !error.message.includes('SENTINEL');
const validate = (value, policy) => validateFeedbackAttachments([value], policy ? { limits: policy } : undefined)[0];

function crc(bytes, reflected) {
  let value = reflected ? 0xffffffff : 0;
  for (let index = 0; index < bytes.length; index++) {
    const byte = !reflected && index >= 22 && index < 26 ? 0 : bytes[index];
    value = reflected ? value ^ byte : value ^ (byte << 24);
    for (let bit = 0; bit < 8; bit++) value = reflected
      ? (value >>> 1) ^ (value & 1 ? 0xedb88320 : 0)
      : (value << 1) ^ (value & 0x80000000 ? 0x04c11db7 : 0);
  }
  return (reflected ? value ^ 0xffffffff : value) >>> 0;
}
function pngChunk(type, body) {
  const header = Buffer.alloc(8); header.writeUInt32BE(body.length); header.write(type, 4, 'ascii');
  const checksum = Buffer.alloc(4); checksum.writeUInt32BE(crc(Buffer.concat([Buffer.from(type), body]), true));
  return Buffer.concat([header, body, checksum]);
}
function oggPages(bytes) {
  const result = [];
  for (let offset = 0; offset < bytes.length;) {
    const segments = bytes[offset + 26], start = offset + 27 + segments;
    const end = start + bytes.subarray(offset + 27, start).reduce((sum, length) => sum + length, 0);
    result.push({ offset, start, end }); offset = end;
  }
  return result;
}

test('actual Chrome Canvas PNG/JPEG/WebP preserve validated dimensions and private buffers', () => {
  for (const [name, mime] of [['chrome.png', 'image/png'], ['chrome.jpg', 'image/jpeg'], ['chrome.webp', 'image/webp']]) {
    const source = file(name), accepted = validate(attachment(name, mime));
    assert.equal(accepted.width, 64); assert.equal(accepted.height, 48);
    assert.equal(accepted.byteLength, source.length); assert(Buffer.isBuffer(accepted.bytes)); assert.deepEqual(accepted.bytes, source);
    assert(Object.isFrozen(accepted)); assert.equal(accepted.durationMs, undefined);
  }
});

test('actual Chrome MediaRecorder with unknown-size streaming clusters has packet-derived duration', () => {
  const accepted = validate(attachment('chrome-recorder.webm', 'audio/webm;codecs=opus'));
  assert.equal(accepted.durationMs, 300); assert.equal(accepted.width, undefined); assert.equal(accepted.height, undefined);
  const provenance = JSON.parse(file('chrome-provenance.json'));
  assert.equal(provenance.synthetic, true); assert.match(provenance.userAgent, /Chrome\//u);
  assert.notEqual(accepted.durationMs, provenance.audio.requestedDurationMs, 'a wall-clock/client duration is not used');
});

test('encoder-valid Ogg Opus uses checksummed granules/preSkip; FFmpeg WebM also parses', () => {
  assert.equal(validate(attachment('ffmpeg-opus.ogg', 'audio/ogg;codecs=opus')).durationMs, 350);
  const other = validate(attachment('ffmpeg-opus-2s.webm', 'audio/webm'));
  assert(other.durationMs >= 2000 && other.durationMs <= 2008, 'ceil-rounded conservative WebM timeline/packet bound');
});

test('audio duration limits use actual packets rather than MIME, client fields or small byte count', () => {
  assert.throws(() => validate(attachment('ffmpeg-opus-2s.webm', 'audio/webm'), { maxAudioSeconds: 1 }), code('feedback_audio_duration_limit'));
  assert(file('ffmpeg-opus-121s.ogg').length < FEEDBACK_MEDIA_LIMITS.totalAttachmentBytes);
  assert.throws(() => validate(attachment('ffmpeg-opus-121s.ogg', 'audio/ogg')), code('feedback_audio_duration_limit'));
  assert.throws(() => validate({ ...attachment('chrome-recorder.webm', 'audio/webm'), durationMs: 1 }), code('feedback_attachment_invalid'));
  assert.throws(() => validate(attachment('chrome-recorder.webm', 'audio/webm'), { maxAudioSeconds: 121 }), code('feedback_media_limits_invalid'));
});

test('WebM timestamps cannot hide long presentation gaps, malformed frames, lacing or missing audio', () => {
  const bytes = file('chrome-recorder.webm'), stretched = Buffer.from(bytes);
  const scale = stretched.indexOf(Buffer.from([0x2a, 0xd7, 0xb1, 0x83])); assert(scale > 0);
  stretched.writeUIntBE(0xffffff, scale + 4, 3);
  assert.throws(() => validate(attachment('stretched.webm', 'audio/webm', 'audio', stretched), { maxAudioSeconds: 1 }), code('feedback_audio_duration_limit'));
  const cluster = bytes.indexOf(Buffer.from([0x1f, 0x43, 0xb6, 0x75])); assert(cluster > 0);
  assert.throws(() => validate(attachment('header.webm', 'audio/webm', 'audio', bytes.subarray(0, cluster))), code('feedback_audio_duration_unknown'));
  // Chrome's first non-laced SimpleBlock is track1, timestamp0, flags0x80.
  const block = bytes.indexOf(Buffer.from([0x81, 0, 0, 0x80]), cluster); assert(block > 0);
  const laced = Buffer.from(bytes); laced[block + 3] |= 2;
  assert.throws(() => validate(attachment('laced.webm', 'audio/webm', 'audio', laced)), code('feedback_media_unsupported'));
  const frame = Buffer.from(bytes); assert.equal(frame[block + 4] & 3, 3); frame[block + 5] &= 0xc0;
  assert.throws(() => validate(attachment('frame.webm', 'audio/webm', 'audio', frame)), code('feedback_media_invalid'));
  const codec = Buffer.from(bytes), id = codec.indexOf(Buffer.from('A_OPUS')); codec.write('A_FLAC', id);
  assert.throws(() => validate(attachment('codec.webm', 'audio/webm', 'audio', codec)), code('feedback_media_unsupported'));
});

test('Ogg corruption, incomplete recordings and spoofed timing are rejected', () => {
  const bytes = file('ffmpeg-opus.ogg'), corrupt = Buffer.from(bytes); corrupt[corrupt.length - 1] ^= 1;
  assert.throws(() => validate(attachment('crc.ogg', 'audio/ogg', 'audio', corrupt)), code('feedback_media_invalid'));
  const pages = oggPages(bytes), last = pages.at(-1), incomplete = Buffer.from(bytes);
  incomplete[last.offset + 5] &= ~4;
  incomplete.writeUInt32LE(crc(incomplete.subarray(last.offset, last.end), false), last.offset + 22);
  // A non-EOS page with a trimmed granule is inconsistent, not an accepted short clip.
  assert.throws(() => validate(attachment('incomplete.ogg', 'audio/ogg', 'audio', incomplete)), error => ['feedback_media_invalid', 'feedback_audio_duration_unknown'].includes(error.code));
  const forged = Buffer.from(bytes); forged.writeBigUInt64LE(99999999n, last.offset + 6);
  forged.writeUInt32LE(crc(forged.subarray(last.offset, last.end), false), last.offset + 22);
  assert.throws(() => validate(attachment('granule.ogg', 'audio/ogg', 'audio', forged)), code('feedback_media_invalid'));
  assert.throws(() => validate(attachment('truncated.ogg', 'audio/ogg', 'audio', bytes.subarray(0, bytes.length - 1))), code('feedback_media_invalid'));
});

test('image dimensions, CRC, truncation, animation and bounded PNG inflation reject unsafe inputs', () => {
  const source = file('chrome.png'), oversized = Buffer.from(source); oversized.writeUInt32BE(8000, 16); oversized.writeUInt32BE(8000, 20);
  oversized.writeUInt32BE(crc(oversized.subarray(12, 29), true), 29);
  assert.throws(() => validate(attachment('large.png', 'image/png', 'image', oversized)), code('feedback_image_dimensions_limit'));
  const badCrc = Buffer.from(source); badCrc[20] ^= 1;
  assert.throws(() => validate(attachment('crc.png', 'image/png', 'image', badCrc)), code('feedback_media_invalid'));
  const expanded = Buffer.concat([source.subarray(0, 33), pngChunk('IDAT', deflateSync(Buffer.alloc(1000000))), pngChunk('IEND', Buffer.alloc(0))]);
  assert.throws(() => validate(attachment('inflate.png', 'image/png', 'image', expanded)), code('feedback_media_invalid'));
  let idat = 33; while (source.toString('ascii', idat + 4, idat + 8) !== 'IDAT') idat += source.readUInt32BE(idat) + 12;
  const packed = source.subarray(idat + 8, idat + 8 + source.readUInt32BE(idat));
  const trailingStream = Buffer.concat([source.subarray(0, 33), pngChunk('IDAT', Buffer.concat([packed, Buffer.from('unused')])), pngChunk('IEND', Buffer.alloc(0))]);
  assert.throws(() => validate(attachment('extra.png', 'image/png', 'image', trailingStream)), code('feedback_media_invalid'));
  const animated = Buffer.concat([source.subarray(0, 33), pngChunk('acTL', Buffer.alloc(8)), source.subarray(33)]);
  assert.throws(() => validate(attachment('animated.png', 'image/png', 'image', animated)), code('feedback_media_unsupported'));
  for (const [name, mime] of [['chrome.png', 'image/png'], ['chrome.jpg', 'image/jpeg'], ['chrome.webp', 'image/webp']]) {
    assert.throws(() => validate(attachment(name, mime, 'image', file(name).subarray(0, file(name).length - 1))), code('feedback_media_invalid'));
  }
});

test('closed DTO, matching magic, canonical base64, byte/count caps and safe names fail without reflecting data', () => {
  const image = attachment('chrome.png', 'image/png');
  assert.throws(() => validate({ ...image, accountId: 'SENTINEL' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...image, name: '../SENTINEL.png' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...image, dataBase64: image.dataBase64 + '\n' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...image, dataBase64: 'AAAA===' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...image, mimeType: 'image/svg+xml' }), code('feedback_media_unsupported'));
  assert.throws(() => validate({ ...image, mimeType: 'constructor' }), code('feedback_media_unsupported'));
  assert.throws(() => validate({ ...image, mimeType: 'audio/webm', kind: 'audio' }), code('feedback_media_invalid'));
  assert.throws(() => validateFeedbackAttachments([image, image, image, image]), code('feedback_attachment_limit'));
  assert.throws(() => validate({ ...image, dataBase64: Buffer.alloc(1048577).toString('base64') }), code('feedback_attachment_limit'));
  const jpeg = attachment('chrome.jpg', 'image/jpeg'), total = file('chrome.png').length + file('chrome.jpg').length;
  assert.equal(validateFeedbackAttachments([image, jpeg], { limits: { totalAttachmentBytes: total } }).length, 2);
  assert.throws(() => validateFeedbackAttachments([image, jpeg], { limits: { totalAttachmentBytes: total - 1 } }), code('feedback_attachment_limit'));
  assert.deepEqual(validateFeedbackAttachments([]), []);
});

test('accessor/sparse input is rejected before getters run; limits cannot be caller-raised', () => {
  let calls = 0; const input = attachment('chrome.png', 'image/png');
  Object.defineProperty(input, 'dataBase64', { enumerable: true, get() { calls++; return 'SENTINEL'; } });
  assert.throws(() => validate(input), code('feedback_attachment_invalid')); assert.equal(calls, 0);
  assert.throws(() => validateFeedbackAttachments(Array(1)), code('feedback_attachment_invalid'));
  assert.throws(() => validateFeedbackAttachments([], { limits: { totalAttachmentBytes: 1048577 } }), code('feedback_media_limits_invalid'));
});
