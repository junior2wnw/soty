import { inflateSync } from 'node:zlib';

// Narrow, bounded upload profile. This parses containers/packet framing; it is
// not an image/Opus decoder, antivirus scanner, ASR or permission check.
export const FEEDBACK_MEDIA_LIMITS = Object.freeze({ maxAttachments: 3, totalAttachmentBytes: 1048576, maxAudioSeconds: 120 });
const IMAGE_DIMENSION = 8192, IMAGE_PIXELS = 16777216, PNG_DECODED_BYTES = 16777216;
export class FeedbackMediaError extends Error {
  constructor(code) { super(code); this.name = 'FeedbackMediaError'; this.code = code; this.status = 400; }
}
const requireMedia = (ok, code = 'feedback_media_invalid') => { if (!ok) throw new FeedbackMediaError(code); };

function closed(input, required, optional = [], code = 'feedback_attachment_invalid') {
  requireMedia(input && typeof input === 'object' && !Array.isArray(input)
    && [Object.prototype, null].includes(Object.getPrototypeOf(input)), code);
  const fields = Object.getOwnPropertyDescriptors(input);
  requireMedia(Object.getOwnPropertySymbols(input).length === 0
    && required.every(key => Object.hasOwn(fields, key))
    && Object.keys(fields).every(key => [...required, ...optional].includes(key)
      && fields[key].enumerable && Object.hasOwn(fields[key], 'value')), code);
  return Object.fromEntries(Object.entries(fields).map(([key, field]) => [key, field.value]));
}
function limits(input) {
  const value = closed(input ?? {}, [], Object.keys(FEEDBACK_MEDIA_LIMITS), 'feedback_media_limits_invalid');
  const result = { ...FEEDBACK_MEDIA_LIMITS, ...value };
  for (const key of Object.keys(result)) requireMedia(Number.isSafeInteger(result[key]) && result[key] >= 1
    && result[key] <= FEEDBACK_MEDIA_LIMITS[key], 'feedback_media_limits_invalid');
  return result;
}
function dimensions(width, height) {
  requireMedia(Number.isSafeInteger(width) && Number.isSafeInteger(height) && width > 0 && height > 0, 'feedback_media_invalid');
  requireMedia(width <= IMAGE_DIMENSION && height <= IMAGE_DIMENSION && width * height <= IMAGE_PIXELS, 'feedback_image_dimensions_limit');
  return { width, height };
}
function crcTable(polynomial, reflected) {
  return Uint32Array.from({ length: 256 }, (_, index) => {
    let value = reflected ? index : index << 24;
    for (let bit = 0; bit < 8; bit++) value = reflected
      ? ((value >>> 1) ^ (value & 1 ? polynomial : 0)) >>> 0
      : ((value << 1) ^ (value & 0x80000000 ? polynomial : 0)) >>> 0;
    return value;
  });
}
const PNG_CRC = crcTable(0xedb88320, true), OGG_CRC = crcTable(0x04c11db7, false);
function pngCrc(bytes) {
  let value = 0xffffffff;
  for (const byte of bytes) value = (value >>> 8) ^ PNG_CRC[(value ^ byte) & 255];
  return (value ^ 0xffffffff) >>> 0;
}
function oggCrc(bytes) {
  let value = 0;
  for (let index = 0; index < bytes.length; index++) {
    const byte = index >= 22 && index < 26 ? 0 : bytes[index];
    value = ((value << 8) ^ OGG_CRC[((value >>> 24) ^ byte) & 255]) >>> 0;
  }
  return value;
}

function png(bytes) {
  requireMedia(bytes.length >= 45 && bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])));
  let offset = 8, header, palette = false, ended = false, dataEnded = false;
  const compressed = []; let chunks = 0;
  while (offset < bytes.length) {
    requireMedia(++chunks <= 4096 && offset + 12 <= bytes.length);
    const length = bytes.readUInt32BE(offset), start = offset + 8, end = start + length;
    requireMedia(end + 4 <= bytes.length);
    const type = bytes.toString('ascii', offset + 4, start), body = bytes.subarray(start, end);
    requireMedia(/^[A-Za-z]{4}$/u.test(type) && pngCrc(bytes.subarray(offset + 4, end)) === bytes.readUInt32BE(end));
    requireMedia(!['acTL', 'fcTL', 'fdAT'].includes(type), 'feedback_media_unsupported');
    if (!header) {
      requireMedia(type === 'IHDR' && length === 13);
      const size = dimensions(body.readUInt32BE(0), body.readUInt32BE(4)), depth = body[8], color = body[9];
      const depths = { 0: [1, 2, 4, 8, 16], 2: [8, 16], 3: [1, 2, 4, 8], 4: [8, 16], 6: [8, 16] };
      requireMedia(depths[color]?.includes(depth) && body[10] === 0 && body[11] === 0 && body[12] <= 1);
      header = { ...size, depth, color, interlace: body[12] };
    } else if (type === 'IHDR') requireMedia(false);
    else if (type === 'PLTE') {
      requireMedia(!palette && compressed.length === 0 && length > 0 && length % 3 === 0 && length <= 768
        && ![0, 4].includes(header.color) && (header.color !== 3 || length / 3 <= 2 ** header.depth)); palette = true;
    } else if (type === 'IDAT') {
      requireMedia(!dataEnded && (header.color !== 3 || palette)); compressed.push(body);
    } else {
      if (compressed.length) dataEnded = true;
      if (type === 'IEND') { requireMedia(length === 0 && compressed.length > 0 && end + 4 === bytes.length); ended = true; }
      else requireMedia(type[0] === type[0].toLowerCase(), 'feedback_media_unsupported');
    }
    offset = end + 4;
  }
  requireMedia(ended);
  const channels = { 0: 1, 2: 3, 3: 1, 4: 2, 6: 4 }[header.color];
  const passes = header.interlace ? [[0, 0, 8, 8], [4, 0, 8, 8], [0, 4, 4, 8], [2, 0, 4, 4], [0, 2, 2, 4], [1, 0, 2, 2], [0, 1, 1, 2]] : [[0, 0, 1, 1]];
  const rows = []; let expected = 0;
  for (const [x, y, dx, dy] of passes) {
    const width = Math.max(0, Math.ceil((header.width - x) / dx)), height = Math.max(0, Math.ceil((header.height - y) / dy));
    if (!width || !height) continue;
    const stride = 1 + Math.ceil(width * channels * header.depth / 8); rows.push({ height, stride }); expected += height * stride;
  }
  requireMedia(expected > 0 && expected <= PNG_DECODED_BYTES, 'feedback_image_dimensions_limit');
  let inflated; const packed = Buffer.concat(compressed);
  try { inflated = inflateSync(packed, { maxOutputLength: expected, info: true }); } catch { requireMedia(false); }
  requireMedia(inflated.buffer.length === expected && inflated.engine.bytesWritten === packed.length);
  let rowOffset = 0;
  for (const row of rows) for (let y = 0; y < row.height; y++) { requireMedia(inflated.buffer[rowOffset] <= 4); rowOffset += row.stride; }
  return dimensions(header.width, header.height);
}

function jpeg(bytes) {
  requireMedia(bytes.length >= 4 && bytes[0] === 255 && bytes[1] === 216);
  let offset = 2, size, scans = 0, markers = 0;
  while (offset < bytes.length) {
    requireMedia(++markers <= 4096 && bytes[offset++] === 255);
    while (offset < bytes.length && bytes[offset] === 255) offset++;
    requireMedia(offset < bytes.length); const marker = bytes[offset++];
    if (marker === 217) { requireMedia(size && scans > 0 && offset === bytes.length); return size; }
    requireMedia(marker !== 0 && marker !== 216 && !(marker >= 208 && marker <= 215));
    requireMedia(![200, 220, 222, 223].includes(marker), 'feedback_media_unsupported');
    if (marker === 1) continue;
    requireMedia(offset + 2 <= bytes.length); const length = bytes.readUInt16BE(offset), end = offset + length;
    requireMedia(length >= 2 && end <= bytes.length);
    if ([192, 194].includes(marker)) {
      requireMedia(!size && length >= 8 && bytes[offset + 2] === 8);
      const count = bytes[offset + 7]; requireMedia([1, 3, 4].includes(count) && length === 8 + count * 3);
      size = dimensions(bytes.readUInt16BE(offset + 5), bytes.readUInt16BE(offset + 3));
    } else if (marker >= 192 && marker <= 207 && ![196, 200, 204].includes(marker)) requireMedia(false, 'feedback_media_unsupported');
    if (marker === 218) {
      requireMedia(size && length >= 6 && bytes[offset + 2] >= 1 && bytes[offset + 2] <= 4 && length === 6 + bytes[offset + 2] * 2); scans++;
      offset = end;
      while (offset < bytes.length) {
        if (bytes[offset] !== 255) { offset++; continue; }
        const start = offset++; while (offset < bytes.length && bytes[offset] === 255) offset++;
        requireMedia(offset < bytes.length);
        if (bytes[offset] === 0 || (bytes[offset] >= 208 && bytes[offset] <= 215)) offset++;
        else { offset = start; break; }
      }
    } else offset = end;
  }
  requireMedia(false);
}

function webp(bytes) {
  requireMedia(bytes.length >= 20 && bytes.toString('ascii', 0, 4) === 'RIFF' && bytes.toString('ascii', 8, 12) === 'WEBP'
    && bytes.readUInt32LE(4) + 8 === bytes.length);
  let offset = 12, canvas, image, chunks = 0, alpha = false, extended = false;
  while (offset < bytes.length) {
    requireMedia(++chunks <= 4096 && offset + 8 <= bytes.length);
    const type = bytes.toString('ascii', offset, offset + 4), length = bytes.readUInt32LE(offset + 4), start = offset + 8, end = start + length;
    requireMedia(end + (length & 1) <= bytes.length && (!(length & 1) || bytes[end] === 0));
    const body = bytes.subarray(start, end);
    requireMedia(!['ANIM', 'ANMF'].includes(type), 'feedback_media_unsupported');
    if (type === 'VP8X') {
      requireMedia(offset === 12 && length === 10 && !(body[0] & 0xc1) && body[1] === 0 && body[2] === 0 && body[3] === 0);
      requireMedia(!(body[0] & 2), 'feedback_media_unsupported');
      extended = true; canvas = dimensions(1 + body.readUIntLE(4, 3), 1 + body.readUIntLE(7, 3));
    } else if (type === 'VP8 ') {
      requireMedia(!image && length >= 10 && !(body[0] & 1) && body.subarray(3, 6).equals(Buffer.from([157, 1, 42])));
      requireMedia(!(body.readUInt16LE(6) & 0xc000) && !(body.readUInt16LE(8) & 0xc000), 'feedback_media_unsupported');
      image = dimensions(body.readUInt16LE(6) & 16383, body.readUInt16LE(8) & 16383);
    } else if (type === 'VP8L') {
      requireMedia(!image && !alpha && length >= 5 && body[0] === 47);
      const bits = body.readUInt32LE(1); requireMedia((bits >>> 29) === 0);
      image = dimensions(1 + (bits & 16383), 1 + ((bits >>> 14) & 16383));
    } else if (type === 'ALPH') { requireMedia(extended && !alpha && !image && length > 0); alpha = true; }
    else requireMedia(extended && ['ICCP', 'EXIF', 'XMP '].includes(type), 'feedback_media_unsupported');
    offset = end + (length & 1);
  }
  requireMedia(image && (!canvas || (image.width === canvas.width && image.height === canvas.height)));
  return image;
}

function opusHead(bytes) {
  requireMedia(bytes.length === 19 && bytes.toString('ascii', 0, 8) === 'OpusHead'
    && bytes[8] <= 1 && [1, 2].includes(bytes[9]) && bytes[18] === 0, 'feedback_media_unsupported');
  return { channels: bytes[9], preSkip: bytes.readUInt16LE(10) };
}
// RFC6716 packet framing, not entropy decoding. All frame byte lengths and the
// <=120ms packet bound are checked before a packet contributes to duration.
function opusSamples(bytes) {
  requireMedia(bytes.length > 0); const config = bytes[0] >>> 3, code = bytes[0] & 3;
  const frame = (config < 12 ? [10, 20, 40, 60][config & 3] : config < 16 ? [10, 20][config & 1] : [2.5, 5, 10, 20][config & 3]) * 48;
  let offset = 1, count = code === 0 ? 1 : 2, end = bytes.length;
  const frameSize = () => {
    requireMedia(offset < end); const first = bytes[offset++];
    if (first < 252) return first;
    requireMedia(offset < end); return first + bytes[offset++] * 4;
  };
  const validSize = value => requireMedia(value >= 0 && value <= 1275);
  if (code === 0) validSize(end - offset);
  else if (code === 1) { requireMedia((end - offset) % 2 === 0); validSize((end - offset) / 2); }
  else if (code === 2) { const first = frameSize(); validSize(first); validSize(end - offset - first); }
  else {
    requireMedia(offset < end); const control = bytes[offset++]; count = control & 63;
    requireMedia(count > 0 && count * frame <= 5760);
    if (control & 64) {
      let padding = 0, value;
      do { requireMedia(offset < end); value = bytes[offset++]; padding += value === 255 ? 254 : value; } while (value === 255);
      end -= padding; requireMedia(end >= offset);
    }
    if (control & 128) {
      let used = 0;
      for (let index = 0; index < count - 1; index++) { const length = frameSize(); validSize(length); used += length; }
      validSize(end - offset - used);
    } else { requireMedia((end - offset) % count === 0); validSize((end - offset) / count); }
  }
  requireMedia(count * frame <= 5760); return count * frame;
}

function ogg(bytes, maxDurationMs) {
  let offset = 0, serial, sequence = 0, head, packets = 0, samples = 0, fragments = [], fragmentBytes = 0, finalGranule, previousGranule = 0n, lastSamples = 0;
  let ended = false, pages = 0;
  while (offset < bytes.length) {
    requireMedia(!ended && ++pages <= 8192 && offset + 27 <= bytes.length && bytes.toString('ascii', offset, offset + 4) === 'OggS' && bytes[offset + 4] === 0);
    const flags = bytes[offset + 5], granule = bytes.readBigUInt64LE(offset + 6), currentSerial = bytes.readUInt32LE(offset + 14);
    const count = bytes[offset + 26], bodyStart = offset + 27 + count;
    requireMedia((flags & ~7) === 0 && bodyStart <= bytes.length && bytes.readUInt32LE(offset + 18) === sequence++);
    if (serial === undefined) { serial = currentSerial; requireMedia((flags & 2) !== 0); }
    else requireMedia(currentSerial === serial && !(flags & 2), 'feedback_media_unsupported');
    requireMedia(Boolean(flags & 1) === (fragmentBytes > 0));
    const laces = bytes.subarray(offset + 27, bodyStart), bodyBytes = laces.reduce((sum, length) => sum + length, 0), end = bodyStart + bodyBytes;
    requireMedia(end <= bytes.length && oggCrc(bytes.subarray(offset, end)) === bytes.readUInt32LE(offset + 22));
    let cursor = bodyStart, completed = 0;
    for (const length of laces) {
      fragments.push(bytes.subarray(cursor, cursor + length)); fragmentBytes += length; cursor += length;
      if (length === 255) continue;
      const packet = Buffer.concat(fragments, fragmentBytes); fragments = []; fragmentBytes = 0; completed++; packets++;
      if (packets === 1) { head = opusHead(packet); requireMedia(flags & 2); }
      else if (packets === 2) {
        requireMedia(packet.length >= 16 && packet.toString('ascii', 0, 8) === 'OpusTags');
        let p = 12 + packet.readUInt32LE(8); requireMedia(p + 4 <= packet.length); const comments = packet.readUInt32LE(p); p += 4;
        requireMedia(comments <= 128, 'feedback_media_unsupported');
        for (let index = 0; index < comments; index++) { requireMedia(p + 4 <= packet.length); p += 4 + packet.readUInt32LE(p); requireMedia(p <= packet.length); }
      } else {
        lastSamples = opusSamples(packet); samples += lastSamples;
        requireMedia(samples <= maxDurationMs * 48 + head.preSkip + 5760, 'feedback_audio_duration_limit');
      }
    }
    if (packets <= 2) requireMedia(granule === 0n);
    else if (granule !== 0xffffffffffffffffn) {
      requireMedia(completed > 0 && granule >= previousGranule && granule <= BigInt(samples)); previousGranule = granule;
      if (!(flags & 4)) requireMedia(granule === BigInt(samples));
    }
    if (flags & 4) {
      requireMedia(fragmentBytes === 0 && granule !== 0xffffffffffffffffn && packets > 2 && granule >= BigInt(head.preSkip)
        && BigInt(samples) - granule <= BigInt(lastSamples)); ended = true; finalGranule = granule;
    }
    offset = end;
  }
  requireMedia(ended && samples > 0 && fragmentBytes === 0, 'feedback_audio_duration_unknown');
  const durationMs = Math.ceil(Number(finalGranule - BigInt(head.preSkip)) / 48);
  requireMedia(durationMs > 0, 'feedback_audio_duration_unknown'); requireMedia(durationMs <= maxDurationMs, 'feedback_audio_duration_limit');
  return { durationMs };
}

function webm(bytes, maxDurationMs) {
  let elements = 0;
  const vint = (offset, isSize, boundary) => {
    requireMedia(offset < boundary && bytes[offset] !== 0); let width = 1, mask = 128;
    while (!(bytes[offset] & mask)) { width++; mask >>>= 1; }
    requireMedia(width <= (isSize ? 8 : 4) && offset + width <= boundary);
    let value = BigInt(isSize ? bytes[offset] & (mask - 1) : bytes[offset]);
    for (let index = 1; index < width; index++) value = value * 256n + BigInt(bytes[offset + index]);
    return { width, value, unknown: isSize && value === (1n << BigInt(width * 7)) - 1n };
  };
  const element = (offset, boundary) => {
    requireMedia(++elements <= 16384); const id = vint(offset, false, boundary), size = vint(offset + id.width, true, boundary), start = offset + id.width + size.width;
    requireMedia(size.unknown || size.value <= BigInt(boundary - start));
    return { id: Number(id.value), start, end: size.unknown ? boundary : start + Number(size.value), unknown: size.unknown };
  };
  const children = item => {
    requireMedia(!item.unknown); const result = [];
    for (let p = item.start; p < item.end;) { const child = element(p, item.end); requireMedia(!child.unknown); result.push(child); p = child.end; }
    return result;
  };
  const uint = item => { requireMedia(item.end > item.start && item.end - item.start <= 8); let value = 0n;
    for (const byte of bytes.subarray(item.start, item.end)) value = value * 256n + BigInt(byte); requireMedia(value <= BigInt(Number.MAX_SAFE_INTEGER)); return Number(value); };
  const signed = item => {
    requireMedia(item.end > item.start && item.end - item.start <= 8); let value = 0n;
    for (const byte of bytes.subarray(item.start, item.end)) value = value * 256n + BigInt(byte);
    if (bytes[item.start] & 128) value -= 1n << BigInt((item.end - item.start) * 8); return value;
  };
  const unique = (items, id, required = false) => { const found = items.filter(item => item.id === id); requireMedia(found.length <= 1 && (!required || found.length === 1)); return found[0]; };
  const header = element(0, bytes.length); requireMedia(header.id === 0x1a45dfa3 && !header.unknown);
  const headerItems = children(header), docType = unique(headerItems, 0x4282, true);
  requireMedia(bytes.toString('ascii', docType.start, docType.end) === 'webm', 'feedback_media_unsupported');
  for (const [id, maximum] of [[0x42f7, 1], [0x4285, 4], [0x42f2, 4], [0x42f3, 8]]) {
    const field = unique(headerItems, id); if (field) requireMedia(uint(field) > 0 && uint(field) <= maximum, 'feedback_media_unsupported');
  }
  const segment = element(header.end, bytes.length); requireMedia(segment.id === 0x18538067 && segment.end === bytes.length);
  let scale = 1000000, infoSeen = false, head, track, codecDelay = 0n, audioSeen = false;
  let samples = 0, packets = 0, maxEndNs = 0n, previousStartNs = -1n, finalPadding = 0n;
  const samplesNs = value => BigInt(value) * 1000000000n / 48000n;
  const maxNs = BigInt(maxDurationMs) * 1000000n;
  const trackEntry = item => {
    const entries = children(item); requireMedia(entries.length === 1 && entries[0].id === 0xae, 'feedback_media_unsupported');
    const fields = children(entries[0]);
    requireMedia(!fields.some(field => [0x6d80, 0x23314f, 0x537f].includes(field.id)), 'feedback_media_unsupported');
    track = uint(unique(fields, 0xd7, true)); requireMedia(track > 0 && uint(unique(fields, 0x83, true)) === 2, 'feedback_media_unsupported');
    const codec = unique(fields, 0x86, true); requireMedia(bytes.toString('ascii', codec.start, codec.end) === 'A_OPUS', 'feedback_media_unsupported');
    const privateData = unique(fields, 0x63a2, true); head = opusHead(bytes.subarray(privateData.start, privateData.end));
    const delay = unique(fields, 0x56aa); codecDelay = delay ? BigInt(uint(delay)) : samplesNs(head.preSkip);
    requireMedia(codecDelay === samplesNs(head.preSkip), 'feedback_media_unsupported');
    const audio = unique(fields, 0xe1, true), audioFields = children(audio), channels = unique(audioFields, 0x9f), frequency = unique(audioFields, 0xb5);
    requireMedia((channels ? uint(channels) : 1) === head.channels);
    if (frequency) { const length = frequency.end - frequency.start; requireMedia([4, 8].includes(length));
      const hz = length === 4 ? bytes.readFloatBE(frequency.start) : bytes.readDoubleBE(frequency.start); requireMedia(hz === 48000, 'feedback_media_unsupported'); }
  };
  const block = (item, timestamp, padding = 0n, declaredDuration) => {
    requireMedia(head, 'feedback_audio_duration_unknown'); const number = vint(item.start, true, item.end), p = item.start + number.width;
    requireMedia(!number.unknown && number.value === BigInt(track) && p + 3 < item.end);
    const relative = bytes.readInt16BE(p), flags = bytes[p + 2];
    requireMedia(!(flags & 0x70)); requireMedia(!(flags & 6), 'feedback_media_unsupported'); // No lacing in this first profile.
    const decoded = opusSamples(bytes.subarray(p + 3, item.end)), durationNs = samplesNs(decoded), startNs = (BigInt(timestamp) + BigInt(relative)) * BigInt(scale);
    requireMedia(startNs >= -codecDelay && startNs >= previousStartNs && padding >= 0n && padding <= durationNs);
    if (declaredDuration !== undefined) requireMedia(BigInt(declaredDuration) * BigInt(scale) <= durationNs);
    previousStartNs = startNs; samples += decoded; packets++; finalPadding = padding;
    const endNs = startNs + durationNs - padding; if (endNs > maxEndNs) maxEndNs = endNs;
    requireMedia(samplesNs(samples) <= maxNs + codecDelay + 120000000n && endNs - codecDelay <= maxNs, 'feedback_audio_duration_limit');
  };
  const segmentIds = new Set([0x1f43b675, 0x1549a966, 0x1654ae6b, 0x114d9b74, 0x1c53bb6b, 0x1254c367]);
  const cluster = item => {
    let p = item.start, timestamp;
    while (p < item.end) {
      const child = element(p, item.end);
      if (item.unknown && segmentIds.has(child.id)) return p;
      requireMedia(!child.unknown);
      if (child.id === 0xe7) { requireMedia(timestamp === undefined); timestamp = uint(child); }
      else if (child.id === 0xa3) { requireMedia(timestamp !== undefined, 'feedback_audio_duration_unknown'); block(child, timestamp); }
      else if (child.id === 0xa0) {
        requireMedia(timestamp !== undefined, 'feedback_audio_duration_unknown'); const fields = children(child), data = unique(fields, 0xa1, true), discard = unique(fields, 0x75a2), duration = unique(fields, 0x9b);
        requireMedia(fields.every(field => [0xa1, 0x75a2, 0x9b, 0xbf, 0xec].includes(field.id)), 'feedback_media_unsupported');
        block(data, timestamp, discard ? signed(discard) : 0n, duration ? uint(duration) : undefined);
      } else requireMedia([0xa7, 0xab, 0xbf, 0xec].includes(child.id), 'feedback_media_unsupported');
      p = child.end;
    }
    return p;
  };
  for (let p = segment.start; p < segment.end;) {
    const item = element(p, segment.end);
    if (item.id === 0x1549a966) {
      requireMedia(!infoSeen && !audioSeen && !head); infoSeen = true; const fields = children(item), scaleField = unique(fields, 0x2ad7b1);
      if (scaleField) scale = uint(scaleField); requireMedia(scale > 0 && scale <= 1000000000);
    } else if (item.id === 0x1654ae6b) { requireMedia(!head && !audioSeen); trackEntry(item); }
    else if (item.id === 0x1f43b675) { audioSeen = true; p = cluster(item); continue; }
    else requireMedia(!item.unknown && [0x114d9b74, 0x1c53bb6b, 0x1254c367, 0xbf, 0xec].includes(item.id), 'feedback_media_unsupported');
    p = item.end;
  }
  requireMedia(head && packets > 0, 'feedback_audio_duration_unknown');
  const decodedNs = samplesNs(samples) - codecDelay - finalPadding, presentationNs = maxEndNs - codecDelay;
  const durationNs = decodedNs > presentationNs ? decodedNs : presentationNs;
  requireMedia(durationNs > 0n, 'feedback_audio_duration_unknown');
  const durationMs = Number((durationNs + 999999n) / 1000000n);
  requireMedia(durationMs <= maxDurationMs, 'feedback_audio_duration_limit'); return { durationMs };
}

const TYPES = Object.freeze({ 'image/png': ['image', png], 'image/jpeg': ['image', jpeg], 'image/webp': ['image', webp],
  'audio/webm': ['audio', webm], 'audio/webm;codecs=opus': ['audio', webm], 'audio/ogg': ['audio', ogg], 'audio/ogg;codecs=opus': ['audio', ogg] });

export function validateFeedbackAttachments(input, { limits: configured = {} } = {}) {
  const policy = limits(configured);
  requireMedia(Array.isArray(input) && input.length <= policy.maxAttachments, 'feedback_attachment_limit');
  const properties = Object.getOwnPropertyDescriptors(input);
  requireMedia(Object.getOwnPropertySymbols(input).length === 0 && Object.keys(properties).length === input.length + 1
    && Array.from({ length: input.length }, (_, index) => properties[index] && Object.hasOwn(properties[index], 'value')).every(Boolean), 'feedback_attachment_invalid');
  let total = 0; const result = [];
  for (let index = 0; index < input.length; index++) {
    const attachment = closed(properties[index].value, ['kind', 'name', 'mimeType', 'dataBase64']);
    requireMedia(typeof attachment.name === 'string' && attachment.name.trim().length > 0 && attachment.name.length <= 160
      && !/[\u0000-\u001f\u007f/\\]/u.test(attachment.name)
      && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(attachment.name), 'feedback_attachment_invalid');
    requireMedia(typeof attachment.mimeType === 'string', 'feedback_attachment_invalid'); const mimeType = attachment.mimeType.toLowerCase(), type = Object.hasOwn(TYPES, mimeType) ? TYPES[mimeType] : null;
    requireMedia(type && attachment.kind === type[0], 'feedback_media_unsupported');
    const encoded = attachment.dataBase64;
    requireMedia(typeof encoded === 'string' && encoded.length > 0 && encoded.length <= 4 * Math.ceil(policy.totalAttachmentBytes / 3), 'feedback_attachment_limit');
    requireMedia(encoded.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/u.test(encoded), 'feedback_attachment_invalid');
    const bytes = Buffer.from(encoded, 'base64'); requireMedia(bytes.toString('base64') === encoded, 'feedback_attachment_invalid');
    total += bytes.length; requireMedia(bytes.length > 0 && total <= policy.totalAttachmentBytes, 'feedback_attachment_limit');
    let metadata;
    try { metadata = type[1](bytes, policy.maxAudioSeconds * 1000); }
    catch (error) { if (error instanceof FeedbackMediaError) throw error; throw new FeedbackMediaError('feedback_media_invalid'); }
    result.push(Object.freeze({ kind: attachment.kind, name: attachment.name, mimeType, byteLength: bytes.length, bytes, ...metadata }));
  }
  return Object.freeze(result);
}
