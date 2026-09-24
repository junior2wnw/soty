// Plaintext is parsed in bounded memory, never written to a file or stdout.
// No success receipt is emitted before GCM authentication and full tar checks.
import { open } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createDecipheriv, privateDecrypt, createHash, createPrivateKey, createPublicKey } from 'node:crypto';

const MAX_INPUT = 64 * 1024, MAX_HEADER = 16 * 1024, MAX_METADATA = 4 * 1024 * 1024;
const SQLITE_MAGIC = Buffer.from('SQLite format 3\0');
const invalid = () => { throw new Error('backup_verification_failed'); };
const text = bytes => bytes.toString('utf8').replace(/\0.*$/s, '');
function number(bytes) {
  if (bytes[0] & 0x80) {
    if (bytes[0] & 0x40) invalid();
    let result = BigInt(bytes[0] & 0x7f);
    for (const byte of bytes.subarray(1)) result = (result << 8n) | BigInt(byte);
    if (result > BigInt(Number.MAX_SAFE_INTEGER)) invalid();
    return Number(result);
  }
  const value = text(bytes).trim();
  if (value && !/^[0-7]+$/.test(value)) invalid();
  const result = value ? parseInt(value, 8) : 0;
  if (!Number.isSafeInteger(result) || result < 0) invalid();
  return result;
}
function safeName(value) {
  if (typeof value !== 'string' || value.length > 4096 || value.includes('\\') || value.includes('\0')
      || value.startsWith('/') || /^[A-Za-z]:/.test(value)) invalid();
  const parts = value.split('/').filter(part => part !== '.' && part !== '');
  if (parts.some(part => part === '..')) invalid();
  return parts.join('/');
}
function pax(bytes) {
  const result = {};
  for (let at = 0; at < bytes.length;) {
    const space = bytes.indexOf(32, at);
    if (space < at || space - at > 12) invalid();
    const digits = bytes.subarray(at, space).toString();
    if (!/^[1-9]\d*$/.test(digits)) invalid();
    const length = Number(digits), end = at + length;
    if (!Number.isSafeInteger(length) || end > bytes.length || end <= space + 2 || bytes[end - 1] !== 10) invalid();
    const record = bytes.subarray(space + 1, end - 1).toString('utf8');
    const equals = record.indexOf('='); if (equals < 1) invalid();
    const key = record.slice(0, equals);
    if (['path', 'linkpath', 'size'].includes(key)) result[key] = record.slice(equals + 1);
    at = end;
  }
  return result;
}
class TarVerifier {
  header = Buffer.alloc(512); headerUsed = 0; remaining = 0; padding = 0;
  entry = null; zeroBlocks = 0; ended = false; count = 0; rooms = 0; sqlite = 0;
  connectorStore = false; nextPax = {}; globalPax = {}; longName = null; longLink = null;
  feed(chunk) {
    let at = 0;
    while (at < chunk.length) {
      if (this.remaining) {
        const size = Math.min(this.remaining, chunk.length - at), part = chunk.subarray(at, at + size);
        if (this.entry.extension) this.entry.parts.push(Buffer.from(part));
        if (this.entry.sqlite && this.entry.prefix.length < 100) {
          this.entry.prefix = Buffer.concat([this.entry.prefix, part.subarray(0, 100 - this.entry.prefix.length)]);
        }
        this.remaining -= size; at += size;
        if (!this.remaining) this.finishEntry();
      } else if (this.padding) {
        const size = Math.min(this.padding, chunk.length - at);
        if (!chunk.subarray(at, at + size).every(byte => byte === 0)) invalid();
        this.padding -= size; at += size;
      } else {
        const size = Math.min(512 - this.headerUsed, chunk.length - at);
        chunk.copy(this.header, this.headerUsed, at); this.headerUsed += size; at += size;
        if (this.headerUsed === 512) { this.headerUsed = 0; this.beginEntry(); }
      }
    }
  }
  beginEntry() {
    const h = this.header;
    if (h.every(byte => byte === 0)) { this.zeroBlocks++; if (this.zeroBlocks >= 2) this.ended = true; return; }
    if (this.zeroBlocks || this.ended) invalid();
    const checksum = number(h.subarray(148, 156));
    let actual = 0; for (let i = 0; i < 512; i++) actual += i >= 148 && i < 156 ? 32 : h[i];
    if (checksum !== actual) invalid();
    const type = h[156] ? String.fromCharCode(h[156]) : '0';
    const extension = ['x', 'g', 'L', 'K'].includes(type);
    if (!extension && !['0', '1', '2', '5', '7'].includes(type)) invalid();
    let name = text(h.subarray(0, 100));
    if (h.subarray(257, 263).equals(Buffer.from('ustar\0'))) {
      const prefix = text(h.subarray(345, 500)); if (prefix) name = `${prefix}/${name}`;
    }
    let size = number(h.subarray(124, 136)), linkName = text(h.subarray(157, 257));
    if (!extension) {
      const attributes = { ...this.globalPax, ...this.nextPax };
      name = attributes.path ?? this.longName ?? name;
      linkName = attributes.linkpath ?? this.longLink ?? linkName;
      if (attributes.size !== undefined) {
        if (!/^\d+$/.test(attributes.size)) invalid();
        size = Number(attributes.size); if (!Number.isSafeInteger(size)) invalid();
      }
      this.nextPax = {}; this.longName = null; this.longLink = null;
      name = safeName(name);
      if ((!name && type !== '5') || (['1', '2', '5'].includes(type) && size !== 0)) invalid();
      if (['1', '2'].includes(type)) safeName(linkName);
    } else if (size > MAX_METADATA) invalid();
    const sqlite = !extension && name.endsWith('.sqlite');
    if (sqlite && (!['0', '7'].includes(type) || size < 100)) invalid();
    this.entry = { name, type, size, extension, sqlite, prefix: Buffer.alloc(0), parts: [] };
    this.remaining = size; this.padding = (512 - size % 512) % 512;
    if (!size) this.finishEntry();
  }
  finishEntry() {
    const entry = this.entry;
    if (entry.extension) {
      const bytes = Buffer.concat(entry.parts);
      if (entry.type === 'x') this.nextPax = { ...this.nextPax, ...pax(bytes) };
      else if (entry.type === 'g') this.globalPax = { ...this.globalPax, ...pax(bytes) };
      else if (entry.type === 'L') this.longName = text(bytes);
      else this.longLink = text(bytes);
      bytes.fill(0);
    } else {
      this.count++;
      if ((entry.name.startsWith('rooms/') || /^[^/]+\.json$/.test(entry.name))
          && entry.size > 0 && ['0', '7'].includes(entry.type)) this.rooms++;
      if (entry.sqlite) {
        if (!entry.prefix.subarray(0, 16).equals(SQLITE_MAGIC)) invalid();
        const rawPageSize = entry.prefix.readUInt16BE(16), pageSize = rawPageSize === 1 ? 65536 : rawPageSize;
        if (pageSize < 512 || pageSize > 65536 || (pageSize & (pageSize - 1)) !== 0 || entry.size % pageSize !== 0) invalid();
        if (![1, 2].includes(entry.prefix[18]) || ![1, 2].includes(entry.prefix[19])) invalid();
        this.sqlite++;
        if (entry.name === 'connector-store.sqlite') this.connectorStore = true;
      }
    }
    entry.prefix.fill(0); this.entry = null;
  }
  finish() {
    if (!this.ended || this.headerUsed || this.remaining || this.padding || this.entry
        || Object.keys(this.nextPax).length || this.longName !== null || this.longLink !== null
        || !this.connectorStore || !this.count) invalid();
    return { archiveEntries: this.count, roomFiles: this.rooms, sqliteFiles: this.sqlite };
  }
}
class PlaintextVerifier {
  sizeBytes = Buffer.alloc(4); sizeUsed = 0; metadataSize = null; metadataParts = []; metadataBytes = 0;
  metadataReady = false; tar = new TarVerifier();
  feed(chunk) {
    let at = 0;
    if (this.metadataSize === null) {
      const size = Math.min(4 - this.sizeUsed, chunk.length);
      chunk.copy(this.sizeBytes, this.sizeUsed, 0); this.sizeUsed += size; at += size;
      if (this.sizeUsed < 4) return;
      this.metadataSize = this.sizeBytes.readUInt32BE(0);
      if (this.metadataSize < 2 || this.metadataSize > MAX_METADATA) invalid();
    }
    if (!this.metadataReady) {
      const size = Math.min(this.metadataSize - this.metadataBytes, chunk.length - at);
      this.metadataParts.push(Buffer.from(chunk.subarray(at, at + size))); this.metadataBytes += size; at += size;
      if (this.metadataBytes < this.metadataSize) return;
      const bytes = Buffer.concat(this.metadataParts); const metadata = JSON.parse(bytes.toString('utf8'));
      bytes.fill(0); this.metadataParts.forEach(part => part.fill(0)); this.metadataParts = [];
      if (metadata.offline !== true || metadata.original?.State?.Running !== false || metadata.dataFormat !== 'tar'
          || !Array.isArray(metadata.original?.Mounts)
          || !metadata.original.Mounts.some(mount => mount.Destination === '/data' && mount.Type === 'volume')) invalid();
      this.metadataReady = true;
    }
    if (at < chunk.length) this.tar.feed(chunk.subarray(at));
  }
  finish() { if (!this.metadataReady) invalid(); return this.tar.finish(); }
}
async function readExactly(handle, size, position) {
  const bytes = Buffer.alloc(size); let at = 0;
  while (at < size) { const result = await handle.read(bytes, at, size - at, position + at); if (!result.bytesRead) invalid(); at += result.bytesRead; }
  return bytes;
}
try {
  const chunks = []; let length = 0;
  for await (const chunk of process.stdin) { length += chunk.length; if (length > MAX_INPUT) invalid(); chunks.push(chunk); }
  const input = Buffer.concat(chunks);
  const { file, privateKeyPem } = JSON.parse(input.toString('utf8').replace(/^\uFEFF/, ''));
  input.fill(0); chunks.forEach(chunk => chunk.fill(0));
  const privateKey = createPrivateKey(privateKeyPem);
  if (privateKey.asymmetricKeyType !== 'rsa' || privateKey.asymmetricKeyDetails.modulusLength < 3072) invalid();
  const handle = await open(file, 'r');
  let receipt;
  try {
    const stat = await handle.stat(); if (!stat.isFile() || stat.size < 32) invalid();
    const first = await readExactly(handle, 12, 0);
    if (first.subarray(0, 8).toString() !== 'SOTYBAK1') invalid();
    const headerSize = first.readUInt32BE(8), end = 12 + headerSize;
    if (headerSize < 2 || end > MAX_HEADER || stat.size < end + 20) invalid();
    const headerBytes = await readExactly(handle, headerSize, 12), prefix = Buffer.concat([first, headerBytes]);
    const header = JSON.parse(headerBytes.toString('utf8'));
    const keyId = createHash('sha256').update(createPublicKey(privateKey).export({ type: 'spki', format: 'der' })).digest('hex');
    if (header.format !== 'soty.encrypted-backup.v1' || header.algorithm !== 'RSA-OAEP-SHA256/AES-256-GCM' || header.keyId !== keyId) invalid();
    const iv = Buffer.from(header.iv, 'base64'); if (iv.length !== 12) invalid();
    const key = privateDecrypt({ key: privateKey, oaepHash: 'sha256' }, Buffer.from(header.key, 'base64'));
    if (key.length !== 32) invalid();
    const cipher = createDecipheriv('aes-256-gcm', key, iv); key.fill(0);
    const tag = await readExactly(handle, 16, stat.size - 16);
    cipher.setAAD(prefix); cipher.setAuthTag(tag);
    const digest = createHash('sha256').update(prefix), parser = new PlaintextVerifier();
    for await (const encrypted of createReadStream(file, { fd: handle.fd, autoClose: false, start: end, end: stat.size - 17, highWaterMark: 64 * 1024 })) {
      digest.update(encrypted); const plain = cipher.update(encrypted);
      try { parser.feed(plain); } finally { plain.fill(0); }
    }
    const final = cipher.final();
    try { parser.feed(final); } finally { final.fill(0); }
    digest.update(tag);
    receipt = { ok: true, authenticated: true, offline: true, ...parser.finish(), sha256: digest.digest('hex') };
  } finally { await handle.close(); }
  console.log(JSON.stringify(receipt));
} catch { console.error(JSON.stringify({ ok: false, code: 'backup_verification_failed' })); process.exitCode = 1; }
