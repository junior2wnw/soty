// Internal SOTYBAK1 reader. No plaintext, metadata or file-name callback escapes
// this module. The public verifier and private restore inspection share one parser.
import { open } from 'node:fs/promises';
import { createDecipheriv, privateDecrypt, createHash, createPrivateKey, createPublicKey } from 'node:crypto';

const MAX_HEADER = 16 * 1024, MAX_METADATA = 4 * 1024 * 1024, CHUNK_BYTES = 64 * 1024;
const SQLITE_MAGIC = Buffer.from('SQLite format 3\0');
const UTF8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const failures = new WeakMap();
export function formatFailure(code = 'restore_archive_invalid') {
  const error = new Error(code); failures.set(error, code); return error;
}
export function formatFailureCode(error) { return failures.get(error); }
const invalid = () => { throw formatFailure(); };
const incomplete = () => { throw formatFailure('restore_incomplete'); };
const limited = () => { throw formatFailure('restore_limit_exceeded'); };
const hash = value => createHash('sha256').update(value).digest('hex');
const legacyText = bytes => bytes.toString('utf8').replace(/\0.*$/s, '');
function strictText(bytes) {
  const end = bytes.indexOf(0);
  if (end !== -1 && !bytes.subarray(end).every(byte => byte === 0)) invalid();
  try { return UTF8.decode(end === -1 ? bytes : bytes.subarray(0, end)); } catch { invalid(); }
}
function number(bytes) {
  if (bytes[0] & 0x80) {
    if (bytes[0] & 0x40) invalid();
    let result = BigInt(bytes[0] & 0x7f);
    for (const byte of bytes.subarray(1)) result = (result << 8n) | BigInt(byte);
    if (result > BigInt(Number.MAX_SAFE_INTEGER)) invalid();
    return Number(result);
  }
  const value = legacyText(bytes).trim();
  if (value && !/^[0-7]+$/.test(value)) invalid();
  const result = value ? parseInt(value, 8) : 0;
  if (!Number.isSafeInteger(result) || result < 0) invalid();
  return result;
}
function legacyName(value) {
  if (typeof value !== 'string' || value.length > 4096 || value.includes('\\') || value.includes('\0')
      || value.startsWith('/') || /^[A-Za-z]:/.test(value)) invalid();
  const parts = value.split('/').filter(part => part !== '.' && part !== '');
  if (parts.some(part => part === '..')) invalid();
  return parts.join('/');
}
function strictName(value, directory, limits) {
  if (typeof value !== 'string') invalid();
  if (value.length > 4096) limited();
  if (!value.isWellFormed() || /[\u0000-\u001f\u007f-\u009f\\]/u.test(value)
      || value.startsWith('/') || value.includes('//') || /^[A-Za-z]:/.test(value)) invalid();
  if (Buffer.byteLength(value) > 4096) limited();
  let path = value;
  while (path.startsWith('./')) path = path.slice(2);
  if (directory && path.endsWith('/')) path = path.slice(0, -1);
  if (directory && path === '.') path = '';
  const parts = path === '' ? [] : path.split('/');
  if (parts.some(part => part === '' || part === '.' || part === '..')) invalid();
  if (parts.length > limits.pathDepth || parts.some(part => Buffer.byteLength(part) > 255)) limited();
  if (!directory && parts.length === 0) invalid();
  return parts.join('/');
}
const PAX_KEYS = new Set(['path', 'size', 'uid', 'gid', 'mtime', 'atime', 'ctime']);
function pax(bytes, strict = false) {
  const result = {};
  for (let at = 0; at < bytes.length;) {
    const space = bytes.indexOf(32, at);
    if (space < at || space - at > 12) invalid();
    const digits = bytes.subarray(at, space).toString();
    if (!/^[1-9]\d*$/.test(digits)) invalid();
    const length = Number(digits), end = at + length;
    if (!Number.isSafeInteger(length) || end > bytes.length || end <= space + 2 || bytes[end - 1] !== 10) invalid();
    let record;
    try { record = strict ? UTF8.decode(bytes.subarray(space + 1, end - 1)) : bytes.subarray(space + 1, end - 1).toString('utf8'); }
    catch { invalid(); }
    const equals = record.indexOf('='); if (equals < 1) invalid();
    const key = record.slice(0, equals), value = record.slice(equals + 1);
    if (strict) {
      if (!PAX_KEYS.has(key) || Object.hasOwn(result, key)) invalid();
      if (['mtime', 'atime', 'ctime'].includes(key) && !/^-?\d{1,16}(?:\.\d{1,12})?$/.test(value)) invalid();
      if (['size', 'uid', 'gid'].includes(key) && !/^(?:0|[1-9]\d{0,15})$/.test(value)) invalid();
      result[key] = value;
    } else if (['path', 'linkpath', 'size'].includes(key)) result[key] = value;
    at = end;
  }
  return result;
}
function mergePax(previous, next, strict) {
  if (strict && Object.keys(next).some(key => Object.hasOwn(previous, key))) invalid();
  return { ...previous, ...next };
}
function exact(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))) invalid();
}
const HEX = /^[a-f0-9]{64}$/;
const logicalId = value => typeof value === 'string' && /^[a-z][a-z0-9._-]{0,95}$/.test(value);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const owner = value => integer(value) && value <= 4294967294;
const mode = value => integer(value) && value <= 0o777;
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;
function add(value, amount, maximum) {
  if (!integer(amount) || amount > maximum - value) limited();
  return value + amount;
}

// The fixed projection is deliberately not a general-purpose JSON canonicalizer.
// Producers and the separate cold-source witness must implement this same contract.
class RestoreInventory {
  files = new Map(); seen = new Set(); headers = 0; dataBytes = 0; externalCount = 0; externalBytes = 0;
  constructor(options) { this.options = options; }
  metadata(metadata) {
    const { limits, sourceWitness, expectedManifestSha256 } = this.options;
    if (!metadata.restoreManifest) incomplete();
    const manifest = metadata.restoreManifest;
    exact(manifest, ['version', 'generationId', 'checkpointSha256', 'inventory']);
    if (manifest.version !== 1 || typeof manifest.generationId !== 'string' || !/^[a-f0-9]{32}$/.test(manifest.generationId)
        || typeof manifest.checkpointSha256 !== 'string' || !HEX.test(manifest.checkpointSha256)) invalid();
    exact(manifest.inventory, ['files', 'stores', 'external']);
    const inventory = manifest.inventory;
    if (!Array.isArray(inventory.files) || !Array.isArray(inventory.stores) || !Array.isArray(inventory.external)) invalid();
    if (inventory.files.length > limits.entries || inventory.stores.length > limits.entries
        || inventory.external.length > limits.externalFiles) limited();
    let pathBytes = 0;
    const files = inventory.files.map(file => {
      exact(file, ['path', 'type', 'size', 'sha256', 'uid', 'gid', 'mode']);
      if (!['file', 'directory'].includes(file.type) || !integer(file.size) || !owner(file.uid) || !owner(file.gid) || !mode(file.mode)) invalid();
      const path = strictName(file.path, file.type === 'directory', limits);
      if (path !== file.path || this.files.has(path)) invalid();
      if (file.type === 'directory' ? file.size !== 0 || file.sha256 !== null : typeof file.sha256 !== 'string' || !HEX.test(file.sha256)) invalid();
      if (file.size > limits.fileBytes) limited();
      pathBytes = add(pathBytes, Buffer.byteLength(path), limits.pathBytes);
      this.dataBytes = add(this.dataBytes, file.size, limits.extractedBytes);
      const result = { path, type: file.type, size: file.size, sha256: file.sha256, uid: file.uid, gid: file.gid, mode: file.mode };
      this.files.set(path, result); return result;
    }).sort((a, b) => compare(a.path, b.path));
    const storeIds = new Set();
    const stores = inventory.stores.map(store => {
      exact(store, ['id', 'required', 'present', 'format', 'identitySha256', 'paths']);
      if (!logicalId(store.id) || storeIds.has(store.id) || typeof store.required !== 'boolean' || typeof store.present !== 'boolean'
          || !Array.isArray(store.paths) || store.paths.length > limits.entries) invalid();
      storeIds.add(store.id);
      if (store.required && !store.present) incomplete();
      if (store.present) {
        if (typeof store.format !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._:/@+-]{0,127}$/.test(store.format)
            || typeof store.identitySha256 !== 'string' || !HEX.test(store.identitySha256) || !store.paths.length) invalid();
      } else if (store.format !== null || store.identitySha256 !== null || store.paths.length) invalid();
      const paths = new Set();
      for (const path of store.paths) {
        if (typeof path !== 'string' || this.files.get(path)?.type !== 'file' || paths.has(path)) invalid();
        paths.add(path);
      }
      return { id: store.id, required: store.required, present: store.present, format: store.format,
        identitySha256: store.identitySha256, paths: [...paths].sort(compare) };
    }).sort((a, b) => compare(a.id, b.id));
    const externalIds = new Set(), presentExternal = new Set();
    const external = inventory.external.map(file => {
      exact(file, ['id', 'required', 'present', 'size', 'sha256', 'uid', 'gid', 'mode']);
      if (!logicalId(file.id) || externalIds.has(file.id) || typeof file.required !== 'boolean' || typeof file.present !== 'boolean'
          || !integer(file.size) || !owner(file.uid) || !owner(file.gid) || !mode(file.mode)) invalid();
      externalIds.add(file.id);
      if (file.required && !file.present) incomplete();
      if (file.present) {
        if (typeof file.sha256 !== 'string' || !HEX.test(file.sha256)) invalid();
        if (file.size > limits.fileBytes) limited();
        presentExternal.add(file.id); this.externalCount++;
        this.externalBytes = add(this.externalBytes, file.size, limits.externalBytes);
      } else if (file.size !== 0 || file.sha256 !== null || file.uid !== 0 || file.gid !== 0 || file.mode !== 0) invalid();
      return { id: file.id, required: file.required, present: file.present, size: file.size,
        sha256: file.sha256, uid: file.uid, gid: file.gid, mode: file.mode };
    }).sort((a, b) => compare(a.id, b.id));
    add(this.dataBytes, this.externalBytes, limits.extractedBytes);
    const projectedInventory = { files, stores, external };
    const inventorySha256 = hash(JSON.stringify(projectedInventory));
    const projectedManifest = { version: 1, generationId: manifest.generationId,
      checkpointSha256: manifest.checkpointSha256, inventory: projectedInventory };
    if (manifest.generationId !== sourceWitness.generationId || manifest.checkpointSha256 !== sourceWitness.checkpointSha256
        || inventorySha256 !== sourceWitness.inventorySha256 || hash(JSON.stringify(projectedManifest)) !== expectedManifestSha256) incomplete();
    if (!metadata.restoreFiles || typeof metadata.restoreFiles !== 'object' || Array.isArray(metadata.restoreFiles)
        || Object.keys(metadata.restoreFiles).length !== presentExternal.size
        || Object.keys(metadata.restoreFiles).some(id => !presentExternal.has(id))) incomplete();
    // The old single-bind secrets object is not an alternate, un-inventoried
    // source of restore material. Its support remains unchanged in R0.
    if (metadata.secrets !== undefined && (!metadata.secrets || typeof metadata.secrets !== 'object'
        || Array.isArray(metadata.secrets) || Object.keys(metadata.secrets).length)) incomplete();
    for (const file of external) {
      if (!file.present) continue;
      const encoded = metadata.restoreFiles[file.id];
      if (typeof encoded !== 'string' || encoded.length !== 4 * Math.ceil(file.size / 3)) invalid();
      const bytes = Buffer.from(encoded, 'base64');
      try { if (bytes.length !== file.size || bytes.toString('base64') !== encoded || hash(bytes) !== file.sha256) incomplete(); }
      finally { bytes.fill(0); }
    }
  }
  header() { this.headers = add(this.headers, 1, this.options.limits.headers); }
  begin(entry) {
    const expected = this.files.get(entry.name);
    if (this.seen.has(entry.name)) invalid();
    if (!expected) incomplete();
    const parentAt = entry.name.lastIndexOf('/'), parent = parentAt < 0 ? '' : entry.name.slice(0, parentAt);
    if (entry.name !== '' && (!this.seen.has(parent) || this.files.get(parent)?.type !== 'directory')) invalid();
    if (expected.type !== (entry.type === '5' ? 'directory' : 'file') || expected.size !== entry.size
        || expected.uid !== entry.uid || expected.gid !== entry.gid || expected.mode !== entry.mode) incomplete();
    this.seen.add(entry.name);
    entry.digest = entry.type === '0' ? createHash('sha256') : null;
  }
  finishEntry(entry) {
    if (entry.digest && entry.digest.digest('hex') !== this.files.get(entry.name).sha256) incomplete();
  }
  finish() {
    if (this.seen.size !== this.files.size) incomplete();
    return { externalFiles: this.externalCount, verifiedFileBytes: this.dataBytes + this.externalBytes };
  }
}

class TarVerifier {
  header = Buffer.alloc(512); headerUsed = 0; remaining = 0; padding = 0;
  entry = null; zeroBlocks = 0; ended = false; count = 0; rooms = 0; sqlite = 0; emptySqlite = 0;
  connectorStore = false; nextPax = {}; globalPax = {}; longName = null; longLink = null;
  constructor(policy = null) { this.policy = policy; }
  feed(chunk) {
    let at = 0;
    while (at < chunk.length) {
      if (this.remaining) {
        const size = Math.min(this.remaining, chunk.length - at), part = chunk.subarray(at, at + size);
        if (this.entry.extension) { part.copy(this.entry.extensionBytes, this.entry.extensionUsed); this.entry.extensionUsed += size; }
        if (this.entry.sqlite && this.entry.prefixUsed < 100) {
          const count = Math.min(size, 100 - this.entry.prefixUsed);
          part.copy(this.entry.prefix, this.entry.prefixUsed, 0, count); this.entry.prefixUsed += count;
        }
        this.entry.digest?.update(part);
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
    this.policy?.header();
    const h = this.header;
    if (h.every(byte => byte === 0)) { this.zeroBlocks++; if (this.zeroBlocks >= 2) this.ended = true; return; }
    if (this.zeroBlocks || this.ended) invalid();
    const checksum = number(h.subarray(148, 156));
    let actual = 0; for (let i = 0; i < 512; i++) actual += i >= 148 && i < 156 ? 32 : h[i];
    if (checksum !== actual) invalid();
    const type = h[156] ? String.fromCharCode(h[156]) : '0';
    const extension = ['x', 'g', 'L', 'K'].includes(type);
    if (!extension && !['0', '1', '2', '5', '7'].includes(type)) invalid();
    if (this.policy && (type === 'K' || !extension && !['0', '5'].includes(type))) invalid();
    const text = this.policy ? strictText : legacyText;
    let name = text(h.subarray(0, 100));
    if (h.subarray(257, 263).equals(Buffer.from('ustar\0'))) {
      const prefix = text(h.subarray(345, 500)); if (prefix) name = `${prefix}/${name}`;
    }
    let size = number(h.subarray(124, 136)), linkName = text(h.subarray(157, 257));
    let uid = 0, gid = 0, permissions = 0;
    if (this.policy) {
      uid = number(h.subarray(108, 116)); gid = number(h.subarray(116, 124)); permissions = number(h.subarray(100, 108));
      if (!owner(uid) || !owner(gid) || !mode(permissions) || linkName) invalid();
    }
    if (!extension) {
      const attributes = { ...this.globalPax, ...this.nextPax };
      name = attributes.path ?? this.longName ?? name;
      linkName = attributes.linkpath ?? this.longLink ?? linkName;
      if (attributes.size !== undefined) {
        if (!/^\d+$/.test(attributes.size)) invalid();
        size = Number(attributes.size); if (!Number.isSafeInteger(size)) invalid();
      }
      if (this.policy) {
        if (attributes.uid !== undefined) uid = Number(attributes.uid);
        if (attributes.gid !== undefined) gid = Number(attributes.gid);
        if (!owner(uid) || !owner(gid)) invalid();
        if (size > this.policy.options.limits.fileBytes) limited();
      }
      this.nextPax = {}; this.longName = null; this.longLink = null;
      name = this.policy ? strictName(name, type === '5', this.policy.options.limits) : legacyName(name);
      if ((!name && type !== '5') || (['1', '2', '5'].includes(type) && size !== 0)) invalid();
      if (['1', '2'].includes(type)) legacyName(linkName);
    } else if (size > MAX_METADATA) invalid();
    const sqliteName = !extension && name.endsWith('.sqlite');
    const emptySqlite = sqliteName && size === 0 && name !== 'connector-store.sqlite' && ['0', '7'].includes(type);
    const sqlite = sqliteName && !emptySqlite;
    if (sqlite && (!['0', '7'].includes(type) || size < 100)) invalid();
    this.entry = { name, type, size, uid, gid, mode: permissions, extension, sqlite, emptySqlite,
      prefix: Buffer.alloc(sqlite ? 100 : 0), prefixUsed: 0,
      extensionBytes: extension ? Buffer.alloc(size) : null, extensionUsed: 0 };
    if (!extension) this.policy?.begin(this.entry);
    this.remaining = size; this.padding = (512 - size % 512) % 512;
    if (!size) this.finishEntry();
  }
  finishEntry() {
    const entry = this.entry;
    if (entry.extension) {
      const bytes = entry.extensionBytes;
      try {
        if (entry.type === 'x') this.nextPax = mergePax(this.nextPax, pax(bytes, !!this.policy), !!this.policy);
        else if (entry.type === 'g') this.globalPax = mergePax(this.globalPax, pax(bytes, !!this.policy), !!this.policy);
        else if (entry.type === 'L') {
          if (this.policy && this.longName !== null) invalid();
          this.longName = this.policy ? strictText(bytes) : legacyText(bytes);
        } else this.longLink = legacyText(bytes);
      } finally { bytes.fill(0); }
    } else {
      this.count++;
      if (entry.emptySqlite) this.emptySqlite++;
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
      this.policy?.finishEntry(entry);
    }
    entry.prefix.fill(0); this.entry = null;
  }
  finish() {
    if (!this.ended || this.headerUsed || this.remaining || this.padding || this.entry
        || Object.keys(this.nextPax).length || this.longName !== null || this.longLink !== null
        || !this.connectorStore || !this.count) invalid();
    return { archiveEntries: this.count, roomFiles: this.rooms, sqliteFiles: this.sqlite, emptySqliteFiles: this.emptySqlite,
      ...(this.policy?.finish() ?? {}) };
  }
  clear() { this.header.fill(0); this.entry?.prefix.fill(0); this.entry?.extensionBytes?.fill(0); }
}
class PlaintextVerifier {
  sizeBytes = Buffer.alloc(4); sizeUsed = 0; metadataSize = null; metadataBuffer = null; metadataBytes = 0;
  metadataReady = false;
  constructor(policy = null) { this.policy = policy; this.tar = new TarVerifier(policy); }
  feed(chunk) {
    let at = 0;
    if (this.metadataSize === null) {
      const size = Math.min(4 - this.sizeUsed, chunk.length);
      chunk.copy(this.sizeBytes, this.sizeUsed, 0); this.sizeUsed += size; at += size;
      if (this.sizeUsed < 4) return;
      this.metadataSize = this.sizeBytes.readUInt32BE(0);
      if (this.metadataSize < 2 || this.metadataSize > MAX_METADATA) invalid();
      if (this.policy && this.metadataSize + 4 > this.policy.options.limits.plaintextBytes) limited();
      this.metadataBuffer = Buffer.alloc(this.metadataSize);
    }
    if (!this.metadataReady) {
      const size = Math.min(this.metadataSize - this.metadataBytes, chunk.length - at);
      chunk.copy(this.metadataBuffer, this.metadataBytes, at, at + size); this.metadataBytes += size; at += size;
      if (this.metadataBytes < this.metadataSize) return;
      let metadata;
      try { metadata = JSON.parse(this.policy ? UTF8.decode(this.metadataBuffer) : this.metadataBuffer.toString('utf8')); }
      catch { invalid(); }
      finally { this.metadataBuffer.fill(0); this.metadataBuffer = null; }
      if (metadata.offline !== true || metadata.original?.State?.Running !== false || metadata.dataFormat !== 'tar'
          || !Array.isArray(metadata.original?.Mounts)
          || !metadata.original.Mounts.some(mount => mount.Destination === '/data' && mount.Type === 'volume')) invalid();
      this.policy?.metadata(metadata);
      this.metadataReady = true;
    }
    if (at < chunk.length) this.tar.feed(chunk.subarray(at));
  }
  finish() { if (!this.metadataReady) invalid(); return this.tar.finish(); }
  clear() { this.sizeBytes.fill(0); this.metadataBuffer?.fill(0); this.tar.clear(); }
}
async function readExactly(handle, size, position, check) {
  const bytes = Buffer.alloc(size); let at = 0;
  while (at < size) {
    check?.();
    const result = await handle.read(bytes, at, size - at, position + at);
    check?.(result.bytesRead > 0);
    if (!result.bytesRead) invalid(); at += result.bytesRead;
  }
  return bytes;
}

// Internal entrypoint: restore is a previously validated, captured policy, not
// an arbitrary callback API. The only observable result contains safe counts.
export async function readEncryptedBackup({ file, privateKeyPem, restore = null }) {
  const check = restore?.check;
  check?.();
  let privateKey;
  try {
    privateKey = createPrivateKey(privateKeyPem);
    if (privateKey.asymmetricKeyType !== 'rsa' || privateKey.asymmetricKeyDetails.modulusLength < 3072) invalid();
  } catch { throw formatFailure('restore_authentication_failed'); }
  check?.();
  const handle = await open(file, 'r');
  let receipt;
  try {
    check?.();
    const stat = await handle.stat(); check?.();
    if (!stat.isFile() || stat.size < 32) invalid();
    if (restore && (!Number.isSafeInteger(stat.size) || stat.size > restore.limits.archiveBytes)) limited();
    const first = await readExactly(handle, 12, 0, check);
    if (first.subarray(0, 8).toString() !== 'SOTYBAK1') invalid();
    const headerSize = first.readUInt32BE(8), end = 12 + headerSize;
    if (headerSize < 2 || end > MAX_HEADER || stat.size < end + 20) invalid();
    const headerBytes = await readExactly(handle, headerSize, 12, check), prefix = Buffer.concat([first, headerBytes]);
    let header;
    try { header = JSON.parse(restore ? UTF8.decode(headerBytes) : headerBytes.toString('utf8')); } catch { invalid(); }
    if (restore) {
      exact(header, ['format', 'algorithm', 'keyId', 'key', 'iv']);
      if (typeof header.key !== 'string' || header.key.length > MAX_HEADER || typeof header.iv !== 'string' || header.iv.length !== 16) invalid();
    }
    const keyId = hash(createPublicKey(privateKey).export({ type: 'spki', format: 'der' }));
    if (header.format !== 'soty.encrypted-backup.v1' || header.algorithm !== 'RSA-OAEP-SHA256/AES-256-GCM' || header.keyId !== keyId)
      throw formatFailure('restore_authentication_failed');
    const iv = Buffer.from(header.iv, 'base64'); if (iv.length !== 12) invalid();
    let key, cipher;
    try {
      key = privateDecrypt({ key: privateKey, oaepHash: 'sha256' }, Buffer.from(header.key, 'base64'));
      if (key.length !== 32) invalid();
      cipher = createDecipheriv('aes-256-gcm', key, iv);
    } catch { throw formatFailure('restore_authentication_failed'); }
    finally { key?.fill(0); }
    check?.();
    const tag = await readExactly(handle, 16, stat.size - 16, check);
    cipher.setAAD(prefix); cipher.setAuthTag(tag);
    const digest = createHash('sha256').update(prefix), parser = new PlaintextVerifier(restore ? new RestoreInventory(restore) : null);
    const encrypted = Buffer.alloc(CHUNK_BYTES), ciphertextEnd = stat.size - 16;
    let plaintextBytes = 0;
    const feed = plain => {
      try {
        if (restore) plaintextBytes = add(plaintextBytes, plain.length, restore.limits.plaintextBytes);
        parser.feed(plain);
      } finally { plain.fill(0); }
    };
    try {
      for (let position = end; position < ciphertextEnd;) {
        check?.();
        const { bytesRead } = await handle.read(encrypted, 0, Math.min(encrypted.length, ciphertextEnd - position), position);
        check?.(bytesRead > 0);
        if (!bytesRead) invalid();
        const bytes = encrypted.subarray(0, bytesRead);
        digest.update(bytes); feed(cipher.update(bytes)); position += bytesRead;
      }
      let final;
      try { final = cipher.final(); } catch { throw formatFailure('restore_authentication_failed'); }
      feed(final); digest.update(tag);
      const counts = parser.finish(), sha256 = digest.digest('hex');
      if (restore && sha256 !== restore.expectedSha256) throw formatFailure('restore_authentication_failed');
      check?.();
      receipt = { ok: true, authenticated: true, offline: true, ...counts, sha256 };
    } finally { encrypted.fill(0); parser.clear(); }
  } finally { await handle.close(); }
  check?.();
  return receipt;
}
