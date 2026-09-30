import { createHash } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { open, stat } from 'node:fs/promises';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { isEncryptedFile, isEncryptedUpdate } from './validators.js';
import { pruneWaiting } from './room-waiting.mjs';

export const roomStorageFormat = 2;
export const roomStorageDefaults = Object.freeze({
  fileBytes: 512_000_000, roomBytes: 2 * 1024 ** 3, totalBytes: 10 * 1024 ** 3,
  receivingFiles: 4, roomFiles: 4096, totalFiles: 100_000,
  roomEvents: 100_000, totalEvents: 1_000_000, roomUpdateBytes: 64 * 1024 ** 2,
  metadataBytes: 256 * 1024 ** 2, roomCount: 10_000, cachedRooms: 128,
  legacyReadBytes: 64 * 1024 ** 2,
});
export class RoomStorageError extends Error {
  constructor(code) { super(code); this.name = 'RoomStorageError'; this.code = code; }
}
const fail = code => { throw new RoomStorageError(code); };
const hash = text => createHash('sha256').update(text).digest('hex');
const safeId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,120}$/u.test(value);

// Retry identity includes author but excludes server timestamps/nicknames.
function canonicalPayload(value, type) {
  const keys = type === 'update' ? ['kind', 'id', 'nonce', 'ciphertext', 'deviceId']
    : ['kind', 'id', 'fileId', 'index', 'total', 'totalBytes', 'bytes', 'nonce', 'ciphertext', 'metaNonce', 'metaCiphertext', 'deviceId'];
  const result = {};
  for (const key of keys) if (value[key] !== undefined) result[key] = value[key];
  if (type === 'file') result.kind ??= 'complete';
  return result;
}
function storedPayload(value, type) {
  const payload = canonicalPayload(value, type);
  if (typeof value.createdAt === 'string') payload.createdAt = value.createdAt.slice(0, 40);
  if (typeof value.deviceNick === 'string') payload.deviceNick = value.deviceNick.slice(0, 80);
  return payload;
}

export function createRoomStore(dataDir, options = {}) {
  const limits = { ...roomStorageDefaults, ...options.limits };
  for (const value of Object.values(limits)) if (!Number.isSafeInteger(value) || value < 1) fail('room_storage_config_invalid');
  mkdirSync(dataDir, { recursive: true, mode: 0o700 });
  const filename = path.join(dataDir, 'rooms-v2.sqlite');
  const db = new DatabaseSync(filename);
  const version = db.prepare('PRAGMA user_version').get().user_version;
  if (version !== 0 && version !== roomStorageFormat) { db.close(); fail('room_storage_format_unsupported'); }
  db.exec([
    'PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;',
    'PRAGMA busy_timeout=5000; PRAGMA cache_size=-4096; PRAGMA mmap_size=0; PRAGMA temp_store=FILE;',
    'CREATE TABLE IF NOT EXISTS room_state (room_id TEXT PRIMARY KEY, auth TEXT, closed_json TEXT, sequence INTEGER NOT NULL DEFAULT 0, reserved_bytes INTEGER NOT NULL DEFAULT 0, update_bytes INTEGER NOT NULL DEFAULT 0);',
    'CREATE TABLE IF NOT EXISTS room_files (room_id TEXT NOT NULL REFERENCES room_state(room_id), file_id TEXT NOT NULL, kind TEXT NOT NULL, device_id TEXT NOT NULL, total_bytes INTEGER NOT NULL, total_chunks INTEGER NOT NULL, received_bytes INTEGER NOT NULL DEFAULT 0, received_chunks INTEGER NOT NULL DEFAULT 0, has_meta INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, PRIMARY KEY(room_id,file_id));',
    "CREATE TABLE IF NOT EXISTS room_receipts (room_id TEXT NOT NULL REFERENCES room_state(room_id), message_id TEXT NOT NULL, fingerprint TEXT NOT NULL, sequence INTEGER NOT NULL, type TEXT NOT NULL, file_id TEXT, disposition TEXT NOT NULL DEFAULT 'stored', PRIMARY KEY(room_id,message_id));",
    'CREATE TABLE IF NOT EXISTS room_events (room_id TEXT NOT NULL REFERENCES room_state(room_id), sequence INTEGER NOT NULL, type TEXT NOT NULL, file_id TEXT, chunk_index INTEGER, device_id TEXT NOT NULL, wire_bytes INTEGER NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(room_id,sequence));',
    'CREATE UNIQUE INDEX IF NOT EXISTS room_chunk_identity ON room_events(room_id,file_id,chunk_index) WHERE chunk_index IS NOT NULL;',
    'CREATE INDEX IF NOT EXISTS room_event_file ON room_events(room_id,file_id);',
    'CREATE TABLE IF NOT EXISTS room_imports (room_id TEXT PRIMARY KEY REFERENCES room_state(room_id), source_hash TEXT NOT NULL, source_bytes INTEGER NOT NULL, source_files INTEGER NOT NULL, source_updates INTEGER NOT NULL, imported_at TEXT NOT NULL);',
    'CREATE TABLE IF NOT EXISTS room_deletions (room_id TEXT NOT NULL REFERENCES room_state(room_id), file_id TEXT NOT NULL, message_id TEXT NOT NULL, fingerprint TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY(room_id,file_id), UNIQUE(room_id,message_id));',
    'PRAGMA user_version=2;',
  ].join('\n'));
  const rooms = new Map(), loading = new Map();
  let closed = false;
  function transaction(fn) {
    if (closed) fail('room_store_closed');
    db.exec('BEGIN IMMEDIATE');
    try { const result = fn(); db.exec('COMMIT'); return result; }
    catch (error) { try { db.exec('ROLLBACK'); } catch {} throw error; }
  }
  function row(id) { return db.prepare('SELECT * FROM room_state WHERE room_id=?').get(id); }
  function metadata(value) {
    if (!value) return { auth: null, closed: null, snapshot: null, updates: [], files: [], sequence: 0 };
    return { auth: value.auth, closed: value.closed_json ? JSON.parse(value.closed_json) : null,
      snapshot: null, updates: [], files: [], sequence: value.sequence };
  }
  function publish(room) { room.state = metadata(row(room.id)); }
  function requireOpen(roomId) {
    const value = row(roomId);
    if (!value || value.closed_json) fail('room_closed');
    return value;
  }
  function duplicate(roomId, payload, type) {
    const fingerprint = hash(JSON.stringify(canonicalPayload(payload, type)));
    const prior = db.prepare('SELECT * FROM room_receipts WHERE room_id=? AND message_id=?').get(roomId, payload.id);
    if (prior) {
      if (prior.fingerprint !== fingerprint || prior.type !== type) fail('message_identity_conflict');
      if (prior.disposition !== 'stored') fail('file_deleted');
      return { duplicate: true, sequence: prior.sequence };
    }
    const deletion = db.prepare('SELECT * FROM room_deletions WHERE room_id=? AND message_id=?').get(roomId, payload.id);
    if (deletion) {
      if (deletion.fingerprint !== fingerprint || type !== 'file' || payload.kind !== 'delete') fail('message_identity_conflict');
      return { duplicate: true, sequence: deletion.sequence };
    }
    return { fingerprint };
  }
  function capacityForEvent(roomId, importing) {
    if (importing) return;
    const local = db.prepare('SELECT count(*) AS n FROM room_receipts WHERE room_id=?').get(roomId).n;
    const global = db.prepare('SELECT count(*) AS n FROM room_receipts').get().n;
    if (local >= limits.roomEvents || global >= limits.totalEvents) fail('room_history_capacity');
  }
  function capacityForFile(roomId, importing) {
    if (!importing && (db.prepare('SELECT count(*) AS n FROM room_files WHERE room_id=?').get(roomId).n >= limits.roomFiles
      || db.prepare('SELECT count(*) AS n FROM room_files').get().n >= limits.totalFiles)) fail('room_file_count_capacity');
  }
  function metadataCapacity(extra, importing) {
    if (importing) return;
    const fileBytes = db.prepare('SELECT coalesce(sum(total_chunks*1024+22000),0) AS n FROM room_files').get().n;
    const updateBytes = db.prepare('SELECT coalesce(sum(update_bytes),0) AS n FROM room_state').get().n;
    const receipts = db.prepare("SELECT count(*)*512 AS n FROM room_receipts WHERE type='update'").get().n;
    if (fileBytes + updateBytes + receipts + extra > limits.metadataBytes) fail('storage_metadata_capacity');
  }
  function addEvent(roomId, payload, type, fingerprint, fileId = null, index = null, receipt = true) {
    const sequence = Number(db.prepare('UPDATE room_state SET sequence=sequence+1 WHERE room_id=? RETURNING sequence').get(roomId).sequence);
    const json = JSON.stringify(payload);
    db.prepare('INSERT INTO room_events VALUES(?,?,?,?,?,?,?,?)').run(roomId, sequence, type, fileId, index,
      payload.deviceId ?? '', Buffer.byteLength(json), json);
    if (receipt) db.prepare('INSERT INTO room_receipts(room_id,message_id,fingerprint,sequence,type,file_id) VALUES(?,?,?,?,?,?)')
      .run(roomId, payload.id, fingerprint, sequence, type, fileId);
    return { duplicate: false, sequence };
  }
  function addUpdate(roomId, payload, importing = false) {
    if (!isEncryptedUpdate(payload)) fail(importing ? 'room_legacy_invalid' : 'update_invalid');
    payload = storedPayload(payload, 'update');
    const state = requireOpen(roomId), receipt = duplicate(roomId, payload, 'update');
    if (receipt.duplicate) return receipt;
    capacityForEvent(roomId, importing);
    const bytes = Buffer.byteLength(JSON.stringify(payload));
    metadataCapacity(bytes + 512, importing);
    if (!importing && state.update_bytes + bytes > limits.roomUpdateBytes) fail('room_history_capacity');
    // An encrypted snapshot cannot prove it contains concurrent updates. Keep
    // all accepted updates until an explicit, verifiable compaction protocol.
    const result = addEvent(roomId, payload, 'update', receipt.fingerprint);
    db.prepare('UPDATE room_state SET update_bytes=update_bytes+? WHERE room_id=?').run(bytes, roomId);
    return result;
  }
  function addFile(roomId, payload, importing = false) {
    if (!isEncryptedFile(payload)) fail(importing ? 'room_legacy_invalid' : 'file_transfer_invalid');
    payload = storedPayload(payload, 'file');
    const state = requireOpen(roomId), receipt = duplicate(roomId, payload, 'file');
    if (receipt.duplicate) return receipt;
    const kind = payload.kind ?? 'complete', fileId = kind === 'complete' ? payload.id : payload.fileId;
    if (!safeId(fileId)) fail(importing ? 'room_legacy_invalid' : 'file_transfer_invalid');
    const existing = db.prepare('SELECT * FROM room_files WHERE room_id=? AND file_id=?').get(roomId, fileId);
    if (kind === 'delete') {
      // A control tombstone has one pre-reserved slot per real file. Full
      // content/history quotas must never prevent deletion of that file.
      if (!existing) fail('file_not_found');
      if (existing.status === 'deleted') fail('file_deleted');
      if (existing?.status !== 'deleted') {
        db.prepare('UPDATE room_state SET reserved_bytes=reserved_bytes-? WHERE room_id=?').run(existing?.total_bytes ?? 0, roomId);
        db.prepare("UPDATE room_receipts SET disposition='deleted' WHERE room_id=? AND file_id=? AND disposition='stored'").run(roomId, fileId);
        db.prepare('DELETE FROM room_events WHERE room_id=? AND file_id=?').run(roomId, fileId);
        if (existing) db.prepare("UPDATE room_files SET status='deleted' WHERE room_id=? AND file_id=?").run(roomId, fileId);
        else db.prepare('INSERT INTO room_files(room_id,file_id,kind,device_id,total_bytes,total_chunks,status) VALUES(?,?,?,?,0,0,?)')
          .run(roomId, fileId, 'delete', payload.deviceId ?? '', 'deleted');
      }
      const result = addEvent(roomId, payload, 'file', receipt.fingerprint, fileId, null, false);
      db.prepare('INSERT INTO room_deletions VALUES(?,?,?,?,?)').run(roomId, fileId, payload.id, receipt.fingerprint, result.sequence);
      return { ...result, pendingChanged: true };
    }
    capacityForEvent(roomId, importing);
    const total = kind === 'chunk' ? payload.total : 1;
    const totalBytes = kind === 'chunk' ? payload.totalBytes : payload.bytes;
    const index = kind === 'chunk' ? payload.index : 0;
    if (!Number.isSafeInteger(totalBytes) || totalBytes < 0 || index >= total || payload.bytes > totalBytes
      || (!importing && totalBytes > limits.fileBytes)) fail('file_transfer_invalid');
    if (!importing) {
      // Prevent a tiny declared reservation carrying an arbitrarily large body.
      if (!/^[A-Za-z0-9+/_-]+={0,2}$/u.test(payload.ciphertext)
        || Buffer.byteLength(payload.ciphertext, 'base64') !== payload.bytes + 16) fail('file_transfer_invalid');
      if (kind === 'chunk' && ((index === 0 && (!payload.metaNonce || !payload.metaCiphertext))
        || (index !== 0 && (payload.metaNonce || payload.metaCiphertext)))) fail('file_transfer_invalid');
    }
    if (existing?.status === 'deleted') fail('file_deleted');
    if (existing && (existing.kind !== kind || existing.total_bytes !== totalBytes || existing.total_chunks !== total
      || existing.device_id !== (payload.deviceId ?? ''))) fail('file_identity_conflict');
    if (!existing) {
      if (!importing) {
        metadataCapacity(total * 1024 + 22000, false);
        const reserved = db.prepare('SELECT coalesce(sum(reserved_bytes),0) AS n FROM room_state').get().n;
        if (state.reserved_bytes + totalBytes > limits.roomBytes) fail('room_file_capacity');
        if (reserved + totalBytes > limits.totalBytes) fail('storage_file_capacity');
        const receiving = db.prepare("SELECT count(*) AS n FROM room_files WHERE room_id=? AND status IN ('receiving','incomplete')").get(roomId).n;
        if (receiving >= limits.receivingFiles) fail('room_transfer_capacity');
        capacityForFile(roomId, importing);
      }
      db.prepare('INSERT INTO room_files(room_id,file_id,kind,device_id,total_bytes,total_chunks,status) VALUES(?,?,?,?,?,?,?)')
        .run(roomId, fileId, kind, payload.deviceId ?? '', totalBytes, total, 'receiving');
      db.prepare('UPDATE room_state SET reserved_bytes=reserved_bytes+? WHERE room_id=?').run(totalBytes, roomId);
    }
    if (db.prepare('SELECT 1 FROM room_events WHERE room_id=? AND file_id=? AND chunk_index=?').get(roomId, fileId, index)) fail('file_identity_conflict');
    const receivedBytes = (existing?.received_bytes ?? 0) + payload.bytes;
    const receivedChunks = (existing?.received_chunks ?? 0) + 1;
    const hasMeta = !!(existing?.has_meta || (payload.metaNonce && payload.metaCiphertext));
    if (receivedBytes > totalBytes || receivedChunks > total || (receivedChunks === total && (receivedBytes !== totalBytes || !hasMeta))) fail('file_transfer_invalid');
    const status = receivedChunks === total ? 'complete' : 'receiving';
    db.prepare('UPDATE room_files SET received_bytes=?,received_chunks=?,has_meta=?,status=? WHERE room_id=? AND file_id=?')
      .run(receivedBytes, receivedChunks, Number(hasMeta), status, roomId, fileId);
    return { ...addEvent(roomId, payload, 'file', receipt.fingerprint, fileId, index),
      pendingChanged: !existing || existing.status !== status,
      progress: { fileId, receivedBytes, receivedChunks } };
  }
  async function ensureRoom(roomId) {
    if (!safeId(roomId)) fail('room_id_invalid');
    if (row(roomId)) return;
    const legacy = path.join(dataDir, roomId + '.json');
    let source = null, text = null;
    try {
      const info = await stat(legacy);
      if (!info.isFile() || info.size > limits.legacyReadBytes) fail('room_legacy_streaming_import_required');
      const handle = await open(legacy, 'r');
      try {
        const buffer = Buffer.alloc(info.size + 1);
        let count = 0;
        while (count < buffer.length) {
          const { bytesRead } = await handle.read(buffer, count, buffer.length - count, count);
          if (!bytesRead) break; count += bytesRead;
        }
        if (count !== info.size) fail('room_legacy_changed');
        text = buffer.subarray(0, count).toString('utf8');
      } finally { await handle.close(); }
      try { source = JSON.parse(text); } catch { fail('room_state_unavailable'); }
      if (!source || typeof source !== 'object' || Array.isArray(source) || (source.auth != null && typeof source.auth !== 'string')
        || (source.updates !== undefined && !Array.isArray(source.updates)) || (source.files !== undefined && !Array.isArray(source.files))
        || (source.snapshot != null && (typeof source.snapshot !== 'object' || Array.isArray(source.snapshot)))
        || (source.closed != null && (typeof source.closed !== 'object' || Array.isArray(source.closed)))) fail('room_state_unavailable');
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (!source) return;
    transaction(() => {
      if (row(roomId)) return;
      db.prepare('INSERT INTO room_state(room_id,auth) VALUES(?,?)').run(roomId, source?.auth ?? null);
      if (source) {
        const updates = [...(source.snapshot ? [source.snapshot] : []), ...(source.updates ?? [])];
        for (const update of updates) addUpdate(roomId, update, true);
        for (const file of source.files ?? []) addFile(roomId, file, true);
        db.prepare("UPDATE room_files SET status='incomplete' WHERE room_id=? AND status='receiving'").run(roomId);
        if (source.closed) db.prepare('UPDATE room_state SET closed_json=? WHERE room_id=?').run(JSON.stringify(source.closed), roomId);
        db.prepare('INSERT INTO room_imports VALUES(?,?,?,?,?,?)').run(roomId, hash(text), Buffer.byteLength(text),
          source.files?.length ?? 0, updates.length, new Date().toISOString());
      }
    });
  }
  return Object.freeze({
    format: roomStorageFormat, filename, limits: Object.freeze(limits),
    load(roomId, { retain = false } = {}) {
      if (closed) return Promise.reject(new RoomStorageError('room_store_closed'));
      const retained = room => { if (retain) room.references++; return room; };
      if (rooms.has(roomId)) return Promise.resolve(retained(rooms.get(roomId)));
      if (loading.has(roomId)) return loading.get(roomId).then(retained);
      for (const [id, room] of rooms) {
        if (rooms.size + loading.size < limits.cachedRooms) break;
        pruneWaiting(room);
        if (room.references === 0 && room.peers.size === 0 && room.waiting.size === 0) rooms.delete(id);
      }
      if (rooms.size + loading.size >= limits.cachedRooms) return Promise.reject(new RoomStorageError('room_cache_capacity'));
      const pending = ensureRoom(roomId).then(() => {
        const room = { id: roomId, references: 0, state: metadata(row(roomId)), peers: new Map(), waiting: new Map() };
        rooms.set(roomId, room); return room;
      });
      loading.set(roomId, pending); pending.then(() => loading.delete(roomId), () => loading.delete(roomId));
      return pending.then(retained);
    },
    release(room) { room.references = Math.max(0, room.references - 1); },
    claimAuth(room, auth) {
      const result = transaction(() => {
        if (!row(room.id)) {
          if (db.prepare('SELECT count(*) AS n FROM room_state').get().n >= limits.roomCount) fail('room_count_capacity');
          db.prepare('INSERT INTO room_state(room_id) VALUES(?)').run(room.id);
        }
        const state = requireOpen(room.id);
        if (state.auth && state.auth !== auth) fail('room_auth_mismatch');
        if (!state.auth) db.prepare('UPDATE room_state SET auth=? WHERE room_id=?').run(auth, room.id);
      });
      publish(room); return result;
    },
    appendUpdate(room, payload) { const result = transaction(() => addUpdate(room.id, payload)); publish(room); return result; },
    appendFile(room, payload) { const result = transaction(() => addFile(room.id, payload)); publish(room); return result; },
    closeRoom(room, value) {
      transaction(() => {
        requireOpen(room.id);
        db.prepare('UPDATE room_state SET closed_json=?,reserved_bytes=0,update_bytes=0 WHERE room_id=?').run(JSON.stringify(value), room.id);
        db.prepare('DELETE FROM room_events WHERE room_id=?').run(room.id);
        db.prepare("UPDATE room_files SET status='deleted' WHERE room_id=?").run(room.id);
        db.prepare("UPDATE room_receipts SET disposition='deleted' WHERE room_id=?").run(room.id);
      });
      publish(room);
    },
    nextEvent(room, after = 0, skippedFiles = []) {
      if (closed) fail('room_store_closed');
      const state = row(room.id);
      if (!state || state.closed_json) return null;
      const excluded = skippedFiles.length ? ' AND (file_id IS NULL OR chunk_index IS NULL OR file_id NOT IN (' + skippedFiles.map(() => '?').join(',') + '))' : '';
      const result = db.prepare('SELECT sequence,type,payload_json,wire_bytes FROM room_events WHERE room_id=? AND sequence>?' + excluded + ' ORDER BY sequence LIMIT 1').get(room.id, after, ...skippedFiles);
      return result ? { sequence: result.sequence, type: result.type, payload: JSON.parse(result.payload_json), wireBytes: result.wire_bytes } : null;
    },
    receipt(room, id) { return db.prepare('SELECT sequence,disposition FROM room_receipts WHERE room_id=? AND message_id=?').get(room.id, id) ?? null; },
    pendingFiles(room) {
      const rows = db.prepare("SELECT f.file_id,f.total_bytes,f.received_bytes,f.total_chunks,f.received_chunks,f.device_id, json_extract(e.payload_json,'$.metaNonce') AS meta_nonce,json_extract(e.payload_json,'$.metaCiphertext') AS meta_ciphertext,json_extract(e.payload_json,'$.deviceNick') AS nick FROM room_files f LEFT JOIN room_events e ON e.room_id=f.room_id AND e.file_id=f.file_id AND e.chunk_index=0 WHERE f.room_id=? AND f.status IN ('receiving','incomplete') ORDER BY f.file_id LIMIT 64").all(room.id);
      let metadataBytes = 0;
      return rows.map(value => {
        const item = { fileId: value.file_id, totalBytes: value.total_bytes, receivedBytes: value.received_bytes,
          totalChunks: value.total_chunks, receivedChunks: value.received_chunks, deviceId: value.device_id, nick: value.nick ?? '' };
        if (value.meta_nonce && value.meta_ciphertext && metadataBytes + value.meta_ciphertext.length <= 128_000) {
          item.metaNonce = value.meta_nonce; item.metaCiphertext = value.meta_ciphertext; metadataBytes += value.meta_ciphertext.length;
        }
        return item;
      });
    },
    stats(room) {
      const state = row(room.id) ?? { reserved_bytes: 0, sequence: 0, update_bytes: 0 };
      return { reservedBytes: state.reserved_bytes, sequence: state.sequence, updateBytes: state.update_bytes,
        files: db.prepare('SELECT file_id,total_bytes,total_chunks,received_bytes,received_chunks,status FROM room_files WHERE room_id=? ORDER BY file_id').all(room.id),
        events: db.prepare('SELECT count(*) AS n FROM room_events WHERE room_id=?').get(room.id).n };
    },
    close() { if (!closed) { closed = true; rooms.clear(); db.close(); } },
  });
}
