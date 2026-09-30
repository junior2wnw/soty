export const maxFileBytes = 512_000_000;
export const fileChunkBytes = 256_000;
export const maxWireChunkBytes = 800_000;
export const maxSocketBufferedBytes = 1_000_000;
export const maxPendingFileBytes = 4_000_000;
export const maxPendingFileChunks = 8;
export const maxControlQueueBytes = 2_000_000;
export const maxControlQueueCount = 64;

export class FileTransferError extends Error {
  constructor(code, fileId) { super(code); this.name = 'FileTransferError'; this.code = code; if (fileId) this.fileId = fileId; }
}
export const wireBytes = value => new TextEncoder().encode(value).byteLength;
export const estimatedChunkWireBytes = bytes => Math.ceil((bytes + 16) / 3) * 4 + 40_000;

/** Bounded delivery window, not a queue of waiting producers. Reserve before
 * reading/encrypting. ACK means relay acceptance, not a recipient's download.
 * JSON and native socket buffers may each copy the wire bytes; those copies are
 * independently bounded. Source Blob storage is not counted as a JS byte buffer. */
export function createFileOutbox({ socket, clock = () => performance.now(), timeoutMs = 90_000,
  tickMs = 25, bytesPerSecond = 4_000_000, maxBytes = maxPendingFileBytes,
  maxEntries = maxPendingFileChunks, socketBytes = maxSocketBufferedBytes } = {}) {
  const entries = new Map();
  let reservedBytes = 0, closed = false, timer = null, nextSendAt = 0;
  const stats = () => ({ pending: entries.size, reservedBytes, committed: [...entries.values()].filter(item => item.wire !== null).length });
  function finish(item, error) {
    if (entries.get(item.id) !== item) return;
    entries.delete(item.id); reservedBytes -= item.bytes;
    if (error) item.reject(error); else item.resolve();
    if (entries.size === 0 && timer !== null) { clearTimeout(timer); timer = null; }
  }
  function schedule() {
    if (!closed && entries.size > 0 && timer === null) timer = setTimeout(() => { timer = null; pump(); }, tickMs);
  }
  function pump() {
    if (closed) return;
    const now = clock(), ws = socket();
    for (const item of entries.values()) {
      if (now >= item.deadline) { finish(item, new FileTransferError('file_transfer_timeout')); continue; }
      if (item.wire === null || !ws || ws.readyState !== 1 || item.sentSocket === ws || now < nextSendAt) continue;
      if (ws.bufferedAmount + item.bytes > socketBytes) continue;
      try {
        item.sentSocket = ws; ws.send(item.wire);
        nextSendAt = now + Math.ceil(item.bytes / bytesPerSecond * 1000);
      } catch { item.sentSocket = null; /* Retain the same wire identity for retry. */ }
    }
    schedule();
  }
  return Object.freeze({
    reserve(id, bytes) {
      if (closed) throw new FileTransferError('file_transfer_closed');
      if (typeof id !== 'string' || !id || !Number.isSafeInteger(bytes) || bytes < 0 || bytes > maxWireChunkBytes || entries.has(id)) throw new FileTransferError('file_transfer_invalid');
      if (entries.size >= maxEntries || reservedBytes + bytes > maxBytes) throw new FileTransferError('file_transfer_busy');
      let resolve, reject;
      const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
      // A source read may be awaiting I/O when destroy/timeout rejects its ticket.
      promise.catch(() => undefined);
      const item = { id, bytes, wire: null, sentSocket: null, deadline: clock() + timeoutMs, resolve, reject };
      entries.set(id, item); reservedBytes += bytes; schedule();
      return Object.freeze({
        send(wire) {
          if (entries.get(id) !== item) return promise;
          const bytes = typeof wire === 'string' ? wireBytes(wire) : -1;
          if (bytes < 0 || item.wire !== null || bytes > item.bytes) {
            finish(item, new FileTransferError('file_transfer_invalid')); return promise;
          }
          reservedBytes -= item.bytes - bytes; item.bytes = bytes; item.wire = wire;
          pump(); return promise;
        },
        cancel(error = new FileTransferError('file_transfer_closed')) { finish(item, error); },
      });
    },
    ack(id) { const item = entries.get(id); if (item?.wire !== null && item?.sentSocket) finish(item); pump(); },
    reject(id, code) { const item = entries.get(id); if (item) finish(item, new FileTransferError(code)); pump(); },
    cancelFile(fileId) {
      const prefix = fileId + '_';
      for (const item of entries.values()) if (item.id.startsWith(prefix) && /^\d+$/u.test(item.id.slice(prefix.length))) {
        finish(item, new FileTransferError('file_transfer_cancelled', fileId));
      }
    },
    pump,
    stats,
    close() {
      closed = true;
      if (timer !== null) clearTimeout(timer);
      timer = null;
      for (const item of entries.values()) finish(item, new FileTransferError('file_transfer_closed'));
    },
  });
}

/** Preserve existing chunk wire format while rejecting inconsistent/malicious
 * declarations before retaining/decrypting their payload. */
export function validChunk(file) {
  return file && typeof file.fileId === 'string' && /^[A-Za-z0-9_-]{1,120}$/u.test(file.fileId)
    && Number.isSafeInteger(file.index) && Number.isSafeInteger(file.total) && file.total >= 1 && file.total <= 8192
    && file.index >= 0 && file.index < file.total && Number.isSafeInteger(file.totalBytes) && file.totalBytes >= 0 && file.totalBytes <= maxFileBytes
    && Number.isSafeInteger(file.bytes) && file.bytes >= 0 && file.bytes <= 512_000
    && typeof file.nonce === 'string' && file.nonce.length <= 64 && typeof file.ciphertext === 'string' && file.ciphertext.length <= maxWireChunkBytes
    && (file.metaCiphertext === undefined || (typeof file.metaCiphertext === 'string' && file.metaCiphertext.length <= 20_000));
}

export function ciphertextMatchesBytes(value, bytes) {
  if (typeof value !== 'string' || !Number.isSafeInteger(bytes) || bytes < 0
    || value.length > Math.ceil((bytes + 16) / 3) * 4 || value.length < Math.floor((bytes + 16) * 4 / 3)) return false;
  const padding = value.endsWith('==') ? 2 : value.endsWith('=') ? 1 : 0;
  return Math.floor(value.length * 3 / 4) - padding === bytes + 16 && /^[A-Za-z0-9+/_-]+={0,2}$/u.test(value);
}
