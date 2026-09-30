import { FileTransferError } from './file-transfer.mjs';

export const maxMemoryFileBytes = 16_000_000;
export const maxMemoryReceiveBytes = 32_000_000;
const cacheName = 'soty-file-cache-v2';
const validId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,120}$/u.test(value);
const storageError = error => error instanceof FileTransferError ? error
  : new FileTransferError(error?.name === 'QuotaExceededError' ? 'file_storage_capacity' : 'file_storage_failed');

/**
 * A lifetime-scoped receive cache. Modern browsers use OPFS files, so assembled
 * downloads stay disk-backed. The bounded memory fallback never accepts a big
 * file. Plaintext is local to this origin; it is not uploaded or a durable user
 * document. A browser lock protects live tabs from abandoned-cache cleanup.
 */
export function createFileReceiveStore({ storage = globalThis.navigator?.storage, locks = globalThis.navigator?.locks } = {}) {
  const files = new Map(), operations = new Set();
  let closed = false, backendPromise = null, releaseLock = null, lockTask = null, reservedMemory = 0;
  async function backend() {
    if (backendPromise) return backendPromise;
    backendPromise = (async () => {
      if (!storage?.getDirectory || !locks?.request) return null;
      const sessionId = crypto.randomUUID();
      let grant, deny;
      const granted = new Promise((yes, no) => { grant = yes; deny = no; });
      const released = new Promise(yes => { releaseLock = yes; });
      lockTask = locks.request(cacheName + ':' + sessionId, async () => { grant(); await released; }).catch(deny);
      await granted;
      if (closed) { releaseLock(); return null; }
      try {
        const root = await (await storage.getDirectory()).getDirectoryHandle(cacheName, { create: true });
        // Only expired session directories, never another tab's active data.
        for await (const name of root.keys()) {
          if (!/^[a-f0-9-]{36}$/u.test(name) || name === sessionId) continue;
          await locks.request(cacheName + ':' + name, { ifAvailable: true }, async lock => {
            if (lock) await root.removeEntry(name, { recursive: true });
          });
        }
        const directory = await root.getDirectoryHandle(sessionId, { create: true });
        return { root, directory, sessionId };
      } catch (error) { releaseLock(); throw storageError(error); }
    })();
    return backendPromise;
  }
  function run(operation) {
    if (closed) return Promise.reject(new FileTransferError('file_storage_closed'));
    if (operations.size >= 4) return Promise.reject(new FileTransferError('file_transfer_busy'));
    const pending = operation().catch(error => { throw storageError(error); });
    operations.add(pending); pending.then(() => operations.delete(pending), () => operations.delete(pending));
    return pending;
  }
  return Object.freeze({
    create(fileId, totalBytes, totalChunks) {
      if (closed) throw new FileTransferError('file_storage_closed');
      if (!validId(fileId) || !Number.isSafeInteger(totalBytes) || totalBytes < 0 || !Number.isSafeInteger(totalChunks)
        || totalChunks < 1 || totalChunks > 8192 || files.has(fileId)) throw new FileTransferError('file_transfer_invalid');
      const entry = { sizes: new Map(), pending: new Set(), pendingBytes: 0, bytes: 0, memory: null, directory: null, initializing: null, finished: false, removed: false };
      files.set(fileId, entry);
      async function initialize() {
        if (entry.initializing) return entry.initializing;
        entry.initializing = (async () => {
          const target = await backend();
          if (closed || entry.removed) throw new FileTransferError('file_storage_closed');
          if (!target) {
            if (totalBytes > maxMemoryFileBytes || reservedMemory + totalBytes > maxMemoryReceiveBytes) throw new FileTransferError('file_storage_unavailable');
            reservedMemory += totalBytes; entry.memory = new Map(); return;
          }
          const estimate = await storage.estimate?.();
          // Parts and the assembled file temporarily coexist. The browser may
          // still reject later writes if another tab consumes the free quota.
          if (estimate?.quota && estimate.quota - (estimate.usage ?? 0) < totalBytes * 2 + 1_000_000) throw new FileTransferError('file_storage_capacity');
          entry.directory = await target.directory.getDirectoryHandle(fileId, { create: true });
        })();
        return entry.initializing;
      }
      return Object.freeze({
        put(index, bytes) {
          return run(async () => {
            if (entry.removed || entry.finished || !Number.isSafeInteger(index) || index < 0 || index >= totalChunks
              || !(bytes instanceof Uint8Array) || bytes.byteLength > 512_000 || entry.sizes.has(index) || entry.pending.has(index)
              || entry.bytes + entry.pendingBytes + bytes.byteLength > totalBytes) throw new FileTransferError('file_transfer_invalid');
            entry.pending.add(index); entry.pendingBytes += bytes.byteLength;
            try {
              await initialize();
              if (entry.removed || closed) throw new FileTransferError('file_storage_closed');
              if (entry.memory) entry.memory.set(index, new Blob([bytes]));
              else {
                const handle = await entry.directory.getFileHandle('chunk-' + index, { create: true });
                const writer = await handle.createWritable();
                try { await writer.write(bytes); await writer.close(); }
                catch (error) { await writer.abort().catch(() => undefined); throw error; }
              }
              entry.sizes.set(index, bytes.byteLength); entry.bytes += bytes.byteLength;
            } finally { entry.pending.delete(index); entry.pendingBytes -= bytes.byteLength; }
          });
        },
        finish(type = 'application/octet-stream') {
          return run(async () => {
            if (entry.removed || entry.finished || entry.sizes.size !== totalChunks || entry.bytes !== totalBytes) throw new FileTransferError('file_transfer_invalid');
            await initialize();
            if (entry.memory) {
              const blob = new Blob(Array.from({ length: totalChunks }, (_, index) => entry.memory.get(index)), { type });
              entry.finished = true; entry.memory.clear(); return blob;
            }
            const handle = await entry.directory.getFileHandle('download', { create: true }), writer = await handle.createWritable();
            try {
              // Pass the disk-backed File directly to the browser writer.
              // No second JS ArrayBuffer copy of every received chunk.
              for (let index = 0; index < totalChunks; index++) {
                if (closed || entry.removed) throw new FileTransferError('file_storage_closed');
                const part = await (await entry.directory.getFileHandle('chunk-' + index)).getFile();
                if (part.size !== entry.sizes.get(index)) throw new FileTransferError('file_transfer_invalid');
                await writer.write(part);
              }
              await writer.close();
            } catch (error) { await writer.abort().catch(() => undefined); throw error; }
            const file = await handle.getFile();
            if (file.size !== totalBytes) throw new FileTransferError('file_transfer_invalid');
            for (let index = 0; index < totalChunks; index++) await entry.directory.removeEntry('chunk-' + index);
            entry.finished = true;
            return file;
          });
        },
      });
    },
    async remove(fileId) {
      const entry = files.get(fileId); if (!entry) return;
      entry.removed = true;
      // Await only the bounded currently executing operations, not producers.
      await Promise.allSettled([...operations]);
      files.delete(fileId);
      if (entry.memory) entry.memory.clear();
      // Reservation is conservative until this store closes: returned Blob
      // references may still be held by the UI after an individual deletion.
      const target = await backendPromise?.catch(() => null);
      if (target && entry.directory) await target.directory.removeEntry(fileId, { recursive: true }).catch(() => undefined);
    },
    stats() { return { files: files.size, operations: operations.size, reservedMemory }; },
    async close() {
      if (closed) return;
      closed = true;
      await Promise.allSettled([...operations]);
      const target = await backendPromise?.catch(() => null);
      if (target) await target.root.removeEntry(target.sessionId, { recursive: true }).catch(() => undefined);
      files.clear(); reservedMemory = 0; releaseLock?.(); await lockTask;
    },
  });
}
