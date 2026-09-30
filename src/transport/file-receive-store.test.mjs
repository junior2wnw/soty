import test from 'node:test';
import assert from 'node:assert/strict';
import { createFileReceiveStore, maxMemoryFileBytes } from './file-receive-store.mjs';

test('memory fallback admits only small files, assembles ordered bytes and rejects duplicate/missing parts', async () => {
  const store = createFileReceiveStore({ storage: {}, locks: {} });
  const spool = store.create('small', 5, 2);
  await spool.put(1, new Uint8Array([4, 5]));
  await assert.rejects(spool.finish(), /file_transfer_invalid/u);
  await assert.rejects(spool.put(1, new Uint8Array([4, 5])), /file_transfer_invalid/u);
  await spool.put(0, new Uint8Array([1, 2, 3]));
  assert.deepEqual(new Uint8Array(await (await spool.finish()).arrayBuffer()), new Uint8Array([1, 2, 3, 4, 5]));
  const big = store.create('large', maxMemoryFileBytes + 1, 100);
  await assert.rejects(big.put(0, new Uint8Array([1])), /file_storage_unavailable/u);
  await store.close();
});

test('OPFS backend keeps files on storage, cleans abandoned caches, protects active cache and removes only its own lifetime', async () => {
  const directories = new Map(), held = new Set();
  class Directory {
    constructor(name) { this.name = name; this.entries = new Map(); directories.set(name, this); }
    async *keys() { yield* this.entries.keys(); }
    async getDirectoryHandle(name, options) {
      let value = this.entries.get(name);
      if (!value && options?.create) { value = new Directory(this.name + '/' + name); this.entries.set(name, value); }
      if (!value) throw new Error('missing'); return value;
    }
    async getFileHandle(name, options) {
      let blob = this.entries.get(name);
      if (!blob && !options?.create) throw new Error('missing');
      const parent = this;
      return { getFile: async () => parent.entries.get(name),
        createWritable: async () => {
          const parts = [];
          return { write: async bytes => { parts.push(new Blob([bytes])); }, abort: async () => {},
            close: async () => { blob = new Blob(parts); parent.entries.set(name, blob); } };
        } };
    }
    async removeEntry(name) { this.entries.delete(name); }
  }
  const root = new Directory('root'), cache = await root.getDirectoryHandle('soty-file-cache-v2', { create: true });
  const stale = '11111111-1111-1111-1111-111111111111', active = '22222222-2222-2222-2222-222222222222';
  await cache.getDirectoryHandle(stale, { create: true }); await cache.getDirectoryHandle(active, { create: true });
  held.add('soty-file-cache-v2:' + active);
  const locks = { async request(name, options, callback) {
    if (typeof options === 'function') { callback = options; options = {}; }
    if (held.has(name)) { assert.equal(options.ifAvailable, true); return callback(null); }
    held.add(name); try { return await callback({ name }); } finally { held.delete(name); }
  } };
  const store = createFileReceiveStore({ storage: { getDirectory: async () => root, estimate: async () => ({ quota: 1e9, usage: 0 }) }, locks });
  const spool = store.create('disk', 5, 2);
  await spool.put(1, new Uint8Array([4, 5])); await spool.put(0, new Uint8Array([1, 2, 3]));
  assert.deepEqual(new Uint8Array(await (await spool.finish()).arrayBuffer()), new Uint8Array([1, 2, 3, 4, 5]));
  assert.equal(store.stats().reservedMemory, 0);
  assert.equal(cache.entries.has(stale), false); assert.equal(cache.entries.has(active), true);
  assert.equal(cache.entries.size, 2);
  await store.close(); assert.deepEqual([...cache.entries.keys()], [active]);
});

test('OPFS quota failure is explicit, and concurrent spool calls cannot enqueue unlimited plaintext', async () => {
  let release; const waiting = new Promise(done => { release = done; });
  const store = createFileReceiveStore({ storage: { getDirectory: async () => { await waiting; throw new Error('no storage'); } },
    locks: { request: async (_name, callback) => callback() } });
  const spool = store.create('blocked', 8, 8), pending = [];
  for (let n = 0; n < 4; n++) pending.push(spool.put(n, new Uint8Array([n])));
  await assert.rejects(spool.put(4, new Uint8Array([4])), /file_transfer_busy/u);
  assert.equal(store.stats().operations, 4);
  release(); assert.equal((await Promise.allSettled(pending)).filter(value => value.status === 'rejected').length, 4);
  await store.close();
});
