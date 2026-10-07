import { DatabaseSync } from 'node:sqlite';
import { existsSync, lstatSync, mkdirSync } from 'node:fs';
import { dirname, isAbsolute } from 'node:path';
import { types } from 'node:util';
import { SCHEMA, SQL, assertSchema, isEmptyDatabase } from './schema.mjs';
import { MemoryError, DEFAULT_LIMITS, check, fields, text, id, integer, canonicalScope, limits, record, recordRef, digest, queryTerms } from './validation.mjs';
export { MemoryError, DEFAULT_LIMITS } from './validation.mjs';
export { SCHEMA } from './schema.mjs';

const OPERATIONS = Object.freeze(['open', 'remember', 'recall', 'supersede', 'delete', 'export']);
const SAFE_ENCODER_CODES = new Set(['memory_access_denied', 'memory_context_stale', 'memory_restore_floor_mismatch',
  'memory_restore_authority_unavailable', 'memory_closed', 'memory_cancelled', 'memory_embedding_timeout',
  'memory_embedding_invalid', 'memory_embedding_failed', 'memory_invalid_arguments']);
function consumeUnexpectedPromise(value) {
  if (types.isPromise(value)) {
    // A miswired trusted adapter must not bring down its host with a late rejection.
    Promise.prototype.then.call(value, () => {}, () => {});
    return true;
  }
  return false;
}
const publicRecord = row => ({ id: row.id, revision: row.revision, type: row.kind, text: row.text,
  source: row.source, importance: row.importance, confidence: row.confidence,
  freshUntil: row.fresh_until, retainUntil: row.retain_until, createdAt: row.created_at });
function bytes(values) { return Buffer.from(Float32Array.from(values).buffer); }
function cosine(left, right) {
  if (!left || !right || left.byteLength !== right.byteLength || left.byteLength % 4) return 0;
  const a = new Float32Array(Uint8Array.from(left).buffer), b = new Float32Array(Uint8Array.from(right).buffer);
  let dot = 0, aa = 0, bb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; aa += a[i] ** 2; bb += b[i] ** 2; }
  return aa && bb ? dot / Math.sqrt(aa * bb) : 0;
}
function embeddingConfig(value) {
  if (value === undefined) return null;
  fields(value, ['id', 'dimensions', 'embed']);
  check(typeof value.embed === 'function');
  return Object.freeze({ id: text(value.id, 512), dimensions: integer(value.dimensions, 1, 4096), embed: value.embed.bind(value) });
}
function capturedLease(value, now) {
  try {
    fields(value, ['leaseId', 'epoch', 'expiresAt']);
    const lease = { leaseId: id(value.leaseId), epoch: integer(value.epoch), expiresAt: integer(value.expiresAt) };
    check(lease.expiresAt > now, 'memory_access_denied'); return Object.freeze(lease);
  } catch { throw new MemoryError('memory_access_denied'); }
}

/** Host-only partition. Context, path, scope, floor and admission callbacks MUST be controller-owned. */
export function openMemoryPartition({ databasePath, scope: inputScope, context: openingContext,
  verifyContext, readRestoreFloor, advanceRestoreFloor, embedding, clock = Date.now, limits: overrides } = {}) {
  const scope = canonicalScope(inputScope), scopeJson = JSON.stringify(scope), partitionId = digest(scope), bounds = limits(overrides);
  check(typeof clock === 'function');
  for (const fn of [verifyContext, readRestoreFloor, advanceRestoreFloor])
    check(typeof fn === 'function' && fn.constructor?.name !== 'AsyncFunction', 'memory_trusted_provider_required');
  check(databasePath === ':memory:' || (typeof databasePath === 'string' && isAbsolute(databasePath)), 'memory_controller_path_required');
  const encoder = embeddingConfig(embedding), encoderId = encoder?.id ?? '', dimensions = encoder?.dimensions ?? 0;
  let db, closed = false, inTransaction = false;
  const pending = new Set();
  function now() { return integer(clock()); }
  function admission(context, operation, previous, signal) {
    check(!closed, 'memory_closed');
    check(OPERATIONS.includes(operation));
    if (signal?.aborted) throw new MemoryError('memory_cancelled');
    let value;
    try { value = verifyContext(Object.freeze({ context, scope, operation, partitionId })); }
    catch { throw new MemoryError('memory_access_denied'); }
    check(!consumeUnexpectedPromise(value), 'memory_access_denied');
    const lease = capturedLease(value, now());
    check(!previous || (lease.leaseId === previous.leaseId && lease.epoch === previous.epoch), 'memory_context_stale');
    return lease;
  }
  function externalFloor() {
    try {
      const value = readRestoreFloor(Object.freeze({ scope, partitionId }));
      check(!consumeUnexpectedPromise(value)); return integer(value);
    }
    catch { throw new MemoryError('memory_restore_authority_unavailable'); }
  }
  function localFloor() {
    const value = db.prepare("SELECT value FROM memory_metadata WHERE key='restore_floor'").get()?.value;
    check(typeof value === 'string' && /^(0|[1-9]\d{0,15})$/u.test(value), 'memory_restore_floor_mismatch');
    return integer(Number(value));
  }
  function guard(context, operation, previous, signal) {
    const lease = admission(context, operation, previous, signal), floor = externalFloor(), stored = localFloor();
    check(floor === stored, 'memory_restore_floor_mismatch');
    check(previous?.restoreFloor === undefined || previous.restoreFloor === floor, 'memory_context_stale');
    return Object.freeze({ ...lease, restoreFloor: floor });
  }
  const firstLease = admission(openingContext, 'open');
  const firstFloor = externalFloor();
  if (databasePath !== ':memory:') {
    mkdirSync(dirname(databasePath), { recursive: true, mode: 0o700 });
    check(!existsSync(databasePath) || (lstatSync(databasePath).isFile() && !lstatSync(databasePath).isSymbolicLink()), 'memory_controller_path_required');
  }
  try {
    db = new DatabaseSync(databasePath);
    db.exec('PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
    if (isEmptyDatabase(db)) {
      db.exec('BEGIN IMMEDIATE');
      try {
        // Another process may have initialized the same controller-owned file while BEGIN waited.
        if (isEmptyDatabase(db)) {
          check(firstFloor === 0, 'memory_restore_reconciliation_required');
          admission(openingContext, 'open', firstLease);
          check(externalFloor() === firstFloor, 'memory_context_stale');
          db.exec(SQL);
          const insert = db.prepare('INSERT INTO memory_metadata(key,value) VALUES(?,?)');
          for (const [key, value] of [['schema', SCHEMA], ['scope', scopeJson], ['partition_id', partitionId],
            ['restore_floor', String(firstFloor)], ['embedding_id', encoderId], ['embedding_dimensions', String(dimensions)]]) insert.run(key, value);
          admission(openingContext, 'open', firstLease);
          check(externalFloor() === firstFloor, 'memory_context_stale');
        }
        db.exec('COMMIT');
      } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    }
    assertSchema(db);
    const metadata = Object.fromEntries(db.prepare('SELECT key,value FROM memory_metadata').all().map(row => [row.key, row.value]));
    check(Object.keys(metadata).length === 6 && metadata.schema === SCHEMA && metadata.scope === scopeJson
      && metadata.partition_id === partitionId, 'memory_partition_mismatch');
    check(metadata.embedding_id === encoderId && metadata.embedding_dimensions === String(dimensions), 'memory_embedding_mismatch');
    guard(openingContext, 'open', { ...firstLease, restoreFloor: firstFloor });
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
    db.function('memory_cosine', { deterministic: true }, cosine);
  } catch (error) { if (db) { try { db.close(); } catch { /* Original rejection is authoritative. */ } } closed = true; throw error; }

  function argumentsFor(value, required, optional = []) {
    fields(value, ['context', ...required], ['signal', ...optional]);
    check(value.signal === undefined || value.signal instanceof AbortSignal);
    return Object.freeze({ ...value });
  }
  function replay(mutationId, intent) {
    const found = db.prepare('SELECT intent_digest,result_json FROM memory_receipts WHERE mutation_id=?').get(mutationId);
    if (!found) return null;
    check(found.intent_digest === intent, 'memory_mutation_reused');
    return { ...JSON.parse(found.result_json), replayed: true };
  }
  function knownId(recordId) {
    check(!db.prepare('SELECT id FROM memory_records WHERE id=? UNION ALL SELECT id FROM memory_tombstones WHERE id=?').get(recordId, recordId),
      'memory_record_exists');
  }
  function requireVersion(reference) {
    const found = db.prepare('SELECT revision FROM memory_records WHERE id=?').get(reference.id);
    check(found?.revision === reference.expectedRevision, 'memory_revision_conflict');
  }
  function capacity(newRecord, erased = []) {
    const current = db.prepare('SELECT count(*) AS count,coalesce(sum(content_bytes),0) AS bytes FROM memory_records').get();
    let erasedBytes = 0;
    for (const reference of erased) erasedBytes += db.prepare('SELECT content_bytes FROM memory_records WHERE id=?').get(reference.id)?.content_bytes ?? 0;
    const receipts = db.prepare('SELECT count(*) AS count FROM memory_receipts').get().count;
    const tombstones = db.prepare('SELECT count(*) AS count FROM memory_tombstones').get().count;
    if (newRecord) {
      const remaining = current.count - erased.length + 1;
      check(remaining <= bounds.records && current.bytes - erasedBytes + Buffer.byteLength(JSON.stringify(newRecord)) <= bounds.contentBytes, 'memory_capacity');
      // Every admitted live record reserves both an erase receipt and a tombstone.
      check(receipts + 1 + remaining <= bounds.receipts, 'memory_receipt_capacity');
      check(tombstones + erased.length + remaining <= bounds.tombstones, 'memory_tombstone_capacity');
    } else {
      // Lowering admission quotas never prevents erasing previously admitted records.
      check(receipts + 1 <= DEFAULT_LIMITS.receipts, 'memory_receipt_capacity');
      check(tombstones + erased.length <= DEFAULT_LIMITS.tombstones, 'memory_tombstone_capacity');
    }
  }
  function insertRecord(entry, vector) {
    db.prepare('INSERT INTO memory_records VALUES(?,1,?,?,?,?,?,?,?,?,?,?)').run(entry.id, entry.type, entry.text, entry.source,
      entry.importance, entry.confidence, entry.freshUntil, entry.retainUntil, now(), Buffer.byteLength(JSON.stringify(entry)), vector);
    db.prepare('INSERT INTO memory_search(id,text) VALUES(?,?)').run(entry.id, entry.text);
  }
  function eraseRecords(references, mutationId, restoreFloor) {
    for (const reference of references) {
      db.prepare('DELETE FROM memory_search WHERE id=?').run(reference.id);
      db.prepare('DELETE FROM memory_records WHERE id=? AND revision=?').run(reference.id, reference.expectedRevision);
      db.prepare('INSERT INTO memory_tombstones VALUES(?,?,?,?,?)').run(reference.id, reference.expectedRevision + 1, now(), restoreFloor, mutationId);
    }
  }
  function advanceFloor(frame, mutationId, references) {
    let next;
    try { next = advanceRestoreFloor(Object.freeze({ scope, partitionId, expectedFloor: frame.restoreFloor,
      mutationId, erasedIds: Object.freeze(references.map(ref => ref.id)) })); }
    catch { throw new MemoryError('memory_restore_authority_unavailable'); }
    check(!consumeUnexpectedPromise(next), 'memory_restore_authority_unavailable');
    check(Number.isSafeInteger(next) && next === frame.restoreFloor + 1 && externalFloor() === next,
      'memory_restore_authority_unavailable');
    db.prepare("UPDATE memory_metadata SET value=? WHERE key='restore_floor'").run(String(next));
    return Object.freeze({ ...frame, restoreFloor: next });
  }
  function mutate(args, operation, intent, entry, references, vector, frame) {
    check(!inTransaction && !db.isTransaction, 'memory_nested_transaction');
    guard(args.context, operation, frame, args.signal);
    db.exec('BEGIN IMMEDIATE'); inTransaction = true;
    try {
      let current = guard(args.context, operation, frame, args.signal);
      const old = replay(args.mutationId, intent);
      if (old) { guard(args.context, operation, current, args.signal); db.exec('COMMIT'); return old; }
      for (const reference of references) requireVersion(reference);
      if (entry) { check(entry.retainUntil > now(), 'memory_retention_invalid'); knownId(entry.id); }
      capacity(entry, references);
      if (references.length) current = advanceFloor(current, args.mutationId, references);
      eraseRecords(references, args.mutationId, current.restoreFloor);
      if (entry) insertRecord(entry, vector);
      const result = operation === 'delete'
        ? { id: references[0].id, revision: references[0].expectedRevision + 1, erased: true, restoreFloor: current.restoreFloor }
        : { id: entry.id, revision: 1, replaced: references.map(ref => ({ id: ref.id, revision: ref.expectedRevision + 1 })), restoreFloor: current.restoreFloor };
      db.prepare('INSERT INTO memory_receipts VALUES(?,?,?,?)').run(args.mutationId, intent, JSON.stringify(result), now());
      guard(args.context, operation, current, args.signal);
      db.exec('COMMIT');
      // A controller can change its live context even after commit; private delivery must still pass.
      guard(args.context, operation, current, args.signal);
      return { ...result, replayed: false };
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    finally { inTransaction = false; }
  }
  async function encode(value, purpose, args, frame) {
    if (!encoder) return null;
    guard(args.context, purpose === 'query' ? 'recall' : (args.operation ?? 'remember'), frame, args.signal);
    check(pending.size < bounds.embeddingConcurrency, 'memory_busy');
    const controller = new AbortController(); pending.add(controller);
    const signals = [controller.signal, ...(args.signal ? [args.signal] : [])], signal = AbortSignal.any(signals);
    const timer = setTimeout(() => controller.abort(new MemoryError('memory_embedding_timeout')), bounds.embeddingTimeoutMs);
    let removeListener = () => {};
    const cancelled = new Promise((resolve, reject) => {
      const abort = () => reject(new MemoryError(closed ? 'memory_closed' : (args.signal?.aborted ? 'memory_cancelled' : 'memory_embedding_timeout')));
      if (signal.aborted) abort(); else { signal.addEventListener('abort', abort, { once: true }); removeListener = () => signal.removeEventListener('abort', abort); }
    });
    const work = Promise.resolve().then(async () => {
      guard(args.context, purpose === 'query' ? 'recall' : (args.operation ?? 'remember'), frame, signal);
      try { return await encoder.embed(value, { purpose, signal }); }
      catch { throw new MemoryError('memory_embedding_failed'); }
    });
    // Ignored abort retains its capacity slot until the actual encoder settles.
    work.then(() => pending.delete(controller), () => pending.delete(controller));
    try {
      const vector = await Promise.race([work, cancelled]);
      check((Array.isArray(vector) || vector instanceof Float32Array) && vector.length === dimensions
        && Array.from(vector).every(n => typeof n === 'number' && Number.isFinite(n) && Math.abs(n) <= 1e20), 'memory_embedding_invalid');
      return bytes(vector);
    } catch (error) {
      if (error instanceof MemoryError && SAFE_ENCODER_CODES.has(error.code)) throw new MemoryError(error.code);
      throw new MemoryError('memory_embedding_failed');
    } finally { clearTimeout(timer); removeListener(); }
  }

  async function remember(value) {
    const args = argumentsFor(value, ['mutationId', 'record']);
    const frame = guard(args.context, 'remember', undefined, args.signal), mutationId = id(args.mutationId), entry = record(args.record, bounds, now());
    const intent = digest({ operation: 'remember', mutationId, record: entry });
    const old = replay(mutationId, intent);
    if (old) { guard(args.context, 'remember', frame, args.signal); return old; }
    check(entry.retainUntil > now(), 'memory_retention_invalid'); knownId(entry.id); capacity(entry);
    const vector = await encode(entry.text, 'record', { ...args, operation: 'remember' }, frame);
    return mutate({ ...args, mutationId }, 'remember', intent, entry, [], vector, frame);
  }
  async function supersede(value) {
    const args = argumentsFor(value, ['mutationId', 'records', 'record']);
    const frame = guard(args.context, 'supersede', undefined, args.signal), mutationId = id(args.mutationId), entry = record(args.record, bounds, now());
    check(Array.isArray(args.records) && args.records.length > 0 && args.records.length <= 20);
    const references = args.records.map(recordRef).sort((a, b) => a.id.localeCompare(b.id));
    check(new Set(references.map(ref => ref.id)).size === references.length && !references.some(ref => ref.id === entry.id));
    const intent = digest({ operation: 'supersede', mutationId, record: entry, records: references });
    const old = replay(mutationId, intent);
    if (old) { guard(args.context, 'supersede', frame, args.signal); return old; }
    for (const reference of references) requireVersion(reference);
    check(entry.retainUntil > now(), 'memory_retention_invalid'); knownId(entry.id); capacity(entry, references);
    const vector = await encode(entry.text, 'record', { ...args, operation: 'supersede' }, frame);
    return mutate({ ...args, mutationId }, 'supersede', intent, entry, references, vector, frame);
  }
  function erase(value) {
    const args = argumentsFor(value, ['mutationId', 'record']);
    const frame = guard(args.context, 'delete', undefined, args.signal), mutationId = id(args.mutationId), reference = recordRef(args.record);
    const intent = digest({ operation: 'delete', mutationId, record: reference });
    return mutate({ ...args, mutationId }, 'delete', intent, null, [reference], null, frame);
  }
  async function recall(value) {
    const args = argumentsFor(value, ['query'], ['limit']);
    const frame = guard(args.context, 'recall', undefined, args.signal), query = text(args.query, 2000);
    const maximum = integer(args.limit ?? 8, 1, bounds.recallLimit);
    const vector = await encode(query, 'query', args, frame);
    guard(args.context, 'recall', frame, args.signal);
    const at = now(), candidates = new Map(), terms = queryTerms(query);
    if (terms) {
      const rows = db.prepare(`SELECT r.*,bm25(memory_search) AS score FROM memory_search JOIN memory_records r ON r.id=memory_search.id
        WHERE memory_search MATCH ? AND r.retain_until>? ORDER BY score,r.id LIMIT ?`).all(terms, at, bounds.candidates);
      rows.forEach((row, rank) => candidates.set(row.id, { row, score: 0.4 / (rank + 1) }));
    }
    if (vector) {
      const rows = db.prepare(`SELECT *,memory_cosine(embedding,?) AS score FROM memory_records
        WHERE retain_until>? AND embedding IS NOT NULL ORDER BY score DESC,id LIMIT ?`).all(vector, at, bounds.candidates);
      rows.filter(row => row.score >= 0.65).forEach((row, rank) => {
        const found = candidates.get(row.id) ?? { row, score: 0 }; found.score += 0.6 / (rank + 1); candidates.set(row.id, found);
      });
    }
    const output = [...candidates.values()].sort((a, b) => b.score - a.score || a.row.id.localeCompare(b.row.id)).slice(0, maximum)
      .map(item => ({ ...publicRecord(item.row), stale: item.row.fresh_until <= at }));
    guard(args.context, 'recall', frame, args.signal);
    return { records: output.filter(entry => entry.retainUntil > now()) };
  }
  function exportMemory(value) {
    const args = argumentsFor(value, []), frame = guard(args.context, 'export', undefined, args.signal), at = now();
    const records = db.prepare('SELECT * FROM memory_records WHERE retain_until>? ORDER BY id').all(at).map(publicRecord);
    const tombstones = db.prepare('SELECT id,revision,erased_at,restore_floor,mutation_id FROM memory_tombstones ORDER BY id').all()
      .map(row => ({ id: row.id, revision: row.revision, erasedAt: row.erased_at, restoreFloor: row.restore_floor, mutationId: row.mutation_id }));
    guard(args.context, 'export', frame, args.signal);
    return { schema: 'soty.personal-memory-export.v1', scope, restoreFloor: frame.restoreFloor, exportedAt: now(),
      records: records.filter(entry => entry.retainUntil > now()), tombstones };
  }
  function close() {
    if (closed) return;
    check(!inTransaction && !db.isTransaction, 'memory_nested_transaction');
    closed = true;
    for (const controller of pending) controller.abort(new MemoryError('memory_closed'));
    db.close();
  }
  return Object.freeze({ remember, supersede, delete: erase, recall, export: exportMemory, close });
}
