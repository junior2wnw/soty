import { check, document, hash, id } from './validation.mjs';

const CAPABILITY_DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const DESCRIPTOR_KEYS = ['projectId', 'sourceStoreId', 'notesStoreId', 'invocationId', 'accountId',
  'noteId', 'mutationId', 'inputDigest', 'capabilityDigest'];
const plain = value => value && typeof value === 'object' && !Array.isArray(value)
  && [null, Object.prototype].includes(Object.getPrototypeOf(value));
function exact(value, keys) {
  check(plain(value) && Object.keys(value).every(key => keys.includes(key)));
  for (const key of Reflect.ownKeys(value)) check(keys.includes(key)
    && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'));
}
function inputValue(input) {
  exact(input, ['title', 'body']);
  check(Object.hasOwn(input, 'title') && Object.hasOwn(input, 'body')
    && typeof input.title === 'string' && typeof input.body === 'string');
  check(input.title.isWellFormed() && input.body.isWellFormed(), 'invalid_unicode');
  return { title: input.title, body: input.body };
}

// This port is local host composition. It is deliberately absent from Notes RPC operations.
export function createNativeNotesPort({ db, projectId, clock, limits, verifyNativeContext,
  transaction, put, ensureOpen }) {
  function storageIdentity() {
    ensureOpen();
    check(typeof verifyNativeContext === 'function', 'native_unavailable');
    const version = db.prepare('PRAGMA user_version').get().user_version;
    check(version === 2, 'native_unavailable');
    const metadata = db.prepare('SELECT key,value FROM notes_meta ORDER BY key').all();
    const values = Object.fromEntries(metadata.map(row => [row.key, row.value]));
    check(metadata.length === 3 && values.lineage === 'soty.notes.sqlite.v2'
      && typeof values.registry_id === 'string' && /^[a-f0-9]{32}$/u.test(values.registry_id), 'notes_storage_corrupt');
    check(values.project_id === projectId, 'native_store_mismatch');
    return Object.freeze({ projectId, registryId: values.registry_id, schemaVersion: 2 });
  }
  function validateDraftInput(args) {
    ensureOpen(); exact(args, ['input']);
    const input = inputValue(args.input);
    const doc = document({ ...input, items: [], color: 'plain', pinned: false, state: 'active' }, limits);
    return Object.freeze({ documentBytes: doc.bytes });
  }
  function verify(context, mode) {
    ensureOpen();
    check(typeof verifyNativeContext === 'function', 'native_unavailable');
    const descriptor = verifyNativeContext(context, mode);
    if (descriptor && typeof descriptor.then === 'function') {
      Promise.resolve(descriptor).catch(() => {});
      check(false, 'native_context_invalid');
    }
    check(plain(descriptor) && Object.isFrozen(descriptor)
      && Object.keys(descriptor).sort().join(',') === [...DESCRIPTOR_KEYS].sort().join(','), 'native_context_invalid');
    for (const key of DESCRIPTOR_KEYS) check(Object.hasOwn(Object.getOwnPropertyDescriptor(descriptor, key), 'value')
      && typeof descriptor[key] === 'string', 'native_context_invalid');
    check(descriptor.projectId === projectId && descriptor.notesStoreId === storageIdentity().registryId, 'native_store_mismatch');
    check(/^[a-f0-9]{32}$/u.test(descriptor.sourceStoreId)
      && /^[a-f0-9]{64}$/u.test(descriptor.inputDigest)
      && descriptor.capabilityDigest === CAPABILITY_DIGEST, 'native_context_invalid');
    id(descriptor.accountId);
    check(/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/u.test(descriptor.invocationId)
      && /^n_[a-f0-9]{64}$/u.test(descriptor.noteId)
      && /^m_[a-f0-9]{64}$/u.test(descriptor.mutationId), 'native_context_invalid');
    return descriptor;
  }
  function proof(descriptor) {
    const row = db.prepare('SELECT * FROM note_native_creates WHERE source_store_id=? AND invocation_id=?')
      .get(descriptor.sourceStoreId, descriptor.invocationId);
    if (!row) return null;
    check(row.account_id === descriptor.accountId && row.note_id === descriptor.noteId
      && row.mutation_id === descriptor.mutationId && row.input_digest === descriptor.inputDigest
      && row.capability_digest === descriptor.capabilityDigest && row.revision === 1
      && Number.isSafeInteger(row.created_at) && row.created_at >= 0, 'native_proof_invalid');
    return Object.freeze({ ...descriptor, revision: 1, createdAt: row.created_at });
  }
  function readCreateProof(args) {
    exact(args, ['context']);
    // Check before entering a Notes transaction, then again inside its snapshot.
    verify(args.context, 'reconcile');
    return transaction(() => {
      const descriptor = verify(args.context, 'reconcile');
      const result = proof(descriptor);
      verify(args.context, 'reconcile');
      return result;
    }, { write: false, busyMs: 100 });
  }
  function createDraftForInvocation(args) {
    exact(args, ['context', 'input']);
    const input = inputValue(args.input);
    const inputDigest = hash(JSON.stringify({ body: input.body, title: input.title }));
    check(inputDigest === verify(args.context, 'create').inputDigest, 'native_context_invalid');
    return transaction(() => {
      const descriptor = verify(args.context, 'create');
      check(inputDigest === descriptor.inputDigest, 'native_context_invalid');
      const existing = proof(descriptor);
      if (existing) { verify(args.context, 'create'); return existing; }
      check(!db.prepare('SELECT 1 FROM notes WHERE account_id=? AND id=?').get(descriptor.accountId, descriptor.noteId)
        && !db.prepare('SELECT 1 FROM note_receipts WHERE account_id=? AND mutation_id=? LIMIT 1')
          .get(descriptor.accountId, descriptor.mutationId), 'native_identity_conflict');
      validateDraftInput({ input });
      const timestamp = clock(); check(Number.isSafeInteger(timestamp) && timestamp >= 0, 'clock_invalid');
      const result = put({ noteId: descriptor.noteId, mutationId: descriptor.mutationId, expectedRevision: 0,
        ...input, items: [], color: 'plain', pinned: false, state: 'active' }, descriptor.accountId, timestamp);
      check(result.noteId === descriptor.noteId && result.revision === 1, 'native_proof_invalid');
      db.prepare(`INSERT INTO note_native_creates(source_store_id,invocation_id,account_id,note_id,mutation_id,
        input_digest,capability_digest,revision,created_at) VALUES(?,?,?,?,?,?,?,1,?)`)
        .run(descriptor.sourceStoreId, descriptor.invocationId, descriptor.accountId, descriptor.noteId,
          descriptor.mutationId, descriptor.inputDigest, descriptor.capabilityDigest, timestamp);
      const saved = proof(descriptor);
      // This is the last authority check before the single Notes effect/proof COMMIT.
      verify(args.context, 'create');
      return saved;
    }, { write: true, busyMs: 100 });
  }
  return Object.freeze({ storageIdentity, validateDraftInput, readCreateProof, createDraftForInvocation });
}
