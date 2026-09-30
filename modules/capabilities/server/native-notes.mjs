import { AccessError, assert, canonicalHash, canonicalJson, exact, integer, now } from './validation.mjs';
import { NotesError } from '../../notes/server/validation.mjs';
import { nativeNoteRequestDigest } from './native-note-contract.mjs';

const ID = 'notes.createDraft', VERSION = 1;
const DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const TARGET = Object.freeze({ kind: 'native', handler: ID, version: VERSION });
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const AUTH_DENIALS = new Set(['authorization_required', 'access_denied', 'invocation_dispatch_denied']);
const NOTES_REFUSALS = new Set(['notes_count_quota', 'notes_identity_quota', 'notes_storage_quota', 'notes_note_too_large']);
const DEFAULT_LIMITS = Object.freeze({ nonterminalPerPrincipal: 4, nonterminalPerAccount: 16, nonterminalTotal: 128,
  admissionsPerPrincipal: 10, admissionsPerAccount: 30, identitiesPerAccount: 10000, identitiesTotal: 100000, recoveryPageSize: 16 });
const DESCRIPTOR_KEYS = ['projectId', 'sourceStoreId', 'notesStoreId', 'invocationId', 'accountId',
  'noteId', 'mutationId', 'inputDigest', 'capabilityDigest'];
const thenable = value => value && typeof value.then === 'function';
const asyncFunction = fn => fn?.constructor?.name === 'AsyncFunction';
const fail = code => { throw new AccessError(code); };
function synchronous(value, code) {
  if (thenable(value)) {
    // Contain a wiring error without leaving a rejected Promise unobserved.
    // Any late work sees an invalidated context/frame; it is never awaited here.
    Promise.resolve(value).catch(() => {});
    fail(code);
  }
  return value;
}
const invocationId = value => { assert(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/u.test(value), 'invocation_invalid_arguments'); return value; };
function dataObject(value, keys, code = 'invalid_input') {
  exact(value, keys, code);
  for (const key of Reflect.ownKeys(value)) assert(keys.includes(key)
    && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'), code);
}
export function normalizeNativeNoteLimits(value = {}) {
  exact(value, Object.keys(DEFAULT_LIMITS), 'limits_invalid');
  const limits = { ...DEFAULT_LIMITS, ...value };
  for (const [key, limit] of Object.entries(limits)) integer(limit, 1, DEFAULT_LIMITS[key], 'limits_invalid');
  return Object.freeze(limits);
}
export function validateNativeNoteComposition(value) {
  if (value === undefined) return null;
  exact(value, ['notes', 'withAuthorityFence'], 'native_configuration_invalid');
  assert(typeof value.withAuthorityFence === 'function' && !asyncFunction(value.withAuthorityFence), 'native_configuration_invalid');
  assert(value.notes && typeof value.notes === 'object', 'native_configuration_invalid');
  for (const name of ['storageIdentity', 'validateDraftInput', 'createDraftForInvocation', 'readCreateProof']) {
    assert(typeof value.notes[name] === 'function' && !asyncFunction(value.notes[name]), 'native_configuration_invalid');
  }
  // Snapshot the actual functions once; caller mutations cannot replace the port later.
  return Object.freeze({ withAuthorityFence: value.withAuthorityFence,
    notes: Object.freeze(Object.fromEntries(['storageIdentity', 'validateDraftInput', 'createDraftForInvocation', 'readCreateProof']
      .map(name => [name, value.notes[name].bind(value.notes)]))) });
}

export function createNativeNotesCoordinator({ db, projectId, registryId, schemaVersion, clock, registry, access, core,
  settleNativeBudget, transaction, ensureOpen, composition, limits, invocationLimits = {} }) {
  const { notes, withAuthorityFence } = composition;
  const contexts = new WeakMap();
  let activeFrame = null, running = false, knownRegistryId = registryId;
  const get = (sql, ...args) => db.prepare(sql).get(...args);
  const run = (sql, ...args) => db.prepare(sql).run(...args);
  const nativeRow = id => get('SELECT * FROM cap_native_note_intents WHERE invocation_id=?', id);
  const snapshot = value => core.projection(value);
  const result = (value, outcome) => ({ outcome, invocation: snapshot(value) });
  function fixedEntry() {
    const entry = registry.get(ID, VERSION);
    assert(entry && entry.digest === DIGEST && canonicalJson(entry.executionBinding) === canonicalJson(TARGET)
      && canonicalJson(entry.resources) === '["notes:new"]' && canonicalJson(entry.effects) === '["create"]'
      && canonicalJson(entry.recipients) === '["soty:notes"]'
      && canonicalJson(entry.charges) === '[{"amount":1,"unit":"invocations"}]', 'native_contract_mismatch');
    return entry;
  }
  function capsIdentity() {
    ensureOpen();
    assert([2, 3].includes(schemaVersion) && get('PRAGMA user_version').user_version === schemaVersion, 'native_unavailable');
    const rows = db.prepare('SELECT key,value FROM cap_metadata ORDER BY key').all();
    const values = Object.fromEntries(rows.map(row => [row.key, row.value]));
    assert(rows.length === 3 && values.lineage === `soty.capabilities.sqlite.v${schemaVersion}`
      && typeof values.registry_id === 'string' && /^[a-f0-9]{32}$/u.test(values.registry_id), 'capabilities_storage_corrupt');
    assert(values.project_id === projectId && (!knownRegistryId || knownRegistryId === values.registry_id), 'native_store_mismatch');
    knownRegistryId ??= values.registry_id;
    assert(get('SELECT digest FROM cap_contracts WHERE capability_id=? AND version=?', ID, VERSION)?.digest === DIGEST, 'native_contract_mismatch');
    fixedEntry();
    return values.registry_id;
  }
  function notesIdentity() {
    const value = synchronous(notes.storageIdentity(), 'native_unavailable');
    assert(value && value.schemaVersion === 2
      && typeof value.registryId === 'string' && /^[a-f0-9]{32}$/u.test(value.registryId), 'native_unavailable');
    assert(value.projectId === projectId, 'native_store_mismatch');
    return value.registryId;
  }
  function fenced(action) {
    ensureOpen(); assert(!running, 'nested_transaction'); running = true;
    let invoked = false, accepting = true;
    try {
      const output = withAuthorityFence(() => {
        assert(accepting && running && !invoked, 'native_context_invalid'); invoked = true;
        return transaction(() => {
          const frame = { tokens: [] }; activeFrame = frame;
          try { return synchronous(action(), 'async_transaction_not_allowed'); }
          finally { for (const token of frame.tokens) contexts.delete(token); activeFrame = null; }
        }, { busyMs: 100 });
      });
      synchronous(output, 'native_context_invalid');
      assert(invoked, 'native_context_invalid');
      return output;
    } catch (error) {
      if (error?.code === 'ERR_SQLITE_ERROR' && [5, 6].includes(error.errcode & 255)) fail('native_storage_busy');
      throw error;
    } finally { accepting = false; activeFrame = null; running = false; }
  }
  function load(id) {
    const value = core.row(invocationId(id)); assert(value, 'invocation_not_found');
    const source = capsIdentity(), native = nativeRow(value.id);
    assert(native, 'native_legacy_invocation_unsupported');
    const suffix = canonicalHash(['soty.native-note.v1', source, value.id]);
    assert(native.account_id === value.account_id && native.note_id === `n_${suffix}` && native.mutation_id === `m_${suffix}`
      && value.capability_id === ID && value.capability_version === VERSION && value.capability_digest === DIGEST
      && value.job_id === null && value.target_json === canonicalJson(TARGET), 'native_contract_mismatch');
    const descriptor = Object.freeze({ projectId, sourceStoreId: source, notesStoreId: native.notes_store_id,
      invocationId: value.id, accountId: value.account_id, noteId: native.note_id, mutationId: native.mutation_id,
      inputDigest: native.input_digest, capabilityDigest: DIGEST });
    return { value, native, descriptor };
  }
  function originalAuthorization(value) {
    const saved = JSON.parse(value.authorization_json);
    assert(Number.isSafeInteger(saved.expiresAt) && saved.expiresAt >= 0, 'capabilities_storage_corrupt');
    assert(now(clock) < saved.expiresAt, 'access_denied');
    const authorized = access.authorizeInvocation({ action: 'dispatch', authorizationSnapshot: saved,
      invocation: { id: value.id, accountId: value.account_id, clientId: value.client_id, principalId: value.principal_id,
        grantId: value.grant_id, capabilityId: ID, version: VERSION, capabilityDigest: DIGEST } });
    assert(authorized.credentialId === saved.credentialId && authorized.audience === saved.audience
      && authorized.rootGrantId === value.root_grant_id && authorized.capabilityDigest === DIGEST,
    'invocation_dispatch_denied');
    for (const key of ['executionBinding', 'resources', 'effects', 'recipients', 'charges']) {
      assert(canonicalJson(authorized[key]) === canonicalJson(saved[key]), 'invocation_dispatch_denied');
    }
    // A sibling revocation can advance the root epoch. Fresh ancestry, original
    // credential and the original absolute deadline are the authorization gates.
    return authorized;
  }
  function verifyContext(token, mode) {
    ensureOpen();
    const context = token && typeof token === 'object' ? contexts.get(token) : null;
    assert(context && activeFrame && context.frame === activeFrame && db.isTransaction
      && context.mode === mode && ['create', 'reconcile'].includes(mode), 'native_context_invalid');
    const current = load(context.descriptor.invocationId);
    assert(!TERMINAL.has(current.value.status)
      && canonicalJson(current.descriptor) === canonicalJson(context.descriptor), 'native_context_invalid');
    if (mode === 'create') {
      assert(current.native.started_at !== null && !current.value.cancel_requested, 'access_denied');
      originalAuthorization(current.value);
    }
    return context.descriptor;
  }
  function withContext(descriptor, mode, action) {
    assert(activeFrame && db.isTransaction, 'native_context_invalid');
    const token = Object.freeze(Object.create(null));
    contexts.set(token, { descriptor, mode, frame: activeFrame }); activeFrame.tokens.push(token);
    try { return synchronous(action(token), 'native_context_invalid'); }
    finally { contexts.delete(token); activeFrame.tokens.pop(); }
  }
  function validateProof(proof, descriptor) {
    dataObject(proof, [...DESCRIPTOR_KEYS, 'revision', 'createdAt'], 'native_proof_invalid');
    assert(Object.keys(proof).length === DESCRIPTOR_KEYS.length + 2 && proof.revision === 1
      && Number.isSafeInteger(proof.createdAt) && proof.createdAt >= 0, 'native_proof_invalid');
    for (const key of DESCRIPTOR_KEYS) assert(proof[key] === descriptor[key], 'native_proof_invalid');
    return proof;
  }
  function readProof(descriptor) {
    assert(notesIdentity() === descriptor.notesStoreId, 'native_store_mismatch');
    const proof = withContext(descriptor, 'reconcile', context => notes.readCreateProof({ context }));
    return proof === null ? null : validateProof(proof, descriptor);
  }
  function complete(current, proof, { status = 'failed', errorCode } = {}) {
    const { value, descriptor } = current, success = proof !== null;
    if (success) {
      validateProof(proof, descriptor);
      try { registry.validateOutput(fixedEntry(), { noteId: descriptor.noteId, revision: 1 }); }
      catch { fail('native_output_invalid'); }
      status = 'succeeded';
    }
    assert(success || ['failed', 'cancelled'].includes(status), 'native_proof_invalid');
    const effectState = success ? 'committed' : 'none', disposition = success ? 'spent' : 'released';
    const effects = success ? [{ kind: 'created', resourceType: 'note', resourceId: descriptor.noteId, revision: 1 }] : [];
    const receipt = { verificationMethod: 'domain_read', artifacts: success ? [{ type: 'note', id: descriptor.noteId, revision: 1 }] : [],
      ...(!success && errorCode ? { errorCode } : {}) };
    const actualCharges = success ? [{ unit: 'invocations', amount: 1 }] : null;
    const digest = canonicalHash({ status, effectState, effects, receipt, disposition, actualCharges }), timestamp = now(clock);
    run('INSERT INTO cap_receipts(invocation_id,value_json,digest,created_at) VALUES(?,?,?,?)', value.id, canonicalJson(receipt), digest, timestamp);
    run(`UPDATE cap_invocations SET status=?,effect_state=?,effects_json=?,input_json='null',updated_at=?,completed_at=? WHERE id=?`,
      status, effectState, canonicalJson(effects), timestamp, timestamp, value.id);
    run('UPDATE cap_native_note_intents SET input_purged_at=? WHERE invocation_id=?', timestamp, value.id);
    settleNativeBudget({ invocationId: value.id, reservationId: value.reservation_id, disposition });
    return result(core.row(value.id), success ? 'committed' : 'not_applied');
  }
  function negativeOrLive(current) {
    if (current.value.cancel_requested) return complete(current, null, { status: 'cancelled' });
    try { originalAuthorization(current.value); return null; }
    catch (error) {
      if (error instanceof AccessError && AUTH_DENIALS.has(error.code)) return complete(current, null, { errorCode: error.code });
      if (error instanceof AccessError && error.code === 'capability_disabled') return result(current.value, 'held');
      throw error;
    }
  }
  function checkedInput(input) {
    dataObject(input, ['title', 'body']);
    assert(Object.hasOwn(input, 'title') && Object.hasOwn(input, 'body')
      && typeof input.title === 'string' && typeof input.body === 'string');
    const copy = { title: input.title, body: input.body };
    registry.validateInput(fixedEntry(), copy);
    assert(copy.title.isWellFormed() && copy.body.isWellFormed(), 'invalid_unicode');
    return copy;
  }
  function admissionBounds(authorized, timestamp) {
    const account = authorized.accountId, principal = authorized.principalId;
    function below(sql, params, limit, code) {
      let count = 0;
      for (const _row of db.prepare(`${sql} LIMIT ?`).iterate(...params, limit)) { if (++count === limit) fail(code); }
    }
    below('SELECT id FROM cap_invocations WHERE account_id=?', [account], limits.identitiesPerAccount, 'native_ledger_limit');
    below('SELECT id FROM cap_invocations', [], limits.identitiesTotal, 'native_ledger_limit');
    const nonterminal = "status NOT IN ('succeeded','failed','cancelled')";
    below(`SELECT id FROM cap_invocations WHERE account_id=? AND principal_id=? AND ${nonterminal}`, [account, principal], limits.nonterminalPerPrincipal, 'native_admission_limit');
    below(`SELECT id FROM cap_invocations WHERE account_id=? AND ${nonterminal}`, [account], limits.nonterminalPerAccount, 'native_admission_limit');
    below(`SELECT id FROM cap_invocations WHERE ${nonterminal}`, [], limits.nonterminalTotal, 'native_admission_limit');
    below('SELECT id FROM cap_invocations WHERE account_id=? AND principal_id=? AND created_at>=?', [account, principal, timestamp - 60000], limits.admissionsPerPrincipal, 'native_rate_limit');
    below('SELECT id FROM cap_invocations WHERE account_id=? AND created_at>=?', [account, timestamp - 60000], limits.admissionsPerAccount, 'native_rate_limit');
  }
  function terminalResult(value) { return result(value, value.status === 'succeeded' ? 'committed' : 'not_applied'); }
  function reconcileOrExecute(id, execute) {
    return fenced(() => {
      const current = load(id);
      if (TERMINAL.has(current.value.status)) return terminalResult(current.value);
      if (execute) assert(current.native.started_at !== null, 'native_attempt_not_started');
      const proof = readProof(current.descriptor);
      if (proof) return complete(current, proof);
      const stopped = negativeOrLive(current); if (stopped) return stopped;
      if (!execute) return result(current.value, 'retryable');
      const input = checkedInput(JSON.parse(current.value.input_json));
      assert(canonicalHash(input) === current.descriptor.inputDigest && nativeNoteRequestDigest(input) === current.value.request_digest,
        'capabilities_storage_corrupt');
      let created;
      try { created = withContext(current.descriptor, 'create', context => notes.createDraftForInvocation({ context, input })); }
      catch (error) {
        const domainRefusal = error instanceof NotesError && NOTES_REFUSALS.has(error.code);
        const authorityRefusal = error instanceof AccessError && AUTH_DENIALS.has(error.code);
        if (!domainRefusal && !authorityRefusal) throw error;
        const after = readProof(current.descriptor);
        if (after) return complete(current, after);
        const stopped = negativeOrLive(current); if (stopped) return stopped;
        if (domainRefusal) return complete(current, null, { errorCode: error.code });
        throw error;
      }
      return complete(current, validateProof(created, current.descriptor));
    });
  }
  const api = {
    readiness() {
      try { ensureOpen(); capsIdentity(); notesIdentity(); return Object.freeze({ ready: fixedEntry().executionEnabled }); }
      catch { return Object.freeze({ ready: false }); }
    },
    admit(args = {}) {
      dataObject(args, ['actor', 'idempotencyKey', 'input']);
      assert(typeof args.idempotencyKey === 'string' && args.idempotencyKey.length >= 8 && args.idempotencyKey.length <= 160
        && !/[\u0000-\u0020\u007f]/u.test(args.idempotencyKey), 'invocation_invalid_arguments');
      const input = checkedInput(args.input), inputJson = canonicalJson(input), requestDigest = nativeNoteRequestDigest(input);
      const requestKey = canonicalHash(args.idempotencyKey);
      return fenced(() => {
        const scope = access.authorize({ actor: args.actor, action: 'history' });
        const previous = get('SELECT * FROM cap_invocations WHERE account_id=? AND client_id=? AND request_key=?', scope.accountId, scope.clientId, requestKey);
        if (previous) {
          core.access(args.actor, previous, 'read');
          assert(previous.request_digest === requestDigest, 'invocation_request_conflict');
          return { reused: true, invocation: snapshot(previous) };
        }
        const sourceStoreId = capsIdentity(), notesStoreId = notesIdentity();
        const authorized = access.authorize({ actor: args.actor, action: 'invoke', capabilityId: ID, version: VERSION, input });
        assert(Buffer.byteLength(inputJson, 'utf8') <= (invocationLimits.inputBytes ?? 262144), 'invocation_payload_too_large');
        const validation = synchronous(notes.validateDraftInput({ input }), 'native_contract_mismatch');
        assert(validation && Number.isSafeInteger(validation.documentBytes)
          && validation.documentBytes >= 0 && validation.documentBytes <= 262144, 'native_contract_mismatch');
        admissionBounds(authorized, now(clock));
        const value = core.insertAuthorized({ authorized, capabilityId: ID, version: VERSION, inputJson,
          targetJson: canonicalJson(TARGET), requestKey, requestDigest });
        const suffix = canonicalHash(['soty.native-note.v1', sourceStoreId, value.id]);
        run(`INSERT INTO cap_native_note_intents(invocation_id,account_id,notes_store_id,note_id,mutation_id,input_digest,input_bytes)
          VALUES(?,?,?,?,?,?,?)`, value.id, value.account_id, notesStoreId, `n_${suffix}`, `m_${suffix}`, canonicalHash(input), Buffer.byteLength(inputJson, 'utf8'));
        return { reused: false, invocation: snapshot(value) };
      });
    },
    get(args = {}) {
      dataObject(args, ['actor', 'invocationId']);
      return fenced(() => { const value = core.row(args.invocationId); core.access(args.actor, value, 'read'); return { invocation: snapshot(value) }; });
    },
    beginAttempt(args = {}) {
      dataObject(args, ['invocationId']);
      return fenced(() => {
        const current = load(args.invocationId);
        if (TERMINAL.has(current.value.status)) return { started: false, invocation: snapshot(current.value) };
        if (current.native.started_at !== null) return { started: true, invocation: snapshot(current.value) };
        assert(!current.value.cancel_requested, 'access_denied');
        assert(notesIdentity() === current.descriptor.notesStoreId, 'native_store_mismatch');
        originalAuthorization(current.value);
        const timestamp = now(clock);
        run('UPDATE cap_native_note_intents SET started_at=? WHERE invocation_id=?', timestamp, current.value.id);
        run("UPDATE cap_dispatch_intents SET state='dispatching',updated_at=? WHERE invocation_id=?", timestamp, current.value.id);
        return { started: true, invocation: snapshot(core.row(current.value.id)) };
      });
    },
    execute(args = {}) { dataObject(args, ['invocationId']); return reconcileOrExecute(args.invocationId, true); },
    reconcile(args = {}) { dataObject(args, ['invocationId']); return reconcileOrExecute(args.invocationId, false); },
    reconcilePage(args = {}) {
      dataObject(args, ['cursor']);
      let after = null;
      if (args.cursor !== undefined) {
        assert(typeof args.cursor === 'string' && /^[A-Za-z0-9_-]{1,512}$/u.test(args.cursor), 'invocation_invalid_cursor');
        try { after = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8')); } catch { fail('invocation_invalid_cursor'); }
        dataObject(after, ['at', 'id'], 'invocation_invalid_cursor'); integer(after.at, 0, Number.MAX_SAFE_INTEGER, 'invocation_invalid_cursor'); invocationId(after.id);
      }
      const rows = fenced(() => {
        capsIdentity();
        return db.prepare(`SELECT i.id,i.created_at FROM cap_invocations i JOIN cap_native_note_intents n ON n.invocation_id=i.id
          WHERE i.status NOT IN ('succeeded','failed','cancelled')
          ${after ? 'AND (i.created_at>? OR (i.created_at=? AND i.id>?))' : ''}
          ORDER BY i.created_at,i.id LIMIT ?`).all(...(after ? [after.at, after.at, after.id] : []), limits.recoveryPageSize + 1);
      });
      const page = rows.slice(0, limits.recoveryPageSize), last = page.at(-1);
      const items = page.map(row => {
        try { return { invocationId: row.id, outcome: api.reconcile({ invocationId: row.id }).outcome }; }
        catch { return { invocationId: row.id, errorCode: 'native_reconciliation_failed' }; }
      });
      return { items, nextCursor: rows.length > page.length ? Buffer.from(JSON.stringify({ at: last.created_at, id: last.id })).toString('base64url') : null };
    },
    verifyContext,
  };
  return Object.freeze(api);
}
