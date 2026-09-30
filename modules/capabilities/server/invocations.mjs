import { createHash, randomUUID } from 'node:crypto';
import { createNativeBaselineGuard } from './native-baseline.mjs';

const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const EFFECT_STATES = new Set(['none', 'committed', 'partial', 'unknown']);
const VERIFICATIONS = new Set(['domain_read', 'artifact_hash', 'test', 'handler_assertion', 'unverified']);
const AUTHORIZATION_DENIALS = new Set(['authorization_required', 'access_denied', 'capability_disabled', 'capability_changed', 'not_found', 'invocation_dispatch_denied']);
const DEFAULT_LIMITS = Object.freeze({ inputBytes: 262144, metadataBytes: 32768, pageSize: 50 });

export class InvocationError extends Error {
  constructor(code) { super(code); this.name = 'InvocationError'; this.code = code; }
}
function check(value, code = 'invocation_invalid_arguments') { if (!value) throw new InvocationError(code); }
function plain(value) { return value !== null && typeof value === 'object' && !Array.isArray(value) && [null, Object.prototype].includes(Object.getPrototypeOf(value)); }
function exact(value, keys) { check(plain(value) && Object.keys(value).every(key => keys.includes(key))); }
function identifier(value) { check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/u.test(value)); return value; }
function integer(value) { check(Number.isSafeInteger(value) && value >= 0); return value; }
function canonical(value, depth = 0) {
  check(depth <= 24);
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return value;
  if (typeof value === 'number') { check(Number.isFinite(value)); return value; }
  if (Array.isArray(value)) return value.map(item => canonical(item, depth + 1));
  check(plain(value));
  const result = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    check(Object.getOwnPropertyDescriptor(value, key)?.get === undefined);
    check(value[key] !== undefined); result[key] = canonical(value[key], depth + 1);
  }
  return result;
}
function encoded(value, maxBytes) {
  const text = JSON.stringify(canonical(value));
  check(Buffer.byteLength(text) <= maxBytes, 'invocation_payload_too_large'); return text;
}
const hash = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

/** Domain ledger only. The host owns the shared SQLite transaction and ACL callbacks.
 * Dispatch intents are recoverable delivery records; connector jobs own execution leases.
 * Internal dispatch/result methods must never be exposed as unauthenticated HTTP operations. */
export function createInvocationStore({ db, clock = Date.now, transaction, authorize, reserveBudget, settleBudget,
  canonicalHash = hash, newId = prefix => `${prefix}_${randomUUID().replaceAll('-', '')}`, limits = {} } = {}) {
  check(db && typeof db.prepare === 'function' && [transaction, authorize, reserveBudget, settleBudget, clock, canonicalHash, newId].every(fn => typeof fn === 'function'), 'invocation_configuration_required');
  const bounds = { ...DEFAULT_LIMITS, ...limits };
  for (const [name, value] of Object.entries(bounds)) check(Object.hasOwn(DEFAULT_LIMITS, name) && Number.isSafeInteger(value) && value > 0 && value <= DEFAULT_LIMITS[name], 'invocation_invalid_limits');
  const stmt = new Map();
  const prepare = sql => { if (!stmt.has(sql)) stmt.set(sql, db.prepare(sql)); return stmt.get(sql); };
  const get = (sql, ...args) => prepare(sql).get(...args);
  const run = (sql, ...args) => prepare(sql).run(...args);
  const now = () => integer(clock());
  const native = createNativeBaselineGuard({ db, error: code => new InvocationError(code) });
  function atomic(fn) {
    return transaction(() => { const result = fn(); check(!result || typeof result.then !== 'function', 'invocation_async_transaction_forbidden'); return result; });
  }
  function policy(args) {
    const result = authorize(args);
    check(result && typeof result.then !== 'function', 'invocation_authorization_required');
    for (const key of ['accountId', 'clientId', 'principalId']) identifier(result[key]);
    return result;
  }
  const row = id => get('SELECT * FROM cap_invocations WHERE id=?', identifier(id));
  const intent = id => get('SELECT * FROM cap_dispatch_intents WHERE invocation_id=?', id);
  const invocationContext = value => ({ id: value.id, accountId: value.account_id, clientId: value.client_id, principalId: value.principal_id,
    grantId: value.grant_id, rootGrantId: value.root_grant_id, capabilityId: value.capability_id, version: value.capability_version,
    capabilityDigest: value.capability_digest });
  function access(actor, value, action) {
    const scope = policy({ actor, action: 'history' });
    check(value && value.account_id === scope.accountId && value.client_id === scope.clientId && value.principal_id === scope.principalId
      && value.grant_id === scope.grantId, 'invocation_not_found');
    const authorized = policy({ actor, action, capabilityId: value.capability_id, version: value.capability_version, invocation: invocationContext(value) });
    check(authorized.accountId === value.account_id && authorized.clientId === value.client_id && authorized.principalId === value.principal_id
      && authorized.grantId === value.grant_id, 'invocation_not_found');
    return authorized;
  }
  function projection(value) {
    const saved = get('SELECT value_json FROM cap_receipts WHERE invocation_id=?', value.id);
    // The pre-effect marker can outlive the Notes COMMIT. This baseline cannot
    // prove absence and must not describe an unresolved started intent as none.
    const started = !TERMINAL.has(value.status) && native.find(value.id)?.started_at != null;
    return { invocationId: value.id, capabilityId: value.capability_id, version: value.capability_version,
      status: value.status, cancelRequested: Boolean(value.cancel_requested), effectState: started ? 'unknown' : value.effect_state,
      effects: JSON.parse(value.effects_json), createdAt: value.created_at, updatedAt: value.updated_at,
      ...(value.completed_at === null ? {} : { completedAt: value.completed_at }),
      ...(saved ? { receipt: JSON.parse(saved.value_json) } : {}) };
  }
  function authorizationSnapshot(value) {
    const snapshot = {};
    for (const key of ['accountId', 'clientId', 'principalId', 'credentialId', 'audience', 'grantId', 'rootGrantId', 'policyEpoch', 'expiresAt',
      'capabilityId', 'version', 'capabilityDigest', 'resources', 'effects', 'recipients', 'executionBinding', 'charges']) {
      if (value[key] !== undefined) snapshot[key] = value[key];
    }
    return encoded(snapshot, bounds.metadataBytes);
  }
  function settle(value, disposition, actualCharges) {
    check(['spent', 'released', 'uncertain'].includes(disposition));
    const result = settleBudget({ reservationId: value.reservation_id, disposition, actualCharges });
    check(!result || typeof result.then !== 'function', 'invocation_async_transaction_forbidden');
  }
  function sanitizeEffects(values) {
    check(Array.isArray(values) && values.length <= 32);
    return values.map(value => {
      exact(value, ['kind', 'resourceType', 'resourceId', 'revision']);
      check(['created', 'updated', 'deleted', 'published', 'sent', 'charged'].includes(value.kind));
      return { kind: value.kind, resourceType: identifier(value.resourceType), resourceId: identifier(value.resourceId),
        ...(value.revision === undefined ? {} : { revision: integer(value.revision) }) };
    });
  }
  function sanitizeReceipt(value) {
    exact(value, ['verificationMethod', 'artifacts', 'errorCode']);
    check(VERIFICATIONS.has(value.verificationMethod));
    check(Array.isArray(value.artifacts) && value.artifacts.length <= 32);
    const artifacts = value.artifacts.map(artifact => {
      exact(artifact, ['type', 'id', 'revision', 'sha256']);
      if (artifact.sha256 !== undefined) check(typeof artifact.sha256 === 'string' && /^[a-f0-9]{64}$/u.test(artifact.sha256));
      return { type: identifier(artifact.type), id: identifier(artifact.id),
        ...(artifact.revision === undefined ? {} : { revision: integer(artifact.revision) }),
        ...(artifact.sha256 === undefined ? {} : { sha256: artifact.sha256 }) };
    });
    if (value.errorCode !== undefined) check(typeof value.errorCode === 'string' && /^[a-z][a-z0-9_]{1,79}$/u.test(value.errorCode));
    return { verificationMethod: value.verificationMethod, artifacts, ...(value.errorCode === undefined ? {} : { errorCode: value.errorCode }) };
  }
  function dispatchProjection(value, authorized) {
    return { invocationId: value.id, internalRequestId: value.internal_request_id, jobId: value.job_id,
      status: value.status, cancelRequested: Boolean(value.cancel_requested),
      input: JSON.parse(value.input_json), target: JSON.parse(value.target_json), authorization: authorized };
  }
  function authorizeDispatch(value) {
    const authorized = policy({ action: 'dispatch', authorizationSnapshot: JSON.parse(value.authorization_json), invocation: invocationContext(value) });
    check(authorized.accountId === value.account_id && authorized.clientId === value.client_id && authorized.principalId === value.principal_id
      && authorized.grantId === value.grant_id && authorized.rootGrantId === value.root_grant_id
      && authorized.capabilityId === value.capability_id && authorized.version === value.capability_version
      && authorized.capabilityDigest === value.capability_digest && canonicalHash(authorized.executionBinding ?? null) === canonicalHash(JSON.parse(value.target_json)), 'invocation_dispatch_denied');
    return authorized;
  }
  const notDispatched = (value, delivery) => delivery?.state === 'pending' && value.job_id === null
    && value.effect_state === 'none' && JSON.parse(value.effects_json).length === 0 && !native.find(value.id);
  function decodeCursor(cursor, scope) {
    if (cursor === undefined) return null;
    check(typeof cursor === 'string' && /^[A-Za-z0-9_-]{1,1024}$/u.test(cursor), 'invocation_invalid_cursor');
    let after;
    try { after = JSON.parse(Buffer.from(cursor, 'base64url').toString()); } catch { throw new InvocationError('invocation_invalid_cursor'); }
    check(plain(after) && after.scope === scope && Number.isSafeInteger(after.at) && after.at >= 0 && typeof after.id === 'string', 'invocation_invalid_cursor');
    identifier(after.id); return after;
  }
  const nextCursor = (values, limit, scope) => values.length > limit
    ? Buffer.from(JSON.stringify({ scope, at: values[limit - 1].created_at, id: values[limit - 1].id })).toString('base64url') : null;

  return Object.freeze({
    admit({ actor, capabilityId, version, idempotencyKey, input, target } = {}) {
      identifier(capabilityId); check(Number.isSafeInteger(version) && version >= 1 && version <= 1000000);
      check(typeof idempotencyKey === 'string' && idempotencyKey.length >= 8 && idempotencyKey.length <= 160 && !/[\u0000-\u0020\u007f]/u.test(idempotencyKey));
      const inputJson = encoded(input, bounds.inputBytes), cleanInput = JSON.parse(inputJson);
      return atomic(() => {
        const authorized = policy({ actor, action: 'invoke', capabilityId, version, input: cleanInput, ...(target === undefined ? {} : { target }) });
        check(authorized.capabilityId === capabilityId && authorized.version === version, 'invocation_capability_mismatch');
        const pinnedTarget = authorized.executionBinding ?? null;
        if (target !== undefined) check(canonicalHash(target) === canonicalHash(pinnedTarget), 'invocation_target_mismatch');
        const targetJson = encoded(pinnedTarget, bounds.metadataBytes);
        const requestKey = canonicalHash(idempotencyKey);
        const requestDigest = canonicalHash({ capabilityId, version, capabilityDigest: authorized.capabilityDigest,
          input: cleanInput, target: pinnedTarget, resources: authorized.resources, effects: authorized.effects, recipients: authorized.recipients });
        const previous = get('SELECT * FROM cap_invocations WHERE account_id=? AND client_id=? AND request_key=?', authorized.accountId, authorized.clientId, requestKey);
        if (previous) {
          check(previous.principal_id === authorized.principalId, 'invocation_not_found');
          check(previous.request_digest === requestDigest, 'invocation_request_conflict');
          access(actor, previous, 'read');
          return { reused: true, invocation: projection(previous) };
        }
        const id = identifier(newId('inv')); const timestamp = now();
        const internalRequestId = `cap_${canonicalHash(id)}`;
        const reservation = reserveBudget({ authorization: authorized, invocationId: id, attemptId: 'admission', charges: authorized.charges });
        check(reservation && typeof reservation.then !== 'function', 'invocation_budget_required');
        const reservationId = identifier(reservation.reservationId);
        run(`INSERT INTO cap_invocations(id,account_id,client_id,principal_id,grant_id,root_grant_id,policy_epoch,
          capability_id,capability_version,capability_digest,request_key,request_digest,internal_request_id,input_json,target_json,
          authorization_json,status,cancel_requested,effect_state,effects_json,reservation_id,job_id,created_at,updated_at,completed_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, authorized.accountId, authorized.clientId, authorized.principalId,
          identifier(authorized.grantId), identifier(authorized.rootGrantId), integer(authorized.policyEpoch), capabilityId, version,
          identifier(authorized.capabilityDigest), requestKey, requestDigest, internalRequestId, inputJson, targetJson,
          authorizationSnapshot(authorized), 'accepted', 0, 'none', '[]', reservationId, null, timestamp, timestamp, null);
        run('INSERT INTO cap_dispatch_intents(invocation_id,internal_request_id,state,created_at,updated_at) VALUES(?,?,?,?,?)', id, internalRequestId, 'pending', timestamp, timestamp);
        return { reused: false, invocation: projection(row(id)) };
      });
    },
    get({ actor, invocationId } = {}) {
      return atomic(() => { const value = row(invocationId); access(actor, value, 'read'); return { invocation: projection(value) }; });
    },
    list({ actor, limit = 20, cursor } = {}) {
      check(Number.isSafeInteger(limit) && limit >= 1 && limit <= bounds.pageSize);
      return atomic(() => {
        const authorized = policy({ actor, action: 'history' });
        const scope = canonicalHash([authorized.accountId, authorized.clientId, authorized.principalId, authorized.grantId]);
        const after = decodeCursor(cursor, scope);
        const params = [authorized.accountId, authorized.clientId, authorized.principalId, authorized.grantId];
        if (after) params.push(after.at, after.at, after.id);
        params.push(limit + 1);
        const values = prepare(`SELECT * FROM cap_invocations WHERE account_id=? AND client_id=? AND principal_id=? AND grant_id=?
          ${after ? 'AND (created_at>? OR (created_at=? AND id>?))' : ''} ORDER BY created_at,id LIMIT ?`).all(...params);
        const page = values.slice(0, limit);
        // Current authorization is checked per returned capability as well as the collection.
        for (const value of page) access(actor, value, 'read');
        return { invocations: page.map(projection), nextCursor: nextCursor(values, limit, scope) };
      });
    },
    // Internal owner read model. The host MUST validate the signed Connect owner
    // and expected account before invoking this; a service credential cannot use it.
    listForOwner({ accountId, limit = 20, cursor } = {}) {
      identifier(accountId); check(Number.isSafeInteger(limit) && limit >= 1 && limit <= bounds.pageSize);
      return atomic(() => {
        const scope = canonicalHash(['owner', accountId]), after = decodeCursor(cursor, scope);
        const params = [accountId];
        if (after) params.push(after.at, after.at, after.id);
        params.push(limit + 1);
        const values = prepare(`SELECT * FROM cap_invocations WHERE account_id=?
          ${after ? 'AND (created_at>? OR (created_at=? AND id>?))' : ''} ORDER BY created_at,id LIMIT ?`).all(...params);
        return { invocations: values.slice(0, limit).map(value => ({ ...projection(value), clientId: value.client_id,
          principalId: value.principal_id, grantId: value.grant_id })), nextCursor: nextCursor(values, limit, scope) };
      });
    },
    requestCancel({ actor, invocationId } = {}) {
      return atomic(() => {
        const value = row(invocationId); access(actor, value, 'cancel');
        if (TERMINAL.has(value.status)) return { invocation: projection(value) };
        const delivery = intent(value.id), timestamp = now();
        const beforeDispatch = notDispatched(value, delivery);
        run('UPDATE cap_invocations SET cancel_requested=1,status=?,updated_at=?,completed_at=? WHERE id=?', beforeDispatch ? 'cancelled' : 'cancel_requested', timestamp, beforeDispatch ? timestamp : null, value.id);
        if (native.find(value.id)?.started_at != null) run("UPDATE cap_invocations SET effect_state='unknown' WHERE id=?", value.id);
        if (beforeDispatch) {
          run("UPDATE cap_dispatch_intents SET state='cancelled',updated_at=? WHERE invocation_id=?", timestamp, value.id);
          settle(value, 'released');
        }
        return { invocation: projection(row(value.id)) };
      });
    },
    // Internal reconciliation inventory. No authority, input, token or request key is returned.
    peekDispatch({ limit = 20 } = {}) {
      check(Number.isSafeInteger(limit) && limit > 0 && limit <= bounds.pageSize);
      return prepare(`SELECT d.invocation_id AS invocationId,d.internal_request_id AS internalRequestId,d.state
        FROM cap_dispatch_intents d JOIN cap_invocations i ON i.id=d.invocation_id
        WHERE d.state IN ('pending','dispatching','uncertain') AND i.status NOT IN ('succeeded','failed','cancelled')
        ${native.available() ? 'AND NOT EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=i.id)' : ''}
        ORDER BY d.created_at,d.invocation_id LIMIT ?`).all(limit);
    },
    beginDispatch({ invocationId } = {}) {
      return atomic(() => {
        const value = row(invocationId); check(value, 'invocation_not_found');
        native.assertGeneric(value.id);
        check(!value.cancel_requested && !TERMINAL.has(value.status), 'invocation_dispatch_denied');
        const authorized = authorizeDispatch(value);
        const delivery = intent(value.id); check(delivery && ['pending', 'dispatching'].includes(delivery.state), 'invocation_reconcile_required');
        const timestamp = now();
        run("UPDATE cap_dispatch_intents SET state='dispatching',updated_at=? WHERE invocation_id=?", timestamp, value.id);
        run('UPDATE cap_invocations SET updated_at=? WHERE id=?', timestamp, value.id);
        return dispatchProjection(row(value.id), authorized);
      });
    },
    // Internal maintenance only. A policy outage/programming error is not proof of
    // revocation; only explicit policy denials may settle a provably unstarted call.
    // This is not a reusable execution permit or a remote runtime lease.
    reconcileAuthorization({ invocationId } = {}) {
      return atomic(() => {
        const value = row(invocationId); check(value, 'invocation_not_found');
        try {
          authorizeDispatch(value);
          return { authorized: true, invocation: projection(value) };
        } catch (error) {
          if (!AUTHORIZATION_DENIALS.has(error?.code)) throw error;
        }
        if (TERMINAL.has(value.status)) return { authorized: false, invocation: projection(value) };
        const beforeDispatch = notDispatched(value, intent(value.id)), timestamp = now();
        run('UPDATE cap_invocations SET cancel_requested=1,status=?,updated_at=?,completed_at=? WHERE id=?', beforeDispatch ? 'cancelled' : 'cancel_requested', timestamp, beforeDispatch ? timestamp : null, value.id);
        if (native.find(value.id)?.started_at != null) run("UPDATE cap_invocations SET effect_state='unknown' WHERE id=?", value.id);
        if (beforeDispatch) {
          run("UPDATE cap_dispatch_intents SET state='cancelled',updated_at=? WHERE invocation_id=?", timestamp, value.id);
          const receipt = { verificationMethod: 'unverified', artifacts: [], errorCode: 'authorization_no_longer_valid' };
          run('INSERT INTO cap_receipts(invocation_id,value_json,digest,created_at) VALUES(?,?,?,?)', value.id,
            encoded(receipt, bounds.metadataBytes), canonicalHash({ status: 'cancelled', effectState: 'none', effects: [], receipt, disposition: 'released', actualCharges: null }), timestamp);
          settle(value, 'released');
        } else {
          // Dispatch may already have committed an external effect. Request stop,
          // retain any known effects and hold quota until a worker reconciles it.
          settle(value, 'uncertain');
        }
        return { authorized: false, invocation: projection(row(value.id)) };
      });
    },
    bindJob({ invocationId, internalRequestId, jobId } = {}) {
      identifier(jobId); identifier(internalRequestId);
      return atomic(() => {
        const value = row(invocationId); check(value, 'invocation_not_found');
        native.assertGeneric(value.id);
        check(value.internal_request_id === internalRequestId, 'invocation_binding_mismatch');
        check(value.job_id === null || value.job_id === jobId, 'invocation_binding_mismatch');
        const delivery = intent(value.id);
        check(delivery && ['dispatching', 'bound', 'uncertain'].includes(delivery.state), 'invocation_binding_mismatch');
        const timestamp = now();
        run('UPDATE cap_invocations SET job_id=?,updated_at=? WHERE id=?', jobId, timestamp, value.id);
        run("UPDATE cap_dispatch_intents SET state='bound',updated_at=? WHERE invocation_id=?", timestamp, value.id);
        return { invocation: projection(row(value.id)), cancelRequested: Boolean(value.cancel_requested) };
      });
    },
    markUncertain({ invocationId, effects = [] } = {}) {
      const cleanEffects = sanitizeEffects(effects);
      return atomic(() => {
        const value = row(invocationId); check(value, 'invocation_not_found');
        native.assertGeneric(value.id);
        if (TERMINAL.has(value.status)) return { invocation: projection(value) };
        check(['dispatching', 'bound', 'uncertain'].includes(intent(value.id)?.state), 'invocation_dispatch_not_started');
        const known = new Map([...JSON.parse(value.effects_json), ...cleanEffects].map(effect => [canonicalHash(effect), effect]));
        const knownEffects = sanitizeEffects([...known.values()]);
        const timestamp = now();
        run("UPDATE cap_invocations SET status='execution_uncertain',effect_state='unknown',effects_json=?,updated_at=? WHERE id=?", encoded(knownEffects, bounds.metadataBytes), timestamp, value.id);
        run("UPDATE cap_dispatch_intents SET state='uncertain',updated_at=? WHERE invocation_id=?", timestamp, value.id);
        settle(value, 'uncertain');
        return { invocation: projection(row(value.id)) };
      });
    },
    recordResult({ invocationId, status, effectState = 'none', effects = [], receipt, disposition = 'spent', actualCharges } = {}) {
      check(TERMINAL.has(status) && EFFECT_STATES.has(effectState));
      check(effectState !== 'unknown', 'invocation_reconcile_required');
      const cleanEffects = sanitizeEffects(effects), cleanReceipt = sanitizeReceipt(receipt);
      check(effectState !== 'none' || cleanEffects.length === 0);
      check(!['committed', 'partial'].includes(effectState) || cleanEffects.length > 0);
      const receiptJson = encoded(cleanReceipt, bounds.metadataBytes);
      return atomic(() => {
        const value = row(invocationId); check(value, 'invocation_not_found');
        native.assertGeneric(value.id);
        // P1 supports count quotas only. Completed work cannot replenish its own
        // allowance by claiming zero cost or releasing a successful reservation.
        if (status === 'succeeded' || cleanEffects.length > 0) {
          check(disposition === 'spent', 'invocation_settlement_conflict');
          if (actualCharges !== undefined) check(canonicalHash(actualCharges) === canonicalHash(JSON.parse(value.authorization_json).charges), 'invocation_settlement_conflict');
        }
        const digest = canonicalHash({ status, effectState, effects: cleanEffects, receipt: cleanReceipt, disposition, actualCharges: actualCharges ?? null });
        const previous = get('SELECT digest FROM cap_receipts WHERE invocation_id=?', value.id);
        if (previous) { check(previous.digest === digest, 'invocation_result_conflict'); return { reused: true, invocation: projection(value) }; }
        check(!TERMINAL.has(value.status), 'invocation_result_conflict');
        check(intent(value.id)?.state !== 'pending', 'invocation_dispatch_not_started');
        const effectDigests = new Set(cleanEffects.map(effect => canonicalHash(effect)));
        check(JSON.parse(value.effects_json).every(effect => effectDigests.has(canonicalHash(effect))), 'invocation_known_effect_conflict');
        const timestamp = now();
        settle(value, disposition, actualCharges);
        run('INSERT INTO cap_receipts(invocation_id,value_json,digest,created_at) VALUES(?,?,?,?)', value.id, receiptJson, digest, timestamp);
        run('UPDATE cap_invocations SET status=?,effect_state=?,effects_json=?,updated_at=?,completed_at=? WHERE id=?', status, effectState, encoded(cleanEffects, bounds.metadataBytes), timestamp, timestamp, value.id);
        return { reused: false, invocation: projection(row(value.id)) };
      });
    },
  });
}
