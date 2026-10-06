import { assert, canonicalHash, canonicalJson, freezeDeep, identifier, integer } from './validation.mjs';

export const EXTERNAL_ADAPTER_PROFILE = 'soty.external-idempotent-effect.v1';
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const EFFECTS = Object.freeze({ created: 'create', updated: 'update', deleted: 'delete', published: 'publish', sent: 'send', charged: 'charge' });
const CODE = 'external_adapter_invalid';

function object(input, required, optional = [], code = CODE) {
  assert(input && typeof input === 'object' && !Array.isArray(input)
    && [null, Object.prototype].includes(Object.getPrototypeOf(input)), code);
  const properties = Object.getOwnPropertyDescriptors(input);
  assert(Object.getOwnPropertySymbols(input).length === 0 && required.every(key => Object.hasOwn(properties, key))
    && Object.entries(properties).every(([key, value]) => [...required, ...optional].includes(key)
      && value.enumerable && Object.hasOwn(value, 'value')), code);
  return Object.fromEntries(Object.entries(properties).map(([key, property]) => [key, property.value]));
}

function pin(input) {
  const value = object(input, ['capabilityId', 'version', 'digest']);
  identifier(value.capabilityId, CODE); integer(value.version, 1, 1000000, CODE);
  assert(typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest), CODE);
  return Object.freeze(value);
}

// Capture trusted code once. Neither the contract nor an HTTP descriptor can
// load a module, select a URL/command, supply a key or register these closures.
export function captureExternalAdapters(input = []) {
  assert(Array.isArray(input) && input.length <= 64, CODE);
  const seen = new Set();
  return Object.freeze(input.map(item => {
    const entry = object(item, ['contract', 'adapter']);
    const contract = pin(entry.contract), key = `${contract.capabilityId}@${contract.version}`;
    assert(!seen.has(key), 'external_adapter_duplicate'); seen.add(key);
    const adapter = object(entry.adapter, ['profile', 'withAuthority', 'execute', 'readProof']);
    assert(adapter.profile === EXTERNAL_ADAPTER_PROFILE && ['withAuthority', 'execute', 'readProof'].every(name => typeof adapter[name] === 'function'), CODE);
    assert(adapter.withAuthority.constructor?.name !== 'AsyncFunction', CODE);
    return Object.freeze({ contract, adapter: Object.freeze({ profile: adapter.profile,
      ...Object.fromEntries(['withAuthority', 'execute', 'readProof'].map(name => [name, adapter[name].bind(entry.adapter)])) }) });
  }));
}

function data(input) {
  // Caller data is captured before the first await, without executing getters.
  let nodes = 0;
  function snapshot(value, depth = 0) {
    assert(++nodes <= 4096 && depth <= 18, 'external_payload_limit');
    if (value === null || ['string', 'boolean', 'number'].includes(typeof value)) return value;
    if (Array.isArray(value)) {
      const descriptors = Object.getOwnPropertyDescriptors(value);
      assert(Object.keys(descriptors).length === value.length + 1 && value.length <= 128, CODE);
      return Array.from({ length: value.length }, (_, index) => {
        assert(descriptors[index] && Object.hasOwn(descriptors[index], 'value'), CODE);
        return snapshot(descriptors[index].value, depth + 1);
      });
    }
    const record = object(value, [], Object.keys(value ?? {}));
    return Object.fromEntries(Object.entries(record).map(([key, child]) => [key, snapshot(child, depth + 1)]));
  }
  const result = snapshot(input);
  canonicalJson(result, { maxBytes: 65536, maxDepth: 18 });
  return freezeDeep(result);
}

function synchronous(value) {
  if (value && typeof value.then === 'function') {
    Promise.resolve(value).catch(() => {}); assert(false, 'external_authority_invalid');
  }
  return value;
}

/** A host-only async delivery coordinator over the existing Capabilities
 * ledger. There is no new database, worker queue, token issuer or shell path.
 * Source effect/proof calls NEVER run inside a SQLite authority transaction.
 * A source must atomically deduplicate the exact request and retain its proof.
 * `not_applied` is retryable absence, never permission to release a started
 * reservation: another process may already have a source request in flight. */
export function createExternalAdapterCoordinator({ entries, registry, invocations, authorize, limits = {} }) {
  object(limits, [], ['inflight', 'callTimeoutMs']);
  const bounds = Object.freeze({ inflight: limits.inflight ?? 4, callTimeoutMs: limits.callTimeoutMs ?? 8000 });
  integer(bounds.inflight, 1, 4, CODE); integer(bounds.callTimeoutMs, 1, 8000, CODE);
  assert(registry && typeof registry.get === 'function' && invocations && typeof invocations.inspectDispatch === 'function' && typeof authorize === 'function', CODE);
  const byKey = new Map();
  for (const value of entries) {
    const entry = registry.get(value.contract.capabilityId, value.contract.version);
    assert(entry && entry.digest === value.contract.digest && entry.capabilityId !== 'notes.createDraft'
      && entry.executionBinding.kind === 'registered' && entry.executionBinding.handler === entry.capabilityId
      && entry.executionBinding.version === 1 && entry.effects.length > 0, 'external_contract_mismatch');
    byKey.set(`${entry.capabilityId}@${entry.version}`, Object.freeze({ ...value, entry }));
  }
  let closed = false;
  const inflight = new Map();
  // Timed-out callers do not release a still-running provider call. A trusted
  // port ignoring AbortSignal therefore cannot accumulate unbounded promises.
  const outstanding = new Map();
  function binding(reference) {
    const contract = pin(reference), value = byKey.get(`${contract.capabilityId}@${contract.version}`);
    assert(value && value.contract.digest === contract.digest, 'external_adapter_not_registered'); return value;
  }
  function authority(value, request, callback) {
    let active = true, entered = false, poisoned = false, outcome;
    try {
      const returned = value.adapter.withAuthority(Object.freeze(request), () => {
        if (!active || entered || closed) poisoned = true;
        assert(active && !entered && !closed, 'external_authority_invalid'); entered = true;
        outcome = synchronous(callback()); return outcome;
      });
      synchronous(returned); assert(entered && !poisoned, 'external_authority_invalid'); return outcome;
    } finally { active = false; }
  }
  async function bounded(callback) {
    assert(!closed, 'external_adapter_closed');
    assert(outstanding.size < bounds.inflight, 'external_adapter_capacity');
    const controller = new AbortController(); let timer;
    const source = Promise.resolve().then(() => { assert(!closed, 'external_adapter_closed'); return callback(controller.signal); });
    outstanding.set(source, controller);
    source.then(() => outstanding.delete(source), () => outstanding.delete(source));
    const timeout = new Promise((_, reject) => { timer = setTimeout(() => {
      controller.abort(); reject(Object.assign(new Error('external_source_unconfirmed'), { code: 'external_source_unconfirmed' }));
    }, bounds.callTimeoutMs); });
    try { return await Promise.race([source, timeout]); }
    finally { clearTimeout(timer); }
  }
  function proof(value, context, inputDigest, raw) {
    const result = data(raw);
    object(result, ['requestId', 'inputDigest', 'outcome'], ['effects', 'receipt']);
    assert(result.requestId === context.internalRequestId && result.inputDigest === inputDigest
      && ['committed', 'not_applied', 'unknown'].includes(result.outcome), 'external_proof_mismatch');
    if (result.outcome !== 'committed') {
      assert(result.effects === undefined && result.receipt === undefined, 'external_proof_mismatch'); return result;
    }
    assert(Array.isArray(result.effects) && result.effects.length > 0 && result.effects.length <= 32
      && result.effects.every(effect => value.entry.effects.includes(EFFECTS[effect.kind])), 'external_proof_mismatch');
    assert(result.receipt && ['domain_read', 'artifact_hash'].includes(result.receipt.verificationMethod), 'external_proof_unverified');
    assert(Array.isArray(result.receipt.artifacts) && result.receipt.artifacts.length === result.effects.length, 'external_proof_mismatch');
    const effectKeys = result.effects.map(effect => canonicalHash({ type: effect.resourceType, id: effect.resourceId, revision: effect.revision ?? null }));
    const artifactKeys = result.receipt.artifacts.map(artifact => canonicalHash({ type: artifact.type, id: artifact.id, revision: artifact.revision ?? null }));
    assert(new Set(effectKeys).size === effectKeys.length && new Set(artifactKeys).size === artifactKeys.length
      && effectKeys.every(key => artifactKeys.includes(key)), 'external_proof_mismatch');
    if (result.receipt.verificationMethod === 'artifact_hash') assert(result.receipt.artifacts.every(artifact => typeof artifact.sha256 === 'string'
      && /^[a-f0-9]{64}$/u.test(artifact.sha256)), 'external_proof_unverified');
    // The ledger independently validates every effect, artifact and receipt
    // field before it spends the original finite reservation.
    return result;
  }
  function hold(invocationId) {
    try { invocations.markUncertain({ invocationId }); } catch { /* A committed terminal receipt or unavailable store remains authoritative. */ }
    return { outcome: 'held' };
  }
  async function deliver(value, invocationId, allowExecute) {
    let context;
    try {
      // The durable pre-effect marker is committed before any source call.
      // Recovery inspects an existing marker; it never makes a new attempt.
      context = invocations.inspectDispatch({ invocationId });
      assert(context.authorization.capabilityId === value.contract.capabilityId
        && context.authorization.version === value.contract.version
        && context.authorization.capabilityDigest === value.contract.digest
        && canonicalJson(context.target) === canonicalJson(value.entry.executionBinding)
        && ['resources', 'effects', 'recipients', 'charges'].every(key => canonicalJson(context.authorization[key]) === canonicalJson(value.entry[key])),
      'external_contract_mismatch');
      if (context.state === 'pending') {
        if (!allowExecute) return { outcome: 'not-started' };
        context = authority(value, { phase: 'dispatch', authorization: context.authorization },
          () => invocations.beginDispatch({ invocationId }));
      }
    } catch { return { outcome: 'held' }; }
    const inputDigest = canonicalHash(context.input);
    const request = freezeDeep({ requestId: context.internalRequestId, inputDigest, input: data(context.input),
      authorization: data(context.authorization) });
    async function read() {
      return proof(value, context, inputDigest, await bounded(signal => value.adapter.readProof(request, signal)));
    }
    function settle(result) {
      assert(!closed, 'external_adapter_closed');
      invocations.recordResult({ invocationId, status: 'succeeded', effectState: 'committed',
        effects: result.effects, receipt: result.receipt, disposition: 'spent' });
      return { outcome: 'committed' };
    }
    try {
      const before = await read();
      if (before.outcome === 'committed') return settle(before);
      if (before.outcome !== 'not_applied' || !allowExecute || context.cancelRequested) return hold(invocationId);
      // Recheck the original credential/deadline, current source binding and
      // app/resource authority after the network proof and just before send.
      const allowed = authority(value, { phase: 'execute', authorization: context.authorization }, () => {
        const current = invocations.reconcileAuthorization({ invocationId });
        return current.authorized === true && !current.invocation.cancelRequested && !TERMINAL.has(current.invocation.status);
      });
      if (!allowed) return hold(invocationId);
      try { await bounded(signal => value.adapter.execute(request, signal)); }
      catch { /* Source exception/timeout is not proof of no effect. */ }
      const after = await read();
      return after.outcome === 'committed' ? settle(after) : hold(invocationId);
    } catch { return hold(invocationId); }
  }
  async function run(value, invocationId, allowExecute) {
    const existing = inflight.get(invocationId);
    if (existing) return existing;
    assert(!closed && inflight.size < bounds.inflight, 'external_adapter_capacity');
    const pending = deliver(value, invocationId, allowExecute); inflight.set(invocationId, pending);
    try { return await pending; } finally { if (inflight.get(invocationId) === pending) inflight.delete(invocationId); }
  }
  function admit({ actor, reference, idempotencyKey, input }) {
      assert(!closed, 'external_adapter_closed'); const value = binding(reference), captured = data(input);
      return authority(value, { phase: 'admit', actor, input: captured }, () => invocations.admit({ actor,
        capabilityId: value.entry.capabilityId, version: value.entry.version, idempotencyKey, input: captured }));
  }
  function get({ actor, invocationId }) {
    const result = invocations.get({ actor, invocationId });
    const value = byKey.get(`${result.invocation.capabilityId}@${result.invocation.version}`);
    assert(value, 'external_adapter_not_registered'); selectedMetadata(actor, value); return result;
  }
  function selectedMetadata(actor, value) {
    const current = authorize({ actor, action: 'read', capabilityId: value.entry.capabilityId, version: value.entry.version });
    return authority(value, { phase: 'catalog', actor, authorization: current }, () => {
      authorize({ actor, action: 'read', capabilityId: value.entry.capabilityId, version: value.entry.version });
      return value.entry;
    });
  }
  function search(request) {
    const args = object(request, ['actor'], ['query', 'limit', 'cursor']);
    const query = args.query ?? '', limit = args.limit ?? 10;
    assert(typeof query === 'string' && query.length <= 200 && query.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(query), 'query_invalid');
    integer(limit, 1, 20); assert(args.cursor === undefined || typeof args.cursor === 'string' && args.cursor.length <= 512, 'cursor_invalid');
    const current = authorize({ actor: args.actor, action: 'history' });
    const normalized = query.normalize('NFKC').toLowerCase().trim(), tokens = normalized ? normalized.split(/\s+/u) : [];
    assert(tokens.length <= 12, 'query_invalid');
    const selected = [];
    const hidden = new Set(['access_denied', 'not_found', 'apps_access_denied', 'apps_owner_required', 'app_unavailable', 'external_resource_denied']);
    for (const value of byKey.values()) {
      let entry;
      try { entry = selectedMetadata(args.actor, value); }
      catch (error) { if (hidden.has(error?.code)) continue; throw error; }
      const haystack = [entry.capabilityId, entry.title, entry.description].join('\n').normalize('NFKC').toLowerCase();
      if (!tokens.every(token => haystack.includes(token))) continue;
      selected.push({ reference: value.contract, appId: entry.appId, title: entry.title, description: entry.description,
        resources: entry.resources, effects: entry.effects, recipients: entry.recipients,
        executionEnabled: entry.executionEnabled, availability: 'unprobed' });
    }
    selected.sort((a,b) => a.reference.capabilityId < b.reference.capabilityId ? -1 : a.reference.capabilityId > b.reference.capabilityId ? 1 : a.reference.version - b.reference.version);
    const revision = canonicalHash({ scope: [current.accountId, current.clientId, current.principalId, current.grantId], normalized, selected });
    let offset = 0;
    if (args.cursor !== undefined) {
      try {
        assert(/^[A-Za-z0-9_-]+$/u.test(args.cursor), 'cursor_invalid');
        const bytes = Buffer.from(args.cursor, 'base64url'); assert(bytes.toString('base64url') === args.cursor, 'cursor_invalid');
        const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes), decoded = JSON.parse(text);
        object(decoded, ['revision', 'offset'], [], 'cursor_invalid');
        assert(decoded.revision === revision && canonicalJson(decoded) === text, 'cursor_invalid'); offset = integer(decoded.offset, 0, selected.length, 'cursor_invalid');
      } catch { assert(false, 'cursor_invalid'); }
    }
    const items = [];
    const page = next => ({ schema: 'soty.authorized-app-capabilities.v1', scope: 'authorized', revision, items,
      total: selected.length, cursor: next < selected.length ? Buffer.from(canonicalJson({ revision, offset: next })).toString('base64url') : null });
    for (let i = offset; i < selected.length && items.length < limit; i++) {
      items.push(selected[i]);
      if (Buffer.byteLength(JSON.stringify(page(i+1))) > 65536) { items.pop(); break; }
    }
    assert(items.length > 0 || offset === selected.length, 'projection_too_large');
    authorize({ actor: args.actor, action: 'history' }); return freezeDeep(page(offset+items.length));
  }
  return Object.freeze({
    contracts: Object.freeze([...byKey.values()].map(value => value.contract)), admit, search,
    getContract({ actor, reference }) {
      const value = binding(reference), entry = selectedMetadata(actor, value);
      const result = { schema: 'soty.authorized-app-capability.v1', scope: 'authorized', reference: value.contract,
        appId: entry.appId, title: entry.title, description: entry.description, inputSchema: entry.inputSchema, outputSchema: entry.outputSchema,
        resources: entry.resources, effects: entry.effects, recipients: entry.recipients, binding: entry.executionBinding.binding,
        executionEnabled: entry.executionEnabled, availability: 'unprobed', skills: [], docs: [] };
      canonicalJson(result, { maxBytes: 384*1024 }); return freezeDeep(result);
    },
    async invoke(request) {
      const captured = object(request, ['actor', 'reference', 'idempotencyKey', 'input']);
      const value = binding(captured.reference);
      const accepted = admit(captured), invocationId = accepted.invocation.invocationId;
      if (!TERMINAL.has(accepted.invocation.status)) await run(value, invocationId, true);
      // Revoke after a source COMMIT may still settle a historical proof, but
      // the caller never receives it without this fresh original read check.
      return { reused: accepted.reused, ...get({ actor: captured.actor, invocationId }) };
    },
    async reconcile({ reference, invocationId }) {
      const value = binding(reference);
      return run(value, identifier(invocationId), false);
    },
    get,
    cancel({ actor, invocationId }) { get({ actor, invocationId }); return invocations.requestCancel({ actor, invocationId }); },
    close() { closed = true; for (const controller of outstanding.values()) controller.abort(); },
  });
}
