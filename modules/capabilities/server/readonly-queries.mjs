import {
  assert,
  canonicalHash,
  canonicalJson,
  freezeDeep,
  identifier,
  integer,
} from './validation.mjs';

export const READONLY_QUERY_PROFILE = 'soty.external-readonly-query.v1';
const approved = new WeakSet(),
  CODE = 'external_query_invalid';
function object(value, required, optional = []) {
  assert(
    value &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      [Object.prototype, null].includes(Object.getPrototypeOf(value)),
    CODE,
  );
  const fields = Object.getOwnPropertyDescriptors(value);
  assert(
    Reflect.ownKeys(fields).every(
      (key) =>
        typeof key === 'string' &&
        [...required, ...optional].includes(key) &&
        fields[key].enumerable &&
        'value' in fields[key],
    ) && required.every((key) => Object.hasOwn(fields, key)),
    CODE,
  );
  return Object.fromEntries(Object.entries(fields).map(([key, field]) => [key, field.value]));
}
export function captureQueryData(value) {
  let count = 0;
  function visit(item, depth = 0) {
    assert(++count <= 4096 && depth <= 18, 'external_payload_limit');
    if (item === null || typeof item === 'boolean') return item;
    if (typeof item === 'string') {
      assert(
        item.isWellFormed() && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(item),
        CODE,
      );
      return item;
    }
    if (typeof item === 'number') {
      assert(Number.isFinite(item), CODE);
      return item;
    }
    if (Array.isArray(item)) {
      const fields = Object.getOwnPropertyDescriptors(item);
      assert(item.length <= 128 && Reflect.ownKeys(fields).length === item.length + 1, CODE);
      return Array.from({ length: item.length }, (_, index) => {
        assert(fields[index] && 'value' in fields[index], CODE);
        return visit(fields[index].value, depth + 1);
      });
    }
    const fields = object(item, [], Object.keys(item ?? {}));
    return Object.fromEntries(
      Object.entries(fields).map(([key, child]) => [key, visit(child, depth + 1)]),
    );
  }
  const result = visit(value);
  canonicalJson(result, { maxBytes: 65536, maxDepth: 18 });
  return freezeDeep(result);
}
function pin(input) {
  const value = object(input, ['capabilityId', 'version', 'digest']);
  identifier(value.capabilityId, CODE);
  integer(value.version, 1, 1000000, CODE);
  assert(typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest), CODE);
  return Object.freeze(value);
}

/** Explicit host CODE admission, never a descriptor's readOnlyHint. This brand
 * is an in-process trust boundary, not cryptographic proof of harmless code.
 * The reviewed handler must also enforce a readonly Source credential/port. */
export function createTrustedReadonlyQueryAdapter(options) {
  const value = object(options, ['withAuthority', 'query']);
  assert(
    typeof value.withAuthority === 'function' &&
      value.withAuthority.constructor?.name !== 'AsyncFunction' &&
      typeof value.query === 'function',
    CODE,
  );
  const adapter = Object.freeze({
    profile: READONLY_QUERY_PROFILE,
    withAuthority: value.withAuthority,
    query: value.query,
  });
  approved.add(adapter);
  return adapter;
}
export function captureReadonlyQueryAdapters(input = []) {
  assert(Array.isArray(input) && input.length <= 64, CODE);
  const seen = new Set();
  return Object.freeze(
    input.map((raw) => {
      const value = object(raw, ['contract', 'adapter']),
        contract = pin(value.contract),
        key = contract.capabilityId + '@' + contract.version;
      assert(!seen.has(key) && approved.has(value.adapter), 'external_query_not_admitted');
      seen.add(key);
      return Object.freeze({ contract, adapter: value.adapter });
    }),
  );
}
export function captureReadonlyQueryLimits(limits = {}) {
  object(limits, [], ['inflight', 'callTimeoutMs']);
  const bounds = { inflight: limits.inflight ?? 4, callTimeoutMs: limits.callTimeoutMs ?? 8000 };
  integer(bounds.inflight, 1, 4, CODE);
  integer(bounds.callTimeoutMs, 1, 8000, CODE);
  return Object.freeze(bounds);
}
const terminal = new Set(['succeeded', 'failed', 'cancelled']);
/** Existing ledger only. No input/output cache or Source call inside a fence.
 * Charge=1 at durable dispatch CAS, even a crash before network delivery.
 * An exact retry discloses metadata only, never repeats a Source query. */
export function createReadonlyQueryCoordinator({
  entries,
  registry,
  core,
  authorize,
  limits = {},
}) {
  const bounds = captureReadonlyQueryLimits(limits);
  assert(core && typeof authorize === 'function', CODE);
  const byKey = new Map(
    entries.map((value) => {
      const entry = registry.get(value.contract.capabilityId, value.contract.version);
      assert(
        entry &&
          entry.digest === value.contract.digest &&
          entry.executionBinding.kind === 'registered' &&
          entry.effects.length === 0,
        'external_contract_mismatch',
      );
      return [entry.capabilityId + '@' + entry.version, { ...value, entry }];
    }),
  );
  let closed = false;
  const outstanding = new Map(),
    running = new Map();
  function binding(input) {
    const ref = pin(input),
      value = byKey.get(ref.capabilityId + '@' + ref.version);
    assert(value && value.contract.digest === ref.digest, 'external_query_not_admitted');
    return value;
  }
  function authority(value, request, callback) {
    let active = true,
      entered = false,
      poisoned = false,
      outcome;
    const sync = (result) => {
      if (result && typeof result.then === 'function') {
        Promise.resolve(result).catch(() => {});
        assert(false, 'external_authority_invalid');
      }
      return result;
    };
    try {
      const returned = value.adapter.withAuthority(Object.freeze(request), () => {
        if (!active || entered || closed) poisoned = true;
        assert(active && !entered && !closed, 'external_authority_invalid');
        entered = true;
        outcome = sync(callback());
        return outcome;
      });
      sync(returned);
      assert(entered && !poisoned, 'external_authority_invalid');
      return outcome;
    } finally {
      active = false;
    }
  }
  function current(value, actor, callback) {
    const authorization = authorize({
      actor,
      action: 'read',
      capabilityId: value.entry.capabilityId,
      version: value.entry.version,
    });
    return authority(value, { phase: 'query', actor, authorization }, () => {
      authorize({
        actor,
        action: 'read',
        capabilityId: value.entry.capabilityId,
        version: value.entry.version,
      });
      return callback(authorization);
    });
  }
  function response(info, { reused, result } = {}) {
    return freezeDeep({
      schema: 'soty.authorized-app-query.v1',
      scope: 'authorized',
      authority: 'application-data',
      invocation: info.invocation,
      reused: reused === true,
      resultUnavailable: result === undefined,
      charge: { unit: 'invocations', amount: info.charged ? 1 : 0, point: 'durable-dispatch-cas' },
      ...(result === undefined ? {} : { result }),
    });
  }
  async function bounded(invocationId, callback) {
    assert(!closed, 'external_adapter_closed');
    assert(outstanding.size < bounds.inflight, 'external_adapter_capacity');
    const controller = new AbortController();
    let timer;
    const source = Promise.resolve().then(() => {
      assert(!closed, 'external_adapter_closed');
      return callback(controller.signal);
    });
    outstanding.set(source, controller);
    running.set(invocationId, controller);
    source.then(
      () => outstanding.delete(source),
      () => outstanding.delete(source),
    );
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(
          Object.assign(new Error('external_query_unconfirmed'), {
            code: 'external_query_unconfirmed',
          }),
        );
      }, bounds.callTimeoutMs);
    });
    try {
      return await Promise.race([source, timeout]);
    } finally {
      clearTimeout(timer);
      running.delete(invocationId);
    }
  }
  function get({ actor, invocationId }) {
    const value = core.get({ actor, invocationId }),
      bindingValue = binding({
        capabilityId: value.invocation.capabilityId,
        version: value.invocation.version,
        digest: byKey.get(value.invocation.capabilityId + '@' + value.invocation.version)?.contract
          .digest,
      });
    return current(bindingValue, actor, () => value);
  }
  return Object.freeze({
    async query(request) {
      const args = object(request, ['actor', 'reference', 'idempotencyKey', 'input']),
        value = binding(args.reference),
        input = captureQueryData(args.input);
      registry.validateInput(value.entry, input);
      assert(!closed, 'external_adapter_closed');
      const accepted = current(value, args.actor, () => {
        // Retries stay available even when all timed-out promises are retained.
        return core.admit({
          actor: args.actor,
          reference: value.contract,
          idempotencyKey: args.idempotencyKey,
          input,
        });
      });
      const invocationId = accepted.invocation.invocationId;
      if (accepted.reused)
        return response(get({ actor: args.actor, invocationId }), { reused: true });
      let started = false;
      try {
        assert(outstanding.size < bounds.inflight, 'external_adapter_capacity');
        const claimed = current(value, args.actor, () =>
          core.claim({ actor: args.actor, invocationId, input }),
        );
        if (!claimed.claimed)
          return response(get({ actor: args.actor, invocationId }), { reused: true });
        started = true;
        const payload = freezeDeep({
          requestId: invocationId,
          input,
          authorization: captureQueryData(claimed.authorization),
        });
        const raw = await bounded(invocationId, (signal) =>
            value.adapter.query(payload, signal, () => {
              assert(!closed && !signal.aborted, 'external_query_unconfirmed');
              return current(value, args.actor, () => {
                const info = core.get({ actor: args.actor, invocationId });
                assert(
                  info.invocation.status === 'running' && !info.invocation.cancelRequested,
                  'external_query_unconfirmed',
                );
                return true;
              });
            }),
          ),
          result = captureQueryData(raw);
        try {
          registry.validateOutput(value.entry, result);
        } catch {
          assert(false, 'external_query_output_invalid');
        }
        const completed = current(value, args.actor, () =>
          core.finish({ invocationId, status: 'succeeded' }),
        );
        assert(
          completed.invocation.status === 'succeeded' && !completed.invocation.cancelRequested,
          'external_query_unconfirmed',
        );
        const info = get({ actor: args.actor, invocationId });
        return response(info, { reused: false, result });
      } catch (error) {
        try {
          core.finish({
            invocationId,
            status: started ? 'failed' : 'cancelled',
            errorCode: started ? 'external_query_unconfirmed' : 'readonly_query_not_started',
          });
        } catch {
          /* Durable old receipt remains authoritative. */
        }
        throw error;
      }
    },
    get,
    cancel({ actor, invocationId }) {
      const existing = get({ actor, invocationId }),
        value = byKey.get(existing.invocation.capabilityId + '@' + existing.invocation.version);
      const result = current(value, actor, () => core.cancel({ actor, invocationId }));
      running.get(invocationId)?.abort();
      return result;
    },
    close() {
      closed = true;
      for (const controller of outstanding.values()) controller.abort();
    },
  });
}
