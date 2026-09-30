import { AccessError, assert, exact, integer, text } from './validation.mjs';

const asyncFunction = value => ['AsyncFunction', 'AsyncGeneratorFunction'].includes(value?.constructor?.name);

function data(value, keys, code) {
  exact(value, keys, code);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    assert(typeof key === 'string' && keys.includes(key) && descriptor.enumerable
      && Object.hasOwn(descriptor, 'value'), code);
  }
  for (const key of keys) assert(Object.hasOwn(value, key), code);
}

/** This is trusted host composition, never part of the caller's JSON body. */
export function normalizeDelegationConfiguration(value) {
  if (value === undefined) return null;
  data(value, ['audience', 'withAuthorityFence'], 'delegation_configuration_invalid');
  assert(typeof value.audience === 'string' && value.audience.length <= 2048, 'delegation_configuration_invalid');
  let url;
  try { url = new URL(value.audience); } catch { assert(false, 'delegation_configuration_invalid'); }
  assert(url.origin === value.audience && !url.username && !url.password
    && (url.protocol === 'https:' || url.protocol === 'http:'
      && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)), 'delegation_configuration_invalid');
  assert(typeof value.withAuthorityFence === 'function' && !asyncFunction(value.withAuthorityFence), 'delegation_configuration_invalid');
  return Object.freeze({ audience: value.audience, withAuthorityFence: value.withAuthorityFence });
}

function synchronous(value) {
  if (value && typeof value.then === 'function') {
    void Promise.resolve(value).catch(() => {});
    throw new AccessError('delegation_context_invalid');
  }
  return value;
}

/** The only exported operation issues a new, non-replayable secret. All actor
 * resolution and SQL stay in the private Access closure. Connect owns the
 * outer authority fence; neither an identity nor a device is invented here. */
export function createDelegationCoordinator({ configuration, transaction, ensureOpen, deriveInTransaction }) {
  let running = false;
  return Object.freeze({
    derive(request) {
      ensureOpen();
      assert(configuration && typeof deriveInTransaction === 'function', 'delegation_unavailable');
      assert(!running, 'delegation_context_invalid');
      data(request, ['actor', 'label', 'expiresAt'], 'invalid_input');
      const label = text(request.label, { max: 100 });
      assert(label.isWellFormed(), 'invalid_unicode');
      const expiresAt = integer(request.expiresAt, 0, Number.MAX_SAFE_INTEGER, 'expiry_invalid');
      const captured = Object.freeze({ actor: request.actor, label, expiresAt, audience: configuration.audience });
      let accepting = true, invoked = false, result;
      running = true;
      try {
        const output = configuration.withAuthorityFence(() => {
          assert(accepting && running && !invoked, 'delegation_context_invalid');
          invoked = true;
          result = transaction(() => synchronous(deriveInTransaction(captured)), { busyMs: 100 });
          return result;
        });
        synchronous(output);
        assert(invoked && output === result, 'delegation_context_invalid');
        return result;
      } catch (error) {
        if (error?.code === 'ERR_SQLITE_ERROR' && [5, 6].includes(Number(error.errcode) & 255)) {
          throw new AccessError('delegation_storage_busy');
        }
        throw error;
      } finally { accepting = false; running = false; }
    }
  });
}
