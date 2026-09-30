import { AppsError, assertApps, textId } from './protocol.mjs';

export function synchronous(value, code) {
  if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); throw new AppsError(code, 500); }
  return value;
}

// The caller supplies a trusted host World fence. This synchronous wrapper
// preserves Connect → World → Apps order and restores the shared connection's
// previous timeout. A host-release error after COMMIT is an unknown ACK.
export function createEngagementTransaction({ db, assertActor, withAuthorityFence, responseBytes,
  busyCode = 'apps_saved_busy', timeoutCode = 'apps_saved_timeout_invalid', responseCode = 'apps_saved_response_too_large' }) {
  function authenticate(actor) {
    assertApps(synchronous(assertActor(actor), 'apps_async_authority') !== false, 'apps_authentication_required', 401);
    textId(actor?.accountId); textId(actor?.deviceId);
  }
  return function run(actor, callback) {
    authenticate(actor);
    const captured = Object.freeze({ accountId: actor.accountId, deviceId: actor.deviceId,
      ...(typeof actor.label === 'string' ? { label: actor.label } : {}) });
    assertApps(typeof withAuthorityFence === 'function' && withAuthorityFence.constructor?.name !== 'AsyncFunction',
      'apps_authority_fence_required', 503);
    let active = true, entered = false, outcome;
    try {
      const value = withAuthorityFence(() => {
        assertApps(active && !entered, 'apps_authority_fence_invalid', 500); entered = true;
        assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
        const priorTimeout = db.prepare('PRAGMA busy_timeout').get().timeout;
        assertApps(Number.isSafeInteger(priorTimeout) && priorTimeout >= 0, timeoutCode, 500);
        db.exec('PRAGMA busy_timeout=100');
        try {
          db.exec('BEGIN IMMEDIATE');
          authenticate(captured);
          outcome = synchronous(callback(captured), 'apps_async_transaction');
          assertApps(Buffer.byteLength(JSON.stringify(outcome), 'utf8') <= responseBytes, responseCode, 500);
          db.exec('COMMIT'); return outcome;
        } catch (error) {
          if (db.isTransaction) db.exec('ROLLBACK');
          if (Number.isInteger(error?.errcode) && [5, 6].includes(error.errcode & 255)) throw new AppsError(busyCode, 503);
          throw error;
        } finally { db.exec(`PRAGMA busy_timeout=${priorTimeout}`); }
      });
      synchronous(value, 'apps_authority_fence_invalid');
      assertApps(entered, 'apps_authority_fence_invalid', 500);
      return outcome;
    } finally { active = false; }
  };
}
