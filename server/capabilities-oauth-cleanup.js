const INITIAL_DELAY = 10000, SWEEP_DELAY = 30000, RETRY_DELAY = 60000, LIMIT = 64;

/** One synchronous, bounded housekeeping page per timer turn. It neither
 * grants authority nor replays work; durable connection/history pins remain
 * the domain store's responsibility. No key or bearer enters this scheduler. */
export function startOAuthCleanup({ oauth, timers = { setTimeout, clearTimeout } }) {
  if (!oauth || typeof oauth.cleanup !== 'function') throw new Error('oauth_cleanup_configuration_invalid');
  let closed = false, timer = null;
  let snapshot = Object.freeze({ lastRunAt: null, deleted: 0, unavailable: false });
  function schedule(delay) {
    if (closed) return;
    timer = timers.setTimeout(tick, delay); timer?.unref?.();
  }
  function tick() {
    timer = null;
    if (closed) return;
    let delay = SWEEP_DELAY;
    try {
      const page = oauth.cleanup({ limit: LIMIT });
      if (page && typeof page.then === 'function') {
        Promise.resolve(page).catch(() => {}); throw new Error('oauth_cleanup_async_forbidden');
      }
      const counts = page && [page.artifactsDeleted, page.interactionsDeleted, page.credentialsDeleted];
      if (!counts || counts.some(value => !Number.isSafeInteger(value) || value < 0 || value > LIMIT)
        || counts.reduce((sum, value) => sum + value, 0) > LIMIT) throw new Error('oauth_cleanup_contract_invalid');
      snapshot = Object.freeze({ lastRunAt: Date.now(), deleted: counts.reduce((sum, value) => sum + value, 0), unavailable: false });
    } catch {
      snapshot = Object.freeze({ lastRunAt: Date.now(), deleted: 0, unavailable: true });
      delay = RETRY_DELAY;
    }
    schedule(delay);
  }
  schedule(INITIAL_DELAY);
  return Object.freeze({ status: () => snapshot,
    close() { if (closed) return; closed = true; if (timer !== null) timers.clearTimeout(timer); timer = null; },
  });
}
