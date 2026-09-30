const INITIAL_DELAY = 1000, PAGE_DELAY = 2000, SWEEP_DELAY = 30000, MAX_BACKOFF = 60000;
const OUTCOMES = new Set(['committed', 'not_applied', 'retryable', 'held']);

/** One bounded, synchronous recovery page per timer turn. Recovery observes and
 * settles durable evidence; it never starts or replays an effect. No timer keeps
 * the process alive, and closing the host cancels the next turn before DB close. */
export function startNativeRecovery({ coordinator, timers = { setTimeout, clearTimeout } }) {
  if (!coordinator || typeof coordinator.reconcilePage !== 'function') throw new Error('native_recovery_configuration_invalid');
  let closed = false, timer = null, cursor, failures = 0;
  let snapshot = Object.freeze({ lastRunAt: null, checked: 0, committed: 0, failed: 0, pending: 0, unavailable: false });
  function schedule(delay) {
    if (closed) return;
    timer = timers.setTimeout(tick, delay); timer?.unref?.();
  }
  function tick() {
    timer = null;
    if (closed) return;
    let delay = SWEEP_DELAY;
    try {
      const page = coordinator.reconcilePage(cursor === undefined ? {} : { cursor });
      if (page && typeof page.then === 'function') {
        // A broken trusted port is not a recovery page; consume a rejection so
        // the rejected sync contract cannot crash the host later.
        Promise.resolve(page).catch(() => {}); throw new Error('native_recovery_async_forbidden');
      }
      if (!page || !Array.isArray(page.items) || page.items.length > 16
        || !(page.nextCursor === null || typeof page.nextCursor === 'string' && /^[A-Za-z0-9_-]{1,512}$/u.test(page.nextCursor))) {
        throw new Error('native_recovery_contract_invalid');
      }
      let committed = 0, failed = 0, pending = 0;
      for (const item of page.items) {
        if (item.errorCode === 'native_reconciliation_failed' && item.outcome === undefined) failed++;
        else if (OUTCOMES.has(item.outcome) && item.errorCode === undefined) {
          if (item.outcome === 'committed') committed++;
          else if (item.outcome === 'held' || item.outcome === 'retryable') pending++;
        } else throw new Error('native_recovery_contract_invalid');
      }
      if (page.nextCursor !== null && (page.items.length === 0 || page.nextCursor === cursor)) throw new Error('native_recovery_cursor_stalled');
      cursor = page.nextCursor ?? undefined;
      failures = 0;
      snapshot = Object.freeze({ lastRunAt: Date.now(), checked: page.items.length, committed, failed, pending, unavailable: false });
      delay = cursor === undefined ? SWEEP_DELAY : PAGE_DELAY;
    } catch {
      failures = Math.min(failures + 1, 6);
      delay = Math.min(MAX_BACKOFF, PAGE_DELAY * 2 ** failures);
      snapshot = Object.freeze({ lastRunAt: Date.now(), checked: 0, committed: 0, failed: 0, pending: 0, unavailable: true });
    }
    schedule(delay);
  }
  schedule(INITIAL_DELAY);
  return Object.freeze({
    status: () => snapshot,
    close() { if (closed) return; closed = true; if (timer !== null) timers.clearTimeout(timer); timer = null; },
  });
}
