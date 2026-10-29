/** The recipient has explicitly opened its own QR. Approval stays on the existing device. */
export function watchDeviceEnrollment({ client, label, isCurrent, onRequest, onWaiting, onComplete, onError }) {
  const lifetime = new AbortController();
  let stopped = false, timer, request, scope;
  const live = () => !stopped && isCurrent();
  const stale = () => Object.assign(new Error('The selected account changed.'), { code: 'ACTIVE_PROFILE_CHANGED' });
  const stop = () => { stopped = true; clearTimeout(timer); lifetime.abort(); };
  const fail = error => { if (live()) { stop(); onError(error); } };
  async function checked() {
    if (!live()) throw stale();
    const state = await client.getLocalState();
    if (!live()) throw stale();
    if (state.current?.revoked) throw Object.assign(new Error('Device revoked.'), { code: 'DEVICE_REVOKED' });
    if (scope && (state.deviceId !== scope.deviceId || state.accountId !== scope.accountId || state.localRevision !== scope.localRevision)) throw stale();
    return state;
  }
  async function poll() {
    try {
      await checked();
      const preview = await client.previewEnrollment(request.requestId);
      await checked();
      if (preview.status !== 'finished' && preview.expiresAt <= Date.now()) throw Object.assign(new Error('QR expired.'), { code: 'ENROLLMENT_EXPIRED' });
      if (preview.account) {
        onWaiting(preview);
        const result = await client.finishEnrollment(request.requestId, preview.account.accountId, { signal: lifetime.signal });
        if (!live()) return;
        const saved = await client.getLocalState();
        if (!live()) return;
        if (saved.accountId !== result.accountId || saved.deviceId !== result.deviceId) throw stale();
        stop(); onComplete(result); return;
      }
      onWaiting(null);
    } catch (error) {
      if (!live()) return;
      if (!['NETWORK_ERROR', 'NETWORK_TIMEOUT'].includes(String(error?.code).toUpperCase())) { fail(error); return; }
      onWaiting(null, error);
    }
    if (live()) timer = setTimeout(() => { void poll(); }, 1200);
  }
  const ready = (async () => {
    try {
      const before = await checked();
      if (before.pendingRecovery) throw Object.assign(new Error('Recovery pending.'), { code: 'RECOVERY_PENDING' });
      request = before.pendingEnrollment?.requestId ? before.pendingEnrollment : await client.startEnrollment(before.pendingEnrollment?.label || label);
      const after = await checked();
      if (after.accountId !== before.accountId || after.deviceId !== before.deviceId) throw stale();
      scope = after;
      await onRequest(request);
      if (live()) void poll();
    } catch (error) { fail(error); }
  })();
  return { ready, stop };
}
