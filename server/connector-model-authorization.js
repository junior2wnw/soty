// Trusted ConnectorStore capability for the existing opt-in account-owned
// executor. Not a new device role, inference node, wallet or application token.
const watches = new WeakMap();
const activeStates = new Set(['leased', 'running']);
export function captureConnectorModelAuthorization(store, tokenHash, { sameHash, hashToken, changeKey, freshMs }) {
  const matching = () => store.state.connectors.filter(item => item.protocol === 2 && sameHash(item.tokenHash, tokenHash));
  // Explicit compatibility: a host without current account-owned execution
  // keeps its pre-existing token authorization; no new policy is inferred.
  if (!store.ownedJobActive) return matching().length > 0;
  const matches = matching();
  if (matches.length !== 1) return false;
  const connector = matches[0];
  const binding = Object.freeze({ linkId: connector.linkId, deviceId: connector.deviceId, connectorId: connector.connectorId });
  const jobs = store.state.jobs.filter(job => job.schema === 'soty.connector-job.v4'
    && job.linkId === binding.linkId && job.deviceId === binding.deviceId && job.connectorId === binding.connectorId
    && job.requestedConnectorId === binding.connectorId && activeStates.has(job.status)
    && !job.cancelRequested && !job.executionUncertain && job.leaseUntil > store.now());
  if (jobs.length !== 1) return false;
  const held = Object.freeze({ jobId: jobs[0].id, ownerAccountId: jobs[0].ownerAccountId, sourceActorDeviceId: jobs[0].sourceActorDeviceId });
  const current = () => {
    if (store.closing || store.failed || store.persistence.failed) return false;
    const candidates = matching();
    if (candidates.length !== 1) return false;
    const device = candidates[0];
    if (device.linkId !== binding.linkId || device.deviceId !== binding.deviceId || device.connectorId !== binding.connectorId
      || store.now() - device.lastSeenAt > freshMs) return false;
    const job = store.state.jobs.find(item => item.id === held.jobId);
    if (!job || job.schema !== 'soty.connector-job.v4' || job.linkId !== binding.linkId || job.deviceId !== binding.deviceId
      || job.connectorId !== binding.connectorId || job.requestedConnectorId !== binding.connectorId
      || job.ownerAccountId !== held.ownerAccountId || job.sourceActorDeviceId !== held.sourceActorDeviceId
      || !held.sourceActorDeviceId || !activeStates.has(job.status) || job.cancelRequested || job.executionUncertain
      || job.leaseUntil <= store.now()) return false;
    try { return store.ownedJobActive(job) === true; } catch { return false; }
  };
  if (!current()) return false;
  const identity = hashToken(JSON.stringify({ ...binding, ...held }));
  return Object.freeze({ identity, assertCurrent: current,
    subscribe(onInvalidated) {
      if (typeof onInvalidated !== 'function') throw TypeError('model_authorization_observer_required');
      if (!current()) { onInvalidated(); return () => {}; }
      let group = watches.get(store); if (!group) { group = new Map(); watches.set(store, group); }
      let watch = group.get(identity);
      if (!watch) {
        const listeners = new Set(), keys = [changeKey(binding.linkId, binding.deviceId), 'model-authorization-close'];
        const stop = () => { clearInterval(watch.timer); for (const key of keys) store.events.removeListener(key, check);
          if (group.get(identity) === watch) group.delete(identity); };
        const check = () => { if (current()) return; const observers = [...listeners]; listeners.clear(); stop();
          for (const listener of observers) { try { listener(); } catch {} } };
        watch = { check, listeners, stop, timer: setInterval(check, 1000) }; watch.timer.unref?.(); group.set(identity, watch);
        for (const key of keys) store.events.on(key, check);
      }
      watch.listeners.add(onInvalidated); watch.check(); let subscribed = true;
      return () => { if (!subscribed) return; subscribed = false; watch.listeners.delete(onInvalidated); if (!watch.listeners.size) watch.stop(); };
    },
  });
}
// Invoked by the existing Connect revoke notification, without waiting for a
// connector poll. One periodic freshness watch exists per admitted job, not RPC.
export function recheckConnectorModelAuthorizations(store) {
  for (const watch of [...watches.get(store)?.values() ?? []]) watch.check();
}
