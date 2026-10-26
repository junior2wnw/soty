/** Reconcile a durable owner-requested job through the existing Apps/Universal services. */
export function createAutomaticAppRegistration({ store, apps, universal, connect }) {
  const pending = new Map(), flights = new Map(), problems = new Map();
  let closed = false;
  const failure = error => {
    const code = String(error?.code || '');
    if (code === 'app_port_already_registered' || code === 'app_job_id_conflict') return 'conflict';
    if (['apps_device_not_owned', 'apps_authentication_required', 'connect_authority_actor_mismatch', 'device_revoked', 'authentication_required'].includes(code)) return 'access-lost';
    return 'waiting';
  };
  async function reconcile(jobId) {
    if (closed) return;
    if (flights.has(jobId)) return flights.get(jobId);
    const flight = (async () => {
      const [job] = await store.automaticAppJobs(jobId);
      if (closed) return;
      if (!job) { pending.delete(jobId); problems.delete(jobId); return; }
      pending.set(job.id, { linkId: job.linkId, deviceId: job.deviceId, connectorId: job.connectorId });
      if (!universal) { problems.set(job.id, 'waiting'); return; }
      const actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
      try {
        // This fence rechecks the persisted original actor immediately around
        // the synchronous Apps, universal registration and feedback work.
        const result = connect.withCurrentContacts(actor, () => {
          const device = apps.resolveOwnedDevice(actor, job.deviceId, job.connectorId);
          if (device.linkId !== job.linkId) throw Object.assign(new Error('apps_device_not_owned'), { code: 'apps_device_not_owned' });
          return universal.registerCreationJob(actor, job);
        });
        if (result.app.state === 'revoked') { pending.delete(job.id); problems.delete(job.id); return; }
        if (result.universalRegistration?.state === 'ready' && result.universalRegistration.feedback?.state === 'ready') {
          pending.delete(job.id); problems.delete(job.id);
        } else problems.set(job.id, 'waiting');
      } catch (error) {
        const problem = failure(error); problems.set(job.id, problem);
        if (problem === 'access-lost') pending.delete(job.id);
      }
    })().finally(() => flights.delete(jobId));
    flights.set(jobId, flight); return flight;
  }
  async function recover() {
    const jobs = await store.automaticAppJobs();
    for (const job of jobs) { if (closed) return; await reconcile(job.id); }
  }
  function poll(identity) {
    if (closed) return;
    for (const [id, target] of pending) if (target.linkId === identity.linkId && target.deviceId === identity.deviceId && target.connectorId === identity.connectorId)
      void reconcile(id).catch(() => {});
  }
  function retryPending() {
    if (closed) return;
    for (const id of [...pending.keys()].slice(0, 10)) {
      const target = pending.get(id); pending.delete(id); pending.set(id, target);
      void reconcile(id).catch(() => {});
    }
  }
  function status(actor, job) {
    if (!job || !['succeeded', 'failed', 'cancelled'].includes(job.status)) return { state: 'creating' };
    if (job.status !== 'succeeded' || !job.result?.appProposal) return { state: 'failed' };
    try {
      const app = apps.creationJobApp(actor, job.id);
      if (!app) return { state: problems.get(job.id) || 'waiting' };
      if (app.state === 'revoked') return { state: 'revoked', appId: app.id };
      let value;
      try { value = universal?.registration.execute({ op: 'apps.universal.get', actor,
        args: { appId: app.id, expectedAccountId: actor.accountId } }).registration; } catch { /* App launch does not depend on feedback. */ }
      const ready = value?.state === 'ready' && value.feedback?.state === 'ready';
      if (!ready && problems.get(job.id) === 'access-lost') return { state: 'access-lost', appId: app.id, appState: app.state };
      return { state: 'ready', appId: app.id, appState: app.state, feedback: ready ? 'ready' : 'waiting' };
    } catch { return { state: 'waiting' }; }
  }
  return { reconcile, recover, poll, retryPending, status, close() { closed = true; pending.clear(); } };
}
