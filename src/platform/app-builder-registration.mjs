/** Remember an attempted registration before dispatch so a lost response or
 * a second tab can only inspect it, never create a second write. */
export function createAppBuilderRegistration({ accountId, pending, proposal, storage, locks, randomId = () => crypto.randomUUID() }) {
  const key = `soty.app-registration.v1:${accountId}:${pending.jobId}`;
  const intent = JSON.stringify({ accountId, hostDeviceId: pending.hostDeviceId, connectorId: pending.connectorId,
    jobId: pending.jobId, name: proposal.name, port: proposal.port, entryPath: proposal.entryPath });
  const fail = code => { throw Object.assign(new Error(code), { code }); };
  const locked = action => {
    if (!locks?.request) return Promise.reject(Object.assign(new Error('app_create_lock_unavailable'), { code: 'app_create_lock_unavailable' }));
    return locks.request(key, action);
  };
  const read = () => {
    let saved;
    try { saved = storage.getItem(key); } catch { fail('app_create_storage_unavailable'); }
    if (saved === null) return null;
    try {
      const value = JSON.parse(saved);
      if (value.intent !== intent || typeof value.attempt !== 'string' || !/^[A-Za-z0-9-]{1,100}$/u.test(value.attempt)
        || Object.keys(value).sort().join(',') !== 'attempt,intent') fail('app_registration_conflict');
      return value.attempt;
    } catch { fail('app_registration_conflict'); }
  };
  const write = () => {
    const attempt = randomId();
    if (typeof attempt !== 'string' || !/^[A-Za-z0-9-]{1,100}$/u.test(attempt)) fail('app_registration_conflict');
    try { storage.setItem(key, JSON.stringify({ intent, attempt })); } catch { fail('app_create_storage_unavailable'); }
    return { dispatch: true, attempt };
  };
  return {
    begin() {
      return locked(() => {
        const attempt = read();
        return attempt ? { dispatch: false, attempt } : write();
      });
    },
    retry(expectedAttempt) {
      return locked(() => {
        const current = read();
        if (current !== expectedAttempt) return { dispatch: false, attempt: current };
        return write();
      });
    },
    inspect() { return locked(read); },
    clear(expectedAttempt) {
      return locked(() => {
        if (!expectedAttempt || read() !== expectedAttempt) return;
        try { storage.removeItem(key); } catch { fail('app_create_storage_unavailable'); }
      });
    },
  };
}
