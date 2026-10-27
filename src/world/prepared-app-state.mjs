const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{3,160}$/u.test(value);
const error = code => { throw Object.assign(new Error(code), { code }); };
const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
function clean(value, withRequest = false) {
  if (!value || !id(value.expectedAccountId) || !id(value.hostDeviceId) || !id(value.connectorId)
    || withRequest && !id(value.requestId) || typeof value.manifestDigest !== 'string' || !/^[a-f0-9]{64}$/u.test(value.manifestDigest)) error('prepared_local_invalid');
  const location = value.location;
  if (!location || typeof location.rootId !== 'string' || !/^[a-f0-9]{64}$/u.test(location.rootId)
    || !Array.isArray(location.segments) || location.segments.length > 32 || location.segments.some(value => typeof value !== 'string'
      || !value.length || value.length > 255 || !value.isWellFormed() || value === '.' || value === '..' || /[\\/\u0000-\u001f\u007f]/u.test(value))
    || new TextEncoder().encode(JSON.stringify(location)).byteLength > 4096) error('prepared_local_invalid');
  return { expectedAccountId: value.expectedAccountId, hostDeviceId: value.hostDeviceId, connectorId: value.connectorId,
    location: { rootId: location.rootId, segments: [...location.segments] }, manifestDigest: value.manifestDigest,
    ...(withRequest ? { requestId: value.requestId } : {}) };
}

// The marker is written before a request and remains until the server confirms
// a card or tombstone. Web Locks serialize same-account tabs. Several unknown
// results can coexist; an unrelated new project need not erase or replay one.
export function createPreparedAppState({ accountId, deviceId, storage, locks, randomId = () => crypto.randomUUID() }) {
  if (!id(accountId) || !id(deviceId)) error('prepared_local_invalid');
  const key = `soty.prepared-app.v1:${accountId}:${deviceId}`;
  const lock = action => {
    if (!locks?.request) return Promise.reject(Object.assign(new Error('prepared_local_unavailable'), { code: 'prepared_local_unavailable' }));
    return locks.request(key, action);
  };
  function read() {
    try {
      const raw = storage.getItem(key); if (raw === null) return [];
      if (typeof raw !== 'string' || raw.length > 120000) error('prepared_local_unavailable');
      const record = JSON.parse(raw);
      if (record?.schema !== 1 || record.accountId !== accountId || record.deviceId !== deviceId || !Array.isArray(record.pending)
        || record.pending.length > 16) error('prepared_local_unavailable');
      const values = record.pending.map(value => clean(value, true));
      if (values.some(value => value.expectedAccountId !== accountId) || new Set(values.map(value => value.requestId)).size !== values.length)
        error('prepared_local_unavailable');
      return values;
    } catch { error('prepared_local_unavailable'); }
  }
  function write(pending) {
    try { storage.setItem(key, JSON.stringify({ schema: 1, accountId, deviceId, pending })); }
    catch { error('prepared_local_unavailable'); }
  }
  return Object.freeze({
    read,
    async prepare(input) {
      const value = clean(input); if (value.expectedAccountId !== accountId) error('prepared_local_invalid');
      return lock(() => {
        const pending = read(), existing = pending.find(item => { const { requestId: _id, ...prior } = item; return same(prior, value); });
        if (existing) return { payload: existing, created: false };
        if (pending.length >= 16) error('prepared_local_capacity');
        const requestId = randomId(); if (!id(requestId)) error('prepared_local_invalid');
        const next = { ...value, requestId }; write([...pending, next]); return { payload: next, created: true };
      });
    },
    async acknowledge(expected) {
      const exact = clean(expected, true);
      return lock(() => { const pending = read(), match = pending.find(value => same(value, exact));
        if (!match) return false; write(pending.filter(value => value.requestId !== exact.requestId)); return true; });
    },
  });
}
