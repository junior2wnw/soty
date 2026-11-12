const KEY = 'soty.app-host-retirement.v1';
const SCHEMA = 'soty.app-host-retirement.v1';
const MAX_RECORDS = 512, MAX_BYTES = 256 * 1024;
const token = value => typeof value === 'string' && /^[A-Za-z0-9_-]{16,128}$/.test(value);
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{3,160}$/.test(value);
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/.test(value);
const positive = value => Number.isSafeInteger(value) && value > 0;
const exact = (value, fields) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === fields;
const fail = (code = 'app_host_retirement_unavailable') => { throw Object.assign(new Error(code), { code }); };
const check = (value, code) => { if (!value) fail(code); };
function captureScope(value) {
  check(exact(value, 'accountId,deviceId') && id(value.accountId) && id(value.deviceId));
  return Object.freeze({ accountId: value.accountId, deviceId: value.deviceId });
}
function captureProof(value) {
  check(exact(value, 'appId,hostSession') && /^app-[a-f0-9]{32}$/.test(value.appId ?? ''));
  const proof = value.hostSession;
  check(exact(proof, 'expiresAt,frameId,lease,revision,schema') && proof.schema === 'soty.app-host-session.v1'
    && opaque(proof.frameId) && opaque(proof.lease) && positive(proof.revision) && positive(proof.expiresAt));
  return { appId: value.appId, frameId: proof.frameId, lease: proof.lease, revision: proof.revision, expiresAt: proof.expiresAt };
}
const sameScope = (a, b) => a.accountId === b.accountId && a.deviceId === b.deviceId;
const sameProof = (a, b) => sameScope(a, b) && ['appId', 'frameId', 'lease', 'revision'].every(key => a[key] === b[key]);
function captureLedger(value) {
  check(exact(value, 'lineageId,records,schema') && value.schema === SCHEMA && token(value.lineageId)
    && Array.isArray(value.records) && value.records.length <= MAX_RECORDS);
  const records = value.records.map(record => {
    check(exact(record, 'accountId,appId,deviceId,documentId,expiresAt,frameId,lease,retired,revision')
      && token(record.documentId) && typeof record.retired === 'boolean');
    const scope = captureScope({ accountId: record.accountId, deviceId: record.deviceId });
    const proof = captureProof({ appId: record.appId, hostSession: { schema: 'soty.app-host-session.v1',
      frameId: record.frameId, lease: record.lease, revision: record.revision, expiresAt: record.expiresAt } });
    return { ...scope, ...proof, documentId: record.documentId, retired: record.retired };
  });
  check(records.every((record, index) => !records.slice(0, index).some(prior => sameProof(prior, record))));
  return { schema: SCHEMA, lineageId: value.lineageId, records };
}

/** Root-private retirement obligations, never passive selectors or Source readiness.
 * Construct once per Root document. A lifetime Web Lock fences cloned tab storage.
 * Initial admission can extend the same proof's expiry without a received ACK;
 * only a signed exact-proof close result discharges the obligation. */
export function createAppHostRetirement({ storage, locks, close, randomId = () => crypto.randomUUID() } = {}) {
  let ledger = null, owner = null, initialization = null, documentId = null, saved = null, queue = Promise.resolve();
  const currentStorage = () => { try { check(storage.getItem(KEY) === saved); } catch { fail(); } };
  function write(next) {
    const text = JSON.stringify(next);
    check(text.length <= MAX_BYTES && next.records.length <= MAX_RECORDS);
    currentStorage();
    try { storage.setItem(KEY, text); } catch { fail(); }
    ledger = next; saved = text;
  }
  const held = () => owner?.held === true;
  function claim(lineageId) {
    return new Promise(resolve => {
      const candidate = { held: false };
      let settled = false;
      const finish = value => { if (!settled) { settled = true; resolve(value); } };
      try {
        Promise.resolve(locks.request(KEY + ':' + lineageId, { ifAvailable: true }, async lock => {
          if (!lock) { finish(null); return; }
          candidate.held = true; finish(candidate);
          // Only document termination releases this lock; profile/navigation
          // callbacks must not make a still-connected iframe reclaimable.
          await new Promise(() => {});
        })).then(() => { candidate.held = false; finish(null); }, () => { candidate.held = false; finish(null); });
      } catch { finish(null); }
    });
  }
  function initialize() {
    if (initialization) return initialization;
    initialization = (async () => {
      if (!storage?.getItem || !storage?.setItem || !locks?.request || typeof close !== 'function') return false;
      let raw, initial;
      try {
        documentId = randomId(); check(token(documentId));
        raw = storage.getItem(KEY);
        check(raw === null || typeof raw === 'string' && raw.length <= MAX_BYTES);
        initial = raw === null ? { schema: SCHEMA, lineageId: randomId(), records: [] } : captureLedger(JSON.parse(raw));
        check(token(initial.lineageId));
      } catch { return false; }
      owner = await claim(initial.lineageId);
      if (!owner) {
        // sessionStorage may have been cloned from a live tab. Fork only this
        // copy; the original tab's lock and obligations remain untouched.
        const nextId = randomId(); check(token(nextId) && nextId !== initial.lineageId);
        initial = { schema: SCHEMA, lineageId: nextId, records: [] };
        owner = await claim(nextId);
        if (!owner) return false;
      }
      try { check(storage.getItem(KEY) === raw && held()); saved = raw; write(initial); } catch { return false; }
      return true;
    })();
    return initialization;
  }
  function beforeLaunch(input, isCurrent) {
    const scope = captureScope(input);
    check(typeof isCurrent === 'function');
    const task = queue.then(async () => {
      check(isCurrent(), 'app_host_retirement_changed');
      if (!await initialize()) return false;
      check(held() && isCurrent(), 'app_host_retirement_changed');
      currentStorage();
      const candidates = ledger.records.filter(record => sameScope(record, scope)
        && (record.documentId !== documentId || record.retired));
      for (const record of candidates) {
        check(held() && isCurrent(), 'app_host_retirement_changed');
        currentStorage();
        let result;
        try { result = await close(scope, { appId: record.appId, frameId: record.frameId, lease: record.lease, revision: record.revision }); }
        catch { fail('app_host_retirement_pending'); }
        check(held() && isCurrent(), 'app_host_retirement_changed');
        // false is the signed exact-proof stale/already-closed CAS result.
        check(exact(result, 'closed') && typeof result.closed === 'boolean', 'app_host_retirement_pending');
        write({ ...ledger, records: ledger.records.filter(value => !sameProof(value, record)) });
      }
      return true;
    });
    queue = task.catch(() => {});
    return task;
  }
  function record(input, value, confirmed = false) {
    const scope = captureScope(input), proof = captureProof(value);
    check(typeof confirmed === 'boolean');
    check(held() && ledger);
    const found = ledger.records.find(value => sameProof(value, { ...scope, ...proof }));
    check(!found || found.documentId === documentId, 'app_host_retirement_pending');
    check(!confirmed || found?.documentId === documentId, 'app_host_retirement_pending');
    const entry = { ...scope, ...proof, documentId, retired: found?.retired ?? false,
      expiresAt: Math.max(found?.expiresAt ?? 0, proof.expiresAt) };
    // The Root caller supplies this bit only after a signed exact-proof
    // confirmation/current-context result, never for a prepared candidate.
    const predecessor = value => confirmed && value.documentId === documentId && sameScope(value, entry)
      && value.appId === entry.appId && value.frameId === entry.frameId && value.revision < entry.revision;
    write({ ...ledger, records: [...ledger.records.filter(value => !sameProof(value, entry) && !predecessor(value)), entry] });
    return () => {
      check(held());
      const current = ledger.records.find(value => sameProof(value, entry));
      if (!current || current.documentId !== documentId || current.retired) return;
      write({ ...ledger, records: ledger.records.map(value => sameProof(value, entry) ? { ...value, retired: true } : value) });
    };
  }
  return Object.freeze({ beforeLaunch, record });
}
