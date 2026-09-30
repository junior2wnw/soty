import { validateAppLaunchPath } from './app-launch.mjs';

const APP = /^app-[a-f0-9]{32}$/u, DOMAIN = /^dom_[a-f0-9]{32}$/u;
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value);
const number = value => Number.isSafeInteger(value) && value >= 0;
const record = value => value && typeof value === 'object' && !Array.isArray(value);
const clone = value => structuredClone(value), same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const check = (value, code = 'app_saved_invalid_data') => { if (!value) fail(code); };
const size = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
const bus = new Map();
const notify = key => { for (const listener of bus.get(key) ?? []) listener(); };
const safeLabel = (value, maximum) => typeof value === 'string' && value.trim() === value && value.length > 0 && value.length <= maximum
  && value.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(value);
export function normalizeAppEntry(value) {
  check(record(value) && typeof value.appId === 'string' && APP.test(value.appId)
    && typeof value.domainId === 'string' && DOMAIN.test(value.domainId));
  const path = validateAppLaunchPath(value.path);
  check(`/_soty/boot?${new URLSearchParams({ path })}`.length <= 8192);
  check(typeof value.origin === 'string' && value.origin.length <= 512);
  let origin; try { origin = new URL(value.origin); } catch { fail('app_saved_invalid_data'); }
  check(origin.origin === value.origin && ['http:', 'https:'].includes(origin.protocol) && !origin.username && !origin.password);
  return { appId: value.appId, domainId: value.domainId, origin: value.origin, path };
}
function savedEntry(value, revision, expectedApp) {
  const entry = normalizeAppEntry(value);
  check((!expectedApp || entry.appId === expectedApp) && safeLabel(value.label, 64) && number(value.savedRevision)
    && value.savedRevision >= 1 && value.savedRevision <= revision && number(value.updatedAt));
  let current = null;
  if (value.current !== null) {
    check(record(value.current) && safeLabel(value.current.name, 64) && ['ready', 'starting', 'offline', 'stopped'].includes(value.current.status)
      && typeof value.current.canManage === 'boolean');
    current = { name: value.current.name, status: value.current.status, canManage: value.current.canManage };
  }
  return { ...entry, label: value.label, savedRevision: value.savedRevision, updatedAt: value.updatedAt, current };
}
export function normalizeAppSavedSnapshot(value, appId) {
  check(typeof appId === 'string' && APP.test(appId) && record(value) && number(value.revision) && size(value) <= 256 * 1024);
  return { revision: value.revision, entry: value.entry === null ? null : savedEntry(value.entry, value.revision, appId) };
}
function pendingValue(value, accountId) {
  check(record(value) && value.op === 'apps.saved.set' && record(value.args), 'app_saved_invalid_pending');
  const entry = normalizeAppEntry(value.entry), args = value.args;
  check(args.appId === entry.appId && args.expectedAccountId === accountId && typeof args.saved === 'boolean'
    && number(args.expectedRevision) && args.expectedRevision < Number.MAX_SAFE_INTEGER && id(args.requestId), 'app_saved_invalid_pending');
  check(Object.keys(args).every(key => ['appId', 'expectedAccountId', 'saved', 'expectedRevision', 'requestId', 'domainId', 'path'].includes(key)), 'app_saved_invalid_pending');
  if (args.saved) check(args.domainId === entry.domainId && args.path === entry.path, 'app_saved_invalid_pending');
  else check(!Object.hasOwn(args, 'domainId') && !Object.hasOwn(args, 'path'), 'app_saved_invalid_pending');
  return { op: 'apps.saved.set', entry, args: { appId: entry.appId, expectedAccountId: accountId, saved: args.saved,
    expectedRevision: args.expectedRevision, requestId: args.requestId, ...(args.saved ? { domainId: entry.domainId, path: entry.path } : {}) } };
}
function verify(pending, value) {
  check(record(value) && value.requestId === pending.args.requestId && typeof value.replayed === 'boolean' && record(value.receipt), 'app_saved_invalid_receipt');
  const receipt = value.receipt;
  check(receipt.appId === pending.args.appId && receipt.saved === pending.args.saved && receipt.revision === pending.args.expectedRevision + 1
    && number(receipt.committedAt), 'app_saved_invalid_receipt');
  const current = normalizeAppSavedSnapshot(value.current, pending.args.appId);
  check(current.revision >= receipt.revision, 'app_saved_invalid_receipt');
  return { requestId: value.requestId, replayed: value.replayed,
    receipt: { appId: receipt.appId, saved: receipt.saved, revision: receipt.revision, committedAt: receipt.committedAt }, current };
}

export function createAppSavedState({ accountId, storage, locks, randomId = () => crypto.randomUUID() }) {
  check(id(accountId), 'app_saved_invalid_scope');
  const key = `soty.app-saved.pending.v1:${accountId}`, listeners = new Set(); let disposed = false;
  function load() {
    try {
      const raw = storage.getItem(key); if (raw === null) return { revision: 0, pending: null };
      check(typeof raw === 'string' && new TextEncoder().encode(raw).byteLength <= 65536);
      const value = JSON.parse(raw); check(value.schema === 1 && value.accountId === accountId && number(value.revision));
      return { revision: value.revision, pending: value.pending === null ? null : pendingValue(value.pending, accountId) };
    } catch { fail('app_saved_storage_unavailable'); }
  }
  function write(value) {
    try {
      check(number(value.revision)); const raw = JSON.stringify({ schema: 1, accountId, ...value });
      check(new TextEncoder().encode(raw).byteLength <= 65536); storage.setItem(key, raw); check(storage.getItem(key) === raw);
    } catch { fail('app_saved_storage_unavailable'); }
    notify(key);
  }
  const emit = () => { if (!disposed) for (const listener of listeners) listener(); };
  if (!bus.has(key)) bus.set(key, new Set()); bus.get(key).add(emit);
  const locked = callback => locks?.request ? locks.request(key, callback) : Promise.reject(Object.assign(new Error('app_saved_lock_unavailable'), { code: 'app_saved_lock_unavailable' }));
  const matches = (a, b) => b !== null && same(a, b);
  return { key, read: () => clone(load()), subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); }, refreshLocal: emit,
    dispose() { disposed = true; listeners.clear(); bus.get(key)?.delete(emit); if (!bus.get(key)?.size) bus.delete(key); },
    async prepare(intent) {
      const entry = normalizeAppEntry(intent?.entry), saved = intent.saved, expectedRevision = intent.expectedRevision;
      check(typeof saved === 'boolean' && number(expectedRevision) && expectedRevision < Number.MAX_SAFE_INTEGER, 'app_saved_invalid_intent');
      const current = intent.currentEntry === null ? null : savedEntry(intent.currentEntry, expectedRevision, entry.appId);
      if (saved && current) {
        check(!same(normalizeAppEntry(current), entry), 'app_saved_no_change');
        check(intent.replace === true, 'app_saved_replace_required');
      }
      if (!saved) check(current !== null, 'app_saved_no_change');
      return locked(() => {
        const state = load(); check(state.pending === null, 'app_saved_pending_unconfirmed');
        const requestId = randomId(); check(id(requestId), 'app_saved_invalid_intent');
        const pending = pendingValue({ op: 'apps.saved.set', entry, args: { appId: entry.appId, expectedAccountId: accountId, saved,
          expectedRevision, requestId, ...(saved ? { domainId: entry.domainId, path: entry.path } : {}) } }, accountId);
        write({ revision: state.revision + 1, pending }); return clone(pending);
      });
    },
    async pendingForDispatch(expected) {
      check(expected, 'app_saved_pending_changed'); const captured = pendingValue(expected, accountId);
      return locked(() => { const state = load(); check(matches(captured, state.pending), 'app_saved_pending_changed'); return clone(state.pending); });
    },
    async acknowledge(expected, response) {
      const captured = pendingValue(expected, accountId); verify(captured, response);
      return locked(() => { const state = load(); if (!matches(captured, state.pending)) return false;
        write({ revision: state.revision + 1, pending: null }); return true; });
    },
    async abandon(expected) {
      const captured = pendingValue(expected, accountId);
      return locked(() => { const state = load(); if (!matches(captured, state.pending)) return false;
        write({ revision: state.revision + 1, pending: null }); return true; });
    },
  };
}
export async function dispatchAppSavedIntent({ state, api, isCurrent, intent, expectedPending }) {
  if (!isCurrent()) return { status: 'stale' };
  check(!(intent && expectedPending), 'app_saved_invalid_intent');
  const pending = intent ? await state.prepare(intent) : await state.pendingForDispatch(expectedPending);
  if (!isCurrent()) return { status: 'stale', pending };
  const response = verify(pending, await api.request(pending.op, clone(pending.args)));
  const settled = await state.acknowledge(pending, response);
  if (!isCurrent()) return { status: 'stale', pending };
  return { status: settled ? 'accepted' : 'superseded', pending, response };
}

export function createAppSavedLibraryState({ accountId }) {
  check(id(accountId), 'app_saved_invalid_scope'); let disposed = false, generation = 0;
  const listeners = new Set(), empty = () => ({ entries: [], revision: null, nextCursor: null, loading: false, stale: false, resetRequired: false, error: null });
  let state = empty(); const emit = () => { for (const listener of listeners) listener(); };
  return { read: () => clone(state), subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    invalidate() { generation++; state = empty(); emit(); }, dispose() { disposed = true; generation++; state = empty(); listeners.clear(); },
    async load({ api, isCurrent, older = false }) {
      if (disposed || !isCurrent()) return 'stale';
      if (older && !state.nextCursor) return 'accepted';
      const ticket = ++generation, prior = clone(state), cursor = older ? prior.nextCursor : undefined;
      const current = () => !disposed && generation === ticket && isCurrent();
      state.loading = true; state.error = null; emit();
      try {
        const value = await api.request('apps.saved.list', { expectedAccountId: accountId, limit: 20, ...(cursor ? { cursor } : {}) });
        if (!current()) return 'stale';
        check(record(value) && number(value.revision) && Array.isArray(value.entries) && value.entries.length <= 20 && size(value) <= 256 * 1024
          && (value.nextCursor === null || typeof value.nextCursor === 'string' && /^[A-Za-z0-9_-]{1,512}$/u.test(value.nextCursor)), 'app_saved_invalid_page');
        if (older && value.revision !== prior.revision) { state = { ...empty(), resetRequired: true }; return 'reset'; }
        const page = value.entries.map(entry => savedEntry(entry, value.revision));
        const entries = older ? [...prior.entries, ...page] : page;
        check(entries.length <= 200 && new Set(entries.map(entry => entry.appId)).size === entries.length
          && entries.every((entry, i) => i === 0 || entries[i - 1].savedRevision > entry.savedRevision)
          && size(entries) <= 2 * 1024 * 1024, 'app_saved_invalid_page');
        state = { ...empty(), entries, revision: value.revision, nextCursor: value.nextCursor }; return 'accepted';
      } catch (error) {
        if (!current()) return 'stale';
        if (error?.code === 'apps_saved_cursor_expired') { state = { ...empty(), resetRequired: true }; return 'reset'; }
        const denied = ['ACTIVE_PROFILE_CHANGED', 'authentication_required', 'device_revoked', 'apps_authentication_required',
          'apps_account_changed', 'access_denied', 'app_unavailable'].includes(error?.code);
        state = { ...(denied ? empty() : prior), loading: false, stale: true, error: error?.code ?? 'app_saved_load_failed' }; throw error;
      } finally { if (current()) { state.loading = false; emit(); } }
    },
  };
}
