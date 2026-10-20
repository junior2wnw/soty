const modes = new Set(['private', 'friends', 'link', 'public']);
const schema = 'soty.profile-audience.intent.v1';
const receiptSchema = 'soty.world-profile-audience.receipt.v1';
const clone = value => structuredClone(value);
const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
const record = value => value && typeof value === 'object' && !Array.isArray(value);
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9_-]{3,160}$/u.test(value);
const revision = value => Number.isSafeInteger(value) && value >= 1;
const intentRevision = value => revision(value) && value < Number.MAX_SAFE_INTEGER;
const supported = value => record(value) && identifier(value.profileId) && revision(value.revision) && value.audienceWriteVersion === 1 && modes.has(value.audience);
const fail = code => { throw Object.assign(new Error(code), { code }); };

/** One persisted owner intent, not a retry of whichever mode is now selected.
 * The same profile Web Lock covers dispatch/readback/cleanup in cooperating tabs.
 * A transport failure leaves the exact request ID available for explicit Check. */
export function createProfileAudienceState({ profile, api, isCurrent, storage, locks, onChange = () => {} }) {
  const profileId = profile.profileId, key = `soty.profile-audience.v1:${encodeURIComponent(profileId)}`;
  const available = Boolean(identifier(profileId) && typeof storage?.getItem === 'function' && typeof storage?.setItem === 'function'
    && typeof storage?.removeItem === 'function' && typeof locks?.request === 'function');
  let confirmed = clone(profile), draft = modes.has(profile.audience) ? profile.audience : profile.discoverable ? 'public' : 'private';
  let phase = supported(profile) ? available ? 'ready' : 'unavailable' : 'unsupported';
  let message = phase === 'unavailable' ? 'Для изменения видимости нужны сохранение на устройстве и защита от одновременных окон.' : '';
  let pending = null, disposed = false, generation = 0, busy = false;
  const current = ticket => !disposed && isCurrent() && ticket === generation;
  const checkCurrent = ticket => { if (!current(ticket)) fail('ACTIVE_PROFILE_CHANGED'); };
  const read = () => ({ profile: clone(confirmed), draft, phase, message, pending: clone(pending), pendingExists: Boolean(pending),
    dirty: supported(confirmed) && draft !== confirmed.audience,
    canSave: available && supported(confirmed) && intentRevision(confirmed.revision) && !pending && ['ready', 'conflict'].includes(phase) && draft !== confirmed.audience });
  const emit = () => { if (!disposed && isCurrent()) onChange(read()); };
  function load() {
    try {
      const raw = storage.getItem(key); if (raw === null) return null;
      if (typeof raw !== 'string' || raw.length > 4096) fail('profile_audience_storage_unavailable');
      const value = JSON.parse(raw), fields = ['schema', 'accountId', 'profileId', 'requestId', 'expectedRevision', 'audience'];
      if (!record(value) || Object.keys(value).length !== fields.length || Object.keys(value).some(name => !fields.includes(name))
          || value.schema !== schema || value.accountId !== profileId || value.profileId !== profileId
          || !identifier(value.requestId) || !intentRevision(value.expectedRevision) || !modes.has(value.audience)) fail('profile_audience_storage_unavailable');
      return { schema, accountId: profileId, profileId, requestId: value.requestId, expectedRevision: value.expectedRevision, audience: value.audience };
    } catch { fail('profile_audience_storage_unavailable'); }
  }
  function store(value) {
    try {
      const raw = JSON.stringify(value); storage.setItem(key, raw);
      if (storage.getItem(key) !== raw) fail('profile_audience_storage_unavailable');
    } catch { fail('profile_audience_storage_unavailable'); }
  }
  function clear(expected, ticket) {
    const stored = load(); checkCurrent(ticket);
    if (!same(stored, expected)) fail('profile_audience_pending_changed');
    try {
      checkCurrent(ticket); storage.removeItem(key); checkCurrent(ticket);
      const retained = storage.getItem(key); checkCurrent(ticket);
      if (retained !== null) fail('profile_audience_storage_unavailable');
    }
    catch { fail('profile_audience_storage_unavailable'); }
    pending = null;
  }
  function validProfile(value, minimum = confirmed.revision) {
    if (!supported(value) || value.profileId !== profileId || !revision(value.revision) || value.revision < minimum
        || (value.revision === confirmed.revision && value.audience !== confirmed.audience)) fail('profile_audience_readback_invalid');
    return clone(value);
  }
  async function readOwner(ticket, minimum) {
    checkCurrent(ticket);
    const response = await api.request('world.profile.get', {});
    checkCurrent(ticket); return validProfile(response?.profile, minimum);
  }
  function verifyReply(reply, intent) {
    const receipt = reply?.receipt;
    if (!record(receipt) || receipt.schema !== receiptSchema || receipt.requestId !== intent.requestId
        || receipt.accountId !== profileId || receipt.audience !== intent.audience || receipt.expectedRevision !== intent.expectedRevision
        || receipt.acceptedRevision !== intent.expectedRevision + 1 || typeof reply.replayed !== 'boolean') fail('profile_audience_receipt_invalid');
    return validProfile(reply.profile, Math.max(confirmed.revision, receipt.acceptedRevision));
  }
  function observed(next, intended, accepted) {
    confirmed = next;
    if (draft === intended && next.audience === intended) {
      draft = next.audience; phase = 'ready'; message = accepted ? 'Видимость сохранена и проверена.' : 'Текущая видимость проверена.';
    } else {
      phase = draft === next.audience ? 'ready' : 'conflict';
      message = accepted ? 'Прежнее изменение подтверждено. Сейчас на сервере другой режим; ваш выбор сохранён.'
        : 'Сейчас на сервере другой режим. Ваш выбор сохранён; сравните его перед новым сохранением.';
    }
    emit(); return read();
  }
  async function dispatch(ticket, intent) {
    const args = { expectedAccountId: profileId, requestId: intent.requestId, expectedRevision: intent.expectedRevision, audience: intent.audience };
    checkCurrent(ticket); emit(); checkCurrent(ticket);
    let reply;
    try { reply = await api.request('world.profile.update', args); }
    catch (error) {
      checkCurrent(ticket);
      // Only a definitive signed CAS refusal plus an advanced owner revision can
      // close this unaccepted intent. An unchanged read never settles uncertainty.
      const next = await readOwner(ticket);
      if (error?.code === 'revision_conflict' && next.revision > intent.expectedRevision) {
        checkCurrent(ticket); clear(intent, ticket); return observed(next, intent.audience, false);
      }
      confirmed = next; phase = 'unknown';
      message = error?.code === 'profile_request_conflict' ? 'Сохранённый запрос не совпал с подтверждённым. Новый запрос не отправлен.'
        : 'Результат пока не подтверждён. «Перепроверить» повторит только прежнее изменение.';
      emit(); return false;
    }
    checkCurrent(ticket);
    const acknowledged = verifyReply(reply, intent);
    const next = await readOwner(ticket, acknowledged.revision);
    checkCurrent(ticket); clear(intent, ticket); return observed(next, intent.audience, true);
  }
  async function run(kind) {
    if (disposed || !isCurrent() || busy || !available || !supported(confirmed) || phase === 'unsupported') return false;
    if (kind === 'save' && !read().canSave) return false;
    const ticket = ++generation; busy = true; phase = kind === 'save' ? 'saving' : 'checking'; message = '';
    try {
      emit(); checkCurrent(ticket);
      return await locks.request(key, async () => {
        checkCurrent(ticket);
        const stored = load(); checkCurrent(ticket);
        if (kind === 'save' && stored) {
          pending = stored; phase = 'unknown'; message = 'В другом окне осталось изменение. Перепроверьте его перед новым сохранением.';
          emit(); return false;
        }
        if (kind === 'check' && !stored) {
          pending = null; const next = await readOwner(ticket);
          confirmed = next; phase = draft === next.audience ? 'ready' : 'conflict'; message = 'Текущее состояние проверено.';
          emit(); return read();
        }
        let intent = stored;
        if (kind === 'save') {
          intent = { schema, accountId: profileId, profileId, requestId: crypto.randomUUID(), expectedRevision: confirmed.revision, audience: draft };
          checkCurrent(ticket); store(intent); checkCurrent(ticket);
        }
        pending = clone(intent);
        return await dispatch(ticket, intent);
      });
    } catch (error) {
      checkCurrent(ticket);
      phase = ['profile_audience_storage_unavailable', 'profile_audience_pending_changed'].includes(error?.code) ? 'storage-error' : 'unknown';
      message = phase === 'storage-error' ? 'Не удалось проверить сохранённый запрос на устройстве. Новое изменение не отправлено.'
        : 'Результат пока не подтверждён. Перепроверьте прежнее изменение перед новым сохранением.';
      emit(); return false;
    } finally { busy = false; }
  }
  if (supported(profile) && available) {
    try { pending = load(); if (pending) { draft = pending.audience; phase = 'unknown'; message = 'Прежнее изменение требует проверки.'; } }
    catch { phase = 'storage-error'; message = 'Сохранённый запрос не удалось проверить. Новое изменение не отправлено.'; }
  }
  return Object.freeze({
    key, read,
    choose(audience) {
      if (disposed || !isCurrent() || busy || pending || !available || !supported(confirmed) || !modes.has(audience) || !['ready', 'conflict'].includes(phase)) return false;
      draft = audience; message = ''; emit(); return true;
    },
    save: () => run('save'), check: () => run('check'),
    refreshPending() {
      if (disposed || !isCurrent() || busy || !available || !supported(confirmed)) return read();
      try {
        const stored = load();
        if (!isCurrent() || disposed) return read();
        pending = stored; phase = 'unknown'; message = stored ? 'Прежнее изменение требует проверки.' : 'Другое окно изменило настройки. Перепроверьте текущее состояние.';
      } catch { phase = 'storage-error'; message = 'Сохранённый запрос не удалось проверить. Новое изменение не отправлено.'; }
      emit(); return read();
    },
    dispose() { disposed = true; generation++; },
  });
}
