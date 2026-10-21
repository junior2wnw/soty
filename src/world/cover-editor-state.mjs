import { coverError, coverId, coverRequestId, closedData, coverScope, sameCoverScope, expectedCover } from './cover-api.mjs';
const JOURNAL = 'soty.cover-intents.v1', RETENTION = 30 * 86400_000, MAX_PENDING = 8;
const defaultCrop = () => ({ x: .5, y: .5, zoom: 1 });
const check = (value, code) => { if (!value) throw coverError(code); };
const requestTime = requestId => Number(requestId.split('.')[1]);
const actions = ['prepare', 'prepareFromVersion', 'commit', 'restore', 'remove', 'cancel'];
const metadataKeys = ['requestId', 'draftId', 'versionId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision', 'crop', 'mode'];
function validPending(value) {
  if (!closedData(value, ['schema', 'identity', 'scope', 'method', 'args', 'savedAt']) || value.schema !== 1 || !actions.includes(value.method)
      || !closedData(value.identity, ['accountId', 'kind', 'targetId']) || !coverId(value.identity.accountId) || !coverId(value.identity.targetId)
      || !['app', 'profile'].includes(value.identity.kind) || !Number.isSafeInteger(value.savedAt) || !closedData(value.args, ['requestId'], metadataKeys.slice(1)) || !coverRequestId(value.args.requestId)) return false;
  try { coverScope(value.scope, value.identity); } catch { return false; }
  for (const key of ['draftId', 'versionId']) if (Object.hasOwn(value.args, key) && !coverId(value.args[key])) return false;
  for (const key of ['expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision']) if (Object.hasOwn(value.args, key) && (!Number.isSafeInteger(value.args[key]) || value.args[key] < (key === 'expectedCoverRevision' ? 0 : 1))) return false;
  if (value.args.mode !== undefined && !['inherit', 'none'].includes(value.args.mode)) return false;
  if (value.args.crop !== undefined && (!closedData(value.args.crop, ['x', 'y', 'zoom']) || !Number.isFinite(value.args.crop.x) || value.args.crop.x < 0 || value.args.crop.x > 1
      || !Number.isFinite(value.args.crop.y) || value.args.crop.y < 0 || value.args.crop.y > 1 || !Number.isFinite(value.args.crop.zoom) || value.args.crop.zoom < 1 || value.args.crop.zoom > 4)) return false;
  const required = value.method === 'commit' || value.method === 'cancel' ? ['draftId']
    : ['expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision', ...(['restore', 'prepareFromVersion'].includes(value.method) ? ['versionId'] : [])];
  const permitted = ['requestId', ...required, ...(['prepare', 'prepareFromVersion'].includes(value.method) ? ['crop'] : value.method === 'remove' ? ['mode'] : [])];
  return required.every(key => Object.hasOwn(value.args, key)) && Object.keys(value.args).every(key => permitted.includes(key));
}
const sameIdentity = (a, b) => a.accountId === b.accountId && a.kind === b.kind && a.targetId === b.targetId;
/** Per-tab, bounded recovery journal. It contains routing/CAS/receipt metadata
 * only. Images, original filenames and base64 never cross this boundary. */
export function createCoverJournal(storage, now = () => Date.now()) {
  function read() {
    let text; try { text = storage.getItem(JOURNAL); } catch { throw coverError('cover_journal_unavailable'); }
    if (text === null) return [];
    check(typeof text === 'string' && text.length <= 32768, 'cover_journal_invalid');
    let rows; try { rows = JSON.parse(text); } catch { throw coverError('cover_journal_invalid'); }
    check(Array.isArray(rows) && rows.length <= MAX_PENDING && rows.every(validPending), 'cover_journal_invalid');
    check(rows.every((row, n) => rows.findIndex(other => sameIdentity(row.identity, other.identity)) === n), 'cover_journal_invalid');
    return rows.filter(row => now() - row.savedAt <= RETENTION && row.savedAt - now() <= 120_000);
  }
  function write(rows) {
    const text = JSON.stringify(rows); check(text.length <= 32768 && rows.length <= MAX_PENDING, 'cover_journal_quota');
    try { storage.setItem(JOURNAL, text); check(storage.getItem(JOURNAL) === text, 'cover_journal_unavailable'); }
    catch { throw coverError('cover_journal_unavailable'); }
  }
  return Object.freeze({
    read(identity) { const value = read().find(row => sameIdentity(row.identity, identity)); return value ? structuredClone(value) : null; },
    save(value, previous = null) {
      check(validPending(value), 'cover_journal_invalid'); const rows = read(), index = rows.findIndex(row => sameIdentity(row.identity, value.identity));
      check(index < 0 ? previous === null : previous && JSON.stringify(rows[index]) === JSON.stringify(previous), 'cover_intent_changed');
      if (index < 0) rows.push(value); else rows[index] = value; write(rows);
    },
    clear(value) {
      const rows = read(), index = rows.findIndex(row => sameIdentity(row.identity, value.identity));
      check(index < 0 || JSON.stringify(rows[index]) === JSON.stringify(value), 'cover_intent_changed');
      if (index >= 0) { rows.splice(index, 1); write(rows); }
    },
  });
}
export function ownedCoverImage(image, urls = URL) {
  const byteCount = image.bytes.length; let url;
  try { url = urls.createObjectURL(new Blob([image.bytes], { type: image.mime })); } finally { image.bytes.fill(0); }
  let disposed = false;
  return Object.freeze({ url, width: image.width, height: image.height, byteCount,
    dispose() { if (disposed) return; disposed = true; urls.revokeObjectURL(url); } });
}
/** Actual async state machine used by the DOM view. No implicit spending,
 * generation, image publication or write on opening/selecting/cropping. */
export function createCoverEditorState({ sdk, prepareFile, journal, isCurrent, onChange = () => {}, onCommitted = () => {}, makeImage = ownedCoverImage,
  now = () => Date.now(), random = bytes => crypto.getRandomValues(bytes) }) {
  const identity = sdk.identity; let disposed = false, epoch = 0, busy = false;
  let phase = 'idle', error = '', notice = '', status = null, versions = [], pending = null, image = null, local = null, selection = null, dirty = false, crop = defaultCrop();
  let readyDraft = null, reviewedRequestId = null;
  const current = ticket => !disposed && isCurrent() === true && (ticket === undefined || ticket === epoch);
  const assert = ticket => check(current(ticket), 'cover_context_changed');
  const snapshot = () => Object.freeze({ phase, busy, error, notice, current: status?.current ?? null, versions: versions.map(v => ({ ...v })),
    source: local ? { url: local.url, width: local.width, height: local.height, kind: 'local' } : image ? { url: image.url, width: image.width, height: image.height, kind: selection?.kind ?? 'current' } : null,
    crop: { ...crop }, dirty, pending: pending ? { method: pending.method, draftId: pending.args.draftId ?? readyDraft } : null,
    canSave: !busy && phase !== 'conflict' && (dirty && !pending || phase === 'prepared' && !!readyDraft && reviewedRequestId === pending?.args.requestId && !!selection),
    hasUnsavedChanges: dirty || !!pending || busy });
  const emit = () => { if (current()) onChange(snapshot()); };
  function dropLocal() { local?.dispose(); local = null; }
  function dropImage() { image?.dispose(); image = null; }
  function clearPending() { if (pending) journal.clear(pending); pending = null; readyDraft = null; reviewedRequestId = null; }
  function requestId(serverTime) {
    const bytes = random(new Uint8Array(16)); check(bytes instanceof Uint8Array && bytes.length === 16, 'cover_random_unavailable');
    return `cover1.${serverTime}.${Array.from(bytes, n => n.toString(16).padStart(2, '0')).join('')}`;
  }
  async function observed(ticket, query = {}) { const value = await sdk.call('status', query); assert(ticket); return value; }
  async function inspect(ticket, initial = false) {
    const next = await observed(ticket); const recovered = pending ?? journal.read(identity);
    if (recovered) { pending = recovered; check(recovered.scope.storeInstanceId === next.scope.storeInstanceId, 'cover_store_changed'); }
    status = next;
    const results = await Promise.allSettled([sdk.call('read', { width: 960 }), sdk.call('versions', { limit: 20 })]); assert(ticket);
    if (results[0].status === 'fulfilled') {
      const read = results[0].value; check(read.current.revision === next.current.revision && read.current.version === next.current.version, 'cover_observation_changed');
      const replacement = read.image ? makeImage(read.image) : null; assert(ticket); dropImage(); image = replacement;
      selection = next.current.version && image ? { kind: 'current', versionId: next.current.version } : null; crop = defaultCrop();
    } else throw results[0].reason;
    if (results[1].status === 'fulfilled' && results[1].value.current.revision === next.current.revision) versions = results[1].value.versions;
    else { versions = []; notice = 'История пока недоступна. Обновите состояние.'; }
    phase = pending ? 'unknown' : 'ready'; return next;
  }
  async function run(operation) {
    if (!current() || busy) return; const ticket = ++epoch; busy = true; error = ''; notice = ''; emit();
    try { await operation(ticket); }
    catch (reason) { if (current(ticket)) { error = typeof reason?.code === 'string' ? reason.code : 'cover_network_unavailable';
      phase = pending ? 'unknown' : error.includes('conflict') || error === 'cover_observation_changed' || error === 'cover_target_changed' ? 'conflict' : 'error'; } }
    finally { if (current(ticket)) { busy = false; emit(); } }
  }
  async function freshBefore(ticket) {
    check(status, 'cover_not_ready'); const next = await observed(ticket);
    check(sameCoverScope(status.scope, next.scope), 'cover_revision_conflict'); return next;
  }
  function record(method, args, scope) {
    const metadata = Object.fromEntries(metadataKeys.filter(key => Object.hasOwn(args, key)).map(key => [key, structuredClone(args[key])]));
    const next = { schema: 1, identity: { ...identity }, scope: { ...scope }, method, args: metadata, savedAt: now() };
    journal.save(next, pending); pending = next; readyDraft = null;
  }
  async function finishWrite(ticket, result) {
    check(result?.receipt && result.receipt.action === 'committed', 'cover_response_invalid'); clearPending(); dropLocal(); dirty = false;
    // Invalidating a mounted cell cannot depend on a later optional read being
    // available. Apps intentionally do not increment their Source revision.
    try { onCommitted({ identity: { ...identity }, current: { ...result.current } }); } catch { /* The confirmed write remains confirmed. */ }
    await inspect(ticket); assert(ticket); notice = 'Сохранено';
  }
  async function commitPrepared(ticket, draftId) {
    const fresh = await freshBefore(ticket), args = { requestId: requestId(fresh.serverTime), draftId };
    record('commit', args, fresh.scope); phase = 'saving'; emit(); const result = await sdk.call('commit', args); assert(ticket); await finishWrite(ticket, result);
  }
  return Object.freeze({
    read: snapshot,
    load() { return run(async ticket => { check(!dirty && !pending, 'cover_unsaved_changes'); phase = 'loading'; emit(); await inspect(ticket, true); }); },
    choose(file) { return run(async ticket => {
      check(status && !pending, 'cover_pending_intent'); check(phase !== 'conflict', 'cover_revision_conflict'); phase = 'decoding'; emit();
      const prepared = await prepareFile(file, () => current(ticket)); if (!current(ticket)) { prepared.dispose(); return; }
      dropLocal(); dropImage(); local = prepared; selection = { kind: 'local' }; crop = defaultCrop(); dirty = true; phase = 'editing';
    }); },
    setCrop(next) {
      if (!current() || busy || pending || phase === 'conflict' || !selection || !local && !image) return;
      check(closedData(next, ['x', 'y', 'zoom']) && Number.isFinite(next.x) && next.x >= 0 && next.x <= 1 && Number.isFinite(next.y) && next.y >= 0 && next.y <= 1 && Number.isFinite(next.zoom) && next.zoom >= 1 && next.zoom <= 4, 'cover_crop_invalid');
      crop = { ...next }; dirty = true; phase = 'editing'; error = ''; emit();
    },
    previewVersion(versionId) { return run(async ticket => {
      check(!pending && !dirty && versions.some(v => v.id === versionId && v.restorable), 'cover_unsaved_changes');
      const read = await sdk.call('read', { versionId, width: 960 }); assert(ticket);
      check(read.current.revision === status.current.revision, 'cover_observation_changed'); const replacement = read.image ? makeImage(read.image) : null;
      dropLocal(); dropImage(); image = replacement; selection = image ? { kind: 'version', versionId } : null; crop = defaultCrop(); phase = 'preview';
      notice = 'Предпросмотр версии. «Восстановить» сохранит её; ползунки позволят кадрировать заново.';
    }); },
    save() { return run(async ticket => {
      if (phase === 'prepared') { check(readyDraft && reviewedRequestId === pending?.args.requestId && selection, 'cover_pending_intent'); return commitPrepared(ticket, readyDraft); }
      check(phase !== 'conflict', 'cover_revision_conflict'); check(dirty && selection && !pending, 'cover_not_ready'); const fresh = await freshBefore(ticket);
      const method = local ? 'prepare' : 'prepareFromVersion'; const args = { requestId: requestId(fresh.serverTime), ...expectedCover(fresh.scope), crop: { ...crop },
        ...(local ? { mime: local.mime, base64: local.base64() } : { versionId: selection.versionId }) };
      record(method, args, fresh.scope); reviewedRequestId = args.requestId; phase = 'preparing'; emit(); const prepared = await sdk.call(method, args); assert(ticket);
      check(prepared.draft && coverId(prepared.draft.id) && ['processing', 'ready'].includes(prepared.draft.state), 'cover_response_invalid');
      // The prepare journal deliberately has no draftId; status(requestId) recovers it.
      if (prepared.draft.state !== 'ready') { phase = 'unknown'; notice = 'Подготовка продолжается. Проверьте результат.'; return; }
      await commitPrepared(ticket, prepared.draft.id);
    }); },
    changeMode(mode) { return run(async ticket => {
      check(['inherit', 'none'].includes(mode) && !pending && !dirty, 'cover_unsaved_changes'); const fresh = await freshBefore(ticket);
      const args = { requestId: requestId(fresh.serverTime), ...expectedCover(fresh.scope), mode }; record('remove', args, fresh.scope); phase = 'saving'; emit();
      const result = await sdk.call('remove', args); assert(ticket); await finishWrite(ticket, result);
    }); },
    restore(versionId) { return run(async ticket => {
      check(!pending && !dirty && versions.some(v => v.id === versionId && v.restorable), 'cover_unsaved_changes'); const fresh = await freshBefore(ticket);
      const args = { requestId: requestId(fresh.serverTime), ...expectedCover(fresh.scope), versionId }; record('restore', args, fresh.scope); phase = 'saving'; emit();
      const result = await sdk.call('restore', args); assert(ticket); await finishWrite(ticket, result);
    }); },
    recheck() { return run(async ticket => {
      check(pending, 'cover_not_ready'); const value = await observed(ticket, { requestId: pending.args.requestId });
      check(value.scope.storeInstanceId === pending.scope.storeInstanceId, 'cover_store_changed');
      if (pending.method === 'prepare' || pending.method === 'prepareFromVersion') {
        if (value.draft && ['processing', 'ready'].includes(value.draft.state)) {
          // Prepared images cannot publish themselves. A fresh explicit Save is
          // allowed only while this same UI still owns its reviewed preview.
          if (value.draft.state === 'ready' && reviewedRequestId === pending.args.requestId && selection && sameCoverScope(pending.scope, value.scope)) {
            status = value; const args = { requestId: pending.args.requestId, draftId: value.draft.id };
            // Keep original metadata and expose a RAM-only ready draft.
            readyDraft = args.draftId; phase = 'prepared'; notice = 'Подготовлено. Проверьте кадр и нажмите «Сохранить».';
          } else { readyDraft = value.draft.id; phase = 'unknown'; notice = value.draft.state === 'ready' ? 'Подготовка найдена. Отмените её и выберите картинку заново.' : 'Подготовка ещё выполняется.'; }
        } else if (value.draft || value.serverTime - requestTime(pending.args.requestId) > 86400_000) { clearPending(); phase = 'editing'; notice = 'Подготовка завершилась без публикации. Можно повторить сохранение.'; }
        else { phase = 'unknown'; notice = 'Подтверждения пока нет. Можно проверить снова или отменить подготовку.'; }
        return;
      }
      // SAME closed arguments and SAME request ID: a lost response cannot create
      // a second publication, and a receipt cannot rewind a later head.
      let result;
      try { result = await sdk.call(pending.method, { ...pending.args }); assert(ticket); }
      catch (reason) {
        const terminal = ['cover_revision_conflict', 'cover_target_changed', 'cover_draft_unavailable', 'cover_draft_expired', 'cover_version_expired', 'cover_request_expired'];
        if (!terminal.includes(reason?.code)) throw reason;
        const rejected = await observed(ticket, { requestId: pending.args.requestId });
        check(rejected.scope.storeInstanceId === pending.scope.storeInstanceId, 'cover_store_changed');
        if (rejected.receipt) throw reason;
        // The exact replay was rejected, and a signed fresh read confirms that
        // no receipt exists. The monotonic CAS/draft/expiry error cannot become
        // a later success. Keep local edits, require explicit refresh/review.
        clearPending(); status = rejected; phase = 'conflict'; error = reason.code;
        notice = 'Сервер не принял сохранение. Отмените правки и обновите состояние перед новым выбором.'; return;
      }
      if (pending.method === 'cancel') { clearPending(); dropLocal(); dirty = false; await inspect(ticket); notice = 'Подготовка отменена'; }
      else await finishWrite(ticket, result);
    }); },
    cancelPreparation() { return run(async ticket => {
      check(pending && ['prepare', 'prepareFromVersion'].includes(pending.method), 'cover_pending_intent');
      const value = await observed(ticket, { requestId: pending.args.requestId }); check(value.scope.storeInstanceId === pending.scope.storeInstanceId, 'cover_store_changed');
      if (value.draft && ['processing', 'ready'].includes(value.draft.state)) {
        const args = { requestId: requestId(value.serverTime), draftId: value.draft.id }; record('cancel', args, value.scope);
        await sdk.call('cancel', args); assert(ticket); clearPending();
      } else clearPending(); // A prepare alone never publishes; late native work expires server-side.
      dropLocal(); dirty = false; await inspect(ticket); notice = 'Подготовка отменена';
    }); },
    discard() { return run(async ticket => { check(!pending, 'cover_pending_intent'); dropLocal(); dirty = false; await inspect(ticket); }); },
    dispose() { if (disposed) return; disposed = true; epoch++; dropLocal(); dropImage(); },
  });
}
