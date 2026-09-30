import { validateAppLaunchPath } from './app-launch.mjs';

const PROFILE = 'soty.relay-restricted.v1';
const PAGE_SIZE = 20;
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value);
const positive = value => Number.isSafeInteger(value) && value >= 1;
const timestamp = value => Number.isSafeInteger(value) && value >= 0;
const clone = value => structuredClone(value);
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const frozen = value => { if (value && typeof value === 'object') { for (const child of Object.values(value)) frozen(child); Object.freeze(value); } return value; };
const fail = code => { throw Object.assign(new Error(code), { code }); };
const check = (value, code = 'app_source_invalid_state') => { if (!value) fail(code); };
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key));
const targetKeys = ['revision', 'digest', 'profile', 'hostDeviceId', 'connectorId', 'deviceName', 'port', 'entryPath'];

/** Bounded local receipt/display data, never a runtime proof or wire argument. */
export function normalizeAppSourceTarget(value) {
  check(exact(value, targetKeys) && positive(value.revision) && typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest)
    && value.profile === PROFILE && id(value.hostDeviceId) && id(value.connectorId)
    && typeof value.deviceName === 'string' && value.deviceName.length <= 180
    && Number.isSafeInteger(value.port) && value.port >= 1024 && value.port <= 65535
    && typeof value.entryPath === 'string' && value.entryPath.length > 0 && value.entryPath.length <= 8192,
  'app_source_invalid_target');
  return { revision: value.revision, digest: value.digest, profile: value.profile, hostDeviceId: value.hostDeviceId,
    connectorId: value.connectorId, deviceName: value.deviceName, port: value.port, entryPath: value.entryPath };
}

function projectTarget(value) { return normalizeAppSourceTarget(Object.fromEntries(targetKeys.map(key => [key, value?.[key]]))); }
function historyTarget(value) {
  check(exact(value, [...targetKeys, 'createdAt']) && timestamp(value.createdAt), 'app_source_invalid_history');
  return { ...projectTarget(value), createdAt: value.createdAt };
}
function tuple(value) { return { hostDeviceId: value.hostDeviceId, connectorId: value.connectorId, port: Number(value.port), entryPath: value.entryPath }; }
function pins(value) { return { revision: value.revision, digest: value.digest, profile: value.profile }; }
function baseIdentity(value) {
  return { appId: value.app.id, state: value.app.state, epoch: value.publication.policyEpoch,
    target: pins(value.source), floor: value.source.requiredBindingVersion ?? 1 };
}
function validateSnapshot(value, appId) {
  check(value?.app?.id === appId && ['enabled', 'revoked'].includes(value.app.state)
    && positive(value.publication?.policyEpoch) && positive(value.source?.revision)
    && value.publication.activeTargetRevision === value.source.revision
    && ['restricted', 'anyone'].includes(value.publication.launchPolicy)
    && typeof value.publication.listed === 'boolean' && Array.isArray(value.publication.activeDomainIds)
    && [1, 2].includes(value.source.requiredBindingVersion ?? 1), 'app_source_invalid_scope');
  projectTarget(value.source);
  return clone(value);
}

/** Use the server's remaining interval and subtract the complete request time.
 * A caller counts the result down with its own monotonic clock. */
export function appSourcePreparationRemaining(preparation, requestElapsedMs) {
  if (!timestamp(preparation?.checkedAt) || !timestamp(preparation?.expiresAt)
    || !Number.isFinite(requestElapsedMs) || requestElapsedMs < 0) return 0;
  return Math.max(0, Math.min(30_000, preparation.expiresAt - preparation.checkedAt) - requestElapsedMs);
}

/** One dialog's unsent draft and ephemeral checks. Durable commands belong to
 * the existing account/app settings store, which is intentionally separate. */
export function createAppSourceState({ accountId, appId, snapshot: initial, now = () => performance.now() }) {
  check(id(accountId) && typeof appId === 'string' && /^app-[a-f0-9]{32}$/u.test(appId), 'app_source_invalid_scope');
  let snapshot = validateSnapshot(initial, appId), base = clone(snapshot), disposed = false, generation = 0;
  const fresh = value => ({ mode: 'new', hostDeviceId: value.source.hostDeviceId, connectorId: value.source.connectorId,
    port: String(value.source.port), entryPath: value.source.entryPath, launchPolicy: value.publication.launchPolicy,
    exposureConfirmed: false, targetRevision: null });
  let draft = fresh(snapshot), selected = null, prepared = null, inFlight = null, publicationDirty = false;
  let historyGeneration = 0, historyFlight = null, history = { loading: false, targets: [], nextCursor: null, stale: false };
  let historyCurrent = null, historyConflict = false;
  let lastClock = 0, clockBroken = false;
  const clock = () => {
    const result = now();
    if (clockBroken || !Number.isFinite(result) || result < lastClock) { clockBroken = true; return null; }
    lastClock = result; return result;
  };
  const dirty = () => (draft.mode === 'history' ? draft.targetRevision !== base.source.revision : !equal(tuple(draft), tuple(base.source)))
    || draft.launchPolicy !== base.publication.launchPolicy;
  const conflict = () => historyConflict || (dirty() && !equal(baseIdentity(base), baseIdentity(snapshot)));
  const noChange = () => draft.mode === 'history' ? draft.targetRevision === snapshot.source.revision : equal(tuple(draft), tuple(snapshot.source));
  const remaining = () => {
    if (!prepared) return 0;
    const at = clock();
    if (at === null || at < prepared.receivedAt || at >= prepared.deadline) prepared.expired = true;
    return prepared.expired ? 0 : Math.max(0, prepared.deadline - at);
  };
  const eligible = () => !disposed && snapshot.app.state === 'enabled' && snapshot.actions.canEdit && snapshot.actions.canPublish
    && !publicationDirty && !conflict() && !noChange();
  function invalidatePreparation() { generation++; prepared = null; draft.exposureConfirmed = false; }
  const reviewAvailable = () => eligible() && !!prepared && prepared.generation === generation;
  function ready() { return eligible() && !!prepared && prepared.generation === generation && remaining() > 0; }
  const consentReady = () => draft.launchPolicy !== 'anyone' || (draft.exposureConfirmed && snapshot.publication.activeDomainIds.length > 0);
  const canRecheck = () => reviewAvailable() && remaining() === 0 && !clockBroken && !inFlight && consentReady();
  function prepareArgs() {
    check(eligible(), publicationDirty ? 'app_source_publication_dirty' : conflict() ? 'app_source_revision_conflict' : noChange() ? 'app_source_unchanged' : 'app_source_unavailable');
    const common = { appId, expectedAccountId: accountId, expectedPolicyEpoch: base.publication.policyEpoch, expectedTargetRevision: base.source.revision };
    if (draft.mode === 'history') {
      check(selected && selected.revision === draft.targetRevision, 'app_source_invalid_target');
      validateAppLaunchPath(selected.entryPath);
      return { ...common, targetRevision: selected.revision };
    }
    check(id(draft.hostDeviceId) && id(draft.connectorId), 'apps_device_not_owned');
    check(/^\d{1,5}$/u.test(draft.port), 'invalid_app_port');
    const port = Number(draft.port);
    check(Number.isSafeInteger(port) && port >= 1024 && port <= 65535 && port !== 49424, 'invalid_app_port');
    return { ...common, source: { hostDeviceId: draft.hostDeviceId, connectorId: draft.connectorId, port, entryPath: validateAppLaunchPath(draft.entryPath) } };
  }
  function validPreparation(value, request) {
    check(value?.schema === 'soty.app-source-preparation.v1' && value.appId === appId
      && typeof value.preparationId === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value.preparationId) && value.requiredBindingVersion === 2
      && value.expectedPolicyEpoch === request.args.expectedPolicyEpoch && value.expectedTargetRevision === request.args.expectedTargetRevision
      && timestamp(value.checkedAt) && timestamp(value.expiresAt) && value.expiresAt > value.checkedAt, 'app_source_invalid_preparation');
    const target = normalizeAppSourceTarget(value.target); validateAppLaunchPath(target.entryPath);
    check(target.port !== 49424, 'app_source_invalid_preparation');
    check(request.args.source ? equal(tuple(target), request.args.source) && target.revision > request.args.expectedTargetRevision
      : equal(pins(target), pins(request.selected)) && equal(tuple(target), tuple(request.selected)), 'app_source_invalid_preparation');
    return { schema: value.schema, appId, preparationId: value.preparationId, requiredBindingVersion: 2,
      expectedPolicyEpoch: value.expectedPolicyEpoch, expectedTargetRevision: value.expectedTargetRevision, target,
      checkedAt: value.checkedAt, expiresAt: value.expiresAt };
  }
  function matchesCompleted(pending) {
    if (pending?.op !== 'apps.source.promote' || !pending.expectedSource) return false;
    const args = pending.args;
    return args.appId === appId && args.expectedAccountId === accountId && args.expectedPolicyEpoch === base.publication.policyEpoch
      && args.expectedTargetRevision === base.source.revision && args.launchPolicy === draft.launchPolicy
      && (draft.mode === 'history' ? draft.targetRevision === pending.expectedSource.revision
        && equal(pins(selected), pins(pending.expectedSource)) : equal(tuple(draft), tuple(pending.expectedSource)));
  }
  function resetDraft() { invalidatePreparation(); base = clone(snapshot); draft = fresh(snapshot); selected = null; }
  function reset() { historyGeneration++; resetDraft(); }
  const controller = {
    read() {
      const remainingMs = remaining();
      return clone({ snapshot, base, draft, dirty: dirty(), conflict: conflict(), publicationDirty, preparing: !!inFlight,
        preparation: prepared?.value ?? null, remainingMs, canPrepare: eligible() && !inFlight,
        canPromote: ready() && consentReady(), canRecheckPromote: canRecheck(), history });
    },
    patch(value) {
      check(!disposed && exact(value, ['hostDeviceId', 'connectorId', 'port', 'entryPath', 'launchPolicy', 'exposureConfirmed']));
      for (const key of ['hostDeviceId', 'connectorId', 'port', 'entryPath']) if (Object.hasOwn(value, key)) {
        check(typeof value[key] === 'string' && value[key].length <= (key === 'entryPath' ? 8192 : key === 'port' ? 32 : 180));
      }
      if (Object.hasOwn(value, 'launchPolicy')) check(['restricted', 'anyone'].includes(value.launchPolicy));
      if (Object.hasOwn(value, 'exposureConfirmed')) check(typeof value.exposureConfirmed === 'boolean');
      const tupleChanged = ['hostDeviceId', 'connectorId', 'port', 'entryPath'].some(key => Object.hasOwn(value, key) && draft[key] !== value[key]);
      const changed = tupleChanged || (value.launchPolicy !== undefined && value.launchPolicy !== draft.launchPolicy)
        || (!!inFlight && Object.hasOwn(value, 'exposureConfirmed'));
      if (changed) invalidatePreparation();
      draft = { ...draft, ...clone(value) };
      if (tupleChanged) { draft.mode = 'new'; draft.targetRevision = null; selected = null; }
      if (changed || !reviewAvailable() || draft.launchPolicy !== 'anyone') draft.exposureConfirmed = false;
    },
    selectHistory(value) {
      check(!disposed && exact(value, [...targetKeys, 'createdAt']), 'app_source_invalid_target');
      const target = projectTarget(value); invalidatePreparation(); selected = target;
      draft = { ...draft, mode: 'history', hostDeviceId: target.hostDeviceId, connectorId: target.connectorId,
        port: String(target.port), entryPath: target.entryPath, targetRevision: target.revision };
    },
    observe(next, options = {}) {
      if (disposed) return;
      const value = validateSnapshot(next, appId), changed = !equal(baseIdentity(snapshot), baseIdentity(value));
      const completed = matchesCompleted(options.completed), wasDirty = dirty();
      if (changed || (options.publicationDirty === true && !publicationDirty)) invalidatePreparation();
      if (Object.hasOwn(options, 'publicationDirty')) { check(typeof options.publicationDirty === 'boolean'); publicationDirty = options.publicationDirty; }
      snapshot = value;
      if (historyCurrent && snapshot.publication.policyEpoch >= historyCurrent.policyEpoch) {
        historyConflict = snapshot.publication.policyEpoch === historyCurrent.policyEpoch
          && (snapshot.source.revision !== historyCurrent.activeTargetRevision || (snapshot.source.requiredBindingVersion ?? 1) !== historyCurrent.requiredBindingVersion);
      }
      history.stale = !!historyCurrent && (historyCurrent.policyEpoch !== snapshot.publication.policyEpoch
        || historyCurrent.activeTargetRevision !== snapshot.source.revision || historyCurrent.requiredBindingVersion !== (snapshot.source.requiredBindingVersion ?? 1));
      // A render may observe the same clean inspection while history is in
      // flight. Refresh the form without cancelling that independent read.
      if (!wasDirty || completed) resetDraft();
    },
    reset,
    invalidatePreparation,
    async prepare({ api, isCurrent }) {
      check(!inFlight, 'apps_source_preparation_capacity');
      if (disposed || !isCurrent()) return { status: 'stale' };
      const args = prepareArgs(), startedAt = clock(); check(startedAt !== null, 'app_source_clock_invalid');
      invalidatePreparation();
      const request = { generation, args, selected: clone(selected), startedAt }; inFlight = request;
      const current = () => !disposed && isCurrent() && request.generation === generation;
      try {
        const response = await api.request('apps.source.prepare', clone(args));
        if (!current()) return { status: 'stale' };
        const receivedAt = clock(); check(receivedAt !== null && receivedAt >= startedAt, 'app_source_clock_invalid');
        const value = validPreparation(response, request), duration = appSourcePreparationRemaining(value, receivedAt - startedAt);
        check(duration > 0, 'apps_source_preparation_expired');
        prepared = { value, generation, receivedAt, deadline: receivedAt + duration }; draft.exposureConfirmed = false;
        return { status: 'ready' };
      } catch (error) { if (!current()) return { status: 'stale' }; throw error; }
      finally { if (inFlight === request) inFlight = null; }
    },
    promoteIntent() {
      check(ready(), publicationDirty ? 'app_source_publication_dirty' : 'apps_source_preparation_expired');
      check(draft.launchPolicy !== 'anyone' || (draft.exposureConfirmed && snapshot.publication.activeDomainIds.length > 0), 'app_exposure_ack_required');
      const expectedSource = clone(prepared.value.target), preparationId = prepared.value.preparationId;
      const args = { appId, expectedAccountId: accountId, preparationId, expectedPolicyEpoch: base.publication.policyEpoch,
        expectedTargetRevision: base.source.revision, launchPolicy: draft.launchPolicy, listed: draft.launchPolicy === 'anyone' ? base.publication.listed : false,
        ...(draft.launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: expectedSource.revision,
          targetDigest: expectedSource.digest, profile: expectedSource.profile } } : {}) };
      const capturedGeneration = generation, capturedPolicy = draft.launchPolicy;
      return frozen({ op: 'apps.source.promote', args, expectedSource, beforeCreate() {
        check(ready() && capturedGeneration === generation && prepared.value.preparationId === preparationId
          && draft.launchPolicy === capturedPolicy && (capturedPolicy !== 'anyone' || draft.exposureConfirmed), 'apps_source_preparation_expired'); return true;
      } });
    },
    /** An explicit alternate gesture after an untimed review. This only returns
     * an intent; the shared durable dispatcher still owns every effect. */
    async recheckPromotion({ api, isCurrent }) {
      if (disposed || !isCurrent()) return { status: 'stale' };
      check(canRecheck(), 'app_source_review_required');
      const review = { target: clone(prepared.value.target), preparationId: prepared.value.preparationId,
        identity: baseIdentity(base), launchPolicy: draft.launchPolicy,
        listed: draft.launchPolicy === 'anyone' ? base.publication.listed : false, consent: draft.exposureConfirmed };
      const checking = controller.prepare({ api, isCurrent }), startedGeneration = generation;
      const result = await checking;
      if (result.status === 'stale' || disposed || !isCurrent() || generation !== startedGeneration) return { status: 'stale' };
      const value = prepared?.value;
      if (value?.preparationId === review.preparationId) { invalidatePreparation(); return { status: 'review' }; }
      const unchanged = value && value.preparationId !== review.preparationId && equal(pins(value.target), pins(review.target))
        && equal(tuple(value.target), tuple(review.target)) && equal(baseIdentity(base), review.identity)
        && draft.launchPolicy === review.launchPolicy && (draft.launchPolicy === 'anyone' ? base.publication.listed : false) === review.listed;
      if (!unchanged) return { status: 'review' };
      draft.exposureConfirmed = review.consent;
      return { status: 'ready', intent: controller.promoteIntent() };
    },
    async loadHistory({ api, isCurrent, older = false }) {
      if (disposed || !isCurrent()) return 'stale';
      check(!historyFlight, 'app_source_history_busy');
      check(!older || history.nextCursor, 'app_source_history_end');
      const request = { generation: ++historyGeneration, before: older ? history.targets.at(-1)?.revision : null,
        args: { appId, expectedAccountId: accountId, limit: PAGE_SIZE, ...(older ? { cursor: history.nextCursor } : {}) } };
      historyFlight = request; history.loading = true;
      const current = () => !disposed && isCurrent() && request.generation === historyGeneration;
      try {
        const response = await api.request('apps.source.history', clone(request.args));
        if (!current()) return 'stale';
        check(response?.schema === 'soty.app-source-history.v1' && response.appId === appId && positive(response.policyEpoch)
          && positive(response.activeTargetRevision) && [1, 2].includes(response.requiredBindingVersion)
          && Array.isArray(response.targets) && response.targets.length <= PAGE_SIZE
          && (response.nextCursor === null || typeof response.nextCursor === 'string' && response.nextCursor.length <= 1024
            && /^[A-Za-z0-9_-]+$/u.test(response.nextCursor)), 'app_source_invalid_history');
        const targets = response.targets.map(historyTarget);
        check(targets.every((item, index) => (index === 0 || item.revision < targets[index - 1].revision)
          && (request.before === null || item.revision < request.before))
          && (response.nextCursor === null || (targets.length > 0 && response.nextCursor !== request.args.cursor)), 'app_source_invalid_history');
        historyCurrent = { policyEpoch: response.policyEpoch, activeTargetRevision: response.activeTargetRevision, requiredBindingVersion: response.requiredBindingVersion };
        const mismatch = response.policyEpoch !== snapshot.publication.policyEpoch || response.activeTargetRevision !== snapshot.source.revision
          || response.requiredBindingVersion !== (snapshot.source.requiredBindingVersion ?? 1);
        historyConflict = mismatch && response.policyEpoch >= snapshot.publication.policyEpoch;
        if (historyConflict) invalidatePreparation();
        history = { loading: true, targets, nextCursor: response.nextCursor, stale: mismatch };
        return 'accepted';
      } catch (error) { if (!current()) return 'stale'; throw error; }
      finally { if (historyFlight === request) { historyFlight = null; history.loading = false; } }
    },
    dispose() { if (disposed) return; disposed = true; invalidatePreparation(); historyGeneration++; },
  };
  return controller;
}
