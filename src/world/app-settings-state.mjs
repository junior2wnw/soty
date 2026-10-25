import { normalizeAppSourceTarget, supportedAppSourceProfile, appSourceExposureScope } from './app-source-state.mjs';

const appIdPattern = /^app-[a-f0-9]{32}$/u, domainIdPattern = /^dom_[a-f0-9]{32}$/u;
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value);
const positive = value => Number.isSafeInteger(value) && value >= 1;
const revision = value => Number.isSafeInteger(value) && value >= 0;
const clone = value => structuredClone(value);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const check = (ok, code = 'app_settings_invalid_intent') => { if (!ok) fail(code); };
const operations = ['apps.publication.update', 'apps.domains.claim', 'apps.domains.retire', 'apps.source.promote'];
const audienceModes = ['private', 'friends', 'link', 'public'];
export function appAudiencePublicationFields(audience) {
  check(audience === null || audienceModes.includes(audience), 'invalid_app_audience');
  return audience === null ? { audience } : { audience, launchPolicy: ['link', 'public'].includes(audience) ? 'anyone' : 'restricted', listed: audience === 'public' };
}
function uniqueIds(value, pattern = null, code = 'app_settings_invalid_intent') {
  check(Array.isArray(value) && value.length <= 100 && value.every(item => pattern ? typeof item === 'string' && pattern.test(item) : id(item)), code);
  check(new Set(value).size === value.length, code); return [...value].sort();
}
function normalize(op, args, includeRequest = false) {
  check(operations.includes(op) && args && typeof args === 'object' && !Array.isArray(args));
  const fields = ['appId', 'expectedAccountId', ...(includeRequest ? ['requestId'] : []),
    ...(op === 'apps.publication.update' || op === 'apps.source.promote' ? ['expectedPolicyEpoch', 'expectedTargetRevision', 'launchPolicy', 'listed', 'exposureAck',
      ...(op === 'apps.source.promote' ? ['preparationId', 'nativeExposureAck', 'nativeEntryAction'] : ['activeDomainIds', 'audience'])]
      : ['expectedDomainsRevision', op === 'apps.domains.claim' ? 'slug' : 'domainId'])];
  check(Object.keys(args).every(key => fields.includes(key)) && typeof args.appId === 'string' && appIdPattern.test(args.appId) && id(args.expectedAccountId));
  if (includeRequest) check(id(args.requestId));
  const base = { appId: args.appId, expectedAccountId: args.expectedAccountId };
  if (op === 'apps.publication.update' || op === 'apps.source.promote') {
    const source = op === 'apps.source.promote', audienceSpecified = !source && Object.hasOwn(args, 'audience');
    const audience = audienceSpecified ? appAudiencePublicationFields(args.audience) : null;
    if (audience && audience.audience !== null) check(args.launchPolicy === audience.launchPolicy && args.listed === audience.listed, 'invalid_app_audience_policy');
    check(positive(args.expectedPolicyEpoch) && positive(args.expectedTargetRevision)
      && ['restricted', 'anyone'].includes(args.launchPolicy) && typeof args.listed === 'boolean');
    const activeDomainIds = source ? null : uniqueIds(args.activeDomainIds, domainIdPattern);
    check(!args.listed || (args.launchPolicy === 'anyone' && (source || activeDomainIds.length > 0)));
    if (audience && ['link', 'public'].includes(audience.audience)) check(activeDomainIds.length > 0, 'app_publication_domain_required');
    if (source) check(typeof args.preparationId === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(args.preparationId), 'invalid_source_preparation');
    let exposureAck;
    if (args.launchPolicy === 'anyone') {
      const ack = args.exposureAck;
      check(ack && typeof ack === 'object' && !Array.isArray(ack) && Object.keys(ack).length === 4
        && (ack.scope === 'whole-port' || ack.scope === 'authenticated-workspace' && ack.profile === 'soty.selected-human-embed.v2') && positive(ack.targetRevision)
        && (source || ack.targetRevision === args.expectedTargetRevision)
        && typeof ack.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(ack.targetDigest) && supportedAppSourceProfile(ack.profile), 'app_exposure_ack_required');
      exposureAck = { scope: ack.scope, targetRevision: ack.targetRevision, targetDigest: ack.targetDigest, profile: ack.profile };
    } else check(args.exposureAck === undefined || args.exposureAck === null);
    Object.assign(base, { expectedPolicyEpoch: args.expectedPolicyEpoch, expectedTargetRevision: args.expectedTargetRevision,
      launchPolicy: args.launchPolicy, listed: args.listed, ...(source ? { preparationId: args.preparationId } : { activeDomainIds }),
      ...(exposureAck ? { exposureAck } : {}), ...(audienceSpecified ? { audience: args.audience } : {}) });
    if (source && args.nativeExposureAck !== undefined) {
      const ack = args.nativeExposureAck;
      check(ack && typeof ack === 'object' && Object.keys(ack).sort().join(',') === 'profile,scope,targetDigest,targetRevision'
        && args.launchPolicy === 'anyone' && ack.scope === 'whole-port' && ack.profile === 'soty.relay-restricted.v1'
        && positive(ack.targetRevision) && /^[a-f0-9]{64}$/u.test(ack.targetDigest), 'app_native_exposure_ack_required');
      base.nativeExposureAck = { scope: ack.scope, targetRevision: ack.targetRevision, targetDigest: ack.targetDigest, profile: ack.profile };
    }
    if (source && args.nativeEntryAction !== undefined) {
      const action = args.nativeEntryAction;
      check(action && typeof action === 'object' && Object.keys(action).sort().join(',') === 'action,origin,targetDigest,targetRevision'
        && action.action === 'remove' && positive(action.targetRevision) && /^[a-f0-9]{64}$/u.test(action.targetDigest)
        && validOrigin(action.origin) && !base.nativeExposureAck, 'invalid_native_pair_action');
      base.nativeEntryAction = { action: 'remove', origin: action.origin, targetRevision: action.targetRevision, targetDigest: action.targetDigest };
    }
  } else {
    check(revision(args.expectedDomainsRevision)); base.expectedDomainsRevision = args.expectedDomainsRevision;
    if (op === 'apps.domains.claim') { check(typeof args.slug === 'string' && /^[A-Za-z0-9][A-Za-z0-9-]{1,46}[A-Za-z0-9]$/u.test(args.slug), 'invalid_app_slug'); base.slug = args.slug.toLowerCase(); }
    else { check(typeof args.domainId === 'string' && domainIdPattern.test(args.domainId)); base.domainId = args.domainId; }
  }
  if (includeRequest) base.requestId = args.requestId;
  return base;
}
function validOrigin(value) { try { const url = new URL(value); return typeof value === 'string' && value.length <= 2048
  && url.protocol === 'https:' && url.origin === value && !url.username && !url.password; } catch { return false; } }
function normalizedPending(op, args, expectedSource, includeRequest = false, expectedNativeEntry) {
  const value = normalize(op, args, includeRequest);
  if (op !== 'apps.source.promote') { check(expectedSource === undefined && expectedNativeEntry === undefined); return { op, args: value }; }
  const target = normalizeAppSourceTarget(expectedSource);
  if (value.exposureAck) check(value.exposureAck.targetRevision === target.revision && value.exposureAck.targetDigest === target.digest
    && value.exposureAck.profile === target.profile, 'app_exposure_ack_required');
  if (value.nativeExposureAck) check(expectedNativeEntry && typeof expectedNativeEntry === 'object'
    && Object.keys(expectedNativeEntry).sort().join(',') === 'domainId,origin' && validOrigin(expectedNativeEntry.origin)
    && domainIdPattern.test(expectedNativeEntry.domainId), 'app_settings_invalid_intent');
  else check(expectedNativeEntry === undefined);
  return { op, args: value, expectedSource: target, ...(expectedNativeEntry ? { expectedNativeEntry: clone(expectedNativeEntry) } : {}) };
}
async function requestHash(requestId) {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(requestId)));
  return Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
}
async function verifyReceipt(pending, response) {
  pending = normalizedPending(pending.op, pending.args, pending.expectedSource, true, pending.expectedNativeEntry);
  const args = pending.args, receipt = response?.receipt;
  check(response && typeof response === 'object' && !Array.isArray(response)
    && response.requestId === args.requestId && typeof response.replayed === 'boolean'
    && receipt && typeof receipt === 'object' && !Array.isArray(receipt) && receipt.appId === args.appId
    && receipt.requestKeyHash === await requestHash(args.requestId), 'app_settings_invalid_receipt');
  if (pending.op === 'apps.publication.update') {
    const audienceSpecified = Object.hasOwn(args, 'audience');
    check(receipt.schema === (audienceSpecified ? 'soty.app-publication-receipt.v2' : 'soty.app-publication-receipt.v1')
      && receipt.namespace === (audienceSpecified ? 'apps.publication.update.v2' : 'apps.publication.update.v1')
      && (audienceSpecified ? receipt.audience === args.audience : !Object.hasOwn(receipt, 'audience'))
      && receipt.policyEpoch === args.expectedPolicyEpoch + 1 && receipt.targetRevision === args.expectedTargetRevision
      && receipt.launchPolicy === args.launchPolicy && receipt.listed === args.listed
      && same(uniqueIds(receipt.activeDomainIds, domainIdPattern, 'app_settings_invalid_receipt'), args.activeDomainIds)
      && same(receipt.exposureAck ?? null, args.exposureAck ?? null)
      && typeof receipt.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(receipt.targetDigest) && supportedAppSourceProfile(receipt.profile)
      && (!args.exposureAck || (receipt.targetDigest === args.exposureAck.targetDigest && receipt.profile === args.exposureAck.profile))
      && response.current?.appId === args.appId && positive(response.current.policyEpoch)
      && response.current.policyEpoch >= receipt.policyEpoch
      && (!audienceSpecified || response.current.policyEpoch > receipt.policyEpoch || response.current.audience === args.audience), 'app_settings_invalid_receipt');
  } else if (pending.op === 'apps.source.promote') {
    const target = pending.expectedSource;
    const paired = Boolean(args.nativeExposureAck || args.nativeEntryAction);
    const native = receipt.nativeEntry, expectedNative = pending.expectedNativeEntry;
    check(receipt.schema === (paired ? 'soty.app-source-receipt.v2' : 'soty.app-source-receipt.v1')
      && receipt.namespace === (paired ? 'apps.source.promote.v2' : 'apps.source.promote.v1')
      && receipt.preparationKeyHash === await requestHash(args.preparationId)
      && receipt.previousTargetRevision === args.expectedTargetRevision && receipt.policyEpoch === args.expectedPolicyEpoch + 1
      && receipt.targetRevision === target.revision && receipt.targetDigest === target.digest && receipt.profile === target.profile
      && receipt.requiredBindingVersion === 2 && receipt.launchPolicy === args.launchPolicy && receipt.listed === args.listed
      && same(receipt.exposureAck ?? null, args.exposureAck ?? null) && revision(receipt.committedAt)
      && response.current?.appId === args.appId && positive(response.current.policyEpoch)
      && response.current.policyEpoch >= receipt.policyEpoch && response.current.requiredBindingVersion === 2
      && positive(response.current.activeTargetRevision), 'app_settings_invalid_receipt');
    check(same(receipt.nativeEntryAction ?? null, args.nativeEntryAction ?? null)
      && (args.nativeExposureAck ? native && native.origin === expectedNative.origin && native.domainId === expectedNative.domainId
        && same(native.exposureAck, args.nativeExposureAck) && native.target?.revision === args.nativeExposureAck.targetRevision
        && native.target?.digest === args.nativeExposureAck.targetDigest && native.target?.profile === args.nativeExposureAck.profile
        : native === undefined), 'app_settings_invalid_receipt');
  } else {
    const action = pending.op === 'apps.domains.claim' ? 'claim' : 'retire';
    check(receipt.schema === 'soty.app-domain-receipt.v1' && receipt.action === action
      && receipt.revision === args.expectedDomainsRevision + 1 && typeof receipt.domainId === 'string' && domainIdPattern.test(receipt.domainId)
      && receipt.state === (action === 'claim' ? 'bound' : 'tombstone')
      && (action === 'claim' ? receipt.slug === args.slug : receipt.domainId === args.domainId), 'app_settings_invalid_receipt');
  }
}

/** Cross-tab serialization covers local transitions, never the network wait. */
export function createAppSettingsState({ accountId, appId, storage, locks, randomId = () => crypto.randomUUID() }) {
  check(id(accountId) && typeof appId === 'string' && appIdPattern.test(appId), 'app_settings_invalid_scope');
  const key = `soty.app-settings.v1:${accountId}:${appId}`;
  const fresh = () => ({ schema: 1, accountId, appId, revision: 0, pending: null });
  function load() {
    try {
      const raw = storage.getItem(key); if (raw === null) return fresh();
      check(typeof raw === 'string' && raw.length <= 100_000);
      const value = JSON.parse(raw);
      check(value?.schema === 1 && value.accountId === accountId && value.appId === appId && revision(value.revision)
        && (value.pending === null || (typeof value.pending === 'object' && !Array.isArray(value.pending))));
      let pending = null;
      if (value.pending) {
        pending = normalizedPending(value.pending.op, value.pending.args, value.pending.expectedSource, true, value.pending.expectedNativeEntry);
        check(pending.args.expectedAccountId === accountId && pending.args.appId === appId);
      }
      return { ...fresh(), revision: value.revision, pending };
    } catch { fail('app_settings_storage_unavailable'); }
  }
  function write(value) {
    try { check(revision(value.revision)); const raw = JSON.stringify(value); storage.setItem(key, raw); check(storage.getItem(key) === raw); }
    catch { fail('app_settings_storage_unavailable'); }
  }
  const locked = action => locks?.request ? locks.request(key, action) : Promise.reject(Object.assign(new Error('app_settings_lock_unavailable'), { code: 'app_settings_lock_unavailable' }));
  const matches = (expected, pending) => pending && expected && expected.op === pending.op
    && same(normalizedPending(expected.op, expected.args, expected.expectedSource, true, expected.expectedNativeEntry), pending);
  return {
    key,
    read: () => clone(load()),
    canDispatch: () => Boolean(locks?.request),
    async prepare({ op, args, expectedSource, expectedNativeEntry, beforeCreate }) {
      const normalized = normalizedPending(op, args, expectedSource, false, expectedNativeEntry), value = normalized.args;
      check(value.expectedAccountId === accountId && value.appId === appId, 'app_settings_invalid_scope');
      return locked(() => {
        const state = load();
        if (state.pending) {
          const { requestId: _requestId, ...prior } = state.pending.args;
          check(state.pending.op === op && same(value, prior) && same(normalized.expectedSource, state.pending.expectedSource)
            && same(normalized.expectedNativeEntry, state.pending.expectedNativeEntry), 'app_settings_pending_unconfirmed');
          return clone(state.pending);
        }
        if (op === 'apps.source.promote') check(typeof beforeCreate === 'function', 'apps_source_preparation_expired');
        if (beforeCreate) {
          const allowed = beforeCreate();
          if (allowed && typeof allowed.then === 'function') Promise.resolve(allowed).catch(() => {});
          check(allowed === true, 'apps_source_preparation_expired');
        }
        const requestId = randomId(); check(id(requestId));
        state.pending = { ...normalized, args: { ...value, requestId } }; state.revision++;
        write(state); return clone(state.pending);
      });
    },
    async pendingForDispatch(expectedPending) {
      // Capture before waiting for the lock: the caller's displayed record is
      // not a reference to whichever command another tab may write next.
      check(expectedPending, 'app_settings_pending_changed');
      const expected = normalizedPending(expectedPending.op, expectedPending.args, expectedPending.expectedSource, true, expectedPending.expectedNativeEntry);
      return locked(() => {
        const state = load();
        check(matches(expected, state.pending), 'app_settings_pending_changed');
        return clone(state.pending);
      });
    },
    async acknowledge(expected, response) {
      await verifyReceipt(expected, response);
      return locked(() => { const state = load(); if (!matches(expected, state.pending)) return false; state.pending = null; state.revision++; write(state); return true; });
    },
    async abandon(expected) {
      return locked(() => { const state = load(); if (!matches(expected, state.pending)) return false; state.pending = null; state.revision++; write(state); return true; });
    },
  };
}

/** Server-relative freshness, conservatively shortened by the entire round trip.
 * A caller counts down from receipt using its monotonic clock, never wall time. */
export function appSettingsObservationRemaining(snapshot, requestElapsedMs) {
  const observed = snapshot?.source?.observation;
  if (!observed || !['responding', 'unreachable'].includes(observed.state)
    || !Number.isSafeInteger(snapshot.checkedAt) || snapshot.checkedAt < 0
    || !Number.isSafeInteger(observed.freshUntil)
    || !Number.isFinite(requestElapsedMs) || requestElapsedMs < 0) return 0;
  return Math.max(0, Math.min(45_000, observed.freshUntil - snapshot.checkedAt) - requestElapsedMs);
}

/** Caller renders only while current. An exact late ACK may settle its own scoped record. */
export async function dispatchAppSettingsIntent({ state, op, args, expectedSource, expectedNativeEntry, beforeCreate, expectedPending, api, isCurrent }) {
  if (!isCurrent()) return { status: 'stale' };
  if (op) check(expectedPending === undefined);
  const pending = op ? await state.prepare({ op, args, expectedSource, expectedNativeEntry, beforeCreate }) : await state.pendingForDispatch(expectedPending);
  if (!isCurrent()) return { status: 'stale', pending };
  const response = await api.request(pending.op, clone(pending.args));
  const accepted = await state.acknowledge(pending, response);
  if (!isCurrent()) return { status: 'stale', pending };
  return { status: accepted ? 'accepted' : 'superseded', pending, response };
}

export function appSettingsUpdateArgs(snapshot, draft, kind, accountId) {
  check(snapshot?.app && typeof snapshot.app.id === 'string' && appIdPattern.test(snapshot.app.id) && positive(snapshot.app.revision) && id(accountId));
  const result = { appId: snapshot.app.id, expectedAccountId: accountId, expectedRevision: snapshot.app.revision };
  if (kind === 'name') {
    check(typeof draft.name === 'string' && draft.name.trim().length > 0 && draft.name.trim().length <= 64 && !/[\u0000-\u001f\u007f]/u.test(draft.name), 'invalid_app_name');
    result.name = draft.name.trim();
  } else {
    check(kind === 'grants'); result.grants = { accountIds: uniqueIds(snapshot.app.grants.accountIds), communityIds: uniqueIds(draft.communityIds) };
  }
  return result;
}

export function appPublicationArgs(snapshot, draft, accountId) {
  check(snapshot?.app && snapshot?.source && snapshot?.publication);
  const activeDomainIds = uniqueIds(draft.activeDomainIds, domainIdPattern);
  check(activeDomainIds.every(value => snapshot.addresses.aliases.some(alias => alias.id === value && alias.state === 'bound')), 'app_publication_domain_unavailable');
  if (draft.launchPolicy === 'anyone') check(draft.exposureConfirmed === true && activeDomainIds.length > 0, 'app_exposure_ack_required');
  return normalize('apps.publication.update', { appId: snapshot.app.id, expectedAccountId: accountId,
    expectedPolicyEpoch: snapshot.publication.policyEpoch, expectedTargetRevision: snapshot.source.revision,
    launchPolicy: draft.launchPolicy, listed: draft.launchPolicy === 'anyone' ? (draft.listed ?? snapshot.publication.listed) : false, activeDomainIds,
    ...(Object.hasOwn(draft, 'audience') ? { audience: draft.audience } : {}),
    ...(draft.launchPolicy === 'anyone' ? { exposureAck: { scope: appSourceExposureScope(snapshot.source.profile), targetRevision: snapshot.source.revision,
      targetDigest: snapshot.source.digest, profile: snapshot.source.profile } } : {}),
  });
}

/** Read refreshes are observations. An edited CAS base changes only after its
 * matching accepted command, or an explicit reset chosen by the user. */
export function createAppSettingsDraftState(initial) {
  let snapshot = clone(initial), nameBase = clone(initial), grantsBase = clone(initial), publicationBase = clone(initial);
  const freshDraft = value => ({ name: value.app.name, communityIds: [...value.app.grants.communityIds],
    launchPolicy: value.publication.launchPolicy, listed: value.publication.listed, activeDomainIds: [...value.publication.activeDomainIds], exposureConfirmed: false, slug: '',
    ...(Object.hasOwn(value.publication, 'audience') ? { audience: value.publication.audience } : {}) });
  let draft = freshDraft(initial);
  const idsEqual = (a, b) => same([...a].sort(), [...b].sort());
  const nameDirty = () => draft.name !== nameBase.app.name;
  const grantsDirty = () => !idsEqual(draft.communityIds, grantsBase.app.grants.communityIds);
  const publicationDirty = () => draft.audience !== publicationBase.publication.audience
    || draft.launchPolicy !== publicationBase.publication.launchPolicy
    || (draft.launchPolicy === 'anyone' && draft.listed !== publicationBase.publication.listed)
    || !idsEqual(draft.activeDomainIds, publicationBase.publication.activeDomainIds);
  const unavailableDomainIds = () => draft.activeDomainIds.filter(id => !snapshot.addresses.aliases.some(alias => alias.id === id && alias.state === 'bound'));
  return {
    read() { return clone({ snapshot, draft, nameDirty: nameDirty(), grantsDirty: grantsDirty(), publicationDirty: publicationDirty(),
      nameConflict: nameDirty() && nameBase.app.revision !== snapshot.app.revision,
      grantsConflict: grantsDirty() && grantsBase.app.revision !== snapshot.app.revision,
      unavailableDomainIds: unavailableDomainIds(),
      publicationConflict: unavailableDomainIds().length > 0 || (publicationDirty() && (publicationBase.publication.policyEpoch !== snapshot.publication.policyEpoch
        || publicationBase.source.revision !== snapshot.source.revision || publicationBase.source.digest !== snapshot.source.digest)) }); },
    base(kind) { check(['name', 'grants', 'publication'].includes(kind)); return clone(kind === 'name' ? nameBase : kind === 'grants' ? grantsBase : publicationBase); },
    patch(value) {
      check(value && Object.keys(value).every(key => ['name', 'communityIds', 'launchPolicy', 'listed', 'activeDomainIds', 'exposureConfirmed', 'slug', 'audience'].includes(key)));
      draft = { ...draft, ...clone(value), ...(Object.hasOwn(value, 'audience') ? appAudiencePublicationFields(value.audience) : {}) };
    },
    observe(next, completed) {
      check(next?.app?.id === snapshot.app.id, 'app_settings_invalid_scope');
      const nameDone = completed?.kind === 'name' && draft.name.trim() === completed.args.name;
      const grantsDone = completed?.kind === 'grants' && idsEqual(draft.communityIds, completed.args.grants.communityIds);
      const pubDone = completed?.kind === 'publication'
        && (Object.hasOwn(completed.args, 'audience') ? draft.audience === completed.args.audience : draft.audience === publicationBase.publication.audience)
        && draft.launchPolicy === completed.args.launchPolicy
        && (draft.launchPolicy !== 'anyone' || draft.listed === completed.args.listed)
        && idsEqual(draft.activeDomainIds, completed.args.activeDomainIds)
        && (draft.launchPolicy !== 'anyone' || (draft.exposureConfirmed && publicationBase.source.digest === completed.args.exposureAck?.targetDigest));
      if (snapshot.source.revision !== next.source.revision || snapshot.source.digest !== next.source.digest
        || snapshot.source.profile !== next.source.profile) draft.exposureConfirmed = false;
      if (!nameDirty() || nameDone) { nameBase = clone(next); draft.name = next.app.name; }
      if (!grantsDirty() || grantsDone) { grantsBase = clone(next); draft.communityIds = [...next.app.grants.communityIds]; }
      if (!publicationDirty() || pubDone) {
        publicationBase = clone(next);
        if (Object.hasOwn(next.publication, 'audience')) draft.audience = next.publication.audience; else delete draft.audience;
        draft.launchPolicy = next.publication.launchPolicy; draft.listed = next.publication.listed; draft.activeDomainIds = [...next.publication.activeDomainIds]; draft.exposureConfirmed = false;
      } else publicationBase.addresses = clone(next.addresses); // New claims are selectable; the policy/target CAS is not rebased.
      snapshot = clone(next);
    },
    reset() { const slug = draft.slug; nameBase = clone(snapshot); grantsBase = clone(snapshot); publicationBase = clone(snapshot); draft = { ...freshDraft(snapshot), slug }; },
    resetPublication() { publicationBase = clone(snapshot);
      if (Object.hasOwn(snapshot.publication, 'audience')) draft.audience = snapshot.publication.audience; else delete draft.audience;
      draft.launchPolicy = snapshot.publication.launchPolicy;
      draft.listed = snapshot.publication.listed;
      draft.activeDomainIds = [...snapshot.publication.activeDomainIds]; draft.exposureConfirmed = false; },
  };
}
