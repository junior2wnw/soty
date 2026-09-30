import test from 'node:test';
import assert from 'node:assert/strict';
import { appPublicationArgs, appSettingsObservationRemaining, appSettingsUpdateArgs, createAppSettingsDraftState } from './app-settings-state.mjs';

const appId = `app-${'1'.repeat(32)}`, first = `dom_${'a'.repeat(32)}`, second = `dom_${'b'.repeat(32)}`;
function inspection() {
  return {
    schema: 'soty.app-inspection.v1', checkedAt: 100_000,
    app: { id: appId, name: 'Мой проект', state: 'enabled', revision: 6, grants: { accountIds: ['person-2', 'person-1'], communityIds: ['old-community'] } },
    addresses: { revision: 3, claimOrigin: 'https://apps.example', canonical: null,
      aliases: [{ id: first, slug: 'project-one', origin: 'https://project-one.apps.example', state: 'bound', active: true, shareUrl: 'https://shell.example/#launch', createdAt: 1, retiredAt: null },
        { id: second, slug: 'project-two', origin: 'https://project-two.apps.example', state: 'bound', active: false, shareUrl: null, createdAt: 2, retiredAt: null }],
      limits: { perApp: 8, perAccount: 80, usedByApp: 2, usedByAccount: 2 } },
    publication: { policyEpoch: 9, launchPolicy: 'restricted', listed: false, activeDomainIds: [first], activeTargetRevision: 2 },
    source: { hostDeviceId: 'device-1', connectorId: 'connector-1', deviceName: 'Ноутбук', port: 4500, entryPath: '/#/dashboard', revision: 2, digest: 'c'.repeat(64), profile: 'soty.relay-restricted.v1',
      observation: { state: 'responding', observedAt: 70_000, freshUntil: 115_000, evidence: 'connector-v1-observation' } },
    actions: { canEdit: true, canPublish: true, canPreview: true, canReserveName: true },
  };
}
const draft = value => createAppSettingsDraftState(value).read().draft;

test('freshness uses only server remaining time, shortened by the complete monotonic round trip', () => {
  const value = inspection();
  assert.equal(appSettingsObservationRemaining(value, 200), 14_800);
  value.checkedAt = 114_000;
  assert.equal(appSettingsObservationRemaining(value, 250), 750);
  assert.equal(appSettingsObservationRemaining(value, 1001), 0);
  value.checkedAt = 115_000;
  assert.equal(appSettingsObservationRemaining(value, 0), 0);
});

test('unknown or malformed freshness fails closed and never creates a window beyond 45 seconds', () => {
  const value = inspection(); value.source.observation.freshUntil = 9_999_999;
  assert.equal(appSettingsObservationRemaining(value, 100), 44_900);
  for (const elapsed of [NaN, Infinity, -1]) assert.equal(appSettingsObservationRemaining(value, elapsed), 0);
  for (const checkedAt of [undefined, null, -1, 1.5, Infinity]) assert.equal(appSettingsObservationRemaining({ ...value, checkedAt }, 0), 0);
  for (const state of ['unknown', 'offline']) {
    value.source.observation.state = state; assert.equal(appSettingsObservationRemaining(value, 0), 0);
  }
});

test('name-only CAS neither resubmits nor deletes existing contacts or inaccessible community grants', () => {
  const value = inspection(), input = { ...draft(value), name: '  Новое имя  ', communityIds: [] };
  assert.deepEqual(appSettingsUpdateArgs(value, input, 'name', 'owner'), {
    appId, expectedAccountId: 'owner', expectedRevision: 6, name: 'Новое имя',
  });
  assert.deepEqual(value.app.grants, { accountIds: ['person-2', 'person-1'], communityIds: ['old-community'] });
});

test('grants-only CAS preserves all account grants and omits unrelated name', () => {
  const value = inspection(), input = { ...draft(value), name: 'Неотправленное имя', communityIds: ['group-b', 'group-a'] };
  assert.deepEqual(appSettingsUpdateArgs(value, input, 'grants', 'owner'), {
    appId, expectedAccountId: 'owner', expectedRevision: 6,
    grants: { accountIds: ['person-1', 'person-2'], communityIds: ['group-a', 'group-b'] },
  });
});

test('name validation rejects an empty or control-bearing value before a request is constructed', () => {
  const value = inspection();
  for (const name of ['', '   ', 'a'.repeat(65), 'first\nsecond']) assert.throws(() => appSettingsUpdateArgs(value, { ...draft(value), name }, 'name', 'owner'), { code: 'invalid_app_name' });
  assert.throws(() => appSettingsUpdateArgs({ ...value, app: { ...value.app, revision: 0 } }, draft(value), 'name', 'owner'));
});

test('checking consent alone does not enable an epoch-changing no-op on an already public app', () => {
  const value = inspection(); value.publication.launchPolicy = 'anyone';
  const model = createAppSettingsDraftState(value); model.patch({ exposureConfirmed: true });
  assert.equal(model.read().publicationDirty, false);
  const next = structuredClone(value); next.checkedAt += 20_000; model.observe(next);
  assert.equal(model.read().publicationDirty, false);
  assert.equal(model.read().draft.exposureConfirmed, false);
});

test('retiring an inactive selected alias exposes a removable conflict without discarding other edits', () => {
  const value = inspection(), model = createAppSettingsDraftState(value);
  model.patch({ name: 'Мой черновик', activeDomainIds: [first, second] });
  const next = structuredClone(value); next.addresses.aliases[1].state = 'tombstone'; next.addresses.revision++;
  model.observe(next);
  assert.deepEqual(model.read().unavailableDomainIds, [second]);
  assert.equal(model.read().publicationConflict, true);
  assert.deepEqual(model.read().draft.activeDomainIds, [first, second]);
  assert.throws(() => appPublicationArgs(model.base('publication'), model.read().draft, 'owner'), { code: 'app_publication_domain_unavailable' });
  const removed = new Set(model.read().unavailableDomainIds);
  model.patch({ activeDomainIds: model.read().draft.activeDomainIds.filter(id => !removed.has(id)), exposureConfirmed: false });
  assert.equal(model.read().publicationConflict, false);
  assert.equal(model.read().draft.name, 'Мой черновик');
  assert.equal(model.base('publication').publication.policyEpoch, 9);
});

test('a missing selected domain is a conflict too, not a silently removed address', () => {
  const value = inspection(), model = createAppSettingsDraftState(value);
  model.patch({ activeDomainIds: [second] });
  const next = structuredClone(value); next.addresses.aliases.pop(); model.observe(next);
  assert.deepEqual(model.read().unavailableDomainIds, [second]);
  assert.equal(model.read().publicationConflict, true);
  model.reset(); assert.deepEqual(model.read().draft.activeDomainIds, [first]);
});

test('claim observation exposes a new alias without replacing a dirty policy or source CAS base', () => {
  const value = inspection(), model = createAppSettingsDraftState(value);
  model.patch({ launchPolicy: 'anyone', exposureConfirmed: true, name: 'Не терять' });
  const next = structuredClone(value), added = `dom_${'d'.repeat(32)}`;
  next.addresses.aliases.push({ ...next.addresses.aliases[1], id: added, slug: 'new-project' }); next.addresses.revision++;
  next.publication.policyEpoch++; next.source.revision++; next.source.digest = 'e'.repeat(64);
  model.observe(next); model.patch({ activeDomainIds: [added] });
  const args = appPublicationArgs(model.base('publication'), model.read().draft, 'owner');
  assert.equal(args.expectedPolicyEpoch, 9); assert.equal(args.expectedTargetRevision, 2);
  assert.equal(args.exposureAck.targetDigest, 'c'.repeat(64)); assert.deepEqual(args.activeDomainIds, [added]);
  assert.equal(model.read().publicationConflict, true); assert.equal(model.read().draft.name, 'Не терять');
});

test('a late accepted name cannot erase a newer draft or silently rebase it', () => {
  const value = inspection(), model = createAppSettingsDraftState(value);
  model.patch({ name: 'Первый' }); const args = appSettingsUpdateArgs(model.base('name'), model.read().draft, 'name', 'owner');
  model.patch({ name: 'Второй' }); const next = structuredClone(value); next.app.name = 'Первый'; next.app.revision++;
  model.observe(next, { kind: 'name', args });
  assert.equal(model.read().draft.name, 'Второй'); assert.equal(model.base('name').app.revision, 6); assert.equal(model.read().nameConflict, true);
});

test('restricted intent removes listed and whole-port acknowledgment while public intent preserves the legacy flag', () => {
  const value = inspection(); value.publication.launchPolicy = 'anyone'; value.publication.listed = true;
  const restricted = appPublicationArgs(value, { ...draft(value), launchPolicy: 'restricted', exposureConfirmed: true }, 'owner');
  assert.equal(restricted.listed, false); assert.equal(Object.hasOwn(restricted, 'exposureAck'), false);
  const anyone = appPublicationArgs(value, { ...draft(value), exposureConfirmed: true }, 'owner');
  assert.equal(anyone.listed, true); assert.deepEqual(anyone.exposureAck, { scope: 'whole-port', targetRevision: 2, targetDigest: 'c'.repeat(64), profile: 'soty.relay-restricted.v1' });
});

test('public consent applies to the exact source and requires a usable named alias', () => {
  const value = inspection();
  for (const input of [{ ...draft(value), launchPolicy: 'anyone' }, { ...draft(value), launchPolicy: 'anyone', activeDomainIds: [], exposureConfirmed: true }]) assert.throws(() => appPublicationArgs(value, input, 'owner'), { code: 'app_exposure_ack_required' });
  const unsupported = structuredClone(value); unsupported.source.profile = 'future-profile';
  assert.throws(() => appPublicationArgs(unsupported, { ...draft(value), launchPolicy: 'anyone', exposureConfirmed: true }, 'owner'), { code: 'app_exposure_ack_required' });
});
