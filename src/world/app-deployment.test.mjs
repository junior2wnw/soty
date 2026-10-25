import test from 'node:test';
import assert from 'node:assert/strict';
import { exportAppDeployment, validateAppDeployment, preferredInspectionEntry } from './app-deployment.mjs';

const appId = 'app-' + 'a'.repeat(32), canonicalId = 'dom_' + 'b'.repeat(32), aliasId = 'dom_' + 'c'.repeat(32);
export function inspection() {
  return { schema: 'soty.app-inspection.v1', checkedAt: Date.now(),
    app: { id: appId, name: 'Project', state: 'enabled', revision: 2, grants: { accountIds: ['private-person'], communityIds: [] } },
    addresses: { revision: 2, canonical: { id: canonicalId, origin: 'https://canonical.example', shareUrl: null },
      aliases: [{ id: aliasId, origin: 'https://project.example', active: true, state: 'bound', secret: 'never exported' }] },
    publication: { policyEpoch: 2, launchPolicy: 'anyone', listed: true, activeTargetRevision: 1, activeDomainIds: [aliasId] },
    source: { port: 8111, entryPath: '/?project=fixture', revision: 1, digest: 'd'.repeat(64), profile: 'soty.relay-restricted.v1',
      hostDeviceId: 'private-host', connectorId: 'private-connector', token: 'never exported' },
    arbitrarySecret: 'never exported' };
}
test('deployment export is a strict whitelist without grants, connector identities or secret extensions', () => {
  const value = exportAppDeployment(inspection(), 'https://shell.example');
  assert.equal(value.source.profile, 'soty.relay-restricted.v1'); assert.equal(value.app.id, appId);
  for (const marker of ['private-person', 'private-host', 'private-connector', 'never exported']) assert.equal(JSON.stringify(value).includes(marker), false);
  const extra = structuredClone(value); extra.source.token = 'sensitive';
  assert.throws(() => validateAppDeployment(extra), { code: 'invalid_app_deployment' });
});
test('fresh metadata chooses an enabled named entry, then canonical; revoked or malformed entries do not open', () => {
  const value = inspection(); assert.equal(preferredInspectionEntry(value).domainId, aliasId);
  value.addresses.aliases[0].active = false; assert.equal(preferredInspectionEntry(value).domainId, canonicalId);
  value.source.entryPath = '//outside.example'; assert.equal(preferredInspectionEntry(value), undefined);
  value.source.entryPath = '/'; value.app.state = 'revoked'; assert.equal(preferredInspectionEntry(value), undefined);
});
test('Source Open uses the admitted address and never falls back to another app or alias', () => {
  const value = inspection(); value.source.profile = 'soty.selected-human-embed.v2'; value.source.entryPath = '/embed';
  assert.equal(preferredInspectionEntry(value), undefined, 'old or missing Source metadata is unavailable');
  value.source.launchEntry = { domainId: canonicalId, origin: value.addresses.canonical.origin, path: '/embed' };
  assert.deepEqual(preferredInspectionEntry(value), value.source.launchEntry, 'Perem keeps canonical despite an active alias');
  value.source.launchEntry = { domainId: aliasId, origin: value.addresses.aliases[0].origin, path: '/embed' };
  assert.deepEqual(preferredInspectionEntry(value), value.source.launchEntry, 'HIVE keeps its admitted alias');
  for (const bad of [null, { ...value.source.launchEntry, domainId: 'dom_' + 'f'.repeat(32) },
    { ...value.source.launchEntry, origin: 'https://other.example' }, { ...value.source.launchEntry, path: '/different' }]) {
    value.source.launchEntry = bad; assert.equal(preferredInspectionEntry(value), undefined);
  }
  value.source.launchEntry = { domainId: aliasId, origin: value.addresses.aliases[0].origin, path: '/embed' };
  value.addresses.aliases[0].active = false; assert.equal(preferredInspectionEntry(value), undefined);
});
test('invalid and ambiguous deployment data cannot generate operator instructions', () => {
  for (const mutate of [
    value => { value.shellOrigin = 'https://user:secret@shell.example'; },
    value => { value.source.entryPath = '/_soty/ingress-check'; },
    value => { value.source.digest = 'invalid'; },
    value => { value.source.digest = [value.source.digest]; },
    value => { value.addresses.aliases[0].id = [aliasId]; },
    value => { value.source.revision++; },
    value => { value.addresses.aliases[0].state = 'tombstone'; },
    value => { value.addresses.aliases.push({ ...value.addresses.aliases[0] }); },
  ]) {
    const value = exportAppDeployment(inspection(), 'https://shell.example'); mutate(value); assert.throws(() => validateAppDeployment(value));
  }
});

function pairedInspection() {
  const value = inspection(), sourceDomain = 'dom_' + 'e'.repeat(32);
  Object.assign(value.source, { profile: 'soty.selected-human-embed.v2', revision: 2, entryPath: '/embed',
    launchEntry: { domainId: sourceDomain, origin: 'https://source.example', path: '/embed', ignored: 'never exported' } });
  value.addresses.aliases.push({ id: sourceDomain, origin: 'https://source.example', state: 'bound', active: true });
  value.publication.activeTargetRevision = 2;
  value.publication.nativeEntry = { schema: 'soty.native-entry.v1', origin: 'https://project.example', domainId: aliasId,
    target: { revision: 1, digest: 'f'.repeat(64), profile: 'soty.relay-restricted.v1', port: 8112, entryPath: '/?view=mine#today', private: 'never exported' },
    committedPolicyEpoch: 2, admissionKeyHash: '1'.repeat(64), runtimeReady: false, private: 'never exported' };
  return value;
}

test('v2 export preserves both exact entries and distinct pins while omitting identities, runtime assertions and extensions', () => {
  const original = pairedInspection(), before = structuredClone(original), value = exportAppDeployment(original, 'https://shell.example');
  assert.equal(value.schema, 'soty.app-deployment.v2'); assert.equal(value.source.launchEntry.origin, 'https://source.example');
  assert.equal(value.nativeEntry.origin, 'https://project.example'); assert.equal(value.nativeEntry.target.entryPath, '/?view=mine#today');
  assert.equal(value.nativeEntry.target.digest, 'f'.repeat(64)); assert.deepEqual(original, before);
  for (const marker of ['private-person', 'private-host', 'private-connector', 'never exported', 'runtimeReady', 'soty.native-entry.v1']) assert.equal(JSON.stringify(value).includes(marker), false);
});

test('Source-only and paused pair snapshots remain inspectable, without certifying public Native admission', () => {
  const sourceOnly = pairedInspection(); delete sourceOnly.publication.nativeEntry;
  assert.equal(exportAppDeployment(sourceOnly, 'https://shell.example').nativeEntry, null);
  sourceOnly.source.launchEntry = null; assert.equal(exportAppDeployment(sourceOnly, 'https://shell.example').source.launchEntry, null);
  const paused = pairedInspection(); paused.publication.launchPolicy = 'restricted'; paused.publication.listed = false;
  paused.addresses.aliases[0].active = false; paused.addresses.aliases[0].state = 'tombstone';
  assert.equal(exportAppDeployment(paused, 'https://shell.example').nativeEntry.domainId, aliasId);
});

test('v2 operator metadata rejects forged origin/domain, same host, changed pin types and unknown fields', () => {
  for (const mutate of [
    value => { value.nativeEntry.token = 'private'; }, value => { value.nativeEntry.target.profile = 'soty.selected-human-embed.v2'; },
    value => { value.nativeEntry.target.port = value.source.port; }, value => { value.nativeEntry.target.digest = [value.nativeEntry.target.digest]; },
    value => { value.nativeEntry.admissionKeyHash = [value.nativeEntry.admissionKeyHash]; },
    value => { value.nativeEntry.domainId = 'dom_' + '9'.repeat(32); }, value => { value.nativeEntry.origin = 'https://unrelated.example'; },
    value => { value.nativeEntry.committedPolicyEpoch = value.publication.policyEpoch + 1; },
    value => { value.source.launchEntry.path = '/unreviewed'; },
    value => { value.source.launchEntry.origin = 'https://project.example'; value.source.launchEntry.domainId = aliasId; },
  ]) { const value = exportAppDeployment(pairedInspection(), 'https://shell.example'); mutate(value); assert.throws(() => validateAppDeployment(value)); }
});
