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
test('invalid and ambiguous deployment data cannot generate operator instructions', () => {
  for (const mutate of [
    value => { value.shellOrigin = 'https://user:secret@shell.example'; },
    value => { value.source.entryPath = '/_soty/ingress-check'; },
    value => { value.source.digest = 'invalid'; },
    value => { value.source.revision++; },
    value => { value.addresses.aliases[0].state = 'tombstone'; },
    value => { value.addresses.aliases.push({ ...value.addresses.aliases[0] }); },
  ]) {
    const value = exportAppDeployment(inspection(), 'https://shell.example'); mutate(value); assert.throws(() => validateAppDeployment(value));
  }
});
