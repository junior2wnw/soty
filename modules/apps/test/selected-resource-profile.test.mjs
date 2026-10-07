import test from 'node:test';
import assert from 'node:assert/strict';
import { selectedResourceProfile, nativeResourceId } from '../scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE, selectedRoute, selectedRouteAdapter } from '../scoped-embed/resource-route-adapters.mjs';
import { scopedRoute, scopedEmbedProfile } from '../scoped-embed/profile.mjs';
const profile = () => ({ schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32),
  connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' }, target: { revision: 1, digest: '1'.repeat(64) },
  sourceProfile: { ...HIVE_SELECTED_SOURCE }, resource: { registryId: 'soty', tenantId: 'root-owner', appId: 'app-' + 'a'.repeat(32),
    environmentId: 'production', resourceId: 'hive:locator', selection: { kind: 'hive.project.v1', nativeId: ' Проект / Case ', incarnationId: 'scope-uuid' } },
  issuer: 'https://root.test/human-identity', clientId: 'hive.selected', embedOrigin: 'https://hive.root.test', nativeOrigin: 'https://hive-native.test', parentOrigin: 'https://root.test' });

test('opaque native IDs remain exact; invalid Unicode, controls and byte overflow never normalize', () => {
  for (const id of [' Проект / Case ', '00123', 'Aa/aA', '界'.repeat(1365)]) assert.equal(nativeResourceId(id), id);
  for (const id of ['', '\ud800', 'x\u0000', 'x\u0085', '界'.repeat(1366)]) assert.throws(() => nativeResourceId(id));
  const value = selectedResourceProfile(profile()); assert.equal(value.resource.selection.nativeId, ' Проект / Case ');
  assert.ok(Object.isFrozen(value.resource.selection)); assert.throws(() => { value.resource.selection.nativeId = 'other'; });
});
test('future reviewed Source kinds share the format; unknown handlers/pins cannot execute', () => {
  const future = profile(); future.resource.selection.kind = 'future.document.v7';
  assert.equal(selectedResourceProfile(future).resource.selection.kind, 'future.document.v7');
  assert.throws(() => selectedRouteAdapter(future), { code: 'scoped_embed_adapter_unapproved' });
  for (const field of ['digest', 'id', 'version']) {
    const value = profile(); value.sourceProfile[field] = field === 'version' ? 3 : field === 'id' ? 'other.source' : 'f'.repeat(64);
    assert.throws(() => selectedRouteAdapter(value), { code: 'scoped_embed_adapter_unapproved' });
  }
});
test('selected HIVE routes never expose global keys/accounts/projects/native resource parameters', () => {
  const value = profile();
  for (const path of ['/api/account/agents', '/api/hive/projects', '/mcp', '/api/embed/project?projectId=other',
    '/api/embed/project/other', '/api/embed/feedback?workspaceId=other', '/%65mbed', '/api/embed/../account', '//other.test', '/embed?x=1'])
    assert.throws(() => selectedRoute(value, 'GET', path));
  assert.throws(() => selectedRoute(value, 'GET', '/api/embed/changes?limit=1&limit=2'));
  assert.equal(selectedRoute(value, 'GET', '/api/embed/project').kind, 'read');
  assert.equal(selectedRoute(value, 'POST', '/api/embed/operations').kind, 'write');
  assert.equal(selectedRoute(value, 'GET', '/api/embed/changes?after=1&limit=25').kind, 'read');
});
test('lawful feedback upload/get caps are fixed-route only; legacy Planner v1 surface is unchanged', () => {
  assert.equal(selectedRoute(profile(), 'POST', '/api/embed/feedback').requestBytes, 1500000);
  assert.equal(selectedRoute(profile(), 'GET', '/api/embed/feedback/ticket?ticketId=f_123').responseBytes, 2097152);
  assert.equal(selectedRoute(profile(), 'PUT', '/api/embed/project').requestBytes, 1048576);
  assert.throws(() => scopedRoute('POST', '/api/embed/feedback'));
  assert.throws(() => scopedEmbedProfile(profile()));
  const spoof = profile(); spoof.resource.selection.owner = true;
  assert.throws(() => selectedResourceProfile(spoof));
});
