import test from 'node:test';
import assert from 'node:assert/strict';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { captureExternalAdapters, EXTERNAL_ADAPTER_PROFILE } from '../../modules/capabilities/server/external-adapters.mjs';
import { captureReadonlyQueryAdapters, createTrustedReadonlyQueryAdapter } from '../../modules/capabilities/server/readonly-queries.mjs';
import { createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { captureExternalApplications } from '../external-applications.js';
import { createAppFeedbackCapabilities } from '../app-feedback-capabilities.js';

const closed = () => { throw new Error('no_runtime_in_composition_test'); };
function source(index, readonly = false) {
  const capabilityId = 'app.fixture:operation' + index;
  const schema = { type: 'object', properties: {}, required: [], additionalProperties: false };
  const catalog = { capabilityId, version: 1, appId: 'fixture', title: 'Fixture operation', description: 'Private synthetic Source contract.',
    visibility: 'private', executionEnabled: true, inputSchema: schema, outputSchema: schema,
    resources: ['fixture:resource'], effects: readonly ? [] : ['create'], recipients: ['fixture:resource'],
    executionBinding: { kind: 'registered', handler: capabilityId, version: 1,
      binding: { id: 'fixture:binding', version: 1, digest: 'a'.repeat(64) } } };
  const adapter = readonly ? createTrustedReadonlyQueryAdapter({ withAuthority: closed, query: closed })
    : { profile: EXTERNAL_ADAPTER_PROFILE, withAuthority: closed, execute: closed, readProof: closed };
  return { appId: 'fixture', target: { revision: 1, digest: 'b'.repeat(64) }, catalog, adapter };
}
test('all previously accepted 64 Source contracts coexist with the fixed seven, with the public capture bound unchanged', () => {
  const raw = Array.from({ length: 64 }, (_, index) => source(index, index % 2 === 0));
  const captured = captureExternalApplications(raw);
  assert.equal(captured.length, 64);
  assert.throws(() => captureExternalApplications([...raw, source(64)]), /external_application_configuration_invalid/u);
  assert.throws(() => captureExternalApplications([...raw.slice(1), raw[1]]), /external_application_configuration_invalid/u);
  const bridge = createAppFeedbackCapabilities({ capabilities: closed, connect: closed, apps: closed, feedback: closed });
  const service = createCapabilitiesService({ databasePath: ':memory:', projectId: 'bounded-composition', actorActive: () => false,
    catalog: [...captured.map(value => value.catalog), ...bridge.catalog], documentation: [],
    externalAdapters: [...captured.filter(value => !value.readonly).map(({ contract, adapter }) => ({ contract, adapter })), ...bridge.effects],
    readonlyQueries: [...captured.filter(value => value.readonly).map(({ contract, adapter }) => ({ contract, adapter })), ...bridge.queries] });
  try { assert.equal(service.external.contracts.length, 71); } finally { service.close(); }
});
test('the catalog-bound internal capture admits 128 unique contracts and refuses 129 or duplicates in either path', () => {
  for (const readonly of [false, true]) {
    const raw = Array.from({ length: 129 }, (_, index) => source(index, readonly));
    const values = raw.map(({ catalog, adapter }) => {
      const entry = createCatalog([catalog]).get(catalog.capabilityId, 1);
      return { contract: { capabilityId: entry.capabilityId, version: 1, digest: entry.digest }, adapter };
    });
    const capture = readonly ? captureReadonlyQueryAdapters : captureExternalAdapters;
    assert.equal(capture(values.slice(0, 128)).length, 128);
    assert.throws(() => capture(values));
    assert.throws(() => capture([values[0], values[0]]));
  }
});
