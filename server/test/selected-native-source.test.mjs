import test from 'node:test';
import assert from 'node:assert/strict';
import { createSelectedNativeSourceApplication, bindSelectedNativeSourceAdapter } from '../selected-native-source.js';
import { createSelectedHiveSourceApplications } from '../selected-hive-source.js';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../../modules/source-app/server/standard-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { authorizeRuntimeAction } from '../../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';

const appId = 'app-' + 'a'.repeat(32), handle = 'H'.repeat(43), requestId = 'synthetic_native_request_001',
  scope = { appId, target: { revision: 2, digest: 'b'.repeat(64) }, sourceProfile: STANDARD_SELECTED_SOURCE_V3,
    resource: { registryId: 'soty', environmentId: 'test', tenantId: 'synthetic', appId, resourceId: 'one',
      selection: { kind: 'soty.resource.v1', nativeId: 'one', incarnationId: 'one' } } };
const shape = properties => ({ type: 'object', additionalProperties: false, required: Object.keys(properties), properties });
function options() { return { scope: structuredClone(scope), catalog: { capabilityId: 'app.' + appId + ':item', version: 1, appId,
  title: 'Synthetic action', description: 'Test only', visibility: 'private', executionEnabled: true, resources: ['one'], recipients: ['one'], effects: ['create'],
  inputSchema: shape({ operation: { type: 'string', maxLength: 20, enum: ['item'] } }), outputSchema: shape({}), },
  projectReceipt: () => ({ effects: [{ kind: 'created', resourceType: 'item', resourceId: 'synthetic-item', revision: 1 }],
    receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'item', id: 'synthetic-item', revision: 1 }] } }) }; }
const input = { handle, input: { operation: 'item' } }, request = () => ({ requestId, input: structuredClone(input), inputDigest: canonicalHash(input) });
const signal = () => new AbortController().signal;
function bind(row, transport) { bindSelectedNativeSourceAdapter(row.adapter, { appId, target: scope.target,
  withOwner: (_request, callback) => callback({ accountId: 'synthetic', deviceId: 'device' }),
  apps: () => ({ callScopedNativeSource: transport }) }); }

test('installed effect capability is exact, default denied and still requires the normal capability allowlist', () => {
  assert.equal(authorizeRuntimeAction('source_write_1', 'source_write_1', ['source_write_1']).allowed, false);
  assert.equal(authorizeRuntimeAction('source_write_1', 'source_write_1', [], ['source_write_1']).allowed, false);
  assert.equal(authorizeRuntimeAction('delete', 'source_write_1', ['source_write_1'], ['source_write_1']).allowed, false);
  assert.equal(authorizeRuntimeAction('source_write_1', 'source_write_1', ['source_write_1'], ['source_write_1']).allowed, true);
  assert.equal(authorizeRuntimeAction('source_receipt_1', 'source_receipt_1', ['source_receipt_1']).allowed, true);
});
test('author-like authority fields, getters, mixed profile/kind and copied adapters cannot bind the selected port', () => {
  let reads = 0; const getter = options(); Object.defineProperty(getter, 'scope', { get() { reads++; throw Error('must not read'); } });
  assert.throws(() => createSelectedNativeSourceApplication(getter)); assert.equal(reads, 0);
  assert.throws(() => createSelectedNativeSourceApplication({ ...options(), owner: true }));
  const nested = options(); Object.defineProperty(nested.scope.target, 'digest', { enumerable: true, get() { reads++; throw Error('must not read'); } });
  assert.throws(() => createSelectedNativeSourceApplication(nested)); assert.equal(reads, 0);
  const mixed = options(); mixed.scope.sourceProfile = HIVE_SELECTED_SOURCE; assert.throws(() => createSelectedNativeSourceApplication(mixed));
  const foreign = options(); foreign.scope.resource.appId = 'app-' + 'c'.repeat(32); assert.throws(() => createSelectedNativeSourceApplication(foreign));
  assert.equal(bindSelectedNativeSourceAdapter({ profile: 'soty.external-adapter.v1', withAuthority: (_r, fn) => fn() },
    { appId, target: scope.target, withOwner() {}, apps() {} }), false);
  assert.throws(() => createSelectedHiveSourceApplications(scope));
});
test('scope is captured, exact target binding happens once and Source receives only Native inner input', async () => {
  const raw = options(), row = createSelectedNativeSourceApplication(raw); raw.scope.target.revision = 99; raw.catalog.resources.push('foreign');
  assert.throws(() => bindSelectedNativeSourceAdapter(row.adapter, { appId, target: { ...scope.target, revision: 99 }, withOwner() {}, apps() {} }));
  const calls = []; bind(row, async value => { calls.push(value); return { status: 200, data: { ok: true, data: {} } }; });
  assert.throws(() => bind(row, () => {})); await row.adapter.execute(request(), signal());
  assert.equal(calls.length, 1); assert.deepEqual(calls[0].scope, scope); assert.equal(calls[0].handle, handle);
  assert.deepEqual(calls[0].input, input.input); assert.equal(Object.hasOwn(calls[0].input, 'handle'), false);
  const wrong = request(); wrong.inputDigest = '0'.repeat(64); await assert.rejects(row.adapter.execute(wrong, signal())); assert.equal(calls.length, 1);
});
test('receipt mismatches/extra private fields and unavailable Source never become a committed proof', async () => {
  for (const mode of ['id', 'digest', 'extra', 'unavailable', 'rejected-projector']) {
    const raw = options(); if (mode === 'rejected-projector') raw.projectReceipt = () => Promise.reject(Error('synthetic'));
    const row = createSelectedNativeSourceApplication(raw); bind(row, async value => {
      assert.equal(value.path, '/api/embed/receipt'); assert.deepEqual(value.input, { inputDigest: canonicalHash(input.input) });
      if (mode === 'unavailable') throw Error('synthetic');
      return { status: 200, data: { ok: true, data: { requestId: mode === 'id' ? 'foreign_request' : requestId,
        inputDigest: mode === 'digest' ? '0'.repeat(64) : canonicalHash(input.input), outcome: 'committed', receipt: {},
        ...(mode === 'extra' ? { privateIdentity: 'synthetic private value' } : {}) } } };
    });
    if (mode === 'unavailable') assert.equal((await row.adapter.readProof(request(), signal())).outcome, 'unknown');
    else await assert.rejects(row.adapter.readProof(request(), signal()));
  }
});
test('HIVE proof uses exact original durable change, ignores a POST replay head and rejects foreign or changed operations', async () => {
  const hiveScope = { ...scope, sourceProfile: HIVE_SELECTED_SOURCE, resource: { ...scope.resource,
    selection: { kind: 'hive.project.v1', nativeId: 'one', incarnationId: 'one' } } };
  for (const mode of ['own', 'foreign', 'changed', 'gap']) {
    const row = createSelectedHiveSourceApplications(hiveScope)[1], data = { handle, input: { operation: 'hive.cell.title', expectedRevision: 7, id: 'cell', title: 'Synthetic title' } };
    bind(row, async value => { assert.equal(value.path, '/api/embed/changes'); assert.deepEqual(value.input, { after: 7 });
      return { status: 200, data: { projectId: 'one', changes: [{ revision: mode === 'gap' ? 9 : 8, requestId: mode === 'foreign' ? 'foreign' : requestId,
        operations: [{ op: 'object.update', id: 'cell', patch: { title: mode === 'changed' ? 'foreign title' : 'Synthetic title' } }], actor: null, createdAt: 'synthetic' }] } };
    });
    const proof = await row.adapter.readProof({ requestId, input: data, inputDigest: canonicalHash(data) }, signal());
    if (mode === 'own') { assert.equal(proof.outcome, 'committed'); assert.equal(proof.receipt.artifacts[0].revision, 8); }
    else assert.equal(proof.outcome, 'unknown');
  }
});

test('HIVE creation is one exact Native object/occurrence batch and its receipt rejects either changed operation', async () => {
  const hiveScope = { ...scope, sourceProfile: HIVE_SELECTED_SOURCE, resource: { ...scope.resource,
    selection: { kind: 'hive.project.v1', nativeId: 'one', incarnationId: 'one' } } };
  for (const changed of [false, true]) {
    const row = createSelectedHiveSourceApplications(hiveScope)[2], data = { handle, input: {
      operation: 'hive.cell.create', expectedRevision: 7, id: 'card_1', title: 'Synthetic card', viewId: 'map', x: 320, y: 0 } };
    let operations;
    bind(row, async value => {
      if (value.path === '/api/embed/operations') { operations = value.input.operations; return { status: 200, data: { revision: 99, replayed: true } }; }
      assert.equal(value.path, '/api/embed/changes'); const stored = structuredClone(operations);
      if (changed) stored[1].viewId = 'foreign';
      return { status: 200, data: { projectId: 'one', changes: [{ revision: 8, requestId, operations: stored, actor: null, createdAt: 'synthetic' }] } };
    });
    const request = { requestId, input: data, inputDigest: canonicalHash(data) };
    await row.adapter.execute(request, signal()); assert.deepEqual(operations.map(item => item.op), ['object.add', 'occurrence.add']);
    const proof = await row.adapter.readProof(request, signal());
    assert.equal(proof.outcome, changed ? 'unknown' : 'committed');
    if (!changed) assert.deepEqual(proof.receipt.artifacts.map(item => [item.type, item.id, item.revision]), [['hive-object', 'card_1', 8], ['hive-project', 'one', 8]]);
  }
});

test('HIVE insertion context never chooses the first of multiple views and preserves the original list wire', async () => {
  const hiveScope = { ...scope, sourceProfile: HIVE_SELECTED_SOURCE, resource: { ...scope.resource,
    selection: { kind: 'hive.project.v1', nativeId: 'one', incarnationId: 'one' } } };
  for (const views of [1, 2]) {
    const rows = createSelectedHiveSourceApplications(hiveScope), project = { id: 'one', schema: 'hive.document', version: 2,
      title: 'Synthetic field', core: { objects: { cell: { id: 'cell', title: 'Old card' } } }, views: {
        map: { id: 'map', placements: { cell: { x: 10, y: -20, width: 280 } } }, ...(views === 2 ? { second: { id: 'second', placements: {} } } : {}) } };
    for (const index of [0, 3]) {
      const row = rows[index]; bind(row, async value => {
        assert.equal(value.path, '/api/embed/project'); return { status: 200, data: { project, revision: 7 } };
      });
      const data = { handle, input: { operation: index === 0 ? 'hive.cells.list' : 'hive.cells.context' } };
      const result = await row.adapter.query({ requestId, input: data, inputDigest: canonicalHash(data) }, signal(), () => true);
      if (index === 0) assert.deepEqual(result, { title: 'Synthetic field', revision: 7, cells: [{ id: 'cell', title: 'Old card' }] });
      else assert.deepEqual(result.insertionViews, views === 1 ? [{ id: 'map', x: 330, y: -20 }] : []);
    }
  }
});


test('only an exact Source 409 unknown-receipt response marks a live unobserved proof', async () => {
  for (const [mode, expected] of [
    ['exact', 'unobserved'], ['legacy', 'unknown'], ['wrong-marker', 'unknown'], ['wrong-status', 'unknown'],
    ['wrong-code', 'unknown'], ['retryable', 'unknown'], ['extra-field', 'unknown'], ['unavailable', 'unknown'],
  ]) {
    const row = createSelectedNativeSourceApplication(options());
    bind(row, async value => {
      assert.equal(value.path, '/api/embed/receipt');
      if (mode === 'unavailable') throw Error('synthetic offline');
      return { status: mode === 'wrong-status' ? 500 : 409, data: { ok: false, error: {
        code: mode === 'wrong-code' ? 'different_error' : 'source_receipt_unknown', retryable: mode === 'retryable',
        ...(mode === 'legacy' ? {} : { receiptValidation: mode === 'wrong-marker' ? 'other' : 'soty.source-receipt-check.v1' }),
        ...(mode === 'extra-field' ? { extra: true } : {}),
      } } };
    });
    const proof = await row.adapter.readProof(request(), signal());
    assert.equal(proof.outcome, expected, mode);
    assert.equal(proof.requestId, requestId);
    assert.equal(proof.inputDigest, canonicalHash(input));
  }
});
