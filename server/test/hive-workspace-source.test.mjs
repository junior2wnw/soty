import test from 'node:test';
import assert from 'node:assert/strict';
import { createSelectedNativeSourceApplication, bindSelectedNativeSourceAdapter } from '../selected-native-source.js';
import { createHiveWorkspaceSourceApplications, hiveWorkspaceReadOutput } from '../hive-workspace-source.js';
import { STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_SELECTED_SOURCE_V3, STANDARD_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { selectedHiveWorkflowOperations, validateHiveWorkflowNativeInput } from '../hive-workflow-input.js';

const appId = 'app-' + 'a'.repeat(32), scope = { appId, target: { revision: 1, digest: 'b'.repeat(64) },
  sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE, resource: { registryId: 'soty', environmentId: 'test', tenantId: 'synthetic',
    appId, resourceId: 'own-workspace', selection: { kind: 'soty.workspace.v1', nativeId: 'namespace', incarnationId: 'one' } } },
  resource = `workspace:${appId}:own-workspace`, handle = 'h'.repeat(43), contextRef = 'c'.repeat(43),
  input = { handle, contextRef, input: { operation: 'hive.project.read' } },
  request = { requestId: 'synthetic-hive-bounded-read-001', input, inputDigest: canonicalHash(input) },
  object = properties => ({ type: 'object', additionalProperties: false, properties, required: Object.keys(properties) }),
  signal = () => new AbortController().signal;
function project() { return { requestId: request.requestId, revision: 3, account: { id: 'private-account', displayName: 'Private person' },
  project: { schema: 'hive.document', version: 2, id: 'field-one', title: 'Field', core: { objects: {
    existing: { id: 'existing', title: 'Existing title', content: [{ body: 'Private card body' }] },
  } }, assets: { private: { uri: 'https://private.fixture.invalid' } }, views: { actual: { id: 'actual', title: 'Actual view', placements: {
    existing: { id: 'existing', x: 10, y: -20, width: 280 },
  } } } } }; }
function options() { return { scope, catalog: { capabilityId: `app.${appId}:synthetic.read`, version: 1, appId,
  title: 'Synthetic bounded read', description: 'Test only', visibility: 'private', executionEnabled: true,
  resources: [resource], recipients: [resource], effects: [], inputSchema: object({ operation: { type: 'string', maxLength: 40, enum: ['hive.project.read'] } }),
  outputSchema: object({ revision: { type: 'integer', minimum: 1 } }) } }; }
function bind(row, transport, withOwner = (_request, action) => action({ accountId: 'synthetic-owner' })) {
  bindSelectedNativeSourceAdapter(row.adapter, { appId, target: scope.target, withOwner, apps: () => ({ callScopedNativeSource: transport }) });
}

test('workspace2 descriptor is additive, exact-profile only and exposes no replacement/deletion/selection operation', () => {
  const rows = createHiveWorkspaceSourceApplications(scope); assert.equal(rows.length, 2);
  for (const row of rows) { assert.deepEqual(row.catalog.resources, [resource]); assert.deepEqual(row.catalog.recipients, [resource]);
    assert.equal(row.catalog.executionBinding.binding.version, 2); }
  assert.deepEqual(rows[0].catalog.inputSchema.properties.input.properties.operation.enum, ['hive.project.read']);
  assert.deepEqual(rows[1].catalog.inputSchema.properties.input.properties.operations.items.properties.op.enum,
    ['object.add', 'occurrence.add', 'relation.add']); assert.deepEqual(rows[1].catalog.effects, ['update']);
  for (const profile of [STANDARD_WORKSPACE_SOURCE, STANDARD_SELECTED_SOURCE_V3])
    assert.throws(() => createHiveWorkspaceSourceApplications({ ...scope, sourceProfile: profile }));
  assert.throws(() => createHiveWorkspaceSourceApplications({ ...scope, resource: { ...scope.resource,
    selection: { ...scope.resource.selection, kind: 'hive.project.v1' } } }));
  const plain = createSelectedNativeSourceApplication(options()), absent = createSelectedNativeSourceApplication({ ...options(), readOutput: undefined });
  assert.deepEqual(plain.catalog, absent.catalog, 'Default contract bytes/digest unchanged');
});

test('HIVE projection returns real views/revision, preserves privacy and never picks a view from several', () => {
  const stored = project(), result = hiveWorkspaceReadOutput(stored, input.input);
  assert.deepEqual(result, { projectId: 'field-one', title: 'Field', revision: 3, cells: [{ id: 'existing', title: 'Existing title' }],
    views: [{ id: 'actual', title: 'Actual view' }], insertionViews: [{ id: 'actual', x: 330, y: -20 }] });
  assert.equal(JSON.stringify(result).includes('Private'), false);
  stored.project.views.second = { id: 'second', title: 'Another view', placements: {} };
  assert.equal(hiveWorkspaceReadOutput(stored, input.input).views.length, 2);
  assert.deepEqual(hiveWorkspaceReadOutput(stored, input.input).insertionViews, []);
  stored.project.views = {}; assert.deepEqual(hiveWorkspaceReadOutput(stored, input.input).insertionViews, []);
});

test('HIVE projection fails explicitly for invalid revision, ID maps, bounds, placements or Native shape', () => {
  const changes = [v => { v.revision = 0; }, v => { v.project.version = 1; }, v => { delete v.project.views; },
    v => { v.project.core.objects.existing.id = 'mismatch'; }, v => { v.project.views.actual.id = 'mismatch'; },
    v => { v.project.title = 'x'.repeat(241); }, v => { v.project.views.actual.placements.existing.x = Infinity; },
    v => { v.project.core.objects = Object.fromEntries(Array.from({ length: 101 }, (_, n) => ['cell' + n, { id: 'cell' + n, title: '' }])); },
    v => { v.project.views = Object.fromEntries(Array.from({ length: 17 }, (_, n) => ['view' + n, { id: 'view' + n, title: '', placements: {} }])); }];
  for (const change of changes) { const stored = project(); change(stored); assert.throws(() => hiveWorkspaceReadOutput(stored, input.input)); }
});

test('trusted readOutput is synchronous CODE only, captured and forbidden on effect/legacy rows', async () => {
  for (const readOutput of [null, {}, async () => ({}), function* () {}, new Proxy(() => ({}), {})])
    assert.throws(() => createSelectedNativeSourceApplication({ ...options(), readOutput }));
  assert.throws(() => createSelectedNativeSourceApplication({ ...options(), catalog: { ...options().catalog, effects: ['update'] }, readOutput: () => ({}) }));
  assert.throws(() => createSelectedNativeSourceApplication({ ...options(), scope: { ...scope, sourceProfile: HIVE_SELECTED_SOURCE,
    resource: { ...scope.resource, selection: { kind: 'hive.project.v1', nativeId: 'field-one', incarnationId: 'one' } } }, readOutput: () => ({}) }));
  let getterCalls = 0; const raw = options(); Object.defineProperty(raw, 'readOutput', { get() { getterCalls++; throw Error('never'); } });
  assert.throws(() => createSelectedNativeSourceApplication(raw)); assert.equal(getterCalls, 0);
  const raw2 = { ...options(), readOutput: stored => ({ revision: stored.revision }) }, row = createSelectedNativeSourceApplication(raw2);
  raw2.readOutput = () => { throw Error('mutated config'); };
  bind(row, async () => ({ status: 200, data: { ok: true, data: { revision: 3, privateBody: 'not exposed' } } }));
  assert.deepEqual(await row.adapter.query(request, signal(), () => true), { revision: 3 });
});

test('oversized/invalid raw reads fail before projection; errors and invalid projected results are not hidden', async () => {
  let projected = 0, getterCalls = 0;
  for (const mode of ['oversized', 'getter', 'throw', 'promise', 'bad-output']) {
    const row = createSelectedNativeSourceApplication({ ...options(), readOutput(stored) {
      projected++; if (mode === 'throw') throw Error('synthetic_projection_failure');
      if (mode === 'promise') return Promise.reject(Error('synthetic_async_failure'));
      return mode === 'bad-output' ? { revision: 'wrong' } : { revision: stored.revision };
    } });
    bind(row, async () => { const data = { revision: 3 };
      if (mode === 'oversized') data.body = 'x'.repeat(65536);
      if (mode === 'getter') Object.defineProperty(data, 'body', { enumerable: true, get() { getterCalls++; throw Error('never'); } });
      return { status: 200, data: { ok: true, data } };
    });
    const before = projected;
    await assert.rejects(row.adapter.query(request, signal(), () => true), mode === 'throw' ? /synthetic_projection_failure/u : undefined);
    if (['oversized', 'getter'].includes(mode)) assert.equal(projected, before);
  }
  assert.equal(getterCalls, 0); assert.equal(projected, 3);
});

test('Root A/B/revoke gates precede transport; changed authority after Native read precedes projection', async () => {
  for (const mode of ['foreign', 'revoked', 'changed']) {
    let dispatched = 0, projected = 0, live = true;
    const row = createSelectedNativeSourceApplication({ ...options(), readOutput(stored) { projected++; return { revision: stored.revision }; } });
    bind(row, async args => { args.withCurrentOwner(() => true); dispatched++; if (mode === 'changed') live = false;
      return { status: 200, data: { ok: true, data: { revision: 3 } } }; });
    if (mode !== 'changed') live = false;
    await assert.rejects(row.adapter.query(request, signal(), () => live));
    assert.equal(dispatched, mode === 'changed' ? 1 : 0); assert.equal(projected, 0);
  }
});

test('workspace Native validator is synchronous private CODE, after schema/owner checks, and never rewrites input', async () => {
  const effectOptions = () => ({ ...options(), catalog: { ...options().catalog, effects: ['update'] } });
  for (const validateNativeInput of [null, {}, async () => true, function* () {}, new Proxy(() => true, {})])
    assert.throws(() => createSelectedNativeSourceApplication({ ...effectOptions(), validateNativeInput }));
  assert.throws(() => createSelectedNativeSourceApplication({ ...options(), validateNativeInput: () => true }));
  for (const mode of ['throw', 'promise', 'false', 'undefined', 'object', 'schema', 'owner']) {
    let calls = 0, validations = 0;
    const row = createSelectedNativeSourceApplication({ ...effectOptions(), validateNativeInput() {
      validations++; if (mode === 'throw') throw Error('synthetic_input_validation_failure');
      if (mode === 'promise') return Promise.reject(Error('synthetic_async_validation_failure'));
      return mode === 'false' ? false : mode === 'undefined' ? undefined : {};
    } });
    bind(row, async () => { calls++; return { status: 200, data: { ok: true, data: {} } }; },
      (_request, action) => { if (mode === 'owner') throw Error('synthetic_owner_denied'); return action({}); });
    const invalid = structuredClone(request); if (mode === 'schema') invalid.input.input.operation = 'soty.context.select';
    await assert.rejects(row.adapter.execute(invalid, signal())); assert.equal(calls, 0);
    if (['schema', 'owner'].includes(mode)) assert.equal(validations, 0);
  }
  let sent;
  const row = createSelectedNativeSourceApplication({ ...effectOptions(), validateNativeInput(value) {
    assert.equal(Object.isFrozen(value), true); assert.deepEqual(value, input.input); return true;
  } });
  bind(row, async args => { sent = args.input; return { status: 200, data: { ok: true, data: {} } }; });
  await row.adapter.execute(request, signal()); assert.deepEqual(sent, input.input);
});

test('generic Native representation reuses the existing bounded workflow validator; ambiguous fields and refs dispatch zero calls', async () => {
  const workflow = { operation: 'hive.workflow.create', expectedRevision: 3, viewId: 'actual', cards: [
    { id: 'start', title: 'Start', x: 0, y: 0 }, { id: 'end', title: 'End', x: 360, y: 0 },
  ], links: [{ id: 'flow', sourceId: 'start', targetId: 'end' }] },
    native = { operation: 'hive.operations', expectedRevision: workflow.expectedRevision, operations: selectedHiveWorkflowOperations(workflow) };
  assert.equal(validateHiveWorkflowNativeInput(native), true);
  const invalid = [value => { value.operations[0].occurrence = value.operations[1].occurrence; },
    value => { value.operations[1].viewId = 'another-view'; }, value => { value.operations[1].occurrence.target.id = 'not-created'; },
    value => { value.operations[1].occurrence.width = 281; }, value => { value.operations[0].object.title = '   '; },
    value => { value.operations[4].relation.endpoints[1].target.id = 'not-created'; },
    value => { value.operations[4].relation.endpoints.reverse(); }, value => { value.operations[4].relation.id = 'start'; },
    value => { value.operations.reverse(); }, value => { value.operations.push(value.operations[0]); }];
  for (const change of invalid) {
    const value = structuredClone(native); change(value); assert.throws(() => validateHiveWorkflowNativeInput(value));
    const row = createHiveWorkspaceSourceApplications(scope)[1]; let dispatches = 0;
    bind(row, async () => { dispatches++; return { status: 200, data: { ok: true, data: {} } }; });
    const input = { handle, contextRef, input: value }, request = { requestId: 'invalid-native-workflow-001', input, inputDigest: canonicalHash(input) };
    await assert.rejects(row.adapter.execute(request, signal())); await assert.rejects(row.adapter.readProof(request, signal())); assert.equal(dispatches, 0);
  }
});
