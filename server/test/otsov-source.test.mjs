import test from 'node:test';
import assert from 'node:assert/strict';
import { createSelectedOtsovSourceApplications } from '../otsov-source.js';
import { bindCurrentContextSourceAdapter } from '../current-context-source.js';
import { bindSelectedNativeSourceAdapter } from '../selected-native-source.js';
import { captureExternalApplications } from '../external-applications.js';
import { captureExternalGuidance, createExternalGuidance } from '../../modules/capabilities/server/external-guidance.mjs';
import { createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { workspaceMutationRef, workspaceWitness } from '../../modules/source-app/server/workspace-context.mjs';

const appId = 'app-' + 'a'.repeat(32), handle = 'h'.repeat(43), contextRef = 'c'.repeat(43);
const scope = () => ({ appId, target: { revision: 2, digest: 'b'.repeat(64) }, sourceProfile: structuredClone(STANDARD_ROOT_WORKSPACE_SOURCE),
  resource: { registryId: 'soty', environmentId: 'test', tenantId: 'synthetic-publisher', appId, resourceId: 'otsov',
    selection: { kind: 'soty.workspace.v1', nativeId: 'otsov', incarnationId: 'one' } } });
const signal = () => new AbortController().signal;
const request = input => {
  const captured = { handle, contextRef, input };
  return { requestId: 'synthetic_otsov_request_001', input: captured, inputDigest: canonicalHash(captured) };
};
const createInput = () => ({ operation: 'otsov.project.create', payload: { name: 'Мой сайт',
  files: [{ path: 'index.html', content: '<!doctype html>\n<title>Мой сайт</title><h1>Привет</h1>' }] } });
function bind(row, transport, owner = 'synthetic-person') {
  assert.equal(bindSelectedNativeSourceAdapter(row.adapter, { appId, target: scope().target,
    withOwner: (_request, action) => action({ accountId: owner, deviceId: 'synthetic-device' }),
    apps: () => ({ callScopedNativeSource: value => value.withCurrentOwner(actor => transport(value, actor)) }) }), true);
}
// These tests exercise the actual Root factories, schema validator, binding,
// guidance capture and workspace receipt projection. Native HTTP is an explicit
// synthetic transport; they do not claim deployment, published apps or paid AI.
test('three bounded OTsov rows use exact workspace2 scope and captured Root/Source hashes', () => {
  const raw = scope(), expected = structuredClone(raw), rows = createSelectedOtsovSourceApplications(raw);
  raw.target.revision = 99; raw.resource.resourceId = 'foreign';
  const entries = captureExternalApplications(rows), registry = createCatalog(entries.map(row => row.catalog));
  assert.equal(entries.length, 3); assert.deepEqual(entries.map(row => row.readonly), [true, true, false]);
  assert.deepEqual(entries.map(row => row.catalog.effects), [[], [], ['create']]);
  for (const row of entries) {
    assert.deepEqual(row.target, expected.target);
    assert.equal(Object.isFrozen(rows.find(item => item.catalog.capabilityId === row.contract.capabilityId).target), true);
    assert.deepEqual(row.catalog.resources, [`workspace:${appId}:otsov`]);
    assert.deepEqual(row.catalog.recipients, row.catalog.resources);
    assert.equal(registry.get(row.contract.capabilityId, 1).digest, row.contract.digest);
  }
  for (const row of rows.slice(1)) {
    const wrapped = row.catalog, original = structuredClone(wrapped);
    delete original.executionBinding; original.inputSchema = wrapped.inputSchema.properties.input;
    const sourceBinding = { id: `app.${appId}:selected-native`, version: 2,
      digest: canonicalHash({ schema: 'soty.selected-native-source.v1', scope: expected, catalog: original }) };
    assert.deepEqual(wrapped.executionBinding.binding, sourceBinding);
    assert.deepEqual(Object.keys(wrapped.inputSchema.properties).sort(), ['contextRef', 'handle', 'input']);
    const captured = entries.find(item => item.contract.capabilityId === wrapped.capabilityId);
    assert.equal(captured.catalog.executionBinding.binding.digest,
      canonicalHash({ profile: 'soty.registered-app-source.v1', appId, target: expected.target, sourceBinding }));
  }
});

test('foreign scope, legacy pin, getters, authority DTOs and unsupported schema constructs are denied', () => {
  for (const alter of [
    raw => { raw.sourceProfile = STANDARD_WORKSPACE_SOURCE; },
    raw => { raw.sourceProfile.digest = '0'.repeat(64); },
    raw => { raw.resource.appId = 'app-' + 'f'.repeat(32); },
    raw => { raw.resource.selection.kind = 'soty.resource.v1'; },
    raw => { raw.ownerKey = 'synthetic-forbidden'; },
  ]) { const raw = scope(); alter(raw); assert.throws(() => createSelectedOtsovSourceApplications(raw)); }
  let reads = 0; const getter = scope();
  Object.defineProperty(getter, 'sourceProfile', { enumerable: true, get() { reads++; throw Error('must not read'); } });
  assert.throws(() => createSelectedOtsovSourceApplications(getter)); assert.equal(reads, 0);
  const rows = createSelectedOtsovSourceApplications(scope()), invalid = structuredClone(rows[1].catalog);
  invalid.inputSchema.properties.input.oneOf = [{ type: 'object' }];
  assert.throws(() => createCatalog([invalid]), error => error.code === 'schema_unsupported');
});

test('builder skill is static bounded authorized guidance on discovery/create only', () => {
  const rows = captureExternalApplications(createSelectedOtsovSourceApplications(scope()));
  assert.equal(rows[0].guidance.length, 0); assert.deepEqual(rows.slice(1).map(row => row.guidance.length), [1, 1]);
  const guides = captureExternalGuidance(rows.flatMap(row => row.guidance));
  assert.equal(guides.length, 2);
  assert.ok(guides.reduce((size, item) => size + Buffer.byteLength(item.content), 0) <= 16384);
  let active = true;
  const service = createExternalGuidance({ entries: guides, contracts: rows.map(row => row.contract),
    getContract({ actor, reference }) {
      assert.equal(active, true); assert.equal(actor, 'synthetic-person');
      const row = rows.find(item => item.contract.capabilityId === reference.capabilityId);
      assert.equal(row?.contract.digest, reference.digest); return { reference: row.contract, appId };
    } });
  const row = rows[2], index = service.list({ actor: 'synthetic-person', reference: row.contract });
  const item = service.get({ actor: 'synthetic-person', reference: row.contract, guidance: index.items[0].reference });
  assert.match(item.content, /Создание приложения через OTsov/u);
  assert.match(item.content, /Не раздавай общий owner key/u);
  assert.match(item.content, /собственными MCP/u);
  assert.match(item.content, /не выдавай за опубликованный сайт/u);
  assert.match(item.content, /Открытый HIVE не изменяется/u);
  assert.equal(item.authority, 'application-data');
  assert.throws(() => service.get({ actor: 'synthetic-foreign', reference: row.contract, guidance: index.items[0].reference }));
  active = false; assert.throws(() => service.list({ actor: 'synthetic-person', reference: row.contract }));
});

test('current context uses the original factory and a fresh workspace lease', async () => {
  const row = createSelectedOtsovSourceApplications(scope())[0], calls = [];
  assert.equal(bindCurrentContextSourceAdapter(row.adapter, { appId, target: scope().target,
    withOwner: (_request, action) => action({ accountId: 'synthetic-person' }),
    apps: () => ({ async callScopedContextSource(value) { calls.push(value);
      return { status: 200, data: { ok: true, data: { schema: 'soty.current-context.v2', selection: scope().resource.selection,
        title: 'Мои приложения', role: 'owner', revision: 1, context: { ref: contextRef, generation: 1, expiresAt: Date.now() + 120000 } } } };
    } }) }), true);
  const result = await row.adapter.query({ requestId: 'synthetic_otsov_context_001', input: { handle } }, signal(), () => true);
  assert.equal(result.title, 'Мои приложения'); assert.equal(result.context.ref, contextRef);
  assert.equal(calls.length, 1); assert.equal(calls[0].handle, handle); assert.deepEqual(calls[0].scope, scope());
});

test('compact list forwards only the inner native schema and preserves actor, handle and context fences', async () => {
  const row = createSelectedOtsovSourceApplications(scope())[1], calls = [], legacyName = 'Название прежнего проекта ' + 'Ж'.repeat(200);
  bind(row, async (value, actor) => { calls.push(value); assert.equal(actor.accountId, 'synthetic-person');
    return { status: 200, data: { ok: true, data: { requestId: value.requestId, operation: 'otsov.projects.list',
      projects: [{ projectId: 'synthetic-project', name: legacyName, revision: 1, createdAt: '2026-10-13T00:00:00Z' }], nextOffset: -1 } } };
  });
  const input = { operation: 'otsov.projects.list', representation: 'compact', offset: 0, limit: 64 };
  const result = await row.adapter.query(request(input), signal(), () => true);
  assert.equal(result.nextOffset, -1); assert.equal(result.projects[0].name, legacyName);
  assert.equal(calls[0].path, '/api/embed/query'); assert.deepEqual(calls[0].input, input);
  assert.equal(calls[0].handle, handle); assert.equal(calls[0].contextRef, contextRef);
  assert.equal(Object.hasOwn(calls[0].input, 'accountId'), false);
  await assert.rejects(row.adapter.query(request(input), signal(), () => false)); assert.equal(calls.length, 1);
  assert.throws(() => bindSelectedNativeSourceAdapter(row.adapter, { appId, target: scope().target, withOwner() {}, apps() {} }));
});

test('unsupported actions, authority fields, map files and oversized arrays stop before transport', async () => {
  const rows = createSelectedOtsovSourceApplications(scope()), calls = [];
  for (const row of rows.slice(1)) bind(row, async value => { calls.push(value); return { status: 200, data: { ok: true, data: {} } }; });
  for (const input of [
    { operation: 'otsov.project.export', representation: 'compact' },
    { operation: 'otsov.projects.list', representation: 'compact', accountId: 'foreign' },
    { operation: 'otsov.projects.list', representation: 'compact', limit: 65 },
    { operation: 'soty.context.select', nativeId: 'foreign', generation: 2 },
  ]) await assert.rejects(rows[1].adapter.query(request(input), signal(), () => true));
  for (const input of [
    { ...createInput(), projectId: 'foreign' },
    { operation: 'otsov.project.create', payload: { ...createInput().payload, name: 'А'.repeat(121) } },
    { operation: 'otsov.project.create', payload: { name: 'Denied', files: { 'index.html': 'old-map' } } },
    { operation: 'otsov.project.create', payload: { ...createInput().payload, requestId: 'foreign' } },
    { operation: 'otsov.project.create', payload: { ...createInput().payload, files: Array.from({ length: 17 }, (_, index) => ({ path: index + '.html', content: '' })) } },
  ]) await assert.rejects(rows[2].adapter.execute(request(input), signal()));
  const changed = request(createInput()); changed.inputDigest = '0'.repeat(64);
  await assert.rejects(rows[2].adapter.execute(changed, signal())); assert.equal(calls.length, 0);
});

test('create uses the original inner digest and durable workspace receipt with exact witness', async () => {
  const row = createSelectedOtsovSourceApplications(scope())[2], input = createInput(), req = request(input), calls = [];
  const after = { schema: 'soty.context-after.v1', mutationRef: workspaceMutationRef(req.requestId, canonicalHash(input), contextRef),
    generation: 1, title: 'Мои приложения', role: 'owner', revision: 1 };
  const artifact = { type: 'otsov-project', id: 'synthetic-created', revision: 1 };
  bind(row, async value => { calls.push(value);
    if (value.path === '/api/embed/invoke') { assert.deepEqual(value.input, input); return { status: 200, data: { ok: true, data: {} } }; }
    assert.equal(value.path, '/api/embed/receipt'); assert.deepEqual(value.input, { inputDigest: canonicalHash(input) });
    return { status: 200, data: { ok: true, data: { requestId: req.requestId, inputDigest: canonicalHash(input),
      outcome: 'committed', replayed: true, contextAfter: after, workspaceReceipt: { schema: 'soty.workspace-receipt.v1',
        effects: [{ kind: 'created', resourceType: artifact.type, resourceId: artifact.id, revision: 1 }],
        receipt: { verificationMethod: 'domain_read', artifacts: [artifact] } } } } };
  });
  await row.adapter.execute(req, signal()); const proof = await row.adapter.readProof(req, signal());
  assert.equal(proof.outcome, 'committed'); assert.equal(proof.inputDigest, req.inputDigest);
  assert.equal(proof.receipt.artifacts[0].sha256, workspaceWitness({ after, artifact, requestId: req.requestId,
    inputDigest: canonicalHash(input), scope: scope() }));
  assert.equal(calls.length, 2);
});

test('lost native receipt and mismatched original digest never claim completion', async () => {
  for (const mode of ['unavailable', 'digest']) {
    const row = createSelectedOtsovSourceApplications(scope())[2], req = request(createInput());
    bind(row, async () => {
      if (mode === 'unavailable') throw Error('synthetic-unavailable');
      return { status: 200, data: { ok: true, data: { requestId: req.requestId, inputDigest: '0'.repeat(64), outcome: 'committed' } } };
    });
    if (mode === 'unavailable') assert.equal((await row.adapter.readProof(req, signal())).outcome, 'unknown');
    else await assert.rejects(row.adapter.readProof(req, signal()));
  }
});
