import test from 'node:test';
import assert from 'node:assert/strict';
import { createSelectedHiveSourceApplications } from '../selected-hive-source.js';
import { bindSelectedNativeSourceAdapter } from '../selected-native-source.js';
import { selectedHiveWorkflowOperations } from '../hive-workflow-input.js';
import { HIVE_SELECTED_SOURCE } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';

const appId = 'app-' + 'a'.repeat(32), handle = 'h'.repeat(43), target = { revision: 1, digest: 'b'.repeat(64) };
const scope = { appId, target, sourceProfile: HIVE_SELECTED_SOURCE,
  resource: { registryId: 'soty', environmentId: 'test', tenantId: 'fixture', appId, resourceId: 'selected',
    selection: { kind: 'hive.project.v1', nativeId: 'own-field', incarnationId: 'one' } } };
const workflow = () => ({ operation: 'hive.workflow.create', expectedRevision: 7, viewId: 'map',
  cards: [{ id: 'start', title: 'Начало', x: 0, y: 0 }, { id: 'finish', title: 'Результат', x: 360, y: 0 }],
  links: [{ id: 'flow', sourceId: 'start', targetId: 'finish' }] });
const signal = () => new AbortController().signal;
const request = inner => { const input = { handle, input: inner };
  return { requestId: 'synthetic_workflow_original_001', input, inputDigest: canonicalHash(input) }; };
const bind = (row, call) => bindSelectedNativeSourceAdapter(row.adapter, { appId, target,
  withOwner: (_request, action) => action({ accountId: 'synthetic-owner' }), apps: () => ({ callScopedNativeSource: call }) });

test('workflow is an explicit optional legacy capability; default contracts stay identical', () => {
  const normal = createSelectedHiveSourceApplications(scope), disabled = createSelectedHiveSourceApplications(scope, { workflow: false });
  const opted = createSelectedHiveSourceApplications(scope, { workflow: true });
  assert.equal(normal.length, 4); assert.deepEqual(disabled.map(x => x.catalog), normal.map(x => x.catalog));
  assert.deepEqual(opted.slice(0, 4).map(x => x.catalog), normal.map(x => x.catalog));
  assert.equal(opted.length, 5); assert.equal(opted[4].catalog.effects.join(','), 'create,update');
  for (const options of [null, [], { workflow: 1 }, { workflow: true, grant: true }]) assert.throws(() => createSelectedHiveSourceApplications(scope, options));
  let reads = 0; const getter = {}; Object.defineProperty(getter, 'workflow', { enumerable: true, get() { reads++; return true; } });
  assert.throws(() => createSelectedHiveSourceApplications(scope, getter)); assert.equal(reads, 0);
});

test('invalid workflow JSON cannot dispatch even one Native operation', async () => {
  const row = createSelectedHiveSourceApplications(scope, { workflow: true })[4]; let calls = 0;
  bind(row, async () => { calls++; throw Error('invalid input reached transport'); });
  const invalid = [
    value => { value.cards[1].id = value.cards[0].id; },
    value => { value.links[0].id = value.cards[0].id; },
    value => { value.links[0].targetId = 'foreign-existing-card'; },
    value => { value.cards[0].id = '../foreign'; },
    value => { value.cards[0].title = '\ud800'; },
    value => { value.cards[0].x = Infinity; },
    value => { value.cards[0].ownerAccountId = 'foreign'; },
    value => { value.contextRef = 'foreign'; },
    value => { value.operations = [{ op: 'project.replace' }]; },
    value => { value.cards = Array.from({ length: 13 }, (_, i) => ({ ...value.cards[0], id: 'card-' + i })); },
    value => { value.links = []; },
  ];
  for (const change of invalid) { const value = workflow(); change(value); await assert.rejects(async () => row.adapter.execute(request(value), signal())); }
  assert.equal(calls, 0);
});

test('one Native batch and exact independently read receipt cover every created card/link', async () => {
  for (const changed of [false, true]) {
    const row = createSelectedHiveSourceApplications(scope, { workflow: true })[4], value = workflow(), req = request(value);
    const expected = selectedHiveWorkflowOperations(value); let invoked = 0;
    bind(row, async call => {
      if (call.path === '/api/embed/operations') { invoked++; assert.deepEqual(call.input, { expectedRevision: 7, operations: expected });
        return { status: 200, data: { revision: 99, replayed: true } }; }
      const operations = structuredClone(expected); if (changed) operations.at(-1).relation.endpoints[1].target.id = 'foreign';
      return { status: 200, data: { projectId: 'own-field', changes: [{ requestId: req.requestId, revision: 8,
        operations, actor: null, createdAt: 'fixture' }] } };
    });
    await row.adapter.execute(req, signal()); const proof = await row.adapter.readProof(req, signal()); assert.equal(invoked, 1);
    assert.equal(proof.outcome, changed ? 'unknown' : 'committed');
    if (!changed) assert.deepEqual(proof.effects.map(x => [x.kind, x.resourceType, x.resourceId, x.revision]), [
      ['created', 'hive-object', 'start', 8], ['created', 'hive-object', 'finish', 8],
      ['created', 'hive-relation', 'flow', 8], ['updated', 'hive-project', 'own-field', 8] ]);
  }
});
