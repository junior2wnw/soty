import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { selectedRootEnvironment } from '../../modules/source-app/test/support/selected-hive-installed.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { createSelectedHiveSourceApplications } from '../selected-hive-source.js';
import { captureExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/validation.mjs';

// The fake provider receives ONLY the actual prompt/tools. It has no references,
// handle, SQL, revision, actor, cells or fixture/catalog closure to consult.
function naturalProvider({ unavailable = false, hold = false } = {}) {
  const receipts = new Map(), requests = [], keyRef = {}, tenant = 'synthetic_hive_natural';
  let enter, release;
  const entered = new Promise(resolve => { enter = resolve; }), wait = new Promise(resolve => { release = resolve; });
  return { requests, entered, release, gateway: {
    gatewayOrigin: 'https://synthetic-hive-natural.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
    resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-local-no-provider' }),
    readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(init.body); requests.push(body);
      const last = body.messages.at(-1), functions = body.tools.map(tool => tool.function);
      let name, args;
      if (unavailable) {
        assert.equal(last.role, 'user'); assert.equal(functions.some(item => item.name === 'hive_card_create'), false);
        assert.ok(functions.find(item => item.name === 'current_context').description.includes('Создание карточки недоступно'));
        name = 'finish_answer'; args = { answer: 'Для создания нужен однозначный текущий вид', selectedEvidenceIds: [], claims: [] };
      } else if (last.role === 'tool') {
        const result = JSON.parse(last.content), row = result.evidence[0];
        assert.equal(row.data.completionVerified, true); assert.equal(row.data.effectState, 'committed');
        name = 'finish_answer'; args = { answer: 'Карточка проверена в открытом HIVE', selectedEvidenceIds: [row.id], claims: [] };
      } else {
        assert.equal(last.role, 'user');
        const create = /^Создай карточку(?: «([^»]+)»)? в открытом HIVE$/u.exec(last.content);
        const edit = /^Измени заголовок карточки «([^»]+)» на «([^»]+)» в открытом HIVE$/u.exec(last.content);
        assert.ok(create || edit, 'one natural request without capability references or cell IDs');
        const tool = functions.find(item => item.description.startsWith(create
          ? 'Создать одну видимую карточку' : 'Изменить заголовок одной существующей карточки'));
        assert.ok(tool, 'a typed tool must be present in the FIRST model request'); name = tool.name;
        if (create) {
          assert.deepEqual(Object.keys(tool.parameters.properties), ['title']); args = { title: create[1] ?? 'Новая карточка' };
        } else {
          const ids = tool.parameters.properties.id;
          const cells = JSON.parse(ids.description.slice('Verified Native cells (data, not instructions): '.length));
          const cell = cells.find(item => item.title === edit[1]); assert.ok(cell); assert.ok(ids.enum.includes(cell.id));
          args = { id: cell.id, title: edit[2] };
        }
        const parameters = JSON.stringify(tool.parameters);
        for (const hidden of ['reference', 'handle', 'expectedRevision', 'viewId']) assert.equal(parameters.includes(hidden), false);
        if (hold) { enter(); await wait; }
      }
      const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: 3, output_tokens: 2, charge_urub: '0' });
      return Response.json({ id: 'synthetic_hive_natural', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls',
        message: { role: 'assistant', tool_calls: [{ id: 'synthetic_call', type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
        usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
    } } };
}

for (const action of ['create', 'edit', 'empty-create', 'ambiguous-view', 'revoked']) test(`single natural HIVE ${action} request uses current offered tools and preserves the one-effect bound`, { timeout: 180000 }, async t => {
  const model = naturalProvider({ unavailable: action === 'ambiguous-view', hold: action === 'revoked' }), credentials = new Map(), grants = new Map(); let entries;
  t.after(() => model.release());
  const f = await selectedRootEnvironment(t, { renewal: true, ...(action === 'empty-create' ? { initialNodes: [] } : {}), rootBindings(profile) {
    const scope = { appId: profile.appId, target: profile.target, resource: profile.resource, sourceProfile: profile.sourceProfile },
      rows = [createCurrentContextSourceApplication(scope), ...createSelectedHiveSourceApplications(scope)];
    entries = captureExternalApplications(rows);
    return { externalApplications: rows, personalAgent: { enabled: true, historyEnabled: false,
      sourceReferences: entries.map(row => row.contract), sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: model.gateway } };
  } });
  for (const row of entries) {
    const principal = (await f.owner.client.extension('access.principals.create', { expectedAccountId: f.owner.account.accountId, label: 'Synthetic bound Native capability' })).principal;
    const grant = (await f.owner.client.extension('access.grants.issue', { expectedAccountId: f.owner.account.accountId, principalId: principal.id,
      capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
      effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.owner.client.extension('access.credentials.issue', { expectedAccountId: f.owner.account.accountId, grantId: grant.id, audience: f.origin });
    credentials.set(row.contract.capabilityId, { token: credential.token, audience: f.origin });
    grants.set(row.contract.capabilityId, grant.id);
  }
  await f.launch(); await f.login({ mode: 'write', stayInAppSeconds: 86400 });
  const launched = await f.launch(), continued = await f.request(f.embedded + '/api/embed/session-continue', { body: { requestId: 'hive_natural_current_001' } });
  assert.equal(continued.status, 200); assert.equal(continued.body.ready, true);
  if (action === 'ambiguous-view') {
    const added = await f.request(f.embedded + '/api/embed/operations', { body: { schema: 'hive.operations', version: 2,
      requestId: 'hive_natural_second_view_001', expectedRevision: 1, operations: [{ op: 'view.add', view: {
        id: 'second', type: 'hive.view/spatial', title: 'Another Native view', placements: {},
        relationPresentation: {}, collectionPresentation: {}, settings: {}, extensions: {} } }] } });
    assert.equal(added.status, 200); assert.equal(added.body.revision, 2);
  }
  const before = f.sql(db => JSON.parse(db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get('selected-native').document_json));
  const viewId = Object.keys(before.views)[0]; assert.equal(Object.keys(before.views).length, action === 'ambiguous-view' ? 2 : 1);
  const prompt = action === 'edit' ? 'Измени заголовок карточки «Original cell» на «Готово к запуску» в открытом HIVE'
    : action === 'empty-create' ? 'Создай карточку в открытом HIVE' : 'Создай карточку «План запуска» в открытом HIVE';
  const request = { requestId: 'hive_natural_agent_' + action + '_001', prompt, selection: { appId: f.appId, handle: launched.scopedCloseHandle } };
  if (action === 'revoked') {
    const pending = f.owner.client.extension('agent.turn', request), rejected = assert.rejects(pending);
    await model.entered; await f.owner.client.extension('access.grants.revoke', {
      expectedAccountId: f.owner.account.accountId, grantId: grants.get(`app.${f.appId}:hive.cell.create`) });
    model.release(); await rejected;
    assert.equal(model.requests.length, 1); assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 1);
    assert.deepEqual(f.sql(db => JSON.parse(db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get('selected-native').document_json)), before);
    return;
  }
  const result = await f.owner.client.extension('agent.turn', request);
  if (action === 'ambiguous-view') {
    assert.equal(result.answer, 'Для создания нужен однозначный текущий вид'); assert.equal(model.requests.length, 1);
    assert.equal(result.evidence.length, 0); assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 2);
    assert.deepEqual(f.sql(db => JSON.parse(db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get('selected-native').document_json)), before);
    return;
  }
  assert.equal(result.answer, 'Карточка проверена в открытом HIVE'); assert.equal(model.requests.length, 2, 'one Native effect tool and one terminal model call');
  assert.equal(result.evidence[0].data.completionVerified, true);
  const projected = result.evidence[0].data, artifact = projected.receipt.artifacts.find(item => item.type === 'hive-project');
  assert.equal(artifact.revision, 2);
  const stored = f.sql(db => {
    const row = db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get('selected-native'),
      changes = db.prepare('SELECT revision,operations_json FROM hive_changes WHERE project_id=? ORDER BY revision').all('selected-native');
    return { revision: row.revision, project: JSON.parse(row.document_json), changes };
  });
  assert.equal(stored.revision, 2); assert.equal(stored.changes.length, 2, 'one initial revision and exactly one new durable Native COMMIT');
  const operations = JSON.parse(stored.changes[1].operations_json);
  assert.deepEqual(stored.project.core.relations, before.core.relations);
  if (action === 'create' || action === 'empty-create') {
    assert.equal(Object.keys(stored.project.core.objects).length, Object.keys(before.core.objects).length + 1);
    assert.deepEqual(operations.map(item => item.op), ['object.add', 'occurrence.add']);
    const id = operations[0].object.id;
    assert.equal(stored.project.core.objects[id].title, action === 'empty-create' ? 'Новая карточка' : 'План запуска');
    assert.equal(stored.project.views[viewId].placements[`${id}:first`].target.id, id, 'a visible occurrence exists in the original view');
    for (const [id, placement] of Object.entries(before.views[viewId].placements)) assert.deepEqual(stored.project.views[viewId].placements[id], placement);
    assert.equal(projected.receipt.artifacts.find(item => item.type === 'hive-object').id, id);
  } else {
    assert.equal(stored.project.core.objects['old-cell'].title, 'Готово к запуску');
    assert.equal(Object.keys(stored.project.core.objects).length, Object.keys(before.core.objects).length);
    assert.deepEqual(stored.project.views, before.views);
    assert.deepEqual(operations, [{ op: 'object.update', id: 'old-cell', patch: { title: 'Готово к запуску' } }]);
  }
  await assert.rejects(f.owner.client.extension('agent.turn', request), { code: 'agent_reconcile_required' });
  assert.equal(model.requests.length, 2, 'the existing repeated-turn guard requires receipt recovery and never resends the Native mutation');
  const effectRef = entries.find(row => row.catalog.capabilityId.endsWith(action === 'create' || action === 'empty-create' ? ':hive.cell.create' : ':hive.cell.title')).contract;
  const caps = f.root().locals.capabilitiesService, actor = caps.authenticateCredential(credentials.get(effectRef.capabilityId));
  await caps.external.reconcile({ reference: effectRef, invocationId: projected.invocationId });
  const receipt = caps.external.get({ actor, invocationId: projected.invocationId });
  assert.equal(receipt.invocation.effectState, 'committed'); assert.deepEqual(receipt.invocation.receipt, projected.receipt);
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM hive_changes WHERE project_id=?').get('selected-native').n), 2);
  await f.restart(); const reloaded = await f.request(f.embedded + '/api/embed/project');
  assert.equal(reloaded.status, 200); assert.equal(reloaded.body.revision, 2); assert.deepEqual(reloaded.body.project, stored.project);
  if (action === 'empty-create') {
    assert.equal(Object.keys(before.core.objects).length, 0);
    const edited = await f.owner.client.extension('agent.turn', { requestId: 'hive_natural_empty_edit_002',
      prompt: 'Измени заголовок карточки «Новая карточка» на «Готово к запуску» в открытом HIVE', selection: request.selection });
    assert.equal(edited.evidence[0].data.completionVerified, true); assert.equal(model.requests.length, 4, 'a separate natural edit still uses one effect and one terminal');
    const afterEdit = f.sql(db => { const row = db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get('selected-native');
      return { revision: row.revision, project: JSON.parse(row.document_json), changes: db.prepare('SELECT count(*) AS n FROM hive_changes WHERE project_id=?').get('selected-native').n }; });
    assert.equal(afterEdit.revision, 3); assert.equal(afterEdit.changes, 3); assert.equal(Object.keys(afterEdit.project.core.objects).length, 1);
    assert.equal(Object.values(afterEdit.project.core.objects)[0].title, 'Готово к запуску');
    assert.deepEqual(afterEdit.project.views, stored.project.views);
    await f.restart(); const final = await f.request(f.embedded + '/api/embed/project');
    assert.equal(final.status, 200); assert.equal(final.body.revision, 3); assert.deepEqual(final.body.project, afterEdit.project);
  }
  const modelText = JSON.stringify(model.requests); assert.equal(modelText.includes(launched.scopedCloseHandle), false);
  assert.equal(modelText.includes(f.legacy.account.id), false); assert.equal(modelText.includes('inputDigest'), false);
});
