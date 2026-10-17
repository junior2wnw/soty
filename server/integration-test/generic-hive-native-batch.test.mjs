import test from 'node:test';
import assert from 'node:assert/strict';
import { actualHiveWorkspace } from '../../modules/source-app/test/support/hive-workspace-actual.mjs';
import { bindSelectedNativeSourceAdapter } from '../selected-native-source.js';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_SELECTED_SOURCE_V3 } from '../../modules/source-app/server/standard-profile.mjs';
import { createHiveWorkspaceSourceApplications } from '../hive-workspace-source.js';

const batch = (viewId, expectedRevision = 1) => ({ operation: 'hive.operations', expectedRevision, operations: [
  { op: 'object.add', object: { id: 'generic-start', type: 'hive.process/action', title: 'Начало', summary: '', content: [], properties: {}, extensions: {} } },
  { op: 'occurrence.add', viewId, occurrence: { id: 'generic-start:first', target: { kind: 'object', id: 'generic-start' }, x: 0, y: 0, width: 280, height: 240 } },
  { op: 'object.add', object: { id: 'generic-result', type: 'hive.process/action', title: 'Результат', summary: '', content: [], properties: {}, extensions: {} } },
  { op: 'occurrence.add', viewId, occurrence: { id: 'generic-result:first', target: { kind: 'object', id: 'generic-result' }, x: 360, y: 0, width: 280, height: 240 } },
  { op: 'relation.add', relation: { id: 'generic-flow', type: 'hive.process/flow', endpoints: [
    { role: 'source', target: { kind: 'object', id: 'generic-start' } }, { role: 'target', target: { kind: 'object', id: 'generic-result' } },
  ], properties: {}, extensions: {} } },
] });

// This test explicitly names the REAL Native profile. Source3's presence in the
// SDK or the serving module cannot turn workspace2 into a Source3 deployment.
test('generic Root workspace2 adapter sends actual HIVE additive batch; lost ACK/proof/reload/foreign actor/revoke preserve one COMMIT', async t => {
  const f = await actualHiveWorkspace(t); assert.deepEqual(f.profile.sourceProfile, STANDARD_ROOT_WORKSPACE_SOURCE);
  assert.notDeepEqual(f.profile.sourceProfile, STANDARD_SELECTED_SOURCE_V3);
  assert.equal((await f.wire('/api/embed/session-continue')).status, 200);
  const current = await f.wire('/api/embed/query', { operation: 'soty.current_context' }); assert.equal(current.status, 200);
  const contextRef = current.body.data.context.ref, read = await f.wire('/api/embed/query', { operation: 'hive.project.read' }, { contextRef });
  assert.equal(read.status, 200); const before = read.body.data, views = Object.keys(before.project.views); assert.equal(views.length, 1);
  const scope = { appId: f.profile.appId, target: f.profile.target, sourceProfile: f.profile.sourceProfile, resource: f.profile.resource },
    rows = createHiveWorkspaceSourceApplications(scope), row = rows[1];
  let invokes = 0;
  for (const entry of rows) bindSelectedNativeSourceAdapter(entry.adapter, { appId: scope.appId, target: scope.target,
    withOwner: (_request, action) => action({ accountId: 'synthetic-root-owner' }), apps: () => ({ async callScopedNativeSource(args) {
      args.withCurrentOwner(() => true); if (args.path.endsWith('/invoke')) invokes++;
      const result = await f.wire(args.path, args.input, { requestId: args.requestId, contextRef: args.contextRef });
      return { status: result.status, data: result.body };
    } }) });
  const contextInput = { handle: 'h'.repeat(43), contextRef, input: { operation: 'hive.project.read' } },
    bounded = await rows[0].adapter.query({ requestId: 'generic-hive-bounded-context-001', input: contextInput,
      inputDigest: canonicalHash(contextInput) }, new AbortController().signal, () => true);
  assert.deepEqual(bounded.insertionViews, [{ id: views[0], x: 0, y: 0 }]); assert.deepEqual(bounded.cells, []);
  assert.equal(Object.hasOwn(bounded, 'account'), false); assert.equal(Object.hasOwn(bounded, 'project'), false);
  const input = { handle: 'h'.repeat(43), contextRef, input: batch(bounded.insertionViews[0].id, bounded.revision) }, request = {
    requestId: 'generic-hive-original-batch-001', input, inputDigest: canonicalHash(input) }, signal = new AbortController().signal;
  f.dropNextAck(); await assert.rejects(row.adapter.execute(request, signal));
  const proof = await row.adapter.readProof(request, signal); assert.equal(proof.outcome, 'committed'); assert.equal(invokes, 1);
  const projectId = before.project.id, saved = f.db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get(projectId), project = JSON.parse(saved.document_json);
  assert.equal(saved.revision, before.revision + 1); assert.equal(project.core.objects['generic-start'].title, 'Начало');
  assert.deepEqual(project.core.relations['generic-flow'].endpoints.map(x => x.target.id), ['generic-start', 'generic-result']);
  assert.equal(f.db.prepare('SELECT count(*) n FROM hive_changes WHERE project_id=?').get(projectId).n, 1);
  const originalArtifact = proof.receipt.artifacts[0]; assert.equal(originalArtifact.revision, saved.revision);
  // The A lease has just succeeded, so this B denial is not explained by a
  // restart/stale-lease control. A new Root person cannot use A's contextRef.
  f.newContext({ subject: 'root-other', accountId: 'root-other-account' }); f.clearCookie(); assert.equal((await f.wire('/api/embed/session-continue')).status, 200);
  assert.equal((await row.adapter.readProof(request, signal)).outcome, 'unknown');
  await assert.rejects(row.adapter.execute({ ...request, requestId: 'generic-hive-denied-other-002' }, signal));
  f.newContext(); f.clearCookie(); assert.equal((await f.wire('/api/embed/session-continue')).status, 200);
  f.restart(); assert.equal((await f.wire('/api/embed/session-continue')).status, 200);
  const afterRestart = await f.wire('/api/embed/query', { operation: 'soty.current_context' }),
    reloaded = await f.wire('/api/embed/query', { operation: 'hive.project.read' }, { contextRef: afterRestart.body.data.context.ref });
  assert.equal(reloaded.status, 200); assert.deepEqual(reloaded.body.data.project, project);
  // The old context lease is not silently rebound after a BFF restart. The
  // already captured original proof remains the only accepted receipt above.
  assert.equal((await row.adapter.readProof(request, signal)).outcome, 'unknown'); assert.equal(invokes, 2);
  const own = await f.wire('/api/embed/query', { operation: 'soty.current_context' }), ownRef = own.body.data.context.ref;
  f.db.prepare('DELETE FROM hive_project_members WHERE account_id=? AND project_id=?').run('windows', projectId);
  const revokedInput = { ...input, contextRef: ownRef }, revoked = { requestId: 'generic-hive-denied-revoke-003', input: revokedInput, inputDigest: canonicalHash(revokedInput) };
  assert.equal((await row.adapter.readProof(revoked, signal)).outcome, 'unknown'); await assert.rejects(row.adapter.execute(revoked, signal));
  assert.equal(f.db.prepare('SELECT revision FROM hive_projects WHERE id=?').get(projectId).revision, saved.revision);
  assert.equal(f.db.prepare('SELECT count(*) n FROM hive_changes WHERE project_id=?').get(projectId).n, 1);
});

test('generic HIVE shared validator rejects mixed payload/dangling references before Native; Native revision conflict makes no partial COMMIT', async t => {
  const f = await actualHiveWorkspace(t); assert.equal((await f.wire('/api/embed/session-continue')).status, 200);
  const current = await f.wire('/api/embed/query', { operation: 'soty.current_context' }), contextRef = current.body.data.context.ref,
    read = await f.wire('/api/embed/query', { operation: 'hive.project.read' }, { contextRef }), before = read.body.data,
    viewId = Object.keys(before.project.views)[0], scope = { appId: f.profile.appId, target: f.profile.target,
      sourceProfile: f.profile.sourceProfile, resource: f.profile.resource }, row = createHiveWorkspaceSourceApplications(scope)[1];
  const original = f.db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get(before.project.id);
  let dispatches = 0;
  bindSelectedNativeSourceAdapter(row.adapter, { appId: scope.appId, target: scope.target,
    withOwner: (_request, action) => action({ accountId: 'synthetic-root-owner' }), apps: () => ({ async callScopedNativeSource(args) {
      args.withCurrentOwner(() => true); dispatches++; const result = await f.wire(args.path, args.input, { requestId: args.requestId, contextRef: args.contextRef });
      return { status: result.status, data: result.body };
    } }) });
  for (const mode of ['mixed-payload', 'dangling-relation', 'stale-revision']) {
    const value = batch(viewId, before.revision);
    if (mode === 'mixed-payload') value.operations[0].occurrence = value.operations[1].occurrence;
    if (mode === 'dangling-relation') value.operations[4].relation.endpoints[1].target.id = 'not-created';
    if (mode === 'stale-revision') value.expectedRevision++;
    const input = { handle: 'h'.repeat(43), contextRef, input: value }, request = { requestId: 'generic-hive-invalid-' + mode,
      input, inputDigest: canonicalHash(input) }, signal = new AbortController().signal;
    const previous = dispatches; await assert.rejects(row.adapter.execute(request, signal));
    if (mode === 'stale-revision') { assert.equal((await row.adapter.readProof(request, signal)).outcome, 'not_applied'); assert.equal(dispatches, previous + 2); }
    else { await assert.rejects(row.adapter.readProof(request, signal)); assert.equal(dispatches, previous); }
    const saved = f.db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get(before.project.id);
    assert.deepEqual(saved, original, 'Compare persisted bytes to the original SQL row, not Native read projection (which removes private camera state)');
    assert.equal(f.db.prepare('SELECT count(*) n FROM hive_changes WHERE project_id=?').get(before.project.id).n, 0);
  }
});
