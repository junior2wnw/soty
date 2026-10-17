import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { actualHiveWorkspace } from '../../modules/source-app/test/support/hive-workspace-actual.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { createHiveWorkspaceSourceApplications } from '../hive-workspace-source.js';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { captureExternalApplications, composeExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';

// Actual Root signer/Connect, private catalog, grant/ledger and product-agent
// composition -> generic workspace2 adapter -> maintained HIVE Native HTTP,
// Root SSO/ACL, repository and persistent SQLite. Only Apps routing and the
// accounted model provider are synthetic. No real model or user grant is used.
test('product agent discovers exact workspace2 HIVE schema, reads real view, creates two linked cards once and reloads; Root B/revoke stop before dispatch', async t => {
  let connect, capabilities, personal, ownerAccountId, clock = Date.now(), models = 0, nativeCalls = 0, invokes = 0,
    mode = 'create', step = 0, selectedRead, selectedWrite, targetProjectId, targetViewId, lostAck = true;
  const native = await actualHiveWorkspace(t), scope = { appId: native.profile.appId, target: native.profile.target,
    sourceProfile: native.profile.sourceProfile, resource: native.profile.resource },
    rows = [createCurrentContextSourceApplication(scope), ...createHiveWorkspaceSourceApplications(scope)],
    entries = captureExternalApplications(rows), credentials = new Map(), grants = new Map(), receipts = new Map(), modelBodies = [],
    origin = 'https://hive-product-agent.fixture.invalid', handle = 'h'.repeat(43), tenant = 'synthetic-hive-product', keyRef = {},
    notes = createNotesService({ databasePath: ':memory:', projectId: 'soty', clock: () => clock });
  const owner = actor => { assert.equal(actor.accountId, ownerAccountId); assert.equal(connect.isActorActive(actor), true); };
  const apps = {
    withAppAuthority({ actor, appId }, action) { owner(actor); assert.equal(appId, scope.appId); return action({ target: scope.target }); },
    withScopedContextSourceAuthority({ owner: actor, scope: selectedScope, handle: selectedHandle }, action) {
      owner(actor); assert.equal(selectedScope.appId, scope.appId); assert.equal(selectedHandle, handle); return action(scope);
    },
    canOpenScopedSourceContext({ owner: actor, appId }) { return actor.accountId === ownerAccountId && appId === scope.appId; },
    async openScopedSourceContext({ owner: actor, appId, withCurrentOwner }) {
      owner(actor); assert.equal(appId, scope.appId); withCurrentOwner(() => true); return { appId, handle };
    },
    async callScopedContextSource(args) {
      args.withCurrentOwner(owner); nativeCalls++;
      const result = await native.wire('/api/embed/query', { operation: 'soty.current_context' }, { requestId: args.requestId });
      return { status: result.status, data: result.body };
    },
    async callScopedNativeSource(args) {
      args.withCurrentOwner(owner); assert.equal(args.handle, handle); nativeCalls++;
      if (args.path.endsWith('/invoke')) { invokes++; if (lostAck) { lostAck = false; native.dropNextAck(); } }
      const result = await native.wire(args.path, args.input, { requestId: args.requestId, contextRef: args.contextRef });
      return { status: result.status, data: result.body };
    },
  };
  const wiring = { withConnectFence: action => connect.withAuthorityFence(action), capabilities: () => capabilities, apps: () => apps };
  capabilities = createCapabilitiesService({ databasePath: ':memory:', projectId: 'soty', clock: () => clock,
    documentation: [], actorActive: actor => connect.isActorActive(actor), catalog: entries.map(x => x.catalog),
    readonlyQueries: composeExternalApplications(entries.filter(x => x.readonly), wiring),
    externalAdapters: composeExternalApplications(entries.filter(x => !x.readonly), wiring) });
  personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({ notes, apps, capabilities }), configuration: {
    enabled: true, historyEnabled: false, clock: () => clock, sourceReferences: entries.map(x => x.contract),
    sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
      gatewayOrigin: 'https://synthetic-provider.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
      resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-no-external-model' }),
      readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
      fetchImpl: async (_url, init) => {
        models++; const body = JSON.parse(init.body); modelBodies.push(body); const last = body.messages.at(-1); let name, args;
        if (step++ === 0) { name = 'source_catalog'; args = { query: 'hive.workspace', limit: 2 }; }
        else {
          const evidence = JSON.parse(last.content).evidence;
          if (evidence[0]?.data?.items) {
            const read = evidence[0].data.items.find(x => x.reference.capabilityId.endsWith(':hive.workspace.cells.context')),
              write = evidence[0].data.items.find(x => x.reference.capabilityId.endsWith(':hive.workspace.additive.batch'));
            assert.ok(read); assert.ok(write); selectedRead = read.reference; selectedWrite = write.reference;
            assert.deepEqual(read.inputSchema.properties.operation.enum, ['hive.project.read']);
            assert.deepEqual(write.inputSchema.properties.operations.items.properties.op.enum, ['object.add', 'occurrence.add', 'relation.add']);
            name = 'source_read_1'; args = { reference: selectedRead, input: { operation: 'hive.project.read' } };
          } else if (evidence[0]?.data?.insertionViews && mode === 'create') {
            const data = evidence[0].data; targetProjectId = data.projectId; assert.equal(data.cells.length, 0); assert.equal(data.insertionViews.length, 1);
            const view = data.insertionViews[0]; targetViewId = view.id;
            const card = (id, title) => ({ id, type: 'hive.process/action', title, summary: '', content: [], properties: {}, extensions: {} }),
              occurrence = (id, x) => ({ id: id + ':first', target: { kind: 'object', id }, x, y: view.y, width: 280, height: 240 });
            name = 'source_write_1'; args = { reference: selectedWrite, input: { operation: 'hive.operations', expectedRevision: data.revision, operations: [
              { op: 'object.add', object: card('product-start', 'Начало') },
              { op: 'occurrence.add', viewId: view.id, occurrence: occurrence('product-start', view.x) },
              { op: 'object.add', object: card('product-result', 'Результат') },
              { op: 'occurrence.add', viewId: view.id, occurrence: occurrence('product-result', view.x + 360) },
              { op: 'relation.add', relation: { id: 'product-flow', type: 'hive.process/flow', endpoints: [
                { role: 'source', target: { kind: 'object', id: 'product-start' } },
                { role: 'target', target: { kind: 'object', id: 'product-result' } },
              ], properties: {}, extensions: {} } },
            ] } };
          } else { name = 'finish_answer'; args = { answer: 'Synthetic model; actual Native HIVE evidence', selectedEvidenceIds: evidence.map(x => x.id), claims: [] }; }
        }
        const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
          input_tokens: 3, output_tokens: 2, charge_urub: '0' });
        return Response.json({ id, model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls', message: { role: 'assistant', tool_calls: [
          { id: 'synthetic_hive_call_' + models, type: 'function', function: { name, arguments: JSON.stringify(args) } },
        ] } }], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
      },
    },
  } });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', clock: () => clock, allowedOrigins: [origin], extensions: [notes, capabilities, personal] });
  t.after(() => { personal.close(); capabilities.close(); notes.close(); connect.close(); });
  const good = result => { assert.equal(result.ok, true, JSON.stringify({ code: result.error?.code, step, invokes })); return result; };
  const client = () => {
    const key = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), publicJwk = key.publicKey.export({ format: 'jwk' });
    return async (op, args = {}) => {
      if (op === 'bootstrap') args = { ...args, encryptionPublicJwk: publicJwk };
      const challenge = good(await connect.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
      return connect.handle({ origin, op, args, proof: { challengeId: challenge.challengeId, publicJwk,
        signature: sign('sha256', Buffer.from(challenge.message), { key: key.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
    };
  };
  const rpc = client(); ownerAccountId = good(await rpc('bootstrap', { label: 'Synthetic HIVE owner' })).accountId;
  native.newContext({ accountId: ownerAccountId }); assert.equal((await native.wire('/api/embed/session-continue')).status, 200);
  for (const row of entries) {
    const principal = good(await rpc('access.principals.create', { expectedAccountId: ownerAccountId, label: 'Synthetic HIVE acceptance' })).principal,
      grant = good(await rpc('access.grants.issue', { expectedAccountId: ownerAccountId, principalId: principal.id,
        capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
        effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: clock + 600000, budget: { unit: 'invocations', limit: 60 } })).grant,
      credential = good(await rpc('access.credentials.issue', { expectedAccountId: ownerAccountId, grantId: grant.id, audience: origin }));
    credentials.set(row.contract.capabilityId, { token: credential.token, audience: origin }); grants.set(row.contract.capabilityId, grant.id);
  }
  const created = good(await rpc('agent.turn', { requestId: 'hive_workspace_product_create_001', prompt: 'Create two linked cards in my HIVE', selection: { appId: scope.appId, handle } }));
  assert.equal(created.evidence.at(-1).data.completionVerified, true); assert.equal(invokes, 1); assert.equal(lostAck, false);
  assert.ok(targetProjectId); assert.equal(native.db.prepare('SELECT count(*) n FROM hive_changes WHERE project_id=?').get(targetProjectId).n, 1);
  const document = JSON.parse(native.db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get(targetProjectId).document_json);
  assert.equal(document.core.objects['product-start'].title, 'Начало'); assert.equal(document.core.objects['product-result'].title, 'Результат');
  assert.equal(document.views[targetViewId].placements['product-start:first'].target.id, 'product-start');
  assert.equal(document.views[targetViewId].placements['product-result:first'].target.id, 'product-result');
  assert.deepEqual(document.core.relations['product-flow'].endpoints.map(x => x.target.id), ['product-start', 'product-result']);
  native.restart(); mode = 'read'; step = 0; clock += 61000;
  const reloaded = good(await rpc('agent.turn', { requestId: 'hive_workspace_product_reload_002', prompt: 'Read the saved HIVE cards', selection: { appId: scope.appId, handle } }));
  assert.equal(reloaded.evidence[0].data.cells.length, 2); assert.equal(reloaded.evidence[0].data.revision, 2); assert.equal(invokes, 1);
  const foreign = client(); good(await foreign('bootstrap', { label: 'Synthetic independent B' }));
  let beforeModels = models, beforeNative = nativeCalls;
  assert.equal((await foreign('agent.turn', { requestId: 'hive_workspace_product_foreign_003', prompt: 'Read A HIVE', selection: { appId: scope.appId, handle } })).ok, false);
  assert.equal(models, beforeModels); assert.equal(nativeCalls, beforeNative);
  for (const grantId of grants.values()) good(await rpc('access.grants.revoke', { expectedAccountId: ownerAccountId, grantId }));
  clock += 61000; beforeModels = models; beforeNative = nativeCalls;
  assert.equal((await rpc('agent.turn', { requestId: 'hive_workspace_product_revoked_004', prompt: 'Read after revoke', selection: { appId: scope.appId, handle } })).ok, false);
  assert.equal(models, beforeModels); assert.equal(nativeCalls, beforeNative); assert.equal(invokes, 1);
  const sent = JSON.stringify(modelBodies); assert.equal(sent.includes(handle), false); assert.equal(sent.includes('synthetic-no-external-model'), false);
  for (const credential of credentials.values()) assert.equal(sent.includes(credential.token), false);
});
