import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { selectedRootEnvironment } from '../../modules/source-app/test/support/selected-hive-installed.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { createSelectedHiveSourceApplications } from '../selected-hive-source.js';
import { captureExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/validation.mjs';

// Actual Root signed Connect/capability ledger -> connector HTTP/WS -> maintained
// HIVE Native/repository/SQLite. Only provider/billing replies are synthetic.
// The optional workflow capability is installed solely in this private fixture.
async function fixture(t, { lostReply = false, proofOnFinish = false } = {}) {
  let entries, selected, input, phase = 'write', invocationId, hideProof = lostReply, dropped = false, invokes = 0;
  const credentials = new Map(), grants = new Map(), receipts = new Map(), requests = [], keyRef = {}, tenant = 'synthetic_hive_workflow';
  const f = await selectedRootEnvironment(t, { renewal: true, rootBindings(profile) {
    const scope = { appId: profile.appId, target: profile.target, sourceProfile: profile.sourceProfile, resource: profile.resource };
    const rows = [createCurrentContextSourceApplication(scope), ...createSelectedHiveSourceApplications(scope, { workflow: true })];
    entries = captureExternalApplications(rows);
    return { externalApplications: rows, personalAgent: { enabled: true, historyEnabled: false,
      sourceReferences: entries.map(row => row.contract), sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
        gatewayOrigin: 'https://synthetic-workflow.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
        resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          const body = JSON.parse(init.body); requests.push(body); const last = body.messages.at(-1); let name, args;
          if (last.role !== 'tool') { name = phase === 'receipt' ? 'source_receipt_1' : 'source_catalog';
            args = phase === 'receipt' ? { reference: selected, input, invocationId } : {}; }
          else {
            const evidence = JSON.parse(last.content).evidence;
            if (evidence[0]?.data?.items) {
              const row = evidence[0].data.items.find(x => x.reference.capabilityId.endsWith(':hive.workflow.create'));
              assert.ok(row); assert.equal(row.inputSchema.properties.cards.maxItems, 12);
              selected = row.reference; name = 'source_write_1'; args = { reference: selected, input };
            } else {
              if (proofOnFinish && evidence[0]?.data?.effectState === 'unknown') hideProof = false;
              name = 'finish_answer'; args = { answer: 'Synthetic workflow outcome', selectedEvidenceIds: evidence.map(row => row.id), claims: [] };
            }
          }
          const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
            input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return Response.json({ id: 'synthetic_workflow', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', tool_calls: [{ id: 'synthetic_call_' + requests.length, type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
        } } } };
  } });
  for (const row of entries) {
    const principal = (await f.owner.client.extension('access.principals.create', { expectedAccountId: f.owner.account.accountId, label: 'Synthetic workflow grant' })).principal;
    const grant = (await f.owner.client.extension('access.grants.issue', { expectedAccountId: f.owner.account.accountId, principalId: principal.id,
      capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
      effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.owner.client.extension('access.credentials.issue', { expectedAccountId: f.owner.account.accountId, grantId: grant.id, audience: f.origin });
    credentials.set(row.contract.capabilityId, { token: credential.token, audience: f.origin }); grants.set(row.contract.capabilityId, grant.id);
  }
  await f.launch(); await f.login({ mode: 'write', stayInAppSeconds: 86400 });
  const launch = await f.launch(), current = await f.request(f.embedded + '/api/embed/session-continue', { body: { requestId: 'workflow_current_ready_001' } });
  assert.equal(current.status, 200); assert.equal(current.body.ready, true);
  const before = f.sql(db => JSON.parse(db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get('selected-native').document_json));
  assert.equal(Object.keys(before.views).length, 1);
  input = { operation: 'hive.workflow.create', expectedRevision: 1, viewId: Object.keys(before.views)[0],
    cards: [{ id: 'workflow-start', title: 'Начало', x: 600, y: 0 }, { id: 'workflow-result', title: 'Результат', x: 960, y: 0 }],
    links: [{ id: 'workflow-flow', sourceId: 'workflow-start', targetId: 'workflow-result' }] };
  const apps = f.root().locals.appsService, actualCall = apps.callScopedNativeSource;
  apps.callScopedNativeSource = async args => {
    if (args.path === '/api/embed/operations') {
      invokes++; const result = await actualCall(args);
      if (lostReply && !dropped) { dropped = true; throw Error('synthetic lost return after actual Native COMMIT'); }
      return result;
    }
    if (args.path === '/api/embed/changes' && hideProof && dropped) throw Error('synthetic temporarily unavailable original proof');
    return actualCall(args);
  };
  const handle = launch.scopedCloseHandle;
  return { ...f, before, entries, credentials, grants, handle, input,
    turn: suffix => f.owner.client.extension('agent.turn', { requestId: 'workflow_native_turn_' + suffix,
      prompt: 'Создай две карточки и связь в текущем HIVE', selection: { appId: f.appId, handle } }),
    recover(id) { hideProof = false; phase = 'receipt'; invocationId = id; },
    invokes: () => invokes, requests,
  };
}

test('optional workflow uses real atomic Native SQL, reload, foreign actor denial and Root revoke', { timeout: 180000 }, async t => {
  const f = await fixture(t), result = await f.turn('001');
  assert.equal(result.evidence[0].data.completionVerified, true);
  const read = () => f.sql(db => { const row = db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get('selected-native');
    return { revision: row.revision, project: JSON.parse(row.document_json), changes: db.prepare('SELECT count(*) n FROM hive_changes').get().n }; });
  const saved = read(); assert.equal(saved.revision, 2); assert.equal(saved.changes, 2); assert.equal(f.invokes(), 1);
  assert.equal(saved.project.core.objects['workflow-start'].title, 'Начало');
  assert.deepEqual(saved.project.core.relations['workflow-flow'].endpoints.map(x => x.target.id), ['workflow-start', 'workflow-result']);
  assert.deepEqual(saved.project.core.objects['old-cell'], f.before.core.objects['old-cell']);
  await f.restart(); const reloaded = await f.request(f.embedded + '/api/embed/project'); assert.equal(reloaded.status, 200);
  assert.deepEqual(reloaded.body.project, saved.project);
  await assert.rejects(f.foreign.client.extension('agent.turn', { requestId: 'workflow_foreign_turn_001', prompt: 'Создай workflow', selection: { appId: f.appId, handle: f.handle } }));
  const write = f.entries.find(x => x.contract.capabilityId.endsWith(':hive.workflow.create'));
  await f.owner.client.extension('access.grants.revoke', { expectedAccountId: f.owner.account.accountId, grantId: f.grants.get(write.contract.capabilityId) });
  f.advance(61000); await assert.rejects(f.turn('002')); assert.deepEqual(read(), saved); assert.equal(f.invokes(), 1);
  const body = JSON.stringify(f.requests); assert.equal(body.includes(f.handle), false); assert.equal(body.includes('synthetic-no-provider'), false);
});

test('lost return after real Native COMMIT reconciles original receipt without a second batch', { timeout: 180000 }, async t => {
  const f = await fixture(t, { lostReply: true });
  // The existing per-round Native revision fence rejects this whole turn:
  // it must not silently adopt a revision with an unproven commit. The ledger
  // nevertheless retains the original unknown invocation for a separate read.
  await assert.rejects(f.turn('lost_001'), { code: 'agent_source_denied' });
  const tool = f.requests.at(-1).messages.filter(row => row.role === 'tool').at(-1),
    unknown = JSON.parse(tool.content).evidence[0].data;
  assert.equal(unknown.completionVerified, false); assert.equal(unknown.effectState, 'unknown');
  assert.equal(f.invokes(), 1);
  const status = await f.owner.client.extension('agent.status', { requestId: 'workflow_native_turn_lost_001' });
  assert.equal(status.state, 'unknown');
  assert.deepEqual(status.sourceRecovery, [{ reference: { capabilityId: f.entries.at(-1).contract.capabilityId, version: 1 }, invocationId: unknown.invocationId }]);
  assert.equal(JSON.stringify(status).includes(f.handle), false); assert.equal(JSON.stringify(status).includes('workflow-start'), false);
  const foreign = await f.foreign.client.extension('agent.status', { requestId: 'workflow_native_turn_lost_001' });
  assert.equal(foreign.state, 'unavailable'); assert.equal(foreign.sourceRecovery, undefined);
  f.recover(unknown.invocationId); f.advance(61000); const recovered = await f.turn('receipt_002');
  assert.equal(recovered.evidence[0].data.completionVerified, true, JSON.stringify(recovered.evidence)); assert.equal(f.invokes(), 1);
  assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 2);
  assert.equal(f.sql(db => db.prepare('SELECT count(*) n FROM hive_changes').get().n), 2);
});

test('Native revoke hides the recovery pointer and cannot recover or replay the unknown committed batch', { timeout: 180000 }, async t => {
  const f = await fixture(t, { lostReply: true });
  await assert.rejects(f.turn('lost_revoke_001'), { code: 'agent_source_denied' });
  const unknown = JSON.parse(f.requests.at(-1).messages.filter(row => row.role === 'tool').at(-1).content).evidence[0].data;
  f.sql(db => db.exec('UPDATE hive_selected_embed_heads SET active=0'));
  const status = await f.owner.client.extension('agent.status', { requestId: 'workflow_native_turn_lost_revoke_001' });
  assert.equal(status.state, 'unknown'); assert.equal(status.sourceRecovery, undefined);
  f.recover(unknown.invocationId); f.advance(61000); await assert.rejects(f.turn('revoked_receipt_002'));
  assert.equal(f.invokes(), 1); assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 2);
});

test('a newly available ORIGINAL proof may advance only its own revision during the final round', { timeout: 180000 }, async t => {
  const f = await fixture(t, { lostReply: true, proofOnFinish: true }), result = await f.turn('proof_returns_001');
  // The already recorded unknown evidence is never silently promoted to success.
  const evidence = result.evidence[0].data; assert.equal(evidence.completionVerified, false);
  const caps = f.root().locals.capabilitiesService, row = f.entries.at(-1),
    actor = caps.authenticateCredential(f.credentials.get(row.contract.capabilityId)),
    settled = caps.external.get({ actor, invocationId: evidence.invocationId });
  assert.equal(settled.invocation.status, 'succeeded'); assert.equal(settled.invocation.effectState, 'committed');
  assert.equal(settled.invocation.receipt.artifacts.find(x => x.type === 'hive-project').revision, 2);
  assert.equal(f.invokes(), 1); assert.equal(f.sql(db => db.prepare('SELECT count(*) n FROM hive_changes').get().n), 2);
});

test('an original unknown batch never accepts a later foreign revision as its own receipt', { timeout: 180000 }, async t => {
  const f = await fixture(t, { lostReply: true });
  await assert.rejects(f.turn('lost_foreign_001'), { code: 'agent_source_denied' });
  const unknown = JSON.parse(f.requests.at(-1).messages.filter(row => row.role === 'tool').at(-1).content).evidence[0].data;
  const foreign = await f.request(f.embedded + '/api/embed/operations', { body: { schema: 'hive.operations', version: 2,
    requestId: 'synthetic_foreign_after_lost_003', expectedRevision: 2,
    operations: [{ op: 'object.update', id: 'old-cell', patch: { title: 'Independent Native writer' } }] } });
  assert.equal(foreign.status, 200); assert.equal(foreign.body.revision, 3);
  f.recover(unknown.invocationId); f.advance(61000); const recovered = await f.turn('foreign_receipt_002');
  assert.equal(recovered.evidence[0].data.completionVerified, true);
  assert.equal(recovered.evidence[0].data.receipt.artifacts.find(x => x.type === 'hive-project').revision, 2);
  assert.equal(f.invokes(), 1); assert.equal(f.sql(db => db.prepare('SELECT count(*) n FROM hive_changes').get().n), 3);
});
