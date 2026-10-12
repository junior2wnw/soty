import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { selectedRootEnvironment } from '../../modules/source-app/test/support/selected-hive-installed.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { createSelectedHiveSourceApplications } from '../selected-hive-source.js';
import { captureExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/validation.mjs';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';

test('catalog discovery then one installed HIVE title patch reaches Native SQL and its exact receipt advances only own revision', { timeout: 180000 }, async t => {
  let entries, credentials = new Map(), phase = 'read', writeInput, invocationId, selectedReference;
  const keyRef = {}, tenant = 'synthetic_hive_agent', receipts = new Map(), models = [];
  const f = await selectedRootEnvironment(t, { renewal: true, rootBindings(profile) {
    const scope = { appId: profile.appId, target: profile.target, resource: profile.resource, sourceProfile: profile.sourceProfile },
      rows = [createCurrentContextSourceApplication(scope), ...createSelectedHiveSourceApplications(scope)];
    entries = captureExternalApplications(rows);
    return { externalApplications: rows, personalAgent: { enabled: true, historyEnabled: false, sourceReferences: entries.map(row => row.contract),
      sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
        gatewayOrigin: 'https://synthetic-hive-agent.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
        resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-local-hive-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          const body = JSON.parse(init.body); models.push(body); let name, args;
          const lastTool = body.messages.filter(row => row.role === 'assistant').at(-1)?.tool_calls?.[0]?.function.name;
          if (phase === 'write' && lastTool === 'source_catalog') {
            assert.ok(body.tools.some(row => row.function.name === 'source_write_1'));
            assert.ok(!body.tools.some(row => row.function.name === 'source_catalog'));
            const discovered = JSON.parse(body.messages.at(-1).content).evidence[0].data.items;
            assert.ok(discovered.some(row => row.reference.capabilityId === selectedReference.capabilityId && row.inputSchema));
            name = 'source_write_1'; args = { reference: selectedReference, input: writeInput };
          } else if (body.messages.at(-1).role === 'tool') { name = 'finish_answer'; const evidence = JSON.parse(body.messages.at(-1).content).evidence;
            args = { answer: 'Verified selected HIVE result', selectedEvidenceIds: evidence.map(row => row.id), claims: [] }; }
          else if (phase === 'read') { name = 'source_read_1'; args = { reference: entries[1].contract, input: { operation: 'hive.cells.list' } }; }
          else if (phase === 'write') { name = 'source_catalog'; args = {}; }
          else { name = 'source_receipt_1'; args = { reference: selectedReference, input: writeInput, invocationId }; }
          const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
            input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return Response.json({ id: 'synthetic_hive_agent', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', tool_calls: [{ id: 'synthetic_call_' + models.length, type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
        } } } };
  } });
  for (const row of entries) {
    const principal = (await f.owner.client.extension('access.principals.create', { expectedAccountId: f.owner.account.accountId, label: 'Synthetic HIVE bound grant' })).principal;
    const grant = (await f.owner.client.extension('access.grants.issue', { expectedAccountId: f.owner.account.accountId, principalId: principal.id,
      capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
      effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.owner.client.extension('access.credentials.issue', { expectedAccountId: f.owner.account.accountId, grantId: grant.id, audience: f.origin });
    credentials.set(row.contract.capabilityId, { token: credential.token, audience: f.origin });
  }
  await f.launch(); await f.login({ mode: 'write', stayInAppSeconds: 86400 });
  const launched = await f.launch(), continued = await f.request(f.embedded + '/api/embed/session-continue', { body: { requestId: 'hive_agent_current_ready_001' } });
  assert.equal(continued.status, 200); assert.equal(continued.body.ready, true);
  const handle = launched.scopedCloseHandle;
  const turn = suffix => f.owner.client.extension('agent.turn', { requestId: 'hive_agent_native_turn_' + suffix,
    prompt: 'Use this selected HIVE field', selection: { appId: f.appId, handle } });
  const initial = await turn('001'), before = initial.evidence[0].data;
  assert.equal(before.revision, 1); assert.ok(before.cells.some(row => row.id === 'old-cell'));
  const original = f.sql(db => JSON.parse(db.prepare('SELECT document_json FROM hive_projects WHERE id=?').get('selected-native').document_json));
  selectedReference = entries[2].contract; writeInput = { operation: 'hive.cell.title', expectedRevision: before.revision,
    id: 'old-cell', title: 'Synthetic title from the selected agent' }; phase = 'write';
  const changed = await turn('002'); invocationId = changed.evidence[0].data.invocationId;
  assert.equal(models.length, 5); assert.deepEqual(models.at(-1).tools.map(row => row.function.name), ['finish_answer']);
  assert.equal(changed.evidence[0].data.completionVerified, true);
  assert.equal(changed.evidence[0].data.receipt.artifacts[0].revision, 2);
  const stored = f.sql(db => { const row = db.prepare('SELECT revision,document_json FROM hive_projects WHERE id=?').get('selected-native');
    return { revision: row.revision, project: JSON.parse(row.document_json), changes: db.prepare('SELECT count(*) AS n FROM hive_changes WHERE project_id=?').get('selected-native').n }; });
  assert.equal(stored.revision, 2); assert.equal(stored.project.core.objects['old-cell'].title, writeInput.title); assert.equal(stored.changes, 2);
  assert.deepEqual(stored.project.core.relations, original.core.relations); assert.deepEqual(stored.project.views, original.views);
  // This separate receipt turn begins outside the preceding turns' per-principal rate window.
  f.advance(61000);
  phase = 'receipt'; const recovered = await turn('003'); assert.equal(recovered.evidence[0].data.completionVerified, true);
  assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 2);
  assert.equal(JSON.stringify(models).includes(handle), false); assert.equal(JSON.stringify(models).includes(f.legacy.account.id), false);
  assert.equal(JSON.stringify(models).includes('inputDigest'), false);
  await f.restart(); const reloaded = await f.request(f.embedded + '/api/embed/project'); assert.equal(reloaded.status, 200);
  assert.equal(reloaded.body.revision, 2); assert.equal(reloaded.body.project.core.objects['old-cell'].title, writeInput.title);
  const ownRequestId = f.sql(db => db.prepare('SELECT request_id FROM hive_changes WHERE project_id=? AND revision=2').get('selected-native').request_id);
  const foreign = await f.request(f.embedded + '/api/embed/operations', { body: { schema: 'hive.operations', version: 2,
    requestId: 'synthetic_foreign_revision_003', expectedRevision: 2,
    operations: [{ op: 'object.update', id: 'old-cell', patch: { title: 'Synthetic other writer' } }] } });
  assert.equal(foreign.status, 200); assert.equal(foreign.body.revision, 3);
  const replay = await f.request(f.embedded + '/api/embed/operations', { body: { schema: 'hive.operations', version: 2,
    requestId: ownRequestId, expectedRevision: 1,
    operations: [{ op: 'object.update', id: writeInput.id, patch: { title: writeInput.title } }] } });
  assert.equal(replay.status, 200); assert.equal(replay.body.replayed, true); assert.equal(replay.body.revision, 3);
  const caps = f.root().locals.capabilitiesService, actor = caps.authenticateCredential(credentials.get(selectedReference.capabilityId)),
    originalInput = { handle, input: writeInput };
  const proof = await entries[2].adapter.readProof({ actor, requestId: ownRequestId, inputDigest: canonicalHash(originalInput), input: originalInput }, new AbortController().signal);
  assert.equal(proof.outcome, 'committed'); assert.equal(proof.receipt.artifacts[0].revision, 2, 'receipt is the original change, never latest replay revision');
  const uncertain = await caps.external.invoke({ actor, reference: selectedReference, idempotencyKey: 'synthetic_stale_revision_002',
    input: { handle, input: { ...writeInput, expectedRevision: 2, title: 'Must not overwrite concurrent writer' } } });
  assert.notEqual(uncertain.invocation.status, 'succeeded'); assert.equal(uncertain.invocation.effectState, 'unknown');
  assert.equal(f.sql(db => db.prepare('SELECT revision FROM hive_projects WHERE id=?').get('selected-native').revision), 3);
  await caps.external.reconcile({ reference: selectedReference, invocationId: uncertain.invocation.invocationId,
    expectedInput: { handle, input: { ...writeInput, expectedRevision: 2, title: 'Must not overwrite concurrent writer' } } });
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM hive_changes WHERE project_id=?').get('selected-native').n), 3);
  f.sql(db => db.exec('UPDATE hive_selected_embed_heads SET active=0'));
  const revoked = await caps.external.invoke({ actor, reference: selectedReference, idempotencyKey: 'synthetic_native_revoked_003',
    input: { handle, input: { ...writeInput, expectedRevision: 3, title: 'Must not bypass revoked Native consent' } } });
  assert.notEqual(revoked.invocation.status, 'succeeded');
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM hive_changes WHERE project_id=?').get('selected-native').n), 3);
});
