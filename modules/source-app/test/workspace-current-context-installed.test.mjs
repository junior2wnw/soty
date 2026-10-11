import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { selectedNativeApplications } from './support/selected-native-contracts.mjs';
import { STANDARD_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { captureExternalApplications } from '../../../server/external-applications.js';
import { GLM_MODEL } from '../../personal-agent/provider/validation.mjs';

// Two Native SQL authorities, actual signed Root/OIDC/connector HTTP+WS and
// the production agent executor. Only provider/accounting and UI selection
// gestures are synthetic; selection itself uses the actual existing query HTTP.
// No model request leaves this local fixture.
test('one Root: two workspace apps/two children, current selection, Native revision COMMIT and original receipt', { timeout: 120000 }, async t => {
  let entries, realm, phase = 'context', serial = 0, onModel = null, invocationId, providerFailure, lastToolEvidence, receiptMode;
  const credentials = new Map(), requests = [], receipts = new Map(), refs = new Set(), keyRef = {}, tenant = 'synthetic_workspace';
  const nativeInput = { operation: 'items.create', title: 'Current child item' };
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, sourceProfile: STANDARD_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    externalApplications(realms) { const rows = realms.flatMap(value => selectedNativeApplications(value.profile, value.workspaceCreation)); entries = captureExternalApplications(rows); return rows; },
    personalAgent() { return { enabled: true, historyEnabled: false, sourceReferences: entries.map(row => row.contract),
      sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
        gatewayOrigin: 'https://synthetic-workspace.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
        resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-only-local-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          try {
          const request = JSON.parse(init.body); requests.push(request);
          if (onModel) { const run = onModel; onModel = null; await run(); }
          let name, args;
          if (request.messages.at(-1).role === 'tool') {
            name = 'finish_answer'; const evidence = JSON.parse(request.messages.at(-1).content).evidence;
            lastToolEvidence = evidence[0].data;
            args = { answer: 'Verified workspace result', selectedEvidenceIds: evidence.map(row => row.id), claims: [] };
          } else if (phase === 'create' || phase === 'receipt') {
            name = phase === 'create' ? 'source_write_1' : 'source_receipt_1';
            args = { reference: entry(realm, 'items.create').contract, input: realm.realmId === 'board' ? nativeInput : { operation: 'books.add', label: 'A Native book', category: 'book' },
              ...(phase === 'receipt' ? { invocationId } : {}) };
          } else { name = 'current_context'; args = {}; }
          const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL, input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return Response.json({ id: 'synthetic_workspace', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', tool_calls: [{ id: 'workspace_call_' + (++serial), type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
          } catch (error) { providerFailure = error; throw error; }
        } } }; } });
  const caps = f.root().locals.capabilitiesService;
  function entry(value, suffix) { return entries.find(row => row.appId === value.appId && row.catalog.capabilityId.endsWith(':' + suffix)); }
  async function issue(kind) {
    const rows = entries.filter(row => row.catalog.capabilityId.endsWith(kind === 'context' ? ':current_context' : ':items.' + kind));
    const principal = (await f.reader.client.extension('access.principals.create', { expectedAccountId: f.reader.account.accountId, label: 'Workspace ' + kind })).principal;
    const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId, principalId: principal.id,
      capabilities: rows.map(row => ({ capabilityId: row.contract.capabilityId, version: row.contract.version })), resources: rows.flatMap(row => row.catalog.resources),
      effects: kind === 'create' ? ['create'] : [], recipients: rows.flatMap(row => row.catalog.recipients), expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 150 } })).grant;
    const issued = await f.reader.client.extension('access.credentials.issue', { expectedAccountId: f.reader.account.accountId, grantId: grant.id, audience: f.origin });
    for (const row of rows) credentials.set(row.contract.capabilityId, { token: issued.token, audience: f.origin });
  }
  for (const kind of ['context', 'list', 'create']) await issue(kind);
  let sequence = 0;
  const actor = row => caps.authenticateCredential(credentials.get(row.contract.capabilityId));
  const context = async value => {
    const row = entry(value, 'current_context'), result = await caps.external.query({ actor: actor(row), reference: row.contract,
      idempotencyKey: 'workspace_context_' + (++sequence), input: { handle: value.lastLaunch.scopedCloseHandle } });
    assert.equal(result.invocation.status, 'succeeded'); refs.add(result.result.context.ref); return result.result;
  };
  async function select(value, projectId) {
    const selected = await f.wire(value.embedded + '/api/embed/query', { body: { requestId: 'workspace_selection_' + (++sequence),
      input: { operation: 'soty.context.select', nativeId: projectId, generation: sequence } } });
    assert.equal(selected.status, 200, selected.value?.error?.code);
    return selected.value.data;
  }
  // Isolate scenarios from the unchanged ten-reads/minute per-principal quota.
  // Each turn still keeps one real finite grant for all its fresh reads.
  const turn = async () => { await issue('context'); providerFailure = undefined; return f.reader.client.extension('agent.turn', { requestId: 'workspace_turn_' + (++sequence), prompt: 'Use my current app project', selection: { appId: realm.appId, handle: realm.lastLaunch.scopedCloseHandle } }).catch(error => { throw providerFailure ?? error; }); };
  for (const value of f.realms) {
    await value.launch(); const login = await f.authorize(value); await f.complete(login.callbackUrl);
    const continued = await f.wire(value.embedded + '/api/embed/session-continue', { body: { requestId: 'workspace-ready-' + value.realmId } });
    assert.equal(continued.status, 200); assert.equal((await context(value)).title, value.realmId + '-one');
  }
  const [a, b] = f.realms, oldA = await context(a), oldB = await context(b);
  await select(a, 'board-two'); realm = a; assert.equal((await turn()).evidence[0].data.title, 'board-two');
  assert.equal((await context(b)).title, 'library-one'); await select(b, 'library-two'); realm = b;
  assert.equal((await turn()).evidence[0].data.title, 'library-two');
  for (const [value, old] of [[a, oldA], [b, oldB]]) {
    const denied = await f.wire(value.embedded + '/api/embed/query', { body: { requestId: 'stale_workspace_' + value.realmId, input: { operation: 'items.list' }, contextRef: old.context.ref } });
    assert.equal(denied.status, 403);
  }
  await assert.rejects(select(a, 'library-two'), /workspace_child_denied/u);
  realm = a; phase = 'create'; let invokes = 0;
  a.instance.server.prependListener('request', (req, res) => {
    if (req.url === '/api/embed/invoke') invokes++;
    if (req.url !== '/api/embed/receipt') return;
    const end = res.end.bind(res); res.end = (...args) => {
      if (res.statusCode === 200 && receiptMode === 'unknown'
        && a.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items').get().n >= 2) { res.destroy(); return res; }
      if (res.statusCode === 200 && receiptMode && receiptMode !== 'unknown') {
        const response = JSON.parse(args[0]);
        if (response.data.contextAfter) {
          if (receiptMode === 'metadata-forged') response.data.contextAfter.revision += 100;
          if (receiptMode === 'wrong-request') response.data.requestId = 'workspace_other_original_intent';
          if (receiptMode === 'wrong-digest') response.data.inputDigest = '0'.repeat(64);
          if (receiptMode === 'sibling-artifact') response.data.workspaceReceipt.receipt.artifacts[0].id = 'other-child-item';
          if (receiptMode === 'undeclared-effect') response.data.workspaceReceipt.effects[0].kind = 'deleted';
        }
        args[0] = JSON.stringify(response);
      }
      return end(...args);
    };
  });
  const before = await context(a), created = await turn(); invocationId = created.evidence[0].data.invocationId;
  assert.equal(created.evidence[0].data.completionVerified, true); assert.equal((await context(a)).revision, before.revision + 1);
  const knownInvocationId = invocationId;
  phase = 'receipt'; const reconciled = await turn(); assert.equal(reconciled.evidence[0].data.completionVerified, true); assert.equal(invokes, 1);
  const durable = a.instance.store.db.prepare('SELECT result_json FROM native_receipts WHERE resource_id=?').get('board-two').result_json;
  assert.equal(JSON.parse(durable).contextAfter.revision, 2);
  for (const value of f.realms) for (const child of value.options.workspace.children) {
    assert.equal(value.instance.store.db.prepare('SELECT count(*) AS n FROM native_items WHERE resource_id=?').get(child).n, child === 'board-two' ? 1 : 0);
  }
  phase = 'create'; receiptMode = 'unknown'; await assert.rejects(turn());
  assert.equal(lastToolEvidence.effectState, 'unknown'); assert.equal(lastToolEvidence.completionVerified, false);
  invocationId = lastToolEvidence.invocationId; receiptMode = null; phase = 'receipt';
  assert.equal((await turn()).evidence[0].data.completionVerified, true); assert.equal(invokes, 2, 'unknown reads the original proof, never sends create again');
  const originals = a.instance.store.db.prepare('SELECT result_json FROM native_receipts WHERE resource_id=? ORDER BY request_id').all('board-two').map(row => JSON.parse(row.result_json));
  assert.equal(originals.length, 2); assert.equal(originals[0].inputDigest, originals[1].inputDigest);
  assert.notEqual(originals[0].requestId, originals[1].requestId); assert.notEqual(originals[0].contextAfter.mutationRef, originals[1].contextAfter.mutationRef);
  phase = 'create'; receiptMode = 'metadata-forged'; await assert.rejects(turn()); receiptMode = null;
  assert.equal(invokes, 3); assert.equal(a.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items WHERE resource_id=?').get('board-two').n, 3,
    'committed write remains durable, but forged contextAfter never authorizes an agent answer');
  realm = b; phase = 'create'; const book = await turn();
  assert.equal(book.evidence[0].data.completionVerified, true); assert.equal(book.evidence[0].data.receipt.artifacts[0].type, 'library-book');
  assert.equal(created.evidence[0].data.receipt.artifacts[0].type, 'ordinary-item');
  assert.equal(b.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items WHERE resource_id=?').get('library-two').n, 1);
  realm = a;
  for (const mode of ['wrong-request', 'wrong-digest', 'sibling-artifact', 'undeclared-effect']) {
    receiptMode = mode; const beforeWrites = invokes;
    await assert.rejects(turn()); assert.equal(invokes, beforeWrites + 1, 'one real write before invalid proof; never repeated');
  }
  receiptMode = null; const finalWrites = invokes;
  phase = 'context'; onModel = () => select(a, 'board-one'); await assert.rejects(turn());
  assert.equal(invokes, finalWrites, 'switch during model cannot dispatch a write or return old context');
  // Native revoke is observed after a model round even while Root access remains.
  onModel = () => a.instance.store.revokeMembership('board-one', 'native-participant'); await assert.rejects(turn());
  realm = b; onModel = () => f.reader.client.extension('apps.scoped.close', { appId: b.appId, handle: b.lastLaunch.scopedCloseHandle }); await assert.rejects(turn());
  await assert.rejects(f.foreign.client.extension('agent.turn', { requestId: 'workspace_foreign_actor_0001', prompt: 'Read it', selection: { appId: a.appId, handle: a.lastLaunch.scopedCloseHandle } }));
  for (const ref of refs) { assert.equal(JSON.stringify(requests).includes(ref), false); assert.equal(JSON.stringify(created).includes(ref), false); }
  assert.equal(JSON.stringify(requests).includes('contextAfter'), false); assert.equal(JSON.stringify(requests).includes('"generation"'), false);
  await a.restart();
  assert.equal(a.instance.store.db.prepare('SELECT result_json FROM native_receipts WHERE resource_id=? AND request_id=?').get('board-two', JSON.parse(durable).requestId).result_json, durable, 'exact Native receipt survives reload');
  assert.equal(caps.external.get({ actor: actor(entry(a, 'items.create')), invocationId: knownInvocationId }).invocation.receipt.artifacts[0].sha256, created.evidence[0].data.receipt.artifacts[0].sha256);
});

test('one installed namespace serves two Root people with separate Native children and no profile change', { timeout: 120000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, workspaceOtherActor: true, sourceProfile: STANDARD_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)) });
  await f.grantBrowser(f.foreign.account.accountId);
  const a = f.realms[0], profile = JSON.stringify(a.profile), root = f.root(); let sequence = 0;
  const query = input => f.wire(a.embedded + '/api/embed/query', { body: { requestId: 'workspace_people_' + (++sequence), input } });
  await a.launch(); await f.login(a);
  assert.equal((await f.wire(a.embedded + '/api/embed/session-continue', { body: { requestId: 'workspace_person_one_ready' } })).status, 200);
  const first = await query({ operation: 'soty.current_context' }); assert.equal(first.status, 200); assert.equal(first.value.data.title, 'board-one');
  assert.equal((await query({ operation: 'soty.context.select', nativeId: 'board-private-other', generation: 1 })).status, 403);
  await a.launch(f.foreign); const login = await f.authorize(a, f.foreign, a.otherNativeToken); await f.complete(login.callbackUrl);
  assert.equal((await f.wire(a.embedded + '/api/embed/session-continue', { body: { requestId: 'workspace_person_two_ready' } })).status, 200);
  const second = await query({ operation: 'soty.current_context' }); assert.equal(second.status, 200); assert.equal(second.value.data.title, 'board-private-other');
  assert.notEqual(second.value.data.context.ref, first.value.data.context.ref);
  assert.equal((await query({ operation: 'soty.context.select', nativeId: 'board-one', generation: 1 })).status, 403);
  const foreignRef = await f.wire(a.embedded + '/api/embed/query', { body: { requestId: 'workspace_foreign_ref_0001', input: { operation: 'items.list' }, contextRef: first.value.data.context.ref } });
  assert.equal(foreignRef.status, 403);
  assert.equal(JSON.stringify(a.profile), profile); assert.equal(f.root(), root, 'same unchanged installed Root');
});
