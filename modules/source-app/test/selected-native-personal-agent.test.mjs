import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { selectedNativeApplications } from './support/selected-native-contracts.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../server/standard-profile.mjs';
import { captureExternalApplications } from '../../../server/external-applications.js';
import { GLM_MODEL } from '../../personal-agent/provider/validation.mjs';

// Actual signed agent/runtime/accounting gate -> capabilities ledger ->
// installed connector HTTP/WS -> maintained BFF -> ordinary Native SQL.
// The model/billing callbacks are synthetic; no provider/network spend.
for (const uncertain of [false, true]) test(`personal agent real Native SQL ${uncertain ? 'unknown → original receipt' : 'create → receipt'} → list without leaking window handle`, { timeout: 120000 }, async t => {
  let entries, selectedRealm, selectedReference, credentials = new Map(), step = 0, phase = 'create', invocationId;
  let hideReceipt = uncertain, invokeCount = 0;
  const requests = [], receipts = new Map(), keyRef = {}, tenant = 'synthetic_selected_native';
  const nativeInput = { operation: 'items.create', title: 'Synthetic personal agent item' };
  const f = await createOrdinaryInstalledFixture(t, { sourceProfile: STANDARD_SELECTED_SOURCE_V3,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    externalApplications(realms) { const rows = realms.flatMap(realm => selectedNativeApplications(realm.profile));
      entries = captureExternalApplications(rows); selectedRealm = realms[0]; return rows; },
    personalAgent() { return { enabled: true, historyEnabled: false, sourceReferences: entries.map(row => row.contract),
      sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
        gatewayOrigin: 'https://synthetic-selected-native.invalid', billingTenant: tenant, keyRef,
        authorizeActor: () => true, resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-only-local-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          const request = JSON.parse(init.body); requests.push(request);
          assert.equal(request.model, GLM_MODEL); assert.equal(request.autoFallback, false);
          let name, args;
          if (request.messages.at(-1).role === 'tool') { name = 'finish_answer'; const evidence = JSON.parse(request.messages.at(-1).content).evidence;
            if (uncertain && phase === 'create') { assert.equal(evidence[0].data.completionVerified, false); assert.equal(evidence[0].data.effectState, 'unknown'); hideReceipt = false; }
            args = { answer: uncertain && phase === 'create' ? 'Native result is unknown; original receipt required' : 'Native item created and read back',
              selectedEvidenceIds: evidence.map(row => row.id), claims: [] }; }
          else if (phase === 'create') { name = 'source_write_1'; args = { reference: selectedReference, input: nativeInput }; }
          else if (phase === 'receipt') { name = 'source_receipt_1'; args = { reference: selectedReference, input: nativeInput, invocationId }; }
          else { name = 'source_read_1'; args = { reference: entries.find(row => row.appId === selectedRealm.appId && row.catalog.capabilityId.endsWith(':items.list')).contract,
            input: { operation: 'items.list' } }; }
          step++; const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
            input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return Response.json({ id: 'synthetic_selected_native', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls',
            message: { role: 'assistant', tool_calls: [{ id: 'synthetic_call_' + step, type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
        } } }; } });
  for (const kind of ['context', 'list', 'create']) {
    const approved = entries.filter(row => kind === 'context' ? row.catalog.capabilityId.endsWith(':current_context') : row.catalog.capabilityId.endsWith(':items.' + kind));
    const principal = (await f.reader.client.extension('access.principals.create', { expectedAccountId: f.reader.account.accountId, label: 'Synthetic personal ' + kind })).principal;
    const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId, principalId: principal.id,
      capabilities: approved.map(row => ({ capabilityId: row.contract.capabilityId, version: 1 })), resources: [...new Set(approved.flatMap(row => row.catalog.resources))],
      effects: kind === 'create' ? ['create'] : [], recipients: [...new Set(approved.flatMap(row => row.catalog.recipients))], expiresAt: Date.now() + 600000,
      budget: { unit: 'invocations', limit: 20 } })).grant;
    const issued = await f.reader.client.extension('access.credentials.issue', { expectedAccountId: f.reader.account.accountId, grantId: grant.id, audience: f.origin });
    for (const row of approved) credentials.set(row.contract.capabilityId, { token: issued.token, audience: f.origin });
  }
  await selectedRealm.launch(); await f.login(selectedRealm);
  const continued = await f.wire(selectedRealm.embedded + '/api/embed/session-continue', { body: { requestId: 'selected-agent-current-ready-001' } });
  assert.equal(continued.status, 200); assert.equal(continued.value.ready, true);
  selectedReference = entries.find(row => row.appId === selectedRealm.appId && row.catalog.capabilityId.endsWith(':items.create')).contract;
  const handle = selectedRealm.lastLaunch.scopedCloseHandle;
  selectedRealm.instance.server.prependListener('request', (req, res) => {
    if (req.url === '/api/embed/invoke') invokeCount++;
    if (req.url !== '/api/embed/receipt') return;
    const end = res.end.bind(res); res.end = (...args) => {
      if (hideReceipt && selectedRealm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n === 1) { res.destroy(); return res; }
      return end(...args);
    };
  });
  assert.equal(f.root().locals.personalAgent.status().configured, true);
  const contextRef = entries.find(row => row.appId === selectedRealm.appId && row.catalog.capabilityId.endsWith(':current_context')).contract;
  const contextActor = f.root().locals.capabilitiesService.authenticateCredential(credentials.get(contextRef.capabilityId));
  const contextRead = await f.root().locals.capabilitiesService.external.query({ actor: contextActor, reference: contextRef,
    idempotencyKey: 'selected_agent_preflight_context_001', input: { handle } });
  assert.equal(contextRead.invocation.status, 'succeeded');
  const turn = suffix => f.reader.client.extension('agent.turn', {
    requestId: 'selected_agent_native_turn_' + suffix, prompt: 'Create and verify my selected Native item', selection: { appId: selectedRealm.appId, handle } });
  const result = await turn('001'); invocationId = result.evidence[0].data.invocationId;
  assert.equal(result.answer, uncertain ? 'Native result is unknown; original receipt required' : 'Native item created and read back');
  phase = 'receipt'; const reconciled = await turn('002'); phase = 'list'; const listed = await turn('003'); assert.equal(step, 6);
  assert.equal(selectedRealm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 1);
  assert.equal(f.realms[1].instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 0);
  assert.equal(result.evidence[0].data.completionVerified, !uncertain); assert.equal(reconciled.evidence[0].data.completionVerified, true);
  assert.equal(invokeCount, 1, 'unknown reconciliation never resends a Native mutation');
  assert.equal(listed.evidence[0].data.items[0].title, nativeInput.title);
  assert.equal(JSON.stringify(requests).includes(handle), false); assert.equal(JSON.stringify(result).includes(handle), false);
  assert.equal(JSON.stringify(requests).includes('synthetic-only-local-no-provider'), false);
  assert.equal(JSON.stringify(requests).includes('inputDigest'), false);
  await selectedRealm.restart();
  const native = await f.wire(selectedRealm.embedded + '/api/embed/query', { body: { requestId: 'selected-native-reloaded-read-001', input: { operation: 'items.list' } } });
  assert.equal(native.status, 200); assert.equal(native.value.data.items.length, 1); assert.equal(native.value.data.items[0].title, nativeInput.title);
});
