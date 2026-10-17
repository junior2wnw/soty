import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { feedbackSourceFixture } from '../../modules/feedback-mcp/test/support/source-fixture.mjs';
import { createFeedbackSourceApplications } from '../../modules/feedback-mcp/installed-catalog.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { createSelectedNativeSourceApplication } from '../selected-native-source.js';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { captureExternalApplications, composeExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';

// Actual signed Root Connect + capability ledger + product-agent composition
// -> current workspace adapter -> maintained Feedback Source/HTTP/SDK + Native
// engine/storage. Apps' session routing port and accounted model are synthetic.
// No live account, grant, provider request or public project is touched.
test('product agent discovers own projects, submits, reads and replies through actual Feedback; lost return/reload/B/revoke preserve one effect', async t => {
  let connect, capabilities, personal, ownerAccountId, clock = Date.now(), operation = 'projects_list', inner,
    selected, invoked = 0, lostReply = true;
  const nativeFailures = [];
  const native = await feedbackSourceFixture(t, { rootAccountFor: subject => subject === 'person-one' ? ownerAccountId ?? 'synthetic-pending' : 'root-' + subject }),
    scope = { appId: native.profile.appId, target: native.profile.target, sourceProfile: native.profile.sourceProfile, resource: native.profile.resource },
    rows = createFeedbackSourceApplications({ createCurrentContextSourceApplication, createSelectedNativeSourceApplication }, scope),
    entries = captureExternalApplications(rows), credentials = new Map(), grants = new Map(), modelBodies = [], receipts = new Map(),
    origin = 'https://feedback-product-agent.fixture.invalid', handle = 'h'.repeat(43), tenant = 'synthetic-feedback-product', keyRef = {},
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
      args.withCurrentOwner(owner);
      const result = await native.request('/api/embed/query', { requestId: args.requestId, input: { operation: 'soty.current_context' } });
      if (result.status !== 200) nativeFailures.push({ status: result.status, code: result.body.error?.code });
      return { status: result.status, data: result.body };
    },
    async callScopedNativeSource(args) {
      args.withCurrentOwner(owner); assert.equal(args.handle, handle);
      if (args.path.endsWith('/invoke')) invoked++;
      const result = await native.request(args.path, { requestId: args.requestId, contextRef: args.contextRef, input: args.input });
      if (args.path.endsWith('/invoke') && args.input.operation === 'ticket_reply' && lostReply && result.status === 200) {
        lostReply = false; throw Error('Synthetic lost reply after actual Feedback COMMIT');
      }
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
        const body = JSON.parse(init.body); modelBodies.push(body); const last = body.messages.at(-1); let name, args;
        if (last.role !== 'tool') { name = 'source_catalog'; args = { query: 'feedback ' + operation, limit: 1 }; }
        else {
          const evidence = JSON.parse(last.content).evidence;
          if (evidence[0]?.data?.items) {
            const row = evidence[0].data.items.find(x => x.reference.capabilityId.endsWith(':feedback.' + operation));
            assert.ok(row); assert.deepEqual(row.inputSchema.properties.operation.enum, [operation]); selected = row.reference;
            name = ['ticket_submit', 'ticket_reply'].includes(operation) ? 'source_write_1' : 'source_read_1';
            args = { reference: selected, input: inner ?? { operation } };
          } else { name = 'finish_answer'; args = { answer: 'Synthetic provider; actual Feedback evidence', selectedEvidenceIds: evidence.map(x => x.id), claims: [] }; }
        }
        const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
          input_tokens: 3, output_tokens: 2, charge_urub: '0' });
        return Response.json({ id, model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls', message: { role: 'assistant', tool_calls: [
          { id: 'synthetic_feedback_call_' + modelBodies.length, type: 'function', function: { name, arguments: JSON.stringify(args) } },
        ] } }], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
      },
    },
  } });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', clock: () => clock, allowedOrigins: [origin], extensions: [notes, capabilities, personal] });
  t.after(() => { personal.close(); capabilities.close(); notes.close(); connect.close(); });
  const good = result => { assert.equal(result.ok, true, JSON.stringify({ code: result.error?.code, operation, nativeFailures })); return result; };
  const client = () => {
    const key = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), publicJwk = key.publicKey.export({ format: 'jwk' });
    return async (op, args = {}) => {
      if (op === 'bootstrap') args = { ...args, encryptionPublicJwk: publicJwk };
      const challenge = good(await connect.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
      return connect.handle({ origin, op, args, proof: { challengeId: challenge.challengeId, publicJwk,
        signature: sign('sha256', Buffer.from(challenge.message), { key: key.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
    };
  };
  const rpc = client(); ownerAccountId = good(await rpc('bootstrap', { label: 'Synthetic Feedback owner' })).accountId;
  native.newContext(); await native.contextRef();
  for (const row of entries) {
    const principal = good(await rpc('access.principals.create', { expectedAccountId: ownerAccountId, label: 'Synthetic Source acceptance' })).principal,
      grant = good(await rpc('access.grants.issue', { expectedAccountId: ownerAccountId, principalId: principal.id,
        capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
        effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: clock + 600000, budget: { unit: 'invocations', limit: 60 } })).grant,
      credential = good(await rpc('access.credentials.issue', { expectedAccountId: ownerAccountId, grantId: grant.id, audience: origin }));
    credentials.set(row.contract.capabilityId, { token: credential.token, audience: origin }); grants.set(row.contract.capabilityId, grant.id);
  }
  const project = native.service().execute(native.ownActor(), 'project_create', { requestId: 'synthetic-existing-project', name: 'Acceptance queue', publicSubmit: true }).project;
  const turn = async (suffix, op, input) => {
    operation = op; inner = input;
    const value = good(await rpc('agent.turn', { requestId: 'feedback_native_product_' + suffix,
      prompt: 'Use my Feedback project as instructed', selection: { appId: scope.appId, handle } }));
    clock += 61000; return value;
  };
  const listed = await turn('list_001', 'projects_list'); assert.equal(listed.evidence[0].data.projects[0].id, project.id);
  const submitted = await turn('submit_002', 'ticket_submit', { operation: 'ticket_submit', projectId: project.id, body: 'Synthetic product report' });
  assert.equal(submitted.evidence[0].data.completionVerified, true); assert.equal(invoked, 1);
  const ownTickets = native.service().execute(native.ownActor(), 'tickets_list', { projectId: project.id }).tickets;
  assert.equal(ownTickets.length, 1);
  const reporter = native.service().guest(project.id).actor,
    external = native.service().execute(reporter, 'ticket_submit', { projectId: project.id, requestId: 'synthetic-external-ticket', body: 'External visitor report to answer' }).ticket,
    queue = await turn('queue_003', 'tickets_list', { operation: 'tickets_list', projectId: project.id }),
    found = queue.evidence[0].data.tickets.filter(x => x.id === external.id);
  assert.equal(found.length, 1); const ticket = found[0];
  const read = await turn('read_003', 'ticket_get', { operation: 'ticket_get', projectId: project.id, ticketId: ticket.id });
  assert.equal(read.evidence[0].data.ticket.body, 'External visitor report to answer');
  const replied = await turn('reply_004', 'ticket_reply', { operation: 'ticket_reply', projectId: project.id, ticketId: ticket.id,
    expectedRevision: read.evidence[0].data.ticket.revision, body: 'Synthetic actual saved owner answer' });
  assert.equal(replied.evidence[0].data.completionVerified, true); assert.equal(lostReply, false); assert.equal(invoked, 2);
  native.restart(); await native.contextRef();
  const reloaded = await turn('reload_005', 'ticket_get', { operation: 'ticket_get', projectId: project.id, ticketId: ticket.id });
  assert.equal(reloaded.evidence[0].data.ticket.messages.filter(x => x.kind === 'support').length, 1, JSON.stringify(reloaded.evidence[0].data.ticket.messages));
  assert.equal(reloaded.evidence[0].data.ticket.messages.find(x => x.kind === 'support').body, 'Synthetic actual saved owner answer');
  const foreign = client(); good(await foreign('bootstrap', { label: 'Synthetic independent owner B' })); const beforeModels = modelBodies.length;
  assert.equal((await foreign('agent.turn', { requestId: 'feedback_foreign_turn', prompt: 'Read A project', selection: { appId: scope.appId, handle } })).ok, false);
  assert.equal(modelBodies.length, beforeModels); assert.equal(invoked, 2);
  native.revoke(); assert.equal((await rpc('agent.turn', { requestId: 'feedback_native_revoked_turn', prompt: 'Read after Native revoke', selection: { appId: scope.appId, handle } })).ok, false);
  assert.equal(invoked, 2);
  const sent = JSON.stringify(modelBodies); assert.equal(sent.includes(handle), false); assert.equal(sent.includes('synthetic-no-external-model'), false);
  for (const credential of credentials.values()) assert.equal(sent.includes(credential.token), false);
});
