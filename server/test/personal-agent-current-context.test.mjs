import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { HIVE_SELECTED_SOURCE } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../../modules/source-app/server/standard-profile.mjs';
import { createCurrentContextSourceApplication } from '../current-context-source.js';
import { captureExternalApplications, composeExternalApplications } from '../external-applications.js';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { createPersonalAgentConversation } from '../../src/platform/personal-agent-client.ts';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';

const ORIGIN = 'https://context-component.test', appId = 'app-' + 'a'.repeat(32), handle = 'H'.repeat(43);
const scope = { appId, target: { revision: 1, digest: 'b'.repeat(64) }, sourceProfile: HIVE_SELECTED_SOURCE,
  resource: { registryId: 'REG.soty', tenantId: 'fixture_tenant', appId, environmentId: 'test', resourceId: 'selected',
    selection: { kind: 'hive.project.v1', nativeId: 'fixture_project', incarnationId: 'fixture_incarnation' } } };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
function identity() { const key = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: key.privateKey, publicJwk: key.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: key.publicKey.export({ format: 'jwk' }) }; }

async function fixture(t, { installed = true, grant = true, role = 'viewer', metadata = true, standard = false } = {}) {
  const selectedScope = standard ? { ...scope, sourceProfile: STANDARD_SELECTED_SOURCE_V3,
    resource: { ...scope.resource, selection: { ...scope.resource.selection, kind: 'soty.resource.v1' } } } : scope;
  let connect, capabilities, personal, owner, credential, grantId, heldModel, heldNative;
  const receipts = new Map(), modelBodies = [], nativeReads = [], state = { nativeAllowed: true, title: 'Bounded selected project', revision: 7, ready: true, closed: false, expiresAt: Date.now() + 300000, project: 'fixture_project' };
  // Synthetic Native transport only. Signed Connect, Source credential/grant,
  // readonly coordinator, adapter, JSON, runtime, Notes and browser are actual.
  const current = (actor, selectedHandle) => {
    assert.ok(connect.isActorActive(actor)); assert.equal(actor.accountId, owner.accountId); assert.equal(actor.deviceId, owner.deviceId);
    assert.equal(selectedHandle, handle); assert.equal(state.ready, true); assert.equal(state.closed, false); assert.ok(state.expiresAt > Date.now());
    assert.equal(state.project, scope.resource.selection.nativeId);
  };
  const apps = {
    withAppAuthority({ actor, appId: selectedApp }, callback) { current(actor, handle); assert.equal(selectedApp, appId); return callback({ target: scope.target }); },
    withScopedContextSourceAuthority({ owner: actor, scope: selectedScope, handle: selectedHandle }, callback) {
      current(actor, selectedHandle); assert.equal(selectedScope.appId, appId); return callback({ ...scope, expiresAt: state.expiresAt });
    },
    async callScopedContextSource({ scope: requestedScope, handle: selectedHandle, signal, withCurrentOwner }) {
      assert.deepEqual(requestedScope, selectedScope);
      withCurrentOwner(actor => current(actor, selectedHandle));
      nativeReads.push(standard ? '/api/embed/query' : '/api/embed/context');
      if (heldNative) { const hold = heldNative; heldNative = null; hold.entered.resolve(); await hold.release.promise; }
      withCurrentOwner(actor => current(actor, selectedHandle)); assert.equal(signal.aborted, false);
      if (!state.nativeAllowed) return { status: 403, data: { error: 'native_access_denied' } };
      if (standard) return { status: 200, data: { ok: true, data: { schema: 'soty.current-context.v1',
        selection: state.nativeSelection ?? { ...selectedScope.resource.selection, nativeId: state.project }, role,
        ...(metadata ? { title: state.title, revision: state.revision } : {}) } } };
      return { status: 200, data: { schema: 'hive.selected-context.v1', version: 1, projectId: state.project,
        incarnationId: scope.resource.selection.incarnationId, ...(metadata ? { title: 'Bounded selected project', revision: state.revision } : {}),
        access: { role, canRead: true, canEdit: role === 'owner' }, account: { id: 'NATIVE_PRIVATE_ACCOUNT' }, privateExtra: 'NATIVE_PRIVATE_CONTENT' } };
    }
  };
  const entries = captureExternalApplications([createCurrentContextSourceApplication(selectedScope)]);
  const notes = createNotesService({ databasePath: ':memory:', projectId: 'soty' });
  capabilities = createCapabilitiesService({ databasePath: ':memory:', projectId: 'soty', documentation: [],
    actorActive: actor => connect.isActorActive(actor), catalog: entries.map(row => row.catalog),
    readonlyQueries: composeExternalApplications(entries, { withConnectFence: callback => connect.withAuthorityFence(callback),
      capabilities: () => capabilities, apps: () => apps }) });
  const keyRef = {}, tenant = 'fixture_context_tenant';
  personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({ notes, apps, capabilities }),
    configuration: { enabled: true, sourceReferences: installed ? entries.map(row => row.contract) : [],
      sourceCredentialFor: () => credential, gateway: { gatewayOrigin: 'https://context-commerce.invalid', billingTenant: tenant, keyRef,
        authorizeActor: ({ actor }) => connect.isActorActive(actor), resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-context-only-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          const body = JSON.parse(init.body); modelBodies.push(body);
          if (heldModel) { const hold = heldModel; heldModel = null; hold.entered.resolve(); await hold.release.promise; }
          const evidence = body.messages.at(-1).role === 'tool' ? JSON.parse(body.messages.at(-1).content).evidence : null;
          const name = evidence ? 'finish_answer' : 'current_context', args = evidence
            ? { answer: 'Confirmed current project', selectedEvidenceIds: evidence.map(row => row.id), claims: [] } : {};
          const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
            input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return new Response(JSON.stringify({ id: 'context_fixture_completion', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls', message: { role: 'assistant',
            tool_calls: [{ id: 'context_fixture_call', type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }), { headers: { 'content-type': 'application/json', 'x-soty-request-id': id } });
        } } } });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [ORIGIN], extensions: [notes, capabilities, personal] });
  t.after(() => { heldModel?.release.resolve(); heldNative?.release.resolve(); personal.close(); capabilities.close(); notes.close(); connect.close(); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  for (const actor of [alice, bob]) actor.account = good(await rpc(actor, 'bootstrap', { label: 'Synthetic context owner', encryptionPublicJwk: actor.encryptionPublicJwk }));
  owner = alice.account;
  if (grant) {
    const principal = good(await rpc(alice, 'access.principals.create', { expectedAccountId: owner.accountId, label: 'Explicit synthetic readonly grant' })).principal;
    const row = entries[0], issued = good(await rpc(alice, 'access.grants.issue', { expectedAccountId: owner.accountId, principalId: principal.id,
      capabilities: [{ capabilityId: row.contract.capabilityId, version: 1 }], resources: row.catalog.resources, effects: [], recipients: row.catalog.recipients,
      expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 10 } })).grant;
    const value = good(await rpc(alice, 'access.credentials.issue', { expectedAccountId: owner.accountId, grantId: issued.id, audience: ORIGIN }));
    credential = { token: value.token, audience: ORIGIN }; grantId = issued.id;
  }
  let selection = { appId, handle }, result;
  const storage = new Map(), ui = createPersonalAgentConversation({ accountId: owner.accountId, idFactory: () => 'context_browser_turn_one',
    readSelection: () => selection, storage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    client: { getLocalState: async () => ({ accountId: owner.accountId }), extension: async (op, args) => {
      result = await rpc(alice, op, args); if (!result.ok) throw Object.assign(new Error(result.error.code), { code: result.error.code }); return result;
    } } });
  return { rpc, alice, bob, state, ui, modelBodies, nativeReads, entries, result: () => result,
    turn: () => ui.turn('Read current project', { appId, name: 'HIVE', path: '/embed', status: 'ready', selection: { appId, handle } }),
    revokeGrant: async () => good(await rpc(alice, 'access.grants.revoke', { expectedAccountId: owner.accountId, grantId })),
    notes: async () => good(await rpc(alice, 'notes.list', { expectedAccountId: owner.accountId })).notes,
    holdNative() { return heldNative = { entered: deferred(), release: deferred() }; },
    holdModel() { return heldModel = { entered: deferred(), release: deferred() }; },
    switchSelection() { selection = { appId, handle: 'N'.repeat(43) }; } };
}

for (const role of ['owner', 'viewer']) test(`actual signed ${role} context read reaches runtime/browser with bounded metadata only`, async t => {
  const f = await fixture(t, { role }); await f.turn();
  assert.equal(f.ui.view().phase, 'complete'); assert.equal((await f.notes()).length, 1); assert.equal(f.nativeReads.length, 3);
  assert.equal(f.modelBodies.length, 2); assert.deepEqual(f.result().evidence[0].data, { title: 'Bounded selected project', role, revision: 7 });
  assert.deepEqual(f.entries[0].catalog.effects, []);
  for (const value of [handle, 'NATIVE_PRIVATE_ACCOUNT', 'NATIVE_PRIVATE_CONTENT', 'fixture_project', 'fixture_incarnation']) {
    assert.equal(JSON.stringify(f.modelBodies).includes(value), false); assert.equal(JSON.stringify(f.result()).includes(value), false);
  }
});

for (const [label, options, damage] of [
  ['old Native response without negotiated metadata', { metadata: false }, () => {}],
  ['missing installation', { installed: false }, () => {}], ['missing grant', { grant: false }, () => {}],
  ['unready session', {}, f => { f.state.ready = false; }], ['expired session', {}, f => { f.state.expiresAt = 0; }],
  ['closed selection', {}, f => { f.state.closed = true; }], ['project switched', {}, f => { f.state.project = 'foreign_project'; }],
]) test(`${label} denies before any paid model dispatch`, async t => {
  const f = await fixture(t, options); damage(f); await assert.rejects(f.turn());
  assert.equal(f.modelBodies.length, 0); assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
});

test('foreign signed actor cannot use another account selection or Source credential', async t => {
  const f = await fixture(t), value = await f.rpc(f.bob, 'agent.turn', { requestId: 'foreign_context_turn', prompt: 'Read', selection: { appId, handle } });
  assert.equal(value.ok, false); assert.equal(f.modelBodies.length, 0); assert.equal(f.nativeReads.length, 0);
});

for (const phase of ['Native', 'Model']) test(`closed selection while ${phase} is pending cannot deliver or persist stale context`, async t => {
  const f = await fixture(t), hold = phase === 'Native' ? f.holdNative() : f.holdModel();
  const pending = f.turn(); const rejected = assert.rejects(pending);
  await hold.entered.promise; f.state.closed = true; hold.release.resolve(); await rejected;
  assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
  const calls = f.modelBodies.length; await assert.rejects(f.turn(), { code: 'agent_reconcile_required' }); assert.equal(f.modelBodies.length, calls);
  if (phase === 'Native') assert.equal(calls, 0);
});

for (const phase of ['before dispatch', 'pending model']) test(`current Source grant revocation ${phase} denies delivery and Notes`, async t => {
  const f = await fixture(t);
  if (phase === 'before dispatch') { await f.revokeGrant(); await assert.rejects(f.turn()); assert.equal(f.modelBodies.length, 0); }
  else { const hold = f.holdModel(), pending = f.turn(), rejected = assert.rejects(pending);
    await hold.entered.promise; await f.revokeGrant(); hold.release.resolve(); await rejected; }
  assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
  const calls = f.modelBodies.length; await assert.rejects(f.turn(), { code: 'agent_reconcile_required' }); assert.equal(f.modelBodies.length, calls);
});

for (const change of ['Native revoke', 'project revision']) test(`${change} during model is freshly denied before tool, Note or answer`, async t => {
  const f = await fixture(t), hold = f.holdModel(), pending = f.turn(), rejected = assert.rejects(pending);
  await hold.entered.promise;
  if (change === 'Native revoke') f.state.nativeAllowed = false; else f.state.revision++;
  hold.release.resolve(); await rejected;
  assert.equal(f.nativeReads.length, 2); assert.equal(f.modelBodies.length, 1);
  assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
  await assert.rejects(f.turn(), { code: 'agent_reconcile_required' }); assert.equal(f.modelBodies.length, 1);
});

test('closing the current selection during post-model Native verification denies late data', async t => {
  const f = await fixture(t), model = f.holdModel(), pending = f.turn(), rejected = assert.rejects(pending);
  await model.entered.promise; const native = f.holdNative(); model.release.resolve();
  await native.entered.promise; f.state.closed = true; native.release.resolve(); await rejected;
  assert.equal(f.modelBodies.length, 1); assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
});

test('standard3 uses the same signed agent/runtime projection without Native identifiers', async t => {
  const f = await fixture(t, { standard: true, role: 'editor' }); await f.turn();
  assert.equal(f.ui.view().phase, 'complete'); assert.equal(f.modelBodies.length, 2);
  assert.deepEqual(f.nativeReads, ['/api/embed/query', '/api/embed/query', '/api/embed/query']);
  assert.deepEqual(f.result().evidence[0].data, { title: 'Bounded selected project', role: 'editor', revision: 7 });
  for (const value of [handle, 'fixture_project', 'fixture_incarnation', 'soty.current_context']) {
    assert.equal(JSON.stringify(f.modelBodies).includes(value), false); assert.equal(JSON.stringify(f.result()).includes(value), false);
  }
});

for (const [label, options, damage] of [
  ['incomplete metadata', { metadata: false }, () => {}], ['missing installation', { installed: false }, () => {}],
  ['missing grant', { grant: false }, () => {}], ['unready', {}, f => { f.state.ready = false; }],
  ['expired', {}, f => { f.state.expiresAt = 0; }], ['closed', {}, f => { f.state.closed = true; }],
  ['switched', {}, f => { f.state.project = 'other'; }],
  ['wrong Native selection', {}, f => { f.state.nativeSelection = { ...scope.resource.selection, kind: 'soty.resource.v1', nativeId: 'other' }; }],
  ['unsupported role', { role: 'admin' }, () => {}],
]) test(`standard3 ${label} denies before model`, async t => {
  const f = await fixture(t, { standard: true, ...options }); damage(f); await assert.rejects(f.turn());
  assert.equal(f.modelBodies.length, 0); assert.equal((await f.notes()).length, 0);
});

for (const change of ['Native revoke', 'metadata revision', 'metadata title', 'close']) test(`standard3 ${change} during model denies its late result`, async t => {
  const f = await fixture(t, { standard: true }), hold = f.holdModel(), pending = f.turn(), rejected = assert.rejects(pending);
  await hold.entered.promise;
  if (change === 'Native revoke') f.state.nativeAllowed = false;
  else if (change === 'close') f.state.closed = true;
  else if (change === 'metadata title') f.state.title = 'Different title';
  else f.state.revision++;
  hold.release.resolve(); await rejected;
  assert.equal(f.modelBodies.length, 1); assert.equal((await f.notes()).length, 0); assert.equal(f.ui.view().answer, undefined);
});
