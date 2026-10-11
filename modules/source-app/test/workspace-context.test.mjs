import test from 'node:test';
import assert from 'node:assert/strict';
import { createWorkspaceContextAuthority, workspaceMutationRef, workspaceWitness } from '../server/workspace-context.mjs';
import { createNativeAuthorityRuntime, createSourceNativeAuthorityPort } from '../server/native-authority.mjs';
import { STANDARD_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { digest } from '../server/wire.mjs';
import { createSelectedNativeSourceApplication, bindSelectedNativeSourceAdapter } from '../../../server/selected-native-source.js';

const binding = () => ({ identity: { issuer: 'issuer', subject: 'subject' }, rootPrincipal: { accountId: 'a', deviceId: 'd' },
  humanPrincipal: { subject: 'subject' }, resource: { selection: { kind: 'soty.workspace.v1', nativeId: 'workspace', incarnationId: 'one' } },
  semanticDigest: 'd', sessionIdHash: 'a'.repeat(64), rootReferenceDigest: 'b'.repeat(64), rootExpiresAt: 5000 });
const projection = () => ({ key: 'first', generation: 1, title: 'First', role: 'editor', revision: 1, expiresAt: 4000 });
test('workspace references bind slot/actor/session/anchor; ABA, expiry, invalidation and capacity are bounded', () => {
  let now = 1000; const leases = createWorkspaceContextAuthority({ clock: () => now }), b = binding(), p = projection();
  const original = leases.read(b, p); assert.equal(original.context.expiresAt, 4000); assert.equal(leases.read(b, p).context.ref, original.context.ref);
  for (const foreign of [{ ...b, rootReferenceDigest: 'c'.repeat(64) }, { ...b, sessionIdHash: 'c'.repeat(64) },
    { ...b, rootPrincipal: { ...b.rootPrincipal, deviceId: 'other' } }, { ...b, resource: { selection: { ...b.resource.selection, nativeId: 'other' } } }])
    assert.throws(() => leases.assert(foreign, p, original.context.ref));
  leases.read(b, { ...p, key: 'second', generation: 2 }); const aba = leases.read(b, { ...p, generation: 3 });
  assert.notEqual(aba.context.ref, original.context.ref); assert.throws(() => leases.assert(b, p, original.context.ref));
  now = 4000; assert.throws(() => leases.assert(b, p, aba.context.ref));
  now = 1000; for (let n = 0; n < 128; n++) leases.read({ ...b, rootReferenceDigest: digest(n) }, p);
  assert.throws(() => leases.read({ ...b, rootReferenceDigest: digest('overflow') }, p)); leases.close();
  assert.throws(() => leases.assert(b, p, aba.context.ref));
});

test('Native contextAfter can only be produced inside original commit and exact readProof intent', async () => {
  const b = binding(), state = projection(); let saved, finalPort, active = true;
  const port = createSourceNativeAuthorityPort({ capture: () => ({}), withCurrent(_proof, _binding, action) { assert.ok(active); return action(); },
    currentContext: () => ({ ...state }), read: () => ({}),
    execute(_proof, _binding, args, final) { finalPort = final; return final.commit(() => {
      state.revision++; saved = { requestId: args.requestId, inputDigest: digest(args.input), outcome: 'committed', receipt: { id: 'item', revision: 1 }, contextAfter: final.contextAfter(),
        workspaceReceipt: { schema: 'soty.workspace-receipt.v1', effects: [{ kind: 'created', resourceType: 'item', resourceId: 'item', revision: 1 }],
          receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'item', id: 'item', revision: 1 }] } } }; return saved;
    }); }, readProof: () => saved });
  const runtime = createNativeAuthorityRuntime(port, { sourceProfile: STANDARD_WORKSPACE_SOURCE, clock: () => 1000 });
  const proof = await runtime.capture(b), current = await runtime.call(proof, 'read', { requestId: 'workspace_current_0001', input: { operation: 'soty.current_context' } });
  const args = { requestId: 'workspace_mutation_0001', input: { operation: 'create' }, contextRef: current.context.ref };
  await runtime.call(proof, 'execute', args); assert.equal(saved.contextAfter.revision, 2); assert.throws(() => finalPort.contextAfter());
  const receiptArgs = { ...args, input: { inputDigest: digest(args.input) } };
  assert.deepEqual(await runtime.call(proof, 'readProof', receiptArgs), saved);
  await assert.rejects(runtime.call(proof, 'readProof', { ...receiptArgs, requestId: 'workspace_mutation_0002' }));
  assert.notEqual(workspaceMutationRef(args.requestId, digest(args.input), args.contextRef), workspaceMutationRef('workspace_mutation_0002', digest(args.input), args.contextRef));
  const authentic = saved.contextAfter; saved = { ...saved, contextAfter: { ...authentic, mutationRef: '0'.repeat(64) } };
  await assert.rejects(runtime.call(proof, 'readProof', receiptArgs)); saved = { ...saved, contextAfter: authentic };
  state.key = 'second'; state.generation++; await assert.rejects(runtime.call(proof, 'execute', args));
  active = false; await assert.rejects(runtime.call(proof, 'read', { requestId: 'workspace_current_0002', input: { operation: 'soty.current_context' } }));
  runtime.close();
});

test('domain-read context witness commits original request, source/anchor and exact effect artifact', () => {
  const after = { schema: 'soty.context-after.v1', mutationRef: 'a'.repeat(64), generation: 2, title: 'Project', role: 'editor', revision: 7 };
  const input = { after, artifact: { type: 'item', id: 'one', revision: 1 }, requestId: 'workspace_mutation_0001', inputDigest: 'b'.repeat(64), scope: { resource: 'workspace-A' } };
  const original = workspaceWitness(input);
  for (const changed of [{ ...input, requestId: 'workspace_mutation_0002' }, { ...input, artifact: { ...input.artifact, id: 'two' } },
    { ...input, scope: { resource: 'workspace-B' } }, { ...input, after: { ...after, revision: 8 } }, { ...input, after: { ...after, generation: 3 } }])
    assert.notEqual(workspaceWitness(changed), original);
  assert.throws(() => workspaceWitness({ ...input, after: { ...after, url: 'https://not-authority.invalid' } }));
});

test('late execute cannot enter COMMIT after child switch, expiry, revoke or close', async () => {
  for (const mode of ['switch', 'expiry', 'revoke', 'close']) {
    const b = binding(), state = projection(); let active = true, writes = 0, release, entered, now = 1000;
    const waiting = new Promise(done => { entered = done; }), hold = new Promise(done => { release = done; });
    const runtime = createNativeAuthorityRuntime(createSourceNativeAuthorityPort({ capture: () => ({}),
      withCurrent(_proof, _binding, action) { assert.ok(active); return action(); }, currentContext: () => ({ ...state }), read: () => ({}),
      async execute(_proof, _binding, _args, final) { entered(); await hold; return final.commit(() => { writes++; return {}; }); }, readProof: () => ({})
    }), { sourceProfile: STANDARD_WORKSPACE_SOURCE, clock: () => now });
    const proof = await runtime.capture(b), current = await runtime.call(proof, 'read', { requestId: 'workspace_pending_context', input: { operation: 'soty.current_context' } });
    const pending = runtime.call(proof, 'execute', { requestId: 'workspace_pending_write', input: { operation: 'create' }, contextRef: current.context.ref });
    await waiting;
    if (mode === 'switch') { state.key = 'other'; state.generation++; }
    if (mode === 'expiry') now = 4000;
    if (mode === 'revoke') active = false;
    if (mode === 'close') runtime.close();
    release(); await assert.rejects(pending); assert.equal(writes, 0); runtime.close();
  }
});

test('workspace grant cannot reuse fixed-resource scope or expose UI selection to an agent', async () => {
  const appId = 'app-' + 'a'.repeat(32), resource = `workspace:${appId}:realm`, scope = {
    appId, target: { revision: 1, digest: 'b'.repeat(64) }, sourceProfile: STANDARD_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', environmentId: 'test', tenantId: 'publisher', appId, resourceId: 'realm',
      selection: { kind: 'soty.workspace.v1', nativeId: 'namespace', incarnationId: 'one' } } };
  const catalog = { capabilityId: 'app.' + appId + ':query', version: 1, appId, title: 'Native query', description: 'Synthetic', visibility: 'private', executionEnabled: true,
    resources: [resource], recipients: [resource], effects: [], inputSchema: { type: 'object', additionalProperties: false, required: ['operation'],
      properties: { operation: { type: 'string', maxLength: 40 } } }, outputSchema: { type: 'object', additionalProperties: false, properties: {} } };
  assert.throws(() => createSelectedNativeSourceApplication({ scope, catalog: { ...catalog, resources: ['selection:realm'] } }));
  assert.throws(() => createSelectedNativeSourceApplication({ scope: { ...scope, resource: { ...scope.resource, selection: { ...scope.resource.selection, kind: 'soty.resource.v1' } } }, catalog }));
  let dispatches = 0; const app = createSelectedNativeSourceApplication({ scope, catalog });
  bindSelectedNativeSourceAdapter(app.adapter, { appId, target: scope.target, withOwner: (_request, action) => action({}),
    apps: () => ({ callScopedNativeSource() { dispatches++; } }) });
  const input = { handle: 'h'.repeat(43), contextRef: 'c'.repeat(43), input: { operation: 'soty.context.select' } };
  await assert.rejects(app.adapter.query({ requestId: 'workspace_agent_select_0001', input, inputDigest: digest(input) }, new AbortController().signal, () => true));
  assert.equal(dispatches, 0);
});
