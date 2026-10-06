import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, access, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { createPlannerEffectAdapter } from '../server/planner-effect-adapter.mjs';
import { plannerToolsDigest } from '../server/planner-adapter.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { canonicalHash } from '../server/validation.mjs';
import { fixtureDocumentation } from './support/documentation.mjs';

const sourceRoot = resolve(
  process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT ||
    'D:/соты/output/planner-universal-integration/worktree',
);
let actualSource,
  available = true;
try {
  await access(join(sourceRoot, 'server/agent-receipts.ts'));
  await access(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs'));
} catch {
  available = false;
  if (process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT)
    throw new Error('Configured actual Planner proof source unavailable.');
}
if (available) {
  const { tsImport } = await import(
    pathToFileURL(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs')).href
  );
  actualSource = await tsImport(
    pathToFileURL(join(sourceRoot, 'server/main.ts')).href,
    import.meta.url,
  );
}
const actualTest = (name, run) =>
  test(
    name,
    {
      skip: available
        ? false
        : 'Actual selected-workspace Planner source not installed; configure SOTY_PLANNER_PROOF_SOURCE_ROOT.',
    },
    run,
  );

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-planner-effect-')),
    sourceDb = join(directory, 'planner.sqlite'),
    rootDb = join(directory, 'capabilities.sqlite');
  let planner,
    root,
    origin,
    port = 0,
    sourceAllowed = true,
    rootActive = true,
    dropAck = false,
    proofDown = false,
    writes = 0,
    reads = 0,
    holdHelp = false,
    releaseHelp = null,
    helpReached = null;
  t.after(async () => {
    root?.close();
    if (planner) {
      planner.server.closeAllConnections();
      await planner.close();
    }
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-planner-effect-/);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  async function start() {
    planner = await actualSource.createPlannerServer({
      dbPath: sourceDb,
      port,
      host: '127.0.0.1',
      scheduler: false,
    });
    port = await planner.listen();
    origin = `http://127.0.0.1:${port}`;
  }
  await start();
  async function http(path, body, token, method = body === undefined ? 'GET' : 'POST') {
    const r = await fetch(origin + path, {
      method,
      redirect: 'error',
      headers: {
        origin,
        connection: 'close',
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(token ? { authorization: 'Bearer ' + token } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    return { status: r.status, data: await r.json() };
  }
  const created = await http('/api/agent/call', {
    name: 'planner_apply',
    arguments: {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'workspaces',
          key: 'workspace',
          data: { name: 'Synthetic external coordinator', timezone: 'UTC' },
        },
      ],
    },
  });
  assert.equal(created.status, 200);
  const workspaceId = created.data.refs.workspace,
    sourceActorId = planner.store.localUser().id;
  const writer = (
    await http('/api/agent/keys', {
      name: 'Synthetic effect writer',
      workspaceIds: [workspaceId],
      readOnly: false,
      expiresInDays: 1,
    })
  ).data;
  const reader = (
    await http('/api/agent/keys', {
      name: 'Synthetic effect proof',
      workspaceIds: [workspaceId],
      readOnly: true,
      expiresInDays: 1,
    })
  ).data;
  const proofProfile = (await http('/api/agent/receipts/profile', undefined, reader.token)).data;
  const expectedToolsDigest = plannerToolsDigest(
    (await http('/api/agent/tools', undefined, writer.token)).data.tools,
  );
  const owner = { accountId: 'account_a', deviceId: 'device_a' },
    resourceId = 'app.planner:synthetic-workspace';
  const resource = {
    registryId: 'soty',
    tenantId: owner.accountId,
    appId: 'planner',
    environmentId: 'fixture',
    resourceId,
    workspaceId,
    sourceActorId,
  };
  const options = {
    origin,
    resource,
    sourceScope: { workspaceIds: [workspaceId], readOnly: false, keyId: writer.id },
    sourceRelease: {
      id: 'planner.selected-workspace-proof',
      version: 1,
      digest: canonicalHash({
        protocol: proofProfile.protocol,
        release: 'synthetic actual source fixture',
      }),
    },
    expectedToolsDigest,
    expectedProofDigest: proofProfile.digest,
    bindingId: 'app.planner:synthetic-binding',
    allowLoopback: true,
    withAuthority(request, selected, callback) {
      assert.equal(sourceAllowed, true);
      assert.deepEqual(selected.resource, resource);
      assert.equal((request.authorization ?? request.actor).accountId, owner.accountId);
      return callback();
    },
    resolveCredential: async (request, { purpose }) => {
      const key = purpose === 'execute' ? writer : reader;
      return {
        sotyAccountId: request.authorization.accountId,
        sourceActorId,
        workspaceIds: [workspaceId],
        readOnly: key.readOnly,
        keyId: key.id,
        expiresAt: key.expiresAt,
        token: key.token,
      };
    },
    assertDestination: async (target) => target === origin,
    fetch: async (url, init) => {
      const body = init.body ? JSON.parse(init.body) : null;
      if (body?.name === 'planner_apply') writes++;
      if (url.endsWith('/lookup')) {
        reads++;
        if (proofDown) throw new Error('synthetic unavailable');
      }
      const result = await fetch(url, init);
      if (body?.name === 'planner_help' && holdHelp) {
        helpReached?.();
        await new Promise((done) => {
          releaseHelp = done;
        });
      }
      if (body?.name === 'planner_apply' && dropAck) {
        proofDown = true;
        throw new Error('synthetic lost ACK after source commit');
      }
      return result;
    },
  };
  const effect = createPlannerEffectAdapter(options),
    cap = {
      capabilityId: 'app.planner:createWorkItem',
      version: 1,
      appId: 'planner',
      title: 'Создать объект без даты',
      description: 'Синтетический actual-source gate.',
      visibility: 'private',
      executionEnabled: true,
      inputSchema: {
        type: 'object',
        properties: { title: { type: 'string', maxLength: 500 } },
        required: ['title'],
        additionalProperties: false,
      },
      outputSchema: {
        type: 'object',
        properties: {
          objectId: { type: 'string', maxLength: 160 },
          revision: { type: 'integer', minimum: 0 },
        },
        required: ['objectId', 'revision'],
        additionalProperties: false,
      },
      resources: [resourceId],
      effects: ['create'],
      recipients: [resourceId],
      executionBinding: {
        kind: 'registered',
        handler: 'app.planner:createWorkItem',
        version: 1,
        binding: effect.binding,
      },
    };
  const reference = {
    capabilityId: cap.capabilityId,
    version: cap.version,
    digest: createCatalog([cap]).get(cap.capabilityId, cap.version).digest,
  };
  const open = () =>
    createCapabilitiesService({
      databasePath: rootDb,
      projectId: 'actual-planner-fixture',
      actorActive: (a) =>
        rootActive && a.accountId === owner.accountId && a.deviceId === owner.deviceId,
      catalog: [cap],
      documentation: fixtureDocumentation([cap]),
      externalAdapters: [{ contract: reference, adapter: effect.adapter }],
    });
  root = open();
  const call = (op, args = {}) =>
    root.execute({
      op: 'access.' + op,
      actor: owner,
      args: { expectedAccountId: owner.accountId, ...args },
    });
  const principal = call('principals.create', { label: 'Synthetic agent' }).principal;
  const grant = call('grants.issue', {
    principalId: principal.id,
    capabilities: [{ capabilityId: cap.capabilityId, version: 1 }],
    resources: cap.resources,
    effects: cap.effects,
    recipients: cap.recipients,
    expiresAt: Date.now() + 600000,
    allowDelegation: false,
    maxDepth: 0,
    budget: { unit: 'invocations', limit: 3 },
  }).grant;
  const credential = call('credentials.issue', {
    grantId: grant.id,
    audience: 'https://soty.fixture',
  });
  const actor = () =>
    root.authenticateCredential({ token: credential.token, audience: 'https://soty.fixture' });
  const invocation = () => ({
    actor: actor(),
    reference,
    idempotencyKey: 'synthetic_work_item_0001',
    input: { title: 'Синтетический объект без даты' },
  });
  return {
    effect,
    options,
    resource,
    reference,
    cap,
    grant,
    call,
    invocation,
    http,
    writer,
    reader,
    get root() {
      return root;
    },
    get planner() {
      return planner;
    },
    get counts() {
      return { writes, reads };
    },
    failAck() {
      dropAck = true;
    },
    restoreProof() {
      proofDown = false;
    },
    revokeRoot() {
      rootActive = false;
    },
    revokeAuthority() {
      sourceAllowed = false;
    },
    pauseHelp() {
      holdHelp = true;
      return new Promise((done) => {
        helpReached = done;
      });
    },
    releaseHelp() {
      releaseHelp?.();
    },
    async restart() {
      root.close();
      planner.server.closeAllConnections();
      await planner.close();
      planner = undefined;
      await start();
      root = open();
    },
  };
}

actualTest(
  'actual Planner plus real Caps ledger commits one undated object and preserves replay across both restarts',
  async (t) => {
    const f = await fixture(t),
      first = await f.root.external.invoke(f.invocation());
    assert.equal(first.invocation.status, 'succeeded');
    assert.equal(first.invocation.receipt.verificationMethod, 'domain_read');
    const id = first.invocation.effects[0].resourceId,
      entity = f.planner.store.read().entities.find((e) => e.id === id);
    assert.equal(entity.workspaceId, f.resource.workspaceId);
    assert.equal(entity.kind, 'note');
    assert.equal(entity.plan.start, null);
    assert.equal(entity.plan.end, null);
    await f.restart();
    const repeated = await f.root.external.invoke(f.invocation());
    assert.equal(repeated.reused, true);
    assert.equal(repeated.invocation.invocationId, first.invocation.invocationId);
    assert.equal(f.counts.writes, 1);
    assert.equal(f.planner.store.read().entities.filter((e) => e.id === id).length, 1);
    assert.equal(JSON.stringify(repeated).includes('Синтетический объект без даты'), false);
    await assert.rejects(
      f.root.external.invoke({ ...f.invocation(), input: { title: 'Different intended object' } }),
      /invocation_request_conflict/u,
    );
  },
);

actualTest(
  'lost source ACK recovery reads real historical proof and never executes after source write-key revoke',
  async (t) => {
    const f = await fixture(t);
    f.failAck();
    const pending = await f.root.external.invoke(f.invocation());
    assert.equal(pending.invocation.status, 'execution_uncertain');
    assert.equal(pending.invocation.effectState, 'unknown');
    assert.equal(f.counts.writes, 1);
    await f.restart();
    assert.equal(
      (await f.http('/api/agent/keys/' + f.writer.id, undefined, undefined, 'DELETE')).status,
      200,
    );
    f.restoreProof();
    const recovered = await f.root.external.reconcile({
      reference: f.reference,
      invocationId: pending.invocation.invocationId,
    });
    assert.equal(recovered.outcome, 'committed');
    const result = f.root.external.get({
      actor: f.invocation().actor,
      invocationId: pending.invocation.invocationId,
    });
    assert.equal(result.invocation.status, 'succeeded');
    assert.equal(f.counts.writes, 1);
    assert.equal(
      f.planner.store.read().entities.filter((e) => e.workspaceId === f.resource.workspaceId)
        .length,
      1,
    );
  },
);

actualTest('revoked source proof key remains unknown; recovery never writes', async (t) => {
  const f = await fixture(t);
  f.failAck();
  const pending = await f.root.external.invoke(f.invocation());
  f.restoreProof();
  assert.equal(
    (await f.http('/api/agent/keys/' + f.reader.id, undefined, undefined, 'DELETE')).status,
    200,
  );
  const recovered = await f.root.external.reconcile({
    reference: f.reference,
    invocationId: pending.invocation.invocationId,
  });
  assert.equal(recovered.outcome, 'held');
  assert.equal(f.counts.writes, 1);
  assert.equal(
    f.root.external.get({
      actor: f.invocation().actor,
      invocationId: pending.invocation.invocationId,
    }).invocation.status,
    'execution_uncertain',
  );
});

actualTest(
  'binding commits approved source scope/protocol/release; key rotation requires a different contract pin',
  async (t) => {
    const f = await fixture(t),
      next = createPlannerEffectAdapter({
        ...f.options,
        sourceScope: { ...f.options.sourceScope, keyId: 'rotated-source-key' },
      });
    assert.notEqual(f.effect.binding.digest, next.binding.digest);
    const request = {
      requestId: 'synthetic_direct_request',
      inputDigest: canonicalHash({ title: 'Work' }),
      input: { title: 'Work' },
      authorization: {
        accountId: 'account_a',
        resources: [f.resource.resourceId],
        effects: ['create'],
        recipients: [f.resource.resourceId],
      },
    };
    await assert.rejects(
      next.adapter.execute(request, AbortSignal.timeout(8000)),
      /planner_source_scope_changed/u,
    );
    f.revokeAuthority();
    await assert.rejects(f.root.external.invoke(f.invocation()));
    assert.equal(f.counts.writes, 0);
  },
);

actualTest(
  'fresh synchronous source-app authority fence after actual discovery awaits prevents revoked mutation send',
  async (t) => {
    const f = await fixture(t),
      reached = f.pauseHelp();
    const pending = f.root.external.invoke(f.invocation());
    pending.catch(() => {});
    await reached;
    f.revokeAuthority();
    f.releaseHelp();
    await assert.rejects(pending);
    assert.equal(f.counts.writes, 0);
    assert.equal(
      f.planner.store.read().entities.filter((e) => e.workspaceId === f.resource.workspaceId)
        .length,
      0,
    );
  },
);

actualTest(
  'actual source principal is checked before effect; host lease cannot mislabel another source actor',
  async (t) => {
    const f = await fixture(t),
      foreign = { id: 'synthetic_source_foreign', displayName: 'Synthetic other source actor' };
    f.planner.store.transaction((state) => {
      state.users.push(foreign);
      state.memberships.push({
        userId: foreign.id,
        workspaceId: f.resource.workspaceId,
        role: 'owner',
      });
    });
    const { tsImport } = await import(
      pathToFileURL(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs')).href
    );
    const { AgentKeys } = await tsImport(
      pathToFileURL(join(sourceRoot, 'server/agent-auth.ts')).href,
      import.meta.url,
    );
    const foreignKey = new AgentKeys(f.planner.store).create(foreign, {
      name: 'Synthetic wrong actor key',
      workspaceIds: [f.resource.workspaceId],
      readOnly: false,
      expiresInDays: 1,
    });
    const wrong = createPlannerEffectAdapter({
      ...f.options,
      resolveCredential: async (request) => ({
        sotyAccountId: request.authorization.accountId,
        sourceActorId: f.resource.sourceActorId,
        workspaceIds: [f.resource.workspaceId],
        readOnly: false,
        keyId: f.writer.id,
        expiresAt: foreignKey.expiresAt,
        token: foreignKey.token,
      }),
    });
    const request = {
      requestId: 'synthetic_wrong_source_actor',
      inputDigest: canonicalHash({ title: 'Wrong actor' }),
      input: { title: 'Wrong actor' },
      authorization: {
        accountId: 'account_a',
        resources: [f.resource.resourceId],
        effects: ['create'],
        recipients: [f.resource.resourceId],
      },
    };
    await assert.rejects(
      wrong.adapter.execute(request, AbortSignal.timeout(8000)),
      /planner_source_actor_mismatch/u,
    );
    assert.equal(f.counts.writes, 0);
  },
);
