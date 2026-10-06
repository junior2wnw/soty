import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, access, rm } from 'node:fs/promises';
import { dirname, join, resolve, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { createPlannerAdapter, plannerToolsDigest } from '../server/planner-adapter.mjs';

// These integration tests use the actual separately owned Planner sources.
// Runtime adapter code imports no external project and changes no source repo.
const sourceRoot = resolve(process.env.SOTY_PLANNER_SOURCE_ROOT || 'D:/планировщик');
const tsApi = join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs');
let sourceFactory,
  available = true;
try {
  await access(join(sourceRoot, 'server/main.ts'));
  await access(tsApi);
} catch {
  available = false;
  if (process.env.SOTY_PLANNER_SOURCE_ROOT)
    throw new Error('Configured Planner source/tsx API is unavailable.');
}
if (available) {
  const { tsImport } = await import(pathToFileURL(tsApi).href);
  sourceFactory = await tsImport(
    pathToFileURL(join(sourceRoot, 'server/main.ts')).href,
    import.meta.url,
  );
}
const sourceTest = (name, run) =>
  test(
    name,
    {
      skip: available
        ? false
        : 'Actual separately owned Planner source is not installed; set SOTY_PLANNER_SOURCE_ROOT.',
    },
    run,
  );
const fails = (code) => (error) => error.code === code;
async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-planner-adapter-')),
    database = join(folder, 'sandbox.sqlite');
  let application;
  const adapters = [];
  t.after(async () => {
    adapters.forEach((value) => value.close());
    if (application) {
      application.server.closeAllConnections();
      await application.close();
    }
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.match(basename(folder), /^soty-planner-adapter-/);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const launch = async () => {
    application = await sourceFactory.createPlannerServer({
      dbPath: database,
      port: 0,
      host: '127.0.0.1',
      scheduler: false,
    });
    return `http://127.0.0.1:${await application.listen()}`;
  };
  let origin = await launch(),
    active = true,
    token,
    key,
    authorityChecks = 0;
  const http = async (path, body, method = body === undefined ? 'GET' : 'POST', credential) => {
    const response = await fetch(origin + path, {
      method,
      redirect: 'error',
      headers: {
        origin,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(credential ? { authorization: 'Bearer ' + credential } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, data: await response.json() };
  };
  const created = await http('/api/agent/call', {
    name: 'planner_apply',
    arguments: {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'workspaces',
          key: 'workspace',
          data: { name: 'Только синтетический Soty sandbox', timezone: 'UTC' },
        },
      ],
    },
  });
  assert.equal(created.status, 200);
  const workspaceId = created.data.refs.workspace,
    sourceActorId = application.store.localUser().id;
  const minted = await http('/api/agent/keys', {
    name: 'Synthetic Soty bridge',
    workspaceIds: [workspaceId],
    readOnly: false,
    expiresInDays: 1,
  });
  assert.equal(minted.status, 201);
  key = minted.data;
  token = key.token;
  const tools = (await http('/api/agent/tools', undefined, 'GET', token)).data.tools;
  const sourcePin = plannerToolsDigest(tools),
    context = Object.freeze({ accountId: 'synthetic_soty_actor' });
  const resource = {
    registryId: 'soty',
    tenantId: 'synthetic_tenant',
    appId: 'synthetic_planner',
    environmentId: 'fixture',
    resourceId: 'synthetic.planner:workspace',
    workspaceId,
    sourceActorId,
  };
  function adapter(patch = {}) {
    const value = createPlannerAdapter({
      origin,
      resource,
      expectedToolsDigest: sourcePin,
      allowLoopback: true,
      authorize: async (received) => {
        authorityChecks++;
        return active && received === context;
      },
      accountId: (received) => received.accountId,
      resolveCredential: async () => ({
        sotyAccountId: context.accountId,
        sourceActorId,
        workspaceIds: [workspaceId],
        readOnly: false,
        expiresAt: key.expiresAt,
        token,
      }),
      assertDestination: async (target) => target === origin,
      ...patch,
    });
    adapters.push(value);
    return value;
  }
  return {
    adapter,
    http,
    context,
    resource,
    sourcePin,
    workspaceId,
    get application() {
      return application;
    },
    get origin() {
      return origin;
    },
    get key() {
      return key;
    },
    get token() {
      return token;
    },
    setToken(value) {
      token = value;
    },
    setActive(value) {
      active = value;
    },
    get checks() {
      return authorityChecks;
    },
    async restart() {
      application.server.closeAllConnections();
      await application.close();
      origin = await launch();
    },
  };
}
sourceTest(
  'actual Planner workspace/object receipt survives source restart and identical replay; unrelated namespace is inaccessible',
  async (t) => {
    const f = await fixture(t),
      bridge = f.adapter();
    const discovered = await bridge.discover(f.context);
    assert.equal(discovered.resourceId, f.resource.resourceId);
    const intent = { requestId: 'soty-synthetic-create-1', title: 'Синтетический объект без даты' };
    const first = await bridge.createWorkItem(f.context, intent),
      read = await bridge.readObjects(f.context);
    assert.equal(read.total, 1);
    assert.equal(read.items[0].id, first.objectId);
    assert.equal(read.items[0].kind, 'note');
    assert.equal(read.items[0].plan.start, null);
    assert.equal(read.items[0].plan.end, null);
    await f.restart();
    const reopened = f.adapter(),
      second = await reopened.createWorkItem(f.context, intent);
    assert.deepEqual(second, first);
    assert.equal((await reopened.readObjects(f.context)).total, 1);
    await assert.rejects(
      () => reopened.createWorkItem(f.context, { ...intent, title: 'Изменённое намерение' }),
      fails('planner_source_idempotency_mismatch'),
    );
    await assert.rejects(
      () => reopened.readObjects(f.context, { workspace: 'another' }),
      fails('planner_adapter_invalid'),
    );
    const foreign = f.application.store
      .read()
      .workspaces.find((value) => value.id !== f.workspaceId);
    const denied = await f.http(
      '/api/agent/call',
      { name: 'planner_read', arguments: { collection: 'objects', workspace: foreign.id } },
      'POST',
      f.token,
    );
    assert.equal(denied.status, 404);
    assert(f.checks >= 12);
  },
);
sourceTest('dropped ACK after real source commit replays one actual durable receipt', async (t) => {
  const f = await fixture(t);
  let lose = true;
  const bridge = f.adapter({
    fetch: async (url, options) => {
      const response = await fetch(url, options);
      if (
        lose &&
        options.body &&
        JSON.parse(options.body).name === 'planner_apply' &&
        response.ok
      ) {
        await response.arrayBuffer();
        lose = false;
        throw new TypeError('synthetic_dropped_ack');
      }
      return response;
    },
  });
  const intent = { requestId: 'soty-lost-ack', title: 'Только синтетический повтор' };
  await assert.rejects(
    () => bridge.createWorkItem(f.context, intent),
    (error) => error.code === 'planner_source_unconfirmed' && error.outcome === 'unknown',
  );
  const committed = await bridge.readObjects(f.context);
  assert.equal(committed.total, 1);
  const receipt = await bridge.createWorkItem(f.context, intent);
  assert.equal(receipt.objectId, committed.items[0].id);
  assert.equal((await bridge.readObjects(f.context)).total, 1);
});
sourceTest(
  'per-call real source key revocation and current memberships deny reads and old receipt replay',
  async (t) => {
    const f = await fixture(t),
      bridge = f.adapter(),
      intent = { requestId: 'soty-before-revoke', title: 'Synthetic source revocation' };
    await bridge.createWorkItem(f.context, intent);
    assert.equal((await f.http('/api/agent/keys/' + f.key.id, undefined, 'DELETE')).status, 200);
    await assert.rejects(
      () => bridge.readObjects(f.context),
      fails('planner_source_unauthenticated'),
    );
    await assert.rejects(
      () => bridge.createWorkItem(f.context, intent),
      fails('planner_source_unauthenticated'),
    );
  },
);
sourceTest(
  'Soty actor/resource fences and source schema pin refuse before effects; no global local-owner fallback',
  async (t) => {
    const f = await fixture(t),
      bridge = f.adapter();
    await assert.rejects(
      () => bridge.readObjects({ accountId: f.context.accountId }),
      fails('planner_access_denied'),
    );
    const mismatched = f.adapter({
      resolveCredential: async () => ({
        sotyAccountId: 'different_soty_actor',
        sourceActorId: f.resource.sourceActorId,
        workspaceIds: [f.workspaceId],
        readOnly: false,
        expiresAt: f.key.expiresAt,
        token: f.token,
      }),
    });
    await assert.rejects(() => mismatched.readObjects(f.context), fails('planner_actor_mismatch'));
    const stale = f.adapter({ expectedToolsDigest: 'f'.repeat(64) });
    await assert.rejects(
      () => stale.createWorkItem(f.context, { requestId: 'stale-schema', title: 'No effect' }),
      fails('planner_schema_changed'),
    );
    f.setToken('plnr_' + 'a'.repeat(43));
    await assert.rejects(
      () => bridge.readObjects(f.context),
      fails('planner_source_unauthenticated'),
    );
    const state = f.application.store.read();
    assert.equal(state.entities.filter((value) => value.workspaceId === f.workspaceId).length, 0);
  },
);
sourceTest(
  'current source ACL after membership removal and post-commit Soty revocation do not disclose/reclassify effects',
  async (t) => {
    const f = await fixture(t),
      bridge = f.adapter({
        fetch: async (url, options) => {
          const response = await fetch(url, options);
          if (options.body && JSON.parse(options.body).name === 'planner_apply' && response.ok)
            f.setActive(false);
          return response;
        },
      });
    await assert.rejects(
      () =>
        bridge.createWorkItem(f.context, {
          requestId: 'source-committed-before-revoke',
          title: 'Synthetic authority race',
        }),
      (error) => error.code === 'planner_authority_changed' && error.outcome === 'unknown',
    );
    const own = f.application.store
      .read()
      .entities.filter((value) => value.workspaceId === f.workspaceId);
    assert.equal(own.length, 1);
    // Actual trusted source store is only this synthetic fixture. Normal adapter paths never access it.
    f.setActive(true);
    f.application.store.transaction((state) => {
      state.memberships = state.memberships.filter((value) => value.workspaceId !== f.workspaceId);
      return true;
    });
    await assert.rejects(
      () => f.adapter().readObjects(f.context),
      fails('planner_source_forbidden'),
    );
  },
);
