import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, mkdir, writeFile, rm, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../../../../server/http-app.js';
import { createClientWithStorage } from '../../../connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../../../scripts/agent-modules/local-apps.mjs';
import { createPlannerReadonlyQueryAdapter } from '../../server/planner-readonly-adapter.mjs';
import { plannerToolsDigest } from '../../server/planner-adapter.mjs';
import { canonicalHash } from '../../server/validation.mjs';
const sourceRoot = resolve(
  process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT ||
    'D:/соты/output/planner-universal-integration/worktree',
);
let source;
export let actualSourceAvailable = true;
try {
  await access(join(sourceRoot, 'server/agent-service.ts'));
  await access(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs'));
} catch {
  actualSourceAvailable = false;
}
if (actualSourceAvailable) {
  const { tsImport } = await import(
    pathToFileURL(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs'))
  );
  source = await tsImport(pathToFileURL(join(sourceRoot, 'server/main.ts')).href, import.meta.url);
} else if (process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT)
  throw new Error('Configured actual Source unavailable.');
const random = () => randomBytes(32).toString('base64url');
const until = async (check) => {
  const deadline = performance.now() + 8000;
  do {
    const value = await check();
    if (value) return value;
    await new Promise((done) => setTimeout(done, 20));
  } while (performance.now() < deadline);
  throw new Error('readonly_fixture_not_ready');
};
function memory() {
  let value;
  return {
    async read() {
      return structuredClone(value ?? null);
    },
    async claim(next) {
      value ??= structuredClone(next);
      return structuredClone(value);
    },
    async compareAndSwap(revision, next) {
      assert.equal(value.localRevision, revision);
      value = structuredClone(next);
      return structuredClone(value);
    },
  };
}
export async function readonlyAppHttpFixture(
  t,
  { writerInstead = false, lieReadOnlyLease = false } = {},
) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-actual-query-')),
    dataDir = join(directory, 'root'),
    dist = join(directory, 'dist');
  await mkdir(dist);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>Synthetic</title>');
  let app, runtime, planner;
  const clients = [];
  const server = createServer((req, res) => (app ? app(req, res) : res.writeHead(503).end()));
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 70000;
  t.after(async () => {
    runtime?.stop();
    clients.forEach((client) => client.dispose());
    server.closeAllConnections();
    await new Promise((done) => server.close(done));
    await app?.locals.closeServices();
    if (planner) {
      planner.server.closeAllConnections();
      await planner.close();
    }
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-actual-query-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  planner = await source.createPlannerServer({
    dbPath: join(directory, 'planner.sqlite'),
    port: 0,
    host: '127.0.0.1',
    scheduler: false,
  });
  const port = await planner.listen(),
    sourceOrigin = 'http://127.0.0.1:' + port;
  async function sourceHttp(path, body, token) {
    const response = await fetch(sourceOrigin + path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: {
        origin: sourceOrigin,
        connection: 'close',
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(token ? { authorization: 'Bearer ' + token } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    return { status: response.status, value: await response.json() };
  }
  const workspace = (
    await sourceHttp('/api/agent/call', {
      name: 'planner_apply',
      arguments: {
        requestId: randomUUID(),
        operations: [
          {
            op: 'create',
            collection: 'workspaces',
            key: 'workspace',
            data: { name: 'Synthetic readonly Source', timezone: 'UTC' },
          },
        ],
      },
    })
  ).value.refs.workspace;
  const seeded = (
    await sourceHttp('/api/agent/call', {
      name: 'planner_apply',
      arguments: {
        requestId: randomUUID(),
        operations: [
          {
            op: 'create',
            collection: 'objects',
            workspace,
            key: 'item',
            data: { title: 'Synthetic private object' },
          },
        ],
      },
    })
  ).value;
  const sourceKey = (
    await sourceHttp('/api/agent/keys', {
      name: 'Synthetic readonly Source key',
      workspaceIds: [workspace],
      readOnly: !writerInstead,
      expiresInDays: 1,
    })
  ).value;
  const profile = (await sourceHttp('/api/agent/receipts/profile', undefined, sourceKey.token))
      .value,
    tools = (await sourceHttp('/api/agent/tools', undefined, sourceKey.token)).value;
  await new Promise((done) => server.listen(0, '127.0.0.1', done));
  const origin = 'http://127.0.0.1:' + server.address().port;
  const base = {
    dataDir,
    connectOrigins: [origin],
    appHosting: {},
    appOriginTemplate: 'http://{appId}.localhost:' + server.address().port,
    namedAppZone: '',
    discoveryOrigin: '',
    capabilityAudience: origin,
  };
  app = createHttpApp(dist, base);
  server.on('upgrade', (req, socket, head) => {
    if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy();
  });
  const ownerClient = createClientWithStorage(
    {
      projectId: 'soty',
      endpoint: origin + '/api/connect/rpc',
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    },
    memory(),
  );
  clients.push(ownerClient);
  const owner = await ownerClient.bootstrap('Synthetic signed Root owner');
  const token = random(),
    identity = {
      linkId: 'query_fixture_link_' + randomUUID(),
      hostDeviceId: 'query_fixture_host',
      connectorId: 'query_fixture_connector',
      name: 'Synthetic Source device',
    };
  const registered = await fetch(origin + '/api/connectors/register', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer ' + token },
    body: JSON.stringify({
      linkId: identity.linkId,
      deviceId: identity.hostDeviceId,
      connectorId: identity.connectorId,
      scope: 'Dev',
      protocol: 2,
      capabilities: ['apps'],
    }),
  });
  assert.equal((await registered.json()).ok, true);
  runtime = createLocalAppsRuntime(
    {
      randomSecret: random,
      digest: (value) => createHash('sha256').update(value).digest('hex'),
      createWebSocket: (url) => new globalThis.WebSocket(url),
      httpRequest: request,
      encodeBase64: (bytes) => Buffer.from(bytes).toString('base64'),
      decodeBase64: (value) => Buffer.from(value, 'base64'),
    },
    { identity, token, serverUrl: origin },
  );
  runtime.start();
  await until(() => runtime.status().connected);
  const claim = await runtime.claim();
  await ownerClient.extension('apps.claim', {
    hostDeviceId: claim.hostDeviceId,
    connectorId: claim.connectorId,
    claimCode: claim.claimCode,
  });
  const registration = await ownerClient.extension('apps.register', {
    hostDeviceId: identity.hostDeviceId,
    connectorId: identity.connectorId,
    name: 'Actual Planner read Source',
    port,
    entryPath: '/',
    grants: { accountIds: [], communityIds: [] },
  });
  const appId = registration.app.id,
    target = registration.universalRegistration.descriptor.app.source,
    resourceId = 'app.' + appId + ':workspace';
  const resource = {
    registryId: 'soty',
    tenantId: owner.accountId,
    appId,
    environmentId: 'fixture',
    resourceId,
    workspaceId: workspace,
    sourceActorId: planner.store.localUser().id,
  };
  let holds = false,
    release,
    entered,
    readCalls = 0,
    profileCalls = 0;
  const gate = () =>
      new Promise((done, reject) => {
        const timer = setTimeout(
          () => reject(new Error('readonly_fixture_query_not_reached')),
          8000,
        );
        entered = () => {
          clearTimeout(timer);
          done();
        };
      }),
    fence = (request) => {
      const key = planner.store.db
        .prepare('SELECT user_id,workspace_ids,read_only,expires_at FROM agent_keys WHERE id=?')
        .get(sourceKey.id);
      // Authority metadata only; do not materialize Source objects inside the
      // Root/App fence. Actual domain reads use the fixed HTTP handler below.
      const membership = planner.store.db
        .prepare(
          "SELECT 1 AS allowed FROM state,json_each(state.document,'$.memberships') membership WHERE state.id=1 AND json_extract(membership.value,'$.userId')=? AND json_extract(membership.value,'$.workspaceId')=? AND json_extract(membership.value,'$.role') IN ('owner','editor','approver','viewer') LIMIT 1",
        )
        .get(key?.user_id ?? '', workspace);
      const selectedWorkspaces = key ? JSON.parse(key.workspace_ids) : [];
      if (
        !key ||
        Date.parse(key.expires_at) <= Date.now() ||
        !membership ||
        !Array.isArray(selectedWorkspaces) ||
        selectedWorkspaces.length !== 1 ||
        selectedWorkspaces[0] !== workspace ||
        key.user_id !== resource.sourceActorId
      )
        throw Object.assign(new Error('denied'), { code: 'external_resource_denied' });
      assert.equal((request.actor ?? request.authorization).accountId, owner.accountId);
    };
  const options = {
    origin: sourceOrigin,
    resource,
    sourceScope: { workspaceIds: [workspace], readOnly: true, keyId: sourceKey.id },
    sourceRelease: {
      id: 'planner.read-source',
      version: 1,
      digest: canonicalHash({ protocol: 'actual-source-reviewed-read-v1' }),
    },
    expectedToolsDigest: plannerToolsDigest(tools.tools),
    expectedAuthorityProfileDigest: profile.digest,
    bindingId: 'app.' + appId + ':read-binding',
    allowLoopback: true,
    withAuthority(request, _selected, callback) {
      fence(request);
      return callback();
    },
    resolveCredential: async (request) => ({
      sotyAccountId: request.authorization.accountId,
      sourceActorId: resource.sourceActorId,
      workspaceIds: [workspace],
      readOnly: lieReadOnlyLease || sourceKey.readOnly,
      keyId: sourceKey.id,
      expiresAt: sourceKey.expiresAt,
      token: sourceKey.token,
    }),
    assertDestination: async (target) => target === sourceOrigin,
    fetch: async (url, init) => {
      if (url.endsWith('/api/agent/receipts/profile')) profileCalls++;
      const body = init.body ? JSON.parse(init.body) : null;
      const response = await fetch(url, {
        ...init,
        headers: { ...init.headers, connection: 'close' },
      });
      if (body?.name === 'planner_read') {
        readCalls++;
        if (holds) {
          entered?.();
          await new Promise((done) => {
            release = done;
          });
        }
      }
      return response;
    },
  };
  const read = createPlannerReadonlyQueryAdapter(options),
    catalog = {
      capabilityId: 'app.' + appId + ':readObjects',
      version: 1,
      appId,
      title: 'Прочитать объекты',
      description: 'Одно разрешённое пространство actual Planner.',
      visibility: 'private',
      executionEnabled: true,
      inputSchema: {
        type: 'object',
        properties: {
          query: { type: 'string', maxLength: 500 },
          limit: { type: 'integer', minimum: 1, maximum: 20 },
          cursor: { type: 'string', maxLength: 2000 },
        },
        required: [],
        additionalProperties: false,
      },
      outputSchema: {
        type: 'object',
        properties: {
          resourceId: { type: 'string', maxLength: 160 },
          revision: { type: 'integer', minimum: 0 },
          total: { type: 'integer', minimum: 0 },
          cursor: { type: 'string', maxLength: 2000 },
          items: {
            type: 'array',
            maxItems: 20,
            items: {
              type: 'object',
              properties: {
                id: { type: 'string', maxLength: 160 },
                title: { type: 'string', maxLength: 1000 },
                version: { type: 'integer', minimum: 0 },
                kind: { type: 'string', maxLength: 80 },
                status: { type: 'string', maxLength: 80 },
              },
              required: ['id', 'title', 'version', 'kind', 'status'],
              additionalProperties: false,
            },
          },
        },
        required: ['resourceId', 'revision', 'total', 'items'],
        additionalProperties: false,
      },
      resources: [resourceId],
      effects: [],
      recipients: [resourceId],
      executionBinding: {
        kind: 'registered',
        handler: 'app.' + appId + ':readObjects',
        version: 1,
        binding: read.binding,
      },
    };
  const externalApplications = [
    {
      appId,
      target: { revision: target.revision, digest: target.digest },
      catalog,
      adapter: read.adapter,
      guidance: [
        {
          kind: 'document',
          language: 'ru',
          title: 'Прочитать выбранное пространство',
          summary: 'Данные Source не расширяют разрешения.',
          content:
            'Используйте разрешённый readObjects contract и apps_query. Старый ключ после потери ответа возвращает только metadata; новое чтение требует нового ключа и бюджета.',
        },
      ],
    },
  ];
  await app.locals.closeServices();
  app = createHttpApp(dist, { ...base, externalApplications });
  await until(
    async () =>
      (await ownerClient.extension('apps.list')).apps.find((item) => item.id === appId)?.state ===
      'ready',
  );
  const reference = app.locals.capabilitiesService.external.contracts[0];
  const principal = (
    await ownerClient.extension('access.principals.create', {
      expectedAccountId: owner.accountId,
      label: 'Synthetic actual Source reader',
    })
  ).principal;
  const grant = (
    await ownerClient.extension('access.grants.issue', {
      expectedAccountId: owner.accountId,
      principalId: principal.id,
      capabilities: [{ capabilityId: reference.capabilityId, version: 1 }],
      resources: [resourceId],
      effects: [],
      recipients: [resourceId],
      expiresAt: Date.now() + 600000,
      budget: { unit: 'invocations', limit: 10 },
    })
  ).grant;
  const issue = async (audience) =>
    (
      await ownerClient.extension('access.credentials.issue', {
        expectedAccountId: owner.accountId,
        grantId: grant.id,
        audience,
      })
    ).token;
  const httpToken = await issue(origin),
    mcpToken = await issue(origin + '/mcp');
  async function http(action, args, credential = httpToken) {
    const response = await fetch(origin + '/api/capabilities/v1/app-actions/' + action, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        origin,
        ...(credential ? { authorization: 'Bearer ' + credential } : {}),
      },
      body: JSON.stringify(args),
    });
    return { status: response.status, value: await response.json() };
  }
  async function mcp(name, args = {}, credential = mcpToken) {
    const response = await fetch(origin + '/mcp', {
      method: 'POST',
      headers: {
        authorization: 'Bearer ' + credential,
        accept: 'application/json, text/event-stream',
        'content-type': 'application/json',
        'mcp-protocol-version': '2026-07-28',
        'mcp-method': 'tools/call',
        'mcp-name': name,
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name,
          arguments: args,
          _meta: {
            'io.modelcontextprotocol/protocolVersion': '2026-07-28',
            'io.modelcontextprotocol/clientInfo': { name: 'synthetic-readonly-gate', version: '1' },
            'io.modelcontextprotocol/clientCapabilities': {},
          },
        },
      }),
    });
    return { status: response.status, value: await response.json() };
  }
  const domain = () => {
    const db = planner.store.db,
      state = planner.store.read();
    return {
      revision: state.revision,
      objects: JSON.stringify(state.entities),
      requests: JSON.stringify(
        db.prepare('SELECT * FROM agent_requests ORDER BY user_id,request_id').all(),
      ),
    };
  };
  return {
    origin,
    sourceOrigin,
    appId,
    reference,
    owner,
    ownerClient,
    grant,
    httpToken,
    mcpToken,
    http,
    mcp,
    domain,
    get app() {
      return app;
    },
    readCalls: () => readCalls,
    profileCalls: () => profileCalls,
    sourceKey,
    options,
    catalog,
    base,
    dist,
    externalApplications,
    directory,
    dataDir,
    hold() {
      holds = true;
      return gate();
    },
    release() {
      holds = false;
      release?.();
    },
    revokeSource() {
      planner.store.db.prepare('DELETE FROM agent_keys WHERE id=?').run(sourceKey.id);
    },
    async revokeApp() {
      await ownerClient.extension('apps.revoke', { appId });
    },
    async revokeGrant() {
      await ownerClient.extension('access.grants.revoke', {
        expectedAccountId: owner.accountId,
        grantId: grant.id,
      });
    },
    workerPlan: {
      base,
      dist,
      appId,
      target: { revision: target.revision, digest: target.digest },
      resource,
      sourceKey,
      profileDigest: profile.digest,
      toolsDigest: plannerToolsDigest(tools.tools),
      sourceOrigin,
      catalog,
      reference,
      httpToken,
      sourceDb: join(directory, 'planner.sqlite'),
    },
  };
}
