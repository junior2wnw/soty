import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { mkdtemp, access, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, basename, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { createClientWithStorage } from '../../connect/browser/client.mjs';
import { attachConnectModule } from '../../../server/connect-module.js';
import { createHumanIdentityHostProfile } from '../../human-identity/profile.mjs';
import { createHumanIdentityService } from '../../human-identity/service.mjs';
import { attachHumanIdentity } from '../../../server/human-identity.js';

const sourceRoot = resolve(
  process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT ||
    'D:/соты/output/planner-universal-integration/worktree',
);
let source,
  available = true;
try {
  await access(join(sourceRoot, 'server/soty-embed.ts'));
  await access(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs'));
} catch {
  available = false;
  if (process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT)
    throw new Error('Configured actual Planner BFF source unavailable.');
}
if (available) {
  const { tsImport } = await import(
    pathToFileURL(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs')).href
  );
  source = await tsImport(pathToFileURL(join(sourceRoot, 'server/main.ts')).href, import.meta.url);
}
const random = () => randomBytes(32).toString('base64url');
function memory() {
  let value = null;
  return {
    async read() {
      return structuredClone(value);
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
function browser() {
  const jar = new Map();
  return {
    async request(
      input,
      { fields, body, method = fields || body ? 'POST' : 'GET', headers = {} } = {},
    ) {
      const url = new URL(input);
      assert.ok(['localhost', '127.0.0.1'].includes(url.hostname));
      const cookie = [...jar.values()]
        .filter((c) => c.host === url.hostname && url.pathname.startsWith(c.path))
        .map((c) => c.name + '=' + c.value)
        .join('; ');
      const r = await fetch(url, {
        method,
        redirect: 'manual',
        signal: AbortSignal.timeout(8000),
        headers: {
          connection: 'close',
          ...(cookie ? { cookie } : {}),
          ...(['GET', 'HEAD'].includes(method)
            ? {}
            : { origin: url.origin, 'sec-fetch-site': 'same-origin' }),
          ...(body && !(body instanceof FormData) ? { 'content-type': 'application/json' } : {}),
          ...headers,
        },
        ...(fields
          ? { body: new URLSearchParams(fields) }
          : body
            ? { body: body instanceof FormData ? body : JSON.stringify(body) }
            : {}),
      });
      for (const raw of r.headers.getSetCookie()) {
        const [pair, ...attrs] = raw.split(';'),
          i = pair.indexOf('='),
          name = pair.slice(0, i),
          value = pair.slice(i + 1),
          path =
            attrs
              .find((a) => a.trim().toLowerCase().startsWith('path='))
              ?.trim()
              .slice(5) || '/';
        assert.equal(
          attrs.some((a) => a.trim().toLowerCase().startsWith('domain=')),
          false,
        );
        jar.set(url.hostname + '\0' + name, { host: url.hostname, name, value, path });
      }
      const text = await r.text();
      assert.ok(Buffer.byteLength(text) <= 524288);
      return {
        status: r.status,
        headers: r.headers,
        text,
        body: r.headers.get('content-type')?.includes('application/json') ? JSON.parse(text) : null,
        location: r.headers.get('location') ? new URL(r.headers.get('location'), url) : null,
      };
    },
  };
}
async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-planner-human-')),
    clients = [];
  let app, connect, identity, planner, currentSubject;
  const server = createServer((req, res) => (app ? app(req, res) : res.writeHead(503).end()));
  await new Promise((done) => server.listen(0, '127.0.0.1', done));
  const rootOrigin = `http://127.0.0.1:${server.address().port}`,
    issuer = rootOrigin + '/human-identity';
  t.after(async () => {
    clients.forEach((c) => c.dispose());
    server.closeAllConnections();
    await new Promise((done) => server.close(done));
    identity?.close();
    connect?.close();
    if (planner) {
      planner.server.closeAllConnections();
      await planner.close();
    }
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-planner-human-/);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const dbPath = join(directory, 'planner.sqlite');
  planner = await source.createPlannerServer({
    dbPath,
    port: 0,
    host: '127.0.0.1',
    scheduler: false,
  });
  const port = await planner.listen(),
    embedded = `http://127.0.0.1:${port}`,
    native = `http://localhost:${port}`;
  const seedRequest = await fetch(embedded + '/api/agent/call', {
    method: 'POST',
    headers: { origin: embedded, 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({
      name: 'planner_apply',
      arguments: {
        requestId: randomUUID(),
        operations: [
          {
            op: 'create',
            collection: 'workspaces',
            key: 'selected',
            data: { name: 'Synthetic selected workspace', timezone: 'UTC' },
          },
          {
            op: 'create',
            collection: 'objects',
            workspace: '$selected',
            key: 'existing',
            data: { title: 'Synthetic selected existing object' },
          },
        ],
      },
    }),
  });
  assert.equal(seedRequest.status, 200);
  const seeded = await seedRequest.json(),
    workspaceId = seeded.refs.selected;
  planner.server.closeAllConnections();
  await planner.close();
  planner = undefined;
  const secret = random(),
    jwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(jwk, { kid: 'planner-fixture', use: 'sig', alg: 'RS256' });
  const profile = createHumanIdentityHostProfile(
    {
      enabled: true,
      issuer,
      registryId: 'REG.soty',
      environmentId: 'fixture',
      clients: [
        {
          id: 'planner-fixture',
          label: 'Synthetic selected Planner',
          redirectUri: embedded + '/api/embed/callback',
          clientSecret: secret,
        },
      ],
      jwks: { keys: [jwk] },
      cookieKeys: [random()],
      artifactKey: randomBytes(32),
      artifactKeyId: 'planner-fixture',
    },
    { shellOrigins: [rootOrigin] },
  );
  const dist = join(directory, 'dist');
  await mkdir(dist);
  await writeFile(
    join(dist, 'index.html'),
    '<!doctype html><title>Synthetic Root interaction</title>',
  );
  app = express();
  identity = createHumanIdentityService({
    databasePath: join(directory, 'human.sqlite'),
    profile,
    actorActive: (actor) => connect?.isActorActive(actor) === true,
    withAuthorityFence: (callback) => connect.withAuthorityFence(callback),
    readProfile: () => ({ name: 'Synthetic same display name' }),
  });
  connect = attachConnectModule(app, {
    dataDir: directory,
    origins: [rootOrigin],
    extensions: [identity],
  });
  attachHumanIdentity(app, { profile, service: identity, distDir: dist });
  async function newClient() {
    const c = createClientWithStorage(
      {
        projectId: 'soty',
        endpoint: rootOrigin + '/api/connect/rpc',
        fetch: (url, init) =>
          fetch(url, { ...init, headers: { ...init.headers, origin: rootOrigin } }),
      },
      memory(),
    );
    clients.push(c);
    return { client: c, account: await c.bootstrap('Synthetic Root profile') };
  }
  const primary = await newClient(),
    other = await newClient();
  currentSubject = primary.account.accountId;
  const options = {
    dbPath,
    port,
    host: '127.0.0.1',
    scheduler: false,
    embed: {
      nativeOrigin: native,
      embedOrigin: embedded,
      parentOrigin: rootOrigin,
      workspaceId,
      profile: {
        issuer,
        clientId: 'planner-fixture',
        clientSecret: secret,
        redirectUri: embedded + '/api/embed/callback',
      },
      sessionKey: random(),
      currentSotySubject: async () => ({ issuer, subject: currentSubject }),
    },
  };
  async function start() {
    planner = await source.createPlannerServer(options);
    await planner.listen();
  }
  await start();
  const wire = browser();
  let serial = 0;
  async function login() {
    const begun = await wire.request(embedded + '/api/embed/login');
    assert.equal(begun.status, 302);
    const auth = await wire.request(begun.location);
    assert.ok([302, 303].includes(auth.status));
    const flow = auth.location;
    await wire.request(flow);
    const context = (await wire.request(flow.href + '/context')).body;
    assert.equal(context.client.id, 'planner-fixture');
    await primary.client.extension(
      'identity.human.approve',
      {
        expectedAccountId: primary.account.accountId,
        interactionId: context.interactionId,
        browserNonce: context.browserNonce,
        csrf: context.csrf,
        requestId: 'planner-decision-' + ++serial,
        decision: 'approve',
      },
      { expectedAccountId: primary.account.accountId },
    );
    let finish = await wire.request(flow.href + '/complete', { fields: { csrf: context.csrf } });
    assert.equal(finish.status, 303);
    finish = await wire.request(finish.location);
    if (finish.status === 200) {
      const action = /<form method="post" action="([^"]+)">/u.exec(finish.text)?.[1];
      const hidden = Object.fromEntries(
        [...finish.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(
          (m) => [m[1], m[2]],
        ),
      );
      finish = await wire.request(action, { fields: hidden });
      finish = await wire.request(finish.location);
    }
    assert.ok([302, 303].includes(finish.status));
    assert.equal(
      finish.location.origin + finish.location.pathname,
      options.embed.profile.redirectUri,
    );
    return wire.request(finish.location);
  }
  return {
    wire,
    login,
    native,
    embedded,
    rootOrigin,
    workspaceId,
    seeded,
    primary,
    other,
    options,
    directory,
    get planner() {
      return planner;
    },
    switchProfile() {
      currentSubject = other.account.accountId;
    },
    async restart() {
      planner.server.closeAllConnections();
      await planner.close();
      planner = undefined;
      await start();
    },
  };
}

test(
  'actual Planner BFF requires independent native consent and maintained signed OIDC; only selected workspace is exposed',
  { skip: !available, timeout: 30000 },
  async (t) => {
    const f = await fixture(t),
      before = await f.wire.request(f.embedded + '/api/embed/state');
    assert.equal(before.status, 401);
    for (const path of ['/api/state', '/api/agent/keys', '/mcp'])
      assert.equal(
        (
          await f.wire.request(f.embedded + path, {
            headers: { 'x-soty-subject': f.primary.account.accountId },
          })
        ).status,
        403,
      );
    const unlinked = await f.login();
    assert.equal(unlinked.status, 200);
    assert.equal((await f.wire.request(f.embedded + '/api/embed/state')).status, 401);
    const consentUrl = /<a href="([^"]+\/soty\/connect\?intent=[A-Za-z0-9_-]+)">/u.exec(
      unlinked.text,
    )?.[1];
    assert.ok(consentUrl);
    assert.equal(new URL(consentUrl).origin, f.native);
    const consent = await f.wire.request(consentUrl);
    assert.equal(consent.status, 200);
    assert.match(consent.text, /Разрешить только это пространство/);
    const token = /name="intent" value="([A-Za-z0-9_-]+)"/u.exec(consent.text)?.[1];
    assert.equal(
      (
        await f.wire.request(f.native + '/soty/connect', {
          fields: { intent: token },
          headers: { origin: 'https://foreign.invalid' },
        })
      ).status,
      403,
    );
    const approval = await f.wire.request(f.native + '/soty/connect', {
      fields: { intent: token },
    });
    assert.equal(approval.status, 302);
    const completed = await f.wire.request(approval.location);
    assert.equal(completed.status, 200);
    assert.match(completed.text, /Профиль подключён/);
    const actual = await f.wire.request(f.embedded + '/api/embed/state');
    assert.equal(actual.status, 200);
    assert.deepEqual(
      actual.body.workspaces.map((w) => w.id),
      [f.workspaceId],
    );
    assert.equal(actual.body.entities.length, 1);
    assert.equal(actual.body.entities[0].id, f.seeded.refs.existing);
    const visibleNative = await f.wire.request(f.native + '/api/state');
    assert.ok(visibleNative.body.workspaces.length > 1, 'native view preserved');
    assert.equal(
      (
        await f.wire.request(f.embedded + '/api/embed/workspaces', {
          body: { name: 'Forbidden source-admin workspace' },
        })
      ).status,
      403,
    );
    assert.equal(
      (await f.wire.request(f.embedded + '/api/embed/settings', { body: {}, method: 'PATCH' }))
        .status,
      403,
    );
    const forbidden = await f.wire.request(f.embedded + '/api/embed/entities', {
      body: {
        workspaceId: visibleNative.body.workspaces.find((w) => w.id !== f.workspaceId).id,
        title: 'Forbidden',
      },
    });
    assert.equal(forbidden.status, 403);
    const upload = workspaceId => {const value=new FormData();value.set('workspaceId',workspaceId);
      value.set('file',new File(['Only synthetic selected-file bytes'],'fixture.txt',{type:'text/plain'}));return value;};
    const selectedFile=await f.wire.request(f.embedded+'/api/embed/files',{body:upload(f.workspaceId)});
    assert.equal(selectedFile.status,201);
    const selectedBytes=await f.wire.request(f.embedded+selectedFile.body.url);
    assert.equal(selectedBytes.status,200);assert.equal(selectedBytes.text,'Only synthetic selected-file bytes');
    const foreignFile=await f.wire.request(f.native+'/api/files',{body:upload(visibleNative.body.workspaces.find(w=>w.id!==f.workspaceId).id)});
    assert.equal(foreignFile.status,201);
    assert.equal((await f.wire.request(f.embedded+foreignFile.body.url)).status,403);
    assert.equal(
      (await f.wire.request(f.embedded + '/api/embed/agent/keys', { body: {} })).status,
      403,
    );
    const created = await f.wire.request(f.embedded + '/api/embed/entities', {
      body: { workspaceId: f.workspaceId, title: 'Synthetic UI-created undated object' },
    });
    assert.equal(created.status, 201);
    assert.equal(created.body.entities.at(-1).plan.start, null);
    const page = await f.wire.request(f.embedded + '/embed');
    assert.equal(page.status, 200);
    assert.equal(page.headers.get('x-frame-options'), null);
    assert.ok(
      page.headers.get('content-security-policy').includes('frame-ancestors ' + f.rootOrigin),
    );
    const revoked = await f.wire.request(f.native + '/soty/disconnect', { body: {} });
    assert.equal(revoked.status, 200);
    const denied = await f.wire.request(f.embedded + '/api/embed/state');
    assert.equal(denied.status, 403);
    assert.equal(denied.body.code, 'embed_link_required');
    // Restore only this synthetic association for restart/current-profile checks.
    f.planner.store.db
      .prepare(
        'INSERT INTO planner_soty_links(issuer,subject,workspace_id,user_id,created_at) VALUES(?,?,?,?,?)',
      )
      .run(
        f.options.embed.profile.issuer,
        f.primary.account.accountId,
        f.workspaceId,
        f.planner.store.localUser().id,
        Date.now(),
      );
    await f.restart();
    assert.equal((await f.wire.request(f.embedded + '/api/embed/state')).status, 200);
    const privateRows = f.planner.store.db
      .prepare('SELECT payload_cipher FROM planner_soty_private')
      .all();
    assert.ok(privateRows.length);
    for (const row of privateRows)
      assert.equal(row.payload_cipher.includes(f.primary.account.accountId), false);
    f.switchProfile();
    const switched = await f.wire.request(f.embedded + '/api/embed/state', {
      headers: { 'x-soty-subject': f.primary.account.accountId },
    });
    assert.equal(switched.status, 401);
    assert.equal(switched.body.code, 'embed_profile_changed');
  },
);
