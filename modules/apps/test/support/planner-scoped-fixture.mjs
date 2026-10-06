import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { mkdtemp, access, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, basename, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { createClientWithStorage } from '../../../connect/browser/client.mjs';
import { attachConnectModule } from '../../../../server/connect-module.js';
import { createHumanIdentityHostProfile } from '../../../human-identity/profile.mjs';
import { createHumanIdentityService } from '../../../human-identity/service.mjs';
import { attachHumanIdentity } from '../../../../server/human-identity.js';

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
export const sourceAvailable = available;
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
export async function fixture(t, configure = () => null) {
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
  const actorRefs = new Map();
  connect = attachConnectModule(app, {
    dataDir: directory,
    origins: [rootOrigin],
    extensions: [
      identity,
      {
        operations: new Set(['fixture.scoped.capture']),
        execute({ actor }) {
          actorRefs.set(actor.accountId, actor);
          return { captured: true };
        },
      },
    ],
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
  await primary.client.extension('fixture.scoped.capture', {});
  await other.client.extension('fixture.scoped.capture', {});
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
  const extension = await configure({
    options,
    primary,
    other,
    actorRefs,
    connect,
    workspaceId,
    rootOrigin,
    embedded,
    native,
    sourceRoot,
    getPlanner: () => planner,
  });
  await start();
  const wire = browser();
  extension?.wrapWire?.(wire);
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
    extension,
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
