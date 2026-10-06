import express from 'express';
import { createServer } from 'node:http';
import { randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createServer as createViteServer } from 'vite';
import { attachConnectModule } from '../../server/connect-module.js';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';
import { createHumanIdentityService } from '../../modules/human-identity/service.mjs';
import { attachHumanIdentity } from '../../server/human-identity.js';

// Synthetic localhost only. The helper selects a fixture connector's expected
// Root subject, not a production authorization API or a profile header.
const root = fileURLToPath(new URL('../../', import.meta.url)),
  sourceRoot = resolve(
    process.env.SOTY_PLANNER_PROOF_SOURCE_ROOT ||
      'D:/соты/output/planner-universal-integration/worktree',
  );
const { tsImport } = await import(
  pathToFileURL(join(sourceRoot, 'node_modules/tsx/dist/esm/api/index.mjs')).href
);
const source = await tsImport(
  pathToFileURL(join(sourceRoot, 'server/main.ts')).href,
  import.meta.url,
);
const frontend = 'http://127.0.0.1:5239',
  backendOrigin = 'http://127.0.0.1:5399',
  embedded = 'http://127.0.0.1:5240',
  native = 'http://localhost:5240';
const folder = await mkdtemp(join(tmpdir(), 'soty-planner-browser-')),
  dbPath = join(folder, 'planner.sqlite');
const random = () => randomBytes(32).toString('base64url');
let planner,
  vite,
  connect,
  identity,
  app,
  currentSubject = null,
  workspaceId,
  stopping = false,
  generation = 0;
const sockets = new Set();
const backend = createServer((req, res) => (app ? app(req, res) : res.writeHead(503).end()));
backend.on('connection', (s) => {
  sockets.add(s);
  s.once('close', () => sockets.delete(s));
});
const send = (res, status, body) => {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
};
async function owner(path, body) {
  const r = await fetch(native + path, {
    method: 'POST',
    headers: { origin: native, 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error('fixture_source_owner_failed');
  return r.json();
}
let options;
async function startPlanner() {
  planner = await source.createPlannerServer(options);
  await planner.listen();
  generation++;
}
async function cleanup() {
  if (stopping) return;
  stopping = true;
  await vite?.close();
  sockets.forEach((s) => s.destroy());
  backend.closeAllConnections();
  await new Promise((done) => backend.close(done));
  identity?.close();
  connect?.close();
  if (planner) {
    planner.server.closeAllConnections();
    await planner.close();
  }
  if (
    dirname(resolve(folder)) !== resolve(tmpdir()) ||
    !/^soty-planner-browser-/.test(basename(folder))
  )
    throw new Error('fixture_cleanup_denied');
  await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
try {
  planner = await source.createPlannerServer({
    dbPath,
    port: 5240,
    host: '127.0.0.1',
    scheduler: false,
  });
  await planner.listen();
  const seeded = await owner('/api/agent/call', {
    name: 'planner_apply',
    arguments: {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'workspaces',
          key: 'selected',
          data: { name: 'Только синтетический выбранный проект', timezone: 'UTC' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: '$selected',
          key: 'existing',
          data: { title: 'Синтетический первый объект' },
        },
      ],
    },
  });
  workspaceId = seeded.refs.selected;
  planner.server.closeAllConnections();
  await planner.close();
  planner = undefined;
  const secret = random(),
    sessionKey = random(),
    jwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(jwk, { kid: 'planner-browser', use: 'sig', alg: 'RS256' });
  const issuer = frontend + '/human-identity',
    profile = createHumanIdentityHostProfile(
      {
        enabled: true,
        issuer,
        registryId: 'REG.soty',
        environmentId: 'fixture',
        clients: [
          {
            id: 'planner-browser',
            label: 'Тестовый выбранный Планировщик',
            redirectUri: embedded + '/api/embed/callback',
            clientSecret: secret,
          },
        ],
        jwks: { keys: [jwk] },
        cookieKeys: [random()],
        artifactKey: randomBytes(32),
        artifactKeyId: 'planner-browser',
      },
      { shellOrigins: [frontend] },
    );
  const dist = join(folder, 'dist');
  await mkdir(dist);
  await writeFile(join(dist, 'index.html'), await readFile(join(root, 'index.html')));
  app = express();
  identity = createHumanIdentityService({
    databasePath: join(folder, 'identity.sqlite'),
    profile,
    actorActive: (actor) => connect?.isActorActive(actor) === true,
    withAuthorityFence: (callback) => connect.withAuthorityFence(callback),
    readProfile: () => ({ name: 'Синтетический профиль' }),
  });
  connect = attachConnectModule(app, {
    dataDir: folder,
    origins: [frontend],
    extensions: [identity],
  });
  attachHumanIdentity(app, { profile, service: identity, distDir: dist });
  app.use(
    '/__fixture',
    (req, res) =>
      void (async () => {
        if (req.headers.origin !== frontend || req.method !== 'POST') {
          send(res, 403, { code: 'fixture_origin_denied' });
          return;
        }
        let raw = '',
          size = 0;
        for await (const chunk of req) {
          size += chunk.length;
          if (size > 4096) throw new Error('fixture_limit');
          raw += chunk.toString();
        }
        const args = raw ? JSON.parse(raw) : {};
        if (req.path === '/bind') {
          if (Object.keys(args).length !== 1 || typeof args.accountId !== 'string')
            throw new Error('fixture_invalid_account');
          const db = new DatabaseSync(join(folder, 'connect', 'accounts.sqlite'), {
            readOnly: true,
          });
          try {
            if (!db.prepare('SELECT id FROM accounts WHERE id=?').get(args.accountId))
              throw new Error('fixture_account_missing');
          } finally {
            db.close();
          }
          currentSubject = args.accountId;
          send(res, 200, { synthetic: true, selected: true });
          return;
        }
        if (Object.keys(args).length) throw new Error('fixture_arguments_denied');
        if (req.path === '/info') {
          send(res, 200, { synthetic: true, embedded, native, parent: frontend, generation });
          return;
        }
        if (req.path === '/restart') {
          planner.server.closeAllConnections();
          await planner.close();
          planner = undefined;
          await startPlanner();
          send(res, 200, { synthetic: true, restarted: true, generation });
          return;
        }
        if (req.path === '/revoke') {
          await owner('/soty/disconnect', {});
          send(res, 200, { synthetic: true, revoked: true });
          return;
        }
        if (req.path === '/counts') {
          send(res, 200, {
            synthetic: true,
            links: planner.store.db.prepare('SELECT count(*) AS n FROM planner_soty_links').get().n,
            selectedObjects: planner.store
              .read()
              .entities.filter((e) => e.workspaceId === workspaceId).length,
            generation,
          });
          return;
        }
        if (req.path === '/shutdown') {
          send(res, 200, { synthetic: true, stopping: true });
          setTimeout(() => void cleanup().then(() => process.exit(0)), 20);
          return;
        }
        send(res, 404, { code: 'fixture_not_found' });
      })().catch(() => send(res, 400, { code: 'fixture_failed' })),
  );
  options = {
    dbPath,
    port: 5240,
    host: '127.0.0.1',
    scheduler: false,
    embed: {
      nativeOrigin: native,
      embedOrigin: embedded,
      parentOrigin: frontend,
      workspaceId,
      profile: {
        issuer,
        clientId: 'planner-browser',
        clientSecret: secret,
        redirectUri: embedded + '/api/embed/callback',
      },
      sessionKey,
      currentSotySubject: async () => (currentSubject ? { issuer, subject: currentSubject } : null),
    },
  };
  await startPlanner();
  await new Promise((done) => backend.listen(5399, '127.0.0.1', done));
  vite = await createViteServer({
    root,
    configFile: join(root, 'vite.config.ts'),
    server: {
      host: '127.0.0.1',
      port: 5239,
      strictPort: true,
      proxy: {
        '/api': { target: backendOrigin },
        '/human-identity': { target: backendOrigin, changeOrigin: false },
        '/__fixture': { target: backendOrigin },
      },
    },
  });
  await vite.listen();
  console.log(
    JSON.stringify({
      state: 'ready',
      synthetic: true,
      url: frontend + '/src/world/planner-embed-fixture.test.html',
      sourceOrigin: embedded,
      launchSecretsLogged: false,
    }),
  );
  process.on('SIGINT', () => void cleanup().then(() => process.exit(0)));
  process.on('SIGTERM', () => void cleanup().then(() => process.exit(0)));
} catch {
  await cleanup();
  console.error('planner_browser_fixture_failed');
  process.exitCode = 1;
}
