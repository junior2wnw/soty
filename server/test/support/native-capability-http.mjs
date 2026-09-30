import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { generateKeyPairSync, sign, randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../../http-app.js';
import { createNotesService } from '../../../modules/notes/server/index.mjs';
import { createCapabilitiesService } from '../../../modules/capabilities/server/index.mjs';
import { digestArgs } from '../../../modules/connect/server/index.mjs';

export function nativeIdentity(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
export function good(value) { assert.equal(value.ok, true, value.error?.code); return value; }

export async function nativeHttpFixture(t, { enabled = true, notesVersion = 2, capabilitiesVersion = 2 } = {}) {
  const parent = realpathSync(tmpdir()), directory = realpathSync(mkdtempSync(join(parent, 'soty-native-http-')));
  const nonce = randomUUID(), marker = join(directory, 'test-owner'); writeFileSync(marker, nonce, { flag: 'wx' });
  let app, front;
  const server = createServer((req, res) => front ? front(req, res) : app ? app(req, res) : res.writeHead(503).end());
  t.after(async () => {
    server.closeAllConnections(); await new Promise(done => server.close(done)); await app?.locals.closeServices();
    assert.equal(realpathSync(directory), directory); assert.equal(dirname(directory), parent);
    assert.ok(directory.startsWith(join(parent, 'soty-native-http-'))); assert.equal(readFileSync(marker, 'utf8'), nonce);
    rmSync(directory, { recursive: true, force: true });
  });
  const notesFile = join(directory, 'notes', 'notes.sqlite'), capsFile = join(directory, 'capabilities', 'capabilities.sqlite');
  if (notesVersion === 2) createNotesService({ databasePath: notesFile, projectId: 'soty', allowNativeMigration: true }).close();
  if (capabilitiesVersion === 2) createCapabilitiesService({ databasePath: capsFile, projectId: 'soty',
    allowNativeMigration: true, actorActive: () => false }).close();
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const create = enabled => createHttpApp(resolve('dist'), { dataDir: directory, connectOrigins: [origin],
    capabilityAudience: origin, nativeNotesEnabled: enabled, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  app = create(enabled);
  const send = async value => {
    const response = await fetch(`${origin}/api/connect/rpc`, { method: 'POST',
      headers: { 'content-type': 'application/json', origin }, body: JSON.stringify({ protocol: 1, ...value }) });
    return response.json();
  };
  const proof = async (actor, op, args) => {
    const challenge = good(await send({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return { op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  };
  const call = async (actor, op, args = {}) => send(await proof(actor, op, args));
  const bootstrap = async actor => good(await call(actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }));
  async function issue(actor, accountId, { audience = origin, grantId, principalId } = {}) {
    if (!principalId) principalId = good(await call(actor, 'access.principals.create', {
      expectedAccountId: accountId, label: 'HTTP проверка', clientLabel: 'Синтетический внешний HTTP клиент',
    })).principal.id;
    if (!grantId) grantId = good(await call(actor, 'access.grants.issue', {
      expectedAccountId: accountId, principalId, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt: Date.now() + 3600000,
      allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 10 },
    })).grant.id;
    const credential = good(await call(actor, 'access.credentials.issue', { expectedAccountId: accountId, grantId, audience }));
    return { principalId, grantId, token: credential.token, credential: credential.credential };
  }
  function http(path, { method = 'GET', token, headers = {}, body } = {}) {
    return new Promise((resolveResult, reject) => {
      const req = request({ hostname: '127.0.0.1', port: server.address().port, method, path,
        headers: { ...(token ? { authorization: `Bearer ${token}` } : {}),
          ...(body !== undefined ? { 'content-type': 'application/json' } : {}), ...headers } }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.once('error', reject);
        res.once('end', () => {
          try { resolveResult({ status: res.statusCode, headers: res.headers,
            body: JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') }); } catch (error) { reject(error); }
        });
      });
      req.once('error', reject); req.end(body);
    });
  }
  return { directory, notesFile, capsFile, origin, call, proof, bootstrap, issue, http, server,
    get app() { return app; },
    setFront(value) { front = value; },
    async restart({ enabled = false } = {}) { await app.locals.closeServices(); app = create(enabled); },
    sql(file, action) { const db = new DatabaseSync(file, { readOnly: true }); try { return action(db); } finally { db.close(); } },
  };
}
