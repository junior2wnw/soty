import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHmac, timingSafeEqual } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { environment } from '../../../human-identity/test/support/renewal-fixture.mjs';
import { createSourceAppBff } from '../../server/bff.mjs';
import { STANDARD_SELECTED_SOURCE } from '../../server/standard-profile.mjs';
import { selectedResourceProfile } from '../../../apps/scoped-embed/resource-profile.mjs';
import { createResourceSourceProofSigner } from '../../../apps/scoped-embed/resource-proof.mjs';
import { createOrdinaryAppStore } from '../../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../../examples/ordinary-app/native.mjs';
import { digest } from '../../server/wire.mjs';

const opaque = () => randomBytes(32).toString('base64url');
/** Native browser cookies are shared by hostname, NEVER by port. Embed jars
 * here model a private per-Source broker, not browser isolation on 127.0.0.1.
 * Root authority IPC is controlled; installed HTTP/WS remains a separate gate. */
export async function createOrdinaryHttpFixture(t) {
  const root = await environment(t, { renewal: false }), directory = await mkdtemp(join(tmpdir(), 'soty-ordinary-http-'));
  const realms = [], sockets = new Set(), nativeJar = new Map();
  for (const [index, realmId] of ['board', 'library'].entries()) {
    const realm = { realmId, key: randomBytes(32), cipherKey: randomBytes(32), embedJar: new Map(), bff: null, store: null, dropPath: null };
    const server = createServer((req, res) => {
      if (realm.dropPath === req.url) {
        realm.dropPath = null; const end = res.end.bind(res);
        res.end = (...args) => { if (res.statusCode === 200) { res.destroy(); return res; } return end(...args); };
      }
      realm.bff.handleRequest(req, res).then(handled => { if (!handled) res.writeHead(404).end(); });
    });
    const ipc = createServer(async (req, res) => {
      const parts = []; for await (const part of req) parts.push(part); const bytes = Buffer.concat(parts), signature = req.headers['x-soty-source-mac'];
      const expected = createHmac('sha256', realm.key).update('authority\0' + bytes.toString('utf8')).digest('base64url');
      if (!realm.rootLive || typeof signature !== 'string' || signature.length !== 43 || !timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) { res.writeHead(403).end(); return; }
      const input = JSON.parse(bytes.toString('utf8')), body = JSON.stringify({ nonce: input.nonce, context: realm.context });
      res.writeHead(200, { 'content-type': 'application/json', 'x-soty-source-mac': createHmac('sha256', realm.key).update('authority-result\0' + body).digest('base64url') }); res.end(body);
    });
    for (const current of [server, ipc]) { current.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
      await new Promise(done => current.listen(0, '127.0.0.1', done)); }
    Object.assign(realm, { server, ipc, index, rootLive: true, nativeOrigin: 'http://localhost:' + server.address().port,
      embedOrigin: 'http://127.0.0.1:' + server.address().port, databasePath: join(directory, realmId + '.sqlite') }); realms.push(realm);
  }
  let configured;
  await root.configureClients(clients => { configured = clients.map((client, index) => ({ ...client, version: 2, redirectUri: realms[index].nativeOrigin + '/soty/callback' })); return configured; });
  for (const realm of realms) {
    const client = configured[realm.index], appId = 'app-' + (realm.index === 0 ? 'a' : 'b').repeat(32);
    realm.client = client; realm.profile = selectedResourceProfile({ schema: 'soty.selected-human-embed.v2', appId,
      connector: { linkId: 'controlled_' + realm.realmId, hostDeviceId: 'synthetic_host', connectorId: 'synthetic_connector' }, target: { revision: 1, digest: (realm.index === 0 ? 'a' : 'b').repeat(64) },
      sourceProfile: STANDARD_SELECTED_SOURCE, resource: { registryId: 'soty', environmentId: 'fixture', tenantId: root.actor.account.accountId, appId,
        resourceId: 'synthetic:' + realm.realmId, selection: { kind: 'soty.resource.v1', nativeId: 'selected', incarnationId: 'one' } },
      issuer: root.issuer, clientId: client.id, embedOrigin: realm.embedOrigin, nativeOrigin: realm.nativeOrigin, parentOrigin: root.origin });
    realm.context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: opaque(), version: 1, digest: 'd'.repeat(64) },
      profileDigest: realm.profile.digest, appId, sourceProfile: realm.profile.sourceProfile, resource: realm.profile.resource,
      rootPrincipal: { accountId: root.actor.account.accountId, deviceId: root.actor.account.deviceId }, humanPrincipal: { issuer: root.issuer,
        subject: root.actor.account.accountId, clientId: client.id, clientProfileDigest: root.profile.publicClients[realm.index].profileDigest, clientGeneration: 1 },
      entry: { domainId: 'synthetic_' + realm.realmId, origin: realm.embedOrigin }, target: realm.profile.target, policyEpoch: 1, expiresAt: Date.now() + 300000 };
    realm.options = { databasePath: realm.databasePath, realmId: realm.realmId, key: realm.cipherKey, keyId: 'fixture-' + realm.realmId };
    realm.restart = () => {
      realm.bff?.close(); realm.store?.close(); realm.store = createOrdinaryAppStore({ ...realm.options, initialize: !realm.initialized });
      if (!realm.initialized) {
        realm.store.createResource({ id: 'selected', incarnationId: 'one', title: 'Synthetic ' + realm.realmId });
        realm.store.createPrincipal('native-participant'); realm.store.createPrincipal('native-owner');
        realm.store.grant('selected', 'native-participant', 'participant'); realm.store.grant('selected', 'native-owner', 'owner');
        realm.nativeToken = realm.store.createNativeSession('native-participant'); realm.initialized = true;
      }
      realm.bff = createSourceAppBff({ profile: realm.profile, transportKey: realm.key, connectorPort: realm.ipc.address().port,
        storage: realm.store.storage, native: createOrdinaryAppNativePort({ store: realm.store, resourceId: 'selected', incarnationId: 'one',
          afterCommit: () => realm.afterCommit?.() }),
        rp: { issuer: root.issuer, clientId: client.id, clientSecret: client.clientSecret, redirectUri: client.redirectUri },
        ui: { appLabel: 'Synthetic ' + realm.realmId, resourceLabel: 'Выбранный проект' } });
    };
    realm.restart(); nativeJar.set('ordinary_native_' + realm.realmId, realm.nativeToken);
    const signer = createResourceSourceProofSigner({ profile: realm.profile, key: realm.key });
    realm.request = async (path, { method = 'GET', data, form, native = false, signed = !native, jar: overrideJar } = {}) => {
      const jar = overrideJar ?? (native ? nativeJar : realm.embedJar), origin = native ? realm.nativeOrigin : realm.embedOrigin;
      const cookie = [...jar].map(([name, token]) => name + '=' + token).join('; '), body = form ? Buffer.from(new URLSearchParams(form).toString()) : data ? Buffer.from(JSON.stringify(data)) : Buffer.alloc(0);
      const response = await fetch(origin + path, { method, redirect: 'manual', headers: { ...(cookie ? { cookie } : {}),
        ...(method === 'POST' ? { origin, 'content-type': form ? 'application/x-www-form-urlencoded' : 'application/json' } : {}),
        ...(signed ? signer.headers({ context: realm.context, method, path, body, cookie }) : {}) }, ...(method === 'POST' ? { body } : {}), signal: AbortSignal.timeout(5000) });
      for (const raw of response.headers.getSetCookie()) { const pair = raw.split(';')[0], split = pair.indexOf('='); jar.set(pair.slice(0, split), pair.slice(split + 1)); }
      const text = await response.text(); let value; try { value = JSON.parse(text); } catch {}
      return { status: response.status, value, text, location: response.headers.get('location') };
    };
    realm.beginNative = async () => {
      const start = await realm.request('/api/embed/login', { method: 'POST', data: {} }); assert.equal(start.status, 200);
      const url = new URL(start.value.nativeUrl), page = await realm.request(url.pathname + url.search, { native: true }); assert.equal(page.status, 200);
      const form = Object.fromEntries([...page.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(match => [match[1], match[2]])); form.consent = 'yes';
      return { form, start, page };
    };
    realm.authorizeNative = async intent => { const response = await realm.request('/soty/authorize', { method: 'POST', form: intent.form, native: true }); assert.equal(response.status, 303); return response; };
    realm.completeOidc = async authorize => {
      const response = await root.wire.request(authorize.location), interaction = await root.wire.request(response.location); assert.equal(interaction.status, 200);
      const current = await root.wire.request(response.location.href + '/context'), flow = { rp: { redirectUri: client.redirectUri }, session: root.wire, location: response.location, context: current.body };
      await root.approve(flow); return root.complete(flow);
    };
    realm.finishNative = async callback => {
      const result = await realm.request(callback.pathname + callback.search, { native: true }); assert.equal(result.status, 303);
      const url = new URL(result.location), linked = await realm.request(url.pathname + url.search); assert.equal(linked.status, 200);
      const locator = realm.embedJar.get('soty_rp_link'), completed = await realm.request('/api/embed/complete-link?intent=' + locator); assert.equal(completed.status, 200); return completed;
    };
  }
  t.after(async () => {
    realms.forEach(realm => { realm.bff.close(); realm.store.close(); }); sockets.forEach(socket => socket.destroy());
    await Promise.all(realms.flatMap(realm => [realm.server, realm.ipc]).map(server => new Promise(done => server.close(done))));
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 40 });
  });
  return { root, realms, nativeJar, directory, digest };
}
