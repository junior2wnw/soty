import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createClientWithStorage } from '../../../connect/browser/client.mjs';
import { attachConnectModule } from '../../../../server/connect-module.js';
import { createHumanIdentityHostProfile } from '../../profile.mjs';
import { createHumanIdentityService } from '../../service.mjs';
import { attachHumanIdentity } from '../../../../server/human-identity.js';
import { createHumanBffFixture, verifyHumanIdToken } from './renewal-bff.mjs';

const random = () => randomBytes(32).toString('base64url');
function memoryVault() {
  let value = null;
  return { async read() { return structuredClone(value); }, async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
    async compareAndSwap(revision, next) { assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value); } };
}
function browser() {
  const jar = new Map();
  function save(response, url) {
    for (const raw of response.headers.getSetCookie()) {
      const [pair, ...attributes] = raw.split(';'), equal = pair.indexOf('=');
      const attrs = Object.fromEntries(attributes.map(value => { const split = value.indexOf('='); return split < 0 ? [value.trim().toLowerCase(), true]
        : [value.slice(0, split).trim().toLowerCase(), value.slice(split + 1).trim()]; }));
      assert.equal(attrs.domain, undefined, 'host-only cookies');
      const cookie = { host: url.hostname, name: pair.slice(0, equal), value: pair.slice(equal + 1), path: attrs.path || '/', secure: attrs.secure === true };
      const key = cookie.host + '\0' + cookie.path + '\0' + cookie.name;
      if (!cookie.value || attrs['max-age'] === '0') jar.delete(key); else jar.set(key, cookie);
    }
  }
  async function request(input, { fields, headers = {}, cookies = true, originHeader } = {}) {
    const url = new URL(input); assert.equal(url.hostname, '127.0.0.1', 'fixture never contacts a non-loopback provider/app');
    const cookie = [...jar.values()].filter(value => value.host === url.hostname && (!value.secure || url.protocol === 'https:')
      && (url.pathname === value.path || url.pathname.startsWith(value.path.endsWith('/') ? value.path : value.path + '/')))
      .map(value => value.name + '=' + value.value).join('; ');
    const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(5000),
      ...(fields ? { method: 'POST', body: new URLSearchParams(fields) } : {}),
      headers: { ...(cookies && cookie ? { cookie } : {}), ...(fields && originHeader !== null ? { Origin: originHeader || url.origin, 'Sec-Fetch-Site': 'same-origin' } : {}), ...headers } });
    if (cookies) save(response, url);
    const text = await response.text(); assert.ok(Buffer.byteLength(text) <= 65536, 'bounded fixture responses');
    const location = response.headers.get('location');
    return { status: response.status, text, headers: response.headers, location: location ? new URL(location, url) : null,
      body: response.headers.get('content-type')?.includes('application/json') ? JSON.parse(text) : null };
  }
  return { request, setCookie(origin, raw) { const url = new URL(origin), equal = raw.indexOf('='); jar.set(url.hostname + '\0/\0' + raw.slice(0, equal),
    { host: url.hostname, path: '/', name: raw.slice(0, equal), value: raw.slice(equal + 1), secure: false }); } };
}
export async function environment(t, { renewal = true, serviceDecorator = value => value, maxDatabaseBytes } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-human-oidc-')), clients = [];
  const secrets = [random(), random()], rps = await Promise.all(['app-alpha', 'app-beta'].map((clientId, index) => createHumanBffFixture({ clientId, clientSecret: secrets[index], sessionDatabasePath:join(directory,'rp-'+index+'.sqlite') })));
  let app, identity, connect;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  // Match the production transport. A slow synchronous quota fill must not
  // race the default 5s idle close with Undici's reused token connection.
  server.keepAliveTimeout = 65000; server.headersTimeout = 70000;
  await new Promise(done => server.listen(0, '127.0.0.1', done)); const origin = `http://127.0.0.1:${server.address().port}`, issuer = origin + '/human-identity';
  const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(privateJwk, { kid: 'human-fixture', alg: 'RS256', use: 'sig' });
  const options = { enabled: true, issuer, registryId: 'REG.soty', environmentId: 'fixture',
    clients: rps.map((rp, index) => ({ id: rp.clientId, label: 'Fixture ' + rp.clientId, redirectUri: rp.redirectUri, clientSecret: secrets[index], version:renewal ? 2 : 1 })),
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'human-fixture', ...(renewal ? {renewal:{admissionEnabled:true,clientIds:rps.map(rp=>rp.clientId)}} : {}) };
  let profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] });
  const identityPath = join(directory, 'human-identity', 'identity.sqlite'), distDir = join(directory, 'dist'); mkdirSync(distDir);
  writeFileSync(join(distDir, 'index.html'), '<!doctype html><title>Fixture root vault login</title><p>Existing Soty profile approval</p>');
  const create = () => {
    app = express();
    identity = createHumanIdentityService({ databasePath: identityPath, profile, allowRenewalMigration:profile.renewalAdmissionEnabled===true,
      ...(maxDatabaseBytes ? {maxDatabaseBytes} : {}), actorActive: actor => connect?.isActorActive(actor) === true,
      withAuthorityFence: callback => connect.withAuthorityFence(callback),
      readProfile: () => ({ name: 'Same display name', preferred_username: 'shared-profile' }) });
    connect = attachConnectModule(app, { dataDir: directory, origins: [origin], extensions: [identity] });
    attachHumanIdentity(app, { profile, service: serviceDecorator(identity), distDir });
    app.use((_req, res) => res.status(404).json({ error: 'not_found' }));
  };
  create(); await Promise.all(rps.map(rp => rp.configure(issuer)));
  t.after(async () => {
    clients.forEach(client => client.dispose()); server.closeAllConnections(); await new Promise(done => server.close(done));
    identity?.close(); connect?.close(); await Promise.all(rps.map(rp => rp.close()));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-human-oidc-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  async function newClient(label, bootstrap = true) {
    const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      fetch: (url, opts) => fetch(url, { ...opts, headers: { ...opts.headers, origin } }) }, memoryVault()); clients.push(client);
    return { client, ...(bootstrap ? { account: await client.bootstrap(label) } : {}) };
  }
  const actor = await newClient('Primary Soty vault'), outsider = await newClient('Independent other profile');
  const wire = browser(); let serial = 0;
  async function begin(rp = rps[0], session = wire, scope) {
    const start = await session.request(rp.origin + '/login' + (scope ? '?scope=' + encodeURIComponent(scope) : ''));
    assert.equal(start.status, 302); const authorize = await session.request(start.location);
    assert.ok([302, 303].includes(authorize.status), 'standard authorize reaches current-device interaction');
    assert.equal(authorize.location.origin, origin); const document = await session.request(authorize.location);
    assert.equal(document.status, 200, 'real root interaction document'); assert.doesNotMatch(document.text, /password|username|type="password"/iu);
    const context = await session.request(authorize.location.href + '/context');
    assert.equal(context.status, 200, 'context safe protocol status=' + context.status);
    return { rp, session, location: authorize.location, context: context.body, authorization: start.location };
  }
  async function approve(flow, signer = actor, changes = {}) {
    const args = { expectedAccountId: signer.account.accountId, interactionId: flow.context.interactionId,
      browserNonce: flow.context.browserNonce, csrf: flow.context.csrf, requestId: 'decision-' + (++serial), decision: 'approve', ...changes };
    return signer.client.extension('identity.human.approve', args, { expectedAccountId: signer.account.accountId });
  }
  async function complete(flow, fields = { csrf: flow.context.csrf }) {
    let response = await flow.session.request(flow.location.href + '/complete', { fields });
    assert.equal(response.status, 303, 'signed proof completed; safe error=' + (response.body?.error || 'none'));
    response = await flow.session.request(response.location);
    if (response.status === 200) {
      const action = /<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
      assert.equal(action, issuer + '/session/end/confirm');
      const hidden = Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(match => [match[1], match[2]]));
      response = await flow.session.request(action, { fields: hidden }); response = await flow.session.request(response.location);
    }
    assert.ok([302, 303].includes(response.status), 'standard resume issues exact RP callback, safe status=' + response.status);
    assert.equal(response.location.origin + response.location.pathname, flow.rp.redirectUri);
    return { ...flow, callback: response.location };
  }
  async function login(rp = rps[0], signer = actor, session = wire, scope, stayInAppSeconds = 0) {
    const flow = await begin(rp, session, scope); await approve(flow, signer, stayInAppSeconds ? {stayInAppSeconds} : {}); const finished = await complete(flow);
    const callback = await session.request(finished.callback); assert.equal(callback.status, 200, 'BFF verified callback; safe error=' + (callback.body?.error || 'none'));
    return finished;
  }
  return { origin, issuer, rps, actor, outsider, wire, begin, approve, complete, login, newClient, directory, identityPath, profile,
    get identity() { return identity; }, get connect() { return connect; },
    async restart() { identity.close(); connect.close(); create(); },
    async admission(enabled) { options.renewal.admissionEnabled=enabled;identity.close();connect.close();profile=createHumanIdentityHostProfile(options,{shellOrigins:[origin]});create();},
    async refresh(rp,refreshToken) {const client=options.clients.find(client=>client.id===rp.clientId);return wire.request(issuer+'/token',{cookies:false,originHeader:null,headers:{authorization:'Basic '+Buffer.from(encodeURIComponent(rp.clientId)+':'+encodeURIComponent(client?.clientSecret||secrets[rps.indexOf(rp)])).toString('base64')},fields:{grant_type:'refresh_token',refresh_token:refreshToken}});},
    async exchangeCode(flow, { clientId = flow.rp.clientId, verifier = flow.rp.pendingFixture().verifier } = {}) {
      const client = options.clients.find(value => value.id === clientId);
      return wire.request(issuer + '/token', { cookies: false, originHeader: null, headers: { authorization: 'Basic ' + Buffer.from(clientId + ':' + client.clientSecret).toString('base64') },
        fields: { grant_type: 'authorization_code', code: flow.callback.searchParams.get('code'), redirect_uri: flow.rp.redirectUri, code_verifier: verifier } });
    },
    async configureClients(change) {
      identity.close(); connect.close(); options.clients = change(structuredClone(options.clients)); if(options.renewal)options.renewal.clientIds=options.renewal.clientIds.filter(id=>options.clients.some(client=>client.id===id));
      profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] }); create();
    },
    tryClientConfiguration(change) {
      const candidate = createHumanIdentityHostProfile({ ...options, clients: change(structuredClone(options.clients)) }, { shellOrigins: [origin] });
      return createHumanIdentityService({ databasePath: identityPath, profile: candidate,
        actorActive: actor => connect.isActorActive(actor), withAuthorityFence: callback => connect.withAuthorityFence(callback) });
    },
    async addThirdRp() {
      const secret = random(), rp = await createHumanBffFixture({ clientId: 'app-gamma', clientSecret: secret }); rps.push(rp);
      options.clients.push({ id: rp.clientId, label: 'Independent gamma', redirectUri: rp.redirectUri, clientSecret: secret });
      identity.close(); connect.close(); profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] }); create();
      await rp.configure(issuer); return rp;
    },
    async backupDevice() {
      const backup = await newClient('Backup', false), pending = await backup.client.startEnrollment('Backup existing profile');
      await actor.client.approveEnrollment(pending.requestId, actor.account.accountId);
      await backup.client.previewEnrollment(pending.requestId);
      backup.account = await backup.client.finishEnrollment(pending.requestId, actor.account.accountId); return backup;
    },
  };
}
