import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomBytes, randomUUID, sign } from 'node:crypto';
import { fork } from 'node:child_process';
import { createServer, request as httpRequest, Agent } from 'node:http';
import { createConnection } from 'node:net';
import { once } from 'node:events';
import { mkdtempSync, realpathSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createNotesService } from '../../../modules/notes/server/index.mjs';
import { createCapabilitiesService } from '../../../modules/capabilities/server/index.mjs';
import { digestArgs } from '../../../modules/connect/server/index.mjs';

export const digest = value => createHash('sha256').update(value).digest('hex');
export const CREATE = '/api/capabilities/v1/notes/drafts';
const HISTORY = '/api/capabilities/v1/invocations/';
const EMPTY_INVOCATION = 'inv_00000000-0000-4000-8000-000000000000';
const SCOPE = 'notes.createDraft';
const KEY = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(KEY, { kid: 'two-as-fixture', use: 'sig', alg: 'RS256' });
const safeError = body => typeof body?.error === 'string' && /^[a-z_]{1,40}$/u.test(body.error) ? body.error : 'none';
export function expectTokens(response) {
  assert.equal(response.status, 200, 'actual token response; safe error=' + safeError(response.body));
  assert.ok(typeof response.body?.access_token === 'string' && typeof response.body?.refresh_token === 'string', 'actual AT and RT');
  assert.equal(response.body.token_type, 'Bearer'); assert.equal(response.body.scope, SCOPE); return response.body;
}
export function expectInvalidGrant(response) {
  assert.equal(response.status, 400); assert.equal(response.body?.error, 'invalid_grant');
}
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };

async function childProcess(args) {
  const child = fork(new URL('./oauth-as-worker.mjs', import.meta.url), [], { execPath: process.execPath,
    execArgv: [], windowsHide: true, stdio: ['pipe', 'ignore', 'pipe', 'ipc', 'pipe'] });
  child.stderr.resume(); // Provider diagnostics are never copied into test logs.
  const events = [], requests = new Map(), waits = new Set();
  let sequence = 0, input = '', ended = false, exitValue, resolveExit;
  const exited = new Promise(resolve => { resolveExit = resolve; });
  function fail(error) {
    for (const pending of requests.values()) { clearTimeout(pending.timer); pending.reject(error); } requests.clear();
    for (const pending of waits) { clearTimeout(pending.timer); pending.reject(error); } waits.clear();
  }
  child.once('error', () => { fail(new Error('fixture_child_error')); });
  child.once('close', (code, signal) => {
    ended = true; exitValue = { code, signal }; fail(new Error('fixture_child_closed')); resolveExit(exitValue);
  });
  child.on('message', message => {
    const pending = requests.get(message.requestId); if (!pending) return;
    requests.delete(message.requestId); clearTimeout(pending.timer);
    if (message.error) pending.reject(new Error('fixture_command_' + message.error)); else pending.resolve(message.result);
  });
  child.stdio[4].on('data', bytes => {
    input += bytes.toString('utf8');
    if (Buffer.byteLength(input) > 16384) { fail(new Error('fixture_event_buffer')); child.kill('SIGKILL'); return; }
    let newline;
    while ((newline = input.indexOf('\n')) >= 0) {
      const line = input.slice(0, newline); input = input.slice(newline + 1);
      try {
        assert.ok(Buffer.byteLength(line) <= 2048 && events.length < 512);
        const value = JSON.parse(line); events.push(value);
        for (const pending of [...waits]) if (pending.matches(value)) {
          waits.delete(pending); clearTimeout(pending.timer); pending.resolve(value);
        }
      } catch { fail(new Error('fixture_event_invalid')); child.kill('SIGKILL'); }
    }
  });
  const command = (command, value) => new Promise((resolve, reject) => {
    if (ended) { reject(new Error('fixture_child_closed')); return; }
    const requestId = ++sequence;
    const timer = setTimeout(() => { requests.delete(requestId); reject(new Error('fixture_command_timeout')); child.kill('SIGKILL'); }, 10000);
    requests.set(requestId, { resolve, reject, timer }); child.send({ requestId, command, args: value });
  });
  function waitFor(matches, label) {
    const previous = events.find(matches); if (previous) return Promise.resolve(previous);
    return new Promise((resolve, reject) => {
      if (ended) { reject(new Error('fixture_child_closed')); return; }
      const pending = { matches, resolve, reject, timer: null };
      pending.timer = setTimeout(() => { waits.delete(pending); reject(new Error('fixture_event_timeout_' + label)); child.kill('SIGKILL'); }, 10000);
      waits.add(pending);
    });
  }
  let ready;
  try { ready = await command('initialize', args); }
  catch (error) { child.kill('SIGKILL'); await exited; throw error; }
  return {
    port: ready.port, pid: ready.pid, events,
    arm: points => command('arm', points),
    wait(point) { return waitFor(event => event.kind === 'barrier' && event.point === point, point); },
    waitOperation(method, model, status, token) {
      const hash = token === undefined ? null : digest(token);
      return waitFor(event => event.kind === 'operation' && event.method === method && event.model === model
        && event.status === status && (hash === null || event.idHash === hash), method + '_' + model + '_' + status);
    },
    release() { assert.equal(ended, false); child.stdin.write(Buffer.from([82])); },
    async kill() { if (!ended) child.kill('SIGKILL'); await exited; return exitValue; },
    async close() {
      if (!ended) { try { await command('close'); } catch { child.kill('SIGKILL'); } }
      await exited;
    },
  };
}

/** A deliberately small, real three-domain HTTP composition. The production
 * profile/router/provider/adapters are used unchanged; full createHttpApp,
 * World/Apps, browser UI and public edge deployment are separate root gates. */
export async function twoASFixture(t) {
  const parent = realpathSync(tmpdir()), directory = realpathSync(mkdtempSync(path.join(parent, 'soty-two-as-')));
  const marker = randomUUID(); writeFileSync(path.join(directory, 'test-owner'), marker, { flag: 'wx' });
  const files = { caps: path.join(directory, 'capabilities', 'capabilities.sqlite'),
    notes: path.join(directory, 'notes', 'notes.sqlite'), connect: path.join(directory, 'connect', 'accounts.sqlite') };
  const workers = new Map(), allWorkers = [], routes = new Map();
  const front = createServer((req, res) => {
    // Routing is attached to the fixture-created TCP socket, not a client
    // header or forwarded identity. The canonical Host is untouched.
    const selected = routes.get(req.socket.remotePort), worker = workers.get(selected);
    if (!worker) { res.writeHead(503).end(); return; }
    const upstream = httpRequest({ hostname: '127.0.0.1', port: worker.port,
      method: req.method, path: req.url, headers: req.headers }, reply => {
      res.writeHead(reply.statusCode, reply.headers); reply.pipe(res);
    });
    upstream.once('error', () => { if (!res.headersSent) res.writeHead(502, { 'content-type': 'application/json' }).end('{"error":"fixture_transport_lost"}'); else res.destroy(); });
    req.once('aborted', () => upstream.destroy());
    res.once('close', () => { if (!res.writableEnded) upstream.destroy(); });
    req.pipe(upstream);
  });
  t.after(async () => {
    await Promise.all(allWorkers.map(worker => worker.close()));
    front.closeAllConnections(); await new Promise(resolve => front.close(resolve));
    assert.equal(realpathSync(directory), directory); assert.equal(path.dirname(directory), parent);
    assert.ok(path.basename(directory).startsWith('soty-two-as-'));
    assert.equal(readFileSync(path.join(directory, 'test-owner'), 'utf8'), marker);
    rmSync(directory, { recursive: true });
  });
  createNotesService({ databasePath: files.notes, projectId: 'soty', allowNativeMigration: true }).close();
  createCapabilitiesService({ databasePath: files.caps, projectId: 'soty', actorActive: () => false, allowNativeMigration: true }).close();
  createCapabilitiesService({ databasePath: files.caps, projectId: 'soty', actorActive: () => false, allowOAuthMigration: true }).close();
  await new Promise(resolve => front.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${front.address().port}`, issuer = origin + '/oauth';
  const keys = { jwks: { keys: [KEY] }, cookieKeys: [randomBytes(32).toString('base64url')],
    artifactKey: randomBytes(32).toString('base64url'), artifactKeyId: 'two-as-fixture' };
  async function start(name) {
    const worker = await childProcess({ directory, origin, keys, distDir: path.resolve('dist') });
    workers.set(name, worker); allWorkers.push(worker); return worker;
  }
  await start('a'); await start('b');
  assert.notEqual(workers.get('a').pid, workers.get('b').pid, 'two separate OS processes');

  async function wire(name, target, { method = 'GET', headers = {}, body } = {}) {
    assert.ok(workers.has(name)); const url = new URL(target, origin);
    assert.equal(url.origin, origin, 'never fetch an external callback');
    const socket = createConnection({ host: '127.0.0.1', port: front.address().port });
    await once(socket, 'connect'); const routeKey = socket.localPort; routes.set(routeKey, name);
    socket.once('close', () => routes.delete(routeKey));
    const agent = new Agent({ keepAlive: false }); agent.createConnection = () => socket;
    return new Promise((resolve, reject) => {
      const bytes = body === undefined ? undefined : Buffer.from(body);
      const req = httpRequest({ hostname: '127.0.0.1', port: front.address().port, method,
        path: url.pathname + url.search, agent, headers: { Host: url.host, Connection: 'close',
          ...(bytes ? { 'Content-Length': bytes.length } : {}), ...headers } }, response => {
        const buffer = Buffer.alloc(1048576); let length = 0;
        response.on('data', chunk => {
          if (length + chunk.length > buffer.length) { req.destroy(new Error('fixture_response_too_large')); return; }
          chunk.copy(buffer, length); length += chunk.length;
        });
        response.once('error', () => reject(new Error('fixture_response_lost')));
        response.once('end', () => {
          clearTimeout(timer); agent.destroy();
          try {
            const text = buffer.subarray(0, length).toString('utf8');
            resolve({ status: response.statusCode, headers: response.headers, text,
              location: response.headers.location ? new URL(response.headers.location, url) : null,
              ...(response.headers['content-type']?.includes('application/json') ? { body: JSON.parse(text) } : {}) });
          } catch { reject(new Error('fixture_response_invalid')); }
        });
      });
      const timer = setTimeout(() => req.destroy(new Error('fixture_http_timeout')), 15000);
      req.once('error', () => { clearTimeout(timer); agent.destroy(); reject(new Error('fixture_http_lost')); });
      req.end(bytes);
    });
  }
  function browser() {
    const cookies = new Map();
    return { async request(name, target, { fields } = {}) {
      const url = new URL(target, origin);
      const cookie = [...cookies.values()].filter(item => item.expires > Date.now()
        && (url.pathname === item.path || url.pathname.startsWith(item.path.endsWith('/') ? item.path : item.path + '/')))
        .sort((a, b) => b.path.length - a.path.length).map(item => item.pair).join('; ');
      const response = await wire(name, target, { ...(fields ? { method: 'POST', body: new URLSearchParams(fields).toString() } : {}),
        headers: { ...(cookie ? { Cookie: cookie } : {}), ...(fields ? { 'Content-Type': 'application/x-www-form-urlencoded',
          Origin: origin, 'Sec-Fetch-Site': 'same-origin' } : {}) } });
      for (const value of response.headers['set-cookie'] ?? []) {
        const [pair, ...parts] = value.split(';'), split = pair.indexOf('=');
        const attributes = Object.fromEntries(parts.map(part => {
          const equal = part.indexOf('='); return equal < 0 ? [part.trim().toLowerCase(), true]
            : [part.slice(0, equal).trim().toLowerCase(), part.slice(equal + 1).trim()];
        }));
        assert.equal(attributes.domain, undefined, 'host-only cookie'); assert.equal(attributes.secure, undefined, 'loopback HTTP profile');
        const item = { pair, path: attributes.path ?? '/', expires: attributes['max-age'] !== undefined
          ? Date.now() + Number(attributes['max-age']) * 1000 : attributes.expires ? Date.parse(attributes.expires) : Infinity };
        const key = pair.slice(0, split) + '\0' + item.path;
        if (!pair.slice(split + 1) || item.expires <= Date.now()) cookies.delete(key); else cookies.set(key, item);
        assert.ok(cookies.size <= 16, 'bounded fixture cookie jar');
      }
      return response;
    } };
  }
  const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const publicJwk = pair.publicKey.export({ format: 'jwk' });
  async function rpc(name, value) {
    return (await wire(name, '/api/connect/rpc', { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin },
      body: JSON.stringify({ protocol: 1, ...value }) })).body;
  }
  async function call(name, op, args = {}) {
    const challenge = good(await rpc(name, { op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return rpc(name, { op, args, proof: { challengeId: challenge.challengeId, publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: pair.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const account = good(await call('a', 'bootstrap', { label: 'Два AS — реальный владелец', encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) }));
  const ownerCall = (name, op, args = {}) => call(name, op, { expectedAccountId: account.accountId, ...args });
  async function begin(name = 'a') {
    const session = browser(), verifier = randomBytes(32).toString('base64url'), state = randomBytes(24).toString('base64url');
    const client = 'soty-codex-cli', redirectUri = 'http://127.0.0.1:19876/callback';
    const query = new URLSearchParams({ client_id: client, redirect_uri: redirectUri, response_type: 'code', resource: origin,
      scope: SCOPE, code_challenge_method: 'S256', code_challenge: createHash('sha256').update(verifier).digest('base64url'), state });
    const authorization = await session.request(name, '/oauth/authorize?' + query);
    assert.ok([302, 303].includes(authorization.status)); assert.equal(authorization.location?.origin, origin);
    assert.match(authorization.location.pathname, /^\/oauth\/interaction\/[A-Za-z0-9_-]{16,128}$/u);
    const pathname = authorization.location.pathname;
    assert.equal((await session.request(name, pathname)).status, 200);
    const context = await session.request(name, pathname + '/context'); assert.equal(context.status, 200);
    assert.equal(context.body.decision, 'pending');
    const decision = good(await ownerCall(name, 'oauth.connections.approve', {
      interactionId: context.body.interactionId, browserNonce: context.body.browserNonce, contextDigest: context.body.contextDigest }));
    return { session, pathname, client, redirectUri, resource: origin, verifier, state, connectionId: decision.connectionId };
  }
  async function complete(name, flow) {
    const response = await flow.session.request(name, flow.pathname + '/complete', { fields: { expectedAccountId: account.accountId } });
    assert.equal(response.status, 303); assert.equal(response.location?.origin, origin);
    const callback = await flow.session.request(name, response.location);
    assert.ok([302, 303].includes(callback.status));
    assert.equal(callback.location?.origin + callback.location?.pathname, flow.redirectUri);
    assert.ok(callback.location.searchParams.get('state') === flow.state && callback.location.searchParams.get('iss') === issuer);
    assert.equal(callback.location.searchParams.get('error'), null);
    const code = callback.location.searchParams.get('code'); assert.ok(typeof code === 'string' && code.length > 20);
    return { ...flow, code };
  }
  const form = (name, fields) => wire(name, '/oauth/token', { method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(fields).toString() });
  const exchange = (name, flow) => form(name, { grant_type: 'authorization_code', client_id: flow.client,
    redirect_uri: flow.redirectUri, resource: flow.resource, code: flow.code, code_verifier: flow.verifier });
  const refresh = (name, flow, tokens) => form(name, { grant_type: 'refresh_token', client_id: flow.client,
    resource: flow.resource, refresh_token: tokens.refresh_token });
  async function connection(name = 'a') {
    const flow = await complete(name, await begin(name)); return { flow, tokens: expectTokens(await exchange(name, flow)) };
  }
  function sql(store, action, writable = false) {
    const db = new DatabaseSync(files[store], { readOnly: !writable }); try { return action(db); } finally { db.close(); }
  }
  const state = id => sql('caps', db => {
    const own = db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(id);
    const root = db.prepare('SELECT revoked_at FROM cap_grants WHERE id=?').get(own.root_grant_id);
    const credentials = db.prepare(`SELECT k.id,k.created_at,k.expires_at,k.revoked_at FROM cap_credentials k
      JOIN cap_oauth_credentials l ON l.credential_id=k.id WHERE l.connection_id=? ORDER BY k.id`).all(id);
    return { state: own.state, revokedAt: own.revoked_at, rootRevokedAt: root.revoked_at,
      providerGrantId: own.provider_grant_id, clientId: own.client_id, principalId: own.principal_id,
      rootGrantId: own.root_grant_id, credentials,
      artifacts: Object.fromEntries(db.prepare('SELECT model,count(*) AS n FROM cap_oauth_artifacts WHERE connection_id=? GROUP BY model').all(id).map(row => [row.model, row.n])),
      invocations: db.prepare('SELECT count(*) AS n FROM cap_invocations WHERE client_id=?').get(own.client_id).n };
  });
  return { origin, issuer, directory, files, workers, accountId: account.accountId, wire, browser, begin, complete, exchange, refresh, connection, ownerCall, state, sql,
    async restart(name) { const previous = workers.get(name); await previous.kill(); const next = await start(name);
      assert.notEqual(previous.pid, next.pid, 'fresh OS process after crash'); return next; },
    source(model, token) { return sql('caps', db => db.prepare('SELECT consumed_at,expires_at FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').get(model, digest(token))); },
    assertLocksReleased() {
      for (const store of ['connect', 'caps']) sql(store, db => {
        db.exec('PRAGMA busy_timeout=100'); db.exec('BEGIN IMMEDIATE'); db.exec('ROLLBACK');
      }, true);
    },
    bearer(name, token) { return wire(name, HISTORY + EMPTY_INVOCATION, { headers: { Authorization: 'Bearer ' + token } }); },
    post(name, token, idempotencyKey) { return wire(name, CREATE, { method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'Два настоящих AS', body: 'Один приватный черновик', idempotencyKey }) }); },
  };
}
