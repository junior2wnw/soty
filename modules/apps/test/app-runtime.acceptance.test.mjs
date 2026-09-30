import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';

// Independent boundary harness: setup uses the actual public service operations,
// not direct registry INSERTs or an author's fixture. Browser HTTP/WS and the
// connector channel are real loopback sockets. The controllable connector is a
// protocol peer, allowing deliberate ordering of ACK/head/data/end messages.
const owner = { accountId: 'runtime_owner', deviceId: 'runtime_owner_device' };
const member = { accountId: 'runtime_member', deviceId: 'runtime_member_device' };
const visitor = { accountId: 'runtime_visitor', deviceId: 'runtime_visitor_device' };
const actors = [owner, member, visitor];
const CHUNK = 48 * 1024;
const sha = value => createHash('sha256').update(value).digest('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
function bounded(promise, label, ms = 2500) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })])
    .finally(() => clearTimeout(timer));
}
function frameLog() {
  const frames = [], observers = new Set();
  return {
    frames,
    push(frame) { const index = frames.push(frame) - 1; for (const entry of [...observers]) if (index >= entry.after && entry.match(frame)) { observers.delete(entry); entry.resolve(frame); } },
    wait(match, after = 0, ms = 2500) {
      const found = frames.slice(after).find(match); if (found) return Promise.resolve(found);
      const pending = deferred(), entry = { ...pending, match, after }; observers.add(entry);
      return bounded(pending.promise, 'connector frame', ms).finally(() => observers.delete(entry));
    },
    close() { for (const entry of observers) entry.reject(new Error('fixture closed')); observers.clear(); },
  };
}
function serverText(value) {
  const body = Buffer.from(value); assert.ok(body.length < 126);
  return Buffer.concat([Buffer.from([0x81, body.length]), body]);
}

async function fixture(t, { auditMs = 10_000, namedOnly = false, secure = false } = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-runtime-acceptance-'));
  const services = [], sockets = new Set(), clients = new Set(), requests = new Set();
  const log = frameLog(), streams = new Map(), behaviors = new Map(), barriers = new Map();
  const members = new Set([owner.accountId, member.accountId]), admins = new Set([owner.accountId]), revoked = new Set();
  let clock = Date.now(), service, listener, connector, sequence = 0, closing = false;
  const networkErrors = [], requestTrace = [];
  const server = createServer((req, res) => {
    const trace = { path: req.url, method: req.method, events: [] }; requestTrace.push(trace);
    req.once('close', () => trace.events.push(`request:close:${req.complete}:${req.destroyed}`));
    req.once('aborted', () => trace.events.push('request:aborted'));
    res.once('finish', () => trace.events.push('response:finish'));
    res.once('close', () => trace.events.push('response:close'));
    const barrier = barriers.get(req.url?.split('?')[0]);
    if (barrier) {
      // A real write completes first; only delivery of its completion callback
      // is held. This tests the after-await fence, not OS backpressure or RSS.
      const write = res.write.bind(res);
      res.write = (...args) => {
        const last = args.length - 1, callback = args[last];
        if (typeof callback === 'function') args[last] = error => {
          barrier.written.resolve(); void barrier.release.promise.then(() => callback(error));
        };
        return write(...args);
      };
    }
    if (!service?.handleRequest(req, res)) { res.writeHead(404, { 'x-fixture-shell': 'outside' }); res.end('outside'); }
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    closing = true;
    for (const barrier of barriers.values()) barrier.release.resolve();
    for (const req of requests) req.destroy();
    for (const ws of clients) ws.terminate();
    connector?.terminate();
    for (const item of services) item.close();
    for (const socket of sockets) socket.destroy();
    log.close();
    if (server.listening) await bounded(new Promise(resolve => server.close(resolve)), 'fixture server close');
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.match(path.basename(directory), /^soty-runtime-acceptance-/u);
    await rm(directory, { recursive: true, force: true });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const shellOrigin = secure ? 'https://shell.example.com' : `http://localhost:${port}`;
  const namedAppZone = secure ? 'https://apps.example.net' : `http://apps.localhost:${port}`;
  const legacy = namedOnly ? '' : secure ? 'https://{appId}.legacy.example.org' : `http://{appId}.legacy.localhost:${port}`;
  const config = {
    databasePath: path.join(directory, 'registry.sqlite'), shellOrigins: [shellOrigin], appOriginTemplate: legacy, namedAppZone,
    now: () => clock, accessAuditMs: auditMs, connectorAuthCheckMs: 30_000,
    actorActive: actor => actors.some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId) && !revoked.has(actor.deviceId),
    canAccessCommunity: (accountId, group) => group === 'runtime_circle' && members.has(accountId),
    isGroupAdmin: (accountId, group) => group === 'runtime_circle' && admins.has(accountId),
    activeCommunityIds: accountId => members.has(accountId) ? ['runtime_circle'] : [],
    subscribeMembership: fn => { listener = fn; return () => {}; },
    authenticateConnector: async auth => auth.linkId === 'runtime_link' && auth.deviceId === 'runtime_host'
      && auth.connectorId === 'runtime_connector' && auth.token === 'synthetic-runtime-token',
  };
  service = createAppsService(config); services.push(service);
  const send = frame => { assert.equal(connector.readyState, WebSocket.OPEN); connector.send(JSON.stringify(frame)); };
  function peerData(id, bytes) {
    const item = streams.get(id); assert.ok(item); const seq = ++item.responseSeq;
    const after = log.frames.length;
    send({ type: 'data', id, seq, data: Buffer.from(bytes).toString('base64') });
    return { seq, acknowledged: () => log.wait(frame => frame.type === 'ack' && frame.id === id && frame.seq === seq, after) };
  }
  async function respond(item) {
    const behavior = behaviors.get(item.path) || {};
    if (behavior.hold) return;
    const bytes = behavior.body ?? Buffer.concat(item.body);
    send({ type: 'head', id: item.id, status: behavior.status ?? 200,
      headers: { 'content-type': 'application/octet-stream', ...(behavior.headers || {}) }, ...(behavior.location ? { location: behavior.location } : {}) });
    for (let offset = 0; offset < bytes.length; offset += CHUNK) await peerData(item.id, bytes.subarray(offset, offset + CHUNK)).acknowledged();
    send({ type: 'end', id: item.id });
  }
  connector = new WebSocket(`ws://127.0.0.1:${port}/api/apps/channel`, { headers: { Host: new URL(shellOrigin).host } });
  connector.on('error', () => {});
  connector.on('message', bytes => {
    const frame = JSON.parse(bytes.toString()); log.push(frame);
    if (frame.type === 'open') {
      const item = { ...frame, path: frame.path.split('?')[0], body: [], responseSeq: 0 }; streams.set(frame.id, item);
      if (frame.kind === 'ws' && !behaviors.get(item.path)?.holdHead) {
        const accept = createHash('sha1').update(frame.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
        send({ type: 'head', id: frame.id, status: 101, headers: { 'sec-websocket-accept': accept } });
      }
    } else if (frame.type === 'data') {
      const item = streams.get(frame.id); if (!item) return;
      item.body.push(Buffer.from(frame.data, 'base64'));
      if (!behaviors.get(item.path)?.holdAck) send({ type: 'ack', id: frame.id, seq: frame.seq });
    } else if (frame.type === 'end') {
      const item = streams.get(frame.id);
      if (item?.kind === 'http') void respond(item).catch(error => { if (!closing) networkErrors.push(error); });
    }
  });
  await bounded(new Promise((resolve, reject) => { connector.once('open', resolve); connector.once('error', reject); }), 'connector open');
  send({ type: 'auth', schema: 'soty.apps-channel.v1', linkId: 'runtime_link', hostDeviceId: 'runtime_host',
    connectorId: 'runtime_connector', token: 'synthetic-runtime-token', name: 'Independent runtime fixture' });
  await log.wait(frame => frame.type === 'ready');
  const call = (op, args = {}, actor = owner, targetService = service) => targetService.execute({ op, args: { expectedAccountId: actor.accountId, ...args }, actor });
  const claimCode = randomBytes(32).toString('base64url'); send({ type: 'claim', claimDigest: sha(claimCode) });
  await log.wait(frame => frame.type === 'claim-ready');
  call('apps.claim', { hostDeviceId: 'runtime_host', connectorId: 'runtime_connector', claimCode });
  const app = call('apps.register', { hostDeviceId: 'runtime_host', connectorId: 'runtime_connector', name: 'Independent private app',
    port: 32123, entryPath: '/start', grants: { communityIds: ['runtime_circle'] } }).app;
  await log.wait(frame => frame.type === 'sync' && frame.apps.some(value => value.id === app.id));
  const canonical = call('apps.domains.get', { appId: app.id }).domains.find(domain => domain.role === 'canonical');
  function alias(slug, appId = app.id) {
    const current = call('apps.domains.get', { appId });
    const result = call('apps.domains.claim', { appId, slug, expectedDomainsRevision: current.revision, requestId: `claim-${++sequence}` });
    return call('apps.domains.get', { appId }).domains.find(domain => domain.id === result.receipt.domainId);
  }
  const primary = alias('primary'), alternate = alias('alternate');
  function publish({ policy = 'anyone', aliases = [primary, alternate], appId = app.id, listed = false, targetService = service } = {}) {
    const current = call('apps.publication.get', { appId }, owner, targetService);
    return call('apps.publication.update', { appId, requestId: `publish-${++sequence}`, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, launchPolicy: policy, listed, activeDomainIds: aliases.map(domain => domain.id),
      ...(policy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: current.target.revision, targetDigest: current.target.digest, profile: current.target.profile } } : {}) }, owner, targetService);
  }
  function begin(target, localPath, { method = 'GET', cookie, origin, headers = {}, onResponse, agent, completionMs = 5000 } = {}) {
    const response = deferred(), done = deferred(), events = []; let observed;
    const req = request({ hostname: '127.0.0.1', port, path: localPath, method, ...(agent === undefined ? {} : { agent }),
      headers: { Host: new URL(target.origin).host, ...(cookie === undefined ? {} : { Cookie: cookie }),
        ...(origin === undefined ? {} : { Origin: origin }), ...headers } }, res => {
      observed = res; const chunks = []; events.push(`response:${res.statusCode}`);
      response.resolve({ status: res.statusCode, headers: res.headers, raw: res }); onResponse?.(res);
      res.on('data', chunk => chunks.push(chunk));
      const finish = () => done.resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks), complete: res.complete });
      for (const event of ['end', 'aborted', 'error']) res.once(event, () => { events.push(`response:${event}`); finish(); });
      res.once('close', () => events.push('response:close'));
    });
    requests.add(req); req.once('close', () => { events.push('request:close'); requests.delete(req); });
    req.once('finish', () => events.push('request:finish'));
    req.once('error', error => { events.push(`request:error:${error.code}`); if (!observed) { response.resolve({ status: 0, error }); done.resolve({ status: 0, body: Buffer.alloc(0), complete: false, error }); } });
    return { req, response: response.promise, done: bounded(done.promise, 'HTTP completion', completionMs).catch(error => { error.message += ` (${method} ${localPath}; reused=${req.reusedSocket}; ${events.join(', ')})`; throw error; }) };
  }
  async function http(target, localPath, options = {}) {
    const headers = { ...(options.body === undefined ? {} : { 'content-length': Buffer.byteLength(options.body) }), ...options.headers };
    const pending = begin(target, localPath, { ...options, headers }); pending.req.end(options.body); return pending.done;
  }
  function launch(actor = owner, target = canonical, localPath) {
    return call('apps.launch', { appId: app.id, ...(target?.role === 'alias' ? { domainId: target.id } : {}), ...(localPath === undefined ? {} : { path: localPath }) }, actor);
  }
  async function exchange(value, target, options = {}) {
    const url = new URL(value.launchUrl);
    return http(target ?? { origin: url.origin }, '/_soty/session', { method: 'POST', origin: url.origin,
      headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }), ...options });
  }
  async function cookie(actor = owner, target = canonical, localPath) {
    const response = await exchange(launch(actor, target, localPath), target);
    assert.equal(response.status, 200, response.body.toString());
    assert.equal(response.headers['set-cookie']?.length, 1);
    return response.headers['set-cookie'][0].split(';')[0];
  }
  async function websocket(target, { cookie, origin = target.origin, localPath = '/socket', headers = {} } = {}) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}${localPath}`, { headers: { Host: new URL(target.origin).host,
      ...(origin === null ? {} : { Origin: origin }), ...(cookie === undefined ? {} : { Cookie: cookie }), ...headers } });
    clients.add(ws); ws.once('close', () => clients.delete(ws)); ws.on('error', () => {});
    await bounded(new Promise((resolve, reject) => {
      ws.once('open', resolve); ws.once('error', reject);
      ws.once('unexpected-response', (_request, response) => { response.resume(); ws.terminate(); reject(Object.assign(new Error('upgrade denied'), { status: response.statusCode })); });
    }), 'visitor WS open');
    return ws;
  }
  function secondService() {
    const other = createAppsService({ ...config, subscribeMembership: undefined }); services.push(other); return other;
  }
  return { directory, server, port, service, config, app, canonical, primary, alternate, alias, publish, call, launch, exchange, cookie,
    begin, http, websocket, send, peerData, log, streams, behaviors, barriers, members, admins, revoked, networkErrors, requestTrace, secondService,
    advance(ms) { clock += ms; }, now: () => clock,
    notify() { listener?.({ communityId: 'runtime_circle', profileId: member.accountId, state: 'removed' }); },
    retire(domain) { const current = call('apps.domains.get', { appId: app.id }); return call('apps.domains.retire', { appId: app.id, domainId: domain.id, expectedDomainsRevision: current.revision, requestId: `retire-${++sequence}` }); },
  };
}

function denied(response, context = '') { assert.ok([401, 403].includes(response.status), `expected access refusal, got ${response.status} ${context}`); }
function frameCount(f, id, type) { return f.log.frames.filter(frame => frame.id === id && frame.type === type).length; }
async function closed(ws) { if (ws.readyState === WebSocket.CLOSED) return; await bounded(new Promise(resolve => ws.once('close', resolve)), 'visitor closed'); }

test('independent real-socket baseline: canonical HTTP bytes and WebSocket survive without leaking visitor credentials', async t => {
  const f = await fixture(t), cookie = await f.cookie(member);
  const payload = Buffer.from('independent transfer '.repeat(8000));
  const response = await f.http(f.canonical, '/echo', { method: 'POST', cookie, origin: f.canonical.origin,
    headers: { authorization: 'Bearer synthetic-not-forwarded', 'content-type': 'application/octet-stream' }, body: payload });
  assert.equal(response.status, 200); assert.equal(response.complete, true); assert.equal(sha(response.body), sha(payload));
  const opened = f.log.frames.find(frame => frame.type === 'open' && frame.path === '/echo');
  assert.equal(opened.headers.cookie, undefined); assert.equal(opened.headers.authorization, undefined);
  const ws = await f.websocket(f.canonical, { cookie });
  const wsOpen = f.log.frames.find(frame => frame.type === 'open' && frame.kind === 'ws');
  const message = bounded(new Promise(resolve => ws.once('message', resolve)), 'WS data');
  await f.peerData(wsOpen.id, serverText('independent websocket')).acknowledged();
  assert.equal((await message).toString(), 'independent websocket');
  ws.terminate(); assert.deepEqual(f.networkErrors, []);
});

test('named public and unlisted access is exact: a new claim, canonical and retired address do not inherit it', async t => {
  const f = await fixture(t); f.publish();
  assert.equal((await f.http(f.primary, '/')).status, 200);
  denied(await f.http(f.canonical, '/'));
  const third = f.alias('brand-new');
  const before = f.log.frames.length;
  assert.equal((await f.http(third, '/')).status, 503);
  assert.equal(f.log.frames.slice(before).filter(frame => frame.type === 'open').length, 0);
  f.retire(f.alternate);
  assert.equal((await f.http(f.alternate, '/')).status, 410);
  const wrongPort = { origin: f.primary.origin.replace(`:${f.port}`, ':1') };
  assert.equal((await f.http(wrongPort, '/')).status, 404);
  assert.equal((await f.http({ origin: f.primary.origin.replace('primary.', 'missing.') }, '/')).status, 404);
});

test('presented invalid or cross-alias account cookies never fall through to anonymous public access', async t => {
  const f = await fixture(t); f.publish(); const cookie = await f.cookie(member, f.primary);
  assert.equal((await f.http(f.primary, '/', { cookie })).status, 200);
  for (const value of [cookie, '__Host-soty_app_session=', '__Host-soty_app_session=invalid',
    '__Host-soty_app_session=' + 'a'.repeat(43), `${cookie}; ${cookie}`]) {
    denied(await f.http(f.alternate, '/', { cookie: value }));
  }
  f.revoked.add(member.deviceId);
  denied(await f.http(f.primary, '/', { cookie }));
  assert.equal((await f.http(f.primary, '/')).status, 200, 'a separate cookie-free request can be public');
  denied(await f.http(f.primary, '/_soty/session'));
});

test('session verify is read-only, exact-origin and absolute one-hour; cookie attributes do not confer cross-alias access', async t => {
  const f = await fixture(t); f.publish();
  const launched = f.launch(visitor, f.primary, '/deep?key=local');
  const response = await f.exchange(launched, f.primary);
  assert.equal(response.status, 200); assert.equal(JSON.parse(response.body).entryPath, '/deep?key=local');
  const setCookie = response.headers['set-cookie'][0], cookie = setCookie.split(';')[0];
  for (const expected of [/^__Host-soty_app_session=/u, /;\s*HttpOnly/iu, /;\s*Secure/iu, /;\s*Path=\//iu, /;\s*SameSite=None/iu, /;\s*Partitioned/iu]) assert.match(setCookie, expected);
  assert.doesNotMatch(setCookie, /;\s*Domain=/iu);
  const verified = await f.http(f.primary, '/_soty/session', { cookie });
  assert.equal(verified.status, 200); assert.equal(JSON.parse(verified.body).ok, true); assert.equal(verified.headers['set-cookie'], undefined);
  f.advance(3_599_999);
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie })).status, 200);
  f.advance(1);
  denied(await f.http(f.primary, '/_soty/session', { cookie }));
  denied(await f.http(f.primary, '/', { cookie }));
});

test('ticket exchange is single-use under parallel requests and preserves server-validated local path', async t => {
  const f = await fixture(t); f.publish();
  for (const value of ['https://evil.example/', '//evil.example/', '/%2fhost', '/%5cevil', '/_soty/session', '/bad\r\nheader']) {
    assert.throws(() => f.launch(owner, f.primary, value));
  }
  const launched = f.launch(owner, f.primary, '/deep/route?q=a%2Fb');
  const responses = await Promise.all([f.exchange(launched, f.primary), f.exchange(launched, f.primary)]);
  assert.equal(responses.filter(response => response.status === 200).length, 1);
  denied(responses.find(response => response.status !== 200));
  assert.equal(JSON.parse(responses.find(response => response.status === 200).body).entryPath, '/deep/route?q=a%2Fb');
  assert.equal(f.log.frames.filter(frame => frame.type === 'open').length, 0, 'exchange does not execute the application');
  const other = f.launch(owner, f.primary);
  denied(await f.exchange(other, f.alternate, { origin: f.alternate.origin }));
});

test('delayed ticket body rechecks grants, actor, policy and retired address after the await', async t => {
  for (const change of ['grants', 'actor', 'policy', 'retire']) await t.test(change, async t => {
    const f = await fixture(t); f.publish({ policy: 'restricted' });
    const launched = f.launch(member, f.primary), ticket = new URL(launched.launchUrl).hash.slice(1);
    const pending = f.begin(f.primary, '/_soty/session', { method: 'POST', origin: f.primary.origin, headers: { 'content-type': 'application/json' } });
    pending.req.write('{"ticket":"'); await pause(20);
    if (change === 'grants') f.call('apps.update', { appId: f.app.id, grants: {} });
    if (change === 'actor') f.revoked.add(member.deviceId);
    if (change === 'policy') f.publish({ policy: 'restricted', aliases: [] });
    if (change === 'retire') f.retire(f.primary);
    pending.req.end(ticket + '"}'); const response = await pending.done;
    // B3 re-reads the address when producing the failure page. Its existing
    // status contract is more specific than a generic expired-ticket refusal.
    if (change === 'policy') assert.equal(response.status, 503);
    else if (change === 'retire') assert.equal(response.status, 410);
    else denied(response);
    assert.equal(response.headers['set-cookie'], undefined); assert.equal(response.headers.location, undefined);
    assert.equal(f.log.frames.filter(frame => frame.type === 'open').length, 0);
  });
});

test('ticket expiry at the body boundary and a foreign application never produce an account cookie', async t => {
  const f = await fixture(t); f.publish();
  const other = f.call('apps.register', { hostDeviceId: 'runtime_host', connectorId: 'runtime_connector',
    name: 'Separate app', port: 32124, entryPath: '/', grants: {} }).app;
  const otherDomain = f.alias('separate-app', other.id); f.publish({ appId: other.id, aliases: [otherDomain] });
  assert.throws(() => f.call('apps.launch', { appId: f.app.id, domainId: otherDomain.id }));
  const foreign = await f.exchange(f.launch(owner, f.primary), otherDomain, { origin: otherDomain.origin });
  denied(foreign); assert.equal(foreign.headers['set-cookie'], undefined);

  const launched = f.launch(owner, f.primary), ticket = new URL(launched.launchUrl).hash.slice(1);
  const pending = f.begin(f.primary, '/_soty/session', { method: 'POST', origin: f.primary.origin });
  pending.req.write('{"ticket":"'); await pause(20);
  f.advance(30_000); pending.req.end(ticket + '"}');
  const expired = await pending.done; denied(expired); assert.equal(expired.headers['set-cookie'], undefined);
  assert.equal(f.log.frames.filter(frame => frame.type === 'open').length, 0);
  assert.equal((await f.exchange(f.launch(owner, f.primary), f.primary)).status, 200);
});

test('unsafe Origin and WS Origin are required before admission, including duplicate values; public GET/HEAD remain usable', async t => {
  const f = await fixture(t); f.publish(); const before = f.log.frames.length;
  for (const method of ['POST', 'PUT', 'PATCH', 'DELETE']) for (const origin of [undefined, 'null', 'https://elsewhere.example', `${f.primary.origin}, ${f.primary.origin}`]) {
    denied(await f.http(f.primary, '/unsafe', { method, origin, body: 'must not execute' }), `${method} origin=${origin}`);
  }
  denied(await f.http(f.primary, '/unsafe', { method: 'POST', headers: { Origin: [f.primary.origin, f.primary.origin] }, body: 'must not execute' }));
  assert.equal(f.log.frames.slice(before).filter(frame => frame.type === 'open').length, 0);
  assert.equal((await f.http(f.primary, '/')).status, 200);
  assert.equal((await f.http(f.primary, '/', { method: 'HEAD' })).status, 200);
  await assert.rejects(f.websocket(f.primary, { origin: null }));
  for (const headers of [{ Origin: '' }, { Origin: 'null' }, { Origin: 'https://elsewhere.example' }, { Origin: [f.primary.origin, f.primary.origin] }]) {
    await assert.rejects(f.websocket(f.primary, { headers }));
  }
});

test('revocation while waiting for request ACK prevents the next chunk and end without killing a neighboring stream', async t => {
  for (const length of [16, CHUNK * 3]) await t.test(`${length}-byte upload`, async t => {
    const f = await fixture(t), memberCookie = await f.cookie(member), ownerCookie = await f.cookie(owner);
    const neighbor = await f.websocket(f.canonical, { cookie: ownerCookie, localPath: '/neighbor' });
    f.behaviors.set('/blocked-upload', { holdAck: true, hold: true });
    const pending = f.begin(f.canonical, '/blocked-upload', { method: 'POST', origin: f.canonical.origin, cookie: memberCookie });
    pending.req.end(Buffer.alloc(length, 81));
    const open = await f.log.wait(frame => frame.type === 'open' && frame.path === '/blocked-upload');
    const data = await f.log.wait(frame => frame.type === 'data' && frame.id === open.id);
    f.members.delete(member.accountId); f.notify();
    await f.log.wait(frame => frame.type === 'cancel' && frame.id === open.id);
    f.send({ type: 'ack', id: open.id, seq: data.seq }); await pending.done; await pause(30);
    assert.equal(frameCount(f, open.id, 'data'), 1); assert.equal(frameCount(f, open.id, 'end'), 0);
    assert.equal(neighbor.readyState, WebSocket.OPEN);
    // Use the ordinary keep-alive Agent here: forcing a new connection would
    // hide the upload-abort regression in which a dead socket was reused.
    const healthy = await f.http(f.canonical, '/', { cookie: ownerCookie }).catch(error => {
      t.diagnostic(JSON.stringify(f.requestTrace)); throw error;
    });
    assert.equal(healthy.status, 200);
  });
});

test('a real response write completed before revoke cannot send a late acknowledgement after its callback resumes', async t => {
  const f = await fixture(t), cookie = await f.cookie(member);
  const barrier = { written: deferred(), release: deferred() }; f.barriers.set('/write-barrier', barrier); f.behaviors.set('/write-barrier', { hold: true });
  const pending = f.begin(f.canonical, '/write-barrier', { cookie }); pending.req.end();
  const open = await f.log.wait(frame => frame.type === 'open' && frame.path === '/write-barrier');
  f.send({ type: 'head', id: open.id, status: 200, headers: { 'content-type': 'text/plain' } });
  f.peerData(open.id, Buffer.from('already written prefix'));
  await bounded(barrier.written.promise, 'real response write');
  f.members.delete(member.accountId); f.notify();
  await f.log.wait(frame => frame.type === 'cancel' && frame.id === open.id);
  barrier.release.resolve(); const result = await pending.done; await pause(25);
  assert.equal(frameCount(f, open.id, 'ack'), 0); assert.equal(result.complete, false);
});

test('an epoch changed by another SQLite connection blocks delayed head/data and audits idle WS without a local event', async t => {
  const f = await fixture(t, { auditMs: 25 }); f.publish();
  const peer = f.secondService();
  const ws = await f.websocket(f.primary);
  f.behaviors.set('/late-head', { hold: true });
  const pending = f.begin(f.primary, '/late-head'); pending.req.end();
  const open = await f.log.wait(frame => frame.type === 'open' && frame.path === '/late-head');
  f.publish({ policy: 'restricted', aliases: [], targetService: peer });
  f.send({ type: 'head', id: open.id, status: 200, headers: { 'content-type': 'text/plain' } });
  const result = await pending.done; assert.notEqual(result.status, 200);
  await closed(ws);
  assert.equal((await f.http(f.canonical, '/', { cookie: await f.cookie(owner) })).status, 200);
});

test('anonymous leases renew before expiry, but expired or policy-changed streams never silently get a new decision', async t => {
  const f = await fixture(t); f.publish(); const ws = await f.websocket(f.primary);
  const opened = f.log.frames.find(frame => frame.type === 'open' && frame.kind === 'ws');
  for (const step of [20_000, 20_000]) {
    f.advance(step); const at = f.log.frames.length; ws.send('lease activity');
    await f.log.wait(frame => frame.type === 'data' && frame.id === opened.id, at);
  }
  assert.equal(ws.readyState, WebSocket.OPEN, 'lease can roll beyond the original 30 seconds');
  f.advance(30_000); ws.send('expired activity');
  await closed(ws);
  assert.equal(frameCount(f, opened.id, 'data'), 2, 'expiry must precede forwarding a third frame');
});

test('an absolute account deadline closes only its old stream while a newly authorized neighbor keeps working', async t => {
  const f = await fixture(t); f.publish();
  const oldCookie = await f.cookie(visitor, f.primary);
  const old = await f.websocket(f.primary, { cookie: oldCookie, localPath: '/old-account' });
  const opened = f.log.frames.find(frame => frame.type === 'open' && frame.path === '/old-account');
  f.advance(3_599_900);
  // Activity near expiry must not move the account session deadline.
  old.send('before deadline'); await f.log.wait(frame => frame.type === 'data' && frame.id === opened.id);
  const freshCookie = await f.cookie(owner, f.primary);
  const neighbor = await f.websocket(f.primary, { cookie: freshCookie, localPath: '/fresh-account' });
  f.advance(100); old.send('at deadline'); await closed(old);
  assert.equal(frameCount(f, opened.id, 'data'), 1);
  denied(await f.http(f.primary, '/', { cookie: oldCookie }));
  const freshOpen = f.log.frames.find(frame => frame.type === 'open' && frame.path === '/fresh-account');
  const message = bounded(new Promise(resolve => neighbor.once('message', resolve)), 'fresh neighbor WS data');
  await f.peerData(freshOpen.id, serverText('still authorized')).acknowledged();
  assert.equal((await message).toString(), 'still authorized');
  assert.equal((await f.http(f.primary, '/', { cookie: freshCookie })).status, 200);
});

test('external policy change is enforced before late data/end/ACK frames without relying on membership events', async t => {
  for (const boundary of ['data', 'end', 'ack']) await t.test(boundary, async t => {
    const f = await fixture(t); f.publish(); const other = f.secondService();
    f.behaviors.set('/external-boundary', { hold: true, holdAck: true });
    const pending = f.begin(f.primary, '/external-boundary', boundary === 'ack'
      ? { method: 'POST', origin: f.primary.origin } : {});
    pending.req.end(boundary === 'ack' ? Buffer.alloc(CHUNK * 3, 67) : undefined);
    const open = await f.log.wait(frame => frame.type === 'open' && frame.path === '/external-boundary');
    let data;
    if (boundary === 'ack') data = await f.log.wait(frame => frame.type === 'data' && frame.id === open.id);
    else {
      await f.log.wait(frame => frame.type === 'end' && frame.id === open.id);
      f.send({ type: 'head', id: open.id, status: 200, headers: { 'content-type': 'text/plain' } });
      // Prove the head was accepted and one prefix delivered before mutation.
      await f.peerData(open.id, Buffer.from('authorized prefix')).acknowledged();
    }
    f.publish({ policy: 'restricted', aliases: [], targetService: other });
    const before = f.log.frames.length;
    if (boundary === 'data') f.peerData(open.id, Buffer.from('MUST-NOT-ARRIVE'));
    if (boundary === 'end') f.send({ type: 'end', id: open.id });
    if (boundary === 'ack') f.send({ type: 'ack', id: open.id, seq: data.seq });
    await f.log.wait(frame => frame.type === 'cancel' && frame.id === open.id, before);
    const result = await pending.done;
    assert.doesNotMatch(result.body.toString(), /MUST-NOT-ARRIVE/u);
    if (boundary !== 'ack') assert.equal(result.complete, false, 'revocation cannot present a truncated 200 as complete');
    if (boundary === 'ack') { assert.equal(frameCount(f, open.id, 'data'), 1); assert.equal(frameCount(f, open.id, 'end'), 0); }
    assert.equal(f.log.frames.slice(before).some(frame => frame.id === open.id && frame.type === 'ack'), false);
    assert.equal((await f.http(f.canonical, '/', { cookie: await f.cookie(owner) })).status, 200);
  });
});

test('losing a grant on an anyone app closes the old grant session; separately opening the public app remains possible', async t => {
  const f = await fixture(t); f.publish(); const cookie = await f.cookie(member, f.primary);
  const ws = await f.websocket(f.primary, { cookie });
  f.members.delete(member.accountId); f.notify(); await closed(ws);
  denied(await f.http(f.primary, '/', { cookie }));
  assert.equal((await f.http(f.primary, '/')).status, 200);
});

test('public capacity includes signed-public sessions, stays public after membership promotion, and leaves eight grant slots', async t => {
  const f = await fixture(t); f.publish(); const publicCookie = await f.cookie(visitor, f.primary), ownCookie = await f.cookie(owner, f.primary);
  const publicSockets = [];
  for (let i = 0; i < 24; i++) publicSockets.push(await f.websocket(f.primary, { ...(i % 2 ? { cookie: publicCookie } : {}), localPath: `/public-${i}` }));
  f.members.add(visitor.accountId); f.notify();
  // Existing public-basis sessions do not gain reserved capacity on recheck.
  await assert.rejects(f.websocket(f.primary, { cookie: publicCookie, localPath: '/public-overflow' }), error => error.status === 429 || /429/u.test(error.message));
  const ownSockets = [];
  for (let i = 0; i < 8; i++) ownSockets.push(await f.websocket(f.primary, { cookie: ownCookie, localPath: `/owner-${i}` }));
  await assert.rejects(f.websocket(f.primary, { cookie: ownCookie, localPath: '/total-overflow' }), error => error.status === 429 || /429/u.test(error.message));
  const victim = publicSockets.shift(); victim.terminate(); await closed(victim);
  const replacement = await f.websocket(f.primary, { cookie: publicCookie, localPath: '/public-replacement' });
  assert.equal(replacement.readyState, WebSocket.OPEN);
  for (const socket of [...publicSockets, ...ownSockets, replacement]) socket.terminate();
});

test('held HTTP and WebSocket share public capacity; source failure and client disconnect each release their own slot', async t => {
  const f = await fixture(t); f.publish(); const ownCookie = await f.cookie(owner, f.primary);
  const held = [], sockets = [];
  for (let i = 0; i < 12; i++) {
    const localPath = `/held-${i}`; f.behaviors.set(localPath, { hold: true });
    const pending = f.begin(f.primary, localPath); pending.req.end();
    const open = await f.log.wait(frame => frame.type === 'open' && frame.path === localPath); held.push({ pending, open });
    sockets.push(await f.websocket(f.primary, { localPath: `/mixed-${i}` }));
  }
  assert.equal((await f.http(f.primary, '/public-overflow')).status, 429);
  assert.equal((await f.http(f.primary, '/private-reserve', { cookie: ownCookie })).status, 200);

  const failed = held.shift(); f.send({ type: 'cancel', id: failed.open.id, error: 'app_stopped' });
  assert.equal((await failed.pending.done).status, 502);
  sockets.push(await f.websocket(f.primary, { localPath: '/after-source-error' }));
  assert.equal((await f.http(f.primary, '/still-full')).status, 429);

  const disconnected = held.shift(); disconnected.pending.req.destroy(); await disconnected.pending.done;
  await f.log.wait(frame => frame.type === 'cancel' && frame.id === disconnected.open.id);
  sockets.push(await f.websocket(f.primary, { localPath: '/after-client-disconnect' }));
  assert.equal((await f.http(f.primary, '/still-full-again')).status, 429);
  for (const item of held) { item.pending.req.destroy(); await item.pending.done; }
  for (const ws of sockets) ws.terminate();
});

test('real head and ACK deadlines release exactly their public slots while neighboring streams remain connected', { timeout: 40_000 }, async t => {
  const f = await fixture(t); f.publish();
  const neighbors = [];
  for (let i = 0; i < 21; i++) neighbors.push(await f.websocket(f.primary, { localPath: `/timeout-neighbor-${i}` }));
  f.behaviors.set('/head-timeout', { hold: true });
  const pending = f.begin(f.primary, '/head-timeout', { completionMs: 35_000 }); pending.req.end();
  const head = await f.log.wait(frame => frame.type === 'open' && frame.path === '/head-timeout');
  f.behaviors.set('/http-ack-timeout', { holdAck: true });
  const httpWaiting = f.begin(f.primary, '/http-ack-timeout', { method: 'POST', origin: f.primary.origin, completionMs: 35_000 });
  httpWaiting.req.end('wait for the unchanged HTTP ACK deadline');
  const httpAck = await f.log.wait(frame => frame.type === 'open' && frame.path === '/http-ack-timeout');
  await f.log.wait(frame => frame.type === 'data' && frame.id === httpAck.id);
  // An early streaming response removes the independent head deadline while
  // the source deliberately leaves the upload chunk unacknowledged.
  f.send({ type: 'head', id: httpAck.id, status: 200, headers: { 'content-type': 'text/plain' } });
  await f.peerData(httpAck.id, Buffer.from('response started; upload ACK still pending')).acknowledged();
  f.behaviors.set('/ack-timeout', { holdAck: true });
  const waiting = await f.websocket(f.primary, { localPath: '/ack-timeout' });
  const ack = f.log.frames.find(frame => frame.type === 'open' && frame.path === '/ack-timeout');
  waiting.send('wait for an actual ACK deadline'); await f.log.wait(frame => frame.type === 'data' && frame.id === ack.id);
  assert.equal((await f.http(f.primary, '/before-timeouts-full')).status, 429);
  // Product timers run unmodified. The injected policy clock remains stable,
  // separating transport timeouts from lease expiration in this scenario.
  const [response, httpAckResponse] = await Promise.all([
    pending.done,
    httpWaiting.done,
    bounded(new Promise(resolve => waiting.once('close', resolve)), 'real ACK timeout close', 35_000),
  ]);
  assert.equal(response.status, 502); assert.match(response.body.toString(), /app_response_timeout/u);
  assert.equal(httpAckResponse.status, 200); assert.equal(httpAckResponse.complete, false);
  assert.equal(httpAckResponse.body.toString(), 'response started; upload ACK still pending');
  assert.equal(f.log.frames.filter(frame => frame.type === 'cancel' && frame.id === head.id).length, 1);
  assert.equal(f.log.frames.filter(frame => frame.type === 'cancel' && frame.id === httpAck.id && frame.error === 'app_ack_timeout').length, 1);
  const wsCancellation = f.log.frames.filter(frame => frame.type === 'cancel' && frame.id === ack.id);
  assert.equal(wsCancellation.length, 1);
  // Both bounds are 30s. The relay offers a write before the inner ACK waiter
  // starts, so either may be the first deadline; neither may release neighbors.
  assert.ok(['app_ack_timeout', 'app_websocket_write_timeout'].includes(wsCancellation[0].error), wsCancellation[0].error);
  assert.equal(neighbors.every(ws => ws.readyState === WebSocket.OPEN), true);
  neighbors.push(await f.websocket(f.primary, { localPath: '/head-slot-reused' }));
  neighbors.push(await f.websocket(f.primary, { localPath: '/http-ack-slot-reused' }));
  neighbors.push(await f.websocket(f.primary, { localPath: '/ack-slot-reused' }));
  assert.equal((await f.http(f.primary, '/after-timeouts-full')).status, 429);
  for (const ws of neighbors) ws.terminate();
});

test('named-only configuration uses the named origin for policy and does not require a legacy origin', async t => {
  const f = await fixture(t, { namedOnly: true, secure: true }); f.publish();
  const response = await f.http(f.primary, '/');
  assert.equal(response.status, 200); assert.equal(response.headers['cache-control'], 'no-store');
  assert.match(response.headers['content-security-policy'], /frame-ancestors https:\/\/shell\.example\.com/u);
  assert.match(response.headers['strict-transport-security'], /max-age=/u);
  const cookie = await f.cookie(visitor, f.primary);
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie })).status, 200);
  denied(await f.http(f.primary, '/', { cookie: cookie.replace(/=.+$/u, '=invalid'), headers: { accept: 'application/json' } }));
  assert.throws(() => f.launch(owner), /origin|domain|canonical/u);
});

test('public HTTP forwards only profile headers and does not turn source errors into private metadata or transport-wide failure', async t => {
  const f = await fixture(t); f.publish();
  f.behaviors.set('/headers', { headers: { 'set-cookie': 'private-upstream=value', authorization: 'private-upstream', 'content-type': 'text/plain' }, body: Buffer.from('public response') });
  const response = await f.http(f.primary, '/headers', { origin: f.primary.origin, headers: { authorization: 'Bearer synthetic-client', 'x-forwarded-host': 'evil.example' } });
  assert.equal(response.status, 200); assert.equal(response.headers['set-cookie'], undefined); assert.equal(response.headers.authorization, undefined);
  const opened = f.log.frames.find(frame => frame.type === 'open' && frame.path === '/headers');
  assert.equal(opened.headers.authorization, undefined); assert.equal(opened.headers['x-forwarded-host'], undefined);
  f.behaviors.set('/source-error', { hold: true });
  const pending = f.begin(f.primary, '/source-error'); pending.req.end();
  const broken = await f.log.wait(frame => frame.type === 'open' && frame.path === '/source-error');
  f.send({ type: 'cancel', id: broken.id, error: 'untrusted details runtime_owner runtime_host 32123' });
  const failed = await pending.done; assert.ok(failed.status >= 400); assert.doesNotMatch(failed.body.toString(), /runtime_owner|runtime_host|32123/u);
  assert.equal((await f.http(f.primary, '/')).status, 200);
});

test('external redirects and informational-only HTTP heads fail locally; same-origin redirects remain usable', async t => {
  const f = await fixture(t); f.publish(); const neighbor = await f.websocket(f.primary);
  f.behaviors.set('/relative-redirect', { status: 307, location: '/destination?local=true' });
  const local = await f.http(f.primary, '/relative-redirect');
  assert.equal(local.status, 307); assert.equal(local.headers.location, '/destination?local=true');
  for (const [localPath, behavior] of [
    ['/external-redirect', { status: 307, location: 'https://outside.example/escape' }],
    ['/informational-only', { status: 103 }],
  ]) {
    f.behaviors.set(localPath, behavior); const response = await f.http(f.primary, localPath);
    assert.ok(response.status >= 400 || response.status === 0); assert.equal(response.headers?.location, undefined);
    assert.equal(neighbor.readyState, WebSocket.OPEN);
    assert.equal((await f.http(f.primary, '/')).status, 200);
  }
});

// B3 entry tests use the same network boundary but a separate prefix, allowing
// the new contract to run red while B2 remains a frozen regression baseline.
async function bootSession(f, actor = owner, target = f.primary, localPath = '/start') {
  const launch = f.launch(actor, target, localPath), response = await f.exchange(launch, target);
  assert.equal(response.status, 200, response.body.toString());
  const payload = JSON.parse(response.body);
  assert.equal(typeof payload.sessionCheck, 'string'); assert.ok(payload.sessionCheck.length >= 32);
  return { launch, cookie: response.headers['set-cookie'][0].split(';')[0], check: payload.sessionCheck, payload };
}
const documentNavigation = { accept: 'text/html', 'sec-fetch-mode': 'navigate', 'sec-fetch-dest': 'document' };
function shellDestination(f, target, localPath, response) {
  assert.equal(response.status, 302);
  const url = new URL(response.headers.location);
  assert.equal(url.origin, f.config.shellOrigins[0]); assert.equal(url.pathname, '/'); assert.equal(url.search, '');
  const [route, query] = url.hash.slice(1).split('?');
  assert.equal(route, `launch/${f.app.id}/${target.id}`);
  assert.equal(new URLSearchParams(query).get('path'), localPath);
  assert.equal([...new URLSearchParams(query).keys()].join(','), 'path');
  return url;
}

test('B3: launch query preserves the exact local recovery path while its fragment remains the single-use ticket', async t => {
  const f = await fixture(t); f.publish({ policy: 'restricted' });
  const localPath = '/deep/route?filter=one%26two&next=%2Flocal&tag=%23item';
  const launch = f.launch(member, f.primary, localPath), url = new URL(launch.launchUrl);
  assert.equal(url.pathname, '/_soty/boot'); assert.equal(url.searchParams.get('path'), localPath);
  assert.deepEqual([...url.searchParams.keys()], ['path']); assert.match(url.hash, /^#[A-Za-z0-9_-]{43}$/u);
  // Only query is sent in HTTP. A reload has no ticket after boot replaces the
  // fragment, so its recovery hint must remain the original local path.
  const boot = await f.http(f.primary, url.pathname + url.search, { headers: documentNavigation });
  assert.equal(boot.status, 200); assert.equal(boot.headers['set-cookie'], undefined);
  assert.doesNotMatch(boot.body.toString(), new RegExp(url.hash.slice(1), 'u'));
  f.advance(30_000); const expired = await f.exchange(launch, f.primary);
  denied(expired); assert.equal(expired.headers['set-cookie'], undefined);
  const refreshed = await bootSession(f, member, f.primary, localPath);
  assert.equal(refreshed.payload.entryPath, localPath);
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie: refreshed.cookie,
    headers: { 'X-Soty-Boot-Check': refreshed.check } })).status, 200);
});

test('B3: cookie acceptance proof binds newly issued B and cannot be satisfied by an older valid A or nonce alone', async t => {
  const f = await fixture(t); f.publish();
  const a = await bootSession(f, member), b = await bootSession(f, member);
  assert.notEqual(a.cookie, b.cookie); assert.notEqual(a.check, b.check);
  for (const cookie of [undefined, a.cookie]) {
    const rejected = await f.http(f.primary, '/_soty/session', { cookie, headers: { 'X-Soty-Boot-Check': b.check } });
    denied(rejected); assert.equal(rejected.headers['set-cookie'], undefined);
  }
  for (let i = 0; i < 2; i++) {
    const checked = await f.http(f.primary, '/_soty/session', { cookie: b.cookie, headers: { 'X-Soty-Boot-Check': b.check } });
    assert.equal(checked.status, 200); assert.equal(JSON.parse(checked.body).sessionCheck, b.check);
    assert.equal(checked.headers['set-cookie'], undefined);
  }
  // A remains a valid independent session. Its validity is precisely why a
  // bare GET could not have proved browser acceptance of B.
  const older = await f.http(f.primary, '/_soty/session', { cookie: a.cookie });
  assert.equal(older.status, 200); assert.deepEqual(JSON.parse(older.body), { ok: true });
  denied(await f.http(f.alternate, '/_soty/session', { cookie: b.cookie, headers: { 'X-Soty-Boot-Check': b.check } }));
  denied(await f.http(f.canonical, '/', { headers: { 'X-Soty-Boot-Check': b.check } }));
});

test('B3: boot check expires at thirty seconds, rejects duplicate headers, and rechecks current rights', async t => {
  const f = await fixture(t); f.publish(); const current = await bootSession(f, member);
  for (const value of ['', 'not-the-new-session', `${current.check}, ${current.check}`, [current.check, current.check]]) {
    denied(await f.http(f.primary, '/_soty/session', { cookie: current.cookie, headers: { 'X-Soty-Boot-Check': value } }));
  }
  f.advance(29_999);
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie: current.cookie, headers: { 'X-Soty-Boot-Check': current.check } })).status, 200);
  f.advance(1);
  denied(await f.http(f.primary, '/_soty/session', { cookie: current.cookie, headers: { 'X-Soty-Boot-Check': current.check } }));
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie: current.cookie })).status, 200);
  const later = await bootSession(f, member); f.members.delete(member.accountId); f.notify();
  denied(await f.http(f.primary, '/_soty/session', { cookie: later.cookie, headers: { 'X-Soty-Boot-Check': later.check } }));
});

test('B3: explicit public reset clears both cookie variants and only its presented exact-origin sessions and streams', async t => {
  const f = await fixture(t); f.publish();
  const a = await bootSession(f, member), b = await bootSession(f, member);
  const otherAlias = await bootSession(f, member, f.alternate), otherAccount = await bootSession(f, owner);
  const closedA = await f.websocket(f.primary, { cookie: a.cookie, localPath: '/reset-a' });
  const remainB = await f.websocket(f.primary, { cookie: b.cookie, localPath: '/reset-b' });
  const remainAlias = await f.websocket(f.alternate, { cookie: otherAlias.cookie, localPath: '/reset-alias' });
  const remainOwner = await f.websocket(f.primary, { cookie: otherAccount.cookie, localPath: '/reset-owner' });
  const remainAnonymous = await f.websocket(f.primary, { localPath: '/reset-anonymous' });
  const reset = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin: f.primary.origin,
    cookie: `${a.cookie}; __Host-soty_app_session=unknown; ${otherAlias.cookie}` });
  assert.equal(reset.status, 200); assert.deepEqual(JSON.parse(reset.body), { ok: true });
  const clears = reset.headers['set-cookie']; assert.equal(clears.length, 2);
  assert.equal(clears.filter(value => /;\s*Partitioned(?:;|$)/iu.test(value)).length, 1);
  for (const value of clears) {
    assert.match(value, /^__Host-soty_app_session=/u); assert.match(value, /;\s*Max-Age=0(?:;|$)/iu);
    assert.match(value, /;\s*Path=\/(?:;|$)/iu); assert.match(value, /;\s*Secure(?:;|$)/iu); assert.doesNotMatch(value, /;\s*Domain=/iu);
  }
  await closed(closedA);
  for (const ws of [remainB, remainAlias, remainOwner, remainAnonymous]) assert.equal(ws.readyState, WebSocket.OPEN);
  denied(await f.http(f.primary, '/_soty/session', { cookie: a.cookie }));
  for (const [target, session] of [[f.primary, b], [f.alternate, otherAlias], [f.primary, otherAccount]]) {
    assert.equal((await f.http(target, '/_soty/session', { cookie: session.cookie })).status, 200);
  }
  assert.equal((await f.http(f.primary, '/')).status, 200);
});

test('B3: reset recovers absent, unknown and duplicate cookies, but requires exact Origin and fresh anonymous policy', async t => {
  const f = await fixture(t); f.publish();
  for (const cookie of [undefined, '__Host-soty_app_session=unknown']) {
    const reset = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin: f.primary.origin, cookie });
    assert.equal(reset.status, 200); assert.equal(reset.headers['set-cookie'].length, 2);
  }
  const a = await bootSession(f, member), b = await bootSession(f, member);
  for (const origin of [undefined, 'null', 'https://other.example', [f.primary.origin, f.primary.origin]]) {
    const refused = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin, cookie: a.cookie });
    denied(refused); assert.equal(refused.headers['set-cookie'], undefined);
  }
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie: a.cookie })).status, 200);
  const duplicate = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin: f.primary.origin,
    cookie: `${a.cookie}; ${b.cookie}; ${a.cookie}` });
  assert.equal(duplicate.status, 200);
  for (const session of [a, b]) denied(await f.http(f.primary, '/_soty/session', { cookie: session.cookie }));

  f.publish({ policy: 'restricted' });
  const privateSession = await bootSession(f, member);
  const privateReset = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin: f.primary.origin, cookie: privateSession.cookie });
  denied(privateReset); assert.equal(privateReset.headers['set-cookie'], undefined);
  assert.equal((await f.http(f.primary, '/_soty/session', { cookie: privateSession.cookie })).status, 200);
  const canonicalReset = await f.http(f.canonical, '/_soty/session', { method: 'DELETE', origin: f.canonical.origin,
    cookie: await f.cookie(owner) });
  denied(canonicalReset); assert.equal(canonicalReset.headers['set-cookie'], undefined);
  const inactive = f.alias('inactive-reset');
  assert.equal((await f.http(inactive, '/_soty/session', { method: 'DELETE', origin: inactive.origin })).status, 503);
  f.retire(f.alternate);
  assert.equal((await f.http(f.alternate, '/_soty/session', { method: 'DELETE', origin: f.alternate.origin })).status, 410);
});

test('B3: private document navigation returns only the trusted shell route with the exact local path', async t => {
  const f = await fixture(t); f.publish({ policy: 'restricted' });
  const localPath = '/workspace/view?name=a%26b&returnUrl=https%3A%2F%2Foutside.example%2F';
  for (const target of [f.primary, f.canonical]) for (const cookie of [undefined, '__Host-soty_app_session=invalid']) {
    const response = await f.http(target, localPath, { cookie, headers: { ...documentNavigation, 'x-forwarded-host': 'outside.example' } });
    shellDestination(f, target, localPath, response);
    assert.equal(response.headers['set-cookie'], undefined);
    assert.doesNotMatch(response.body.toString(), /Independent private app|runtime_owner|runtime_host|32123/u);
  }
  const allowed = await f.http(f.primary, localPath, { cookie: await f.cookie(member, f.primary), headers: documentNavigation });
  assert.equal(allowed.status, 200); assert.equal(allowed.headers.location, undefined);
});

test('B3: iframe, fetch, HEAD, unsafe, malformed and reserved paths cannot trigger private shell redirects', async t => {
  const f = await fixture(t); f.publish({ policy: 'restricted' });
  const cases = [
    { headers: { accept: 'text/html' } },
    { headers: { ...documentNavigation, 'sec-fetch-dest': 'iframe' } },
    { headers: { ...documentNavigation, 'sec-fetch-mode': 'cors', 'sec-fetch-dest': 'empty' } },
    { headers: { ...documentNavigation, 'sec-fetch-mode': ['navigate', 'navigate'] } },
    { headers: { ...documentNavigation, 'sec-fetch-dest': ['document', 'document'] } },
    { method: 'HEAD', headers: documentNavigation },
    { method: 'POST', origin: f.primary.origin, body: 'unsafe', headers: documentNavigation },
    { origin: 'https://outside.example', headers: documentNavigation },
  ];
  for (const options of cases) {
    const response = await f.http(f.primary, '/private/path', options);
    assert.ok(response.status >= 400); assert.equal(response.headers.location, undefined);
    assert.equal(response.headers['set-cookie'], undefined);
    assert.doesNotMatch(response.body.toString(), /Independent private app|runtime_owner|runtime_host|32123/u);
  }
  for (const localPath of ['//outside.example/', '/%2foutside.example/', '/%5cpath', '/_soty/session', '/_soty/unknown', '/%5Fsoty/session']) {
    const response = await f.http(f.primary, localPath, { headers: documentNavigation });
    assert.ok(response.status >= 400); assert.equal(response.headers.location, undefined);
  }
  assert.equal(f.log.frames.filter(frame => frame.type === 'open').length, 0);
});

test('B3: an invalid signed-public session shows a status without automatic guest fallback, cookie reset or shell redirect', async t => {
  const f = await fixture(t); f.publish(); const current = await bootSession(f, visitor);
  f.revoked.add(visitor.deviceId);
  const before = f.log.frames.length;
  const blocked = await f.http(f.primary, '/private-result', { cookie: current.cookie, headers: documentNavigation });
  denied(blocked); assert.equal(blocked.headers.location, undefined); assert.equal(blocked.headers['set-cookie'], undefined);
  assert.equal(f.log.frames.slice(before).some(frame => frame.type === 'open'), false);
  assert.doesNotMatch(blocked.body.toString(), /Independent private app|runtime_owner|runtime_host|32123/u);
  const reset = await f.http(f.primary, '/_soty/session', { method: 'DELETE', origin: f.primary.origin, cookie: current.cookie });
  assert.equal(reset.status, 200);
  assert.equal((await f.http(f.primary, '/private-result', { headers: documentNavigation })).status, 200);
});

test('B3: managed pages keep query content inert, pin their script nonce, and reject ambiguous recovery input', async t => {
  const f = await fixture(t); f.publish();
  const localPath = '/?q=</script><script>globalThis.entryInjected=true</script>&quote="';
  const launched = new URL(f.launch(owner, f.primary, localPath).launchUrl);
  const page = await f.http(f.primary, launched.pathname + launched.search, { headers: documentNavigation });
  assert.equal(page.status, 200);
  const markup = page.body.toString(), scripts = [...markup.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gu)];
  assert.equal(scripts.length, 1); assert.doesNotMatch(markup, /<script>globalThis\.entryInjected/u);
  const nonce = /\bnonce="([^"]+)"/u.exec(scripts[0][1])?.[1]; assert.ok(nonce);
  assert.ok(page.headers['content-security-policy'].includes(`script-src 'nonce-${nonce}'`));
  assert.ok(page.headers['content-security-policy'].includes(`frame-ancestors ${f.config.shellOrigins[0]}`));
  assert.doesNotMatch(page.headers['content-security-policy'], /allow-popups|allow-top-navigation|script-src[^;]*'unsafe-inline'/u);
  assert.equal(page.headers['referrer-policy'], 'no-referrer'); assert.equal(page.headers['cache-control'], 'no-store');

  const noHint = await f.http(f.primary, '/_soty/boot', { headers: documentNavigation });
  assert.equal(noHint.status, 200); assert.doesNotMatch(noHint.body.toString(), /%2Fstart|\/start|Independent private app|runtime_owner|runtime_host|32123/u);
  const secondNonce = /<script\b[^>]*\bnonce="([^"]+)"/u.exec(noHint.body.toString())?.[1];
  assert.ok(secondNonce && secondNonce !== nonce);
  for (const query of ['path=%2F%2Foutside.example', 'path=%2F_soty%2Fsession', 'path=%2Fone&path=%2Ftwo', 'returnUrl=https%3A%2F%2Foutside.example']) {
    const response = await f.http(f.primary, '/_soty/boot?' + query, { headers: documentNavigation });
    assert.ok(response.status >= 400); assert.equal(response.headers.location, undefined);
  }
  const inactive = f.alias('entry-inactive'); f.retire(f.alternate);
  for (const [target, status] of [[inactive, 503], [f.alternate, 410], [{ origin: f.primary.origin.replace('primary.', 'missing.') }, 404]]) {
    const response = await f.http(target, '/', { headers: documentNavigation });
    assert.equal(response.status, status); assert.equal(response.headers.location, undefined);
    assert.doesNotMatch(response.body.toString(), /Independent private app|runtime_owner|runtime_host|32123/u);
  }
  assert.equal(f.log.frames.some(frame => frame.type === 'open'), false);
});

test('B3: normalized ambiguous targets are rejected while local hash-SPA navigation remains compatible', async t => {
  const f = await fixture(t); f.publish();
  for (const localPath of ['/x/..//double', '/x/%2e%2e//double']) {
    assert.throws(() => f.launch(owner, f.primary, localPath), `must not issue an unusable recovery target: ${localPath}`);
    const response = await f.http(f.primary, '/_soty/boot?' + new URLSearchParams({ path: localPath }), { headers: documentNavigation });
    assert.ok(response.status >= 400); assert.equal(response.headers.location, undefined);
  }
  for (const localPath of ['/#/dashboard', '/board?tag=a%2Bb#item', '/ordinary?q=value%23fragment']) {
    const accepted = await bootSession(f, owner, f.primary, localPath);
    assert.equal(accepted.payload.entryPath, localPath, 'local query and client fragment must remain exact');
    assert.equal(new URL(accepted.launch.launchUrl).searchParams.get('path'), localPath);
    const navigated = new URL(accepted.payload.entryPath, f.primary.origin);
    assert.equal(navigated.origin, f.primary.origin);
    const response = await f.http(f.primary, navigated.pathname + navigated.search, { cookie: accepted.cookie });
    assert.equal(response.status, 200);
  }
});
