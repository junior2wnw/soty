import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHmac, generateKeyPairSync } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createFeedbackSource } from '../source.mjs';
import { createProjectService } from '../service.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/index.mjs';
import { selectedResourceProfile } from '../../apps/scoped-embed/resource-profile.mjs';
import { createResourceSourceProofSigner } from '../../apps/scoped-embed/resource-proof.mjs';
import { digest } from '../../source-app/server/wire.mjs';
const require = createRequire(import.meta.url), oidc = createRequire(require.resolve('openid-client'));
const { SignJWT, importJWK } = await import(pathToFileURL(oidc.resolve('jose')).href);
const random = () => randomBytes(32).toString('base64url');

test('Source is disabled without configuration and never reads service', async () => {
  const source = createFeedbackSource({ service: new Proxy({}, { get() { throw Error('must not read'); } }) });
  assert.equal(source.enabled, false); assert.equal(await source.handleRequest({}, {}), false); source.close();
});

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'feedback-source-')), transportKey = randomBytes(32), storageKey = randomBytes(32);
  let source, service, context, profile, token, forged = false, live = true, clock = Date.now();
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const signing = await importJWK({ ...pair.privateKey.export({ format: 'jwk' }), alg: 'RS256' }, 'RS256');
  const issuer = createServer((_req, res) => res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ keys: [publicJwk] })));
  const connector = createServer(async (req, res) => { try {
    if (!live) { res.writeHead(403).end(); return; }
    const parts = []; for await (const part of req) parts.push(part);
    const input = JSON.parse(Buffer.concat(parts));
    const now = clock;
    const identityWitness = await new SignJWT({ iss: profile.issuer, aud: 'urn:soty:source-authority', sub: context.humanPrincipal.subject,
      iat: Math.floor(now / 1000), exp: Math.floor(Math.min(now + 10000, context.expiresAt) / 1000),
      sourceChallenge: input.nonce, contextDigest: digest(context) })
      .setProtectedHeader({ alg: 'RS256', typ: 'soty-source-authority+jwt', kid: 'fixture' }).sign(signing);
    const body = JSON.stringify({ nonce: input.nonce, context, ...(forged ? {} : { identityWitness }) });
    res.writeHead(200, { 'content-type': 'application/json',
      'x-soty-source-mac': createHmac('sha256', transportKey).update('authority-result\0' + body).digest('base64url') }).end(body);
  } catch { res.writeHead(503).end(); } });
  const server = createServer((req, res) => void source.handleRequest(req, res));
  for (const s of [issuer, connector, server]) await new Promise(done => s.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`, issuerUrl = `http://127.0.0.1:${issuer.address().port}/human-identity`;
  const appId = 'app-' + 'a'.repeat(32);
  profile = selectedResourceProfile({ schema: 'soty.selected-human-embed.v2', appId,
    connector: { linkId: 'fixture', hostDeviceId: 'host', connectorId: 'connector' }, target: { revision: 1, digest: '1'.repeat(64) },
    sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', environmentId: 'fixture', tenantId: 'owner', appId, resourceId: 'feedback',
      selection: { kind: 'soty.workspace.v1', nativeId: 'feedback', incarnationId: 'one' } },
    issuer: issuerUrl, clientId: 'feedback-client', embedOrigin: origin, nativeOrigin: origin, parentOrigin: new URL(issuerUrl).origin });
  function newContext(subject = 'person-one') {
    token = undefined;
    context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: random(), version: 1, digest: randomBytes(32).toString('hex') },
      profileDigest: profile.digest, appId, sourceProfile: profile.sourceProfile, resource: profile.resource, instanceId: 'main',
      rootPrincipal: { accountId: 'root-' + subject, deviceId: 'device' },
      humanPrincipal: { issuer: issuerUrl, subject, clientId: profile.clientId, clientProfileDigest: '2'.repeat(64), clientGeneration: 1 },
      entry: { domainId: 'domain', origin }, target: profile.target, policyEpoch: 1, expiresAt: clock + 300000 };
  }
  function start() {
    service = createProjectService({ dataDirectory: dir, origin, now: () => clock });
    source = createFeedbackSource({ service, now: () => clock, config: { profile, transportKey, connectorPort: connector.address().port,
      databasePath: join(dir, 'source.sqlite'), storageKey, storageKeyId: 'fixture' } });
  }
  newContext(); start();
  const signer = createResourceSourceProofSigner({ profile, key: transportKey, clock: () => clock });
  async function request(path = '/api/embed/session-continue', input = { requestId: 'source-continue-0001' }, signed = true, deferred = false) {
    const body = Buffer.from(JSON.stringify(input)), cookie = token ? 'soty_rp_session=' + token : '';
    const headers = { origin, 'content-type': 'application/json', ...(cookie ? { cookie } : {}),
      ...(signed ? signer.headers({ context, method: 'POST', path, body, cookie }) : {}) };
    const send = async () => {
      const response = await fetch(origin + path, { method: 'POST', headers, body });
      const set = response.headers.get('set-cookie'); if (set) token = set.split(';')[0].split('=')[1];
      return { status: response.status, body: await response.json() };
    };
    return deferred ? send : send();
  }
  async function contextRef() {
    const ready = await request(); assert.equal(ready.status, 200, JSON.stringify(ready.body));
    const result = await request('/api/embed/query', { requestId: 'source-context-0001', input: { operation: 'soty.current_context' } });
    assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body.data.context.ref;
  }
  t.after(async () => { source?.close(); service?.close(); for (const s of [server, connector, issuer]) {
    s.closeAllConnections(); await new Promise(done => s.close(done));
  } await rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 40 }); });
  return { request, contextRef, newContext, forge: value => { forged = value; }, revoke: () => { live = false; },
    restart() { source.close(); service.close(); start(); }, service: () => service,
    ownActor: () => service.actor(service.identity(issuerUrl, context.humanPrincipal.subject)), advance: ms => { clock += ms; } };
}

test('real Root witness: empty own workspace, create/read/receipt/restart and foreign denial', async t => {
  const f = await fixture(t);
  f.forge(true); assert.ok((await f.request()).status >= 400); f.forge(false);
  assert.ok((await f.request(undefined, undefined, false)).status >= 400);
  let ref = await f.contextRef();
  const query = input => f.request('/api/embed/query', { requestId: 'source-read-00000001', contextRef: ref, input });
  const empty = (await query({ operation: 'projects_list' })).body.data;
  assert.deepEqual(empty.projects, []); assert.equal(empty.nextCursor, '');
  const input = { operation: 'project_create', name: 'Own queue', publicSubmit: false }, requestId = 'source-create-00000001';
  const result = await f.request('/api/embed/invoke', { requestId, contextRef: ref, input });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const listed = await query({ operation: 'projects_list' }); assert.equal(listed.body.data.projects.length, 1);
  const projectId = listed.body.data.projects[0].id;
  const proof = await f.request('/api/embed/receipt', { requestId, contextRef: ref, input: { inputDigest: digest(input) } });
  assert.equal(proof.status, 200, JSON.stringify(proof.body)); assert.equal(proof.body.data.outcome, 'committed');
  const again = await f.request('/api/embed/invoke', { requestId, contextRef: ref, input }); assert.equal(again.status, 200);
  assert.equal((await query({ operation: 'projects_list' })).body.data.projects.length, 1);
  assert.ok((await f.request('/api/embed/invoke', { requestId, contextRef: ref, input: { ...input, name: 'Conflicting queue' } })).status >= 400);
  const ticketInput = { operation: 'ticket_submit', projectId, body: 'Synthetic Source report' }, ticketRequest = 'source-ticket-00000001';
  const ticket = await f.request('/api/embed/invoke', { requestId: ticketRequest, contextRef: ref, input: ticketInput });
  assert.equal(ticket.status, 200, JSON.stringify(ticket.body));
  const ticketProof = await f.request('/api/embed/receipt', { requestId: ticketRequest, contextRef: ref, input: { inputDigest: digest(ticketInput) } });
  assert.equal(ticketProof.status, 200, JSON.stringify(ticketProof.body)); assert.equal(ticketProof.body.data.outcome, 'committed');
  const tickets = await query({ operation: 'tickets_list', projectId }); assert.equal(tickets.status, 200, JSON.stringify(tickets.body));
  assert.equal(tickets.body.data.tickets.length, 1); assert.equal(JSON.stringify(tickets.body).includes('_hostProof'), false);
  assert.ok((await f.request('/api/embed/receipt', { requestId: ticketRequest, contextRef: ref, input: { inputDigest: 'f'.repeat(64) } })).status >= 400);
  f.restart();
  // Existing session survives encrypted storage restart; acquire its new SDK context lease.
  ref = await f.contextRef();
  const durable = await f.request('/api/embed/receipt', { requestId, contextRef: ref, input: { inputDigest: digest(input) } });
  // A new context lease must not falsely rebind an original mutation's proof.
  assert.equal(durable.body.error?.code, 'source_workspace_receipt_denied');
  assert.deepEqual(f.service().sourceReceipt(f.ownActor(), { requestId, inputDigest: digest(input) }), proof.body.data);
  assert.deepEqual(f.service().sourceReceipt(f.ownActor(), { requestId: ticketRequest, inputDigest: digest(ticketInput) }), ticketProof.body.data);
  f.newContext('person-two'); ref = await f.contextRef();
  assert.deepEqual((await query({ operation: 'projects_list' })).body.data.projects, []);
  assert.ok((await query({ operation: 'integration_get', projectId })).status >= 400);
  assert.ok((await f.request('/api/embed/invoke', { requestId: 'source-foreign-000001', contextRef: ref, input: ticketInput })).status >= 400);
  assert.ok((await f.request('/api/embed/query', { requestId: 'source-select-000001', input: { operation: 'soty.context.select', nativeId: projectId, generation: 1 } })).status >= 400);
  f.revoke(); assert.ok((await query({ operation: 'projects_list' })).status >= 400);
});

test('embedded browser client uses maintained frame fetch and reconciles lost ACK without a second invoke', async t => {
  const f = await fixture(t), calls = [];
  const sdkUrl = new URL('../../source-app/browser/frame-fetch.mjs', import.meta.url).href;
  const source = (await readFile(new URL('../public/source-client.js', import.meta.url), 'utf8'))
    .replace("'./source-frame-fetch.js'", JSON.stringify(sdkUrl));
  const { createFeedbackSourceClient } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  let loseAck = true;
  const client = createFeedbackSourceClient({ href: 'https://feedback.example/embed?sotyFrame=' + 'a'.repeat(43),
    async fetch(path, options) {
      assert.equal(options.credentials, 'same-origin'); assert.equal(options.redirect, 'error');
      assert.equal(new Headers(options.headers).get('x-soty-frame'), 'a'.repeat(43));
      assert.equal(new Headers(options.headers).has('authorization'), false);
      const input = JSON.parse(options.body); calls.push({ path, input });
      const result = await f.request(path, input);
      if (path.endsWith('/invoke') && loseAck) { loseAck = false; throw Error('Synthetic lost ACK after commit'); }
      return new Response(JSON.stringify(result.body), { status: result.status, headers: { 'content-type': 'application/json' } });
    } });
  const empty = await client.call('projects_list'); assert.deepEqual(empty.projects, []); assert.equal(empty.nextCursor, null);
  const args = { name: 'Browser queue', publicSubmit: false, requestId: 'browser-source-create-001' };
  const [made, concurrent] = await Promise.all([client.call('project_create', args), client.call('project_create', args)]);
  assert.equal(made.project.name, args.name); assert.equal(concurrent.project.id, made.project.id);
  assert.equal((await client.call('project_create', args)).project.id, made.project.id);
  assert.equal(calls.filter(row => row.path.endsWith('/invoke')).length, 1);
  assert.ok(calls.some(row => row.path.endsWith('/receipt')));
  await assert.rejects(client.call('project_create', { ...args, name: 'Changed' }), { code: 'request_conflict' });
  await assert.rejects(client.call('arbitrary_owner_operation', {}), { code: 'feedback_source_operation_denied' });
  const ticket = await client.call('ticket_submit', { projectId: made.project.id, body: 'Browser ticket', requestId: 'browser-source-ticket-001' });
  assert.equal(ticket.ticket.body, 'Browser ticket');
  assert.equal((await client.call('tickets_list', { projectId: made.project.id })).tickets.length, 1);
  const image = await readFile(new URL('../../feedback/test/fixtures/chrome.png', import.meta.url));
  const media = f.service().execute(f.ownActor(), 'ticket_submit', { projectId: made.project.id,
    requestId: 'browser-media-seed-001', body: 'Selected image',
    attachments: [{ kind: 'image', name: 'synthetic.png', mimeType: 'image/png', dataBase64: image.toString('base64') }] });
  const detail = await client.call('ticket_get', { projectId: made.project.id, ticketId: media.ticket.id, messageOffset: 0, messageLimit: 1 });
  assert.equal(detail.ticket.attachments.length, 1);
  assert.equal(Object.hasOwn(detail.ticket.attachments[0], 'dataBase64'), false);
  assert.equal(Object.hasOwn(detail, 'content'), false);
  assert.equal(detail.messagesPage.nextOffset, null);
  assert.ok(Buffer.byteLength(JSON.stringify(detail)) < 65536);
  assert.equal(calls.filter(row => row.input.input?.operation === 'attachment_chunk').length, 0);
  const loaded = await client.call('attachment_get', { projectId: made.project.id, ticketId: media.ticket.id, attachmentId: detail.ticket.attachments[0].id });
  assert.deepEqual(Buffer.from(loaded.attachment.dataBase64, 'base64'), image);
  assert.ok(calls.some(row => row.input.input?.operation === 'attachment_chunk'));
  assert.ok(calls.every(row => row.path.startsWith('/api/embed/')));
});

test('expired Root context stays denied; a fresh verified context creates a new bounded actor', async t => {
  const f = await fixture(t); await f.contextRef();
  const oldSignedRequest = await f.request(undefined, undefined, true, true); f.advance(300001);
  assert.ok((await oldSignedRequest()).status >= 400);
  await assert.rejects(f.request(), /./);
  f.newContext(); await f.contextRef();
  const ref = await f.contextRef();
  const result = await f.request('/api/embed/query', { requestId: 'source-refreshed-001', contextRef: ref, input: { operation: 'projects_list' } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
});
