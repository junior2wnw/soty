import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomUUID, randomBytes, sign } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { createHttpApp, waitForRejectedHttpStart } from '../http-app.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { createTrustedReadonlyQueryAdapter } from '../../modules/capabilities/server/readonly-queries.mjs';
import { captureExternalApplications } from '../external-applications.js';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createPersonalAgentConversation } from '../../src/platform/personal-agent-client.ts';
import { agentError } from '../personal-agent-transport.js';
import { loadInstalledApplicationsEntry } from '../installed-applications-entry.js';
import { createAgentSourceCredentialStore } from '../agent-source-credential-store.js';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from '../../modules/source-app/test/support/ordinary-installed.mjs';
import { selectedNativeApplications } from '../../modules/source-app/test/support/selected-native-contracts.mjs';
import { STANDARD_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { loadPersonalAgentConfiguration } from '../personal-agent-configuration.js';

const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) }; };

async function fixture(t, { enabled = true, finishAnswer, policy, lowEffortHost = false, cachedMultiCall = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-agent-http-owned-')), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Owned agent fixture</title>');
  const tenant = 'synthetic_billing', keyRef = Object.freeze({}), receipts = new Map(), calls = [], holds = [];
  let allowed = true, lost = false, hold = null, streamHold = null, receiptHold = null, app, closed = false, settledReceiptReads = 0, sourceInput = {};
  const connections = new Set(), server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  // Synthetic billing ports test mounting, not the real commerce reserve/finish implementation.
  // The separately mandatory, explicit integration-test proves the unchanged actual commerce gateway.
  const configuration = { enabled, ...(policy ? { policy } : {}), gateway: { gatewayOrigin: 'https://fixture-commerce.invalid', billingTenant: tenant, keyRef,
    authorizeActor: () => allowed, resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-only-fixture-accounted-key' }),
    readReceipt: async ({ requestId, billingTenant, keyRef: ref }) => { assert.equal(billingTenant, tenant); assert.equal(ref, keyRef);
      if (receiptHold) { const barrier = receiptHold; receiptHold = null; barrier.entered.resolve(); await barrier.release.promise; }
      const request = receipts.get(requestId); if (request?.status === 'completed') settledReceiptReads++;
      return { tenantId: tenant, request }; },
    fetchImpl: async (url, init) => {
      assert.equal(url, 'https://fixture-commerce.invalid/v1/chat/completions');
      const body = JSON.parse(init.body); assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
      calls.push({ body, key: init.headers['Idempotency-Key'] });
      if (lowEffortHost && (body.reasoning_effort !== 'low' || Object.hasOwn(body, 'thinking') || Object.hasOwn(body, 'tool_stream'))) {
        return new Response('{}', { status: 400, headers: { 'content-type': 'application/json' } });
      }
      if (hold && (!hold.afterTool || body.messages.at(-1).role === 'tool')) { const barrier = hold; hold = null; barrier.entered.resolve(); await barrier.release.promise; }
      const last = body.messages.at(-1);
      let name = 'finish_answer', args = { answer: 'Подтверждённый ответ', selectedEvidenceIds: [], claims: [] };
      if (last.role === 'user' && last.content === 'Read my notes') { name = 'list_my_notes'; args = {}; }
      if (last.role === 'user' && last.content.startsWith('Read note ')) { name = 'read_my_note'; args = { noteId: last.content.slice(10) }; }
      if (last.role === 'user' && last.content === 'Read source') { name = 'source_read_1'; args = sourceInput; }
      if (last.role === 'user' && last.content === 'Read guidance') { name = 'apps_guidance_get'; args = sourceInput; }
      if (last.role === 'user' && last.content === 'Catalog sources') { name = 'source_catalog'; args = { limit: 20 }; }
      if (last.role === 'user' && last.content.startsWith('Local task ')) { name = 'run_on_my_device'; args = { text: last.content.slice(11) }; }
      if (last.role === 'user' && last.content.startsWith('Read local ')) { name = 'read_my_device_task'; args = { jobId: last.content.slice(11) }; }
      if (last.role === 'user' && last.content.startsWith('Cancel local ')) { name = 'cancel_my_device_task'; args = { jobId: last.content.slice(13) }; }
      if (last.role === 'user' && last.content.startsWith('Read then task ')) { name = 'read_my_device_task'; args = { jobId: last.content.slice(15) }; }
      if (last.role === 'tool') { const evidence = JSON.parse(last.content).evidence; args = { answer: 'Источник проверен', selectedEvidenceIds: evidence.map(item => item.id),
        claims: [{ kind: 'fact', statement: 'Источник доступен текущему пользователю', evidenceIds: evidence.map(item => item.id),
          freshness: 'current', confidence: 'high', applicability: 'Текущий пользователь и разрешённые источники' }] }; }
      if (last.role === 'tool' && body.messages.some(item => item.role === 'user' && item.content.startsWith('Read then task '))
        && body.messages.findLast(item => item.role === 'assistant')?.tool_calls?.[0]?.function?.name === 'read_my_device_task') {
        name = 'run_on_my_device'; args = { text: 'One follow-up task after reading the original receipt' };
      }
      const terminal = name === 'finish_answer' && finishAnswer;
      if (terminal) args.answer = finishAnswer.answer;
      // Synthetic usage is declared by this fixture, not measured by a GLM tokenizer.
      const requestedTokens = terminal ? finishAnswer.completionTokens : 2;
      const truncated = terminal && (finishAnswer.forceLength || requestedTokens > body.max_tokens);
      const completionTokens = Math.min(requestedTokens, body.max_tokens);
      const argumentsJson = JSON.stringify(args);
      const wireArguments = truncated && !finishAnswer.keepCompleteJson ? argumentsJson.slice(0, -1) : argumentsJson;
      const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: 3, output_tokens: completionTokens, charge_urub: '7' });
      if (lost) throw new Error('private_fixture_payload_must_not_escape');
      assert.equal(body.stream, false); assert.equal(Object.hasOwn(body, 'stream_options'), false); assert.equal(init.headers.Accept, 'application/json');
      // Only the observed 2026-10-12 cached response structure is reproduced.
      // Argument values are synthetic; no private cached message enters this fixture.
      const toolCalls = cachedMultiCall ? ['list_my_apps', 'source_catalog', 'list_my_notes', 'source_read_1'].map((name, index) => ({
        id: 'cached_shape_' + index, type: 'function', function: { name, arguments: JSON.stringify(index ? { query: 'synthetic' } : {}) },
      })) : [{ id: 'call_owned', type: 'function', function: { name, arguments: wireArguments } }];
      const packet = JSON.stringify({ id: 'chatcmpl_owned', model: GLM_MODEL, choices: [{ index: 0, message: { role: 'assistant',
        tool_calls: toolCalls }, finish_reason: truncated ? 'length' : 'tool_calls' }],
        usage: { prompt_tokens: 3, completion_tokens: completionTokens, total_tokens: 3 + completionTokens } });
      const bytes = new TextEncoder().encode(packet), firstEventEnd = Math.floor(bytes.length / 2); let offset = 0;
      const bodyStream = lowEffortHost ? new ReadableStream({ async pull(controller) {
        if (streamHold && offset >= firstEventEnd) { const barrier = streamHold; streamHold = null; barrier.entered.resolve(); await barrier.release.promise; }
        if (offset >= bytes.length) { controller.close(); return; }
        const end = Math.min(offset + 17, offset < firstEventEnd ? firstEventEnd : bytes.length);
        controller.enqueue(bytes.slice(offset, end)); offset = end;
      } }) : packet;
      return new Response(bodyStream, { status: 200, headers: { 'content-type': 'application/json', 'x-soty-request-id': id } });
    } } };
  const base = { dataDir: join(directory, 'data'), connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}`,
    appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' }, universalAppsEnabled: false,
    gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1/v1', applicationTokens: {} }, personalAgent: configuration };
  app = createHttpApp(dist, base);
  async function call(actor, op, args = {}) {
    const post = value => fetch(`${origin}/api/connect/rpc`, { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...value }) }).then(reply => reply.json());
    const challenge = await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }); assert.equal(challenge.ok, true);
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  const bootstrap = actor => call(actor, 'bootstrap', { label: 'Synthetic fixture human', encryptionPublicJwk: actor.encryptionPublicJwk });
  async function stop() {
    if (closed) return; closed = true; holds.forEach(barrier => barrier.release.resolve());
    const stopped = new Promise(done => server.close(done)); connections.forEach(socket => socket.destroy());
    await app.locals.closeServices(); await stopped;
  }
  t.after(async () => { await stop(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(directory, /soty-agent-http-owned-/); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  const a = await bootstrap(alice), b = await bootstrap(bob); assert.equal(a.ok, true); assert.equal(b.ok, true);
  return { directory, dist, origin, alice, bob, a, b, calls, call, base, configuration, settledReceipts: () => settledReceiptReads,
    service: () => app, stop,
    turn: (actor, requestId, prompt = 'Neutral question', extra = {}) => call(actor, 'agent.turn', { requestId, prompt, ...extra }),
    hold(afterTool = false) { const barrier = { entered: defer(), release: defer(), afterTool }; holds.push(barrier); hold = barrier; return barrier; },
    holdReceipt() { const barrier = { entered: defer(), release: defer() }; holds.push(barrier); receiptHold = barrier; return barrier; },
    holdStream() { const barrier = { entered: defer(), release: defer() }; holds.push(barrier); streamHold = barrier; return barrier; },
    setSourceInput(value) { sourceInput = value; },
    setAllowed(value) { allowed = value; }, setLost(value) { lost = value; },
    async restart(extra = {}) { await app.locals.closeServices(); app = createHttpApp(dist, { ...base, ...extra }); },
    async phone() { const phone = identity(), started = await call(phone, 'enrollment.start', { label: 'Own second device', encryptionPublicJwk: phone.encryptionPublicJwk });
      assert.equal(started.ok, true); assert.equal((await call(alice, 'enrollment.approve', { requestId: started.requestId,
        wrappedKey: { schema: 'synthetic.encrypted', ciphertext: 'opaque' } })).ok, true);
      assert.equal((await call(phone, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: a.accountId })).ok, true); return phone; },
  };
}

async function sourceFixture(t, { count = 1, guidance, splitApps = false, broad = false } = {}) {
  const f = await fixture(t); await f.restart({ universalAppsEnabled: true });
  const source = createServer((_req, res) => res.end('Synthetic Source contract fixture'));
  await new Promise(done => source.listen(0, '127.0.0.1', done));
  const token = randomBytes(32).toString('base64url');
  const sourceIdentity = { linkId: 'agent_source_link_123456789012345', hostDeviceId: 'agent_source_host', connectorId: 'agent_source_connector', name: 'Own Source fixture host' };
  const registered = await fetch(f.origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: sourceIdentity.linkId, deviceId: sourceIdentity.hostDeviceId, connectorId: sourceIdentity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registered.json()).ok, true);
  const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity: sourceIdentity, token, serverUrl: f.origin });
  t.after(async () => { runtime.stop(); source.closeAllConnections(); await new Promise(done => source.close(done)); });
  runtime.start(); const connectionDeadline = Date.now() + 5000;
  while (!runtime.status().connected) { assert.ok(Date.now() < connectionDeadline); await new Promise(done => setTimeout(done, 10)); }
  const claim = await runtime.claim();
  assert.equal((await f.call(f.alice, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode })).ok, true);
  const registration = await f.call(f.alice, 'apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId,
    name: 'Own synthetic readonly source', port: source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  assert.equal(registration.ok, true); const appId = registration.app.id, pin = registration.universalRegistration.descriptor.app.source;
  const deadline = Date.now() + 5000;
  while ((await f.call(f.alice, 'apps.list')).apps.find(item => item.id === appId)?.state !== 'ready') {
    assert.ok(Date.now() < deadline); await new Promise(done => setTimeout(done, 10));
  }
  let second;
  if (splitApps) {
    const secondServer=createServer((_req,res)=>res.end('Second synthetic source'));
    await new Promise(done=>secondServer.listen(0,'127.0.0.1',done));
    t.after(()=>new Promise(done=>{secondServer.closeAllConnections();secondServer.close(done);}));
    second = await f.call(f.alice, 'apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId,
      name: 'Second own workspace', port: secondServer.address().port, entryPath: '/second', grants: { accountIds: [], communityIds: [] } });
    assert.equal(second.ok, true, JSON.stringify(second.error));
  }
  const ids = Array.from({length: count}, (_, index) => splitApps && index > 0 ? second.app.id : appId);
  let sourceAllowed = true, sourceReads = 0;
  // Synthetic Source current-authority port, not a claim of installed Planner/HIVE acceptance or Native SSO.
  // Root must honor its independent refusal even though this human owns the registered Root app.
  const adapter = createTrustedReadonlyQueryAdapter({ withAuthority(request, callback) {
    assert.equal(request.actor.accountId, f.a.accountId); if (!sourceAllowed) throw new Error('synthetic_native_source_permission_denied'); return callback();
  }, query(_request, _signal, current) { current(); sourceReads++; return { text: 'Private scoped Source result' }; } });
  const capabilityIds = Array.from({ length: count }, (_, index) => `app.${ids[index]}:read${index + 1}`), resources = ids.map(id => `workspace:${id}:own`);
  const entries = capabilityIds.map((capabilityId, index) => ({ appId: ids[index], target: splitApps && index > 0 ? {revision:second.universalRegistration.descriptor.app.source.revision,digest:second.universalRegistration.descriptor.app.source.digest} : { revision: pin.revision, digest: pin.digest }, adapter,
    ...(guidance ? { guidance } : {}),
    catalog: { capabilityId, version: 1, appId: ids[index], title: `Selected Source read ${index + 1}`, description: 'Read one independently authorized Source resource',
      visibility: 'private', executionEnabled: true, inputSchema: { type: 'object', properties: {}, additionalProperties: false },
      outputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 1000 } }, required: ['text'], additionalProperties: false },
      resources: [resources[index]], effects: [], recipients: [resources[index]], executionBinding: { kind: 'registered', handler: capabilityId, version: 1,
        binding: { id: capabilityId + ':readonly', version: 1, digest: canonicalHash({ source: 'synthetic', resource: resources[index] }) } } } }));
  const references = captureExternalApplications(entries).map(value => value.contract), reference = references[0];
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: entries });
  // Actual signed grant/credential consent, kept private in this synthetic fixture. The composition never creates one.
  const principal = (await f.call(f.alice, 'access.principals.create', { expectedAccountId: f.a.accountId, label: 'Explicit fixture reader' })).principal;
  const grant = (await f.call(f.alice, 'access.grants.issue', { expectedAccountId: f.a.accountId, principalId: principal.id,
    capabilities: (broad ? capabilityIds : capabilityIds.slice(0, 1)).map(capabilityId => ({ capabilityId, version: 1 })), resources: [...new Set(resources)], effects: [], recipients: [...new Set(resources)], expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: Math.max(4, count + 4) } })).grant;
  const issued = await f.call(f.alice, 'access.credentials.issue', { expectedAccountId: f.a.accountId, grantId: grant.id, audience: f.origin + '/mcp' });
  assert.equal(issued.ok, true);
  // Genuine Root grant above remains distinct from CODE registry and Native port.
  // Exercise the actual installed loader; it derives refs and captures this private callback.
  const installedFile = join(f.directory, 'installed-source.mjs');
  const held = { entries, sourceCredentialFor: () => ({ token: issued.token, audience: f.origin + '/mcp' }), closed: 0 };
  globalThis.__installedAgentSourceFixture = held; t.after(() => { delete globalThis.__installedAgentSourceFixture; });
  const code = `export default async function(api){const held=globalThis.__installedAgentSourceFixture;
    const close=()=>{held.closed++;};api.ownClose(close);
    return {...await api.loadInstalledExistingApplications({createRegistry:()=>held.entries}),close,sourceCredentialFor:held.sourceCredentialFor};}`;
  await writeFile(installedFile, code);
  const installed = await loadInstalledApplicationsEntry({ SOTY_INSTALLED_APPLICATIONS_MODULE: installedFile,
    SOTY_INSTALLED_APPLICATIONS_SHA256: createHash('sha256').update(code).digest('hex') });
  assert.deepEqual(installed.sourceReferences, references); assert.equal(installed.sourceCredentialFor, held.sourceCredentialFor);
  const personalAgent = { ...f.configuration, sourceReferences: installed.sourceReferences, sourceCredentialFor: installed.sourceCredentialFor };
  assert.equal(personalAgent.enabled, f.configuration.enabled); assert.equal(personalAgent.gateway, f.configuration.gateway);
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, ...installed.httpOptions, personalAgent });
  return { f, appId, ids, references, reference, entries, grant, issued, reads: () => sourceReads, setNativeAllowed(value) { sourceAllowed = value; } };
}


test('signed generic consent persists encrypted credentials; mixed legacy/denied discovery, device/account isolation, revoke and restart', { timeout: 30000 }, async t => {
  const source = await sourceFixture(t, { count: 2 }), { f, appId, entries, references, issued } = source;
  const key = randomBytes(32), config = { key, keyId: 'synthetic-independent-key' };
  const legacy = ({ reference }) => reference.capabilityId === references[0].capabilityId ? { token: issued.token, audience: f.origin + '/mcp' } : undefined;
  const options = { universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: entries,
    agentSourceAccess: config, personalAgent: { ...f.configuration, sourceReferences: references, sourceCredentialFor: legacy } };
  await f.restart(options);
  const first = await f.turn(f.alice, 'mixed_catalog_before', 'Catalog sources');
  assert.equal(first.ok, true, JSON.stringify(first.error)); assert.equal(first.evidence[0].data.items.length, 1);
  const list = await f.call(f.alice, 'agent.sources.list', { expectedAccountId: f.a.accountId });
  assert.equal(list.ok, true, JSON.stringify(list.error)); assert.equal(list.apps.length, 1);
  assert.equal((await f.call(f.bob, 'agent.sources.list', { expectedAccountId: f.b.accountId })).apps.length, 0);
  const args = { expectedAccountId: f.a.accountId, appId, proposalDigest: list.apps[0].proposalDigest, mode: 'read', durationSeconds: 3600, budget: 20, requestId: randomUUID() };
  assert.equal((await f.call(f.alice, 'agent.sources.allow', { ...args, proposalDigest: '0'.repeat(64) })).ok, false);
  assert.equal((await f.call(f.bob, 'agent.sources.allow', args)).ok, false);
  const allowed = await f.call(f.alice, 'agent.sources.allow', args);
  assert.equal(allowed.ok, true, JSON.stringify(allowed.error)); assert.equal(allowed.status.state, 'active');
  assert.equal(JSON.stringify(allowed).includes('soty_cap_'), false);
  const replay = await f.call(f.alice, 'agent.sources.allow', args); assert.deepEqual(replay.status, allowed.status);
  assert.equal((await f.call(f.alice, 'agent.sources.allow', { ...args, budget: 21 })).ok, false);
  const phone = await f.phone();
  assert.equal((await f.call(phone, 'agent.sources.list', { expectedAccountId: f.a.accountId })).apps[0].status.state, 'none');
  f.setSourceInput({ reference: references[1], input: {} });
  assert.equal((await f.turn(phone, 'phone_denied_read', 'Read source')).ok, false);
  assert.equal((await f.turn(f.alice, 'allowed_read_one', 'Read source')).ok, true);
  const database = await readFile(join(f.directory, 'data', 'agent-source-access', 'credentials.sqlite'));
  assert.equal(database.includes(Buffer.from('soty_cap_')), false); assert.equal(database.includes(Buffer.from(f.a.accountId)), false);
  await f.restart(options);
  assert.equal((await f.turn(f.alice, 'allowed_read_restart', 'Read source')).ok, true);
  const revoked = await f.call(f.alice, 'agent.sources.revoke', { expectedAccountId: f.a.accountId, appId }); assert.equal(revoked.ok, true);
  const after = await f.turn(f.alice, 'mixed_catalog_revoked', 'Catalog sources'); assert.equal(after.ok, true); assert.equal(after.evidence[0].data.items.length, 0, 'tombstone never falls back to the legacy credential');
  await f.restart(options);
  assert.equal((await f.call(f.alice, 'agent.sources.list', { expectedAccountId: f.a.accountId })).apps[0].status.state, 'revoked');
  const newArgs = { ...args, requestId: randomUUID() }; assert.equal((await f.call(f.alice, 'agent.sources.allow', newArgs)).ok, true);
  const changed = { ...options, agentSourceAccess: { key: randomBytes(32), keyId: config.keyId } };
  await f.restart(changed);
  const badKey = await f.call(f.alice, 'agent.sources.list', { expectedAccountId: f.a.accountId }); assert.equal(badKey.ok, false);
  assert.equal(JSON.stringify(badKey).includes('soty_cap_'), false);
  await f.restart(options);
  assert.equal((await f.turn(f.alice, 'restored_key_read', 'Read source')).ok, true);
  const status = (await f.call(f.alice, 'agent.sources.list', { expectedAccountId: f.a.accountId })).apps[0].status;
  assert.equal((await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: status.grantId })).ok, true);
  assert.equal((await f.turn(f.alice, 'externally_revoked_read', 'Read source')).ok, false);
});

test('broad legacy grant cannot bypass another app tombstone; managed ref uses its own delegated grant', {timeout:30000}, async t => {
  const source = await sourceFixture(t,{count:2,splitApps:true,broad:true}), {f,ids,entries,references,issued}=source;
  const options={universalAppsEnabled:true,capabilityAudience:f.origin,externalApplications:entries,
    agentSourceAccess:{key:randomBytes(32),keyId:'broad-test-key'},
    personalAgent:{...f.configuration,sourceReferences:references,sourceCredentialFor:()=>({token:issued.token,audience:f.origin+'/mcp'})}};
  await f.restart(options);
  assert.equal((await f.turn(f.alice,'broad_before','Catalog sources')).evidence[0].data.items.length,2);
  assert.equal((await f.call(f.alice,'agent.sources.revoke',{expectedAccountId:f.a.accountId,appId:ids[1]})).ok,true);
  const denied=await f.turn(f.alice,'broad_after_revoke','Catalog sources'); assert.equal(denied.ok,true);
  assert.equal(denied.evidence[0].data.items.length,1); assert.equal(denied.evidence[0].data.items[0].reference.capabilityId,references[0].capabilityId);
  const list=await f.call(f.alice,'agent.sources.list',{expectedAccountId:f.a.accountId});
  const row=list.apps.find(value=>value.appId===ids[1]);
  const granted=await f.call(f.alice,'agent.sources.allow',{expectedAccountId:f.a.accountId,appId:ids[1],proposalDigest:row.proposalDigest,
    mode:'read',durationSeconds:3600,budget:7,requestId:randomUUID()}); assert.equal(granted.ok,true,JSON.stringify(granted.error));
  // First ref remains broad legacy; second must carry the newly consented principal.
  f.setSourceInput({reference:references[1],input:{}});
  assert.equal((await f.turn(f.alice,'broad_managed_read','Read source')).ok,true);
  const grants=await f.call(f.alice,'access.grants.list',{expectedAccountId:f.a.accountId,limit:40});
  const managed=grants.grants.find(value=>value.id===granted.status.grantId);
  assert.equal(managed.budget.spent,1,'read charges managed grant, not broad legacy delegation');
  assert.equal((await f.call(f.alice,'agent.sources.revoke',{expectedAccountId:f.a.accountId,appId:ids[1]})).ok,true);
  await f.restart(options);
  const after=await f.turn(f.alice,'broad_restart','Catalog sources'); assert.equal(after.ok,true); assert.equal(after.evidence[0].data.items.length,1);
});

test('durable pending never becomes a grant on retry; encrypted tamper, stale proposal and disabled app fail closed', {timeout:30000}, async t => {
  const source=await sourceFixture(t),{f,appId,entries,references}=source;
  const config={key:randomBytes(32),keyId:'journal-key'};
  const options={universalAppsEnabled:true,capabilityAudience:f.origin,externalApplications:entries,agentSourceAccess:config,
    personalAgent:{...f.configuration,sourceReferences:references}};
  await f.restart(options);
  const listing=()=>f.call(f.alice,'agent.sources.list',{expectedAccountId:f.a.accountId});
  const proposal=(await listing()).apps[0];
  const args={expectedAccountId:f.a.accountId,appId,proposalDigest:proposal.proposalDigest,mode:'read',durationSeconds:3600,budget:5,requestId:randomUUID()};
  const issued=await f.call(f.alice,'agent.sources.allow',args); assert.equal(issued.ok,true);
  await f.service().locals.closeServices();
  const databasePath=join(f.directory,'data','agent-source-access','credentials.sqlite');
  const vault=createAgentSourceCredentialStore({databasePath,...config});
  const row=vault.read(f.a,appId);vault.writePair(f.a,{id:appId,value:{...row,state:'pending',token:null}},{id:`request:${appId}:${row.requestId}`,value:{inputDigest:row.inputDigest,status:{state:'pending',mode:row.mode,expiresAt:row.expiresAt,grantId:row.grantId,requestId:row.requestId}}});vault.close();
  await f.restart(options);
  const before=(await f.call(f.alice,'access.principals.list',{expectedAccountId:f.a.accountId,limit:40})).principals.length;
  const retry=await f.call(f.alice,'agent.sources.allow',args);assert.equal(retry.status.state,'pending');
  assert.equal((await f.call(f.alice,'access.principals.list',{expectedAccountId:f.a.accountId,limit:40})).principals.length,before);
  f.setSourceInput({reference:references[0],input:{}});assert.equal((await f.turn(f.alice,'pending_denied','Read source')).ok,false);
  assert.equal((await f.call(f.alice,'agent.sources.revoke',{expectedAccountId:f.a.accountId,appId})).ok,true);
  assert.equal((await f.call(f.alice,'apps.update',{appId,name:'Changed public name'})).ok,true);
  assert.equal((await f.call(f.alice,'agent.sources.allow',{...args,requestId:randomUUID()})).error.code,'agent_source_proposal_stale');
  const fresh=(await listing()).apps[0];assert.equal((await f.call(f.alice,'agent.sources.allow',{...args,proposalDigest:fresh.proposalDigest,requestId:randomUUID()})).ok,true);
  assert.equal((await f.call(f.alice,'apps.revoke',{appId})).ok,true);
  assert.equal((await listing()).apps.length,0);assert.equal((await f.turn(f.alice,'disabled_denied','Read source')).ok,false);
  // Authenticated ciphertext is never silently discarded/recreated after corruption.
  await f.service().locals.closeServices();
  const db=new DatabaseSync(databasePath);db.exec("UPDATE agent_source_credentials SET cipher=zeroblob(40)");db.close();
  await f.restart(options);
  const corrupt=await f.call(f.alice,'agent.sources.revoke',{expectedAccountId:f.a.accountId,appId});assert.equal(corrupt.ok,false);assert.equal(corrupt.error.code,'agent_source_storage_invalid');
});

test('in-flight retained Source authority rechecks durable tombstone even when principal revocation fails', {timeout:30000}, async t => {
  const source=await sourceFixture(t),{f,appId,entries,references}=source;
  await f.restart({universalAppsEnabled:true,capabilityAudience:f.origin,externalApplications:entries,
    agentSourceAccess:{key:randomBytes(32),keyId:'inflight-key'},personalAgent:{...f.configuration,sourceReferences:references}});
  const list=await f.call(f.alice,'agent.sources.list',{expectedAccountId:f.a.accountId});
  assert.equal((await f.call(f.alice,'agent.sources.allow',{expectedAccountId:f.a.accountId,appId,proposalDigest:list.apps[0].proposalDigest,
    mode:'read',durationSeconds:3600,budget:10,requestId:randomUUID()})).ok,true);
  f.setSourceInput({reference:references[0],input:{}});
  const hold=f.hold(true),pending=f.turn(f.alice,'inflight_tombstone_read','Read source');await hold.entered.promise;
  const reads=source.reads();assert.equal(reads,1);
  const lock=new DatabaseSync(join(f.directory,'data','capabilities','capabilities.sqlite'));
  try {
    lock.exec('BEGIN IMMEDIATE');
    const revoked=await f.call(f.alice,'agent.sources.revoke',{expectedAccountId:f.a.accountId,appId});
    assert.equal(revoked.ok,false,'actual capabilities lock makes downstream principal revoke fail');
  } finally {lock.exec('ROLLBACK');lock.close();hold.release.resolve();}
  const result=await pending;assert.equal(result.ok,false);assert.equal(source.reads(),reads);assert.equal(result.evidence,undefined);
  assert.equal((await f.call(f.alice,'agent.sources.list',{expectedAccountId:f.a.accountId})).apps[0].status.state,'revoked');
});

test('consent Read/Write uses actual Root connector/OIDC workspace Native commit and original receipt', {timeout:120000}, async t => {
  let entries,realm,phase='context',invocationId,serial=0,providerFailure;
  const receipts=new Map(),keyRef={},tenant='synthetic-consent-workspace',nativeInput={operation:'items.create',title:'Consent-created item'};
  const entry=suffix=>entries.find(row=>row.appId===realm.appId&&row.catalog.capabilityId.endsWith(':'+suffix));
  const f=await createOrdinaryInstalledFixture(t,{workspace:true,sourceProfile:STANDARD_WORKSPACE_SOURCE,
    connectorScript:fileURLToPath(new URL('../../scripts/soty-connector.mjs',import.meta.url)),
    agentSourceAccess:{key:randomBytes(32),keyId:'actual-workspace-key'},
    externalApplications(realms){const rows=realms.flatMap(value=>selectedNativeApplications(value.profile,value.workspaceCreation));entries=captureExternalApplications(rows);return rows;},
    personalAgent(){return {enabled:true,historyEnabled:false,sourceReferences:entries.map(row=>row.contract),gateway:{
      gatewayOrigin:'https://synthetic-consent.invalid',billingTenant:tenant,keyRef,authorizeActor:()=>true,
      resolveCredential:async()=>({keyRef,tenantId:tenant,bearer:'synthetic-local-no-provider'}),
      readReceipt:async({requestId})=>({tenantId:tenant,request:receipts.get(requestId)}),
      fetchImpl:async(_url,init)=>{try{
        const request=JSON.parse(init.body);let name,args;
        if(request.messages.at(-1).role==='tool'){
          name='finish_answer';const evidence=JSON.parse(request.messages.at(-1).content).evidence;
          args={answer:'Verified native result',selectedEvidenceIds:evidence.map(row=>row.id),claims:[]};
        }else if(phase==='context'){name='current_context';args={};}
        else{name=phase==='create'?'source_write_1':'source_receipt_1';args={reference:entry('items.create').contract,input:nativeInput,...(phase==='receipt'?{invocationId}:{})};}
        const id=randomUUID();receipts.set(id,{id,status:'completed',model_requested:GLM_MODEL,model_resolved:GLM_MODEL,input_tokens:3,output_tokens:2,charge_urub:'0'});
        return Response.json({id:'synthetic-consent',model:GLM_MODEL,choices:[{index:0,finish_reason:'tool_calls',message:{role:'assistant',tool_calls:[{id:'consent_'+(++serial),type:'function',function:{name,arguments:JSON.stringify(args)}}]}}],usage:{prompt_tokens:3,completion_tokens:2,total_tokens:5}},{headers:{'x-soty-request-id':id}});
      }catch(error){providerFailure=error;throw error;}}
    }};}});
  realm=f.realms[0];await realm.launch();const login=await f.authorize(realm);await f.complete(login.callbackUrl);
  assert.equal((await f.wire(realm.embedded+'/api/embed/session-continue',{body:{requestId:'consent-session-continue'}})).status,200);
  const accountId=f.reader.account.accountId,client=f.reader.client,consentRequests=[];
  const consent=async mode=>{
    const list=await client.extension('agent.sources.list',{expectedAccountId:accountId});const row=list.apps.find(value=>value.appId===realm.appId);assert.ok(row);
    const args={expectedAccountId:accountId,appId:realm.appId,proposalDigest:row.proposalDigest,mode,durationSeconds:3600,budget:40,requestId:randomUUID()};consentRequests.push(args);
    return client.extension('agent.sources.allow',args);
  };
  const turn=()=>client.extension('agent.turn',{requestId:'consent-turn-'+(++serial),prompt:'Use my workspace',selection:{appId:realm.appId,handle:realm.lastLaunch.scopedCloseHandle}}).catch(error=>{throw providerFailure??error;});
  await consent('read');assert.equal((await turn()).evidence[0].data.title,'board-one');
  const count=()=>realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n;
  const before=count();phase='create';await assert.rejects(turn());assert.equal(count(),before,'read consent cannot write');
  await consent('write');const created=await turn();assert.equal(count(),before+1);invocationId=created.evidence[0].data.invocationId;assert.ok(invocationId);
  phase='receipt';const receipt=await turn();assert.equal(receipt.evidence[0].data.invocationId,invocationId);assert.equal(count(),before+1,'receipt never repeats Native effect');
  assert.equal(JSON.stringify(created).includes('soty_cap_'),false);
  const originalWrite=consentRequests.find(args=>args.mode==='write');
  const narrower=await consent('read');
  await client.extension('agent.sources.allow',originalWrite);
  const current=(await client.extension('agent.sources.list',{expectedAccountId:accountId})).apps.find(app=>app.appId===realm.appId);
  assert.equal(current.status.mode,'read');assert.equal(current.status.grantId,narrower.status.grantId,'old write replay cannot replace a newer read consent');
  phase='create';await assert.rejects(turn());assert.equal(count(),before+1);
  await client.extension('agent.sources.revoke',{expectedAccountId:accountId,appId:realm.appId});phase='create';await assert.rejects(turn());assert.equal(count(),before+1);
});

test('existing private config accepts only an independent bounded source key namespace', async t => {
  const directory=await mkdtemp(join(tmpdir(),'agent-source-config-')),file=join(directory,'private.json');
  t.after(()=>rm(directory,{recursive:true,force:true}));
  const key=randomBytes(32),base={schema:'soty.personal-agent-installed.v1',gateway:{origin:'https://fixture-commerce.invalid',billingTenant:'fixture',
    inferenceBearer:'synthetic-only-inference-not-a-storage-key',managementSession:'synthetic_only_management_session'},accountIds:['fixture_owner']};
  const env={SOTY_PERSONAL_AGENT_ENABLED:'1',SOTY_PERSONAL_AGENT_CONFIG_FILE:file};
  await writeFile(file,JSON.stringify(base),{mode:0o600});assert.equal(loadPersonalAgentConfiguration(env).agentSourceAccess,undefined);
  await writeFile(file,JSON.stringify({...base,sourceAccess:{schema:1,keyId:'own-source-key',key:key.toString('base64url')}}),{mode:0o600});
  const config=loadPersonalAgentConfiguration(env);assert.deepEqual(config.agentSourceAccess.key,key);assert.equal(config.personalAgent.sourceAccess,undefined);
  for(const sourceAccess of [null,{schema:1,keyId:'key',key:'short'},{schema:1,keyId:'key',key:key.toString('base64url'),token:'forbidden'}]){
    await writeFile(file,JSON.stringify({...base,sourceAccess}));assert.throws(()=>loadPersonalAgentConfiguration(env),{code:'agent_gateway_configuration_invalid'});
  }
});

test('vault limits new receipts atomically while retaining existing tombstones and replay rows', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'source-quota-'));
  const databasePath = join(directory, 'private', 'credentials.sqlite'), key = randomBytes(32), actor = { accountId: 'synthetic', deviceId: 'own' };
  const { SOURCE_CREDENTIAL_LIMITS: limits } = await import('../agent-source-credential-store.js');
  const store = createAgentSourceCredentialStore({ databasePath, key, keyId: 'quota' });
  store.write(actor, 'app', { state: 'active', token: 'synthetic' });
  const db = new DatabaseSync(databasePath); t.after(async () => { db.close(); store.close(); await rm(directory, { recursive: true, force: true }); });
  db.exec('BEGIN IMMEDIATE');
  const insert = db.prepare('INSERT INTO agent_source_credentials VALUES(?,?)');
  for (let i = 1; i < limits.rows; i++) insert.run(`synthetic-${i}`, Buffer.alloc(29));
  db.exec('COMMIT');
  assert.throws(() => store.writePair(actor, { id: 'app', value: { state: 'pending' } }, { id: 'new-receipt', value: {} }), /agent_source_storage_full/);
  assert.equal(store.read(actor, 'app').state, 'active', 'pair rollback preserves current record');
  store.write(actor, 'app', { state: 'revoked', token: null });
  assert.equal(store.read(actor, 'app').state, 'revoked', 'row quota never deletes or blocks replacement tombstone');
  assert.equal(db.prepare('SELECT count(*) AS n FROM agent_source_credentials').get().n, limits.rows);
  db.exec('BEGIN IMMEDIATE');
  db.exec("DELETE FROM agent_source_credentials WHERE id LIKE 'synthetic-%'");
  for (let i = 1; i <= 516; i++) insert.run(`synthetic-${i}`, Buffer.alloc(65000));
  db.exec('COMMIT');
  assert.throws(() => store.write(actor, 'oversized', { text: 'x'.repeat(64000) }), /agent_source_storage_full/);
  assert.ok(db.prepare('PRAGMA page_count').get().page_count * db.prepare('PRAGMA page_size').get().page_size < limits.databaseBytes);
});
