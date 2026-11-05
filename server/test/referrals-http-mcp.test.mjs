import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import WebSocket from 'ws';
import { createHttpApp } from '../http-app.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { REFERRAL_CATALOG, referralResource } from '../referral-capabilities.js';
import { createReferralSettingsState, dispatchReferralSettingsIntent } from '../../src/world/referral-settings-state.mjs';

async function until(read) {
  const deadline = Date.now() + 5000;
  while (!read()) { assert.ok(Date.now() < deadline, 'fixture timeout'); await new Promise(done => setTimeout(done, 5)); }
}
async function fixture(t) {
  const events = new Map(), sockets = [];
  const f = await nativeHttpFixture(t, { appOrigins: true, appFactory: (dist, options) => createHttpApp(dist, {
    ...options, referralEvents: { readTrustedEvent: context => context?.kind === 'fixture' ? events.get(context.id) : null },
  }) });
  f.server.on('upgrade', (req, socket, head) => { if (!f.app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(() => sockets.forEach(socket => socket.terminate()));
  const owner = nativeIdentity('Referral app owner'), reader = nativeIdentity('Reader'), outsider = nativeIdentity('Other owner');
  const accounts = new Map();
  for (const actor of [owner, reader, outsider]) accounts.set(actor, (await f.bootstrap(actor)).accountId);
  let serial = 0;
  async function placement(actor, ports) {
    const suffix = ++serial, token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url');
    const identity = { linkId: 'referral_link_012345678901234567890_' + suffix, hostDeviceId: 'referral_host_' + suffix, connectorId: 'referral_connector_' + suffix };
    const registration = await f.http('/api/connectors/register', { method: 'POST', token, body: JSON.stringify({
      linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'],
    }) }); assert.equal(registration.body.ok, true, JSON.stringify(registration.body));
    const socket = new WebSocket(f.origin.replace('http:', 'ws:') + '/api/apps/channel'), frames = [];
    sockets.push(socket); socket.on('error', () => {}); socket.on('message', bytes => frames.push(JSON.parse(bytes)));
    await until(() => socket.readyState === WebSocket.OPEN);
    socket.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Referral fixture connector' }));
    await until(() => frames.some(frame => frame.type === 'ready'));
    socket.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
    await until(() => frames.some(frame => frame.type === 'claim-ready'));
    good(await f.call(actor, 'apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode }));
    socket.close(); await until(() => socket.readyState === WebSocket.CLOSED);
    const ids = [];
    for (const port of ports) ids.push(good(await f.call(actor, 'apps.register', {
      hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port, name: 'Referral fixture ' + port,
      grants: { accountIds: [accounts.get(reader)], communityIds: [] },
    })).app.id);
    return ids;
  }
  const [appId, siblingId] = await placement(owner, [41401, 41402]), [foreignId] = await placement(outsider, [41403]);
  const call = (actor, op, args = {}) => f.call(actor, op, { expectedAccountId: accounts.get(actor), appId, ...args });
  async function grant(actor, selectedApp, ops = REFERRAL_CATALOG.map(entry => entry.capabilityId)) {
    const accountId = accounts.get(actor), principal = good(await f.call(actor, 'access.principals.create', { expectedAccountId: accountId, label: 'Scoped referral agent' })).principal;
    const effects = [...new Set(REFERRAL_CATALOG.filter(entry => ops.includes(entry.capabilityId)).flatMap(entry => entry.effects))];
    const issued = good(await f.call(actor, 'access.grants.issue', { expectedAccountId: accountId, principalId: principal.id,
      capabilities: ops.map(capabilityId => ({ capabilityId, version: 1 })), resources: ['referrals', referralResource(selectedApp)], effects,
      recipients: ['soty:referrals'], expiresAt: Date.now() + 600000, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 30 } })).grant;
    return { ...await f.issue(actor, accountId, { principalId: principal.id, grantId: issued.id, audience: f.origin + '/mcp' }), actor, accountId };
  }
  const reference = op => f.app.locals.capabilitiesService.external.contracts.find(entry => entry.capabilityId === 'referrals.' + op);
  function mcp(identity, name, args) {
    return f.http('/mcp', { method: 'POST', token: identity.token,
      headers: { accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2026-07-28', 'mcp-method': 'tools/call', 'mcp-name': name },
      body: JSON.stringify({ jsonrpc: '2.0', id: ++serial, method: 'tools/call', params: { name,
        arguments: args, _meta: {
          'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientInfo': { name: 'isolated-referral-fixture', version: '1' },
          'io.modelcontextprotocol/clientCapabilities': {},
        } } }),
    });
  }
  function invoke(identity, op, input, idempotencyKey = 'referral-' + ++serial) {
    return mcp(identity, op === 'config.put' ? 'apps_invoke' : 'apps_query', { reference: reference(op), input, idempotencyKey });
  }
  function event(id, value) { events.set(id, value); return f.app.locals.referralsService.internal.applyEvent({ context: { kind: 'fixture', id } }); }
  return { ...f, get app() { return f.app; }, owner, reader, outsider, accounts, appId, siblingId, foreignId, call, rpc:f.call, grant, invoke, mcp, event,
    receipts: () => f.sql(join(f.directory, 'referrals', 'referrals.sqlite'), db => db.prepare("SELECT count(*) n FROM referral_receipts WHERE kind='config'").get().n) };
}
function success(response) {
  assert.equal(response.status, 200, JSON.stringify(response.body)); const result = response.body.result;
  assert.ok(result && !result.isError, JSON.stringify(response.body));
  assert.deepEqual(JSON.parse(result.content[0].text), result.structuredContent); return result.structuredContent;
}
function denied(response) {
  if (response.status !== 200) { assert.ok([401, 403].includes(response.status)); return; }
  assert.equal(response.body.result?.isError, true, JSON.stringify(response.body));
}

function referralClient(f, actor, factory = createReferralSettingsState) {
  const values = new Map(), queues = new Map(); let serial = 0;
  const storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  const locks = { request(key, action) {
    const result = (queues.get(key) ?? Promise.resolve()).catch(() => {}).then(action);
    queues.set(key, result.catch(() => {})); return result;
  } };
  const state = () => factory({ accountId: f.accounts.get(actor), appId: f.appId, storage, locks, randomId: () => `client-intent-${++serial}` });
  const request = async (op, args) => {
    const response = await f.call(actor, op, args);
    if (!response.ok) throw Object.assign(new Error(response.error.code), { code: response.error.code });
    return response;
  };
  return { state, request };
}

test('referral settings client: lost ACK, reopening, old replay receipt, current head and actual Root restart', { timeout: 20000 }, async t => {
  let factory = createReferralSettingsState;
  if (process.env.SOTY_REFERRAL_SETTINGS_TEST_MUTANT === 'unstable-id') {
    const source = await readFile(new URL('../../src/world/referral-settings-state.mjs', import.meta.url), 'utf8');
    const marker = 'return clone(value.pending); // Dispatch the exact durable mutation, including its identity.';
    assert.equal(source.split(marker).length, 2, 'the negative control must mutate exactly one dispatch boundary');
    const broken = source.replace(marker, 'return { ...clone(value.pending), mutationId: randomId() }; // Intentionally broken test control.');
    factory = (await import('data:text/javascript;base64,' + Buffer.from(broken).toString('base64'))).createReferralSettingsState;
  }
  const f = await fixture(t), client = referralClient(f, f.owner, factory), state = client.state();
  const pending = await state.prepare({ expectedRevision: 0, ratesBps: [1500, 1000, 500] });
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending, isCurrent: () => true, request: async (op, args) => {
    const committed = await client.request(op, args);
    if (op === 'referrals.config.put') throw new Error('connection lost after real COMMIT');
    return committed;
  } }), /connection lost after real COMMIT/u);
  assert.equal(good(await f.call(f.owner, 'referrals.config.get')).revision, 1);
  assert.deepEqual(client.state().read().pending, pending);
  good(await f.call(f.owner, 'referrals.config.put', { mutationId: 'another-owner-editor', expectedRevision: 1, ratesBps: [2000, 500, 500] }));
  await f.restart();
  const reopened = client.state(), head = await dispatchReferralSettingsIntent({ state: reopened, pending: reopened.read().pending,
    isCurrent: () => true, request: client.request });
  assert.equal(head.revision, 2); assert.deepEqual(head.ratesBps, [2000, 500, 500]);
  assert.equal(client.state().read().pending, null); assert.equal(f.receipts(), 2, 'recovery must not add a third mutation');
  await f.restart();
  const durable = good(await f.call(f.owner, 'referrals.config.get'));
  assert.equal(durable.revision, 2); assert.deepEqual(durable.ratesBps, [2000, 500, 500]);
});

test('referral settings client: real owner CAS conflict and read-only denial preserve local intent and server data', { timeout: 20000 }, async t => {
  const f = await fixture(t), owner = referralClient(f, f.owner), reader = referralClient(f, f.reader);
  const state = owner.state(), pending = await state.prepare({ expectedRevision: 0, ratesBps: [2000, 500, 500] });
  good(await f.call(f.owner, 'referrals.config.put', { mutationId: 'winning-editor', expectedRevision: 0, ratesBps: [1000] }));
  let rejection;
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending, isCurrent: () => true, request: owner.request }), error => { rejection = error; return error.code === 'referrals_revision_conflict'; });
  assert.deepEqual(state.read().pending, pending);
  const readState = reader.state(), deniedPending = await readState.prepare({ expectedRevision: 1, ratesBps: [9000] });
  await assert.rejects(dispatchReferralSettingsIntent({ state: readState, pending: deniedPending, isCurrent: () => true, request: reader.request }), /required|forbidden/u);
  assert.deepEqual(readState.read().pending, deniedPending);
  assert.deepEqual(good(await f.call(f.owner, 'referrals.config.get')).ratesBps, [1000]); assert.equal(f.receipts(), 1);
  const replacement = await state.resolveRejected(pending, rejection, { expectedRevision: 1, ratesBps: pending.ratesBps }, () => true);
  assert.notEqual(replacement.mutationId, pending.mutationId);
  const saved = await dispatchReferralSettingsIntent({ state, pending: replacement, request: owner.request, isCurrent: () => true });
  assert.equal(saved.revision, 2); assert.deepEqual(saved.ratesBps, [2000, 500, 500]); assert.equal(state.read().pending, null); assert.equal(f.receipts(), 2);
});

test('real signed Root: referral owner configuration persists and rejects other owners, wrong account and public credit', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  assert.deepEqual(good(await f.call(f.owner, 'referrals.config.get')).ratesBps, [1000, 1000, 1000]);
  const args = { mutationId: 'native_config_once', expectedRevision: 0, ratesBps: [1500, 1000, 500] };
  assert.equal(good(await f.call(f.owner, 'referrals.config.put', args)).revision, 1);
  assert.equal(good(await f.call(f.owner, 'referrals.config.put', args)).replayed, true);
  assert.equal((await f.call(f.reader, 'referrals.config.put', { ...args, mutationId: 'member_denied' })).ok, false);
  assert.equal((await f.call(f.owner, 'referrals.config.get', { expectedAccountId: f.accounts.get(f.reader) })).ok, false);
  await assert.rejects(f.call(f.owner, 'referrals.payment', { eventId: 'invented_credit' }), /unsupported_operation/u);
  assert.equal(f.receipts(), 1);
  await f.restart();
  assert.deepEqual(good(await f.call(f.owner, 'referrals.config.get')).ratesBps, [1500, 1000, 500]);
  assert.deepEqual(good(await f.call(f.owner, 'referrals.config.get', { appId: f.siblingId })).ratesBps, [1000, 1000, 1000]);
});

test('actual MCP: exact app grant, domain receipt, idempotent replay/restart, wrong-app and revoked-grant negative controls', { timeout: 20000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.owner, f.appId);
  assert.deepEqual(success(await f.invoke(identity, 'config.get', { appId: f.appId })).result.ratesBps, [1000, 1000, 1000]);
  denied(await f.invoke(identity, 'config.get', { appId: f.siblingId }));
  denied(await f.invoke(identity, 'config.put', { appId: f.foreignId, expectedRevision: 0, ratesBps: [1000] }));
  const input = { appId: f.appId, expectedRevision: 0, ratesBps: [2000, 500, 500] };
  const committed = success(await f.invoke(identity, 'config.put', input, 'configure-exactly-once'));
  assert.equal(committed.invocation.status, 'succeeded');
  assert.equal(success(await f.invoke(identity, 'config.put', input, 'configure-exactly-once')).invocation.invocationId, committed.invocation.invocationId);
  assert.equal(f.receipts(), 1);
  await f.restart();
  assert.equal(success(await f.invoke(identity, 'config.put', input, 'configure-exactly-once')).invocation.invocationId, committed.invocation.invocationId);
  assert.deepEqual(good(await f.call(f.owner, 'referrals.config.get')).ratesBps, input.ratesBps);
  good(await f.rpc(f.owner, 'access.grants.revoke', { expectedAccountId:f.accounts.get(f.owner), grantId: identity.grantId }));
  denied(await f.invoke(identity, 'config.get', { appId: f.appId }));
  assert.equal(f.receipts(), 1);
});

test('actual MCP reads the current beneficiary only and loses access when the app grant is removed', { timeout: 20000 }, async t => {
  const f = await fixture(t), parent = f.accounts.get(f.reader), payer = f.accounts.get(f.outsider);
  f.event('attribution', { kind: 'attribution', appId: f.appId, eventId: 'bind-payer', accountId: payer, parentAccountId: parent });
  f.event('payment', { kind: 'payment', appId: f.appId, eventId: 'paid-once', payerAccountId: payer, currency: 'RUB', eligibleAmountMinor: 10003 });
  const identity = await f.grant(f.reader, f.appId, ['referrals.balance.get', 'referrals.ledger.list']);
  assert.equal(success(await f.invoke(identity, 'balance.get', { appId: f.appId, currency: 'RUB' })).result.amountMinor, '1000');
  assert.equal(success(await f.invoke(identity, 'ledger.list', { appId: f.appId })).result.entries.length, 1);
  denied(await f.invoke(identity, 'balance.get', { appId: f.appId, currency: 'RUB', beneficiaryAccountId: payer }));
  denied(await f.invoke(identity, 'config.put', { appId: f.appId, expectedRevision: 0, ratesBps: [1000] }));
  good(await f.call(f.owner, 'apps.update', { grants: { accountIds: [], communityIds: [] } }));
  denied(await f.invoke(identity, 'balance.get', { appId: f.appId, currency: 'RUB' }));
});

test('clean Root discovery explains referrals and supplies the actual granted references without internal host knowledge', { timeout: 20000 },async t=>{
  const f=await fixture(t),root=await fetch(f.origin+'/'),link=root.headers.get('link'),discoveryPath=/<([^>]+)>; rel="service-desc"/u.exec(link)?.[1];assert.ok(discoveryPath,'A new client starts at the Root service-desc link');
  const discovery=await(await fetch(new URL(discoveryPath,f.origin))).json(),modules=await(await fetch(new URL(discovery.modules,f.origin))).json();
  const module=modules.modules.find(value=>value.id==='soty.referrals');assert.equal(module?.readiness,'available');assert.equal(module.operations.some(value=>/payment|refund|payout/u.test(value)),false);
  const guideResponse=await fetch(new URL(module.documentation,f.origin)),guide=await guideResponse.text();assert.equal(guideResponse.status,200);
  assert.match(guide,/referrals\.config\.put/u,'The public guide must explain how the discovered referral module is configured');
  assert.match(guide,/\[1000,\s*1000,\s*1000\]/u);assert.match(guide,/apps_catalog_search/u);assert.match(guide,/referrals:<appId>/u);
  const identity=await f.grant(f.owner,f.appId),catalog=success(await f.mcp(identity,'apps_catalog_search',{query:'referrals.',limit:20}));
  assert.equal(catalog.items.length,5);const put=catalog.items.find(value=>value.reference.capabilityId==='referrals.config.put');assert.ok(put);
  const detail=success(await f.mcp(identity,'apps_catalog_get',{reference:put.reference}));assert.ok(detail.inputSchema.required.includes('appId'));assert.ok(detail.inputSchema.required.includes('ratesBps'));
  const result=success(await f.mcp(identity,'apps_invoke',{reference:detail.reference,input:{appId:f.appId,expectedRevision:0,ratesBps:[1000,1000,1000]},idempotencyKey:'discovered-referral-config'}));assert.equal(result.invocation.status,'succeeded');
  const reader=await f.grant(f.reader,f.appId,['referrals.config.get']),readCatalog=success(await f.mcp(reader,'apps_catalog_search',{query:'referrals.'}));assert.deepEqual(readCatalog.items.map(value=>value.reference.capabilityId),['referrals.config.get']);
  denied(await f.mcp(reader,'apps_invoke',{reference:detail.reference,input:{appId:f.appId,expectedRevision:1,ratesBps:[9000]},idempotencyKey:'borrowed-reference-denied'}));
  good(await f.rpc(f.owner,'access.grants.revoke',{expectedAccountId:f.accounts.get(f.owner),grantId:identity.grantId}));denied(await f.mcp(identity,'apps_catalog_search',{query:'referrals.'}));
});

test('actual Root and MCP preserve three ten-percent lines, no fourth credit, exact refunds and configuration history across restart', { timeout: 20000 },async t=>{
  const f=await fixture(t),fourth=nativeIdentity('Fourth referral line'),payer=nativeIdentity('Synthetic paid-event payer');
  for(const actor of [fourth,payer])f.accounts.set(actor,(await f.bootstrap(actor)).accountId);
  good(await f.call(f.owner,'apps.update',{grants:{accountIds:[f.reader,f.outsider,fourth,payer].map(actor=>f.accounts.get(actor)),communityIds:[]}}));
  const chain=[payer,f.reader,f.owner,f.outsider,fourth];for(let index=0;index<chain.length;index++)f.event('chain-'+index,{kind:'attribution',appId:f.appId,eventId:'chain-'+index,accountId:f.accounts.get(chain[index]),parentAccountId:chain[index+1]?f.accounts.get(chain[index+1]):null});
  const beneficiaries=[f.reader,f.owner,f.outsider,fourth],identities=[];for(const actor of beneficiaries)identities.push(await f.grant(actor,f.appId,['referrals.balance.get','referrals.ledger.list']));
  const paid={kind:'payment',appId:f.appId,eventId:'three-lines-paid-once',payerAccountId:f.accounts.get(payer),currency:'RUB',eligibleAmountMinor:10005},payment=f.event('trusted-paid',paid);
  assert.deepEqual(payment.ratesBps,[1000,1000,1000]);assert.deepEqual(payment.credits.map(value=>[value.level,value.amountMinor]),[[1,1000],[2,1000],[3,1000]]);assert.equal(payment.payoutsEnabled,false);assert.equal(f.event('trusted-paid',paid).replayed,true);
  const balances=async()=>{const amounts=[];for(const identity of identities)amounts.push(success(await f.invoke(identity,'balance.get',{appId:f.appId,currency:'RUB'})).result.amountMinor);return amounts;};
  assert.deepEqual(await balances(),['1000','1000','1000','0']);const owner=await f.grant(f.owner,f.appId),changed=success(await f.invoke(owner,'config.put',{appId:f.appId,expectedRevision:0,ratesBps:[2000,500,500]},'future-payments-only'));assert.equal(changed.invocation.status,'succeeded');
  await f.restart();assert.deepEqual(await balances(),['1000','1000','1000','0']);assert.deepEqual(good(await f.call(f.owner,'referrals.config.get')).ratesBps,[2000,500,500]);
  const partial=f.event('partial-refund',{kind:'refund',appId:f.appId,eventId:'partial-refund',paymentId:paid.eventId,refundAmountMinor:5000});assert.deepEqual(partial.reversals.map(value=>value.amountMinor),[-499,-499,-499]);
  const rest={kind:'refund',appId:f.appId,eventId:'remaining-refund',paymentId:paid.eventId,refundAmountMinor:5005};assert.deepEqual(f.event('remaining-refund',rest).reversals.map(value=>value.amountMinor),[-501,-501,-501]);assert.equal(f.event('remaining-refund',rest).replayed,true);
  await f.restart();assert.deepEqual(await balances(),['0','0','0','0']);
  for(let index=0;index<identities.length;index++){const entries=success(await f.invoke(identities[index],'ledger.list',{appId:f.appId})).result.entries;assert.deepEqual(entries.map(value=>value.amountMinor),index===3?[]:[1000,-499,-501]);assert.ok(entries.every(value=>!Object.hasOwn(value,'paymentId')&&!Object.hasOwn(value,'payerAccountId')));}
  const future=f.event('future-paid',{...paid,eventId:'future-paid',eligibleAmountMinor:10000});assert.equal(future.configRevision,1);assert.deepEqual(future.credits.map(value=>value.amountMinor),[2000,500,500]);assert.deepEqual(await balances(),['2000','500','500','0']);
});
