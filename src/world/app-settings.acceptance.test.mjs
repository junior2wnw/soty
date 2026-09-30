import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../../modules/apps/server/index.mjs';
import { createAppSettingsState, dispatchAppSettingsIntent, createAppSettingsDraftState,
  appSettingsUpdateArgs, appPublicationArgs, appSettingsObservationRemaining } from './app-settings-state.mjs';

// This is an independent client-contract harness, not a browser fixture. The
// receipts below come from the real Apps service/SQLite, never from a copied
// author receipt builder. Storage and lock scheduling are controlled to expose
// persistence and account races; actual browser Web Locks remain a separate gate.
const owner = Object.freeze({ accountId: 'settings-client-owner', deviceId: 'settings-client-browser' });
const secondAccount = 'settings-client-other';
const host = { linkId: 'client-link', hostDeviceId: 'client-host', connectorId: 'client-connector' };
const clone = value => structuredClone(value);
const delay = ms => new Promise(done => setTimeout(done, ms));
function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
async function until(check, label) { const deadline = Date.now() + 3000; while (!check()) { if (Date.now() >= deadline) throw new Error(`Timeout: ${label}`); await delay(5); } }
const codeIs = code => error => { assert.equal(error?.code, code); return true; };

function localAdapters() {
  const values = new Map(), queues = new Map();
  let failWrites = false, failReads = false, nextGate = null, generated = 0;
  const storage = {
    getItem(key) { if (failReads) throw new Error('simulated storage read denial'); return values.get(key) ?? null; },
    setItem(key, value) { if (failWrites) throw new Error('simulated storage quota'); values.set(key, value); },
  };
  const locks = { request(name, work) {
    const prior = queues.get(name) ?? Promise.resolve(), gate = nextGate; nextGate = null;
    const result = prior.catch(() => {}).then(async () => {
      if (gate) { gate.entered.resolve(); await gate.release.promise; }
      return work();
    });
    queues.set(name, result.catch(() => {})); return result;
  } };
  return { storage, locks,
    state(accountId, appId) { return createAppSettingsState({ accountId, appId, storage, locks, randomId: () => `independent-client-${++generated}` }); },
    setFailWrites(value) { failWrites = value; }, setFailReads(value) { failReads = value; },
    holdNextLock() { nextGate = { entered: deferred(), release: deferred() }; return nextGate; },
    generated: () => generated,
  };
}

async function serviceFixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-settings-client-independent-'));
  const sockets = new Set(), frames = [], active = new Set([owner.deviceId]);
  let service, connector, serial = 0;
  const server = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = `http://127.0.0.1:${port}`, token = randomBytes(32).toString('base64url');
  service = createAppsService({ databasePath: join(directory, 'registry.sqlite'),
    shellOrigins: [origin], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}`,
    actorActive: actor => actor?.accountId === owner.accountId && actor?.deviceId === owner.deviceId && active.has(actor.deviceId),
    isGroupAdmin: (accountId, id) => accountId === owner.accountId && ['group-one', 'group-two'].includes(id),
    canAccessCommunity: () => false,
    authenticateConnector: async value => value.token === token && value.linkId === host.linkId && value.deviceId === host.hostDeviceId && value.connectorId === host.connectorId,
  });
  t.after(async () => {
    connector?.terminate(); service.close(); for (const socket of sockets) socket.destroy();
    await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(directory.split(/[\\/]/u).at(-1), /^soty-settings-client-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  connector = new WebSocket(`${origin.replace('http:', 'ws:')}/api/apps/channel`);
  connector.on('error', () => {}); connector.on('message', bytes => frames.push(JSON.parse(bytes.toString())));
  await new Promise((done, reject) => { connector.once('open', done); connector.once('error', reject); });
  connector.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...host, token, name: 'Client intent test device' }));
  await until(() => frames.some(item => item.type === 'ready'), 'channel authentication');
  const claimCode = randomBytes(32).toString('base64url'), claimDigest = createHash('sha256').update(claimCode).digest('hex');
  connector.send(JSON.stringify({ type: 'claim', claimDigest }));
  await until(() => frames.some(item => item.type === 'claim-ready'), 'claim confirmation');
  const call = (op, args) => service.execute({ op, args: { expectedAccountId: owner.accountId, ...args }, actor: owner });
  call('apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode });
  const app = call('apps.register', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId,
    name: 'Client contract app', port: 32567, entryPath: '/#/dashboard', grants: { accountIds: ['existing-person'], communityIds: ['group-one'] } }).app;
  const inspect = () => call('apps.inspect', { appId: app.id });
  function claim(slug) { return call('apps.domains.claim', { appId: app.id, expectedDomainsRevision: inspect().addresses.revision, requestId: `fixture-claim-${++serial}`, slug }); }
  claim('initial-address');
  const api = { async request(op, args) { return call(op, args); } };
  function publish(policy = 'anyone') {
    const current = inspect();
    const args = appPublicationArgs(current, { launchPolicy: policy, activeDomainIds: current.addresses.aliases.filter(item => item.state === 'bound').map(item => item.id), exposureConfirmed: policy === 'anyone' }, owner.accountId);
    return call('apps.publication.update', { ...args, requestId: `fixture-publication-${++serial}` });
  }
  const publicArgs = () => { const current = inspect(); return appPublicationArgs(current, { launchPolicy: 'anyone', activeDomainIds: [current.addresses.aliases[0].id], exposureConfirmed: true }, owner.accountId); };
  return { app, inspect, api, call, publish, publicArgs, claim };
}

test('C1 client lost publication ACK survives remount with exact request and reconciles history separately from current', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id), args = f.publicArgs(), sent = [];
  const unreliable = { async request(op, input) { sent.push(clone(input)); await f.api.request(op, input); throw new Error('connection lost after commit'); } };
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args, api: unreliable, isCurrent: () => true }));
  const pending = state.read().pending; assert.ok(pending); assert.equal(f.inspect().publication.launchPolicy, 'anyone');
  f.publish('restricted');
  const remounted = local.state(owner.accountId, f.app.id);
  const response = await dispatchAppSettingsIntent({ state: remounted, api: { async request(op, input) { sent.push(clone(input)); return f.api.request(op, input); } }, isCurrent: () => true });
  assert.equal(response.status, 'accepted'); assert.equal(response.response.replayed, true);
  assert.equal(response.response.receipt.launchPolicy, 'anyone'); assert.equal(response.response.current.launchPolicy, 'restricted');
  assert.deepEqual(sent[0], sent[1]); assert.equal(local.generated(), 1); assert.equal(remounted.read().pending, null);
});

test('C1 two tabs serialize incompatible local intents before either can dispatch', async t => {
  const f = await serviceFixture(t), local = localAdapters(), a = local.state(owner.accountId, f.app.id), b = local.state(owner.accountId, f.app.id);
  const args = f.publicArgs(), incompatible = appPublicationArgs(f.inspect(), { launchPolicy: 'restricted', activeDomainIds: [], exposureConfirmed: false }, owner.accountId);
  const gate = local.holdNextLock(), first = a.prepare({ op: 'apps.publication.update', args });
  await gate.entered.promise;
  const second = b.prepare({ op: 'apps.publication.update', args: incompatible }); second.catch(() => {});
  gate.release.resolve(); const pending = await first;
  await assert.rejects(second, codeIs('app_settings_pending_unconfirmed'));
  assert.deepEqual(b.read().pending, pending); assert.equal(local.generated(), 1);
  assert.deepEqual(await b.prepare({ op: 'apps.publication.update', args }), pending);
});

test('C1 an accepted late ACK cannot clear a different intent explicitly prepared after abandonment', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id), ack = deferred(), committed = deferred();
  const sending = dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api: { async request(op, args) {
    const value = await f.api.request(op, args); committed.resolve(value); return ack.promise;
  } }, isCurrent: () => true });
  const oldReceipt = await committed.promise, oldPending = state.read().pending;
  assert.equal(await state.abandon(oldPending), true);
  const newer = await state.prepare({ op: 'apps.publication.update', args: appPublicationArgs(f.inspect(), { launchPolicy: 'restricted', activeDomainIds: [], exposureConfirmed: false }, owner.accountId) });
  ack.resolve(oldReceipt);
  const outcome = await sending; assert.equal(outcome.status, 'superseded'); assert.deepEqual(state.read().pending, newer);
  assert.notEqual(newer.args.requestId, oldPending.args.requestId);
});

test('C1 account switch during local persistence performs zero network calls, while a late committed ACK settles only its original scope', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id);
  let current = true, network = 0;
  const gate = local.holdNextLock();
  const first = dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api: { request() { network++; throw new Error('must not dispatch'); } }, isCurrent: () => current });
  await gate.entered.promise; current = false; gate.release.resolve();
  assert.equal((await first).status, 'stale'); assert.equal(network, 0); assert.ok(state.read().pending);
  const otherScope = local.state(secondAccount, f.app.id); assert.equal(otherScope.read().pending, null);
  const ack = deferred(), reached = deferred(); current = true;
  const second = dispatchAppSettingsIntent({ state, api: { async request(op, args) { const result = await f.api.request(op, args); reached.resolve(result); return ack.promise; } }, isCurrent: () => current });
  const result = await reached.promise; current = false; ack.resolve(result);
  const outcome = await second; assert.equal(outcome.status, 'stale'); assert.equal(Object.hasOwn(outcome, 'response'), false);
  assert.equal(state.read().pending, null); assert.equal(otherScope.read().pending, null);
});

test('C1 storage denial, missing locks and failed ACK persistence never create a new intent on retry', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id);
  let dispatches = 0; const api = { async request(op, args) { dispatches++; return f.api.request(op, args); } };
  local.setFailWrites(true);
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api, isCurrent: () => true }), codeIs('app_settings_storage_unavailable'));
  assert.equal(dispatches, 0); local.setFailWrites(false); local.setFailReads(true);
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api, isCurrent: () => true }), codeIs('app_settings_storage_unavailable'));
  assert.equal(dispatches, 0); local.setFailReads(false);
  const noLocks = createAppSettingsState({ accountId: owner.accountId, appId: f.app.id, storage: local.storage });
  await assert.rejects(dispatchAppSettingsIntent({ state: noLocks, op: 'apps.publication.update', args: f.publicArgs(), api, isCurrent: () => true }), codeIs('app_settings_lock_unavailable'));
  assert.equal(dispatches, 0);
  const loseReceiptStorage = { async request(op, args) { const result = await api.request(op, args); local.setFailWrites(true); return result; } };
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api: loseReceiptStorage, isCurrent: () => true }), codeIs('app_settings_storage_unavailable'));
  const pending = state.read().pending; assert.ok(pending); assert.equal(dispatches, 1);
  local.setFailWrites(false);
  const replay = await dispatchAppSettingsIntent({ state: local.state(owner.accountId, f.app.id), api, isCurrent: () => true });
  assert.equal(replay.pending.args.requestId, pending.args.requestId); assert.equal(replay.response.replayed, true); assert.equal(dispatches, 2);
});

test('C1 wrong receipt identity, scope, intent or revision cannot clear the durable pending command', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id);
  const pending = await state.prepare({ op: 'apps.publication.update', args: f.publicArgs() });
  const real = await f.api.request(pending.op, pending.args);
  const mutations = [
    value => { value.requestId += '-wrong'; },
    value => { value.receipt.requestKeyHash = '0'.repeat(64); },
    value => { value.receipt.appId = `app-${'f'.repeat(32)}`; },
    value => { value.receipt.policyEpoch++; },
    value => { value.receipt.activeDomainIds = []; },
    value => { value.receipt.exposureAck.targetDigest = '0'.repeat(64); },
    value => { value.receipt.targetDigest = '0'.repeat(64); },
    value => { value.current.policyEpoch = value.receipt.policyEpoch - 1; },
    value => { value.current.appId = `app-${'f'.repeat(32)}`; },
  ];
  for (const mutate of mutations) {
    const wrong = clone(real); mutate(wrong);
    await assert.rejects(state.acknowledge(pending, wrong), codeIs('app_settings_invalid_receipt'));
    assert.deepEqual(state.read().pending, pending);
  }
  assert.equal(await state.acknowledge(pending, real), true); assert.equal(state.read().pending, null);
});

test('C1 claimed name survives a subsequent publication conflict and claim retry does not reserve it twice', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id), initial = f.inspect();
  const claimArgs = { appId: f.app.id, expectedAccountId: owner.accountId, expectedDomainsRevision: initial.addresses.revision, slug: 'second-client-address' };
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.domains.claim', args: claimArgs, api: { async request(op, args) { await f.api.request(op, args); throw new Error('lost claim acknowledgement'); } }, isCurrent: () => true }));
  const after = f.inspect(); assert.equal(after.addresses.aliases.length, initial.addresses.aliases.length + 1);
  const replay = await dispatchAppSettingsIntent({ state: local.state(owner.accountId, f.app.id), api: f.api, isCurrent: () => true });
  assert.equal(replay.response.replayed, true); assert.equal(f.inspect().addresses.aliases.length, after.addresses.aliases.length);
  const stale = f.publicArgs(); f.publish('restricted');
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: stale, api: f.api, isCurrent: () => true }), codeIs('app_publication_revision_conflict'));
  assert.equal(f.inspect().addresses.aliases.find(item => item.slug === claimArgs.slug).state, 'bound');
  assert.equal(f.inspect().addresses.aliases.find(item => item.slug === claimArgs.slug).active, true);
  assert.deepEqual(state.read().pending.args.expectedPolicyEpoch, stale.expectedPolicyEpoch);
});

test('C1 a lost retire ACK replays its exact request after remount without a second domain revision', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id), before = f.inspect();
  const args = { appId: f.app.id, expectedAccountId: owner.accountId, domainId: before.addresses.aliases[0].id, expectedDomainsRevision: before.addresses.revision };
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.domains.retire', args, api: { async request(op, input) {
    await f.api.request(op, input); throw new Error('lost retire acknowledgement');
  } }, isCurrent: () => true }));
  const pending = state.read().pending, committed = f.inspect();
  assert.equal(committed.addresses.aliases[0].state, 'tombstone'); assert.equal(committed.addresses.revision, before.addresses.revision + 1);
  const result = await dispatchAppSettingsIntent({ state: local.state(owner.accountId, f.app.id), api: f.api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(result.response.replayed, true);
  assert.equal(result.pending.args.requestId, pending.args.requestId); assert.equal(f.inspect().addresses.revision, committed.addresses.revision);
  assert.equal(state.read().pending, null);
});

test('C1 a pruned publication receipt leaves the old client intent unresolved without silently rebasing or republishing', async t => {
  const f = await serviceFixture(t), local = localAdapters(), state = local.state(owner.accountId, f.app.id);
  await assert.rejects(dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args: f.publicArgs(), api: { async request(op, args) {
    await f.api.request(op, args); throw new Error('lost original acknowledgement');
  } }, isCurrent: () => true }));
  const original = state.read();
  for (let i = 0; i < 64; i++) f.publish('restricted');
  const latest = f.inspect();
  await assert.rejects(dispatchAppSettingsIntent({ state: local.state(owner.accountId, f.app.id), api: f.api, isCurrent: () => true }), codeIs('app_publication_revision_conflict'));
  assert.deepEqual(state.read(), original); assert.equal(local.generated(), 1);
  assert.deepEqual(f.inspect().publication, latest.publication); assert.equal(latest.publication.launchPolicy, 'restricted');
});

test('C1 observing a newer server state retains edited text, grants, publication consent and their old CAS bases until explicit reset', async t => {
  const f = await serviceFixture(t), initial = f.inspect(), drafts = createAppSettingsDraftState(initial);
  drafts.patch({ name: 'Unsent owner wording', communityIds: ['group-two'], launchPolicy: 'anyone', activeDomainIds: [initial.addresses.aliases[0].id], exposureConfirmed: true, slug: 'not-yet-claimed' });
  const localDraft = drafts.read().draft;
  f.call('apps.update', { appId: f.app.id, expectedRevision: initial.app.revision, name: 'Other window name', grants: { accountIds: ['other-person'], communityIds: [] } });
  f.publish('restricted'); drafts.observe(f.inspect());
  const state = drafts.read(); assert.deepEqual(state.draft, localDraft);
  assert.equal(state.nameConflict, true); assert.equal(state.grantsConflict, true); assert.equal(state.publicationConflict, true);
  assert.equal(drafts.base('name').app.revision, initial.app.revision);
  assert.equal(drafts.base('publication').publication.policyEpoch, initial.publication.policyEpoch);
  assert.throws(() => f.call('apps.update', appSettingsUpdateArgs(drafts.base('name'), state.draft, 'name', owner.accountId)), codeIs('app_revision_conflict'));
  assert.throws(() => f.call('apps.publication.update', { ...appPublicationArgs(drafts.base('publication'), state.draft, owner.accountId), requestId: 'stale-draft-must-not-rebase' }), codeIs('app_publication_revision_conflict'));
  assert.deepEqual(drafts.read().draft, localDraft);
  drafts.reset(); const reset = drafts.read(); assert.equal(reset.draft.name, 'Other window name'); assert.equal(reset.draft.slug, 'not-yet-claimed');
  assert.equal(reset.draft.exposureConfirmed, false); assert.equal(reset.nameConflict, false); assert.equal(reset.publicationConflict, false);
});

test('C1 a matching earlier save does not erase a newer edit; name payload omits grants and grants payload preserves personal access', async t => {
  const f = await serviceFixture(t), initial = f.inspect(), drafts = createAppSettingsDraftState(initial);
  drafts.patch({ name: 'Sent wording' }); const sent = appSettingsUpdateArgs(drafts.base('name'), drafts.read().draft, 'name', owner.accountId);
  assert.equal(Object.hasOwn(sent, 'grants'), false); await f.api.request('apps.update', sent);
  drafts.patch({ name: 'Newer unsent wording' }); drafts.observe(f.inspect(), { kind: 'name', args: sent });
  assert.equal(drafts.read().draft.name, 'Newer unsent wording'); assert.equal(drafts.read().nameConflict, true);
  drafts.patch({ communityIds: ['group-two'] });
  const groups = appSettingsUpdateArgs(drafts.base('grants'), drafts.read().draft, 'grants', owner.accountId);
  assert.deepEqual(groups.grants.accountIds, initial.app.grants.accountIds); assert.deepEqual(groups.grants.communityIds, ['group-two']);
});

test('C1 public consent stays explicit and emergency restriction turns off listing without dropping target CAS', async t => {
  const f = await serviceFixture(t), initial = f.inspect(), aliases = initial.addresses.aliases.map(item => item.id);
  assert.throws(() => appPublicationArgs(initial, { launchPolicy: 'anyone', activeDomainIds: aliases, exposureConfirmed: false }, owner.accountId), codeIs('app_exposure_ack_required'));
  const args = { ...f.publicArgs(), listed: true, requestId: 'fixture-listed-publication' }; f.call('apps.publication.update', args);
  const current = f.inspect(), restricted = appPublicationArgs(current, { launchPolicy: 'restricted', activeDomainIds: [], exposureConfirmed: false }, owner.accountId);
  assert.equal(restricted.listed, false); assert.equal(Object.hasOwn(restricted, 'exposureAck'), false);
  assert.equal(restricted.expectedTargetRevision, current.source.revision); assert.equal(restricted.expectedPolicyEpoch, current.publication.policyEpoch);
  const local = localAdapters();
  const result = await dispatchAppSettingsIntent({ state: local.state(owner.accountId, f.app.id), op: 'apps.publication.update', args: restricted, api: f.api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(f.inspect().publication.listed, false); assert.equal(f.inspect().publication.launchPolicy, 'restricted');
});

test('C1 remaining evidence uses server checkedAt and the whole request duration, never a fresh45s per read', () => {
  const snapshot = { checkedAt: 1_000_000, source: { observation: { state: 'responding', observedAt: 960_000, freshUntil: 1_005_000, evidence: 'connector-v1-observation' } } };
  assert.equal(appSettingsObservationRemaining(snapshot, 1200), 3800);
  assert.equal(appSettingsObservationRemaining(snapshot, 6000), 0);
  const later = clone(snapshot); later.checkedAt += 4999;
  assert.equal(appSettingsObservationRemaining(later, 0), 1);
  later.checkedAt++; assert.equal(appSettingsObservationRemaining(later, 0), 0);
  for (const state of ['unknown', 'offline']) { const changed = clone(snapshot); changed.source.observation.state = state; assert.equal(appSettingsObservationRemaining(changed, 0), 0); }
  for (const elapsed of [-1, NaN, Infinity]) assert.equal(appSettingsObservationRemaining(snapshot, elapsed), 0);
});
