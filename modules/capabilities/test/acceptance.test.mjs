import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { Worker } from 'node:worker_threads';
import { createCapabilitiesService } from '../server/index.mjs';
import { fixtureDocumentation } from './support/documentation.mjs';

// Independent acceptance tests exercise the composed boundary, not mocked ACL callbacks.
const ALICE = Object.freeze({ accountId: 'account_acceptance_alice', deviceId: 'device_acceptance_alice' });
const BOB = Object.freeze({ accountId: 'account_acceptance_bob', deviceId: 'device_acceptance_bob' });
const AUDIENCE = 'https://capabilities.test/api';
const CAPABILITY = Object.freeze({
  capabilityId: 'acceptance.create', version: 1, appId: 'acceptance',
  title: 'Acceptance create', description: 'A test-only create operation.', visibility: 'public', executionEnabled: true,
  inputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 100 } }, required: ['text'], additionalProperties: false },
  outputSchema: { type: 'object', properties: {}, additionalProperties: false },
  resources: ['acceptance:new'], effects: ['create'], recipients: ['soty:acceptance'],
  executionBinding: { kind: 'native', handler: 'acceptance.create', version: 1 },
});
const SCOPE = Object.freeze({ capabilityId: CAPABILITY.capabilityId, version: CAPABILITY.version,
  resources: CAPABILITY.resources, effects: CAPABILITY.effects, recipients: CAPABILITY.recipients });

function fixture(t, { catalog = [CAPABILITY] } = {}) {
  const base = resolve(tmpdir());
  const directory = mkdtempSync(join(base, 'soty-capability-acceptance-'));
  const databasePath = join(directory, 'capabilities.sqlite');
  let now = 1_800_000_000_000;
  const activeDevices = new Set([ALICE, BOB].map(actor => `${actor.accountId}/${actor.deviceId}`));
  const options = { databasePath, clock: () => now, catalog, documentation: fixtureDocumentation(catalog),
    actorActive: actor => activeDevices.has(`${actor.accountId}/${actor.deviceId}`) };
  let service = createCapabilitiesService(options);
  t.after(() => {
    service.close();
    assert.equal(dirname(resolve(directory)), base);
    assert.ok(directory.startsWith(join(base, 'soty-capability-acceptance-')));
    rmSync(directory, { recursive: true, force: true });
  });
  const call = (operation, args = {}, actor = ALICE) => service.execute({ op: `access.${operation}`,
    args: { expectedAccountId: actor.accountId, ...args }, actor });
  function issue({ owner = ALICE, limit = 10, grantOverrides = {}, principal, expiresAt = now + 60_000 } = {}) {
    principal ??= call('principals.create', { label: 'External test client' }, owner).principal;
    const grant = call('grants.issue', {
      principalId: principal.id, capabilities: [{ capabilityId: CAPABILITY.capabilityId, version: 1 }],
      resources: [...CAPABILITY.resources], effects: [...CAPABILITY.effects], recipients: [...CAPABILITY.recipients],
      expiresAt, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit }, ...grantOverrides,
    }, owner).grant;
    return credential(grant, owner, principal);
  }
  function credential(grant, owner = ALICE, principal) {
    const result = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE }, owner);
    return { grant, principal, credential: result.credential, token: result.token,
      actor: service.authenticateCredential({ token: result.token, audience: AUDIENCE }) };
  }
  function derive(parent, overrides = {}) {
    const grant = call('grants.derive', {
      parentGrantId: parent.grant.id, capabilities: [{ capabilityId: CAPABILITY.capabilityId, version: 1 }],
      resources: [...CAPABILITY.resources], effects: [...CAPABILITY.effects], recipients: [...CAPABILITY.recipients],
      expiresAt: now + 30_000, allowDelegation: false, maxDepth: 0, ...overrides,
    }).grant;
    return credential(grant);
  }
  return { get service() { return service; }, databasePath, call, issue, credential, derive,
    get now() { return now; }, setNow(value) { now = value; },
    reopen() { service.close(); service = createCapabilitiesService(options); },
    revokeDevice(actor) { activeDevices.delete(`${actor.accountId}/${actor.deviceId}`); },
  };
}

function denied(action, codes) {
  assert.throws(action, error => typeof error?.code === 'string'
    && (!codes || codes.includes(error.code)), 'The operation must reject with a stable domain error.');
}
function admit(f, actor, key, text = 'New private content') {
  return f.service.invocations.admit({ actor, capabilityId: CAPABILITY.capabilityId, version: 1,
    idempotencyKey: key, input: { text } });
}
const created = Object.freeze({ kind: 'created', resourceType: 'note', resourceId: 'note_acceptance', revision: 1 });
const receipt = Object.freeze({ verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: 'note_acceptance', revision: 1 }] });

test('an opaque credential actor cannot be replaced by a copied JSON identity or reused for another audience', t => {
  const f = fixture(t), client = f.issue();
  assert.equal(f.service.authorize({ actor: client.actor, ...SCOPE }).capabilityId, CAPABILITY.capabilityId);
  denied(() => f.service.authorize({ actor: { ...client.actor }, ...SCOPE }));
  denied(() => f.service.authorize({ actor: JSON.parse(JSON.stringify(client.actor)), ...SCOPE }));
  denied(() => f.service.authenticateCredential({ token: client.token, audience: `${AUDIENCE}/other` }));
  denied(() => f.service.authenticateCredential({ token: 'not-a-credential', audience: AUDIENCE }));
  assert.equal(JSON.stringify(client.actor).includes(client.token), false);
});

test('an already authenticated actor loses access immediately after credential or principal revocation', t => {
  const f = fixture(t), first = f.issue(), second = f.issue();
  f.service.authorize({ actor: first.actor, ...SCOPE });
  f.service.authorize({ actor: second.actor, ...SCOPE });
  f.call('credentials.revoke', { credentialId: first.credential.id });
  denied(() => f.service.authorize({ actor: first.actor, ...SCOPE }));
  denied(() => f.service.authenticateCredential({ token: first.token, audience: AUDIENCE }));
  f.call('principals.revoke', { principalId: second.principal.id });
  denied(() => f.service.authorize({ actor: second.actor, ...SCOPE }));
  denied(() => admit(f, second.actor, 'revoked-principal'));
});

test('expiry is exclusive at the exact deadline, including an actor authenticated before that deadline', t => {
  const f = fixture(t), expiresAt = f.now + 1000, client = f.issue({ expiresAt });
  f.setNow(expiresAt - 1);
  f.service.authorize({ actor: client.actor, ...SCOPE });
  f.setNow(expiresAt);
  denied(() => f.service.authorize({ actor: client.actor, ...SCOPE }));
  denied(() => admit(f, client.actor, 'expired-at-deadline'));
});

test('children cannot widen resources, effects, recipients, lifetime or delegation and lose access with their ancestor', t => {
  const f = fixture(t), parent = f.issue({ grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const child = f.derive(parent);
  f.service.authorize({ actor: child.actor, ...SCOPE });
  for (const widening of [
    { resources: ['acceptance:new', 'acceptance:existing'] },
    { effects: ['create', 'delete'] },
    { recipients: ['soty:acceptance', 'external:recipient'] },
    { expiresAt: f.now + 60_001 },
    { allowDelegation: true, maxDepth: 1 },
  ]) denied(() => f.derive(parent, widening));
  denied(() => f.derive(child));
  f.call('grants.revoke', { grantId: parent.grant.id });
  denied(() => f.service.authorize({ actor: child.actor, ...SCOPE }));
  denied(() => admit(f, child.actor, 'revoked-parent-child'));
});

test('a signed owner cannot select a different account or issue a grant for its service principal', t => {
  const f = fixture(t), foreign = f.call('principals.create', { label: 'Bob service' }, BOB).principal;
  denied(() => f.call('principals.create', { expectedAccountId: BOB.accountId, label: 'Forged account' }));
  denied(() => f.issue({ principal: foreign }));
  const own = f.issue();
  denied(() => f.call('credentials.issue', { grantId: own.grant.id, audience: AUDIENCE }, BOB));
  denied(() => f.call('grants.revoke', { grantId: own.grant.id }, BOB));
  f.service.authorize({ actor: own.actor, ...SCOPE });
});

test('create-only authorization uses exact set membership and checks every required effect', t => {
  const f = fixture(t), client = f.issue();
  f.service.authorize({ actor: client.actor, ...SCOPE });
  for (const extra of [
    { resources: ['acceptance:new', 'acceptance:newer'] },
    { effects: ['create', 'read'] },
    { effects: ['create', 'delete'] },
    { recipients: ['soty:acceptance', 'soty:acceptance.other'] },
  ]) denied(() => f.service.authorize({ actor: client.actor, ...SCOPE, ...extra }), ['access_denied']);
  const compound = fixture(t, { catalog: [{ ...CAPABILITY, effects: ['create', 'send'] }] });
  denied(() => compound.issue(), ['invalid_scope']);
  const approved = compound.issue({ grantOverrides: { effects: ['create', 'send'] } });
  assert.deepEqual(compound.service.authorize({ actor: approved.actor, ...SCOPE }).effects, ['create', 'send']);
});

test('removing the trusted creator device also invalidates cached delegated authority before an effect', t => {
  const f = fixture(t), client = f.issue();
  const { invocation } = admit(f, client.actor, 'device-revoked-before-dispatch');
  f.revokeDevice(ALICE);
  denied(() => f.service.authorize({ actor: client.actor, ...SCOPE }));
  denied(() => f.service.invocations.beginDispatch({ invocationId: invocation.invocationId }));
  denied(() => f.call('credentials.issue', { grantId: client.grant.id, audience: AUDIENCE }));
});

test('public discovery excludes private metadata before search, counts and pagination, even for an authorized client', t => {
  const privateCapability = { ...CAPABILITY, capabilityId: 'acceptance.private', visibility: 'private',
    title: 'PRIVATE_CATALOG_MARKER', description: 'PRIVATE_DESCRIPTION_MARKER' };
  const secondPublic = { ...CAPABILITY, capabilityId: 'acceptance.publicTwo', title: 'Second public capability' };
  const f = fixture(t, { catalog: [CAPABILITY, privateCapability, secondPublic] });
  const client = f.issue({ grantOverrides: { capabilities: [{ capabilityId: privateCapability.capabilityId, version: 1 }] } });
  const first = f.service.catalog.search({ limit: 1 });
  assert.equal(first.total, 2); assert.equal(first.items.length, 1); assert.ok(first.cursor);
  const second = f.service.catalog.search({ limit: 1, cursor: first.cursor });
  assert.equal(second.total, 2); assert.equal(second.items.length, 1); assert.equal(second.cursor, null);
  for (const query of ['PRIVATE_CATALOG_MARKER', 'PRIVATE_DESCRIPTION_MARKER', privateCapability.capabilityId]) {
    denied(() => f.service.catalog.search({ query, actor: client.actor }), ['invalid_input']);
    const hidden = f.service.catalog.search({ query });
    assert.deepEqual(hidden.items, []); assert.equal(hidden.total, 0); assert.equal(hidden.cursor, null);
  }
  denied(() => f.service.catalog.get({ capabilityId: privateCapability.capabilityId, version: 1, actor: client.actor }), ['invalid_input']);
  denied(() => f.service.catalog.get({ capabilityId: privateCapability.capabilityId, version: 1 }), ['not_found']);
  denied(() => f.service.catalog.get({ capabilityId: 'acceptance.nonexistent', version: 1 }), ['not_found']);
  denied(() => f.service.catalog.search({ query: 'Second', cursor: first.cursor }), ['cursor_invalid']);
});

test('disabled handlers and unsupported money units remain closed instead of pretending to enforce a financial cap', t => {
  const f = fixture(t, { catalog: [{ ...CAPABILITY, executionEnabled: false }] }), client = f.issue();
  assert.equal(f.service.catalog.get({ capabilityId: CAPABILITY.capabilityId, version: 1 }).capability.executionEnabled, false);
  denied(() => admit(f, client.actor, 'disabled-capability-request'), ['capability_disabled']);
  assert.equal(f.service.invocations.list({ actor: client.actor }).invocations.length, 0);
  denied(() => f.issue({ grantOverrides: { budget: { unit: 'usd', limit: 10 } } }), ['budget_unit_unsupported']);
});

test('caller-selected targets and schema-smuggled effects cannot consume the only quota reservation', t => {
  const f = fixture(t), client = f.issue({ limit: 1 });
  denied(() => f.service.invocations.admit({ actor: client.actor, capabilityId: CAPABILITY.capabilityId, version: 1,
    idempotencyKey: 'forged-target-request', input: { text: 'Legitimate text' },
    target: { kind: 'native', handler: 'notes.purge', version: 1 } }));
  denied(() => f.service.invocations.admit({ actor: client.actor, capabilityId: CAPABILITY.capabilityId, version: 1,
    idempotencyKey: 'smuggled-effect-request', input: { text: 'Legitimate text', effect: 'delete', accountId: BOB.accountId } }));
  const accepted = admit(f, client.actor, 'valid-after-denials');
  assert.equal(accepted.reused, false);
  assert.equal(accepted.invocation.status, 'accepted');
});

test('same key is scoped to the verified client; a conflicting repeat cannot create another effect or expose another client', t => {
  const f = fixture(t), first = f.issue({ limit: 1 }), second = f.issue({ limit: 1 }), foreign = f.issue({ owner: BOB });
  const original = admit(f, first.actor, 'same-external-key');
  const repeat = admit(f, first.actor, 'same-external-key');
  assert.equal(repeat.reused, true);
  assert.deepEqual(repeat.invocation, original.invocation);
  denied(() => admit(f, first.actor, 'same-external-key', 'Changed content'), ['invocation_request_conflict']);
  const independent = admit(f, second.actor, 'same-external-key');
  assert.notEqual(independent.invocation.invocationId, original.invocation.invocationId);
  for (const actor of [second.actor, foreign.actor]) {
    denied(() => f.service.invocations.get({ actor, invocationId: original.invocation.invocationId }), ['invocation_not_found']);
    denied(() => f.service.invocations.requestCancel({ actor, invocationId: original.invocation.invocationId }), ['invocation_not_found']);
    assert.equal(f.service.invocations.list({ actor }).invocations.some(value => value.invocationId === original.invocation.invocationId), false);
  }
  const firstDispatch = f.service.invocations.beginDispatch({ invocationId: original.invocation.invocationId });
  const secondDispatch = f.service.invocations.beginDispatch({ invocationId: independent.invocation.invocationId });
  assert.notEqual(firstDispatch.internalRequestId, secondDispatch.internalRequestId);
});

test('revocation after admission prevents dispatch and access through a cached child actor', t => {
  const f = fixture(t), parent = f.issue({ grantOverrides: { allowDelegation: true, maxDepth: 1 } }), child = f.derive(parent);
  const { invocation } = admit(f, child.actor, 'admitted-before-revoke');
  f.call('grants.revoke', { grantId: parent.grant.id });
  denied(() => f.service.invocations.beginDispatch({ invocationId: invocation.invocationId }));
  denied(() => f.service.invocations.get({ actor: child.actor, invocationId: invocation.invocationId }));
  denied(() => admit(f, child.actor, 'admitted-before-revoke'));
});

test('history pagination is scoped before selection so sibling grants cannot expose or break each other', t => {
  const f = fixture(t), parent = f.issue({ grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const first = f.derive(parent), second = f.derive(parent);
  const firstIds = new Set([
    admit(f, first.actor, 'history-first-grant-one').invocation.invocationId,
    admit(f, first.actor, 'history-first-grant-two').invocation.invocationId,
  ]);
  const secondId = admit(f, second.actor, 'history-second-grant').invocation.invocationId;
  const page = f.service.invocations.list({ actor: first.actor, limit: 1 });
  assert.equal(page.invocations.length, 1); assert.ok(page.nextCursor);
  assert.ok(firstIds.has(page.invocations[0].invocationId));
  const last = f.service.invocations.list({ actor: first.actor, limit: 1, cursor: page.nextCursor });
  assert.equal(last.invocations.length, 1); assert.equal(last.nextCursor, null);
  assert.equal(new Set([...page.invocations, ...last.invocations].map(value => value.invocationId)).size, 2);
  assert.ok(firstIds.has(last.invocations[0].invocationId));
  assert.deepEqual(f.service.invocations.list({ actor: second.actor }).invocations.map(value => value.invocationId), [secondId]);
  denied(() => f.service.invocations.get({ actor: first.actor, invocationId: secondId }), ['invocation_not_found']);
  denied(() => f.service.invocations.list({ actor: second.actor, cursor: page.nextCursor }), ['invocation_invalid_cursor']);
  const renewedCredential = f.credential(first.grant);
  assert.equal(f.service.invocations.list({ actor: renewedCredential.actor }).invocations.length, 2);
});

test('the verified owner sees its durable action history after grant revocation without borrowing a service identity', t => {
  const f = fixture(t), first = f.issue(), second = f.issue(), bob = f.issue({ owner: BOB });
  const privateText = 'OWNER_HISTORY_MUST_NOT_RETURN_INPUT';
  const expected = new Set([
    admit(f, first.actor, 'owner-history-first-client', privateText).invocation.invocationId,
    admit(f, second.actor, 'owner-history-second-client', privateText).invocation.invocationId,
  ]);
  const foreignId = admit(f, bob.actor, 'owner-history-other-account').invocation.invocationId;
  f.call('grants.revoke', { grantId: first.grant.id });
  const page = f.call('invocations.list', { limit: 1 });
  assert.equal(page.invocations.length, 1); assert.ok(page.nextCursor);
  const last = f.call('invocations.list', { limit: 1, cursor: page.nextCursor });
  assert.equal(last.invocations.length, 1); assert.equal(last.nextCursor, null);
  const history = [...page.invocations, ...last.invocations];
  assert.deepEqual(new Set(history.map(value => value.invocationId)), expected);
  assert.ok(history.every(value => typeof value.clientId === 'string' && typeof value.grantId === 'string'));
  assert.equal(JSON.stringify(history).includes(privateText), false);
  assert.equal(JSON.stringify(history).includes(first.token), false);
  assert.deepEqual(f.call('invocations.list', {}, BOB).invocations.map(value => value.invocationId), [foreignId]);
  denied(() => f.call('invocations.list', { expectedAccountId: ALICE.accountId }, BOB), ['account_mismatch']);
  denied(() => f.call('invocations.list', { cursor: page.nextCursor }, BOB), ['invocation_invalid_cursor']);
  denied(() => f.service.execute({ op: 'access.invocations.list', args: { expectedAccountId: ALICE.accountId }, actor: second.actor }), ['authorization_required']);
  denied(() => f.call('invocations.list', { accountId: BOB.accountId }), ['invalid_input']);
  f.revokeDevice(ALICE);
  denied(() => f.call('invocations.list'), ['authorization_required']);
});

test('revoking an undispatched child cancels only its pending effect and releases the shared reservation once', t => {
  const f = fixture(t), parent = f.issue({ limit: 1, grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const first = f.derive(parent), second = f.derive(parent);
  const { invocation } = admit(f, first.actor, 'revoked-pending-reservation');
  f.call('grants.revoke', { grantId: first.grant.id });
  const reconciled = f.service.invocations.reconcileAuthorization({ invocationId: invocation.invocationId });
  assert.equal(reconciled.authorized, false);
  assert.equal(reconciled.invocation.status, 'cancelled');
  assert.equal(reconciled.invocation.effectState, 'none');
  assert.deepEqual(reconciled.invocation.effects, []);
  assert.equal(reconciled.invocation.receipt.errorCode, 'authorization_no_longer_valid');
  f.service.invocations.reconcileAuthorization({ invocationId: invocation.invocationId });
  assert.equal(admit(f, second.actor, 'sibling-after-revoked-pending').invocation.status, 'accepted');
  denied(() => admit(f, second.actor, 'sibling-cannot-double-release'), ['budget_exceeded']);
});

test('revocation after possible dispatch requests a stop, holds quota and never claims the effect was undone', t => {
  const f = fixture(t), parent = f.issue({ limit: 1, grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const first = f.derive(parent), second = f.derive(parent);
  const { invocation } = admit(f, first.actor, 'revoked-after-dispatch');
  f.service.invocations.beginDispatch({ invocationId: invocation.invocationId });
  f.call('grants.revoke', { grantId: first.grant.id });
  const result = f.service.invocations.reconcileAuthorization({ invocationId: invocation.invocationId });
  assert.equal(result.authorized, false);
  assert.equal(result.invocation.status, 'cancel_requested');
  assert.equal(result.invocation.cancelRequested, true);
  assert.equal(Object.hasOwn(result.invocation, 'completedAt'), false);
  denied(() => admit(f, second.actor, 'cannot-spend-unconfirmed-stop'), ['budget_exceeded']);
  const completed = f.service.invocations.recordResult({ invocationId: invocation.invocationId, status: 'succeeded',
    effectState: 'committed', effects: [created], receipt }).invocation;
  assert.equal(completed.effectState, 'committed');
  assert.deepEqual(completed.effects, [created]);
});

test('siblings share one quota and an uncertain result holds its reservation instead of authorizing a replay', t => {
  const f = fixture(t), parent = f.issue({ limit: 1, grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const first = f.derive(parent), second = f.derive(parent);
  const { invocation } = admit(f, first.actor, 'sibling-first-request');
  denied(() => admit(f, second.actor, 'sibling-second-request'));
  f.service.invocations.beginDispatch({ invocationId: invocation.invocationId });
  f.service.invocations.markUncertain({ invocationId: invocation.invocationId });
  assert.equal(admit(f, first.actor, 'sibling-first-request').invocation.status, 'execution_uncertain');
  denied(() => f.service.invocations.beginDispatch({ invocationId: invocation.invocationId }), ['invocation_reconcile_required']);
  denied(() => admit(f, second.actor, 'sibling-after-timeout'));
});

test('two independent service instances cannot both reserve the final shared unit', { timeout: 20_000 }, async t => {
  const f = fixture(t), parent = f.issue({ limit: 1, grantOverrides: { allowDelegation: true, maxDepth: 1 } });
  const first = f.derive(parent), second = f.derive(parent);
  const latch = new Int32Array(new SharedArrayBuffer(4));
  const workers = [];
  let ready = 0;
  const script = `
    const { parentPort, workerData } = require('node:worker_threads');
    import(workerData.serviceUrl).then(({ createCapabilitiesService }) => {
      const service = createCapabilitiesService({ databasePath: workerData.databasePath,
        catalog: [workerData.capability], documentation: workerData.documentation, clock: () => workerData.now,
        actorActive: actor => actor.accountId === workerData.owner.accountId && actor.deviceId === workerData.owner.deviceId });
      try {
        const actor = service.authenticateCredential({ token: workerData.token, audience: workerData.audience });
        const latch = new Int32Array(workerData.latch);
        parentPort.postMessage({ ready: true });
        if (Atomics.wait(latch, 0, 0, 10000) === 'timed-out') throw Object.assign(new Error('barrier_timeout'), { code: 'barrier_timeout' });
        const result = service.invocations.admit({ actor, capabilityId: workerData.capability.capabilityId,
          version: 1, idempotencyKey: workerData.key, input: { text: 'Concurrent admission' } });
        parentPort.postMessage({ status: result.invocation.status, invocationId: result.invocation.invocationId });
      } catch (error) { parentPort.postMessage({ code: error.code || 'unexpected_worker_error' }); }
      finally { service.close(); parentPort.close(); }
    }).catch(() => { parentPort.postMessage({ code: 'worker_import_failed' }); parentPort.close(); });
  `;
  try {
    const results = await Promise.all([first, second].map((client, index) => new Promise((resolveResult, reject) => {
      const worker = new Worker(script, { eval: true, workerData: {
        serviceUrl: new URL('../server/index.mjs', import.meta.url).href,
        databasePath: f.databasePath, capability: CAPABILITY, documentation: fixtureDocumentation([CAPABILITY]), now: f.now, owner: ALICE,
        token: client.token, audience: AUDIENCE, key: `concurrent-shared-${index}`, latch: latch.buffer,
      } });
      workers.push(worker);
      let outcome;
      worker.on('message', message => {
        if (message.ready) { if (++ready === 2) { Atomics.store(latch, 0, 1); Atomics.notify(latch, 0); } }
        else outcome = message;
      });
      worker.once('error', reject);
      worker.once('exit', code => code === 0 && outcome ? resolveResult(outcome) : reject(new Error('Admission worker did not complete.')));
    })));
    assert.equal(results.filter(result => result.status === 'accepted').length, 1);
    assert.equal(results.filter(result => result.code === 'budget_exceeded').length, 1);
    const histories = [first, second].flatMap(client => f.service.invocations.list({ actor: client.actor }).invocations);
    assert.equal(histories.length, 1);
  } finally { await Promise.all(workers.map(worker => worker.terminate())); }
});

test('pending cancellation releases quota; cancellation after dispatch does not silently erase a committed creation', t => {
  const f = fixture(t), client = f.issue({ limit: 1 });
  const before = admit(f, client.actor, 'cancel-before-dispatch').invocation;
  const cancelled = f.service.invocations.requestCancel({ actor: client.actor, invocationId: before.invocationId }).invocation;
  assert.equal(cancelled.status, 'cancelled'); assert.equal(cancelled.effectState, 'none');
  denied(() => f.service.invocations.beginDispatch({ invocationId: before.invocationId }), ['invocation_dispatch_denied']);
  const after = admit(f, client.actor, 'cancel-after-dispatch').invocation;
  f.service.invocations.beginDispatch({ invocationId: after.invocationId });
  const stopping = f.service.invocations.requestCancel({ actor: client.actor, invocationId: after.invocationId }).invocation;
  assert.equal(stopping.status, 'cancel_requested'); assert.equal(stopping.cancelRequested, true);
  assert.equal(Object.hasOwn(stopping, 'completedAt'), false);
  const completed = f.service.invocations.recordResult({ invocationId: after.invocationId, status: 'succeeded',
    effectState: 'committed', effects: [created], receipt }).invocation;
  assert.equal(completed.status, 'succeeded'); assert.equal(completed.effectState, 'committed');
  assert.deepEqual(completed.effects, [created]);
  const lateCancel = f.service.invocations.requestCancel({ actor: client.actor, invocationId: after.invocationId }).invocation;
  assert.deepEqual(lateCancel, completed);
});

test('repeated uncertainty preserves known effects and final reconciliation cannot downgrade a known creation', t => {
  const f = fixture(t), client = f.issue();
  const { invocation } = admit(f, client.actor, 'known-effect-uncertainty');
  f.service.invocations.beginDispatch({ invocationId: invocation.invocationId });
  f.service.invocations.markUncertain({ invocationId: invocation.invocationId, effects: [created] });
  const again = f.service.invocations.markUncertain({ invocationId: invocation.invocationId }).invocation;
  assert.deepEqual(again.effects, [created]); assert.equal(again.effectState, 'unknown');
  denied(() => f.service.invocations.recordResult({ invocationId: invocation.invocationId, status: 'failed',
    effectState: 'unknown', effects: [created], disposition: 'uncertain',
    receipt: { verificationMethod: 'unverified', artifacts: [], errorCode: 'executor_lost' } }), ['invocation_reconcile_required']);
  denied(() => f.service.invocations.recordResult({ invocationId: invocation.invocationId, status: 'failed',
    effectState: 'none', effects: [], receipt: { verificationMethod: 'unverified', artifacts: [], errorCode: 'executor_lost' } }));
  const final = f.service.invocations.recordResult({ invocationId: invocation.invocationId, status: 'succeeded',
    effectState: 'committed', effects: [created], receipt }).invocation;
  assert.equal(final.effectState, 'committed'); assert.deepEqual(final.effects, [created]);
  assert.equal(f.service.invocations.peekDispatch().some(value => value.invocationId === final.invocationId), false);
});

test('a confirmed creation cannot release or zero its invocation charge and thereby bypass a quota of one', t => {
  const f = fixture(t), client = f.issue({ limit: 1 });
  const { invocation } = admit(f, client.actor, 'committed-action-must-spend');
  f.service.invocations.beginDispatch({ invocationId: invocation.invocationId });
  const result = { invocationId: invocation.invocationId, status: 'succeeded', effectState: 'committed', effects: [created], receipt };
  denied(() => f.service.invocations.recordResult({ ...result, disposition: 'released' }));
  denied(() => f.service.invocations.recordResult({ ...result, actualCharges: [{ unit: 'invocations', amount: 0 }] }));
  f.service.invocations.recordResult(result);
  denied(() => admit(f, client.actor, 'cannot-reuse-spent-quota'), ['budget_exceeded']);
});

test('restart keeps idempotency, dispatch identity and minimal immutable receipt without returning input content', t => {
  const f = fixture(t), client = f.issue({ limit: 1 }), privateText = 'PRIVATE_INPUT_SHOULD_NOT_BE_A_RECEIPT';
  const original = admit(f, client.actor, 'restart-stable-request', privateText).invocation;
  const before = f.service.invocations.beginDispatch({ invocationId: original.invocationId });
  f.reopen();
  const actor = f.service.authenticateCredential({ token: client.token, audience: AUDIENCE });
  const after = f.service.invocations.beginDispatch({ invocationId: original.invocationId });
  assert.equal(after.internalRequestId, before.internalRequestId);
  assert.equal(admit(f, actor, 'restart-stable-request', privateText).invocation.invocationId, original.invocationId);
  f.service.invocations.bindJob({ invocationId: original.invocationId, internalRequestId: after.internalRequestId, jobId: 'job_acceptance_stable' });
  const outcome = { invocationId: original.invocationId, status: 'succeeded', effectState: 'committed', effects: [created], receipt };
  f.service.invocations.recordResult(outcome);
  assert.equal(f.service.invocations.recordResult(outcome).reused, true);
  denied(() => f.service.invocations.recordResult({ ...outcome, receipt: { ...receipt, artifacts: [{ type: 'note', id: 'note_replaced', revision: 2 }] } }), ['invocation_result_conflict']);
  const visible = f.service.invocations.get({ actor, invocationId: original.invocationId });
  assert.equal(JSON.stringify(visible).includes(privateText), false);
  assert.equal(JSON.stringify(visible).includes(client.token), false);
  assert.equal(JSON.stringify(f.service.invocations.list({ actor })).includes(privateText), false);
  assert.deepEqual(visible.invocation.receipt, receipt);
  denied(() => admit(f, actor, 'new-request-after-spent'));
});
