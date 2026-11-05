import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createHumanCallModule } from '../../server/human-call-module.js';
import { createCallLifecycle } from '../../modules/personal-agent/call/lifecycle.mjs';

const ORIGIN = 'https://call-identity-sdk.test';
const PROJECT = 'call-identity-sdk';
function memoryStorage() {
  let state = null;
  const clone = value => value === null ? null : structuredClone(value);
  return { read: async () => clone(state),
    claim: async candidate => { if (!state) state = clone(candidate); return clone(state); },
    compareAndSwap: async (revision, candidate) => {
      if (state?.localRevision !== revision) throw Object.assign(new Error('LOCAL_CONFLICT'), { code: 'LOCAL_CONFLICT' });
      state = clone(candidate); return clone(state);
    } };
}

test('actual SDK public LocalState binds its own signed private-room call admission', async t => {
  let connect, admission, dispose;
  const observers = new Set(), operations = [], log = { authorize: 0, joins: 0, captures: 0, closes: 0 };
  const world = createWorldService({ databasePath: ':memory:', projectId: PROJECT });
  const calls = createHumanCallModule({ getConnect: () => connect, getWorld: () => world, allowedOrigins: [ORIGIN] });
  connect = createConnectService({ databasePath: ':memory:', projectId: PROJECT, allowedOrigins: [ORIGIN], extensions: [world, calls] });
  calls.attachSignaling(new EventEmitter());
  const client = createClientWithStorage({ projectId: PROJECT, endpoint: ORIGIN + '/rpc',
    createChannel: () => ({ close() {}, postMessage() {} }),
    onState: state => { for (const observer of observers) observer(state); },
    fetch: async (_url, init) => {
      const request = JSON.parse(init.body); operations.push(request.op);
      return Response.json(await connect.handle({ origin: ORIGIN, ...request }));
    } }, memoryStorage());
  const lifecycle = createCallLifecycle({
    host: { async authorize(scope, action) {
      log.authorize++; assert.equal(action, 'join');
      admission = await client.extension('call.join', { requestId: 'identity_sdk_join', mode: scope.mode, targetId: scope.roomId },
        { expectedAccountId: scope.accountId });
      assert.equal(admission.accountId, scope.accountId); assert.equal(admission.deviceId, scope.deviceId);
      return admission;
    } },
    // Authority is real signed Connect/World/call.join. Only media/RTC IO is synthetic here.
    transport: { join({ admission: authorized }) {
      assert.equal(authorized, admission); assert.equal(authorized.schema, 'soty.human-call-admission.v1'); log.joins++;
      return { close() { log.closes++; }, preparePublication() { throw new Error('unexpected_publication'); } };
    } },
    media: { acquire() { log.captures++; throw new Error('unexpected_capture'); } },
  });
  t.after(async () => {
    dispose?.(); lifecycle.dispose();
    try { if (admission) await client.extension('call.leave', { callId: admission.callId, epoch: admission.epoch,
      expectedJoinRef: admission.grantRef }, { expectedAccountId: admission.accountId }); }
    finally { client.dispose(); calls.close(); connect.close(); world.close(); }
  });
  const account = await client.bootstrap('Own SDK identity QA');
  await client.extension('world.profile.get', {});
  const { community } = await client.extension('world.community.create', { requestId: 'identity_sdk_room', name: 'Own private SDK QA', joinPolicy: 'invite' });
  const actualView = await client.getLocalState();
  assert.equal(actualView.schema, 'connect.local-view.v1');
  assert.equal(Object.hasOwn(actualView, 'localRevision'), true);
  assert.equal(Number.isSafeInteger(actualView.localRevision) && actualView.localRevision >= 0, true);
  assert.equal(actualView.accountId, account.accountId); assert.equal(actualView.deviceId, account.deviceId);
  const source = await readFile(process.env.SOTY_CALL_IDENTITY_TEST_SOURCE ?? new URL('./call-identity.ts', import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports, queueMicrotask });
  dispose = module.exports.bindCallIdentity({ lifecycle, client, observeAccount(listener) { observers.add(listener); return () => observers.delete(listener); },
    pageEvents: new EventTarget() });
  await client.getLocalState(); // Queue behind the binding's initial real SDK read.
  for (let count = 0; count < 8; count++) await Promise.resolve();
  assert.deepEqual(await lifecycle.join({ roomId: community.communityId, mode: 'group' }), { ok: true });
  assert.equal(operations.filter(op => op === 'call.join').length, 1);
  assert.equal(log.authorize, 1); assert.equal(log.joins, 1); assert.equal(log.captures, 0);
  assert.equal(lifecycle.snapshot().state, 'listening');
  assert.equal(lifecycle.snapshot().scope.accountId, account.accountId);
  assert.equal(lifecycle.snapshot().scope.deviceId, account.deviceId);
  lifecycle.end(); assert.equal(log.closes, 1);
});
