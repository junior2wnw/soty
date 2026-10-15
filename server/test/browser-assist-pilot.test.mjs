import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createBrowserAssistPilot } from '../browser-assist-pilot.js';
import { validCommand } from '../../modules/source-app/browser/assist-wire.mjs';
const origin = 'https://root.test';
const binding = { appId: 'app-' + 'a'.repeat(32), origin: 'https://app.test', targetRevision: 1, targetDigest: 'b'.repeat(64), generation: 1, channel: 'c'.repeat(32) };
function identity() { const k = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }); return { privateKey: k.privateKey, publicJwk: k.publicKey.export({ format: 'jwk' }) }; }
async function invoke(service, who, op, args) {
  const c = await service.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }); assert.equal(c.ok, true);
  return service.handle({ origin, op, args, proof: { challengeId: c.challengeId, publicJwk: who.publicJwk,
    signature: sign('sha256', Buffer.from(c.message), { key: who.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
}
test('real signed Connect continuation: owner/device, stale source, no replayed action, shutdown', async t => {
  let service, ownerId, active = true, targetRevision = 1, port;
  const abort = new AbortController();
  const pilot = createBrowserAssistPilot({ getConnect: () => service, getApps: () => ({ execute({ actor, op, args }) {
    assert.equal(op, 'apps.inspect'); if (actor.accountId !== ownerId || args.appId !== binding.appId) throw Error('denied');
    return { app: { state: active ? 'enabled' : 'revoked' }, actions: { canPreview: true }, addresses: { canonical: { origin: binding.origin } },
      source: { revision: targetRevision, digest: binding.targetDigest } };
  } }) });
  const extension = { operations: new Set([...pilot.operations, 'pilot.run']), async executeAsync(call) {
    if (call.op !== 'pilot.run') return pilot.executeAsync(call);
    port = pilot.openTurn({ actor: call.actor, requestId: call.args.requestId, binding: call.args.binding, signal: abort.signal });
    try { return await port.tools.browser_observe.execute({}, abort.signal); } finally { port.close(); }
  } };
  service = createConnectService({ databasePath: ':memory:', projectId: 'pilot-test', allowedOrigins: [origin], extensions: [extension] });
  t.after(() => { abort.abort(); pilot.close(); service.close(); });
  const owner = identity(), other = identity();
  const boot = who => invoke(service, who, 'bootstrap', { label: 'Fixture', encryptionPublicJwk: identity().publicJwk });
  ownerId = (await boot(owner)).accountId; await boot(other);
  const args = { requestId: 'pilot_turn_1', channel: binding.channel };
  const run = invoke(service, owner, 'pilot.run', { requestId: args.requestId, binding });
  let request;
  for (let i = 0; i < 20; i++) { await new Promise(r => setImmediate(r)); request = await invoke(service, owner, 'agent.browser.poll', args); if (request.commandId) break; }
  assert.equal(request.command.kind, 'observe');
  assert.equal((await invoke(service, other, 'agent.browser.poll', args)).pending, false);
  const reply = { ...args, commandId: request.commandId, result: { ok: true, data: { untrusted: true, snapshot: 'e'.repeat(32), items: [] } } };
  assert.equal((await invoke(service, other, 'agent.browser.reply', reply)).ok, false);
  assert.equal((await invoke(service, owner, 'agent.browser.reply', { ...reply, channel: 'd'.repeat(32) })).ok, false);
  assert.equal((await invoke(service, owner, 'agent.browser.reply', reply)).accepted, true);
  const result = await run; assert.equal(result.ok, true); assert.equal(result.evidence[0].sourceStatus, 'browser-reported-untrusted');
  // A completed turn cannot be used as a new action channel.
  assert.equal((await invoke(service, owner, 'agent.browser.reply', reply)).ok, false);
  targetRevision++;
  assert.equal((await invoke(service, owner, 'pilot.run', { requestId: 'pilot_turn_2', binding })).ok, false);
  targetRevision--; active = false;
  assert.equal((await invoke(service, owner, 'pilot.run', { requestId: 'pilot_turn_3', binding })).ok, false);
  active = true; pilot.close();
  assert.equal((await invoke(service, owner, 'pilot.run', { requestId: 'pilot_turn_4', binding })).ok, false);
});
test('closed data grammar rejects script, selectors, URLs and secret/file operations', () => {
  for (const command of [{ kind: 'eval', script: '1' }, { kind: 'observe', selector: '*' }, { kind: 'navigate', url: 'https://other.test' },
    { kind: 'password', text: 'x' }, { kind: 'file', path: '/' }, { kind: 'fill', target: 'n0', snapshot: 'e'.repeat(32), text: 'x\n' }]) assert.equal(validCommand(command), false);
  assert.equal(validCommand({ kind: 'fill', target: 'n0', snapshot: 'e'.repeat(32), text: 'Own task' }), true);
});
