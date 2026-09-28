import { generateKeyPairSync, sign, randomUUID } from 'node:crypto';
import { access } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { resolve } from 'node:path';
import { digestArgs } from '../modules/connect/server/index.mjs';

// Explicit localhost browser acceptance. Accounts use the real signed Connect
// boundary; private keys stay in this process and are never logged or saved.
await access(resolve('output/world-validation-20260928/data/world/world.sqlite'));
const origin = 'http://localhost:5200';
async function rpc(body) {
  const response = await fetch(`${origin}/api/connect/rpc`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin }, body: JSON.stringify({ protocol: 1, ...body }) });
  const value = await response.json();
  if (!value.ok) throw new Error(/^[a-z0-9_:-]+$/i.test(value.error?.code ?? '') ? value.error.code : 'local_fixture_failed');
  return value;
}
async function call(actor, op, args = {}) {
  const challenge = await rpc({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
  return rpc({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk, signature: sign('sha256', Buffer.from(challenge.message), { key: actor.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
}
const actors = [];
for (let i = 0; i < 4; i++) {
  const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const actor = { privateKey: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }) };
  await call(actor, 'bootstrap', { label: `Проверка чата ${i + 1} · локально`, encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) });
  actors.push(actor);
}
const created = await call(actors[0], 'world.community.create', { requestId: randomUUID(), name: 'Проверка чата · локально', description: 'Временный сценарий приёмки. Только localhost.', joinPolicy: 'open' });
const communityId = created.communityId;
for (const actor of actors.slice(1)) await call(actor, 'world.membership.join', { communityId });
await call(actors[0], 'world.chat.send', { communityId, clientId: randomUUID(), text: 'QA baseline · перед быстрой серией' });
console.log(JSON.stringify({ state: 'ready', url: `${origin}/#community/${communityId}/chat`, communityId, commands: ['burst', 'archive', 'quit'], credentialsSaved: false }));
const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
let sent = false;
for await (const line of input) {
  const command = line.trim();
  try {
    if (command === 'burst' && !sent) {
      const start = performance.now();
      await Promise.all(Array.from({ length: 160 }, (_, index) => call(actors[index % 4], 'world.chat.send', { communityId, clientId: randomUUID(), text: `QA catchup ${String(index + 1).padStart(3, '0')} / 160` })));
      sent = true; console.log(JSON.stringify({ state: 'burst_complete', messages: 160, durationMs: Math.round(performance.now() - start) }));
    } else if (command === 'archive') {
      const current = await call(actors[0], 'world.community.get', { communityId });
      await call(actors[0], 'world.community.archive', { communityId, expectedRevision: current.community.revision });
      console.log(JSON.stringify({ state: 'archived', communityId })); break;
    } else if (command === 'quit') break;
  } catch (error) { console.log(JSON.stringify({ state: 'error', code: String(error.message).replace(/[^a-z0-9_:-]/gi, '').slice(0, 80) })); }
}
input.close();
