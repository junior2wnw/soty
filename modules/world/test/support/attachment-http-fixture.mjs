import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { signedFixture, good, ORIGIN } from './world6-signed-fixture.mjs';
import { createConnectHandler } from '../../../connect/server/http.mjs';
import { ATTACHMENT_LIMITS as L, attachmentManifest } from '../../attachment-contract.mjs';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

export async function attachmentHttpFixture(t) {
  const f = await signedFixture(t, { world7: true }), [owner] = f.actors;
  const group = good(await f.rpc(owner, 'world.community.create', { requestId: 'attachments_room', name: 'Disposable files' })).community;
  for (const actor of f.actors.slice(1)) good(await f.rpc(actor, 'world.membership.join', { communityId: group.communityId }));
  good(await f.rpc(owner, 'world.chat.send', { communityId: group.communityId, clientId: 'before_attachments', text: 'Before files' }));
  const handler = createConnectHandler({ handle: input => f.connect.handle(input) });
  const server = createServer(async (req, res) => { if (!await handler(req, res)) { res.statusCode = 404; res.end(); } });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const endpoint = `http://127.0.0.1:${server.address().port}/api/connect/rpc`;
  const call = async (actor, op, args = {}) => {
    const input = await f.proof(actor, op, args); if (input.denied) return input.denied;
    const response = await fetch(endpoint, { method: 'POST', headers: { 'content-type': 'application/json', origin: ORIGIN }, body: JSON.stringify({ protocol: 1, op, args, proof: input.proof }) });
    assert.equal(response.headers.get('cache-control'), 'no-store'); return response.json();
  };
  const base = (actor, communityId = group.communityId) => ({ communityId, expectedAccountId: actor.accountId });
  let sequence = 0;
  const upload = async (actor, bytes, name = 'файл.bin', target = base(actor)) => {
    const args = { ...target, attachmentId: `file_${++sequence}` };
    good(await call(actor, 'world.attachment.begin', { ...args, name, size: bytes.length, mediaType: 'application/octet-stream' }));
    const chunks = [];
    for (let offset = 0, index = 0; offset < bytes.length; offset += L.chunkBytes, index++) {
      const part = bytes.subarray(offset, offset + L.chunkBytes); chunks.push({ bytes: part.length, sha256: sha(part) });
      good(await call(actor, 'world.attachment.chunk', { ...args, index, sha256: sha(part), data: part.toString('base64') }));
    }
    return good(await call(actor, 'world.attachment.complete', { ...args, manifestSha256: sha(attachmentManifest(bytes.length, chunks)) })).attachment;
  };
  return { ...f, group, call, base, upload, readChunks: async (actor, ref, target = base(actor)) => {
    const parts = []; for (let index = 0; index < ref.chunks; index++) { const part = good(await call(actor, 'world.attachment.read', { ...target, attachmentId: ref.attachmentId, index })); parts.push(Buffer.from(part.data, 'base64')); } return Buffer.concat(parts);
  } };
}
