import { generateKeyPairSync, sign } from 'node:crypto';
import { digestArgs } from '../../../modules/connect/server/index.mjs';

/** Test/integration client for a real Root HTTP server. Signing keys stay in
 * this closure; callers receive only public account replies and safe errors. */
export function createSignedConnectHttpClient({ origin, endpoint = origin + '/api/connect/rpc', fetchImpl = fetch }) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const publicJwk = signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk = encryption.publicKey.export({ format: 'jwk' });
  const post = input => fetchImpl(endpoint, { method: 'POST', headers: { origin, 'content-type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...input }) }).then(response => response.json());
  const good = value => { if (!value?.ok) throw Object.assign(new Error(value?.error?.code ?? 'connect_unavailable'), { code: value?.error?.code ?? 'connect_unavailable' }); return value; };
  async function raw(op, args = {}) {
    const challenge = good(await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: signing.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const call = async (op, args = {}) => good(await raw(op, args));
  return Object.freeze({ raw, call, bootstrap: label => call('bootstrap', { label, encryptionPublicJwk }),
    enrollmentStart: label => call('enrollment.start', { label, encryptionPublicJwk }) });
}
