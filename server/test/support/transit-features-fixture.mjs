import assert from 'node:assert/strict';
import { createCipheriv, randomBytes, verify } from 'node:crypto';

export function profileFeaturesResponse(url, request, publicKey, deliveryKey, changes = {}) {
  if (new URL(url).pathname !== '/v1/features') return null;
  const body = JSON.parse(request.body);
  assert.equal(body.schema, 'soty.transit-profile-features.v1');
  assert.deepEqual(Object.keys(body).sort(), ['expiresAt','issuedAt','issuer','nonce','schema']);
  assert.ok(verify(null, Buffer.concat([Buffer.from('TRANSIT/SOTY-PROFILE-FEATURES/V1\0'), Buffer.from(request.body)]), publicKey,
    Buffer.from(request.headers['x-transit-signature'],'base64')));
  const nonce=randomBytes(12),cipher=createCipheriv('aes-256-gcm',deliveryKey,nonce);
  cipher.setAAD(Buffer.concat([Buffer.from('TRANSIT/PROFILE-FEATURES-DELIVERY/V1\0'),Buffer.from(request.body)]));
  const bytes=Buffer.concat([cipher.update(JSON.stringify({schema:'transit.profile-features.v1',portableProfiles:true,portableCoreVersion:'1.14.4',subscriptions:true,usage:false,topology:false,...changes})),cipher.final()]);
  return Response.json({schema:'transit.encrypted-profile-features.v1',nonce:nonce.toString('base64'),tag:cipher.getAuthTag().toString('base64'),ciphertext:bytes.toString('base64')});
}
