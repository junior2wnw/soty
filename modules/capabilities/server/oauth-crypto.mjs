import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { AccessError } from './validation.mjs';
import { OAUTH_MODELS, OAUTH_PROFILE, OAUTH_PAYLOAD_BYTES, canonicalOAuthJson, oauthCheck, oauthData, oauthUri } from './oauth-profile.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);

/** No key getters and no fallback key generation. A codec belongs to exactly
 * one immutable registry/issuer/profile. Its caller still validates model pins. */
export function createOAuthArtifactCodec({ registryId, issuer, artifactKey, artifactKeyId }) {
  oauthCheck(typeof registryId === 'string' && /^[a-f0-9]{32}$/u.test(registryId), 'oauth_configuration_invalid');
  try { oauthCheck(issuer === oauthUri(issuer).origin + '/oauth'); }
  catch { throw new AccessError('oauth_configuration_invalid'); }
  oauthCheck(artifactKey == null || artifactKey instanceof Uint8Array, 'oauth_configuration_invalid');
  let key = artifactKey === null || artifactKey === undefined ? null : Buffer.from(artifactKey);
  oauthCheck(key === null ? artifactKeyId == null : (key.byteLength === 32 && typeof artifactKeyId === 'string'
    && /^[A-Za-z0-9._:-]{1,64}$/u.test(artifactKeyId)), 'oauth_configuration_invalid');
  let closed = false;
  function ready() { oauthCheck(!closed && key, 'oauth_storage_key_unavailable'); }
  function aad(model, idHash) {
    oauthCheck(OAUTH_MODELS.includes(model) && hex(idHash));
    return Buffer.from(canonicalOAuthJson(['soty.oauth-artifact.v1', registryId, issuer, model, idHash, OAUTH_PROFILE, artifactKeyId]));
  }
  return Object.freeze({
    available() { return !closed && key !== null; },
    seal({ model, idHash, payload }) {
      ready(); oauthData(payload); const json = canonicalOAuthJson(payload), plain = Buffer.from(json);
      const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, nonce);
      cipher.setAAD(aad(model, idHash));
      const encrypted = Buffer.concat([cipher.update(plain), cipher.final()]);
      return { profile: OAUTH_PROFILE, keyId: artifactKeyId, payloadDigest: sha(plain),
        payloadCipher: Buffer.concat([nonce, cipher.getAuthTag(), encrypted]) };
    },
    open(args) {
      ready();
      oauthData(args, ['model', 'idHash', 'profile', 'keyId', 'payloadDigest', 'payloadCipher']);
      const { model, idHash, profile, keyId, payloadDigest, payloadCipher } = args;
      oauthCheck(keyId === artifactKeyId, 'oauth_storage_key_unavailable');
      oauthCheck(profile === OAUTH_PROFILE && hex(payloadDigest) && payloadCipher instanceof Uint8Array
        && payloadCipher.byteLength >= 30 && payloadCipher.byteLength <= OAUTH_PAYLOAD_BYTES + 28, 'capabilities_storage_corrupt');
      try {
        const bytes = Buffer.from(payloadCipher), decipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
        decipher.setAAD(aad(model, idHash)); decipher.setAuthTag(bytes.subarray(12, 28));
        const plain = Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]);
        const json = new TextDecoder('utf-8', { fatal: true }).decode(plain);
        oauthCheck(sha(plain) === payloadDigest, 'capabilities_storage_corrupt');
        const payload = JSON.parse(json);
        oauthCheck(canonicalOAuthJson(payload) === json, 'capabilities_storage_corrupt');
        return payload;
      } catch { throw new AccessError('capabilities_storage_corrupt'); }
    },
    close() { if (!closed) { closed = true; key?.fill(0); key = null; } },
  });
}
