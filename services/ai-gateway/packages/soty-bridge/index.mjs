import { createHash, createPrivateKey, createPublicKey, randomUUID, sign, verify } from 'node:crypto';

const fields = ['v', 'kid', 'iss', 'aud', 'accountId', 'deviceId', 'operation', 'requestId', 'authorizationId', 'jti', 'method', 'path', 'bodySha256', 'iat', 'exp'];
const required = fields.filter(field => field !== 'authorizationId');
const canonical = value => `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${JSON.stringify(value[key])}`).join(',')}}`;
const bytes = body => typeof body === 'string' ? Buffer.from(body, 'utf8') : Buffer.isBuffer(body) ? body : null;
const digest = body => createHash('sha256').update(body).digest('hex');

function failure(code = 'bridge_invalid') {
  const error = new Error('Service authorization failed.');
  error.code = code; error.status = 401; error.retryable = false;
  return error;
}

function text(value, maximum) {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum && !/[\u0000-\u0020\u007f]/.test(value);
}

function valid(proof, now) {
  return proof && typeof proof === 'object' && !Array.isArray(proof)
    && Object.keys(proof).every(field => fields.includes(field)) && required.every(field => Object.hasOwn(proof, field))
    && proof.v === 1 && text(proof.kid, 80) && text(proof.iss, 512) && text(proof.aud, 256)
    && text(proof.accountId, 256) && text(proof.deviceId, 256) && text(proof.operation, 80)
    && /^[a-z][a-z0-9.]{0,79}$/.test(proof.operation) && text(proof.requestId, 200)
    && (proof.authorizationId === undefined || text(proof.authorizationId, 200))
    && (!['chat.complete', 'dispatch.authorize'].includes(proof.operation) || proof.authorizationId !== undefined)
    && /^[a-zA-Z0-9_-]{16,128}$/.test(proof.jti) && ['GET', 'POST', 'PATCH', 'DELETE', 'PUT', 'OPTIONS', 'HEAD'].includes(proof.method)
    && text(proof.path, 2048) && proof.path.startsWith('/') && !proof.path.startsWith('//') && !/[#\\]/.test(proof.path)
    && /^[0-9a-f]{64}$/.test(proof.bodySha256)
    && Number.isSafeInteger(proof.iat) && Number.isSafeInteger(proof.exp)
    && proof.iat >= 0 && proof.iat <= now + 5 && proof.exp > now && proof.exp > proof.iat && proof.exp - proof.iat <= 30;
}

function key(input, type) {
  const result = input?.type === type ? input : type === 'private' ? createPrivateKey(input) : createPublicKey(input);
  if (result.asymmetricKeyType !== 'ed25519' || result.type !== type) throw new Error('Invalid bridge signing configuration.');
  return result;
}

export function createBridgeSigner({ issuer, audience, kid, privateKey, clock = Date.now }) {
  if (!text(issuer, 512) || !text(audience, 256) || !text(kid, 80) || typeof clock !== 'function') throw new Error('Invalid bridge signing configuration.');
  const signingKey = key(privateKey, 'private');
  return Object.freeze({
    headers({ method, path, body = '', operation, accountId, deviceId, requestId, authorizationId }) {
      const content = bytes(body); const now = Math.floor(clock() / 1000);
      const proof = { v: 1, kid, iss: issuer, aud: audience, accountId, deviceId, operation, requestId, ...(authorizationId === undefined ? {} : { authorizationId }), jti: randomUUID().replaceAll('-', ''), method, path, bodySha256: content && digest(content), iat: now, exp: now + 30 };
      if (!content || !valid(proof, now)) throw failure();
      const payload = Buffer.from(canonical(proof));
      return { 'x-soty-bridge': `${payload.toString('base64url')}.${sign(null, payload, signingKey).toString('base64url')}` };
    },
  });
}

export function createBridgeVerifier({ audience, issuers, claimNonce, clock = Date.now }) {
  if (!text(audience, 256) || !Array.isArray(issuers) || !issuers.length || typeof claimNonce !== 'function' || typeof clock !== 'function') throw new Error('Invalid bridge verification configuration.');
  const trusted = new Map();
  for (const issuer of issuers) {
    if (!text(issuer?.issuer, 512) || !text(issuer?.kid, 80)) throw new Error('Invalid bridge verification configuration.');
    const id = JSON.stringify([issuer.issuer, issuer.kid]);
    if (trusted.has(id)) throw new Error('Duplicate bridge signing identity.');
    trusted.set(id, key(issuer.publicKey, 'public'));
  }
  return Object.freeze({
    async verify({ method, path, body = '', headers }) {
      const header = typeof headers?.get === 'function' ? headers.get('x-soty-bridge') : headers?.['x-soty-bridge'];
      const content = bytes(body);
      if (!content || typeof header !== 'string' || header.length > 8192 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(header)) throw failure();
      const [encoded, signature] = header.split('.');
      const payload = Buffer.from(encoded, 'base64url'); const signatureBytes = Buffer.from(signature, 'base64url');
      if (payload.toString('base64url') !== encoded || signatureBytes.toString('base64url') !== signature || signatureBytes.length !== 64) throw failure();
      let proof;
      try { proof = JSON.parse(payload.toString('utf8')); } catch { throw failure(); }
      const now = Math.floor(clock() / 1000);
      if (!valid(proof, now) || proof.aud !== audience || proof.method !== method || proof.path !== path || proof.bodySha256 !== digest(content) || payload.toString('utf8') !== canonical(proof)) throw failure();
      const verificationKey = trusted.get(JSON.stringify([proof.iss, proof.kid]));
      if (!verificationKey || !verify(null, payload, verificationKey, signatureBytes)) throw failure();
      const validated = Object.freeze(proof);
      if (await claimNonce(validated) !== true) throw failure('bridge_replay');
      return validated;
    },
  });
}
