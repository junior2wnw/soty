import { readFile } from 'node:fs/promises';
import { createBridgeSigner, createBridgeVerifier } from '../../../packages/soty-bridge/index.mjs';
import { DomainError } from '../../../packages/domain/index.mjs';
import { sotyAllowance } from './soty-policy.mjs';

const CALLBACK_PATH = '/internal/soty-ai/v1/authorize-dispatch';

export async function loadSotyBridgeConfig(path) {
  if (!path) return null;
  try {
    const raw = await readFile(path, 'utf8');
    if (Buffer.byteLength(raw) > 131072) throw new Error();
    return validateSotyBridgeConfig(JSON.parse(raw));
  } catch { throw new Error('Invalid Soty bridge configuration.'); }
}

export function validateSotyBridgeConfig(config) {
  if (!config || config.schema !== 'soty.ai-gateway-bridge.v1' || Object.keys(config).some(key => !['schema', 'audience', 'issuers', 'signing', 'root', 'allowance'].includes(key))
      || !config.signing || !config.root || !Array.isArray(config.issuers) || !config.issuers.length) throw new Error('Invalid Soty bridge configuration.');
  const origin = new URL(config.root.origin);
  if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== '/'
      || (origin.protocol !== 'https:' && !(origin.protocol === 'http:' && config.root.allowPrivateHttp === true && ['soty-online-chat', 'localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)))) throw new Error('Invalid private Root origin.');
  if (config.audience !== 'urn:soty:ai-gateway:v1' || config.signing.issuer !== 'urn:soty:ai-gateway:production' || config.root.audience !== 'urn:soty:root-ai-dispatch:v1') throw new Error('Invalid bridge service identity.');
  if (Object.keys(config.signing).some(key => !['issuer', 'kid', 'privateKey'].includes(key)) || Object.keys(config.root).some(key => !['origin', 'audience', 'allowPrivateHttp'].includes(key))) throw new Error('Invalid bridge configuration fields.');
  if (config.issuers.some(entry => !entry || Object.keys(entry).some(key => !['issuer', 'kid', 'publicKey'].includes(key)))) throw new Error('Invalid bridge trust registry.');
  const allowance = sotyAllowance(config.allowance);
  createBridgeSigner({ ...config.signing, audience: config.root.audience });
  createBridgeVerifier({ audience: config.audience, issuers: config.issuers, claimNonce: async () => true });
  return { ...config, root: { ...config.root, origin: origin.origin }, allowance };
}

export function createSotyBridge({ config, store, fetchImpl = fetch, clock = Date.now }) {
  const verifier = createBridgeVerifier({ audience: config.audience, issuers: config.issuers, claimNonce: proof => store.claimBridgeNonce(proof), clock });
  const signer = createBridgeSigner({ ...config.signing, audience: config.root.audience, clock });
  return {
    verify: input => verifier.verify(input),
    async authorizeDispatch({ proof, gatewayRequestId, providerId, model, signal }) {
      if (signal.aborted) throw new DomainError('request_canceled', 'Запрос отменён.', 499);
      const body = JSON.stringify({ issuer: proof.iss, accountId: proof.accountId, deviceId: proof.deviceId, authorizationId: proof.authorizationId, requestId: proof.requestId, gatewayRequestId, bodySha256: proof.bodySha256, providerId, model });
      const headers = signer.headers({ method: 'POST', path: CALLBACK_PATH, body, operation: 'dispatch.authorize', accountId: proof.accountId, deviceId: proof.deviceId, requestId: proof.requestId, authorizationId: proof.authorizationId });
      try {
        const response = await fetchImpl(`${config.root.origin}${CALLBACK_PATH}`, { method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body, redirect: 'error', signal: AbortSignal.any([signal, AbortSignal.timeout(3000)]) });
        if (!response.ok) { await response.body?.cancel(); throw new Error(); }
        const reader = response.body?.getReader(); const chunks = []; let size = 0;
        if (!reader) throw new Error();
        try {
          while (true) {
            const part = await reader.read(); if (part.done) break;
            size += part.value.length; if (size > 1024) throw new Error(); chunks.push(part.value);
          }
        } finally { await reader.cancel().catch(() => {}); }
        const result = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (result?.authorized !== true || Object.keys(result).some(key => key !== 'authorized') || signal.aborted) throw new Error();
      } catch {
        throw new DomainError(signal.aborted ? 'request_canceled' : 'dispatch_authorization_denied', signal.aborted ? 'Запрос отменён.' : 'Разрешение на запрос ИИ больше не действует.', signal.aborted ? 499 : 403);
      }
    },
  };
}
