import { openSync, closeSync, fstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { createPrivateKey, createPublicKey } from 'node:crypto';
import { TextDecoder } from 'node:util';
import { createBridgeSigner, createBridgeVerifier } from '../services/ai-gateway/packages/soty-bridge/index.mjs';

export const ROOT_AI_ISSUER = 'https://4-2.xn--p1ai';
export const GATEWAY_AI_ISSUER = 'urn:soty:ai-gateway:production';
export const GATEWAY_AI_AUDIENCE = 'urn:soty:ai-gateway:v1';
export const ROOT_AI_AUDIENCE = 'urn:soty:root-ai-dispatch:v1';
const installed = new WeakSet();
const invalid = () => { throw Object.assign(new Error('ai_configuration_invalid'), { code: 'ai_configuration_invalid' }); };
const need = value => { if (!value) invalid(); };
export function isAccountAiConfiguration(value) { return installed.has(value); }
function exact(value, required, optional = []) {
  need(value && typeof value === 'object' && !Array.isArray(value));
  need(required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => [...required, ...optional].includes(key)));
}
function bindings(values, field, credentialPin = false) {
  need(Array.isArray(values) && values.length <= 128); const result = new Map();
  for (const value of values) {
    exact(value, [field, 'accountId', 'models', ...(credentialPin ? ['tokenHash'] : [])]);
    if (credentialPin) need(typeof value.tokenHash === 'string' && /^[0-9a-f]{64}$/u.test(value.tokenHash));
    need(typeof value[field] === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value[field]) && !result.has(value[field]));
    need(typeof value.accountId === 'string' && /^(?:acct|svc)_[A-Za-z0-9_-]{32}$/u.test(value.accountId));
    need(Array.isArray(value.models) && value.models.length > 0 && value.models.length <= 32
      && value.models.every(model => typeof model === 'string' && /^[A-Za-z0-9_./:-]{1,160}$/u.test(model))
      && new Set(value.models).size === value.models.length);
    result.set(value[field], Object.freeze({ accountId: value.accountId, models: Object.freeze([...value.models]), ...(credentialPin ? { tokenHash: value.tokenHash } : {}) }));
  }
  return id => result.get(id) ?? null;
}

/** This factory captures server-only key objects. Its result serializes no key.
 * The explicit installed service origin is the only permitted HTTP target. */
export function captureAccountAiConfiguration(value, { clock = Date.now, allowLoopbackFixture = false } = {}) {
  try {
    exact(value, ['schema', 'issuer', 'gatewayOrigin', 'signing', 'gatewaySigner'], ['applications', 'legacyConnectors']);
    need(value.schema === 'soty.account-ai-installed.v1' && value.issuer === ROOT_AI_ISSUER);
    const url = new URL(value.gatewayOrigin);
    need(url.origin === value.gatewayOrigin && !url.username && !url.password && !url.search && !url.hash
      && (url.protocol === 'https:' || url.protocol === 'http:' && (url.hostname === 'inference-commerce-gateway-1'
        || allowLoopbackFixture && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))));
    exact(value.signing, ['kid', 'privateKey']); exact(value.gatewaySigner, ['issuer', 'kid', 'publicKey']);
    need(value.gatewaySigner.issuer === GATEWAY_AI_ISSUER);
    for (const kid of [value.signing.kid, value.gatewaySigner.kid]) need(typeof kid === 'string' && /^[A-Za-z0-9_.:-]{1,80}$/u.test(kid));
    need(typeof value.signing.privateKey === 'string' && value.signing.privateKey.length <= 8192
      && typeof value.gatewaySigner.publicKey === 'string' && value.gatewaySigner.publicKey.length <= 8192
      && !value.gatewaySigner.publicKey.includes('PRIVATE KEY'));
    const privateKey = createPrivateKey(value.signing.privateKey), publicKey = createPublicKey(value.gatewaySigner.publicKey);
    need(privateKey.asymmetricKeyType === 'ed25519' && publicKey.asymmetricKeyType === 'ed25519');
    need(!createPublicKey(privateKey).export({ format: 'der', type: 'spki' }).equals(publicKey.export({ format: 'der', type: 'spki' })));
    const applicationFor = bindings(value.applications ?? [], 'appId'), legacyConnectorFor = bindings(value.legacyConnectors ?? [], 'connectorId', true);
    const issuer = value.issuer, gatewayOrigin = value.gatewayOrigin, kid = value.signing.kid, gatewayKid = value.gatewaySigner.kid;
    const sponsorIds = Object.freeze([...new Set([...(value.applications ?? []), ...(value.legacyConnectors ?? [])].map(binding => binding.accountId).filter(id => id.startsWith('svc_')))]);
    const config = Object.freeze({ issuer, gatewayOrigin, applicationFor, legacyConnectorFor,
      sponsorAccountIds: () => sponsorIds, isSponsorAccount: id => sponsorIds.includes(id),
      signer: () => createBridgeSigner({ issuer, audience: GATEWAY_AI_AUDIENCE, kid, privateKey, clock }),
      verifier: claimNonce => createBridgeVerifier({ audience: ROOT_AI_AUDIENCE, issuers: [{ issuer: GATEWAY_AI_ISSUER, kid: gatewayKid, publicKey }], claimNonce, clock }) });
    installed.add(config); return config;
  } catch { invalid(); }
}

export function loadAccountAiConfiguration(env = process.env) {
  const filename = env.SOTY_ACCOUNT_AI_CONFIG_FILE;
  if (filename === undefined || filename === '') return {};
  let fd; const bytes = Buffer.alloc(65537);
  try {
    need(typeof filename === 'string' && path.isAbsolute(filename) && filename.length <= 4096 && !filename.includes('\0'));
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0) | (constants.O_NOFOLLOW || 0));
    const stat = fstatSync(fd); need(stat.isFile() && stat.size > 0 && stat.size <= 65536 && (process.platform === 'win32' || (stat.mode & 0o077) === 0));
    let size = 0;
    while (size < bytes.length) { const count = readSync(fd, bytes, size, bytes.length - size, size); if (!count) break; size += count; }
    need(size > 0 && size <= 65536);
    return { accountAi: captureAccountAiConfiguration(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, size)))) };
  } catch { invalid(); }
  finally { bytes.fill(0); if (fd !== undefined) closeSync(fd); }
}
