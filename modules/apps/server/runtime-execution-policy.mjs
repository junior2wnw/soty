import { parse } from 'tldts';
import { AppsError } from './protocol.mjs';
import { validDnsHostname } from './domain-policy.mjs';
import { supportedRuntimeProfile } from './schema.mjs';

const SCHEMA = 'soty.app-execution-policy.v1';
const registries = new WeakMap();
const fail = () => { throw new AppsError('app_execution_policy_invalid', 503); };
const check = value => { if (!value) fail(); };
const positive = value => Number.isSafeInteger(value) && value > 0;
const appId = value => typeof value === 'string' && /^app-[a-f0-9]{32}$/.test(value);
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

function fields(value, expected) {
  check(value && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value)));
  const properties = Object.getOwnPropertyDescriptors(value), names = Reflect.ownKeys(properties);
  check(names.length === expected.length && expected.every(name => properties[name]?.enumerable && 'value' in properties[name]));
  return Object.fromEntries(expected.map(name => [name, properties[name].value]));
}
function array(value, maximum, capture) {
  check(Array.isArray(value) && value.length <= maximum && Reflect.ownKeys(value).length === value.length + 1);
  const result = [];
  for (let index = 0; index < value.length; index++) {
    const property = Object.getOwnPropertyDescriptor(value, index);
    check(property?.enumerable && 'value' in property); result.push(capture(property.value));
  }
  return Object.freeze(result);
}
function source(value) {
  check(typeof value === 'string' && value.length > 0 && value.length <= 2048
    && /^[\x21-\x7e]+$/.test(value) && !/[\\"';,*<>]/.test(value));
  let url;
  try { url = new URL(value); } catch { fail(); }
  check(url.protocol === 'https:' && !url.username && !url.password && !url.search && !url.hash
    && validDnsHostname(url.hostname) && url.hostname.includes('.') && (!url.port || Number(url.port) > 0));
  const host = parse(url.hostname, { allowPrivateDomains: false, detectSpecialUse: true });
  // Syntactic public-domain admission, not DNS resolution or a server fetch.
  check(!host.isIp && !host.isSpecialUse && host.isIcann === true && host.domain);
  check(value === url.origin || value === url.href && url.pathname.endsWith('/'));
  let path;
  try { path = decodeURIComponent(url.pathname); } catch { fail(); }
  check(!/[\s\u0000-\u001f\u007f\\"';,*<>?#%]/.test(path)
    && !path.split('/').some(part => part === '.' || part === '..'));
  return value;
}
function sources(value) {
  const result = array(value, 8, source);
  check(new Set(result).size === result.length); return result;
}
function target(value) {
  const result = fields(value, ['revision', 'digest', 'profile']);
  check(positive(result.revision) && digest(result.digest) && supportedRuntimeProfile(result.profile));
  return Object.freeze(result);
}
function scope(value) {
  const result = fields(value, ['appId', 'policyEpoch', 'target']);
  check(appId(result.appId) && positive(result.policyEpoch));
  return Object.freeze({ ...result, target: target(result.target) });
}
const key = value => JSON.stringify([value.appId, value.policyEpoch, value.target.revision, value.target.digest, value.target.profile]);

/** Host-owned immutable input. No request, identity, connector or Source grant. */
export function captureRuntimeExecutionPolicy(input = { schema: SCHEMA, admissions: [] }) {
  try {
    const value = fields(input, ['schema', 'admissions']); check(value.schema === SCHEMA);
    const index = new Map();
    const admissions = array(value.admissions, 128, item => {
      const entry = fields(item, ['appId', 'policyEpoch', 'target', 'capabilities']);
      const binding = scope({ appId: entry.appId, policyEpoch: entry.policyEpoch, target: entry.target });
      const capabilities = fields(entry.capabilities, ['wasm', 'workers', 'connectSrc', 'imgSrc']);
      check(typeof capabilities.wasm === 'boolean' && ['none', 'self'].includes(capabilities.workers));
      const captured = Object.freeze({ wasm: capabilities.wasm, workers: capabilities.workers,
        connectSrc: sources(capabilities.connectSrc), imgSrc: sources(capabilities.imgSrc) });
      const identity = key(binding); check(!index.has(identity)); index.set(identity, captured);
      return Object.freeze({ ...binding, capabilities: captured });
    });
    const registry = Object.freeze({ schema: SCHEMA, admissions }); registries.set(registry, index); return registry;
  } catch { fail(); }
}

/** Only a current full app/epoch/target tuple can select captured capabilities. */
export function findRuntimeExecutionCapabilities(registry, binding) {
  const index = registries.get(registry); check(index);
  try { return index.get(key(scope(binding))) ?? null; } catch { return null; }
}
