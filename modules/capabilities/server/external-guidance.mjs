import { snapshot } from '../../app-contract/json.mjs';
import { assert, canonicalHash, canonicalJson, exact, freezeDeep, identifier, integer } from './validation.mjs';

export const EXTERNAL_GUIDANCE_SCHEMA = 'soty.application-guidance.v1';
export const EXTERNAL_GUIDANCE_LIMITS = Object.freeze({ entries: 128, perCapability: 32, contentBytes: 32768, hostBytes: 65536 });
const CODE = 'external_guidance_invalid';
const check = value => assert(value, CODE);
const key = value => `${value.capabilityId}@${value.version}`;
const refKey = value => `${value.id}@${value.version}`;
function contract(value) {
  exact(value, ['capabilityId', 'version', 'digest'], CODE);
  check(Object.keys(value).length === 3); identifier(value.capabilityId, CODE); integer(value.version, 1, 1000000, CODE);
  check(typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest)); return value;
}
function text(value, maximum, multiline = false) {
  check(typeof value === 'string' && value.trim().length > 0 && value.length <= maximum && value.isWellFormed()
    && !(multiline ? /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u : /[\u0000-\u001f\u007f]/u).test(value));
  return value;
}
/** Trusted static data only. No package import, network lookup or instruction execution.
 * Content-addressed references cannot be silently rewritten under an old pin. */
export function captureExternalGuidance(input = []) {
  const values = snapshot(input); check(Array.isArray(values) && values.length <= EXTERNAL_GUIDANCE_LIMITS.entries);
  const unique = new Set(), counts = new Map(); let bytes = 0;
  return freezeDeep(values.map(raw => {
    exact(raw, ['contract', 'kind', 'language', 'title', 'summary', 'content'], CODE); check(Object.keys(raw).length === 6);
    const reference = contract(raw.contract); check(['skill', 'document'].includes(raw.kind) && ['ru', 'en'].includes(raw.language));
    text(raw.title, 160); text(raw.summary, 2000); text(raw.content, EXTERNAL_GUIDANCE_LIMITS.contentBytes, true);
    check(Buffer.byteLength(raw.content) <= EXTERNAL_GUIDANCE_LIMITS.contentBytes);
    const content = { schema: EXTERNAL_GUIDANCE_SCHEMA, contract: reference, kind: raw.kind, language: raw.language,
      title: raw.title, summary: raw.summary, content: raw.content };
    const digest = canonicalHash(content), ref = { id: `guide:${raw.kind}/${digest}`, version: 1, digest };
    check(!unique.has(ref.id)); unique.add(ref.id);
    const count = (counts.get(key(reference)) ?? 0) + 1; check(count <= EXTERNAL_GUIDANCE_LIMITS.perCapability); counts.set(key(reference), count);
    bytes += Buffer.byteLength(canonicalJson(content)); check(bytes <= EXTERNAL_GUIDANCE_LIMITS.hostBytes);
    return { ...content, reference: ref };
  }));
}

/** Fresh Root/App/Source contract authority gates both index and content.
 * A skill is returned as application data, never as a new source of authority. */
export function createExternalGuidance({ entries, contracts, getContract }) {
  check(typeof getContract === 'function' && getContract.constructor?.name !== 'AsyncFunction');
  const byCapability = new Map();
  for (const item of entries) {
    const accepted = contracts.find(value => key(value) === key(item.contract));
    assert(accepted && accepted.digest === item.contract.digest, 'external_guidance_contract_mismatch');
    const values = byCapability.get(key(accepted)) ?? []; values.push(item); byCapability.set(key(accepted), values);
  }
  function selected(actor, input) {
    const reference = contract(snapshot(input));
    const current = getContract({ actor, reference });
    check(current.reference.digest === reference.digest); return { reference, current, values: byCapability.get(key(reference)) ?? [] };
  }
  return Object.freeze({
    list({ actor, reference: input }) {
      const { reference, current, values } = selected(actor, input);
      const items = values.map(({ reference, kind, language, title, summary }) => ({ reference, kind, language, title, summary }));
      const result = { schema: 'soty.authorized-app-guidance-index.v1', scope: 'authorized', authority: 'application-data',
        appId: current.appId, contract: reference, revision: canonicalHash(items), items };
      canonicalJson(result, { maxBytes: 65536 }); getContract({ actor, reference }); return freezeDeep(result);
    },
    get({ actor, reference: input, guidance: guidanceInput }) {
      const guidance = snapshot(guidanceInput); exact(guidance, ['id', 'version', 'digest'], CODE); check(Object.keys(guidance).length === 3);
      identifier(guidance.id, CODE); check(guidance.version === 1 && typeof guidance.digest === 'string' && /^[a-f0-9]{64}$/u.test(guidance.digest));
      const { reference, current, values } = selected(actor, input);
      const item = values.find(value => refKey(value.reference) === refKey(guidance));
      assert(item && item.reference.digest === guidance.digest, 'external_guidance_not_found');
      const { kind, language, title, summary, content } = item;
      const result = { schema: 'soty.authorized-app-guidance.v1', scope: 'authorized', authority: 'application-data',
        appId: current.appId, contract: reference, reference: item.reference, kind, language, title, summary, content };
      canonicalJson(result, { maxBytes: 65536 }); getContract({ actor, reference }); return freezeDeep(result);
    },
  });
}
