import { freezeDeep } from '../capabilities/server/validation.mjs';
import { check, snapshot, contractDigest, canonicalContractJson, LIMITS } from './json.mjs';
export { ContractError, parseContractJson, canonicalContractJson, contractDigest, LIMITS } from './json.mjs';
export const SCHEMA = 'soty.app-agent.v1';
export const AUTHOR_SCHEMA = 'soty.app-author-draft.v1';
function closed(value, required, optional = [], code = 'closed_fields') {
  check(value && typeof value === 'object' && !Array.isArray(value), code);
  check(required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => [...required, ...optional].includes(key)), code);
}
const id = value => {
  check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,95}$/.test(value) && !value.includes('..') && !value.includes('//'), 'invalid_id');
  return value;
};
const namespaced = value => {
  check(typeof value === 'string' && /^[A-Za-z][A-Za-z0-9._-]{0,63}:[A-Za-z][A-Za-z0-9._/-]{0,95}$/.test(value) && !value.includes('..') && !value.includes('//'), 'invalid_namespace');
  return value;
};
const namespace = value => check(typeof value === 'string' && /^[A-Za-z][A-Za-z0-9._-]{0,63}$/.test(value), 'invalid_namespace');
const version = value => check(Number.isSafeInteger(value) && value >= 1 && value <= 1000000, 'unsupported_version');
const digest = value => check(typeof value === 'string' && /^[a-f0-9]{64}$/.test(value), 'invalid_digest');
function ref(value) { closed(value, ['id', 'version', 'digest']); namespaced(value.id); version(value.version); digest(value.digest); }
function refs(value, max = LIMITS.refs) {
  check(Array.isArray(value) && value.length <= max, 'input_limit'); value.forEach(ref);
  check(new Set(value.map(v => v.id)).size === value.length, 'duplicate_reference');
}
const choice = (value, allowed, code = 'invalid_profile') => check(allowed.includes(value), code);
const same = (left, right) => canonicalContractJson(left) === canonicalContractJson(right);
function source(value) { closed(value, ['id', 'revision', 'digest']); namespaced(value.id); version(value.revision); digest(value.digest); }
function auth(value) {
  choice(value?.mode, ['public', 'linked-existing', 'shared-soty'], 'unsupported_auth_profile');
  if (value.mode === 'public') closed(value, ['mode']);
  else { closed(value, ['mode', 'profile']); ref(value.profile); }
}
function strings(value, allowed) {
  check(Array.isArray(value) && value.length <= 16 && value.length > 0 && new Set(value).size === value.length, 'invalid_set');
  value.forEach(v => choice(v, allowed));
}
const EFFECTS = ['read', 'create', 'update', 'delete', 'publish', 'send', 'spend'];
function feedback(value, visibility) {
  closed(value, ['mode', 'provider', 'captureProfile', 'retentionProfile', 'submitAudience', 'ticketVisibility']);
  check(value.mode === 'required', 'feedback_required');
  ref(value.provider); ref(value.captureProfile); ref(value.retentionProfile);
  choice(value.submitAudience, ['members', 'public']);
  check(value.ticketVisibility === 'reporter-and-support', 'ticket_privacy');
  check(visibility !== 'private' || value.submitAudience === 'members', 'audience_privacy');
}
export function validateAuthorDraft(input) {
  const value = snapshot(input);
  closed(value, ['title'], ['schema']);
  if (!Object.hasOwn(value, 'schema')) value.schema = AUTHOR_SCHEMA;
  check(value.schema === AUTHOR_SCHEMA, 'unsupported_schema');
  check(typeof value.title === 'string' && value.title.trim().length > 0 && value.title.length <= 160, 'invalid_title');
  return freezeDeep(value);
}
function jsonSchema(schema, depth = 0) {
  check(depth <= 6, 'schema_limit');
  closed(schema, ['type'], ['$schema', 'properties', 'required', 'additionalProperties', 'items', 'minItems', 'maxItems', 'minLength', 'maxLength', 'minimum', 'maximum', 'enum']);
  choice(schema.type, ['object', 'array', 'string', 'integer', 'boolean', 'null'], 'schema_unsupported');
  if (schema.$schema !== undefined) check(schema.$schema === 'https://json-schema.org/draft/2020-12/schema', 'schema_unsupported');
  if (schema.enum !== undefined) {
    check(Array.isArray(schema.enum) && schema.enum.length > 0 && schema.enum.length <= 32, 'schema_limit');
    check(schema.enum.every(v => v === null || ['string', 'number', 'boolean'].includes(typeof v)), 'schema_unsupported');
  }
  if (schema.type === 'object') {
    check(schema.properties && typeof schema.properties === 'object' && !Array.isArray(schema.properties) && Object.keys(schema.properties).length <= 32 && schema.additionalProperties === false, 'schema_unsupported');
    for (const [key, child] of Object.entries(schema.properties)) {
      check(/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(key), 'schema_unsupported'); jsonSchema(child, depth + 1);
    }
    if (schema.required !== undefined) check(Array.isArray(schema.required) && new Set(schema.required).size === schema.required.length && schema.required.every(v => Object.hasOwn(schema.properties, v)), 'schema_unsupported');
  } else check(schema.properties === undefined && schema.required === undefined && schema.additionalProperties === undefined, 'schema_unsupported');
  if (schema.type === 'array') {
    jsonSchema(schema.items, depth + 1);
    check(Number.isSafeInteger(schema.maxItems) && schema.maxItems >= 0 && schema.maxItems <= 128, 'schema_limit');
  } else check(schema.items === undefined && schema.minItems === undefined && schema.maxItems === undefined, 'schema_unsupported');
  for (const [key, type] of [['minLength', 'string'], ['maxLength', 'string'], ['minItems', 'array'], ['maxItems', 'array'], ['minimum', 'integer'], ['maximum', 'integer']]) {
    if (schema[key] !== undefined) {
      check(schema.type === type && Number.isSafeInteger(schema[key]), 'schema_unsupported');
      if (!['minimum', 'maximum'].includes(key)) check(schema[key] >= 0 && schema[key] <= 100000, 'schema_limit');
    }
  }
  if (schema.type === 'string') check(Number.isSafeInteger(schema.maxLength) && schema.maxLength >= 0 && schema.maxLength <= 100000, 'schema_limit');
  for (const [min, max] of [['minLength', 'maxLength'], ['minItems', 'maxItems'], ['minimum', 'maximum']]) {
    if (schema[min] !== undefined && schema[max] !== undefined) check(schema[min] <= schema[max], 'schema_limit');
  }
}
const orderedRefs = values => [...values].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
/** Semantic digest excludes prose, skills/docs, binding and deployment metadata. */
export function capabilityDigest(capability) {
  const { id: capabilityId, version: capabilityVersion, inputSchema, outputSchema, resources, effects, recipients } = snapshot(capability);
  namespaced(capabilityId); version(capabilityVersion);
  jsonSchema(inputSchema); jsonSchema(outputSchema); refs(resources); refs(recipients); strings(effects, EFFECTS);
  return contractDigest({ id: capabilityId, version: capabilityVersion, inputSchema, outputSchema,
    resources: orderedRefs(resources), effects: [...effects].sort(), recipients: orderedRefs(recipients) });
}
export function validateDescriptor(input) {
  const value = snapshot(input);
  closed(value, ['schema', 'app', 'capabilities', 'skills', 'docs', 'feedback', 'reviews']);
  check(value.schema === SCHEMA, 'unsupported_schema');
  closed(value.app, ['id', 'namespace', 'title', 'visibility', 'source', 'auth']);
  id(value.app.id); namespace(value.app.namespace);
  check(typeof value.app.title === 'string' && value.app.title.trim().length > 0 && value.app.title.length <= 160, 'invalid_title');
  choice(value.app.visibility, ['private', 'public']); source(value.app.source); auth(value.app.auth);
  check(Array.isArray(value.capabilities) && value.capabilities.length <= LIMITS.capabilities, 'input_limit');
  const ids = new Set();
  for (const cap of value.capabilities) {
    closed(cap, ['id', 'version', 'digest', 'inputSchema', 'outputSchema', 'resources', 'effects', 'recipients', 'binding'], ['title', 'description']);
    namespaced(cap.id); check(cap.id.startsWith(value.app.namespace + ':'), 'capability_namespace');
    version(cap.version); digest(cap.digest); ref(cap.binding);
    check(!ids.has(cap.id), 'duplicate_capability'); ids.add(cap.id);
    for (const key of ['title', 'description']) if (cap[key] !== undefined) check(typeof cap[key] === 'string' && cap[key].length <= (key === 'title' ? 160 : 2000), 'invalid_string');
    jsonSchema(cap.inputSchema); jsonSchema(cap.outputSchema); refs(cap.resources); refs(cap.recipients); strings(cap.effects, EFFECTS);
    check(cap.resources.every(r => r.id.startsWith(value.app.namespace + ':')), 'resource_namespace');
    check(cap.digest === capabilityDigest(cap), 'capability_digest_mismatch');
  }
  refs(value.skills); refs(value.docs);
  feedback(value.feedback, value.app.visibility);
  choice(value.reviews?.mode, ['disabled', 'public-read', 'managed'], 'unsupported_review_profile');
  if (value.reviews.mode === 'disabled') closed(value.reviews, ['mode']);
  else {
    ref(value.reviews.provider);
    if (value.reviews.mode === 'public-read') { closed(value.reviews, ['mode', 'provider', 'subjects']); refs(value.reviews.subjects); }
    else {
      closed(value.reviews, ['mode', 'provider', 'placements']);
      check(Array.isArray(value.reviews.placements) && value.reviews.placements.length <= 32, 'input_limit');
      const seen = new Set();
      for (const p of value.reviews.placements) {
        closed(p, ['id', 'version', 'digest', 'subjectId', 'rights']);
        ref({ id: p.id, version: p.version, digest: p.digest }); namespaced(p.subjectId);
        strings(p.rights, ['display', 'collect', 'reply', 'moderate-origin']);
        check(!seen.has(p.id), 'duplicate_reference'); seen.add(p.id);
      }
    }
  }
  return freezeDeep(value);
}

const hosts = new WeakMap(), heads = new WeakMap(), histories = new WeakMap(), retiredHosts = new WeakSet(), states = new WeakSet(), receipts = new WeakMap();
const disposedHosts = new WeakSet(), registryIndexes = new WeakMap(), authorityDigests = new WeakMap();
const REGISTRIES = ['bindings', 'providers', 'profiles', 'skills', 'docs', 'placements', 'publicSubjects'];
function scope(value) { closed(value, ['registryId', 'tenantId', 'appId', 'environmentId']); Object.values(value).forEach(id); }
const refPart = v => ({ id: v.id, version: v.version, digest: v.digest });
const key = v => v.id + '@' + v.version;
function lookup(entries, reference, code) {
  let index = registryIndexes.get(entries);
  if (!index) { index = new Map(entries.map(entry => [key(entry), entry])); registryIndexes.set(entries, index); }
  const entry = index.get(key(reference));
  check(entry && same(refPart(entry), reference), code); return entry;
}
function registeredFeedback(config, value) {
  check(lookup(config.providers, value.provider, 'feedback_provider_missing').kind === 'feedback', 'feedback_provider_missing');
  for (const [field, kind] of [['captureProfile', 'capture'], ['retentionProfile', 'retention']]) {
    check(lookup(config.profiles, value[field], 'feedback_profile_missing').kind === kind, 'feedback_profile_missing');
  }
}
function historyPins(config) {
  const pins = REGISTRIES.flatMap(list => config[list].map(entry => [list + ':' + key(entry), entry]));
  if (config.authorProfile) pins.push(['authorProfiles:' + key(config.authorProfile), config.authorProfile]);
  return pins;
}
/** TRUSTED SERVER CODE ONLY: never derive this configuration from HTTP Host or a payload. */
export function createAdmissionHost(input) {
  const config = snapshot(input);
  closed(config, ['context', 'bindings', 'providers', 'profiles', 'skills', 'docs', 'placements', 'publicSubjects'], ['authorProfile']);
  const c = config.context;
  closed(c, ['scope', 'namespace', 'ownerId', 'authorityRevision', 'visibility', 'source', 'auth']);
  scope(c.scope); namespace(c.namespace); id(c.ownerId); version(c.authorityRevision);
  choice(c.visibility, ['private', 'public']); source(c.source); auth(c.auth);
  for (const list of REGISTRIES) {
    check(Array.isArray(config[list]) && config[list].length <= LIMITS.registryEntries, 'host_registry_limit');
    const seen = new Set();
    for (const entry of config[list]) {
      const required = list === 'bindings' ? ['scope', 'source', 'capability']
        : list === 'providers' ? ['kind', 'publicRead'] : list === 'profiles' ? ['kind']
          : list === 'placements' ? ['scope', 'provider', 'subjectId', 'rights'] : list === 'publicSubjects' ? ['provider'] : [];
      closed(entry, ['id', 'version', 'digest', ...required]); ref(refPart(entry));
      check(!seen.has(key(entry)), 'host_registry_duplicate'); seen.add(key(entry));
      if (list === 'bindings') { scope(entry.scope); source(entry.source); ref(entry.capability); }
      if (list === 'providers') { choice(entry.kind, ['feedback', 'reviews']); check(typeof entry.publicRead === 'boolean', 'host_registry_invalid'); }
      if (list === 'profiles') choice(entry.kind, ['capture', 'retention', 'auth']);
      if (list === 'placements') {
        scope(entry.scope); ref(entry.provider);
        check(lookup(config.providers, entry.provider, 'review_provider_missing').kind === 'reviews', 'review_provider_missing');
        namespaced(entry.subjectId); strings(entry.rights, ['display', 'collect', 'reply', 'moderate-origin']);
      }
      if (list === 'publicSubjects') ref(entry.provider);
    }
  }
  const contracts = new Map();
  for (const binding of config.bindings) {
    const contractKey = key(binding.capability), prior = contracts.get(contractKey);
    check(!prior || prior === binding.capability.digest, 'immutable_capability_conflict');
    contracts.set(contractKey, binding.capability.digest);
  }
  if (Object.hasOwn(config, 'authorProfile')) {
    closed(config.authorProfile, ['id', 'version', 'digest', 'feedback']);
    ref(refPart(config.authorProfile)); feedback(config.authorProfile.feedback, c.visibility);
    registeredFeedback(config, config.authorProfile.feedback);
  }
  // Pins are data: no function, executable, endpoint or package is resolved or loaded.
  const host = Object.freeze({});
  hosts.set(host, freezeDeep(config)); heads.set(host, new Map());
  authorityDigests.set(config, contractDigest(config));
  for (const list of REGISTRIES) registryIndexes.set(config[list], new Map(config[list].map(entry => [key(entry), entry])));
  histories.set(host, {
    pins: new Map(historyPins(config)),
    contracts: new Map(config.bindings.map(entry => [key(entry.capability), entry.capability.digest]))
  });
  return host;
}
/** Upgrade an in-process fixture registry without reinterpreting an existing version. */
export function upgradeAdmissionHost(previous, input) {
  const old = hostConfiguration(previous), candidate = createAdmissionHost(input), next = hostConfiguration(candidate);
  check(same(old.context.scope, next.context.scope), 'host_scope_change');
  check(next.context.authorityRevision > old.context.authorityRevision, 'authority_revision_required');
  const previousHistory = histories.get(previous), pins = new Map(previousHistory.pins), contracts = new Map(previousHistory.contracts);
  for (const [historyKey, entry] of historyPins(next)) {
    const prior = pins.get(historyKey);
    check(!prior || same(prior, entry), 'immutable_pin_conflict');
    pins.set(historyKey, entry);
  }
  for (const entry of next.bindings) {
    const contractKey = key(entry.capability), prior = contracts.get(contractKey);
    check(!prior || prior === entry.capability.digest, 'immutable_capability_conflict');
    contracts.set(contractKey, entry.capability.digest);
  }
  check(pins.size <= LIMITS.registryHistory && contracts.size <= LIMITS.registryHistory, 'host_history_limit');
  histories.set(candidate, { pins, contracts });
  retiredHosts.add(previous);
  return candidate;
}
function hostConfiguration(host) {
  check(!disposedHosts.has(host), 'host_closed');
  const config = hosts.get(host); check(config, 'host_context_required');
  check(!retiredHosts.has(host), 'authority_changed'); return config;
}
function authorityDigest(config) { return authorityDigests.get(config); }
/** Dispose this ephemeral fixture only; never erase a production idempotency ledger. */
export function closeAdmissionHost(host) {
  if (disposedHosts.has(host)) return false;
  check(hosts.has(host), 'host_context_required');
  disposedHosts.add(host); retiredHosts.add(host);
  hosts.delete(host); heads.delete(host); histories.delete(host); return true;
}
/** Simple author metadata is materialized by trusted code, never by a frontend selecting a host. */
export function materializeAuthorDraft(host, input) {
  const config = hostConfiguration(host), draft = validateAuthorDraft(input), c = config.context;
  check(config.authorProfile, 'author_profile_required');
  const descriptor = validateDescriptor({
    schema: SCHEMA, app: { id: c.scope.appId, namespace: c.namespace, title: draft.title,
      visibility: c.visibility, source: c.source, auth: c.auth },
    capabilities: [], skills: [], docs: [], feedback: config.authorProfile.feedback, reviews: { mode: 'disabled' }
  });
  planAdmission(host, descriptor, 'author.materialize-check');
  return descriptor;
}
export function planAdmission(host, descriptor, requestId) {
  const config = hostConfiguration(host), c = config.context, d = validateDescriptor(descriptor); id(requestId);
  check(d.app.id === c.scope.appId && d.app.namespace === c.namespace, 'app_scope_mismatch');
  check(same(d.app.source, c.source), 'source_authority_mismatch');
  check(d.app.visibility === c.visibility, 'visibility_authority_mismatch');
  check(same(d.app.auth, c.auth), 'auth_authority_mismatch');
  if (d.app.auth.mode !== 'public') check(lookup(config.profiles, d.app.auth.profile, 'auth_profile_missing').kind === 'auth', 'auth_profile_missing');
  for (const cap of d.capabilities) {
    const b = lookup(config.bindings, cap.binding, 'binding_missing');
    check(same(b.scope, c.scope), 'binding_scope_mismatch'); check(same(b.source, c.source), 'binding_source_mismatch');
    check(same(b.capability, { id: cap.id, version: cap.version, digest: cap.digest }), 'binding_contract_mismatch');
  }
  for (const list of ['skills', 'docs']) for (const r of d[list]) lookup(config[list], r, list + '_pin_missing');
  registeredFeedback(config, d.feedback);
  if (d.reviews.mode !== 'disabled') {
    const provider = lookup(config.providers, d.reviews.provider, 'review_provider_missing');
    check(provider.kind === 'reviews', 'review_provider_missing');
    if (d.reviews.mode === 'public-read') {
      check(provider.publicRead, 'review_not_public');
      for (const s of d.reviews.subjects) check(same(lookup(config.publicSubjects, s, 'review_not_public').provider, d.reviews.provider), 'review_not_public');
    } else for (const p of d.reviews.placements) {
      const binding = lookup(config.placements, refPart(p), 'review_binding_missing');
      check(same(binding.provider, d.reviews.provider), 'review_provider_mismatch');
      check(same(binding.scope, c.scope) && binding.subjectId === p.subjectId && p.rights.every(r => binding.rights.includes(r)), 'review_scope_mismatch');
    }
  }
  const descriptorDigest = contractDigest(d), authority = authorityDigest(config);
  return freezeDeep({ schema: 'soty.app-admission-plan.v1', prototype: true, productionAdmission: false,
    requestId, scope: c.scope, ownerId: c.ownerId, authorityRevision: c.authorityRevision,
    descriptorDigest, authorityDigest: authority,
    intentDigest: contractDigest({ requestId, descriptorDigest, authorityDigest: authority }),
    feedbackIntent: { provisioningKey: contractDigest({ scope: c.scope, providerId: d.feedback.provider.id }), ...d.feedback },
    bindings: d.capabilities.map(cap => cap.binding), reviews: d.reviews,
    gates: { ui: 'pending-feedback', agent: 'not-admitted', local: 'not-admitted' } });
}
function state(host, descriptor, plan, status = 'pending-feedback', revision = 1, receipt = null) {
  const result = freezeDeep({ schema: 'soty.app-admission-state.v1', descriptor, plan, status, revision, feedbackReceipt: receipt });
  states.add(result); heads.get(host).set(plan.requestId, result); return result;
}
export function beginAdmission(host, descriptor, requestId) {
  const d = validateDescriptor(descriptor), plan = planAdmission(host, d, requestId);
  const existing = heads.get(host).get(requestId);
  if (existing) { check(existing.plan.intentDigest === plan.intentDigest, 'admission_intent_conflict'); return existing; }
  check(heads.get(host).size < LIMITS.admissionRequests, 'admission_request_limit');
  return state(host, d, plan);
}
function current(host, previous) {
  check(states.has(previous), 'admission_state_required'); const config = hostConfiguration(host);
  check(previous.plan.authorityDigest === authorityDigest(config), 'authority_changed');
  const head = heads.get(host).get(previous.plan.requestId);
  check(head && head.plan.intentDigest === previous.plan.intentDigest, 'admission_state_required');
  return head;
}
export function replayAdmission(host, previous, descriptor, requestId) {
  const head = current(host, previous), plan = planAdmission(host, descriptor, requestId);
  check(plan.intentDigest === head.plan.intentDigest, 'admission_intent_conflict'); return head;
}
/** Caller is a trusted host adapter after separate verification; structurally branded, not crypto. */
export function createFeedbackReceipt(host, previous, input) {
  const head = current(host, previous); check(head === previous, 'admission_stale');
  const data = snapshot(input);
  closed(data, ['installationId', 'receiptDigest']); id(data.installationId); digest(data.receiptDigest);
  const receipt = freezeDeep({ ...data, provisioningKey: previous.plan.feedbackIntent.provisioningKey,
    provider: previous.plan.feedbackIntent.provider, scope: previous.plan.scope,
    intentDigest: previous.plan.intentDigest, authorityDigest: previous.plan.authorityDigest, admissionRevision: previous.revision });
  receipts.set(receipt, previous); return receipt;
}
export function confirmFeedback(host, previous, receipt) {
  const head = current(host, previous), origin = receipts.get(receipt);
  check(origin && origin.plan.intentDigest === head.plan.intentDigest, 'feedback_receipt_required');
  // A lost-ACK replay returns the single recorded receipt, including from an older projection.
  if (head.feedbackReceipt) { check(same(head.feedbackReceipt, receipt), 'feedback_receipt_conflict'); return head; }
  check(head === previous && origin === previous, 'admission_stale');
  const plan = freezeDeep({ ...previous.plan, gates: { ...previous.plan.gates, ui: 'fixture-ready' } });
  return state(host, previous.descriptor, plan, 'fixture-ready', previous.revision + 1, receipt);
}
export function holdFeedback(host, previous) {
  const head = current(host, previous); check(head.status !== 'fixture-ready', 'admission_transition_invalid');
  if (head.status === 'feedback-held') return head;
  check(head === previous, 'admission_stale');
  return state(host, previous.descriptor, previous.plan, 'feedback-held', previous.revision + 1);
}
