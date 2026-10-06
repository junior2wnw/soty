import { snapshot, canonicalContractJson, contractDigest } from '../../app-contract/json.mjs';
import { freezeDeep } from '../../capabilities/server/validation.mjs';
import { requestPath } from '../../apps/server/protocol.mjs';
import { REVIEWS_OPERATIONS, REVIEWS_LIMITS, LOCAL_REVIEW_SUBJECT_TYPES, EMPTY_REVIEWS_CONFIGURATION } from './profile.mjs';

export class ReviewsError extends Error {
  constructor(code, status = 400) { super(code); this.name = 'ReviewsError'; this.code = code; this.status = status; }
}
const requireThat = (ok, code = 'reviews_invalid_arguments', status) => { if (!ok) throw new ReviewsError(code, status); };
const closed = (value, required, optional = [], code = 'reviews_configuration_invalid', status = 500) => {
  requireThat(value && typeof value === 'object' && !Array.isArray(value)
    && required.every(key => Object.hasOwn(value, key))
    && Object.keys(value).every(key => required.includes(key) || optional.includes(key)), code, status);
};
const id = (value, code = 'reviews_configuration_invalid', status = 500) => {
  requireThat(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,95}$/u.test(value)
    && !value.includes('..') && !value.includes('//'), code, status); return value;
};
const reference = value => {
  closed(value, ['id', 'version', 'digest']);
  requireThat(typeof value.id === 'string' && /^[A-Za-z][A-Za-z0-9._-]{0,63}:[A-Za-z][A-Za-z0-9._/-]{0,95}$/u.test(value.id)
    && !value.id.includes('..') && !value.id.includes('//')
    && Number.isSafeInteger(value.version) && value.version >= 1 && value.version <= 1000000
    && typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest), 'reviews_configuration_invalid', 500);
};
const refPart = ({ id, version, digest }) => ({ id, version, digest });
const refKey = value => JSON.stringify([value.id, value.version]);
const same = (a, b) => canonicalContractJson(a) === canonicalContractJson(b);
const scopeKey = value => JSON.stringify([value.registryId, value.tenantId, value.appId, value.environmentId]);
const sync = value => {
  if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); throw new ReviewsError('reviews_async_authority', 500); }
  return value;
};
const capture = (value, code = 'reviews_configuration_invalid', status = 500) => {
  try { return snapshot(value); } catch { throw new ReviewsError(code, status); }
};
function origin(value, allowFixtureOrigins) {
  let parsed;
  try { parsed = new URL(value); } catch { throw new ReviewsError('reviews_origin_invalid', 500); }
  const fixture = allowFixtureOrigins && parsed.protocol === 'http:'
    && ['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname)
    && Number(parsed.port) >= 1024 && Number(parsed.port) <= 65535;
  requireThat(typeof value === 'string' && value.length <= 2048 && value === parsed.origin
    && !parsed.username && !parsed.password && (parsed.protocol === 'https:' || fixture), 'reviews_origin_invalid', 500);
  return value;
}
function captureActor(input) {
  requireThat(input && typeof input === 'object' && !Array.isArray(input), 'reviews_authentication_required', 401);
  const fields = Object.getOwnPropertyDescriptors(input);
  const values = {};
  for (const key of ['accountId', 'deviceId']) {
    requireThat(fields[key] && 'value' in fields[key], 'reviews_authentication_required', 401);
    values[key] = id(fields[key].value, 'reviews_authentication_required', 401);
  }
  return Object.freeze(values);
}

/** Trusted host bindings only. A public subject's existence never proves a
 * local app, project or person association. No network or identity issuer is
 * installed here; the caller supplies the current signed Connect/Apps fences. */
export function createReviewsService({ actorActive, withAppAuthority, registryId = 'soty', environmentId = 'production',
  configuration = EMPTY_REVIEWS_CONFIGURATION, allowFixtureOrigins = false } = {}) {
  requireThat(typeof actorActive === 'function' && actorActive.constructor?.name !== 'AsyncFunction'
    && typeof withAppAuthority === 'function' && withAppAuthority.constructor?.name !== 'AsyncFunction', 'reviews_host_required', 500);
  requireThat(typeof allowFixtureOrigins === 'boolean', 'reviews_configuration_invalid', 500);
  id(registryId); id(environmentId);
  const config = capture(configuration);
  closed(config, ['providers', 'bindings']);
  requireThat(Array.isArray(config.providers) && config.providers.length <= REVIEWS_LIMITS.providerPins
    && Array.isArray(config.bindings) && config.bindings.length <= REVIEWS_LIMITS.subjectBindings, 'reviews_configuration_limit', 500);
  const providers = new Map(), subjects = new Map(), bindings = new Map();
  for (const value of config.providers) {
    closed(value, ['id', 'version', 'digest', 'origin']); reference(refPart(value)); origin(value.origin, allowFixtureOrigins);
    const key = refKey(value);
    requireThat(!providers.has(key), 'reviews_provider_pin_conflict', 500);
    providers.set(key, freezeDeep(value));
  }
  for (const value of config.bindings) {
    closed(value, ['scope', 'localSubject', 'providerRef', 'subjectRef', 'providerSubjectId', 'providerEntityType', 'mode']);
    closed(value.scope, ['registryId', 'tenantId', 'appId', 'environmentId']); Object.values(value.scope).forEach(item => id(item));
    requireThat(value.scope.registryId === registryId && value.scope.environmentId === environmentId, 'reviews_scope_mismatch', 500);
    closed(value.localSubject, ['kind', 'id']); id(value.localSubject.id);
    requireThat(Object.hasOwn(LOCAL_REVIEW_SUBJECT_TYPES, value.localSubject.kind)
      && LOCAL_REVIEW_SUBJECT_TYPES[value.localSubject.kind].includes(value.providerEntityType)
      && (value.localSubject.kind !== 'app' || value.localSubject.id === value.scope.appId), 'reviews_subject_kind_mismatch', 500);
    requireThat(value.mode === 'public-read' && typeof value.providerSubjectId === 'string'
      && /^subject_[0-9a-f]{20,64}$/u.test(value.providerSubjectId), 'reviews_configuration_invalid', 500);
    reference(value.providerRef); reference(value.subjectRef);
    const provider = providers.get(refKey(value.providerRef));
    requireThat(provider && same(refPart(provider), value.providerRef), 'reviews_provider_pin_mismatch', 500);
    const subject = { ...value.subjectRef, provider: value.providerRef }, key = refKey(subject);
    const target = { providerRef: value.providerRef, providerSubjectId: value.providerSubjectId, providerEntityType: value.providerEntityType };
    const prior = subjects.get(key);
    requireThat(!prior || (same(prior.subject, subject) && same(prior.target, target)), 'reviews_subject_pin_conflict', 500);
    if (!prior) subjects.set(key, freezeDeep({ subject, target }));
    const scope = scopeKey(value.scope), list = bindings.get(scope) || [];
    requireThat(list.length < REVIEWS_LIMITS.subjectsPerApp
      && !list.some(item => item.localSubject.kind === value.localSubject.kind), 'reviews_binding_conflict', 500);
    list.push(freezeDeep({ ...value, origin: provider.origin })); bindings.set(scope, list);
  }
  const approved = freezeDeep({
    providers: [...providers.values()].map(value => ({ ...refPart(value), kind: 'reviews', publicRead: true })),
    publicSubjects: [...subjects.values()].map(value => value.subject),
  });
  const origins = Object.freeze([...new Set([...providers.values()].map(value => value.origin))].sort());
  let disposed = false;
  const authenticate = actor => {
    requireThat(!disposed, 'reviews_closed', 503);
    requireThat(sync(actorActive(actor)) === true, 'reviews_authentication_required', 401);
  };
  return Object.freeze({
    operations: new Set(REVIEWS_OPERATIONS),
    origins() { requireThat(!disposed, 'reviews_closed', 503); return origins; },
    approvedReferences() { requireThat(!disposed, 'reviews_closed', 503); return approved; },
    /** Private synchronous host resolver. A binding's operational digest also
     * fences origin/association changes without redefining semantic ref pins. */
    approvedReferencesFor(inputScope) {
      requireThat(!disposed, 'reviews_closed', 503);
      const scope = capture(inputScope); closed(scope, ['registryId', 'tenantId', 'appId', 'environmentId']);
      Object.values(scope).forEach(value => id(value));
      requireThat(scope.registryId === registryId && scope.environmentId === environmentId, 'reviews_scope_mismatch', 500);
      const order = (a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : a.version - b.version;
      const selected = [...(bindings.get(scopeKey(scope)) || [])].sort((a, b) => order(a.subjectRef, b.subjectRef));
      const selectedProviders = [...new Map(selected.map(value => [refKey(value.providerRef), providers.get(refKey(value.providerRef))])).values()].sort(order);
      const selectedSubjects = [...new Map(selected.map(value => [refKey(value.subjectRef), subjects.get(refKey(value.subjectRef)).subject])).values()].sort(order);
      return freezeDeep({
        references: { providers: selectedProviders.map(value => ({ ...refPart(value), kind: 'reviews', publicRead: true })), publicSubjects: selectedSubjects },
        bindingDigest: contractDigest({ scope, providers: selectedProviders, bindings: selected }),
      });
    },
    execute({ op, actor: inputActor, args = {} }) {
      requireThat(op === 'apps.reviews.context', 'unsupported_operation');
      const actor = captureActor(inputActor); authenticate(actor);
      const input = capture(args, 'reviews_invalid_arguments', 400);
      closed(input, ['appId'], ['domainId', 'path'], 'reviews_invalid_arguments', 400);
      id(input.appId, 'reviews_invalid_arguments', 400);
      if (input.domainId !== undefined) requireThat(typeof input.domainId === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(input.domainId), 'reviews_invalid_arguments');
      if (input.path !== undefined) {
        try { requestPath(input.path); } catch { throw new ReviewsError('reviews_invalid_arguments'); }
      }
      let active = true, entered = false, outcome;
      try {
        const returned = withAppAuthority({ actor, appId: input.appId, mode: 'participant',
          ...(input.domainId === undefined ? {} : { domainId: input.domainId }), ...(input.path === undefined ? {} : { path: input.path }) }, context => {
          requireThat(active && !entered, 'reviews_authority_fence_invalid', 500); entered = true; authenticate(actor);
          requireThat(context && context.appId === input.appId && context.accountId === actor.accountId
            && typeof context.canManage === 'boolean' && context.canManage === (context.ownerId === actor.accountId), 'reviews_authority_context_invalid', 500);
          id(context.ownerId, 'reviews_authority_context_invalid', 500);
          const values = bindings.get(scopeKey({ registryId, tenantId: context.ownerId, appId: input.appId, environmentId })) || [];
          const entries = [...values].sort((a, b) => ['app', 'project', 'person'].indexOf(a.localSubject.kind) - ['app', 'project', 'person'].indexOf(b.localSubject.kind));
          outcome = freezeDeep(entries.length ? { mode: 'public-read', availability: 'unprobed', subjects: entries.map(value => {
            const base = `${value.origin}/api/public/v1/subjects/${value.providerSubjectId}`;
            return { subjectKind: value.localSubject.kind, providerRef: value.providerRef, subjectRef: value.subjectRef,
              providerSubjectId: value.providerSubjectId, providerEntityType: value.providerEntityType,
              api: { subject: base, rating: `${base}/rating`, reviews: `${base}/reviews` },
              publicPageUrl: `${value.origin}/subjects/${value.providerSubjectId}` };
          }) } : { mode: 'disabled', subjects: [] });
          authenticate(actor); return outcome;
        });
        sync(returned); requireThat(entered, 'reviews_authority_fence_invalid', 500); authenticate(actor); return outcome;
      } finally { active = false; }
    },
    close() { disposed = true; providers.clear(); subjects.clear(); bindings.clear(); },
  });
}
