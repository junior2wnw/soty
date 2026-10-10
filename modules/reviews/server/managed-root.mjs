import { snapshot, canonicalContractJson } from '../../app-contract/json.mjs';
import { freezeDeep } from '../../capabilities/server/validation.mjs';
import { createReviewsService } from './index.mjs';
import { EMPTY_REVIEWS_CONFIGURATION } from './profile.mjs';
import { createManagedReviewsPort, ManagedReviewsPortError } from './managed-port.mjs';

const prefix = 'apps.reviews.managed.';
const methods = ['context', 'list', 'collect'];
const need = (ok, code = 'managed_reviews_invalid', status = 400) => { if (!ok) throw new ManagedReviewsPortError(code, status); };
const same = (a, b) => canonicalContractJson(a) === canonicalContractJson(b);
const closed = (value, keys, optional = []) => need(value && typeof value === 'object' && !Array.isArray(value)
  && keys.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => keys.includes(key) || optional.includes(key)));
const capture = value => { try { return freezeDeep(snapshot(value)); } catch { throw new ManagedReviewsPortError('managed_reviews_invalid'); } };

/** Concrete signed Connect extension. The selector names a reviewed binding;
 * it never supplies Native identity, local resource IDs or Source rights. */
export function createManagedRootReviews({ configuration = EMPTY_REVIEWS_CONFIGURATION, registryId = 'soty', environmentId = 'production',
  withRootActorAuthority, withAppAuthority, managedSource, allowFixtureOrigins = false } = {}) {
  need(typeof withRootActorAuthority === 'function' && typeof withAppAuthority === 'function', 'managed_reviews_host_required', 500);
  const config = capture(configuration);
  // Reuse the public profile's complete pin/subject validation, without IO.
  const validator = createReviewsService({ configuration: config, registryId, environmentId, actorActive: () => false, withAppAuthority, allowFixtureOrigins });
  validator.close();
  let disposed = false, port;
  const approved = (scope, localSubject, providerRef) => config.bindings.find(binding => same(binding.scope, scope)
    && same(binding.localSubject, localSubject) && same(binding.providerRef, providerRef));
  const permits = ({ scope, localSubject, context }) => {
    const binding = approved(scope, localSubject, managedSource.providerRef);
    return Boolean(binding && (localSubject.kind !== 'app' || localSubject.id === context.appId)
      && (localSubject.kind !== 'person' || localSubject.id === context.ownerId));
  };
  function nativeContext(raw, input) {
    const value = capture(raw), binding = approved(input.scope, input.localSubject, managedSource.providerRef);
    need(binding && value.protocol === 'povedai.managed-reviews.v1' && same(value.localSubject, input.localSubject)
      && value.subjectId === binding.providerSubjectId && value.entityType === binding.providerEntityType && value.generation === 1,
      'managed_reviews_source_binding_changed', 502);
    closed(value, ['protocol', 'localSubject', 'entityType', 'bindingId', 'siteId', 'siteKey', 'objectId', 'objectSlug', 'subjectId', 'placementId', 'generation', 'canDisplay']);
    for (const key of ['bindingId', 'siteId', 'siteKey', 'objectId', 'objectSlug', 'subjectId', 'placementId']) {
      need(typeof value[key] === 'string' && value[key].length > 0 && value[key].length <= 200 && !/[\u0000-\u001f\u007f]/u.test(value[key]), 'managed_reviews_source_binding_changed', 502);
    }
    need(typeof value.canDisplay === 'boolean', 'managed_reviews_source_binding_changed', 502);
    return value;
  }
  if (managedSource !== undefined) {
    need(managedSource && typeof managedSource === 'object', 'managed_reviews_host_required', 500);
    const fields = Object.getOwnPropertyDescriptors(managedSource);
    need(Object.getOwnPropertySymbols(managedSource).length === 0 && Object.keys(fields).every(key => ['providerRef', 'source', 'resolveSourceCredential', 'timeoutMs'].includes(key))
      && ['providerRef', 'source', 'resolveSourceCredential'].every(key => fields[key] && Object.hasOwn(fields[key], 'value'))
      && Object.values(fields).every(field => Object.hasOwn(field, 'value')), 'managed_reviews_host_required', 500);
    const providerRef = capture(fields.providerRef.value), source = fields.source.value, resolveSourceCredential = fields.resolveSourceCredential.value;
    need(config.providers.some(provider => same({ id: provider.id, version: provider.version, digest: provider.digest }, providerRef)), 'managed_reviews_provider_invalid', 500);
    need(source && typeof resolveSourceCredential === 'function', 'managed_reviews_host_required', 500);
    const native = Object.fromEntries(['withActor', 'provision', 'context', 'list', 'collect', 'publish', 'moderate'].map(name => {
      const method = source[name]; need(typeof method === 'function', 'managed_reviews_host_required', 500); return [name, method.bind(source)];
    }));
    // Capture these trusted host ports once, not mutable caller configuration.
    managedSource = Object.freeze({ providerRef });
    const selected = { ...native,
      async context(actor, input) { return nativeContext(await native.context(actor, input), input); },
      async collect(actor, input) {
        // Native context is freshly checked for this exact immutable binding.
        // Native collect still owns its grant+binding+receipt transaction; the
        // Root cannot undo a Source commit if its later fence rejects the ACK.
        nativeContext(await native.context(actor, { scope: input.scope, localSubject: input.localSubject }), input);
        return native.collect(actor, input);
      },
    };
    port = createManagedReviewsPort({ registryId, environmentId, providerRef, source: selected,
      resolveSourceCredential, withRootActorAuthority, withAppAuthority, validateLocalSubject: permits,
      ...(fields.timeoutMs ? { timeoutMs: fields.timeoutMs.value } : {}) });
  }
  function unavailable(actor, binding) {
    let rootOpen = true, appOpen = false, rootEntered = false, appEntered = false, poisoned = false;
    const synchronous = value => { if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); poisoned = true; throw new ManagedReviewsPortError('managed_reviews_async_authority', 500); } };
    try {
      const result = withRootActorAuthority(actor, rawOwner => {
        if (!rootOpen || rootEntered) { poisoned = true; throw new ManagedReviewsPortError('managed_reviews_authority_invalid', 500); } rootEntered = true;
        const owner = capture(rawOwner); closed(owner, ['accountId', 'deviceId']); appOpen = true;
        try {
          const value = withAppAuthority({ actor: owner, appId: binding.scope.appId, mode: 'participant' }, rawContext => {
            if (!rootOpen || !appOpen || appEntered) { poisoned = true; throw new ManagedReviewsPortError('managed_reviews_authority_invalid', 500); } appEntered = true;
            const context = capture(rawContext);
            need(context.appId === binding.scope.appId && context.accountId === owner.accountId && context.ownerId === binding.scope.tenantId
              && (binding.localSubject.kind !== 'person' || binding.localSubject.id === context.ownerId), 'managed_reviews_subject_scope_invalid', 403);
          }); synchronous(value);
        } finally { appOpen = false; }
      }); synchronous(result); need(rootEntered && appEntered && !poisoned && !disposed, 'managed_reviews_authority_invalid', 500);
      return Object.freeze({ available: false, code: 'managed_reviews_unavailable' });
    } finally { rootOpen = false; appOpen = false; }
  }
  return Object.freeze({
    operations: new Set(methods.map(method => prefix + method)),
    async executeAsync({ op, actor, args }) {
      need(!disposed, 'managed_reviews_closed', 503);
      const method = methods.find(value => prefix + value === op); need(method, 'managed_reviews_operation_unknown');
      const input = capture(args);
      closed(input, ['appId', 'subjectKind', 'providerRef', 'subjectRef', 'providerSubjectId'],
        method === 'collect' ? ['requestId', 'body', 'rating', 'authorLabel', 'parentId'] : method === 'list' ? ['limit', 'beforeId'] : []);
      const matches = config.bindings.filter(binding => binding.scope.appId === input.appId && binding.localSubject.kind === input.subjectKind
        && same(binding.providerRef, input.providerRef) && same(binding.subjectRef, input.subjectRef) && binding.providerSubjectId === input.providerSubjectId);
      need(matches.length === 1, 'managed_reviews_binding_not_found', 404); const binding = matches[0];
      if (!port) { need(method === 'context', 'managed_reviews_unavailable', 503); return unavailable(actor, binding); }
      need(same(binding.providerRef, managedSource.providerRef), 'managed_reviews_provider_invalid', 403);
      const selected = { appId: input.appId, localSubject: binding.localSubject };
      for (const key of ['requestId', 'body', 'rating', 'authorLabel', 'parentId', 'limit', 'beforeId']) if (Object.hasOwn(input, key)) selected[key] = input[key];
      const result = await port.execute({ op: method, actor, args: selected });
      need(!disposed, 'managed_reviews_closed', 503);
      if (method === 'context') return freezeDeep({ available: true, subjectKind: binding.localSubject.kind,
        providerSubjectId: binding.providerSubjectId, providerRef: binding.providerRef, subjectRef: binding.subjectRef });
      if (method === 'collect') return Object.freeze({ status: 'pending', reviewId: result.reviewId, requestId: input.requestId });
      return result;
    },
    close() { disposed = true; port?.close(); },
  });
}
