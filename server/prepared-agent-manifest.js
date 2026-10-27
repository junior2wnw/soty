import { createCatalog } from '../modules/capabilities/server/catalog.mjs';
import { createTrustedReadonlyQueryAdapter } from '../modules/capabilities/server/readonly-queries.mjs';
import { AccessError, canonicalHash, freezeDeep } from '../modules/capabilities/server/validation.mjs';

const need = (condition, code = 'external_resource_denied') => { if (!condition) throw new AccessError(code); };
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const text = (maxLength, minLength = 1) => ({ type: 'string', minLength, maxLength });
const locationSchema = object({ rootId: text(64, 64), segments: { type: 'array', maxItems: 32, items: text(255) } });
const targetSchema = { hostDeviceId: text(180), connectorId: text(180), location: locationSchema };
const digestSchema = text(64, 64);
const binding = { id: 'soty.prepared-manifest.read', version: 1, digest: canonicalHash({ schema: 'soty.prepared-manifest-read.v1' }) };
export const PREPARED_MANIFEST_CATALOG = freezeDeep([{
  capabilityId: 'apps.prepared.manifest.read', version: 1, appId: 'soty.apps', title: 'Прочитать описание выбранного проекта',
  description: 'Read the selected local manifest only. Does not call the application, start it or register it in Soty.',
  visibility: 'private', executionEnabled: true,
  inputSchema: object({ ...targetSchema, manifestDigest: digestSchema }),
  outputSchema: object({ schema: { type: 'string', maxLength: 40, enum: ['soty.prepared-manifest.v1'] },
    state: { type: 'string', maxLength: 7, enum: ['present'] }, name: text(64), digest: digestSchema }),
  resources: ['apps.prepared.manifest'], effects: [], recipients: ['soty:prepared-manifest'],
  executionBinding: { kind: 'registered', handler: 'apps.prepared.manifest.read', version: 1, binding },
}]);
const registry = createCatalog(PREPARED_MANIFEST_CATALOG), entry = registry.get('apps.prepared.manifest.read', 1);
const reference = Object.freeze({ capabilityId: entry.capabilityId, version: entry.version, digest: entry.digest });
const segment = value => typeof value === 'string' && value.length > 0 && value.length <= 255 && value.isWellFormed()
  && value !== '.' && value !== '..' && !/[\\/\u0000-\u001f\u007f]/u.test(value);
function target(value, includeDigest) {
  need(value && typeof value === 'object' && !Array.isArray(value), 'invalid_input');
  const keys = includeDigest ? ['hostDeviceId', 'connectorId', 'location', 'manifestDigest'] : ['hostDeviceId', 'connectorId', 'location'];
  need(Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key)), 'invalid_input');
  need([value.hostDeviceId, value.connectorId].every(id => typeof id === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(id)), 'invalid_input');
  const location = value.location;
  need(location && typeof location === 'object' && !Array.isArray(location) && Object.keys(location).length === 2
    && Object.hasOwn(location, 'rootId') && Object.hasOwn(location, 'segments') && /^[a-f0-9]{64}$/u.test(location.rootId || '')
    && Array.isArray(location.segments) && location.segments.length <= 32 && location.segments.every(segment)
    && Buffer.byteLength(JSON.stringify(location), 'utf8') <= 4096, 'invalid_input');
  if (includeDigest) need(/^[a-f0-9]{64}$/u.test(value.manifestDigest || ''), 'invalid_input');
  return value;
}
function scope(identity, selection, manifestDigest) {
  return 'apps.prepared.project:' + canonicalHash({ linkId: identity.linkId, hostDeviceId: selection.hostDeviceId,
    connectorId: selection.connectorId, location: selection.location, manifestDigest });
}

/** Root-host composition: this does not turn an agent into a signed person. */
export function createPreparedAgentManifest({ capabilities, connect, apps }) {
  need([capabilities, connect, apps].every(port => typeof port === 'function'), 'host_auth_required');
  const ownerCall = (request, options, callback) => request.actor
    ? capabilities().withOwnerAuthority({ actor: request.actor, ...options }, callback)
    : capabilities().withSnapshotOwnerAuthority({ authorization: request.authorization, ...options }, callback);
  function owner(request, selection, callback) {
    return connect().withAuthorityFence(() => ownerCall(request, {}, actor => {
      if (!selection) return callback(actor);
      let identity;
      try { identity = apps().resolveOwnedDevice(actor, selection.hostDeviceId, selection.connectorId); }
      catch { throw new AccessError('external_resource_denied'); }
      const requiredResources = [scope(identity, selection, selection.manifestDigest)];
      return ownerCall(request, { requiredResources }, current => {
        need(current.accountId === actor.accountId && current.deviceId === actor.deviceId);
        const fresh = apps().resolveOwnedDevice(current, selection.hostDeviceId, selection.connectorId);
        need(fresh.linkId === identity.linkId);
        return callback(current, identity);
      });
    }));
  }
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(request, callback) { return owner(request, undefined, callback); },
    async query(request, signal, current) {
      const selection = target(request.input, true);
      need(current() === true && !signal.aborted, 'external_query_unconfirmed');
      const first = owner({ authorization: request.authorization }, selection, (actor, identity) => ({ actor, linkId: identity.linkId }));
      const { manifestDigest: _digest, ...selectedTarget } = selection;
      const response = await apps().readPreparedManifest(first.actor, selectedTarget);
      need(current() === true && !signal.aborted, 'external_query_unconfirmed');
      owner({ authorization: request.authorization }, selection, (_actor, identity) => {
        need(identity.linkId === first.linkId); response.assertCurrent();
      });
      need(response.result.state === 'present' && response.result.digest === selection.manifestDigest, 'external_resource_denied');
      return response.result;
    },
  });
  function beforeCall({ actor, name, args }) {
    if (name !== 'apps_query' || args?.reference?.capabilityId !== entry.capabilityId) return;
    need(args.reference.version === reference.version && args.reference.digest === reference.digest);
    const selection = target(args.input, true);
    owner({ actor }, selection, () => capabilities().authorize({ actor, capabilityId: entry.capabilityId,
      version: entry.version, input: selection, action: 'invoke' }));
  }
  const extension = Object.freeze({ operations: new Set(['apps.prepared.agentScope']), async executeAsync({ op, args, actor }) {
    need(op === 'apps.prepared.agentScope' && connect().isActorActive(actor), 'authentication_required');
    need(args && typeof args === 'object' && args.expectedAccountId === actor.accountId, 'invalid_input');
    const { expectedAccountId: _account, ...selection } = args; target(selection, false);
    const identity = apps().resolveOwnedDevice(actor, selection.hostDeviceId, selection.connectorId);
    const inspected = await apps().readPreparedManifest(actor, selection);
    return connect().withCurrentContacts(actor, () => {
      need(connect().isActorActive(actor), 'authentication_required');
      const current = apps().resolveOwnedDevice(actor, selection.hostDeviceId, selection.connectorId);
      need(current.linkId === identity.linkId); inspected.assertCurrent();
      need(inspected.result.state === 'present', 'external_resource_denied');
      return { resource: scope(identity, selection, inspected.result.digest), project: inspected.result };
    });
  } });
  return Object.freeze({ catalog: PREPARED_MANIFEST_CATALOG, queries: [{ contract: reference, adapter }], extension, beforeCall,
    recheck(request) { beforeCall(request); } });
}
