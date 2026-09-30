import { AppsError, assertApps, appId, cleanGrants, connectorKey, runtimePath, textId } from './protocol.mjs';
import { normalizeNamedAppZone } from './domain-policy.mjs';

const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const isAsync = value => value && typeof value.then === 'function';
const nonnegative = value => Number.isSafeInteger(value) && value >= 0;
const positive = value => Number.isSafeInteger(value) && value >= 1;
const unknownObservation = () => ({ state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' });

function origin(value) {
  if (typeof value !== 'string' || value.length > 512 || /[\\\s]/u.test(value)) return null;
  try {
    const parsed = new URL(value);
    return ['http:', 'https:'].includes(parsed.protocol) && !parsed.username && !parsed.password
      && (value === parsed.origin || value === parsed.origin + '/') ? parsed.origin : null;
  } catch { return null; }
}

function observationView(value) {
  if (value === undefined || value === null) return unknownObservation();
  const valid = isObject(value) && !isAsync(value)
    && Object.keys(value).every(key => ['state', 'observedAt', 'freshUntil', 'evidence'].includes(key));
  assertApps(valid, 'apps_observation_invalid', 500);
  const { state, observedAt, freshUntil, evidence } = value;
  const absent = state === 'unknown' && evidence === 'not-observed';
  const offline = state === 'offline' && evidence === 'connector-offline';
  const observed = ['unknown', 'responding', 'unreachable'].includes(state)
    && ['connector-v1-observation', 'connector-v2-observation'].includes(evidence);
  assertApps(((absent || offline) && observedAt === null && freshUntil === null)
    || (observed && nonnegative(observedAt) && positive(freshUntil) && freshUntil > observedAt), 'apps_observation_invalid', 500);
  // Freshness and channel evidence are the synchronous observer's responsibility.
  // No identity, runtime-ready flag or extra provider data passes this boundary.
  return { state, observedAt, freshUntil, evidence };
}

function deviceView(row, target, ownerAccountId) {
  assertApps(row && row.owner_account_id === ownerAccountId && row.connector_key === target.connector_key, 'apps_registry_corrupt', 500);
  try {
    const stored = JSON.parse(row.identity_json);
    assertApps(isObject(stored), 'apps_registry_corrupt', 500);
    const identity = Object.freeze({ linkId: textId(stored.linkId), hostDeviceId: textId(stored.hostDeviceId), connectorId: textId(stored.connectorId) });
    assertApps(connectorKey(identity) === row.connector_key && typeof row.name === 'string', 'apps_registry_corrupt', 500);
    return Object.freeze({ connectorKey: row.connector_key, ownerAccountId, name: row.name, identity });
  } catch { throw new AppsError('apps_registry_corrupt', 500); }
}

// Caller supplies registries on this SAME DatabaseSync and a synchronous,
// content-free observation of the supplied immutable target. No transport call
// or await is allowed while this read snapshot is open.
export function createAppInspection({ db, assertActor, domains, publications, inspectSource, shellOrigin,
  nameClaimsEnabled = false, namedAppZone = '', now = Date.now } = {}) {
  assertApps(db && typeof db.prepare === 'function' && typeof db.exec === 'function'
    && typeof assertActor === 'function' && typeof domains?.execute === 'function'
    && typeof publications?.execute === 'function' && typeof inspectSource === 'function', 'apps_inspection_dependencies_required', 500);
  const trustedOrigin = origin(shellOrigin);
  assertApps(trustedOrigin, 'apps_inspection_shell_origin_invalid', 500);
  assertApps(typeof nameClaimsEnabled === 'boolean', 'apps_inspection_configuration_invalid', 500);
  assertApps(typeof now === 'function', 'apps_inspection_clock_invalid', 500);
  const namedOrigin = normalizeNamedAppZone(namedAppZone);
  const claimOrigin = nameClaimsEnabled ? namedOrigin || null : null;
  function authenticate(actor) {
    const result = assertActor(actor);
    assertApps(!isAsync(result), 'apps_inspection_actor_validator_invalid', 500);
    assertApps(result !== false, 'apps_authentication_required', 401);
  }
  function shellLink(id, domainId, path) {
    const url = new URL(trustedOrigin);
    url.hash = `launch/${id}/${domainId}?${new URLSearchParams({ path })}`;
    return url.href;
  }
  function observe(context) {
    let value;
    try { value = inspectSource(context); }
    catch { throw new AppsError('apps_observation_unavailable', 503); }
    return observationView(value);
  }
  return {
    read(actor, args = {}) {
      authenticate(actor);
      assertApps(isObject(args) && Object.keys(args).every(key => key === 'appId'), 'unexpected_argument');
      const id = appId(args.appId);
      assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
      db.exec('BEGIN');
      try {
        // The first SELECT establishes a WAL snapshot before any subordinate read.
        const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
        assertApps(app && app.owner_account_id === actor.accountId, 'apps_owner_required', 403);
        assertApps(['enabled', 'revoked'].includes(app.state) && positive(app.revision), 'apps_registry_corrupt', 500);
        let grants;
        try { grants = cleanGrants(JSON.parse(app.grants_json)); }
        catch { throw new AppsError('apps_registry_corrupt', 500); }
        const addresses = domains.execute({ actor, op: 'apps.domains.get', args: { appId: id } });
        const policy = publications.execute({ actor, op: 'apps.publication.get', args: { appId: id } });
        assertApps(!isAsync(addresses) && !isAsync(policy), 'apps_inspection_dependencies_invalid', 500);
        assertApps(policy?.appId === id && policy.appState === app.state && positive(policy.policyEpoch)
          && positive(policy.activeTargetRevision) && ['restricted', 'anyone'].includes(policy.launchPolicy)
          && typeof policy.listed === 'boolean' && Array.isArray(policy.activeDomainIds)
          && nonnegative(addresses?.revision) && Array.isArray(addresses.domains), 'apps_registry_corrupt', 500);
        const limits = addresses.limits;
        assertApps(limits && positive(limits.perApp) && positive(limits.perAccount)
          && nonnegative(limits.usedByApp) && nonnegative(limits.usedByAccount), 'apps_registry_corrupt', 500);
        const target = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(id, policy.activeTargetRevision);
        assertApps(target && target.owner_account_id === app.owner_account_id && policy.target
          && ['revision', 'digest', 'profile', 'port'].every(key => target[key] === policy.target[key])
          && target.entry_path === policy.target.entryPath, 'apps_registry_corrupt', 500);
        const device = deviceView(db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(target.connector_key), target, app.owner_account_id);
        const sourceContext = Object.freeze({
          app: Object.freeze({ id, ownerAccountId: app.owner_account_id, state: app.state, revision: app.revision }),
          target: Object.freeze({ appId: id, revision: target.revision, digest: target.digest, profile: target.profile,
            connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path }),
          device,
        });
        const observation = app.state === 'revoked' ? unknownObservation() : observe(sourceContext);
        let checkedAt;
        try { checkedAt = now(); } catch { throw new AppsError('apps_inspection_clock_invalid', 500); }
        assertApps(nonnegative(checkedAt), 'apps_inspection_clock_invalid', 500);
        const active = new Set(policy.activeDomainIds);
        assertApps(active.size === policy.activeDomainIds.length && [...active].every(domainId => addresses.domains.some(item =>
          item.id === domainId && item.role === 'alias' && item.state === 'bound')), 'apps_registry_corrupt', 500);
        let entryPath = null;
        try { entryPath = runtimePath(target.entry_path); }
        catch (error) {
          // Old registrations can predate the strict navigation guard. Preserve
          // the owner's ability to inspect and restrict them without making links.
          if (!(error instanceof AppsError) && !(error instanceof URIError)) throw error;
        }
        const enabled = app.state === 'enabled', canLink = enabled && entryPath !== null;
        const domainIds = new Set();
        for (const item of addresses.domains) {
          assertApps(/^dom_[a-f0-9]{32}$/u.test(item.id) && !domainIds.has(item.id)
            && origin(item.origin) === item.origin && ['canonical', 'alias'].includes(item.role)
            && ['bound', 'tombstone'].includes(item.state), 'apps_registry_corrupt', 500);
          domainIds.add(item.id);
        }
        const canonicalDomains = addresses.domains.filter(item => item.role === 'canonical');
        assertApps(canonicalDomains.length <= 1 && canonicalDomains.every(item => item.state === 'bound'), 'apps_registry_corrupt', 500);
        const canonical = canonicalDomains[0];
        const aliases = addresses.domains.filter(item => item.role === 'alias').map(item => {
          const isActive = active.has(item.id) && item.state === 'bound';
          return { id: item.id, slug: item.slug, origin: item.origin, state: item.state, active: isActive,
            shareUrl: canLink && isActive ? (policy.launchPolicy === 'anyone'
              ? item.origin + entryPath : shellLink(id, item.id, entryPath)) : null,
            createdAt: item.createdAt, retiredAt: item.retiredAt };
        });
        // A trusted synchronous observer must not change the actor being read.
        authenticate(actor);
        assertApps(actor.accountId === app.owner_account_id, 'apps_owner_required', 403);
        const result = {
          schema: 'soty.app-inspection.v1', checkedAt,
          app: { id, name: app.name, state: app.state, revision: app.revision, grants },
          addresses: { revision: addresses.revision, claimOrigin,
            canonical: canonical ? { id: canonical.id, origin: canonical.origin,
              shareUrl: canLink ? shellLink(id, canonical.id, entryPath) : null } : null,
            aliases, limits: { perApp: limits.perApp, perAccount: limits.perAccount,
              usedByApp: limits.usedByApp, usedByAccount: limits.usedByAccount } },
          publication: { policyEpoch: policy.policyEpoch, launchPolicy: policy.launchPolicy, listed: policy.listed,
            activeDomainIds: [...policy.activeDomainIds], activeTargetRevision: policy.activeTargetRevision },
          source: { hostDeviceId: device.identity.hostDeviceId, connectorId: device.identity.connectorId, deviceName: device.name,
            port: target.port, entryPath: target.entry_path, revision: target.revision, digest: target.digest, profile: target.profile, observation },
          actions: { canReserveName: enabled && Boolean(claimOrigin) && limits.usedByApp < limits.perApp && limits.usedByAccount < limits.perAccount,
            canEdit: enabled, canPublish: enabled, canPreview: canLink && Boolean(canonical || aliases.some(item => item.active)) },
        };
        db.exec('COMMIT');
        return result;
      } catch (error) {
        if (db.isTransaction) db.exec('ROLLBACK');
        throw error;
      }
    },
  };
}
