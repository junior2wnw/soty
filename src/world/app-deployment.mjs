import { validateAppLaunchPath } from './app-launch.mjs';

export class AppDeploymentError extends Error {
  constructor(code) { super(code); this.code = code; }
}
const requireValue = (condition, code = 'invalid_app_deployment') => { if (!condition) throw new AppDeploymentError(code); };
function exact(value, keys) {
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === [...keys].sort().join(','));
}
export function deploymentOrigin(value) {
  let url; try { url = new URL(value); } catch { throw new AppDeploymentError('invalid_deployment_origin'); }
  requireValue(typeof value === 'string' && value.length <= 512 && url.protocol === 'https:'
    && !url.username && !url.password && url.origin === value, 'invalid_deployment_origin');
  return value;
}
function address(value) {
  exact(value, ['id', 'origin']);
  requireValue(/^dom_[a-f0-9]{32}$/u.test(value.id));
  return { id: value.id, origin: deploymentOrigin(value.origin) };
}
/** Pick an enabled named address without changing an already admitted frame. */
export function preferredInspectionEntry(snapshot) {
  if (snapshot.app.state !== 'enabled') return undefined;
  const alias = snapshot.addresses.aliases.find(value => value.state === 'bound' && value.active);
  const selected = alias ?? snapshot.addresses.canonical;
  if (!selected) return undefined;
  try { return { domainId: selected.id, origin: selected.origin, path: validateAppLaunchPath(snapshot.source.entryPath) }; }
  catch { return undefined; }
}
/** A whitelisted operator handoff. Never include grants, identities or credentials. */
export function exportAppDeployment(snapshot, shellOrigin) {
  requireValue(snapshot.schema === 'soty.app-inspection.v1');
  const value = {
    schema: 'soty.app-deployment.v1', checkedAt: snapshot.checkedAt, shellOrigin,
    app: { id: snapshot.app.id, name: snapshot.app.name, state: snapshot.app.state },
    addresses: { revision: snapshot.addresses.revision,
      canonical: snapshot.addresses.canonical ? { id: snapshot.addresses.canonical.id, origin: snapshot.addresses.canonical.origin } : null,
      aliases: snapshot.addresses.aliases.map(item => ({ id: item.id, origin: item.origin, state: item.state, active: item.active })) },
    publication: { policyEpoch: snapshot.publication.policyEpoch, launchPolicy: snapshot.publication.launchPolicy,
      listed: snapshot.publication.listed, activeTargetRevision: snapshot.publication.activeTargetRevision },
    source: { port: snapshot.source.port, entryPath: snapshot.source.entryPath, revision: snapshot.source.revision,
      digest: snapshot.source.digest, profile: snapshot.source.profile },
  };
  return validateAppDeployment(value);
}
export function validateAppDeployment(value) {
  exact(value, ['schema', 'checkedAt', 'shellOrigin', 'app', 'addresses', 'publication', 'source']);
  requireValue(value.schema === 'soty.app-deployment.v1' && Number.isSafeInteger(value.checkedAt) && value.checkedAt > 0);
  deploymentOrigin(value.shellOrigin);
  exact(value.app, ['id', 'name', 'state']);
  requireValue(/^app-[a-f0-9]{32}$/u.test(value.app.id) && ['enabled', 'revoked'].includes(value.app.state)
    && typeof value.app.name === 'string' && value.app.name.trim().length > 0 && value.app.name.length <= 64
    && !/[\u0000-\u001f\u007f]/u.test(value.app.name));
  exact(value.addresses, ['revision', 'canonical', 'aliases']);
  requireValue(Number.isSafeInteger(value.addresses.revision) && value.addresses.revision >= 0
    && Array.isArray(value.addresses.aliases) && value.addresses.aliases.length <= 64);
  if (value.addresses.canonical) address(value.addresses.canonical);
  const seen = new Set(value.addresses.canonical ? [value.addresses.canonical.id] : []);
  for (const alias of value.addresses.aliases) {
    exact(alias, ['id', 'origin', 'state', 'active']); address({ id: alias.id, origin: alias.origin });
    requireValue(!seen.has(alias.id) && ['bound', 'tombstone'].includes(alias.state) && typeof alias.active === 'boolean'
      && (!alias.active || alias.state === 'bound'));
    seen.add(alias.id);
  }
  exact(value.publication, ['policyEpoch', 'launchPolicy', 'listed', 'activeTargetRevision']);
  requireValue(['restricted', 'anyone'].includes(value.publication.launchPolicy) && typeof value.publication.listed === 'boolean'
    && Number.isSafeInteger(value.publication.policyEpoch) && value.publication.policyEpoch >= 0
    && Number.isSafeInteger(value.publication.activeTargetRevision) && value.publication.activeTargetRevision >= 1);
  exact(value.source, ['port', 'entryPath', 'revision', 'digest', 'profile']);
  requireValue(Number.isInteger(value.source.port) && value.source.port >= 1024 && value.source.port <= 65535
    && Number.isSafeInteger(value.source.revision) && value.source.revision === value.publication.activeTargetRevision
    && /^[a-f0-9]{64}$/u.test(value.source.digest) && typeof value.source.profile === 'string'
    && /^[a-z0-9][a-z0-9._-]{0,63}$/u.test(value.source.profile));
  validateAppLaunchPath(value.source.entryPath);
  const origins = [value.addresses.canonical, ...value.addresses.aliases].filter(Boolean).map(item => item.origin);
  requireValue(new Set(origins).size === origins.length && !origins.includes(value.shellOrigin));
  return structuredClone(value);
}
