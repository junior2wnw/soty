import { readFileSync } from 'node:fs';
export const appHostingPolicyPath = '/run/connect-releases/soty-app-hosting.json';
/** Operator-owned, nonsecret deployment settings. Absence keeps legacy behavior. */
export function readAppHostingConfig(path = appHostingPolicyPath) {
  let bytes;
  try { bytes = readFileSync(path); } catch (error) { if (error.code === 'ENOENT') return {}; throw new Error('apps_hosting_config_unreadable'); }
  if (bytes.length > 4096) throw new Error('apps_hosting_config_invalid');
  let value; try { value = JSON.parse(bytes); } catch { throw new Error('apps_hosting_config_invalid'); }
  const allowed = ['discoveryOrigin', 'domainProfile', 'namedAppZone', 'schema', 'retainedNamedAppZones'];
  if (!value || Array.isArray(value) || Object.keys(value).some(key => !allowed.includes(key))
    || !['discoveryOrigin', 'domainProfile', 'namedAppZone', 'schema'].every(key => Object.hasOwn(value, key))
    || value.schema !== 'soty.app-hosting.v1' || value.domainProfile !== 'shell-subdomains-v1'
    || typeof value.namedAppZone !== 'string' || typeof value.discoveryOrigin !== 'string'
    || (value.retainedNamedAppZones !== undefined && (!Array.isArray(value.retainedNamedAppZones)
      || value.retainedNamedAppZones.length > 8 || value.retainedNamedAppZones.some(zone => typeof zone !== 'string' || !zone)))) throw new Error('apps_hosting_config_invalid');
  return { namedAppZone: value.namedAppZone, discoveryOrigin: value.discoveryOrigin, domainProfile: value.domainProfile,
    ...(value.retainedNamedAppZones === undefined ? {} : { retainedNamedAppZones: value.retainedNamedAppZones }) };
}
