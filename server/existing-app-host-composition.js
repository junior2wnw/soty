import { types } from 'node:util';
import { captureExternalApplications } from './external-applications.js';
import { snapshot } from '../modules/app-contract/json.mjs';
import { freezeDeep, AccessError } from '../modules/capabilities/server/validation.mjs';

const requireThat = value => { if (!value) throw new AccessError('external_application_configuration_invalid'); };
function fields(input, required, optional = []) {
  requireThat(input && typeof input === 'object' && !types.isProxy(input) && !Array.isArray(input)
    && [Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const descriptors = Object.getOwnPropertyDescriptors(input);
  requireThat(Reflect.ownKeys(descriptors).every(key => typeof key === 'string'
    && [...required, ...optional].includes(key) && descriptors[key].enumerable && Object.hasOwn(descriptors[key], 'value'))
    && required.every(key => Object.hasOwn(descriptors, key)));
  return Object.fromEntries(Object.entries(descriptors).map(([key, item]) => [key, item.value]));
}
function rows(input) {
  requireThat(Array.isArray(input) && !types.isProxy(input) && Object.getPrototypeOf(input) === Array.prototype && input.length <= 64);
  const descriptors = Object.getOwnPropertyDescriptors(input);
  requireThat(Reflect.ownKeys(descriptors).length === input.length + 1);
  return Array.from({ length: input.length }, (_, index) => {
    requireThat(descriptors[index] && Object.hasOwn(descriptors[index], 'value'));
    const value = fields(descriptors[index].value, ['appId', 'target', 'catalog', 'adapter'], ['guidance']);
    requireThat(value.adapter && typeof value.adapter === 'object' && !types.isProxy(value.adapter));
    return Object.freeze({ appId: value.appId, target: freezeDeep(snapshot(value.target)),
      catalog: freezeDeep(snapshot(value.catalog)), adapter: value.adapter,
      ...(value.guidance === undefined ? {} : { guidance: freezeDeep(snapshot(value.guidance)) }) });
  });
}

/** Private CODE registry -> the existing createHttpApp externalApplications seam.
 * Registry records are the existing {appId,target,catalog,adapter,guidance} input,
 * not manifests that create a Native owner or Root capability grant. The installed
 * adapter must already own its current Native credential/ACL implementation.
 * No environment/file/provider discovery, storage, new dispatcher or auth flow.
 */
export function createExistingAppHostComposition(options = {}) {
  const value = fields(options, [], ['enabled', 'registry']);
  requireThat(value.enabled === undefined || typeof value.enabled === 'boolean');
  if (value.enabled !== true) return Object.freeze({
    httpOptions: Object.freeze({ externalApplications: Object.freeze([]) }),
    sourceReferences: Object.freeze([]),
  });
  const registry = rows(value.registry ?? []);
  const captured = captureExternalApplications(registry);
  // captureExternalApplications combines the current Apps target with the Native
  // Source binding. Keep the ORIGINAL Source catalogue in createHttpApp options:
  // it captures exactly once again, yielding these same references, not a second
  // recursively combined digest. Preserve the admitted adapter's existing brand.
  const externalApplications = Object.freeze(captured.map((entry, index) => Object.freeze({
    appId: entry.appId, target: entry.target, catalog: registry[index].catalog, adapter: entry.adapter,
    ...(registry[index].guidance === undefined ? {} : { guidance: registry[index].guidance }),
  })));
  return Object.freeze({ httpOptions: Object.freeze({ externalApplications }),
    sourceReferences: Object.freeze(captured.map(entry => entry.contract)) });
}
