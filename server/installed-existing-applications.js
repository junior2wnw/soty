import { types } from 'node:util';
import { AccessError } from '../modules/capabilities/server/validation.mjs';
import { createExistingAppHostComposition } from './existing-app-host-composition.js';

/** The only input is a private installed CODE factory, not a JSON manifest,
 * actor, an HTTP option or a provider credential. Its rows reuse the existing
 * Root adapter brands/Native ports and common catalogue. Missing native ports
 * produce no row; a registry must not turn fixtureOnly/carrierReady flags on.
 * Public server/index integration is a separate, coordinated one-line mount.
 */
export async function loadInstalledExistingApplications(options={}){
  if(!options||typeof options!=='object'||types.isProxy(options)||Array.isArray(options)||![Object.prototype,null].includes(Object.getPrototypeOf(options)))throw new AccessError('external_application_configuration_invalid');
  const descriptors=Object.getOwnPropertyDescriptors(options);
  if(Reflect.ownKeys(descriptors).some(key=>key!=='createRegistry'||!descriptors[key].enumerable||!Object.hasOwn(descriptors[key],'value')))throw new AccessError('external_application_configuration_invalid');
  const createRegistry=descriptors.createRegistry?.value;
  if(createRegistry===undefined)return createExistingAppHostComposition();
  if(typeof createRegistry!=='function'||types.isProxy(createRegistry))throw new AccessError('external_application_configuration_invalid');
  const registry=await createRegistry();
  return createExistingAppHostComposition({enabled:true,registry});
}
