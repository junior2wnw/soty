import { snapshot } from '../modules/app-contract/json.mjs';
import { AppAgentDirectoryError } from '../modules/app-agent-directory/server/index.mjs';
import {MONITORING_OPERATIONS} from '../modules/monitoring/definitions.mjs';

export const DIRECTORY_MODULE = Object.freeze({ id: 'soty.app-agent-directory', version: 1, title: 'App and module discovery',
  description: 'Current owner-opt-in application capabilities. Metadata does not grant execution.', kind: 'service',
  contract: 'soty.app-agent-directory.v1', documentation: '/agents/app-platform.md', readiness: 'available',
  operations: ['apps.agentDescriptor.get', 'apps.agentDescriptor.put'], providerKinds: [] });
export const GLASS_UI_MODULE = Object.freeze({id:'soty.optical-glass',version:1,title:'Стеклянный UI и виджеты',
 description:'Shared Glass UI styles and optional optical runtime for application authors. UI code grants no platform or Source access.',kind:'sdk',
 contract:'volumetric-glass-ui.local.v1',documentation:'/agents/glass-ui.md',readiness:'available',operations:[],providerKinds:[]});
// Include from the installed host registry when its module is composed. Metadata never grants tenant/admin access.
export const MONITORING_MODULE=Object.freeze({id:'soty.monitoring',version:1,title:'Состояние, аналитика и управление',
 description:'Reusable timestamped metrics with explicit scope, collection settings and installed canonical administration providers. Separate current rights and grants are mandatory.',
 kind:'service',contract:'soty.monitoring.v1',documentation:'/agents/monitoring.md',readiness:'available',operations:MONITORING_OPERATIONS,providerKinds:[]});
/** Entries come from the installed host module registry, including future canvas.widget providers. */
export function createAppModuleCatalog({ entries = () => [] } = {}) {
  if (typeof entries !== 'function') throw new AppAgentDirectoryError('directory_module_registry_invalid', 500);
  return Object.freeze({ list() {
    const modules = snapshot([DIRECTORY_MODULE, GLASS_UI_MODULE, ...entries()]);
    if (modules.length > 64) throw new AppAgentDirectoryError('directory_module_limit', 503);
    const seen = new Set();
    for (const value of modules) {
      const keys = ['id', 'version', 'title', 'description', 'kind', 'contract', 'documentation', 'readiness', 'operations', 'providerKinds'];
      if (!value || !keys.every(key => Object.hasOwn(value, key)) || Object.keys(value).some(key => !keys.includes(key))
        || !/^[a-z][a-z0-9._-]{0,95}$/u.test(value.id) || seen.has(value.id) || !Number.isSafeInteger(value.version) || value.version < 1
        || typeof value.title !== 'string' || value.title.length > 160 || typeof value.description !== 'string' || value.description.length > 2000
        || !['service', 'sdk', 'provider-contract'].includes(value.kind) || !['available', 'contract-only'].includes(value.readiness)
        || typeof value.contract !== 'string' || value.contract.length > 128
        || !/^\/(?:agents|sdk)\/[A-Za-z0-9._/-]{1,180}$/u.test(value.documentation) || value.documentation.includes('..')
        || !Array.isArray(value.operations) || !Array.isArray(value.providerKinds)
        || [...value.operations, ...value.providerKinds].some(item => typeof item !== 'string' || !/^[A-Za-z0-9._:/-]{1,160}$/u.test(item)))
        throw new AppAgentDirectoryError('directory_module_registry_invalid', 503);
      seen.add(value.id);
    }
    return { schema: 'soty.agent-module-catalog.v1', modules, authority: 'installed-module-metadata; separate grants required' };
  } });
}
