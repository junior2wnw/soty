import { createExternalCapabilityOperations } from './external-capabilities.js';
import { AppAgentDirectoryError } from '../modules/app-agent-directory/server/index.mjs';
import { AccessError } from '../modules/capabilities/server/validation.mjs';
import { canonicalContractJson } from '../modules/app-contract/index.mjs';

const require = (value, code = 'directory_execution_denied') => { if (!value) throw new AppAgentDirectoryError(code, 403); };
/** resolveBinding is reviewed host CODE. An author's binding reference is never imported or executed. */
export function createAppAgentDirectoryTransport({ service, origin, resolveBinding, appFeedback }) {
  require(typeof service === 'function' && typeof resolveBinding === 'function', 'directory_transport_invalid');
  function current(request) {
    require(request.actor && request.capability.effects.every(effect => effect === 'read'));
    const reference = resolveBinding(request);
    if (!reference) return null;
    require(Object.keys(reference).every(key => ['capabilityId', 'version', 'digest'].includes(key))
      && typeof reference.capabilityId === 'string' && Number.isSafeInteger(reference.version) && /^[a-f0-9]{64}$/u.test(reference.digest));
    const host = service();
    require(host?.external?.queryConfigured === true, 'directory_execution_unavailable');
    const entry = host.external.getContract({ actor: request.actor, reference });
    require(entry && entry.appId === request.authority.appId && entry.reference.digest === reference.digest && entry.effects.length === 0 && entry.executionEnabled === true);
    const same = (left, right) => canonicalContractJson(left) === canonicalContractJson(right);
    require(request.capability.version === reference.version && same(request.capability.binding, entry.binding)
      && same(request.capability.inputSchema, entry.inputSchema) && same(request.capability.outputSchema, entry.outputSchema)
      && same(request.capability.resources.map(value => value.id).sort(), [...entry.resources].sort())
      && same(request.capability.recipients.map(value => value.id).sort(), [...entry.recipients].sort()));
    // This checks the current credential's capability/resource/recipient scope;
    // the admitted readonly adapter separately rechecks Source authority at call time.
    host.authorize({ actor: request.actor, action: 'read', capabilityId: reference.capabilityId, version: reference.version });
    return { ready: true, protocol: 'soty-root-mcp', tool: 'apps_query', reference };
  }
  return Object.freeze({
    inspect(request) { try { return current(request); } catch (error) {
      if (error instanceof AccessError || ['access_denied', 'grant_revoked', 'grant_expired', 'credential_revoked', 'credential_expired',
        'directory_execution_denied', 'directory_execution_unavailable', 'capability_not_found', 'not_found', 'authentication_required', 'external_resource_denied'].includes(error?.code)) return null;
      throw error;
    } },
    async query({ actor, execution, input, idempotencyKey }) {
      require(execution?.ready === true && execution.protocol === 'soty-root-mcp' && execution.tool === 'apps_query');
      const operations = createExternalCapabilityOperations({ service: service(), origin, appFeedback });
      const args = { reference: execution.reference, input, idempotencyKey };
      const result = await operations.call({ actor, name: 'apps_query', args });
      operations.recheck({ actor, name: 'apps_query', args, value: result });
      return result;
    },
    recheck({ actor, execution, input, idempotencyKey, value }) {
      const operations = createExternalCapabilityOperations({ service: service(), origin, appFeedback });
      operations.recheck({ actor, name: 'apps_query', args: { reference: execution.reference, input, idempotencyKey }, value });
    },
  });
}
