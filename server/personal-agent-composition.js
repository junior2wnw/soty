import { createHash, randomUUID } from 'node:crypto';
import { types } from 'node:util';
import { runBoundedRuntimeTurn, createIsolatedContextStore } from '../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';
import { fields, identifier, canonical, freeze } from '../modules/personal-agent/provider/validation.mjs';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { createPersonalAgentModule } from './personal-agent-module.js';
import { createPersonalAgentTransport, agentError } from './personal-agent-transport.js';

const need = (ok, code = 'agent_invalid_input') => { if (!ok) throw agentError(code); };
const simple = (value, required, optional = []) => {
  need(value && typeof value === 'object' && !types.isProxy(value));
  fields(value, required, optional);
  return value;
};
const schema = properties => ({ type: 'object', additionalProperties: false, properties });
const validate = (value, required, optional = []) => { try { simple(value, required, optional); return true; } catch { return false; } };

/** Installed code only. No key/model/tenant/source authority is read from RPC, a manifest, or the HTTP Host header.
 * Notes are the existing account-owned Notes API; encrypted legacy rooms/identity are not an agent data source. */
export function createPersonalAgentComposition({ getConnect, getServices, configuration = {} } = {}) {
  simple(configuration, [], ['enabled', 'gateway', 'sourceReferences', 'sourceCredentialFor', 'scope', 'policy', 'clock']);
  need(configuration.enabled === undefined || typeof configuration.enabled === 'boolean');
  need(typeof getConnect === 'function' && typeof getServices === 'function');
  const enabled = configuration.enabled === true, refs = configuration.sourceReferences ?? [];
  need(Array.isArray(refs) && !types.isProxy(refs) && refs.length <= 8);
  const refFields = Object.getOwnPropertyDescriptors(refs);
  need(Reflect.ownKeys(refFields).length === refs.length + 1);
  const references = Object.freeze(Array.from({ length: refs.length }, (_, index) => {
    need(refFields[index] && Object.hasOwn(refFields[index], 'value'));
    const ref = refFields[index].value;
    simple(ref, ['capabilityId', 'version', 'digest']); identifier(ref.capabilityId, 160);
    need(Number.isSafeInteger(ref.version) && ref.version >= 1 && ref.version <= 1000000
      && typeof ref.digest === 'string' && /^[a-f0-9]{64}$/u.test(ref.digest));
    return freeze({ ...ref });
  }));
  need(new Set(references.map(ref => `${ref.capabilityId}@${ref.version}`)).size === references.length);
  need(configuration.sourceCredentialFor === undefined || typeof configuration.sourceCredentialFor === 'function');
  const sourceCredentialFor = configuration.sourceCredentialFor;
  const sourceBound = references.length === 0 || typeof sourceCredentialFor === 'function';
  const gateway = configuration.gateway ?? {};
  simple(gateway, [], ['gatewayOrigin', 'billingTenant', 'keyRef', 'resolveCredential', 'readReceipt', 'authorizeActor',
    'fetchImpl', 'allowLoopbackFixture', 'receiptWaitMs']);
  const transport = createPersonalAgentTransport(enabled ? gateway : {});
  let closed = false;
  const current = (actor, callback) => {
    need(!closed, 'agent_closed');
    return getConnect().withActorAuthorityFence(actor, () => callback(getServices()));
  };
  const toolsForActor = ({ actor }) => {
    const sourceActor = reference => current(actor, services => {
      need(sourceBound && services.capabilities, 'agent_source_unavailable');
      const credential = sourceCredentialFor({ actor, reference });
      if (types.isPromise(credential)) { void Promise.prototype.then.call(credential, () => {}, () => {}); throw agentError('agent_current_port_async'); }
      simple(credential, ['token', 'audience']);
      const delegated = services.capabilities.authenticateCredential(credential);
      // A billing tenant or installed app publisher does not supply another user's capability authority.
      services.capabilities.withOwnerAuthority({ actor: delegated }, owner => need(owner.accountId === actor.accountId && owner.deviceId === actor.deviceId, 'agent_source_denied'));
      return delegated;
    });
    const sourceDetail = reference => {
      const delegated = sourceActor(reference);
      // The existing broker opens its own Connect/Apps/Source fence. Never nest it in the actor fence.
      // Its live root-grant creator is exactly the caller device checked above, so admission fences that same principal.
      const contract = getServices().capabilities.external?.getContract({ actor: delegated, reference });
      current(actor, () => true); return contract;
    };
    const assertCurrent = () => {
      current(actor, services => need(services.notes && services.capabilities, 'agent_source_unavailable'));
      for (const reference of references) {
        const contract = sourceDetail(reference);
        need(contract && contract.effects.length === 0 && contract.executionEnabled === true, 'agent_source_denied');
      }
      return true;
    };
    assertCurrent();
    const evidence = (title, data) => ({ evidence: [{ id: `root_${randomUUID().replaceAll('-', '')}`, title,
      observedAt: new Date().toISOString(), sourceStatus: 'current', data: captureQueryData(data) }] });
    const notes = (op, args) => current(actor, services => services.notes.execute({ op, actor,
      args: { ...args, expectedAccountId: actor.accountId } }));
    const tools = {
      list_my_notes: { capability: 'root_notes_read', description: 'Find this signed-in user\'s existing Notes. No other account or encrypted room is read.',
        inputSchema: schema({ query: { type: 'string', maxLength: 120 }, limit: { type: 'integer', minimum: 1, maximum: 20 } }),
        validate: input => validate(input, [], ['query', 'limit']) && (input.query === undefined || typeof input.query === 'string' && input.query.length <= 120)
          && (input.limit === undefined || Number.isSafeInteger(input.limit) && input.limit >= 1 && input.limit <= 20),
        execute(input, signal) { need(!signal.aborted, 'agent_cancelled'); const result = notes('notes.list', { query: input.query ?? '', limit: input.limit ?? 20 });
          assertCurrent(); return evidence('Мои записки', { notes: result.notes, nextCursor: result.nextCursor }); } },
      read_my_note: { capability: 'root_notes_read', description: 'Read one existing Note owned by this signed-in user.',
        inputSchema: { ...schema({ noteId: { type: 'string', minLength: 3, maxLength: 160 } }), required: ['noteId'] },
        validate: input => validate(input, ['noteId']) && typeof input.noteId === 'string' && /^[A-Za-z0-9_-]{3,160}$/u.test(input.noteId),
        execute(input, signal) { need(!signal.aborted, 'agent_cancelled'); const result = notes('notes.get', { noteId: input.noteId });
          assertCurrent(); return evidence('Моя записка', result); } },
    };
    references.forEach((reference, index) => {
      const detail = sourceDetail(reference);
      const keys = new Map(), prefix = randomUUID();
      tools[`source_read_${index + 1}`] = { capability: `source_read_${index + 1}`, description: detail.description,
        inputSchema: detail.inputSchema,
        validate(input) { try { captureQueryData(input); return true; } catch { return false; } },
        async execute(input, signal) {
          need(!signal.aborted, 'agent_cancelled'); assertCurrent();
          const digest = createHash('sha256').update(canonical(input, 65536)).digest('hex');
          if (!keys.has(digest)) { need(keys.size < 8, 'agent_capacity'); keys.set(digest, `agent_read_${prefix}_${digest.slice(0, 32)}`); }
          const service = current(actor, services => services.capabilities.external);
          // Existing coordinator validates input and current Root+Native authority, accounts dispatch, and returns its actual receipt.
          const result = await service.query({ actor: sourceActor(reference), reference, idempotencyKey: keys.get(digest), input });
          assertCurrent(); need(!signal.aborted, 'agent_cancelled');
          need(result.resultUnavailable === false && result.invocation.status === 'succeeded', 'agent_source_unavailable');
          return evidence(detail.title, result.result);
        } };
    });
    return { tools, assertCurrent };
  };
  const runtime = { runTurn: runBoundedRuntimeTurn, createContextStore: createIsolatedContextStore,
    outputProfile: 'host-enforced-terminal.v1', adapterFor: ({ tools }) => ({ id: 'soty-personal-read-agent', roles: ['human'],
      capabilities: [...new Set(Object.values(tools).map(tool => tool.capability))], applicability: 'Текущий пользователь и разрешённые источники',
      systemContext: terminal => `Помогай текущему пользователю. Читай только предложенные инструменты. Заверши реальным вызовом ${terminal}; факты подтверждай выбранными источниками.`,
      isActionHrefAllowed: () => false }) };
  const module = createPersonalAgentModule({ getConnect, runtime: enabled && sourceBound ? runtime : undefined, transport, toolsForActor,
    ...(configuration.scope ? { scope: configuration.scope } : {}), ...(configuration.policy ? { policy: configuration.policy } : {}),
    ...(configuration.clock ? { clock: configuration.clock } : {}) });
  return Object.freeze({ operations: module.operations, executeAsync: request => module.executeAsync(request),
    status() { const status = module.status(); return Object.freeze({ ...status, configured: !closed && status.configured,
      runtimeBound: true, missingBinding: !enabled ? 'not_enabled' : !sourceBound ? 'current_source_credential' : status.configured ? null : 'approved_accounted_gateway_actor_credential_receipt',
      memory: 'ephemeral_context_and_current_notes', voiceConfigured: false }); },
    close() { if (closed) return; closed = true; module.close(); transport.close(); } });
}
