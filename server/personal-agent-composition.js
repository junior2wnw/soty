import { createHash, randomUUID } from 'node:crypto';
import { types } from 'node:util';
import { runBoundedRuntimeTurn, createIsolatedContextStore } from '../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';
import { fields, identifier, canonical, freeze } from '../modules/personal-agent/provider/validation.mjs';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { createPersonalAgentModule } from './personal-agent-module.js';
import { createPersonalAgentTransport, agentError } from './personal-agent-transport.js';
import { createNotesConversationStore } from './personal-agent-history.js';
import { createAppJobsExtension } from './apps-jobs.js';
import path from 'node:path';

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
  simple(configuration, [], ['enabled', 'gateway', 'sourceReferences', 'sourceCredentialFor', 'scope', 'policy', 'clock', 'historyEnabled', 'localTargetFor']);
  need(configuration.enabled === undefined || typeof configuration.enabled === 'boolean');
  need(configuration.historyEnabled === undefined || typeof configuration.historyEnabled === 'boolean');
  need(configuration.localTargetFor === undefined || typeof configuration.localTargetFor === 'function');
  const historyEnabled = configuration.historyEnabled !== false, localTargetFor = configuration.localTargetFor;
  need(typeof getConnect === 'function' && typeof getServices === 'function');
  const enabled = configuration.enabled === true, refs = configuration.sourceReferences ?? [];
  need(Array.isArray(refs) && !types.isProxy(refs) && refs.length <= 64);
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
  need(configuration.sourceCredentialFor === undefined || typeof configuration.sourceCredentialFor === 'function'
    && !types.isProxy(configuration.sourceCredentialFor) && !types.isAsyncFunction(configuration.sourceCredentialFor)
    && !types.isGeneratorFunction(configuration.sourceCredentialFor));
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
  const toolsForActor = ({ actor, scope, turnId }) => {
    const keyFor = reference => `${reference.capabilityId}@${reference.version}`;
    const admitted = new Map(references.map(reference => [keyFor(reference), reference]));
    const selected = new Map(), readKeys = new Map(), prefix = randomUUID();
    const pinned = value => {
      simple(value, ['capabilityId', 'version', 'digest']);
      const reference = admitted.get(keyFor(value));
      need(reference && reference.capabilityId === value.capabilityId && reference.version === value.version
        && reference.digest === value.digest, 'agent_source_denied'); return reference;
    };
    const sourceActor = reference => current(actor, services => {
      need(sourceBound && services.capabilities, 'agent_source_unavailable');
      const credential = sourceCredentialFor({ actor, reference });
      if (types.isPromise(credential)) { void Promise.prototype.then.call(credential, () => {}, () => {}); throw agentError('agent_current_port_async'); }
      simple(credential, ['token', 'audience']);
      const delegated = services.capabilities.authenticateCredential(credential);
      // A billing tenant or installed publisher cannot substitute a user's account/device authority.
      services.capabilities.withOwnerAuthority({ actor: delegated }, owner => need(owner.accountId === actor.accountId && owner.deviceId === actor.deviceId, 'agent_source_denied'));
      return delegated;
    });
    const sourceDetail = (reference, delegated) => {
      current(actor, services => services.capabilities.withOwnerAuthority({ actor: delegated }, owner =>
        need(owner.accountId === actor.accountId && owner.deviceId === actor.deviceId, 'agent_source_denied')));
      // Existing Connect/Apps/Native fences remain outside the current actor fence.
      const contract = getServices().capabilities.external?.getContract({ actor: delegated, reference });
      need(contract && contract.effects.length === 0 && contract.executionEnabled === true, 'agent_source_denied');
      current(actor, () => true); return contract;
    };
    const assertCurrent = () => {
      current(actor, services => need(services.notes && services.capabilities, 'agent_source_unavailable'));
      // EMPTY on a Notes/apps-only turn. Only Source data/metadata actually used
      // by this turn retains its original credential and Native current predicate.
      for (const value of selected.values()) sourceDetail(value.reference, value.delegated);
      return true;
    };
    const denied = error => {
      if (!error || typeof error !== 'object' || types.isProxy(error) || !types.isNativeError(error)) return false;
      const field = Object.getOwnPropertyDescriptor(error, 'code');
      return field && Object.hasOwn(field, 'value') && ['authorization_required', 'access_denied', 'not_found',
        'apps_access_denied', 'apps_owner_required', 'app_unavailable', 'external_resource_denied', 'agent_source_denied'].includes(field.value);
    };
    const sourceMatches = (query, signal) => {
      const items = new Map(), checkedGrants = new Set(), service = current(actor, services => services.capabilities.external);
      need(service && typeof service.search === 'function', 'agent_source_unavailable');
      for (const reference of references) {
        need(!signal.aborted, 'agent_cancelled'); let delegated;
        try { delegated = sourceActor(reference); } catch (error) { if (denied(error)) continue; throw error; }
        const grantKey = canonical({ accountId: delegated.accountId, clientId: delegated.clientId, principalId: delegated.principalId, grantId: delegated.grantId });
        if (checkedGrants.has(grantKey)) continue; checkedGrants.add(grantKey);
        let cursor, pages = 0;
        do {
          need(++pages <= 4, 'agent_capacity'); need(!signal.aborted, 'agent_cancelled');
          const page = service.search({ actor: delegated, query, limit: 20, ...(cursor ? { cursor } : {}) });
          for (const item of page.items) {
            const own = admitted.get(keyFor(item.reference));
            if (!own || own.digest !== item.reference.digest || item.effects.length || item.executionEnabled !== true) continue;
            items.set(keyFor(own), { item, delegated, reference: own });
          }
          cursor = page.cursor;
        } while (cursor);
      }
      return [...items.values()].sort((a, b) => keyFor(a.reference).localeCompare(keyFor(b.reference)));
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
      list_my_apps: { capability: 'root_apps_read',
        description: 'List this current user\'s permitted Root application metadata, including their own revoked registrations. Status is a retained observation, not Native SSO or execution readiness.',
        inputSchema: schema({ query: { type: 'string', maxLength: 120 }, limit: { type: 'integer', minimum: 1, maximum: 20 } }),
        validate: input => validate(input, [], ['query', 'limit'])
          && (input.query === undefined || typeof input.query === 'string' && input.query.isWellFormed() && input.query.length <= 120)
          && (input.limit === undefined || Number.isSafeInteger(input.limit) && input.limit >= 1 && input.limit <= 20),
        execute(input, signal) {
          need(!signal.aborted, 'agent_cancelled');
          const result = current(actor, services => {
            need(typeof services.apps?.execute === 'function', 'agent_source_unavailable');
            return services.apps.execute({ op: 'apps.list', actor, args: { expectedAccountId: actor.accountId } });
          });
          assertCurrent(); need(!signal.aborted, 'agent_cancelled');
          need(Array.isArray(result?.apps), 'agent_source_unavailable');
          const query = (input.query ?? '').trim().toLowerCase(), limit = input.limit ?? 20;
          const statuses = ['ready', 'offline', 'starting', 'stopped', 'revoked'];
          const matching = result.apps.filter(row => {
            need(row && typeof row.id === 'string' && /^app-[a-f0-9]{32}$/u.test(row.id)
              && typeof row.name === 'string' && row.name.isWellFormed() && row.name.length <= 64
              && statuses.includes(row.state), 'agent_source_unavailable');
            return row.name.toLowerCase().includes(query);
          });
          // Apps owns ACL and observation. Its private owner/device/entry fields
          // never enter the model's evidence, and this tool performs no launch.
          return evidence('Мои приложения', { apps: matching.slice(0, limit).map(row => ({ appId: row.id, name: row.name, status: row.state })),
            hasMore: matching.length > limit, truncated: matching.length > limit });
        } },
    };
    const target = localTargetFor ? current(actor, () => localTargetFor({ actor })) : null;
    if (target) {
      simple(target, ['hostDeviceId', 'connectorId', 'cwd']); identifier(target.hostDeviceId, 180); identifier(target.connectorId, 180);
      need(typeof target.cwd === 'string' && target.cwd.length <= 2000 && !/[\u0000-\u001f]/u.test(target.cwd)
        && target.cwd === target.cwd.trim() && (path.posix.isAbsolute(target.cwd) || path.win32.isAbsolute(target.cwd)));
      const selected = freeze({ ...target });
      current(actor, services => services.apps.resolveOwnedDevice(actor, selected.hostDeviceId, selected.connectorId));
      tools.run_on_my_device = { capability: 'root_owned_local_task',
        description: 'Queue one task on the current user\'s approved local device/workspace. A queued receipt is not task completion.',
        inputSchema: { ...schema({ text: { type: 'string', minLength: 1, maxLength: 8000 } }), required: ['text'] },
        validate: input => validate(input, ['text']) && typeof input.text === 'string' && input.text.isWellFormed() && input.text.trim().length > 0 && input.text.length <= 8000,
        async execute(input, signal) {
          need(!signal.aborted, 'agent_cancelled'); assertCurrent();
          const services = getServices(), taskId = `agent_${createHash('sha256').update(canonical({ scope, turnId, input, target: selected })).digest('hex')}`;
          // Reuse the existing owner/device/request ledger. Its write-queue guard also observes this turn's cancellation.
          const jobs = createAppJobsExtension({ store: services.connectors.store,
            actorActive: candidate => !closed && !signal.aborted && getConnect().isActorActive(candidate),
            inferenceReady: () => services.connectors.modelProxy.ready === true,
            resolveOwnedDevice: (candidate, ids) => services.apps.resolveOwnedDevice(candidate, ids.hostDeviceId, ids.connectorId) });
          const result = await jobs.executeAsync({ op: 'apps.assistant.send', actor, args: { ...selected, expectedAccountId: actor.accountId,
            requestId: taskId, conversationId: taskId, text: input.text } });
          assertCurrent(); need(!signal.aborted, 'agent_cancelled');
          // createAppJobsExtension's owner-scoped read/write guards produced this closed public projection.
          need(result.job?.schema === 'soty.connector-job.v4' && result.job.deviceId === selected.hostDeviceId, 'agent_source_unavailable');
          return evidence('Задача на моём устройстве', { jobId: result.job.id, status: result.job.status, requestId: taskId,
            completionVerified: ['succeeded', 'failed', 'cancelled'].includes(result.job.status) });
        } };
    }
    if (references.length) {
      tools.source_catalog = { capability: 'source_catalog',
        description: 'Discover current permitted readonly Native Source contracts. Metadata is unprobed availability and grants no rights.',
        inputSchema: schema({ query: { type: 'string', maxLength: 200 }, limit: { type: 'integer', minimum: 1, maximum: 20 } }),
        validate: input => validate(input, [], ['query', 'limit']) && (input.query === undefined || typeof input.query === 'string'
          && input.query.isWellFormed() && input.query.length <= 200) && (input.limit === undefined || Number.isSafeInteger(input.limit) && input.limit >= 1 && input.limit <= 20),
        execute(input, signal) {
          need(!signal.aborted, 'agent_cancelled'); assertCurrent();
          const ordered = sourceMatches(input.query ?? '', signal);
          const chosen = ordered.filter(value => selected.has(keyFor(value.reference))).slice(0, input.limit ?? 20);
          for (const value of ordered) {
            if (chosen.length >= (input.limit ?? 20) || selected.size + chosen.filter(item => !selected.has(keyFor(item.reference))).length >= 8) break;
            if (!selected.has(keyFor(value.reference))) chosen.push(value);
          }
          for (const value of chosen) {
            const retained = selected.get(keyFor(value.reference)) ?? value;
            sourceDetail(retained.reference, retained.delegated); selected.set(keyFor(retained.reference), retained);
          }
          assertCurrent(); need(!signal.aborted, 'agent_cancelled');
          return evidence('Разрешённые источники', { items: chosen.map(value => value.item), hasMore: ordered.length > chosen.length, availability: 'unprobed' });
        } };
      tools.source_read_1 = { capability: 'source_read_1',
        description: 'Read one exact reference or UNIQUE authorized catalogue query through current Root/Apps/Native grants. Ambiguous queries never read. Omission selects only a sole installed Source.',
        inputSchema: schema({ reference: { type: 'object', additionalProperties: false, properties: { capabilityId: { type: 'string', maxLength: 160 },
          version: { type: 'integer', minimum: 1, maximum: 1000000 }, digest: { type: 'string', pattern: '^[a-f0-9]{64}$' } }, required: ['capabilityId', 'version', 'digest'] }, query: { type: 'string', maxLength: 200 }, input: { type: 'object' } }),
        validate(input) {
          try { if (!validate(input, [], ['reference', 'query', 'input']) || input.reference !== undefined && input.query !== undefined
              || input.reference === undefined && input.query === undefined && references.length !== 1) return false;
            if (input.reference !== undefined) pinned(input.reference);
            if (input.query !== undefined && (typeof input.query !== 'string' || !input.query.isWellFormed() || input.query.length > 200)) return false;
            if (input.input !== undefined) captureQueryData(input.input); return true;
          } catch { return false; }
        },
        async execute(input, signal) {
          need(!signal.aborted, 'agent_cancelled'); assertCurrent();
          const matches = input.query === undefined ? null : sourceMatches(input.query, signal);
          if (matches) need(matches.length === 1, 'agent_source_denied');
          const reference = matches ? matches[0].reference : input.reference === undefined ? references[0] : pinned(input.reference), key = keyFor(reference);
          need(selected.has(key) || selected.size < 8, 'agent_capacity');
          const delegated = selected.get(key)?.delegated ?? matches?.[0].delegated ?? sourceActor(reference),
            detail = sourceDetail(reference, delegated), data = captureQueryData(input.input ?? {});
          const keys = readKeys.get(key) ?? new Map(), digest = createHash('sha256').update(canonical(data, 65536)).digest('hex');
          if (!keys.has(digest)) { need(keys.size < 8, 'agent_capacity'); keys.set(digest, `agent_read_${prefix}_${createHash('sha256').update(key).digest('hex').slice(0, 16)}_${digest.slice(0, 32)}`); }
          readKeys.set(key, keys);
          const service = current(actor, services => services.capabilities.external);
          // The original coordinator owns validation/current Native predicates, durable dispatch CAS and receipt.
          const result = await service.query({ actor: delegated, reference, idempotencyKey: keys.get(digest), input: data });
          sourceDetail(reference, delegated); assertCurrent(); need(!signal.aborted, 'agent_cancelled');
          need(result.resultUnavailable === false && result.invocation.status === 'succeeded', 'agent_source_unavailable');
          selected.set(key, { reference, delegated }); return evidence(detail.title, result.result);
        } };
    }
    return { tools, assertCurrent, ...(historyEnabled ? { contextStore: createNotesConversationStore({ actor, scope, turnId,
      // The module wraps every context callback in its fresh Connect fence. Do not nest that fence here.
      io: (op, args) => getServices().notes.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } }) }) } : {}) };
  };
  const runtime = { runTurn: runBoundedRuntimeTurn, createContextStore: createIsolatedContextStore,
    outputProfile: 'host-enforced-terminal.v1', adapterFor: ({ tools }) => ({ id: 'soty-personal-agent', roles: ['human'],
      capabilities: [...new Set(Object.values(tools).map(tool => tool.capability))], applicability: 'Текущий пользователь и разрешённые источники',
      systemContext: terminal => `Помогай текущему пользователю. Читай только предложенные инструменты. Заверши реальным вызовом ${terminal}; факты подтверждай выбранными источниками.`,
      isActionHrefAllowed: () => false }) };
  const module = createPersonalAgentModule({ getConnect, runtime: enabled && sourceBound ? runtime : undefined, transport, toolsForActor,
    ...(configuration.scope ? { scope: configuration.scope } : {}), ...(configuration.policy ? { policy: configuration.policy } : {}),
    ...(configuration.clock ? { clock: configuration.clock } : {}) });
  return Object.freeze({ operations: module.operations, executeAsync: request => module.executeAsync(request),
    status() { const status = module.status(); return Object.freeze({ ...status, configured: !closed && status.configured,
      runtimeBound: true, missingBinding: !enabled ? 'not_enabled' : !sourceBound ? 'current_source_credential' : status.configured ? null : 'approved_accounted_gateway_actor_credential_receipt',
      memory: historyEnabled ? 'owner_notes_labelled_history' : 'ephemeral_context_and_current_notes', voiceConfigured: false }); },
    close() { if (closed) return; closed = true; module.close(); transport.close(); } });
}
