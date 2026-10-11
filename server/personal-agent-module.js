import { createHash } from 'node:crypto';
import { types } from 'node:util';
import { createGlmProvider, providerFailureCode } from '../modules/personal-agent/provider/index.mjs';
import { boundedPolicy, canonical, fields, freeze, identifier, string } from '../modules/personal-agent/provider/validation.mjs';
import { agentError, agentErrorCode, isPersonalAgentTransport } from './personal-agent-transport.js';

const requireValue = (ok, code = 'agent_invalid_input') => { if (!ok) throw agentError(code); };
const syncResult = value => {
  if (types.isPromise(value)) { void Promise.prototype.then.call(value, () => {}, () => {}); throw agentError('agent_current_port_async'); }
  return value;
};
const terminalArguments = value => {
  try {
    fields(value, ['answer', 'selectedEvidenceIds', 'claims']);
    return typeof value.answer === 'string' && value.answer.trim().length > 0 && value.answer.length <= 8000
      && Array.isArray(value.selectedEvidenceIds) && value.selectedEvidenceIds.length <= 32 && Array.isArray(value.claims) && value.claims.length <= 16;
  } catch { return false; }
};

/** Callable signed extension. Runtime functions are explicit host imports; no TS donor is dynamically loaded here.
 * outputProfile is local completed-result enforcement, not a claim of upstream forced-tool/json_schema support. */
export function createPersonalAgentModule({ getConnect, runtime, transport, toolsForActor,
  scope = { registryId: 'REG.soty', environmentId: 'production', audienceId: 'personal' }, policy = {}, clock = Date.now } = {}) {
  fields(scope, ['registryId', 'environmentId', 'audienceId']);
  for (const value of Object.values(scope)) identifier(value, 160);
  const hostScope = freeze({ ...scope }), modelPolicy = boundedPolicy(policy);
  const configured = typeof getConnect === 'function' && typeof runtime?.runTurn === 'function'
    && typeof runtime?.createContextStore === 'function' && typeof runtime?.adapterFor === 'function'
    && runtime.outputProfile === 'host-enforced-terminal.v1' && typeof toolsForActor === 'function'
    && isPersonalAgentTransport(transport) && transport.configured === true;
  const contexts = new Map(), turns = new Map(), operations = new Set(['agent.turn', 'agent.cancel']); let closed = false;
  const now = () => { const value = clock(); requireValue(Number.isSafeInteger(value) && value >= 0); return value; };
  const hash = value => createHash('sha256').update(canonical(value)).digest('hex');
  const actorScope = actor => ({ ...hostScope, accountId: actor.accountId });
  const currentActor = actor => {
    requireValue(!closed, 'agent_closed');
    const connect = getConnect();
    try { return connect.withActorAuthorityFence(actor, () => true); } catch { throw agentError('agent_actor_denied'); }
  };
  const contextFor = actor => {
    const at = now(), key = hash(actorScope(actor));
    for (const [id, entry] of contexts) if (entry.active === 0 && entry.expiresAt <= at) contexts.delete(id);
    let entry = contexts.get(key);
    if (!entry) {
      requireValue(contexts.size < 128, 'agent_capacity');
      entry = { store: runtime.createContextStore({ roles: ['human'], ttlMs: 1800000, maxContexts: 32 }), active: 0, expiresAt: at + 1800000 };
      contexts.set(key, entry);
    }
    entry.active++; entry.expiresAt = at + 1800000;
    return entry;
  };
  async function executeAsync({ op, args, actor }) {
    requireValue(configured, 'agent_transport_unavailable'); currentActor(actor);
    if (op === 'agent.cancel') {
      fields(args, ['requestId']); const turnId = identifier(args.requestId, 100);
      const turn = turns.get(hash({ scope: actorScope(actor), turnId }));
      requireValue(turn, 'agent_turn_unavailable');
      const cancelRequested = turn.state === 'running';
      if (cancelRequested) turn.abort.abort();
      return { schema: 'soty.personal-agent-cancel.v1', requestId: turnId, cancelRequested, remoteTerminationVerified: false };
    }
    requireValue(op === 'agent.turn');
    fields(args, ['requestId', 'prompt'], ['contextId']);
    const turnId = identifier(args.requestId, 100), prompt = string(args.prompt, 16000);
    requireValue(turnId.length >= 8 && prompt.length <= 8000);
    if (args.contextId !== undefined) identifier(args.contextId, 160);
    const frameScope = actorScope(actor), key = hash({ scope: frameScope, turnId });
    const intent = hash({ deviceId: actor.deviceId, args });
    const previous = turns.get(key);
    if (previous) throw agentError(previous.intent === intent ? 'agent_reconcile_required' : 'agent_intent_conflict');
    // Only ephemeral dispatch metadata expires; the commerce ledger retains durable idempotency.
    const timestamp = now();
    for (const [id, value] of turns) if (value.state !== 'running' && value.retireAfter <= timestamp) turns.delete(id);
    requireValue(turns.size < 128, 'agent_capacity');
    const turn = { intent, state: 'running', receipts: [], abort: new AbortController(), retireAfter: timestamp + 1800000 }; turns.set(key, turn);
    const unsubscribe = getConnect().subscribeRevocations(event => { if (event.deviceId === actor.deviceId) turn.abort.abort(); });
    let bundle, activeProvider, contextEntry;
    const assertCurrent = () => {
      currentActor(actor);
      requireValue(!turn.abort.signal.aborted, 'agent_cancelled');
      if (bundle) requireValue(syncResult(bundle.assertCurrent()) === true, 'agent_source_denied');
    };
    try {
      bundle = await toolsForActor({ actor, scope: frameScope, turnId }); currentActor(actor);
      requireValue(bundle && typeof bundle.assertCurrent === 'function' && bundle.tools && typeof bundle.tools === 'object', 'agent_source_unavailable');
      assertCurrent();
      const bound = transport.bind({ actor, scope: frameScope, turnId, assertCurrent }); bound.guard();
      if (bundle.contextStore === undefined) contextEntry = contextFor(actor);
      const rawContext = bundle.contextStore ?? contextEntry.store;
      fields(rawContext, ['resolve', 'snapshot', 'append'], bundle.contextStore === undefined ? ['switchRole', 'replaceHistory', 'setState', 'clear', 'size'] : []);
      requireValue(['resolve', 'snapshot', 'append'].every(name => typeof rawContext[name] === 'function'), 'agent_source_unavailable');
      const contextStore = Object.freeze(Object.fromEntries(['resolve', 'snapshot', 'append'].map(name => [name, (...values) => {
        assertCurrent(); return getConnect().withActorAuthorityFence(actor, () => rawContext[name](...values));
      }])));
      const tools = {};
      for (const [name, tool] of Object.entries(bundle.tools)) {
        requireValue(name !== 'finish_answer' && /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/u.test(name)
          && typeof tool.validate === 'function' && typeof tool.execute === 'function' && tool.inputSchema, 'agent_tool_binding_invalid');
        tools[name] = { ...tool, execute: async (input, signal) => {
          const currentSignal = AbortSignal.any([signal, turn.abort.signal]);
          assertCurrent(); requireValue(!currentSignal.aborted, 'agent_cancelled');
          const result = await tool.execute(input, currentSignal); assertCurrent(); requireValue(!currentSignal.aborted, 'agent_cancelled'); return result;
        } };
      }
      const adapter = runtime.adapterFor({ actor, scope: frameScope, tools }); assertCurrent();
      let round = 0;
      const provider = async (messages, signal, offered, options = {}) => {
        signal = AbortSignal.any([signal, turn.abort.signal]);
        assertCurrent(); bound.guard(); fields(options, [], ['toolChoice', 'parallelToolCalls', 'maxTokens']);
        requireValue(options.parallelToolCalls === undefined || options.parallelToolCalls === false, 'agent_output_profile_invalid');
        const required = options.toolChoice;
        requireValue(required === undefined || required === 'finish_answer', 'agent_output_profile_invalid');
        requireValue(offered.finish_answer?.capability === '__terminal__', 'agent_output_profile_invalid');
        for (const [name, tool] of Object.entries(offered)) requireValue(name === 'finish_answer' || tools[name] === tool, 'agent_tool_binding_invalid');
        // Register prior resolved history names too; local final enforcement does not execute/offer new permissions.
        const selected = { ...tools, ...offered };
        const registry = Object.entries(selected).map(([name, tool]) => ({ name, description: tool.description, parameters: tool.inputSchema,
          validateArguments: name === 'finish_answer' ? terminalArguments : tool.validate.bind(tool) }));
        const requestId = `${turnId}:round:${round++}`;
        activeProvider = createGlmProvider({ transport: bound.complete, tools: registry,
          policy: { ...modelPolicy, ...(options.maxTokens !== undefined ? { maxOutputTokens: Math.min(modelPolicy.maxOutputTokens, options.maxTokens) } : {}) } });
        let result;
        try { result = await activeProvider.complete({ requestId, messages, signal }); }
        catch (error) {
          assertCurrent();
          try { console.error(JSON.stringify({ schema: 'soty.personal-agent-provider-failure.v1', code: providerFailureCode(error) })); } catch {}
          throw agentError('agent_model_unknown');
        }
        finally { activeProvider.close(); activeProvider = null; }
        assertCurrent();
        const receipt = await bound.account(requestId, result, signal); assertCurrent(); turn.receipts.push(receipt);
        requireValue(result.message.toolCalls.length === 1 && (required === undefined || result.message.toolCalls[0].name === required), 'agent_output_invalid');
        return { choices: [{ finish_reason: result.finishReason, message: { content: result.message.content || null,
          tool_calls: result.message.toolCalls.map(call => ({ id: call.id, type: 'function', function: { name: call.name,
            arguments: canonical(call.arguments, modelPolicy.toolArgumentBytes, modelPolicy.jsonDepth) } })) } }] };
      };
      // Keep the complete runtime below the browser's signed-turn transport deadline.
      const result = await runtime.runTurn({ timeoutMs: 90_000, adapter, role: 'human', prompt, contextStore, contextId: args.contextId, tools, provider });
      assertCurrent(); requireValue(result.status === 'complete', 'agent_output_invalid');
      turn.state = 'completed';
      return { schema: 'soty.personal-agent-turn.v1', requestId: turnId, ...result, accounting: turn.receipts };
    } catch (error) { turn.state = 'unknown'; throw agentError(agentErrorCode(error)); }
    finally { activeProvider?.close(); unsubscribe(); if (contextEntry) contextEntry.active--; }
  }
  return Object.freeze({ operations, executeAsync,
    status: () => Object.freeze({ configured, wholeReady: false, runtimeBound: Boolean(runtime?.runTurn),
      missingBinding: configured ? null : 'current_runtime_tools_and_accounted_gateway', activeTurns: [...turns.values()].filter(value => value.state === 'running').length }),
    close() { closed = true; for (const turn of turns.values()) if (turn.state === 'running') turn.abort.abort(); },
  });
}
