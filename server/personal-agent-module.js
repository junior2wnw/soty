import { createHash } from 'node:crypto';
import { types } from 'node:util';
import { createGlmProvider, providerFailureDiagnostic } from '../modules/personal-agent/provider/index.mjs';
import { boundedPolicy, canonical, fields, freeze, identifier, string } from '../modules/personal-agent/provider/validation.mjs';
import { agentError, agentErrorCode, isPersonalAgentTransport } from './personal-agent-transport.js';

const requireValue = (ok, code = 'agent_invalid_input') => { if (!ok) throw agentError(code); };
const runtimeFailureCodes = new Set(['runtime-deadline', 'provider-round-limit', 'provider-tool-count-invalid',
  'provider-tool-unknown', 'tool-not-authorized', 'tool-arguments-invalid', 'tool-action-limit',
  'tool-evidence-invalid', 'tool-evidence-duplicate', 'tool-card-invalid', 'tool-action-invalid',
  'terminal-evidence-invalid', 'terminal-claim-invalid', 'terminal-claim-evidence-invalid']);
export function turnFailureDiagnostic(error, stage) {
  const message = types.isNativeError(error) && Object.getOwnPropertyDescriptor(error, 'message');
  const code = message && Object.hasOwn(message, 'value') && runtimeFailureCodes.has(message.value)
    ? message.value : agentErrorCode(error);
  return Object.freeze({ code, stage: ['prepare', 'model', 'account', 'tool', 'runtime'].includes(stage) ? stage : 'runtime' });
}
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
// Retain only the bounded presentation already returned to this actor. Source
// payloads, tool arguments and provider messages are not a status transport.
function recoverableAnswer(result, requestId) {
  if (typeof result.answer !== 'string' || !result.answer.trim() || result.answer.length > 8000
    || typeof result.contextId !== 'string' || !/^ctx_[A-Za-z0-9_-]{16,124}$/u.test(result.contextId)) return undefined;
  const evidence = result.evidence ?? [];
  if (!Array.isArray(evidence) || evidence.length > 32) return undefined;
  const projected = [];
  for (const source of evidence) {
    if (!source || typeof source !== 'object'
      || !['id', 'title', 'sourceStatus'].every(name => typeof source[name] === 'string'
        && source[name].trim() && source[name].length <= (name === 'title' ? 160 : name === 'id' ? 128 : 64)
        && !/[\u0000-\u001f\u007f]/u.test(source[name]))
      || !(source.observedAt === null || typeof source.observedAt === 'string'
        && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(source.observedAt)
        && Number.isFinite(Date.parse(source.observedAt)) && new Date(source.observedAt).toISOString() === source.observedAt)) return undefined;
    projected.push({ id: source.id, title: source.title, observedAt: source.observedAt, sourceStatus: source.sourceStatus });
  }
  return Object.freeze({ schema: 'soty.personal-agent-turn.v1', status: 'complete', requestId,
    answer: result.answer, contextId: result.contextId, evidence: projected });
}
function recoveryPointers(value) {
  requireValue(Array.isArray(value) && value.length <= 8, 'agent_output_invalid');
  return value.map(entry => {
    fields(entry, ['reference', 'invocationId']); fields(entry.reference, ['capabilityId', 'version']);
    identifier(entry.reference.capabilityId, 160); identifier(entry.invocationId, 160);
    requireValue(Number.isSafeInteger(entry.reference.version) && entry.reference.version >= 1 && entry.reference.version <= 1000000, 'agent_output_invalid');
    return { reference: { capabilityId: entry.reference.capabilityId, version: entry.reference.version }, invocationId: entry.invocationId };
  });
}

/** Callable signed extension. Runtime functions are explicit host imports; no TS donor is dynamically loaded here.
 * outputProfile is local completed-result enforcement, not a claim of upstream forced-tool/json_schema support. */
export function createPersonalAgentModule({ getConnect, runtime, transport, toolsForActor,
  browserPilot, scope = { registryId: 'REG.soty', environmentId: 'production', audienceId: 'personal' }, policy = {}, capacity = {}, clock = Date.now } = {}) {
  fields(scope, ['registryId', 'environmentId', 'audienceId']);
  for (const value of Object.values(scope)) identifier(value, 160);
  const hostScope = freeze({ ...scope }), modelPolicy = boundedPolicy(policy);
  fields(capacity, [], ['maxAccountContexts', 'maxRetainedTurns']);
  const maxAccountContexts = capacity.maxAccountContexts === undefined ? 128 : capacity.maxAccountContexts;
  const maxRetainedTurns = capacity.maxRetainedTurns === undefined ? 128 : capacity.maxRetainedTurns;
  requireValue([maxAccountContexts, maxRetainedTurns].every(value => Number.isSafeInteger(value) && value >= 1 && value <= 65536));
  const configured = typeof getConnect === 'function' && typeof runtime?.runTurn === 'function'
    && typeof runtime?.createContextStore === 'function' && typeof runtime?.adapterFor === 'function'
    && runtime.outputProfile === 'host-enforced-terminal.v1' && typeof toolsForActor === 'function'
    && isPersonalAgentTransport(transport) && transport.configured === true;
  const contexts = new Map(), turns = new Map(), operations = new Set(['agent.turn', 'agent.cancel', 'agent.status', ...(browserPilot?.operations ?? [])]); let closed = false;
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
      requireValue(contexts.size < maxAccountContexts, 'agent_capacity');
      entry = { store: runtime.createContextStore({ roles: ['human'], ttlMs: 1800000, maxContexts: 32 }), active: 0, expiresAt: at + 1800000 };
      contexts.set(key, entry);
    }
    entry.active++; entry.expiresAt = at + 1800000;
    return entry;
  };
  async function executeAsync({ op, args, actor }) {
    requireValue(configured, 'agent_transport_unavailable'); currentActor(actor);
    if (browserPilot?.operations.has(op)) return browserPilot.executeAsync({ op, args, actor });
    if (op === 'agent.status') {
      fields(args, ['requestId']); const turnId = identifier(args.requestId, 100);
      const turn = turns.get(hash({ scope: actorScope(actor), turnId }));
      // Absence after expiry/restart is UNKNOWN, never evidence of no effect.
      const at = now();
      if (!turn || turn.state !== 'running' && turn.retireAfter <= at) {
        return { schema: 'soty.personal-agent-status.v1', requestId: turnId, state: 'unavailable' };
      }
      const progress = { stage: turn.stage, modelRounds: turn.modelRounds, completedTools: turn.completedTools,
        elapsedMs: Math.min(1800000, Math.max(0, at - turn.startedAt)), cancelRequested: turn.abort.signal.aborted };
      let result;
      if (turn.state === 'completed' && turn.result) {
        try { turn.assertRecoveryCurrent(); result = structuredClone(turn.result); }
        catch { return { schema: 'soty.personal-agent-status.v1', requestId: turnId, state: 'unknown', ...progress }; }
      }
      let sourceRecovery;
      if (turn.state === 'unknown' && typeof turn.sourceRecovery === 'function') {
        // Read only under current Native/Root fences; no Source invocation or
        // model request is retried. Unavailable proof remains unknown.
        try { sourceRecovery = recoveryPointers(await turn.sourceRecovery(AbortSignal.timeout(3000))); }
        catch { sourceRecovery = undefined; }
      }
      currentActor(actor);
      return { schema: 'soty.personal-agent-status.v1', requestId: turnId, state: turn.state,
        ...progress, ...(result ? { result } : {}), ...(sourceRecovery?.length ? { sourceRecovery } : {}) };
    }
    if (op === 'agent.cancel') {
      fields(args, ['requestId']); const turnId = identifier(args.requestId, 100);
      const turn = turns.get(hash({ scope: actorScope(actor), turnId }));
      requireValue(turn, 'agent_turn_unavailable');
      const cancelRequested = turn.state === 'running';
      if (cancelRequested) turn.abort.abort();
      return { schema: 'soty.personal-agent-cancel.v1', requestId: turnId, cancelRequested, remoteTerminationVerified: false };
    }
    requireValue(op === 'agent.turn');
    fields(args, ['requestId', 'prompt'], ['contextId', 'selection', 'browser', 'noteCreation']);
    requireValue(args.noteCreation === undefined || args.noteCreation === true);
    requireValue(args.browser === undefined || browserPilot, 'agent_source_denied');
    let selection;
    if (args.selection !== undefined) {
      fields(args.selection, ['appId', 'handle']);
      requireValue(/^app-[a-f0-9]{32}$/u.test(args.selection.appId) && /^[A-Za-z0-9_-]{43}$/u.test(args.selection.handle));
      selection = freeze({ ...args.selection });
      requireValue(args.browser === undefined || args.browser.appId === selection.appId, 'agent_source_denied');
    }
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
    requireValue(turns.size < maxRetainedTurns, 'agent_capacity');
    const turn = { intent, state: 'running', stage: 'prepare', modelRounds: 0, completedTools: 0,
      startedAt: timestamp, receipts: [], abort: new AbortController(), retireAfter: timestamp + 1800000 }; turns.set(key, turn);
    const unsubscribe = getConnect().subscribeRevocations(event => { if (event.deviceId === actor.deviceId) turn.abort.abort(); });
    let bundle, browserTurn, activeProvider, contextEntry, stage = 'prepare';
    const assertCurrent = () => {
      currentActor(actor);
      requireValue(!turn.abort.signal.aborted, 'agent_cancelled');
      if (bundle) requireValue(syncResult(bundle.assertCurrent()) === true, 'agent_source_denied');
      if (browserTurn) requireValue(browserTurn.assertCurrent() === true, 'agent_source_denied');
    };
    // A completed result is owned conversation data. Recovering it needs fresh
    // actor/Source authority, not a browser execution lease already closed in
    // finally. This port cannot execute a browser or Source action.
    turn.assertRecoveryCurrent = () => {
      currentActor(actor);
      if (bundle) requireValue(syncResult(bundle.assertCurrent()) === true, 'agent_source_denied');
      if (browserTurn) requireValue(typeof browserTurn.assertResultAuthority === 'function'
        && syncResult(browserTurn.assertResultAuthority()) === true, 'agent_source_denied');
    };
    try {
      if (args.browser !== undefined) browserTurn = browserPilot.openTurn({ actor, requestId: turnId, binding: args.browser, signal: turn.abort.signal });
      bundle = await toolsForActor({ actor, scope: frameScope, turnId, selection, signal: turn.abort.signal, noteCreation: args.noteCreation === true }); currentActor(actor);
      requireValue(bundle && typeof bundle.assertCurrent === 'function' && bundle.tools && typeof bundle.tools === 'object', 'agent_source_unavailable');
      requireValue(bundle.verifyCurrent === undefined || typeof bundle.verifyCurrent === 'function', 'agent_source_unavailable');
      requireValue(bundle.sourceRecovery === undefined || typeof bundle.sourceRecovery === 'function', 'agent_source_unavailable');
      turn.sourceRecovery = bundle.sourceRecovery;
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
      for (const [name, tool] of Object.entries({ ...bundle.tools, ...(browserTurn?.tools ?? {}) })) {
        requireValue(name !== 'finish_answer' && /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/u.test(name)
          && typeof tool.validate === 'function' && typeof tool.execute === 'function' && tool.inputSchema, 'agent_tool_binding_invalid');
        tools[name] = { ...tool, execute: async (input, signal) => {
          const currentSignal = AbortSignal.any([signal, turn.abort.signal]);
          assertCurrent(); requireValue(!currentSignal.aborted, 'agent_cancelled');
          stage = turn.stage = 'tool';
          const result = await tool.execute(input, currentSignal); assertCurrent(); requireValue(!currentSignal.aborted, 'agent_cancelled'); turn.completedTools++; return result;
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
        activeProvider = createGlmProvider({ transport: bound.complete, tools: registry, responseMode: 'json',
          policy: { ...modelPolicy, ...(options.maxTokens !== undefined ? { maxOutputTokens: Math.min(modelPolicy.maxOutputTokens, options.maxTokens) } : {}) } });
        let result;
        try { stage = turn.stage = 'model'; turn.modelRounds++; result = await activeProvider.complete({ requestId, messages, signal,
          offeredToolNames: Object.keys(offered), parallelToolCalls: options.parallelToolCalls }); }
        catch (error) {
          assertCurrent();
          try { console.error(JSON.stringify({ schema: 'soty.personal-agent-provider-failure.v1', ...providerFailureDiagnostic(error) })); } catch {}
          throw agentError('agent_model_unknown');
        }
        finally { activeProvider.close(); activeProvider = null; }
        assertCurrent();
        stage = turn.stage = 'account';
        const receipt = await bound.account(requestId, result, signal); assertCurrent(); turn.receipts.push(receipt);
        if (bundle.verifyCurrent) { await bundle.verifyCurrent(signal); assertCurrent(); }
        requireValue(result.message.toolCalls.length === 1 && (required === undefined || result.message.toolCalls[0].name === required), 'agent_output_invalid');
        stage = turn.stage = 'runtime';
        return { choices: [{ finish_reason: result.finishReason, message: { content: result.message.content || null,
          tool_calls: result.message.toolCalls.map(call => ({ id: call.id, type: 'function', function: { name: call.name,
            arguments: canonical(call.arguments, modelPolicy.toolArgumentBytes, modelPolicy.jsonDepth) } })) } }] };
      };
      // Keep the complete runtime below the browser's signed-turn transport deadline.
      const result = await runtime.runTurn({ timeoutMs: 90_000, adapter, role: 'human', prompt, contextStore, contextId: args.contextId, tools, provider });
      assertCurrent(); requireValue(result.status === 'complete', 'agent_output_invalid');
      turn.result = recoverableAnswer(result, turnId);
      turn.state = 'completed';
      return { schema: 'soty.personal-agent-turn.v1', requestId: turnId, ...result, accounting: turn.receipts };
    } catch (error) {
      turn.state = 'unknown';
      if (agentErrorCode(error) !== 'agent_model_unknown') {
        try { console.error(JSON.stringify({ schema: 'soty.personal-agent-turn-failure.v1', ...turnFailureDiagnostic(error, stage) })); } catch {}
      }
      throw agentError(agentErrorCode(error));
    }
    finally { browserTurn?.close(); activeProvider?.close(); unsubscribe(); if (contextEntry) contextEntry.active--; }
  }
  return Object.freeze({ operations, executeAsync,
    status: () => Object.freeze({ configured, wholeReady: false, runtimeBound: Boolean(runtime?.runTurn),
      missingBinding: configured ? null : 'current_runtime_tools_and_accounted_gateway', activeTurns: [...turns.values()].filter(value => value.state === 'running').length }),
    close() { closed = true; browserPilot?.close(); for (const turn of turns.values()) if (turn.state === 'running') turn.abort.abort(); },
  });
}
