import { createHash, randomUUID } from 'node:crypto';
import { validArtifact, copyArtifact } from '../modules/otsov-editor-handoff/wire.mjs';
import { fields, canonical } from '../modules/personal-agent/provider/validation.mjs';
import { GLM_MODEL } from '../modules/personal-agent/provider/index.mjs';
import { VOICE_ORB_MODEL, voiceOrbError, voiceOrbFailure, isVoiceOrbTransport } from './voice-orb-transport.js';

const need = (value, code = 'voice_orb_invalid') => { if (!value) throw voiceOrbError(code); };
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const text = (value, maximum) => typeof value === 'string' && value.isWellFormed() && value.length <= maximum && !value.includes('\0');
const selected = value => value && Object.keys(value).sort().join(',') === 'appId,handle'
  && /^app-[a-f0-9]{32}$/u.test(value.appId) && /^[A-Za-z0-9_-]{43}$/u.test(value.handle);
const hash = value => createHash('sha256').update(canonical(value)).digest('hex');
const usageProjection = value => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const result = {};
  for (const key of ['total_tokens', 'input_tokens', 'output_tokens'])
    if (Number.isSafeInteger(value[key]) && value[key] >= 0) result[key] = value[key];
  return Object.keys(result).length ? result : null;
};

/** Host-only composition. The installed session quota lease MUST durably claim the
 * actor/request/intent once across process restarts before any provider POST.
 * Unknown usage does not return a reserved start. Response token observations are not a final
 * invoice; a confirmed hangup is not confirmed billing. No KUF binding reused. */
export function createVoiceOrbComposition({ getConnect, agent, configuration = {} } = {}) {
  fields(configuration, [], ['enabled', 'transport', 'admitQuota', 'quotaStatus', 'maxDurationMs', 'connectProjectId']);
  need(configuration.connectProjectId === undefined || configuration.connectProjectId === 'soty');
  need(typeof getConnect === 'function' && typeof agent?.executeAsync === 'function' && typeof agent?.status === 'function');
  need(configuration.enabled === undefined || typeof configuration.enabled === 'boolean');
  const maxDurationMs = configuration.maxDurationMs ?? 180000;
  need(Number.isSafeInteger(maxDurationMs) && maxDurationMs >= 15000 && maxDurationMs <= 900000);
  const configured = configuration.enabled === true && isVoiceOrbTransport(configuration.transport) && typeof configuration.admitQuota === 'function';
  const missingBinding = configuration.enabled !== true ? 'not_enabled' : !isVoiceOrbTransport(configuration.transport)
    ? 'voice_transport_not_bound' : typeof configuration.admitQuota !== 'function' ? 'voice_quota_not_bound' : null;
  const sessions = new Map(), attempts = new Map(); let closed = false;
  const operations = new Set(['voice.orb.status', 'voice.orb.start', 'voice.orb.turn', 'voice.orb.cancel', 'voice.orb.end']);
  const actorKey = actor => canonical({ accountId: actor.accountId, deviceId: actor.deviceId });
  const current = actor => {
    need(!closed, 'voice_orb_unavailable'); const connect = getConnect();
    // V2 admission names the existing Connect application namespace, NOT the
    // operator's OpenAI proj_* value or a user-data tenant from RPC arguments.
    need(configuration.connectProjectId === undefined || connect.projectId === configuration.connectProjectId, 'voice_orb_denied');
    connect.withActorAuthorityFence(actor, () => true);
  };
  const modelEligible = actor => {
    if (configuration.connectProjectId === undefined) return true; // V1 behavior.
    try {
      if (typeof agent.modelEligible !== 'function') return false;
      const result = agent.modelEligible(actor);
      // The installed port is synchronous. Unexpected async adapters never
      // grant admission or leave a rejected Promise unobserved.
      if (result && typeof result.then === 'function') { void Promise.resolve(result).catch(() => {}); return false; }
      return result === true;
    } catch { return false; }
  };
  const status = () => Object.freeze({ schema: 'soty.voice-orb-status.v1', configured: !closed && configured && agent.status()?.configured === true,
    missingBinding: closed ? 'closed' : missingBinding, voiceModel: VOICE_ORB_MODEL, brainModel: GLM_MODEL,
    delegation: 'completed_function_call', wholeVoiceReady: false, paidActivationVerified: false });
  const own = (actor, sessionId) => { current(actor); const frame = sessions.get(sessionId);
    need(frame && frame.actorKey === actorKey(actor), 'voice_orb_denied'); return frame; };
  const active = frame => { current(frame.actor); need(frame.state === 'active' && !frame.abort.signal.aborted
    && Date.now() < frame.expiresAt && frame.quota.assertCurrent() === true, 'voice_orb_expired'); };
  const cancel = async frame => {
    const turn = frame.turn; if (!turn || turn.settled) return { cancelRequested: false, remoteTerminationVerified: false };
    turn.cancelled = true;
    try { return await agent.executeAsync({ op: 'agent.cancel', args: { requestId: turn.requestId }, actor: frame.actor }); }
    catch { return { cancelRequested: true, remoteTerminationVerified: false }; }
  };
  function recordOutcome(frame, proof) {
    if (!frame.outcomeRecording) frame.outcomeRecording = Promise.resolve().then(() => frame.quota?.recordOutcome(Object.freeze({
      voiceModel: VOICE_ORB_MODEL, transportAttempted: frame.transportAttempted, providerCallId: frame.remote?.callId ?? frame.providerCallId ?? null,
      hangupAccepted: proof.hangupAccepted, terminationVerified: proof.terminationVerified, accountingFinalized: false, responseUsage: [...frame.usage.values()], reason: proof.reason,
    }))).catch(() => { frame.quotaRecording = 'unknown'; });
    return frame.outcomeRecording;
  }
  function stop(frame, reason) {
    if (frame.stopping) return frame.stopping;
    frame.state = 'closing'; clearTimeout(frame.timer); frame.abort.abort(); frame.unsubscribe?.();
    frame.stopping = (async () => {
      await cancel(frame);
      if (frame.admission) { try { frame.quota = await frame.admission; } catch {} }
      if (frame.transportResult) { try { frame.remote = await frame.transportResult; } catch {} }
      let proof = { hangupAccepted: frame.hangupAccepted === true, terminationVerified: false, accountingFinalized: false, reason };
      if (frame.remote) { try { proof = await frame.remote.close(); } catch {} }
      frame.proof = proof; frame.state = 'ended'; // Owned resources ended; proof still records unknown remote/accounting.
      await recordOutcome(frame, proof);
      return { schema: 'soty.voice-orb-ended.v1', sessionId: frame.id, hangupAccepted: proof.hangupAccepted, terminationVerified: proof.terminationVerified,
        accountingFinalized: false, remoteTerminationVerified: proof.terminationVerified, playbackVerified: false };
    })(); return frame.stopping;
  }
  function compact() {
    // This RAM cache is not the paid-start dedupe ledger. Durable claimTransport
    // still rejects an old actor/request/intent after cache eviction or restart.
    for (const [key, attempt] of attempts) {
      if (attempts.size < 128) break;
      if (['ended'].includes(attempt.frame.state)) { attempts.delete(key); sessions.delete(attempt.frame.id); }
    }
  }
  function providerEvent(frame, event) {
    if (event.type === 'transport.hangup') { frame.hangupAccepted = event.hangupAccepted === true; return; }
    if (event.type === 'transport.initialized') { need(id(event.callId)); frame.providerCallId = event.callId; return; }
    if (event.type === 'transport.lost' || event.type === 'transport.error') { void stop(frame, 'provider_lost'); return; }
    if (!['starting', 'active'].includes(frame.state)) return;
    if (event.type === 'input_audio_buffer.cleared') { frame.clearAwaiting = false; frame.requireFreshSpeech = true; return; }
    if (event.type === 'input_audio_buffer.speech_started') {
      frame.inputGeneration++; frame.speaking = true;
      if (!frame.clearAwaiting) frame.requireFreshSpeech = false;
      void cancel(frame); if (frame.remote) try { frame.remote.interrupt(); } catch {} return;
    }
    if (event.type === 'input_audio_buffer.committed') {
      need(id(event.item_id), 'voice_orb_unknown');
      if (frame.userItems.has(event.item_id)) return;
      need(frame.userItems.size < 128, 'voice_orb_unknown'); frame.userItems.add(event.item_id);
      if (frame.clearAwaiting || frame.requireFreshSpeech) return;
      frame.latestUserItem = event.item_id; frame.speaking = false;
      const token = randomUUID();
      frame.issued.set(token, { userItemId: event.item_id, inputGeneration: frame.inputGeneration, controlGeneration: frame.controlGeneration });
      // Automatic model responses are disabled. Only this application-issued
      // token binds a completed structured task to THIS committed user input.
      need(frame.remote && frame.state === 'active', 'voice_orb_unknown');
      frame.remote.requestUserResponse({ token, userItemId: event.item_id }); return;
    }
    if (['conversation.item.added', 'conversation.item.done'].includes(event.type)) {
      if (event.item?.type === 'function_call_output' && frame.turn?.callId === event.item.call_id && frame.turn.outputDigest
        && text(event.item.output, 16000) && hash(JSON.parse(event.item.output)) === frame.turn.outputDigest)
        frame.turn.delivery = 'function_output_ack';
      return;
    }
    if (event.type !== 'response.done') return;
    const response = event.response;
    need(id(response?.id) && Array.isArray(response.output), 'voice_orb_unknown');
    const digest = hash({ status: response.status, output: response.output, token: response.metadata?.soty_turn ?? null, usage: usageProjection(response.usage) });
    const prior = frame.responses.get(response.id);
    if (prior) { need(prior === digest, 'voice_orb_unknown'); return; }
    need(frame.responses.size < 128, 'voice_orb_unknown'); frame.responses.set(response.id, digest);
    const usage = usageProjection(response.usage); if (usage) frame.usage.set(response.id, { responseId: response.id, ...usage });
    const functions = response.output.filter(item => item.type === 'function_call');
    if (!functions.length || response.status !== 'completed') return;
    need(functions.length === 1, 'voice_orb_unknown'); const call = functions[0];
    need(call.status === 'completed' && call.name === 'delegate_to_agent' && id(call.call_id)
      && text(call.arguments, 16000) && Buffer.byteLength(call.arguments) <= 32000, 'voice_orb_unknown');
    const args = JSON.parse(call.arguments); fields(args, ['task'], ['noteIntent']);
    need(text(args.task, 8000) && args.task.trim() && Buffer.byteLength(args.task) <= 16000, 'voice_orb_unknown');
    need(args.noteIntent === undefined || args.noteIntent === 'create_my_note', 'voice_orb_unknown');
    const issued = frame.issued.get(response.metadata?.soty_turn);
    need(issued && (!issued.responseId || issued.responseId === response.id), 'voice_orb_expired');
    issued.responseId = response.id;
    // Known late completion from an interrupted input is expected protocol
    // traffic. Ignore its task instead of ending the new conversation.
    if (issued.userItemId !== frame.latestUserItem || issued.inputGeneration !== frame.inputGeneration
      || issued.controlGeneration !== frame.controlGeneration || frame.speaking || frame.clearAwaiting || frame.requireFreshSpeech) return;
    const noteIntent = args.noteIntent === 'create_my_note' ? args.noteIntent : undefined;
    const intent = hash({ responseId: response.id, task: args.task, userItemId: frame.latestUserItem, ...(noteIntent ? { noteIntent } : {}) });
    if (frame.calls.has(call.call_id)) { need(frame.calls.get(call.call_id).intent === intent, 'voice_orb_unknown'); return; }
    // No recursive GLM/tool work from the speech response to a previous tool.
    // Every new delegation consumes one newly committed user input item.
    need(!frame.speaking && frame.latestUserItem && frame.latestUserItem !== frame.consumedUserItem, 'voice_orb_unknown');
    frame.consumedUserItem = frame.latestUserItem;
    frame.calls.set(call.call_id, { callId: call.call_id, task: args.task, ...(noteIntent ? { noteIntent } : {}), intent, userItemId: frame.latestUserItem, inputGeneration: frame.inputGeneration, controlGeneration: frame.controlGeneration });
  }
  async function taskFor(frame, callId) {
    const deadline = Date.now() + 1500;
    while (true) { active(frame); const call = frame.calls.get(callId);
      if (call) { need(!frame.speaking && call.userItemId === frame.latestUserItem && call.inputGeneration === frame.inputGeneration && call.controlGeneration === frame.controlGeneration, 'voice_orb_expired'); return call; }
      need(Date.now() < deadline, 'voice_orb_task_pending'); await new Promise(resolve => setTimeout(resolve, 50));
    }
  }
  return Object.freeze({ operations, status,
    async executeAsync({ op, args, actor }) {
      current(actor);
      if (op === 'voice.orb.status') {
        fields(args, [], ['sessionId', 'requestId']); need(args.sessionId === undefined || args.requestId === undefined);
        need(args.sessionId === undefined || id(args.sessionId)); need(args.requestId === undefined || id(args.requestId));
        if (args.sessionId === undefined && args.requestId === undefined) {
          if (!modelEligible(actor)) return { ...status(), canStart: false };
          const quota = configuration.quotaStatus?.({ actor });
          return { ...status(), ...(quota ? { quota, canStart: quota.allowed === true } : {}) };
        }
        const located = args.requestId === undefined ? args.sessionId : attempts.get(actorKey(actor) + ':' + args.requestId)?.frame.id;
        need(located, 'voice_orb_unavailable'); const frame = own(actor, located); let task = null;
        const observedTurn = frame.turn && Object.freeze({ requestId: frame.turn.requestId, callId: frame.turn.callId, delivery: frame.turn.delivery });
        if (observedTurn) { const observed = await agent.executeAsync({ op: 'agent.status', args: { requestId: observedTurn.requestId }, actor }); current(actor);
          task = { callId: observedTurn.callId, state: observed.state, ...(observed.result ? { result: observed.result } : {}),
            delivery: observedTurn.delivery, playbackVerified: false }; }
        return { ...status(), sessionId: frame.id, state: frame.state, task, hangupAccepted: frame.proof?.hangupAccepted ?? false, terminationVerified: frame.proof?.terminationVerified ?? false, accountingFinalized: false };
      }
      need(configured && !closed && agent.status()?.configured === true, 'voice_orb_unavailable');
      if (op === 'voice.orb.start') {
        fields(args, ['requestId', 'sdp'], ['notesMode']); need(args.notesMode === undefined || args.notesMode === true);
        need(id(args.requestId) && args.requestId.length >= 8 && text(args.sdp, 65536)
          && args.sdp.trim() && Buffer.byteLength(args.sdp) <= 65536);
        const start = Object.freeze({ requestId: args.requestId, sdp: args.sdp, ...(args.notesMode === true ? { notesMode: true } : {}) });
        need(modelEligible(actor), 'voice_orb_denied');
        const key = actorKey(actor) + ':' + start.requestId, digest = hash(start), previous = attempts.get(key);
        if (previous) throw voiceOrbError(previous.digest === digest ? 'voice_orb_reconcile_required' : 'voice_orb_intent_conflict');
        compact(); need(attempts.size < 128 && ![...sessions.values()].some(row => row.actorKey === actorKey(actor) && row.state !== 'ended'), 'voice_orb_busy');
        const frame = { id: randomUUID(), actor, actorKey: actorKey(actor), notesMode: start.notesMode === true, abort: new AbortController(), state: 'starting', transportAttempted: false,
          calls: new Map(), responses: new Map(), issued: new Map(), usage: new Map(), userItems: new Set(), latestUserItem: null, consumedUserItem: null,
          inputGeneration: 0, controlGeneration: 0, speaking: false, requireFreshSpeech: false, clearAwaiting: false, turns: new Map(), turn: null, contextId: null, expiresAt: Date.now() + maxDurationMs };
        attempts.set(key, { digest, frame }); sessions.set(frame.id, frame);
        frame.timer = setTimeout(() => { void stop(frame, 'duration_limit'); }, maxDurationMs); frame.timer.unref?.();
        frame.unsubscribe = getConnect().subscribeRevocations(event => { if (event.deviceId === actor.deviceId) void stop(frame, 'identity_revoked'); });
        try {
          frame.admission = Promise.resolve().then(() => configuration.admitQuota({ actor, requestId: start.requestId, intentDigest: digest,
            sessionId: frame.id, voiceModel: VOICE_ORB_MODEL, brainModel: GLM_MODEL, maxDurationMs, signal: frame.abort.signal }));
          frame.quota = await frame.admission; current(actor);
          need(!frame.abort.signal.aborted && frame.state === 'starting', 'voice_orb_expired');
          need(modelEligible(actor), 'voice_orb_denied');
          need(frame.quota && typeof frame.quota.assertCurrent === 'function' && typeof frame.quota.claimTransport === 'function'
            && typeof frame.quota.recordOutcome === 'function' && frame.quota.assertCurrent() === true, 'voice_orb_denied');
          need(await frame.quota.claimTransport() === true, 'voice_orb_reconcile_required'); current(actor);
          need(!frame.abort.signal.aborted && frame.state === 'starting', 'voice_orb_expired');
          frame.transportAttempted = true;
          frame.transportResult = Promise.resolve().then(() => {
            if (configuration.connectProjectId !== undefined) { current(actor); need(modelEligible(actor), 'voice_orb_denied'); }
            return configuration.transport.start({ actor, sdp: start.sdp, signal: frame.abort.signal,
              onEvent: event => { try { providerEvent(frame, event); } catch { void stop(frame, 'provider_event_invalid'); } } });
          });
          frame.remote = await frame.transportResult; current(actor);
          need(!frame.abort.signal.aborted && frame.quota.assertCurrent() === true, 'voice_orb_expired'); frame.state = 'active';
          return { schema: 'soty.voice-orb-started.v1', sessionId: frame.id, providerCallId: frame.remote.callId, providerSessionId: frame.remote.sessionId,
            sdp: frame.remote.sdp, voiceModel: VOICE_ORB_MODEL, brainModel: GLM_MODEL, expiresAt: frame.expiresAt, remainingDurationMs: Math.max(0, frame.expiresAt - Date.now()), playbackVerified: false };
        } catch { await stop(frame, 'startup_unknown'); throw voiceOrbError('voice_orb_unknown'); }
      }
      fields(args, op === 'voice.orb.turn' ? ['sessionId', 'callId'] : ['sessionId'], op === 'voice.orb.turn' ? ['selection', 'noteCreation'] : []);
      need(id(args.sessionId)); const frame = own(actor, args.sessionId);
      if (op === 'voice.orb.end') return stop(frame, 'owner_requested');
      if (op === 'voice.orb.cancel') { frame.controlGeneration++; frame.consumedUserItem = frame.latestUserItem;
        frame.requireFreshSpeech = true; frame.clearAwaiting = true; const result = await cancel(frame); current(actor);
        if (frame.remote && frame.state === 'active') try { frame.remote.interrupt({ clearInput: true }); } catch {}
        return { schema: 'soty.voice-orb-cancel.v1', sessionId: frame.id, cancelRequested: result.cancelRequested === true,
          remoteTerminationVerified: false, playbackVerified: false }; }
      need(op === 'voice.orb.turn' && id(args.callId)); need(args.noteCreation === undefined || args.noteCreation === true);
      if (args.selection !== undefined) fields(args.selection, ['appId', 'handle']);
      const turnArgs = Object.freeze({ sessionId: args.sessionId, callId: args.callId,
        ...(args.selection ? { selection: Object.freeze({ appId: args.selection.appId, handle: args.selection.handle }) } : {}),
        ...(args.noteCreation === true ? { noteCreation: true } : {}) });
      need(turnArgs.selection === undefined || selected(turnArgs.selection));
      active(frame); need(!frame.taskUnknown, 'voice_orb_reconcile_required'); need(!frame.turn || frame.turn.settled, 'voice_orb_busy');
      need(!frame.turns.has(turnArgs.callId), 'voice_orb_reconcile_required'); const call = await taskFor(frame, turnArgs.callId); active(frame);
      need(!frame.speaking && !frame.clearAwaiting && !frame.requireFreshSpeech && call.userItemId === frame.latestUserItem && call.inputGeneration === frame.inputGeneration
        && call.controlGeneration === frame.controlGeneration, 'voice_orb_expired');
      need(!frame.taskUnknown, 'voice_orb_reconcile_required');
      need(!frame.turn || frame.turn.settled, 'voice_orb_busy'); need(!frame.turns.has(turnArgs.callId), 'voice_orb_reconcile_required');
      // A model labels the current request; only this actor's signed session opt-in
      // or the existing explicit one-turn permission can authorize one new Note.
      const noteCreation = turnArgs.noteCreation === true || (frame.notesMode && call.noteIntent === 'create_my_note');
      const requestId = 'voice_' + hash({ sessionId: frame.id, callId: turnArgs.callId }).slice(0, 64);
      const turn = { requestId, callId: turnArgs.callId, cancelled: false, settled: false, delivery: 'not_sent' };
      frame.turns.set(turnArgs.callId, turn); frame.turn = turn;
      try {
        active(frame);
        const result = await agent.executeAsync({ op: 'agent.turn', actor, args: { requestId, prompt: call.task,
          ...(frame.contextId ? { contextId: frame.contextId } : {}), ...(turnArgs.selection ? { selection: turnArgs.selection } : {}), ...(noteCreation ? { noteCreation: true } : {}) } });
        active(frame); need(!turn.cancelled && frame.turn === turn && call.inputGeneration === frame.inputGeneration && call.controlGeneration === frame.controlGeneration, 'voice_orb_expired');
        need(result.schema === 'soty.personal-agent-turn.v1' && result.status === 'complete' && text(result.answer, 8000)
          && result.answer.trim() && Array.isArray(result.accounting) && result.accounting.length > 0, 'voice_orb_unknown');
        // Only the owned personal-agent host may supply this typed committed artifact.
        // Browser captions, function arguments and arbitrary Source evidence never mint it.
        const editorHandoff = result.editorHandoff;
        need(editorHandoff === undefined || validArtifact(editorHandoff) && result.requestId === requestId
          && (result.ok === undefined || result.ok === true), 'voice_orb_unknown');
        frame.contextId = result.contextId; turn.outputDigest = hash({ status: 'complete', answer: result.answer });
        turn.delivery = 'function_output_attempted'; frame.remote.deliver({ callId: turnArgs.callId, answer: result.answer });
        return { schema: 'soty.voice-orb-turn.v1', sessionId: frame.id, callId: turnArgs.callId, answer: result.answer,
          contextId: result.contextId, delivery: turn.delivery, playbackVerified: false,
          ...(editorHandoff ? { requestId, editorHandoff: copyArtifact(editorHandoff) } : {}) };
      } catch (error) {
        // A cancelled call remains spent in turns, with no verified outcome.
        // Only fresh committed speech can issue a different current call. Other
        // unknown failures still close task dispatch for this conversation.
        if (!turn.cancelled) frame.taskUnknown = true;
        throw error;
      } finally { turn.settled = true; }
    },
    async close() { if (closed) return; closed = true; await Promise.allSettled([...sessions.values()].map(frame => stop(frame, 'host_closed'))); },
  });
}
