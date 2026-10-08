import { createHash } from 'node:crypto';
import { setTimeout as wait } from 'node:timers/promises';
import { GLM_MODEL } from '../modules/personal-agent/provider/index.mjs';
import { canonical, fields, identifier } from '../modules/personal-agent/provider/validation.mjs';

const errors = new WeakMap(), transports = new WeakSet();
const SAFE_CODES = new Set(['agent_unavailable', 'agent_invalid_input', 'agent_gateway_configuration_invalid', 'agent_transport_unavailable',
  'agent_model_access_denied', 'agent_cancelled', 'agent_reconcile_required', 'agent_intent_conflict', 'agent_round_limit', 'agent_key_binding_invalid',
  'agent_accounting_unknown', 'agent_current_port_async', 'agent_closed', 'agent_actor_denied', 'agent_capacity', 'agent_source_denied',
  'agent_source_unavailable', 'agent_tool_binding_invalid', 'agent_output_profile_invalid', 'agent_model_unknown', 'agent_output_invalid', 'agent_turn_unavailable']);
export function agentError(value) { const code = SAFE_CODES.has(value) ? value : 'agent_unavailable';
  const error = new Error(code); errors.set(error, code); error.code = code; return error; }
export function agentErrorCode(error) { return errors.get(error) ?? 'agent_unavailable'; }
export function isPersonalAgentTransport(value) { return transports.has(value); }
const requireValue = (ok, code = 'agent_invalid_input') => { if (!ok) throw agentError(code); };
const uuid = value => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(value);

/** Host-only fixed billing binding. This uses the existing commerce POST, never its raw connector.complete().
 * readReceipt must be a current tenant-scoped Source reader; an inference key cannot read /api/requests.
 * resolveCredential/authorizeActor/readReceipt are installed code, not JSON or declarations of authority. */
export function createPersonalAgentTransport({ gatewayOrigin, billingTenant, keyRef, resolveCredential,
  readReceipt, authorizeActor, fetchImpl = fetch, allowLoopbackFixture = false, receiptWaitMs = 3000 } = {}) {
  requireValue(Number.isSafeInteger(receiptWaitMs) && receiptWaitMs >= 1 && receiptWaitMs <= 3000, 'agent_gateway_configuration_invalid');
  const missing = ['gatewayOrigin', 'billingTenant', 'keyRef', 'resolveCredential', 'readReceipt', 'authorizeActor']
    .filter(name => ({ gatewayOrigin, billingTenant, keyRef, resolveCredential, readReceipt, authorizeActor })[name] === undefined);
  const configured = missing.length === 0;
  let endpoint;
  if (configured) {
    requireValue(typeof gatewayOrigin === 'string', 'agent_gateway_configuration_invalid');
    let origin; try { origin = new URL(gatewayOrigin); } catch { throw agentError('agent_gateway_configuration_invalid'); }
    requireValue(origin.origin === gatewayOrigin && !origin.username && !origin.password && !origin.search && !origin.hash
      && (origin.protocol === 'https:' || (allowLoopbackFixture === true && origin.protocol === 'http:' && ['127.0.0.1', 'localhost'].includes(origin.hostname))), 'agent_gateway_configuration_invalid');
    identifier(billingTenant, 160);
    requireValue(keyRef !== null && (typeof keyRef === 'object' || typeof keyRef === 'function')
      && [resolveCredential, readReceipt, authorizeActor, fetchImpl].every(value => typeof value === 'function'), 'agent_gateway_configuration_invalid');
    endpoint = `${gatewayOrigin}/v1/chat/completions`;
  }
  let closed = false;
  function bind({ actor, scope, turnId, assertCurrent }) {
    requireValue(configured && !closed, 'agent_transport_unavailable');
    requireValue(typeof assertCurrent === 'function');
    const rounds = new Map();
    const guard = () => {
      requireValue(!closed, 'agent_transport_unavailable'); assertCurrent();
      requireValue(authorizeActor({ actor, billingTenant, keyRef, model: GLM_MODEL }) === true, 'agent_model_access_denied');
    };
    const keyFor = requestId => `soty-agent-${createHash('sha256').update(canonical({ scope, turnId, requestId })).digest('hex')}`;
    async function complete(request) {
      guard(); fields(request, ['requestId', 'requestDigest', 'body', 'signal']);
      identifier(request.requestId, 160);
      requireValue(request.body.model === GLM_MODEL && request.body.stream === true && request.signal instanceof AbortSignal);
      requireValue(!request.signal.aborted, 'agent_cancelled');
      const previous = rounds.get(request.requestId);
      if (previous) throw agentError(previous.digest === request.requestDigest ? 'agent_reconcile_required' : 'agent_intent_conflict');
      requireValue(rounds.size < 8, 'agent_round_limit');
      const round = { digest: request.requestDigest, state: 'unknown', requestId: null }; rounds.set(request.requestId, round);
      let response;
      try {
        const credential = await resolveCredential(keyRef); guard();
        requireValue(credential && credential.keyRef === keyRef && credential.tenantId === billingTenant
          && typeof credential.bearer === 'string' && /^[^\s\u0000-\u001f]{20,512}$/u.test(credential.bearer), 'agent_key_binding_invalid');
        requireValue(!request.signal.aborted, 'agent_cancelled');
        response = await fetchImpl(endpoint, { method: 'POST', redirect: 'error', signal: request.signal,
          headers: { 'Content-Type': 'application/json', Accept: 'text/event-stream', Authorization: `Bearer ${credential.bearer}`,
            'Idempotency-Key': keyFor(request.requestId) }, body: JSON.stringify({ ...request.body, autoFallback: false }) });
        guard();
        if (request.signal.aborted) { void response.body?.cancel().catch(() => {}); throw agentError('agent_cancelled'); }
        const reservedId = response.headers.get('x-soty-request-id');
        if (response.status === 200) { requireValue(uuid(reservedId), 'agent_accounting_unknown'); round.requestId = reservedId; }
        return response;
      } catch (error) {
        try { await response?.body?.cancel(); } catch { /* Cancellation is not evidence of no provider work. */ }
        throw agentError(agentErrorCode(error));
      }
    }
    async function account(requestId, result, signal) {
      guard(); const round = rounds.get(requestId);
      requireValue(round?.requestId && round.digest === result.requestDigest, 'agent_accounting_unknown');
      try {
        // Gateway ends the SSE body before its finish() transaction commits. Only readonly receipt queries may wait.
        const deadlineSignal = AbortSignal.any([AbortSignal.timeout(receiptWaitMs), ...(signal ? [signal] : [])]);
        let proof;
        for (let attempt = 0; attempt < 8; attempt++) {
          guard(); requireValue(!deadlineSignal.aborted, 'agent_accounting_unknown');
          proof = await readReceipt({ billingTenant, keyRef, requestId: round.requestId, signal: deadlineSignal }); guard();
          fields(proof, ['tenantId', 'request']); requireValue(proof.tenantId === billingTenant, 'agent_key_binding_invalid');
          if (proof.request.id !== round.requestId || proof.request.status !== 'running') break;
          if (attempt === 7) throw agentError('agent_accounting_unknown');
          await wait(Math.min(50 * 2 ** attempt, 800), undefined, { signal: deadlineSignal });
        }
        requireValue(!signal?.aborted, 'agent_cancelled');
        // The approved Source reader enforces tenant/key scope; this closed receipt is not an actor/grant.
        fields(proof, ['tenantId', 'request']);
        requireValue(proof.tenantId === billingTenant, 'agent_key_binding_invalid');
        const receipt = proof.request;
        requireValue(receipt.id === round.requestId && receipt.model_requested === GLM_MODEL && receipt.model_resolved === GLM_MODEL
          && ['completed', 'usage_pending'].includes(receipt.status), 'agent_accounting_unknown');
        requireValue(typeof receipt.charge_urub === 'string' && /^\d{1,30}$/u.test(receipt.charge_urub), 'agent_accounting_unknown');
        const reported = result.usage.status === 'reported';
        requireValue(receipt.status === 'usage_pending' ? !reported
          : reported && receipt.input_tokens === result.usage.promptTokens && receipt.output_tokens === result.usage.completionTokens, 'agent_accounting_unknown');
        round.state = receipt.status;
        return Object.freeze({ status: receipt.status, usage: result.usage,
          chargeMicrorubles: receipt.status === 'completed' ? receipt.charge_urub : null });
      } catch (error) {
        guard();
        throw agentError(signal?.aborted ? 'agent_cancelled' : errors.has(error) ? agentErrorCode(error) : 'agent_accounting_unknown');
      }
    }
    return Object.freeze({ complete, account, guard });
  }
  const value = Object.freeze({ configured, missing: Object.freeze(missing), bind, close() { closed = true; } });
  transports.add(value); return value;
}
