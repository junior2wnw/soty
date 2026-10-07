import { createSourceFeedbackClient } from './feedback-client.mjs';

const copy = value => JSON.parse(JSON.stringify(value));
const randomId = () => 'feedback-' + crypto.randomUUID();
const freeze = value => { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; };
const denied = error => [401, 403].includes(error?.status) || ['source_app_native_access_denied', 'source_app_profile_changed'].includes(error?.code);
/** Portable view-independent controller. The Source server decides current
 * Native resource/recipient/roles. A changing context never moves a draft. */
export function createSourceFeedbackController({ api = createSourceFeedbackClient(), isCurrent = () => true, requestId = randomId, onChange = () => {} } = {}) {
  let disposed = false, generation = 0, context = null, pending = null, error = null, state = 'idle', ticket = null;
  const drafts = new Map(), active = new Set();
  const current = (gen = generation, binding = context?.bindingDigest) => !disposed && isCurrent() === true && gen === generation && binding === context?.bindingDigest;
  const snapshot = () => Object.freeze({ state, context: context ? copy(context) : null, ticket: ticket ? copy(ticket) : null,
    draft: context ? copy(drafts.get(context.bindingDigest) ?? { body: '', attachments: [] }) : { body: '', attachments: [] },
    pending: Boolean(pending), error: error?.code ?? null });
  const changed = () => { if (!disposed) onChange(snapshot()); };
  function abort() { active.forEach(controller => controller.abort()); active.clear(); }
  return Object.freeze({
    snapshot,
    async open() {
      const gen = ++generation; abort(); state = 'loading'; error = null; changed();
      const controller = new AbortController(); active.add(controller);
      try {
        const value = await api.context(controller.signal);
        if (disposed || isCurrent() !== true || gen !== generation) return false;
        if (value?.schema !== 'soty.source-feedback.context.v1' || typeof value.bindingDigest !== 'string'
          || !/^[a-f0-9]{64}$/u.test(value.bindingDigest) || typeof value.ready !== 'boolean' || typeof value.recipientLabel !== 'string')
          throw Object.assign(new Error('source_feedback_response_invalid'), { code: 'source_feedback_response_invalid' });
        if (context?.bindingDigest !== value.bindingDigest) { pending = null; ticket = null; }
        context = copy(value); state = value.ready ? 'ready' : 'not_ready'; changed(); return value.ready;
      } catch (cause) { if (!disposed && gen === generation) { error = cause; state = denied(cause) ? 'login_required' : 'unknown'; changed(); } return false; }
      finally { active.delete(controller); }
    },
    setDraft(draft) {
      if (!current() || !context || pending) return false;
      if (typeof draft?.body !== 'string' || draft.body.length > 8000 || !Array.isArray(draft.attachments) || draft.attachments.length > 3) return false;
      drafts.set(context.bindingDigest, copy(draft)); while (drafts.size > 8) drafts.delete(drafts.keys().next().value); changed(); return true;
    },
    async send() {
      if (!current() || state === 'sending' || context?.ready !== true || context?.canSubmit !== true) return false;
      const gen = generation, binding = context.bindingDigest;
      if (!pending) pending = freeze({ binding, args: { requestId: requestId(), ...copy(drafts.get(binding) ?? { body: '', attachments: [] }) } });
      if (pending.binding !== binding) return false;
      const intent = pending, controller = new AbortController(); active.add(controller); state = 'sending'; error = null; changed();
      try {
        const result = await api.submit(intent.args, controller.signal);
        if (!current(gen, binding) || pending !== intent) return false;
        if (!result?.ticket || result.requestId !== intent.args.requestId) throw Object.assign(new Error('source_feedback_response_invalid'), { code: 'source_feedback_response_invalid' });
        ticket = copy(result.ticket); drafts.set(binding, { body: '', attachments: [] }); pending = null; state = 'received'; changed(); return true;
      } catch (cause) {
        if (!current(gen, binding) || pending !== intent) return false;
        error = cause;
        if (denied(cause)) { pending = null; state = 'login_required'; }
        else state = cause?.status === 409 ? 'conflict' : 'unknown';
        changed(); return false;
      } finally { active.delete(controller); }
    },
    invalidate() { generation++; abort(); context = null; ticket = null; pending = null; state = 'login_required'; changed(); },
    dispose() { disposed = true; generation++; abort(); context = null; ticket = null; pending = null; drafts.clear(); },
  });
}
