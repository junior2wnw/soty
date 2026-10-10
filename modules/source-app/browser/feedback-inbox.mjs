import { createSourceFeedbackClient } from './feedback-client.mjs';

/** Uses the existing Source cookie, ticket permissions and mutation receipts. */
export function createSourceFeedbackInbox({ api = createSourceFeedbackClient(), isCurrent = () => true,
  requestId = () => 'feedback-' + crypto.randomUUID(), onChange = () => {} } = {}) {
  let disposed = false, generation = 0, context = null, tickets = [], ticket = null, cursor = null;
  let state = 'idle', pending = null, active = null;
  const drafts = new Map();
  const snapshot = () => ({ state, context, tickets, ticket, nextCursor: cursor, pending: Boolean(pending),
    replyDraft: ticket ? drafts.get(ticket.id) ?? '' : '' });
  const changed = () => { if (!disposed) onChange(snapshot()); };
  const current = gen => !disposed && isCurrent() === true && gen === generation;
  function invalidate() {
    generation++; active?.abort(); active = null; context = ticket = cursor = pending = null;
    tickets = []; drafts.clear(); state = 'login_required'; changed();
  }
  async function sameContext(gen, signal) {
    const value = await api.context(signal);
    if (!current(gen)) return false;
    if (value.ready !== true || value.bindingDigest !== context?.bindingDigest) { invalidate(); return false; }
    return true;
  }
  async function read(action, requireBinding = true) {
    if (disposed || isCurrent() !== true || active || pending) return false;
    const gen = generation, controller = new AbortController(); active = controller; state = 'loading'; changed();
    try {
      if (requireBinding && !await sameContext(gen, controller.signal)) return false;
      const result = await action(controller.signal);
      if (!current(gen)) return false;
      if (!await sameContext(gen, controller.signal)) return false;
      state = 'ready'; changed(); return result;
    } catch (error) {
      if (!current(gen)) return false;
      if ([401, 403].includes(error?.status)) invalidate();
      else { state = 'unavailable'; changed(); }
      return false;
    } finally { if (active === controller) active = null; }
  }
  return Object.freeze({
    snapshot,
    open() {
      return read(async signal => {
        const gen = generation, value = await api.context(signal);
        if (!current(gen)) return false;
        if (context?.bindingDigest !== value.bindingDigest) { tickets = []; ticket = cursor = null; drafts.clear(); }
        context = value;
        if (value.ready !== true) throw new Error('not_ready');
        const page = await api.list({ limit: 20 }, signal);
        if (!current(gen)) return false;
        tickets = page.tickets; cursor = page.nextCursor; return true;
      }, false);
    },
    more() {
      if (!context || !cursor) return Promise.resolve(false);
      return read(async signal => {
        const gen = generation, next = cursor, page = await api.list({ limit: 20, cursor: next }, signal);
        if (!current(gen)) return false;
        const ids = new Set(tickets.map(value => value.id));
        tickets = [...tickets, ...page.tickets.filter(value => !ids.has(value.id))]; cursor = page.nextCursor; return true;
      });
    },
    select(ticketId) {
      if (!context) return Promise.resolve(false);
      return read(async signal => {
        const gen = generation, result = await api.get(ticketId, signal);
        if (!current(gen)) return false;
        ticket = result.ticket; return true;
      });
    },
    setReply(body) {
      if (!ticket || pending || typeof body !== 'string' || body.length > 8000) return false;
      drafts.set(ticket.id, body); while (drafts.size > 20) drafts.delete(drafts.keys().next().value); return true;
    },
    async write(operation, status) {
      if (context?.ready !== true || !ticket || active || disposed || isCurrent() !== true) return false;
      if (!pending) {
        if (operation === 'reply' ? !ticket.canReply : operation === 'status' ? !ticket.canManage : operation === 'accept' ? !ticket.canAccept : true) return false;
        const args = { ticketId: ticket.id, expectedRevision: ticket.revision, requestId: requestId() };
        if (operation === 'reply') { args.body = drafts.get(ticket.id) ?? ''; if (!args.body.trim()) return false; }
        if (operation === 'status') { if (!['in_progress', 'needs_action', 'ready_to_check'].includes(status)) return false; args.status = status; }
        pending = { operation, args: Object.freeze(args), binding: context.bindingDigest };
      }
      const intent = pending, gen = generation, controller = new AbortController();
      if (intent.binding !== context.bindingDigest) return false;
      active = controller; state = 'sending'; changed();
      try {
        if (!await sameContext(gen, controller.signal)) return false;
        const result = await api[intent.operation](intent.args, controller.signal);
        if (!current(gen) || pending !== intent) return false;
        if (!await sameContext(gen, controller.signal)) return false;
        if (result.requestId !== intent.args.requestId || result.ticket.id !== intent.args.ticketId) throw new Error('invalid_receipt');
        ticket = result.ticket; tickets = tickets.map(value => value.id === ticket.id ? ticket : value);
        if (intent.operation === 'reply') drafts.delete(ticket.id);
        pending = null; state = 'ready';
        // Mutation receipts contain media metadata. Read the original bytes
        // again; a failed detail refresh never repeats a confirmed write.
        if (ticket.attachments.some(media => !media.dataBase64)) {
          try {
            const detail = await api.get(ticket.id, controller.signal);
            if (!current(gen) || !await sameContext(gen, controller.signal)) return false;
            if (detail.ticket.id !== ticket.id || detail.ticket.revision < ticket.revision) throw new Error('stale_detail');
            ticket = detail.ticket;
          } catch (cause) {
            if (!current(gen)) return false;
            if ([401, 403].includes(cause?.status)) { invalidate(); return false; }
            state = 'unavailable';
          }
        }
        changed(); return true;
      } catch (error) {
        if (!current(gen) || pending !== intent) return false;
        if ([401, 403].includes(error?.status)) invalidate();
        else if (error?.status === 409) { pending = null; state = 'conflict'; changed(); }
        else { state = 'unknown'; changed(); }
        return false;
      } finally { if (active === controller) active = null; }
    },
    invalidate,
    dispose() { invalidate(); disposed = true; },
  });
}
