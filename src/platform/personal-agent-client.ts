type Client = {
  getLocalState(): Promise<{ accountId?: string | null }>;
  extension<T>(op: string, args: Record<string, unknown>, options: { expectedAccountId: string }): Promise<T>;
};
type Request = { requestId: string; prompt: string; contextId?: string };
type Saved = { schema: 'soty.personal-agent-ui.v1'; draft: string; pending: Request[]; active?: string; contextId?: string };
export type PersonalAgentView = { phase: 'draft' | 'running' | 'complete' | 'unknown' | 'cancel_requested'; draft: string;
  contextId?: string; requestId?: string; answer?: string; unconfirmed: number };
const context = (value: unknown): value is string => typeof value === 'string' && /^ctx_[A-Za-z0-9_-]{16,124}$/u.test(value);
const requestId = (value: unknown): value is string => typeof value === 'string' && /^[A-Za-z0-9_.:-]{8,100}$/u.test(value);
const own = (value: unknown, names: string[]): value is Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) return false;
  const descriptors = Object.getOwnPropertyDescriptors(value);
  return Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && names.includes(key)
    && descriptors[key]!.enumerable && 'value' in descriptors[key]!);
};
const failure = (code: string) => Object.assign(new Error(code), { code });

/** Browser intent only. The signed Connect server independently authenticates, authorizes, and accounts every turn.
 * Unknown sends are retained; neither refresh nor recovery automatically repeats a model POST. */
export function createPersonalAgentConversation({ accountId, client, storage, idFactory = () => crypto.randomUUID(), onChange = () => {} }:
  { accountId: string; client: Client; storage: Pick<Storage, 'getItem' | 'setItem'>; idFactory?: () => string; onChange?: () => void }) {
  const key = `soty.personal-agent-ui.v1:${accountId}`;
  let saved: Saved = { schema: 'soty.personal-agent-ui.v1', draft: '', pending: [] }, phase: PersonalAgentView['phase'] = 'draft';
  let disposed = false, running = false, answer: string | undefined;
  const notify = () => { try { onChange(); } catch { /* A renderer failure does not alter a committed server outcome. */ } };
  const raw = storage.getItem(key);
  if (raw !== null) {
    if (raw.length > 80000) throw failure('assistant_storage_unavailable');
    let parsed: unknown; try { parsed = JSON.parse(raw); } catch { throw failure('assistant_storage_unavailable'); }
    if (!own(parsed, ['schema', 'draft', 'pending', 'active', 'contextId']) || parsed.schema !== saved.schema
      || typeof parsed.draft !== 'string' || parsed.draft.length > 8000 || !Array.isArray(parsed.pending) || parsed.pending.length > 8
      || parsed.contextId !== undefined && !context(parsed.contextId)
      || parsed.pending.some(item => !own(item, ['requestId', 'prompt', 'contextId']) || !requestId(item.requestId)
        || typeof item.prompt !== 'string' || !item.prompt.trim() || item.prompt.length > 8000 || item.contextId !== undefined && !context(item.contextId))
      || new Set(parsed.pending.map(item => item.requestId)).size !== parsed.pending.length
      || parsed.active !== undefined && !parsed.pending.some(item => item.requestId === parsed.active)) throw failure('assistant_storage_unavailable');
    saved = parsed as unknown as Saved; if (saved.active) phase = 'unknown';
  }
  const persist = (next: Saved) => { const text = JSON.stringify(next); if (text.length > 80000) throw failure('assistant_storage_unavailable'); storage.setItem(key, text); saved = next; };
  const current = async () => { if (disposed || (await client.getLocalState()).accountId !== accountId || disposed) throw failure('authentication_required'); };
  const view = (): PersonalAgentView => ({ phase, draft: saved.draft, unconfirmed: saved.pending.length,
    ...(saved.contextId ? { contextId: saved.contextId } : {}), ...(saved.active ? { requestId: saved.active } : {}), ...(answer ? { answer } : {}) });
  return {
    view,
    saveDraft(text: string) { if (disposed || running || saved.active) return; if (text.length > 8000) throw failure('agent_invalid_input'); persist({ ...saved, draft: text }); },
    startNew() { if (disposed || running) throw failure('assistant_conversation_busy');
      persist({ schema: saved.schema, draft: '', pending: saved.pending }); phase = 'draft'; answer = undefined; notify(); },
    async turn(text: string) {
      if (running || saved.active) throw failure('agent_reconcile_required'); await current();
      if (running || saved.active) throw failure('agent_reconcile_required');
      if (!text.trim() || text.length > 8000 || saved.pending.length >= 8) throw failure('agent_capacity');
      const id = idFactory(); if (!requestId(id) || saved.pending.some(item => item.requestId === id)) throw failure('agent_invalid_input');
      const intent: Request = { requestId: id, prompt: text, ...(saved.contextId ? { contextId: saved.contextId } : {}) };
      // A storage failure precedes dispatch. Preserve the exact prompt, ID and context before asking the server.
      persist({ ...saved, draft: text, pending: [...saved.pending, intent], active: id }); phase = 'running'; running = true; notify();
      try {
        const result = await client.extension<Record<string, unknown>>('agent.turn', { ...intent }, { expectedAccountId: accountId }); await current();
        if (!own(result, ['ok', 'schema', 'requestId', 'status', 'answer', 'contextId', 'evidence', 'claims', 'cards', 'actions', 'telemetry', 'accounting'])
          || result.ok !== undefined && result.ok !== true
          || result.schema !== 'soty.personal-agent-turn.v1' || result.requestId !== id || result.status !== 'complete'
          || typeof result.answer !== 'string' || !result.answer.trim() || result.answer.length > 8000 || !context(result.contextId)) throw failure('agent_output_invalid');
        persist({ schema: saved.schema, draft: '', pending: saved.pending.filter(item => item.requestId !== id), contextId: result.contextId });
        answer = result.answer; phase = 'complete'; notify(); return view();
      } catch (error) { phase = 'unknown'; notify(); throw error; }
      finally { running = false; notify(); }
    },
    async cancel() {
      const id = saved.active; if (!id) throw failure('agent_turn_unavailable'); await current();
      const result = await client.extension<Record<string, unknown>>('agent.cancel', { requestId: id }, { expectedAccountId: accountId }); await current();
      if (!own(result, ['ok', 'schema', 'requestId', 'cancelRequested', 'remoteTerminationVerified']) || result.ok !== undefined && result.ok !== true || result.schema !== 'soty.personal-agent-cancel.v1'
        || result.requestId !== id || typeof result.cancelRequested !== 'boolean' || result.remoteTerminationVerified !== false) throw failure('agent_output_invalid');
      if (saved.active === id && result.cancelRequested) phase = 'cancel_requested';
      notify();
      return result;
    },
    dispose() { disposed = true; },
  };
}
