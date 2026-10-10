import { blankNote, createNoteSession, noteErrorCode, type DraftStore, type NoteSession } from '../../modules/notes/browser/state.mjs';

type Api = { request<T>(op: string, args: Record<string, unknown>): Promise<T> };
export type AnswerNoteView = { phase: 'idle' | 'saving' | 'saved' | 'pending' | 'local-error' | 'unavailable' | 'closed'; noteId?: string };

/** One explicitly saved, confirmed answer. Notes owns its durable outbox, CAS and recovery. */
export function createAssistantAnswerNote({ accountId, answer, api, store, isCurrent }: {
  accountId: string; answer: string; api: Api; store: DraftStore; isCurrent(): Promise<boolean>;
}) {
  if (!answer.trim() || answer.length > 8000) throw new Error('agent_output_invalid');
  let session: NoteSession | undefined, flight: Promise<void> | undefined, closed = false, failed = false;
  const assertCurrent = async () => {
    if (closed || !await isCurrent() || closed) throw new Error('notes_account_changed');
  };
  const view = (): AnswerNoteView => {
    if (closed) return { phase: 'closed' };
    if (flight) return { phase: 'saving' };
    if (!session) return { phase: failed ? 'unavailable' : 'idle' };
    const state = session.state();
    if (!state.dirty && state.note.revision > 0) return { phase: 'saved', noteId: state.note.noteId };
    if (!state.localDurable || state.localError) return { phase: 'local-error' };
    return { phase: 'pending', noteId: state.note.noteId };
  };
  return {
    view,
    save() {
      if (flight) return flight;
      flight = (async () => {
        await assertCurrent(); failed = false;
        if (!session) {
          session = createNoteSession({ accountId, store, note: blankNote(), debounceMs: 60000,
            api: { async request<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
              await assertCurrent();
              const ack = await api.request<{ noteId: string; revision: number; updatedAt: number }>(op, args);
              await assertCurrent();
              if (ack.noteId !== args.noteId || ack.revision !== Number(args.expectedRevision) + 1
                || !Number.isSafeInteger(ack.updatedAt) || ack.updatedAt < 0) throw new Error('notes_response_invalid');
              return ack as T;
            } },
          });
          session.edit({ title: 'Черновик ответа помощника', body: answer });
        }
        await session.flush();
        await assertCurrent();
      })().catch(cause => {
        if (noteErrorCode(cause) === 'notes_account_changed') { closed = true; session?.dispose(); }
        else failed = true;
        throw cause;
      }).finally(() => { flight = undefined; });
      return flight;
    },
    async flush() { await flight; },
    hasUnsavedChanges() { return Boolean(flight || session?.hasUnsavedChanges()); },
    dispose() { closed = true; session?.dispose(); },
  };
}
