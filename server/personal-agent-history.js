import { createHash } from 'node:crypto';
import { types } from 'node:util';
import { NotesError } from '../modules/notes/server/index.mjs';
import { createIsolatedContextStore } from '../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';
import { canonical, fields } from '../modules/personal-agent/provider/validation.mjs';
import { agentError } from './personal-agent-transport.js';

const schema = 'soty.personal-agent-history.v1';
const contextId = value => typeof value === 'string' && /^ctx_[A-Za-z0-9_-]{16,124}$/u.test(value);
const hash = value => createHash('sha256').update(canonical(value, 131072)).digest('hex');
const need = (value, code = 'agent_memory_unavailable') => { if (!value) throw agentError(code); };
const MAX_HISTORY = 256, MAX_BODY = 96 * 1024;
const MAX_SERIALIZED_ENTRY = 48064; // 8000 UTF-16 units, worst JSON escape6 per unit, closed role/content wrapper.
function message(entry) {
  need(typeof entry === 'string' && entry.length <= MAX_SERIALIZED_ENTRY);
  let parsed; try { parsed = JSON.parse(entry); fields(parsed, ['role', 'content']); } catch { throw agentError('agent_memory_unavailable'); }
  need(['user', 'assistant'].includes(parsed.role) && typeof parsed.content === 'string' && parsed.content.length <= 8000);
  return parsed;
}
function notFound(error) {
  if (!error || typeof error !== 'object' || types.isProxy(error) || Object.getPrototypeOf(error) !== NotesError.prototype) return false;
  const code = Object.getOwnPropertyDescriptor(error, 'code'); return code?.value === 'notes_note_not_found';
}

/** Installed host port, called only inside the module's fresh Connect actor fence.
 * io uses the existing account-owned Notes.execute. A stored note is conversation data,
 * never a grant, credential or anti-rollback floor. No new database or schema is installed. */
export function createNotesConversationStore({ scope, actor, turnId, io } = {}) {
  fields(scope, ['registryId', 'environmentId', 'audienceId', 'accountId']);
  need(scope.accountId === actor.accountId && typeof io === 'function');
  const ownerScope = Object.freeze({ ...scope });
  let id, previous, record, first, working;
  const noteFor = value => `agent_${hash({ scope: ownerScope, contextId: value })}`;
  function read(value) {
    try { return io('notes.get', { noteId: noteFor(value) }).note; }
    catch (error) { if (notFound(error)) return null; throw agentError('agent_memory_unavailable'); }
  }
  function capture(note, value) {
    need(note && note.state === 'active' && Buffer.byteLength(note.body, 'utf8') <= MAX_BODY);
    let data;
    try {
      data = JSON.parse(note.body); fields(data, ['schema', 'scope', 'contextId', 'history', 'turns']);
      fields(data.scope, ['registryId', 'environmentId', 'audienceId', 'accountId']);
      need(data.schema === schema && data.contextId === value && canonical(data.scope) === canonical(ownerScope));
      need(Array.isArray(data.history) && data.history.length <= MAX_HISTORY && data.history.length % 2 === 0
        && Array.isArray(data.turns) && data.turns.length === data.history.length / 2);
      data.history.forEach((entry, index) => need(message(entry).role === (index % 2 ? 'assistant' : 'user')));
      data.turns.forEach(entry => { fields(entry, ['requestId', 'deviceId']);
        need(typeof entry.requestId === 'string' && /^[A-Za-z0-9_.:-]{8,100}$/u.test(entry.requestId)
          && typeof entry.deviceId === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(entry.deviceId)); });
      need(new Set(data.turns.map(entry => entry.requestId)).size === data.turns.length);
    } catch { throw agentError('agent_memory_unavailable'); }
    return data;
  }
  function unchanged() {
    const note = read(id);
    need(previous ? note?.revision === previous.revision && note.body === previous.body : note === null, 'agent_memory_conflict');
  }
  return Object.freeze({
    resolve(role, requestedId) {
      need(role === 'human' && id === undefined);
      id = requestedId ?? `ctx_${hash({ scope: ownerScope, requestId: turnId, deviceId: actor.deviceId }).slice(0, 32)}`;
      need(contextId(id)); previous = read(id);
      need(requestedId === undefined || previous !== null);
      record = previous ? capture(previous, id) : { schema, scope: ownerScope, contextId: id, history: [], turns: [] };
      need(!record.turns.some(entry => entry.requestId === turnId), 'agent_reconcile_required');
      need(record.history.length + 2 <= MAX_HISTORY, 'agent_memory_capacity');
      // Reuse donor identity/resolution. Its serialized-entry8000 cap is narrower than the accepted content contract.
      working = createIsolatedContextStore({ roles: ['human'], idFactory: () => id, maxContexts: 1, maxHistoryEntries: 8 });
      return working.resolve(role);
    },
    snapshot(value) { need(value === id && working); unchanged(); const metadata = working.snapshot(id);
      return metadata ? { ...metadata, history: record.history.slice(-8) } : undefined; },
    append(value, entry) {
      need(value === id && working); const parsed = message(entry); unchanged();
      if (!first) { need(parsed.role === 'user'); first = entry; return; }
      need(parsed.role === 'assistant');
      const next = { ...record, history: [...record.history, first, entry],
        turns: [...record.turns, { requestId: turnId, deviceId: actor.deviceId }] };
      const body = JSON.stringify(next); need(Buffer.byteLength(body, 'utf8') <= MAX_BODY, 'agent_memory_capacity');
      const args = { noteId: noteFor(id), mutationId: `agent_${hash({ scope: ownerScope, contextId: id, requestId: turnId })}`,
        expectedRevision: previous?.revision ?? 0, title: 'Диалог с помощником', body,
        items: [], color: 'honey', pinned: false, state: 'active' };
      let ack; try { ack = io('notes.put', args); } catch { throw agentError('agent_memory_conflict'); }
      need(ack.noteId === args.noteId && ack.revision === args.expectedRevision + 1, 'agent_memory_conflict');
      // Both messages share one actual Notes transaction/receipt. The first append alone never writes.
      record = next; previous = { ...args, revision: ack.revision }; first = undefined;
    },
  });
}
