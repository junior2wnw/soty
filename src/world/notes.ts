import { el, button, iconButton, textInput } from './dom';
import { icon } from './icons';
import { createDialog } from './dialogs';
import type { WorldApi } from './types';
import { blankNote, createDraftStore, createNoteSession, NOTE_COLORS, noteErrorCode, uid } from '../../modules/notes/browser/state.mjs';
import type { Draft, Note, NoteColor, NoteMetadata, NoteSession, NotesList, NoteState, SessionState } from '../../modules/notes/browser/state.mjs';
import './notes.css';

export interface NotesOptions {
  api: WorldApi; accountId: string; projectId?: string; initialNoteId?: string | undefined;
  openLegacy(): void; onClose?(): void; onOpenNote?(noteId: string, title?: string): void;
}
export interface NotesHandle { dispose(): void; focus(): void; flush(): Promise<void>; reconnect(): void; hasUnsavedChanges(): boolean }
const bucketNames: Record<NoteState, string> = { active: 'Все', archived: 'Архив', trashed: 'Корзина' };
const colorNames: Record<NoteColor, string> = { plain: 'Без цвета', honey: 'Мёд', sage: 'Шалфей', lilac: 'Сирень', blue: 'Небо', coral: 'Коралл' };
export function notesErrorText(error: unknown): string {
  const code = noteErrorCode(error);
  const messages: Record<string, string> = {
    notes_account_changed: 'Аккаунт изменился. Откройте записки заново.',
    device_revoked: 'Устройство отключено от аккаунта. Черновик остался здесь.',
    authentication_required: 'Подключите аккаунт, чтобы сохранить записку на сервере.',
    notes_revision_conflict: 'Эту записку изменили на другом устройстве.',
    notes_note_deleted: 'Записка удалена на другом устройстве. Ваш текст остался в черновике.',
    notes_note_not_found: 'Записка недоступна. Локальный черновик можно сохранить как новую.',
    notes_note_too_large: 'Записка слишком большая. Разделите её на несколько.',
    notes_storage_quota: 'Хранилище записок заполнено. Освободите место в корзине.',
    notes_count_quota: 'Достигнут лимит записок. Освободите место в корзине.',
    notes_identity_quota: 'Достигнут лимит созданных записок для этого аккаунта.',
    notes_local_quota: 'Место для черновиков на устройстве заполнено. Сохраните или скачайте их.',
    notes_local_unavailable: 'Браузер не сохранил черновик. Скачайте текст перед закрытием.',
    notes_invalid_arguments: 'Проверьте размер записки и пунктов списка.',
    rate_limited: 'Подождите немного и повторите сохранение.',
  };
  return messages[code] || 'Нет связи с сервером. Сохранённый на устройстве черновик отправится после подключения.';
}

export function mountNotes(host: HTMLElement, options: NotesOptions): NotesHandle {
  const store = createDraftStore(options.accountId, options.projectId);
  const root = el('section', 'sn-workspace'); root.setAttribute('aria-label', 'Личные записки');
  const sidebar = el('aside', 'sn-sidebar'); const editor = el('section', 'sn-editor'); editor.setAttribute('aria-label', 'Редактор записки');
  const heading = el('div', 'sn-heading'); const title = el('div'); title.append(el('h1', '', 'Записки'), el('span', 'sn-private', 'Личные'));
  heading.append(title, iconButton('Новая записка', 'plus', () => { void openNew(); }));
  const create = button('Новая записка', 'plus', 'sn-create sw-primary', () => { void openNew(); });
  const searchWrap = el('div', 'sn-search'); const search = textInput('', 'Найти в записках', 160); search.type = 'search'; search.setAttribute('aria-label', 'Поиск в записках'); searchWrap.append(icon('search'), search);
  const filters = el('div', 'sn-filters'); filters.setAttribute('aria-label', 'Разделы записок');
  const filterButtons = new Map<NoteState, HTMLButtonElement>();
  const message = el('div', 'sn-list-message'); message.setAttribute('role', 'status');
  const draftList = el('div', 'sn-drafts'); const list = el('div', 'sn-list'); list.setAttribute('aria-label', 'Список записок');
  const loadMore = button('Ещё записки', 'down', 'sn-more', () => { void loadList(true); }); loadMore.hidden = true;
  const legacy = button('Общие тексты в комнатах', 'external', 'sn-legacy', () => { void safeAction(options.openLegacy); });
  legacy.title = 'Открыть совместные тексты в общих комнатах';
  const footer = el('div', 'sn-footer'); footer.append(legacy);
  sidebar.append(heading, create, searchWrap, filters, message, draftList, list, loadMore, footer); root.append(sidebar, editor); host.replaceChildren(root);
  let disposed = false; let session: NoteSession | null = null; let bucket: NoteState = 'active'; let rows: NoteMetadata[] = [];
  let nextCursor: string | null = null; let listRequest = 0; let openRequest = 0; let searchTimer: ReturnType<typeof setTimeout> | undefined;
  let drafts: Draft[] = []; let initialOpening = true; let status: HTMLElement | null = null; let alert: HTMLElement | null = null;
  let statusText: HTMLElement | null = null; let retry: HTMLButtonElement | null = null; let copyConflict: HTMLButtonElement | null = null; let currentConflict: HTMLButtonElement | null = null;
  let pin: HTMLButtonElement | null = null; let lastCleanRevision = -1; let listRefreshTimer: ReturnType<typeof setTimeout> | undefined;
  const request = <T>(method: string, args: Record<string, unknown> = {}) => options.api.request<T>(method, { expectedAccountId: options.accountId, ...args });

  for (const key of ['active', 'archived', 'trashed'] as const) {
    const control = button(bucketNames[key], undefined, 'sn-filter', () => { bucket = key; updateFilters(); void loadList(); });
    filterButtons.set(key, control); filters.append(control);
  }
  function updateFilters(counts?: Record<NoteState, number>) {
    for (const [key, control] of filterButtons) {
      control.setAttribute('aria-pressed', String(key === bucket));
      control.title = counts ? `${bucketNames[key]} · ${counts[key]}` : bucketNames[key];
    }
  }
  updateFilters();
  search.addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { void loadList(); }, 250); });

  function dateLabel(timestamp: number) {
    const date = new Date(timestamp); const now = new Date();
    return date.toDateString() === now.toDateString() ? date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }) : date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
  }
  function displayTitle(note: NoteMetadata) { return note.title.trim() || note.preview.trim().slice(0, 50) || 'Без названия'; }
  function renderRows() {
    const activeId = session?.state().note.noteId; list.replaceChildren();
    if (!rows.length) {
      const empty = el('div', 'sn-list-empty');
      empty.append(el('strong', '', search.value.trim() ? 'Ничего не найдено' : bucket === 'active' ? 'Место для ваших мыслей' : bucket === 'archived' ? 'В архиве пусто' : 'В корзине пусто'));
      empty.append(el('p', '', search.value.trim() ? 'Попробуйте другое слово.' : bucket === 'active' ? 'Идея, список или важная мелочь — всё под рукой.' : bucket === 'archived' ? 'Сюда можно убрать завершённые записки.' : 'Удалённые записки можно восстановить.'));
      list.append(empty); return;
    }
    for (const note of rows) {
      const card = el('button', `sn-card sn-color-${note.color}`); card.type = 'button'; card.dataset.noteId = note.noteId;
      card.setAttribute('aria-label', `${displayTitle(note)}${note.pinned ? ', закреплена' : ''}`);
      card.setAttribute('aria-current', String(note.noteId === activeId));
      const line = el('div', 'sn-card-title'); line.append(el('strong', '', displayTitle(note))); if (note.pinned) line.append(icon('pin'));
      card.append(line, el('p', 'sn-card-preview', note.preview || 'Пустая записка'), el('time', 'sn-card-time', dateLabel(note.updatedAt)));
      card.addEventListener('click', () => { void openNote(note.noteId); }); list.append(card);
    }
  }
  async function loadList(append = false) {
    const seq = ++listRequest; const cursor = append ? nextCursor : null; loadMore.disabled = true;
    if (!append) message.textContent = 'Открываем записки…';
    try {
      const result = await request<NotesList>('notes.list', { bucket, query: search.value.trim(), limit: 30, ...(cursor ? { cursor } : {}) });
      if (disposed || seq !== listRequest) return;
      rows = append ? [...rows, ...result.notes.filter(note => !rows.some(old => old.noteId === note.noteId))] : result.notes;
      nextCursor = result.nextCursor; loadMore.hidden = !nextCursor; message.replaceChildren(); updateFilters(result.usage.counts); renderRows();
      if (initialOpening) {
        initialOpening = false;
        if (options.initialNoteId === 'new') await openNew();
        else if (options.initialNoteId) await openNote(options.initialNoteId);
        else if (rows[0] && matchMedia('(min-width: 801px)').matches) await openNote(rows[0].noteId, false);
        else showLanding();
      }
    } catch (error) {
      if (disposed || seq !== listRequest) return;
      message.replaceChildren(el('span', '', notesErrorText(error)), button('Повторить', 'refresh', '', () => { void loadList(); }));
      if (initialOpening) {
        initialOpening = false;
        const recovery = drafts.find(draft => draft.note.noteId === options.initialNoteId);
        if (recovery) await openDraft(recovery); else if (options.initialNoteId === 'new') await openNew(); else showLanding();
      }
    } finally { if (!disposed && seq === listRequest) loadMore.disabled = false; }
  }
  async function loadDrafts() {
    try { drafts = await store.list(); if (!disposed) renderDrafts(); }
    catch { /* Editing reports local storage errors explicitly. Server catalog stays available. */ }
  }
  function renderDrafts() {
    draftList.replaceChildren();
    const activeBranch = session?.state().branchId;
    const recoverable = drafts.filter(draft => draft.branchId !== activeBranch && (draft.generation > draft.committedGeneration || draft.pending || draft.conflict));
    if (!recoverable.length) { draftList.hidden = true; return; } draftList.hidden = false;
    draftList.append(el('h2', '', 'На этом устройстве'));
    for (const draft of recoverable) {
      const card = button(draft.note.title.trim() || draft.note.body.trim().slice(0, 45) || 'Черновик', 'folder', 'sn-draft', () => { void openDraft(draft); });
      card.append(el('small', '', `${draft.conflict ? 'Есть другая версия · ' : ''}${dateLabel(draft.savedAt)}`)); draftList.append(card);
    }
  }
  function showLanding() {
    if (session || disposed) return; editor.replaceChildren();
    const blank = el('div', 'sn-landing'); const paper = el('div', 'sn-paper-art'); paper.setAttribute('aria-hidden', 'true');
    for (let index = 0; index < 3; index++) paper.append(el('span'));
    blank.append(paper, el('h2', '', 'Мысль появилась — запишите'), el('p', '', 'Текст, списки и идеи в одном спокойном месте.'),
      button('Создать записку', 'plus', 'sw-primary', () => { void openNew(); })); editor.append(blank);
  }
  async function flush() { if (session) await session.flush(); }
  async function safeAction(action: () => void) {
    try { await flush(); if (!disposed) action(); }
    catch (error) { if (alert) { alert.textContent = notesErrorText(error); alert.hidden = false; } else message.textContent = notesErrorText(error); }
  }
  async function releaseSession() {
    const previous = session; if (!previous) return; await previous.flush(); previous.dispose(); if (session === previous) session = null;
  }
  async function openNew(asChecklist = false) {
    const seq = ++openRequest;
    try {
      await releaseSession(); if (disposed || seq !== openRequest) return;
      const note = blankNote(); if (asChecklist) note.items = [{ id: uid(), text: '', done: false }];
      installSession(note); root.classList.add('sn-editing');
      if (asChecklist) session?.edit({ items: note.items });
      editor.querySelector<HTMLTextAreaElement>('.sn-note-title')?.focus();
    } catch (error) { message.textContent = notesErrorText(error); }
  }
  async function openNote(noteId: string, focus = true) {
    if (session?.state().note.noteId === noteId) { root.classList.add('sn-editing'); return; }
    const seq = ++openRequest;
    try {
      await releaseSession(); if (disposed || seq !== openRequest) return;
      await loadDrafts();
      const savedDraft = drafts.find(draft => draft.note.noteId === noteId && (draft.generation > draft.committedGeneration || draft.pending || draft.conflict));
      if (savedDraft) { installSession(savedDraft.note, savedDraft); root.classList.add('sn-editing'); return; }
      const result = await request<{ note: Note }>('notes.get', { noteId });
      if (disposed || seq !== openRequest) return;
      installSession(result.note); if (focus) root.classList.add('sn-editing');
    } catch (error) { if (!disposed && seq === openRequest) { message.textContent = notesErrorText(error); showLanding(); } }
  }
  async function openDraft(draft: Draft) {
    const seq = ++openRequest;
    try { await releaseSession(); if (disposed || seq !== openRequest) return; installSession(draft.note, draft); root.classList.add('sn-editing'); }
    catch (error) { message.textContent = notesErrorText(error); }
  }
  function installSession(note: Note, draft?: Draft) {
    lastCleanRevision = note.revision;
    session = createNoteSession({ api: options.api, accountId: options.accountId, store, note, ...(draft ? { draft } : {}), onChange: updateStatus });
    renderEditor(); renderRows(); renderDrafts(); updateStatus(session.state());
    options.onOpenNote?.(note.revision || draft ? note.noteId : 'new', displayTitle(note));
  }
  function updateStatus(state: SessionState) {
    if (disposed || !status || !statusText) return;
    status.dataset.state = state.localError ? 'error' : state.conflict ? 'conflict' : state.saving ? 'saving' : state.dirty ? 'local' : 'saved';
    statusText.textContent = state.localError ? 'Не сохранено на устройстве' : state.conflict ? 'Две версии' : state.saving ? 'Сохраняем…' : state.dirty ? 'На устройстве' : state.note.revision ? 'Сохранено' : 'Новая записка';
    if (retry) retry.hidden = !state.error || state.conflict || Boolean(state.localError);
    if (copyConflict) copyConflict.hidden = !state.conflict;
    if (currentConflict) currentConflict.hidden = !state.conflict;
    if (alert) { alert.textContent = state.localError ? notesErrorText({ code: state.localError }) : state.error ? notesErrorText({ code: state.error }) : ''; alert.hidden = !alert.textContent; }
    if (pin) { pin.setAttribute('aria-pressed', String(state.note.pinned)); pin.title = state.note.pinned ? 'Открепить' : 'Закрепить'; pin.setAttribute('aria-label', pin.title); }
    editor.dataset.color = state.note.color;
    if (!state.dirty && state.note.revision !== lastCleanRevision) {
      lastCleanRevision = state.note.revision; clearTimeout(listRefreshTimer);
      options.onOpenNote?.(state.note.noteId, displayTitle(state.note));
      listRefreshTimer = setTimeout(() => { void loadList(); void loadDrafts(); }, 250);
    }
  }
  function edit(patch: Parameters<NoteSession['edit']>[0]) { session?.edit(patch); }
  function renderEditor() {
    if (!session) return; const note = session.state().note; const readOnly = note.state === 'trashed'; editor.replaceChildren(); editor.dataset.color = note.color;
    const toolbar = el('div', 'sn-editor-toolbar');
    const back = iconButton('К списку записок', 'back', () => { void safeAction(() => { root.classList.remove('sn-editing'); search.focus(); }); }); back.classList.add('sn-mobile-back');
    status = el('div', 'sn-save-status'); status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite'); statusText = el('span'); status.append(el('i'), statusText);
    pin = iconButton(note.pinned ? 'Открепить' : 'Закрепить', 'pin', () => edit({ pinned: !session!.state().note.pinned })); pin.disabled = readOnly;
    const actions = el('div', 'sn-editor-actions');
    const addList = iconButton('Добавить пункт списка', 'list', () => { addItem(); }); addList.disabled = readOnly;
    const menu = el('details', 'sn-note-menu'); const summary = el('summary'); summary.title = 'Действия с запиской'; summary.setAttribute('aria-label', 'Действия с запиской'); summary.append(icon('more'));
    menu.addEventListener('keydown', event => { if (event.key === 'Escape' && menu.open) { event.preventDefault(); event.stopPropagation(); menu.open = false; summary.focus(); } });
    menu.addEventListener('focusout', event => { if (event.relatedTarget instanceof Node && !menu.contains(event.relatedTarget)) menu.open = false; });
    const menuBody = el('div', 'sn-menu-body');
    if (!readOnly) {
      const palette = el('div', 'sn-palette'); palette.setAttribute('aria-label', 'Цвет записки');
      for (const color of NOTE_COLORS) {
        const swatch = el('button', `sn-swatch sn-color-${color}`); swatch.type = 'button'; swatch.title = colorNames[color]; swatch.setAttribute('aria-label', colorNames[color]); swatch.setAttribute('aria-pressed', String(note.color === color));
        swatch.addEventListener('click', () => { edit({ color }); for (const item of palette.querySelectorAll('button')) item.setAttribute('aria-pressed', String(item === swatch)); }); palette.append(swatch);
      }
      menuBody.append(palette, button(note.state === 'archived' ? 'Вернуть в записки' : 'В архив', 'folder', '', () => { menu.open = false; void changeState(note.state === 'archived' ? 'active' : 'archived'); }),
        button('В корзину', 'close', '', () => { menu.open = false; void changeState('trashed'); }));
    } else menuBody.append(button('Восстановить', 'refresh', '', () => { menu.open = false; void changeState('active'); }), button('Удалить навсегда', 'close', 'sn-danger', confirmPurge));
    menuBody.append(button('Скачать текст', 'external', '', () => { downloadNote(); menu.open = false; })); menu.append(summary, menuBody); actions.append(pin, addList, menu); toolbar.append(back, status, actions);
    const banner = el('div', 'sn-note-banner');
    if (note.state !== 'active') banner.append(el('span', '', note.state === 'trashed' ? 'В корзине' : 'В архиве'), button('Восстановить', 'refresh', '', () => { void changeState('active'); })); else banner.hidden = true;
    alert = el('p', 'sn-save-alert'); alert.setAttribute('role', 'status'); alert.hidden = true;
    const recovery = el('div', 'sn-recovery-actions');
    retry = button('Повторить', 'refresh', '', () => { void session?.retry(); }); retry.hidden = true;
    copyConflict = button('Сохранить копию', 'plus', 'sw-primary', () => { void saveCopy(); }); copyConflict.hidden = true;
    currentConflict = button('Открыть актуальную', 'refresh', '', () => { void openCurrent(); }); currentConflict.hidden = true;
    recovery.append(retry, copyConflict, currentConflict);
    const content = el('div', 'sn-note-content');
    const noteTitle = el('textarea', 'sn-note-title'); noteTitle.value = note.title; noteTitle.placeholder = 'Название'; noteTitle.maxLength = 160; noteTitle.rows = 1;
    noteTitle.setAttribute('aria-label', 'Название записки'); noteTitle.disabled = readOnly;
    noteTitle.addEventListener('input', () => { edit({ title: noteTitle.value }); fitTextarea(noteTitle); });
    const body = el('textarea', 'sn-note-body'); body.value = note.body; body.placeholder = 'О чём думаете?'; body.maxLength = 100000; body.spellcheck = true; body.setAttribute('aria-label', 'Текст записки'); body.disabled = readOnly;
    body.addEventListener('input', () => { edit({ body: body.value }); fitTextarea(body); });
    noteTitle.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); body.focus(); } });
    const checklist = el('div', 'sn-checklist'); checklist.setAttribute('aria-label', 'Чеклист');
    const add = button('Пункт списка', 'plus', 'sn-add-item', () => addItem()); add.hidden = readOnly;
    content.append(noteTitle, body, checklist, add); editor.append(toolbar, banner, alert, recovery, content); renderItems(); fitTextarea(noteTitle); fitTextarea(body);
    function renderItems(focusId?: string) {
      checklist.replaceChildren(); if (!session) return;
      for (const item of session.state().note.items) {
        const row = el('div', 'sn-check-item'); row.dataset.done = String(item.done); row.dataset.itemId = item.id;
        const check = el('input'); check.type = 'checkbox'; check.checked = item.done; check.disabled = readOnly; check.setAttribute('aria-label', item.text || 'Отметить пункт');
        const toggle = el('label', 'sn-check-toggle'); toggle.append(check);
        const field = textInput(item.text, 'Новый пункт', 1000); field.setAttribute('aria-label', 'Текст пункта'); field.disabled = readOnly;
        const change = (patch: Partial<typeof item>) => { const items = session!.state().note.items.map(entry => entry.id === item.id ? { ...entry, ...patch } : entry); edit({ items }); };
        check.addEventListener('change', () => { change({ done: check.checked }); row.dataset.done = String(check.checked); });
        field.addEventListener('input', () => { change({ text: field.value }); check.setAttribute('aria-label', field.value || 'Отметить пункт'); });
        field.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); addItem(item.id); } });
        const remove = iconButton('Удалить пункт', 'close', () => { edit({ items: session!.state().note.items.filter(entry => entry.id !== item.id) }); renderItems(); }); remove.disabled = readOnly;
        row.append(toggle, field, remove); checklist.append(row); if (focusId === item.id) field.focus();
      }
    }
    function addItem(afterId?: string) {
      if (!session) return; const items = session.state().note.items; if (items.length >= 200) { if (alert) { alert.textContent = 'В записке может быть до 200 пунктов.'; alert.hidden = false; } return; }
      const item = { id: uid(), text: '', done: false }; const index = afterId ? items.findIndex(entry => entry.id === afterId) + 1 : items.length;
      items.splice(index, 0, item); edit({ items }); renderItems(item.id);
    }
  }
  function fitTextarea(body: HTMLTextAreaElement) { body.style.height = 'auto'; body.style.height = `${Math.max(body.classList.contains('sn-note-title') ? 1 : 150, body.scrollHeight)}px`; }
  async function changeState(state: NoteState) {
    if (!session) return; edit({ state }); await session.flush().catch(() => {}); if (disposed) return;
    renderEditor(); if (session) updateStatus(session.state()); void loadList();
  }
  async function saveCopy() {
    if (!session) return; const old = session; editor.inert = true;
    try {
      await old.flush(); if (disposed) return;
      const copy = { ...old.state().note, noteId: uid(), revision: 0, createdAt: Date.now(), updatedAt: Date.now(), state: 'active' as const };
      const newSession = createNoteSession({ api: options.api, accountId: options.accountId, store, note: copy }); newSession.edit({ title: copy.title });
      await newSession.flush();
      // The copy must be durable before retiring the conflicting branch.
      if (!newSession.state().localDurable) throw new Error('notes_local_unavailable');
      const branchDraft = (await store.list()).find(draft => draft.branchId === newSession.state().branchId);
      newSession.dispose(); if (disposed) return; await old.discardBranch(); old.dispose(); session = null;
      installSession(newSession.state().note, branchDraft); void loadList(); void loadDrafts();
    } catch (error) { if (alert) { alert.textContent = notesErrorText(error); alert.hidden = false; } }
    finally { editor.inert = false; }
  }
  async function openCurrent() {
    if (!session) return; const previous = session; const noteId = previous.state().note.noteId; editor.inert = true;
    try {
      await previous.flush(); const result = await request<{ note: Note }>('notes.get', { noteId });
      if (disposed) return; previous.dispose(); session = null; installSession(result.note); void loadDrafts();
    } catch (error) { if (alert) { alert.textContent = notesErrorText(error); alert.hidden = false; } }
    finally { editor.inert = false; }
  }
  function downloadNote() {
    if (!session) return; const note = session.state().note;
    const value = [note.title, note.body, note.items.map(item => `${item.done ? '[x]' : '[ ]'} ${item.text}`).join('\n')].filter(Boolean).join('\n\n');
    const url = URL.createObjectURL(new Blob([value], { type: 'text/plain;charset=utf-8' })); const link = el('a'); link.href = url;
    link.download = `${(note.title || 'Записка').replace(/[<>:"/\\|?*\u0000-\u001f]/gu, '').slice(0, 80) || 'Записка'}.txt`; document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function confirmPurge() {
    if (!session) return; const dialog = createDialog('Удалить записку навсегда?');
    const mutationId = uid();
    dialog.body.append(el('p', '', 'Текст и список будут удалены из аккаунта. Это действие нельзя отменить.'));
    const error = el('p', 'sn-save-alert'); error.hidden = true;
    const confirm = button('Удалить навсегда', undefined, 'sn-danger', () => { void (async () => {
      if (!session) return; confirm.disabled = true;
      try {
        await session.flush(); const state = session.state(); if (state.dirty || state.conflict) throw new Error('notes_revision_conflict');
        await request('notes.purge', { noteId: state.note.noteId, expectedRevision: state.note.revision, mutationId });
        await session.discardBranch(); session.dispose(); session = null; dialog.close(); root.classList.remove('sn-editing'); showLanding(); void loadList(); void loadDrafts();
      } catch (cause) { error.textContent = notesErrorText(cause); error.hidden = false; }
      finally { confirm.disabled = false; }
    })(); });
    dialog.body.append(error, button('Оставить', undefined, '', dialog.close), confirm);
  }
  const online = () => { if (disposed) return; void session?.retry(); void loadList(); };
  let measuredWidth = -1;
  const resizeObserver = new ResizeObserver(() => {
    if (!editor.clientWidth || editor.clientWidth === measuredWidth) return; measuredWidth = editor.clientWidth;
    for (const textarea of editor.querySelectorAll('textarea')) fitTextarea(textarea);
  });
  resizeObserver.observe(editor);
  const hidden = () => { if (document.visibilityState === 'hidden') void flush().catch(() => {}); };
  const beforeUnload = (event: BeforeUnloadEvent) => { if (session?.hasUnsavedChanges()) { event.preventDefault(); event.returnValue = ''; } };
  window.addEventListener('online', online); document.addEventListener('visibilitychange', hidden); window.addEventListener('beforeunload', beforeUnload);
  root.addEventListener('keydown', event => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') { event.preventDefault(); void flush().catch(() => {}); }
    if (event.key === 'Escape' && !root.querySelector('details[open]')) root.classList.remove('sn-editing');
  });
  void loadDrafts().finally(() => { if (!disposed) void loadList(); });
  return {
    flush, reconnect: online, hasUnsavedChanges: () => session?.hasUnsavedChanges() ?? false,
    focus: () => (root.classList.contains('sn-editing') ? editor.querySelector<HTMLTextAreaElement>('.sn-note-title') : search)?.focus(),
    dispose() {
      disposed = true; listRequest++; openRequest++; clearTimeout(searchTimer); clearTimeout(listRefreshTimer); session?.dispose();
      resizeObserver.disconnect();
      window.removeEventListener('online', online); document.removeEventListener('visibilitychange', hidden); window.removeEventListener('beforeunload', beforeUnload); root.remove();
    },
  };
}
