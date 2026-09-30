import './app-engagement.css';
import { button, el, iconButton } from './dom';
import { createDialog, type WorldDialog } from './dialogs';
import { engagementError, engagementStorage, sameAppEntry, type EngagementStorageOptions } from './app-saved';
import { normalizeAppEntry } from './app-saved-state.mjs';
import { createAppDiscussionDraftState, createAppDiscussionFeed, dispatchAppDiscussionIntent, listAppDiscussionDrafts } from './app-discussion-state.mjs';
import type { AppEntry } from './app-saved-state.mjs';
import type { AppDiscussionDraftState, AppDiscussionFeed, DiscussionContext, DiscussionDraft, DiscussionMessage, DiscussionPending, DiscussionScope, RetainedDiscussionDraft } from './app-discussion-state.mjs';
import type { WorldApi } from './types';

export interface DiscussionSelection { conversationId?: string; administrative?: boolean; }
export interface AppDiscussionOptions extends EngagementStorageOptions {
  api: WorldApi; accountId: string; appId: string; entry: AppEntry | null; administrative?: boolean;
  initialConversationId?: string; retainedEntry?: { domainId: string; path: string; origin?: string };
  isCurrent(): boolean; onConversationChange?(selection: DiscussionSelection): void; onClose?(): void;
}
export interface AppDiscussionHandle {
  dispose(): void; refresh(): Promise<void>; focus(): void; flush(): Promise<void>; hasUnsavedChanges(): boolean;
  setVisible(value: boolean): void; updateSelection(selection: DiscussionSelection): Promise<void>; updateEntry(entry: AppEntry): Promise<void>;
}
interface MessageRow { element: HTMLLIElement; value: DiscussionMessage; author: HTMLElement; time: HTMLTimeElement; body: HTMLElement; replyLabel: HTMLElement; reply: HTMLButtonElement; remove: HTMLButtonElement; }
interface DraftSlot { model: AppDiscussionDraftState; unsubscribe(): void; }
interface RetainedRow { element: HTMLDetailsElement; value: RetainedDiscussionDraft; text: HTMLTextAreaElement; status: HTMLElement; retry: HTMLButtonElement; abandon: HTMLButtonElement; remove: HTMLButtonElement; }
const text = (node: HTMLElement, value: string): void => { if (node.textContent !== value) node.textContent = value; };
const disabled = (node: HTMLButtonElement, value: boolean): void => node.setAttribute('aria-disabled', String(value));
const audienceLabel = (value: DiscussionContext['audience']): string => ({ public: 'Публичное', shared: 'Выбранным участникам', owner: 'Личное' })[value];
const scopeKey = (scope: DiscussionScope): string => JSON.stringify([scope.accountId, scope.appId, scope.conversationId, scope.entry.domainId, scope.entry.origin, scope.entry.path]);
const controlledError = (code: string): Error & { code: string } => Object.assign(new Error(code), { code });

/** One visible conversation, with separately scoped durable drafts. The parent
 * owns routes and the runtime frame; this component never reads either. */
export function mountAppDiscussion(host: HTMLElement, options: AppDiscussionOptions): AppDiscussionHandle {
  let entry = options.entry ? normalizeAppEntry(options.entry) : null;
  if (entry && entry.appId !== options.appId) throw controlledError('app_discussion_invalid_scope');
  const persistence = engagementStorage(host, options), view = host.ownerDocument.defaultView, document = host.ownerDocument;
  let administrative = options.administrative === true, requestedConversationId = options.initialConversationId;
  // A resolved current conversation is a snapshot, not the user's selection.
  // Only an explicit archive route pins subsequent context reads to an ID.
  let resolvedConversationId: string | undefined;
  let disposed = false, visible = true, selectionVersion = 0, busy = false, switching = false, rendering = false, renderQueued = false;
  let error = '', notice = '', archiveError = '', localError = '', timer: ReturnType<typeof setTimeout> | null = null;
  let activeDraft: AppDiscussionDraftState | null = null, lastPending: DiscussionPending | null = null, dialog: WorldDialog | null = null;
  let feed: AppDiscussionFeed | null = null, unsubscribeFeed: (() => void) | null = null;
  const drafts = new Map<string, DraftSlot>(), messages = new Map<string, MessageRow>(), retainedRows = new Map<string, RetainedRow>();
  const archiveRows = new Map<string, HTMLButtonElement>();
  let retained: RetainedDiscussionDraft[] = [], messageConversation: string | null = null, archiveLoaded = false;
  const current = (): boolean => !disposed && options.isCurrent();
  const foreground = (): boolean => current() && visible && document.visibilityState !== 'hidden';
  const valid = (token: number): boolean => foreground() && token === selectionVersion;

  const root = el('section', 'se-discussion'); root.setAttribute('aria-label', 'Обсуждение приложения'); root.dataset.engagementKey = 'discussion';
  const header = el('header', 'se-discussion-header'), heading = el('div', 'se-discussion-heading');
  const title = el('h2', '', 'Обсуждение'), audience = el('span', 'se-discussion-audience'); audience.dataset.engagementKey = 'discussion-audience';
  title.tabIndex = -1;
  heading.append(title, audience); const headerActions = el('div', 'se-actions');
  const refreshButton = iconButton('Обновить обсуждение', 'refresh', () => { void refresh(); }); refreshButton.dataset.engagementKey = 'discussion-refresh';
  headerActions.append(refreshButton);
  if (options.onClose) headerActions.append(iconButton('Закрыть обсуждение', 'close', () => { if (current()) options.onClose?.(); }));
  header.append(heading, headerActions);
  const scroll = el('div', 'se-discussion-scroll'); scroll.tabIndex = 0; scroll.setAttribute('aria-label', 'Сообщения и черновики');
  const banner = el('div', 'se-discussion-banner'), bannerText = el('p');
  const currentButton = button('Текущее обсуждение', 'chat', 'sw-button-quiet', () => { void choose({ administrative }); }); currentButton.dataset.engagementKey = 'discussion-current';
  banner.append(bannerText, currentButton);
  const archiveFold = el('details', 'se-discussion-archives'), archiveSummary = el('summary', '', 'Ранее'); archiveFold.append(archiveSummary);
  archiveFold.dataset.engagementKey = 'discussion-archives';
  const archiveList = el('div', 'se-archive-list'), archiveStatus = el('p', 'se-status'), archiveActions = el('div', 'se-actions');
  const archiveLatest = button('К последним', 'refresh', 'sw-button-quiet', () => { void loadArchives(false); });
  const archiveOlder = button('Ещё', 'down', 'sw-button-quiet', () => { void loadArchives(true); });
  archiveActions.append(archiveLatest, archiveOlder); archiveFold.append(archiveList, archiveStatus, archiveActions);
  const olderBox = el('div', 'se-discussion-older'), olderButton = button('Раньше', 'up', 'sw-button-quiet', () => { void older(); });
  olderButton.dataset.engagementKey = 'discussion-older'; olderBox.append(olderButton);
  const list = el('ol', 'se-discussion-messages'); list.setAttribute('aria-label', 'Сообщения'); list.dataset.engagementKey = 'discussion-messages';
  const empty = el('p', 'se-discussion-empty');
  const retainedBox = el('div', 'se-discussion-local'); retainedBox.dataset.engagementKey = 'discussion-retained';
  scroll.append(banner, archiveFold, olderBox, list, empty, retainedBox);
  const bottom = el('div', 'se-discussion-bottom');
  const status = el('p', 'se-status'); status.setAttribute('role', 'status'); status.dataset.engagementKey = 'discussion-status';
  const latest = button('К последним сообщениям', 'down', 'se-discussion-new', () => { void refresh(); }); latest.dataset.engagementKey = 'discussion-latest';
  const pendingBox = el('div', 'se-pending'), pendingText = el('p', '', 'Результат отправки не подтверждён.'), pendingActions = el('div', 'se-actions');
  const pendingRetry = button('Проверить отправку', 'refresh', 'sw-button-quiet', () => { const expected = lastPending, model = activeDraft; if (expected && model && !busy) void send(model, { expectedPending: expected }); });
  pendingRetry.dataset.engagementKey = 'discussion-retry';
  const pendingAbandon = button('Снять ожидание', undefined, 'sw-button-quiet', () => { const expected = lastPending, model = activeDraft; if (expected && model && !busy) showAbandon(model, expected); });
  pendingActions.append(pendingRetry, pendingAbandon); pendingBox.append(pendingText, pendingActions);
  const conflict = el('div', 'se-pending'), conflictText = el('p', '', 'В другом окне есть другой черновик. Выберите текст перед отправкой.');
  const compare = button('Сравнить черновики', undefined, 'sw-button-quiet', () => { if (activeDraft && !busy) showConflict(activeDraft); }); compare.dataset.engagementKey = 'discussion-conflict'; conflict.append(conflictText, compare);
  const reply = el('div', 'se-discussion-reply'), replyText = el('span');
  const clearReply = iconButton('Не отвечать на сообщение', 'close', () => { if (canWrite() && activeDraft) edit(activeDraft.read().draft.text, null); }); reply.append(replyText, clearReply);
  const composer = el('form', 'se-discussion-composer'), input = el('textarea'); input.rows = 1; input.maxLength = 4000;
  input.placeholder = 'Сообщение'; input.setAttribute('aria-label', 'Сообщение в обсуждение'); input.dataset.engagementKey = 'discussion-input';
  const sendButton = button('Отправить', 'send', 'sw-button-primary'); sendButton.type = 'submit'; sendButton.dataset.engagementKey = 'discussion-send';
  composer.append(input, sendButton); bottom.append(latest, status, pendingBox, conflict, reply, composer); root.append(header, scroll, bottom); host.replaceChildren(root);

  let composerFrame: number | null = null, composerTyping = false, observedComposerWidth = 0;
  let appliedComposerHeight = '', manualComposerHeight: number | null = null;
  let measuredComposerValue: string | null = null, measuredComposerLayout = '';
  function fitComposer(typing: boolean): void {
    if (!view || !foreground() || composer.hidden || !input.isConnected) return;
    const width = input.getBoundingClientRect().width;
    if (width <= 0) return; // A hidden panel has no useful wrapping width; reveal will measure again.
    const styles = view.getComputedStyle(input), minimum = Number.parseFloat(styles.minHeight), maximum = Number.parseFloat(styles.maxHeight);
    const borders = Number.parseFloat(styles.borderTopWidth) + Number.parseFloat(styles.borderBottomWidth);
    if (!Number.isFinite(minimum) || !Number.isFinite(maximum) || !Number.isFinite(borders)) return;
    // CSS UI specifies that native manual resizing writes the inline height.
    // Keep that preference distinct from our own writes, including orientation clamps.
    if (input.style.height !== appliedComposerHeight) {
      const requested = Number.parseFloat(input.style.height);
      manualComposerHeight = Number.isFinite(requested) ? requested : null;
    }
    const layout = [width, minimum, maximum, borders, styles.paddingTop, styles.paddingBottom, styles.font, styles.lineHeight, styles.letterSpacing, manualComposerHeight].join('|');
    if (measuredComposerValue === input.value && measuredComposerLayout === layout) return;
    const historyTop = scroll.scrollTop, atEnd = scroll.scrollHeight - historyTop - scroll.clientHeight < 2;
    const inputTop = input.scrollTop, inputLeft = input.scrollLeft;
    const typingAtEnd = typing && document.activeElement === input && input.selectionStart === input.value.length && input.selectionEnd === input.value.length;
    let preferred = manualComposerHeight;
    if (preferred === null) {
      input.style.overflowY = 'hidden'; input.style.height = 'auto';
      preferred = input.scrollHeight + borders;
    }
    appliedComposerHeight = `${Math.max(minimum, Math.min(maximum, Math.ceil(preferred)))}px`;
    input.style.height = appliedComposerHeight; input.style.overflowY = 'auto';
    if (!typing) { input.scrollTop = inputTop; input.scrollLeft = inputLeft; }
    else if (typingAtEnd) input.scrollTop = input.scrollHeight;
    scroll.scrollTop = atEnd ? scroll.scrollHeight : historyTop;
    measuredComposerValue = input.value; measuredComposerLayout = layout;
  }
  function scheduleComposerFit(typing = false): void {
    composerTyping ||= typing;
    if (!view || !foreground() || composerFrame !== null) return;
    composerFrame = view.requestAnimationFrame(() => {
      composerFrame = null; const fromInput = composerTyping; composerTyping = false; fitComposer(fromInput);
    });
  }
  function cancelComposerFit(): void {
    if (composerFrame !== null) view?.cancelAnimationFrame(composerFrame);
    composerFrame = null; composerTyping = false;
  }
  const composerResize = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => {
    const width = input.getBoundingClientRect().width;
    if (width !== observedComposerWidth || input.style.height !== appliedComposerHeight) {
      observedComposerWidth = width; scheduleComposerFit();
    }
  });
  composerResize?.observe(input);
  const onComposerResize = (): void => scheduleComposerFit();
  const onComposerFontLoad = (): void => { measuredComposerLayout = ''; scheduleComposerFit(); };
  view?.addEventListener('resize', onComposerResize); view?.visualViewport?.addEventListener('resize', onComposerResize);
  document.fonts?.addEventListener('loadingdone', onComposerFontLoad);
  void document.fonts?.ready.then(onComposerFontLoad);

  function closeDialog(): void { const previous = dialog; dialog = null; previous?.close(); }
  function openDialog(label: string): WorldDialog {
    closeDialog(); const value = createDialog(label, () => { if (dialog === value) dialog = null; }); dialog = value; value.element.classList.add('se-dialog'); return value;
  }
  function stopPoll(): void { if (timer !== null) clearTimeout(timer); timer = null; }
  function schedulePoll(): void {
    stopPoll(); if (!foreground() || !feed?.read().context) return;
    timer = setTimeout(() => { timer = null; if (foreground() && !busy && !switching && !feed?.read().loading) void poll(); else schedulePoll(); }, 7000);
  }
  function createFeed(): void {
    unsubscribeFeed?.(); feed?.dispose(); feed = null; unsubscribeFeed = null; archiveLoaded = false;
    if (!entry && !administrative) return;
    feed = createAppDiscussionFeed({ accountId: options.accountId, appId: options.appId, entry: administrative ? null : entry, ...(administrative ? { administrative: true } : {}) });
    unsubscribeFeed = feed.subscribe(render);
  }
  function draftFor(scope: DiscussionScope): AppDiscussionDraftState {
    const key = scopeKey(scope), present = drafts.get(key); if (present) return present.model;
    const model = createAppDiscussionDraftState({ scope, ...persistence });
    const unsubscribe = model.subscribe(() => { refreshRetained(); render(); }); drafts.set(key, { model, unsubscribe }); return model;
  }
  function refreshRetained(): void {
    if (!current()) return;
    const filter = entry ?? options.retainedEntry;
    try {
      retained = filter ? listAppDiscussionDrafts({ accountId: options.accountId, appId: options.appId, domainId: filter.domainId, path: filter.path,
        ...(filter.origin ? { origin: filter.origin } : {}), storage: persistence.storage }) : [];
      localError = '';
    } catch (reason) { localError = engagementError(reason); }
    // Include volatile text even if storage failed. The owning model is kept
    // alive until dispose; it is never relabelled as another conversation.
    for (const { model } of drafts.values()) {
        const value = model.read(), key = scopeKey(model.scope);
        const at = retained.findIndex(item => scopeKey(item.scope) === key);
        if (value.draft.text || value.draft.replyTo || value.pending) {
          const row = { scope: model.scope, draft: value.draft, pending: value.pending };
          if (at >= 0) retained[at] = row; else retained.push(row);
        } else if (at >= 0) retained.splice(at, 1);
    }
  }
  function canWrite(): boolean {
    const state = feed?.read(); return foreground() && !switching && !!state?.context?.canPost && !state.stale && !state.resetRequired && !!activeDraft;
  }
  function edit(value: string, replyTo: string | null): void {
    if (!canWrite() || !activeDraft) return;
    try { activeDraft.edit({ text: value, replyTo }); notice = ''; void activeDraft.flush().then(() => { if (current()) { refreshRetained(); render(); } }); }
    catch (reason) { error = engagementError(reason); render(); }
  }
  input.addEventListener('input', () => { scheduleComposerFit(true); if (activeDraft) edit(input.value, activeDraft.read().draft.replyTo); });
  input.addEventListener('keydown', event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && !event.isComposing) { event.preventDefault(); submit(); } });
  composer.addEventListener('submit', event => { event.preventDefault(); submit(); });
  function submit(): void {
    if (!canWrite() || !activeDraft || busy) return;
    const model = activeDraft, value = model.read(); if (value.pending || value.conflict || !value.draft.text.trim()) return;
    void send(model, { expectedDraft: value.draft });
  }
  async function send(model: AppDiscussionDraftState, action: { expectedDraft: DiscussionDraft } | { expectedPending: DiscussionPending }): Promise<void> {
    if (!foreground() || busy || switching) return;
    const token = selectionVersion; busy = true; error = ''; notice = ''; render();
    const guard = (): boolean => valid(token);
    try {
      const result = 'expectedDraft' in action ? await dispatchAppDiscussionIntent({ state: model, api: options.api, isCurrent: guard, expectedDraft: action.expectedDraft,
        beforeCreate: () => guard() && activeDraft === model && canWrite() && feed?.read().context?.conversationId === model.scope.conversationId })
        : await dispatchAppDiscussionIntent({ state: model, api: options.api, isCurrent: guard, expectedPending: action.expectedPending });
      if (!guard()) return;
      if (result.status === 'accepted' && result.pending && result.response) {
        notice = result.response.ownCurrent.removed ? 'Отправка подтверждена. Сообщение уже удалено.' : 'Отправка подтверждена.';
        feed?.acceptSend(result.pending, result.response);
        if (feed?.read().context?.conversationId === result.pending.scope.conversationId) await poll(true);
      } else if (result.status === 'superseded') error = 'Ожидание изменилось в другом окне. Проверьте показанный запрос.';
    } catch (reason) { if (guard()) error = engagementError(reason); }
    finally { busy = false; if (current()) { refreshRetained(); render(); schedulePoll(); } }
  }
  function showAbandon(model: AppDiscussionDraftState, expected: DiscussionPending): void {
    if (!foreground() || busy) return;
    const surface = openDialog('Снять ожидание?'); surface.body.append(el('p', '', 'Сообщение могло быть отправлено. Уберём только ожидание ответа; текст и история не удалятся.'));
    const actions = el('div', 'se-actions'), no = button('Оставить', undefined, 'sw-button-quiet', closeDialog);
    actions.append(no, button('Снять ожидание', undefined, 'sw-button-quiet', () => {
      if (!foreground() || busy) return; closeDialog();
      void model.abandon(expected).then(() => { if (current()) { notice = 'Ожидание снято. Это не отменяет отправку.'; refreshRetained(); render(); } }).catch(reason => { if (current()) { error = engagementError(reason); render(); } });
    })); surface.body.append(actions); no.focus();
  }
  function showConflict(model: AppDiscussionDraftState): void {
    if (!foreground()) return;
    const value = model.read(), expected = value.remoteDraft?.revision ?? null, surface = openDialog('Выберите черновик');
    const pair = el('div', 'se-draft-compare');
    for (const [label, content] of [['В этом окне', value.draft.text], ['В другом окне', value.remoteDraft?.text ?? '']]) {
      const field = el('label'), caption = el('span', 'se-muted', label), area = el('textarea'); area.readOnly = true; area.value = content ?? ''; area.rows = 4; field.append(caption, area); pair.append(field);
    }
    const actions = el('div', 'se-actions'), no = button('Не менять', undefined, 'sw-button-quiet', closeDialog);
    const chooseDraft = (remote: boolean): void => {
      if (!foreground() || busy) return; closeDialog();
      void (remote ? model.chooseRemote(expected) : model.keepMine(expected)).then(() => { if (current()) { refreshRetained(); render(); } }).catch(reason => { if (current()) { error = engagementError(reason); render(); } });
    };
    actions.append(no, button('Из другого окна', undefined, 'sw-button-quiet', () => chooseDraft(true)), button('Оставить мой', undefined, 'sw-button-primary', () => chooseDraft(false)));
    surface.body.append(pair, actions); no.focus();
  }
  function removeMessage(message: DiscussionMessage): void {
    if (!foreground() || busy || !message.canRemove || !message.body || feed?.read().context?.conversationId !== message.conversationId) return;
    const surface = openDialog('Удалить сообщение?'); surface.body.append(el('p', '', 'В обсуждении останется отметка об удалении.'));
    const no = button('Оставить', undefined, 'sw-button-quiet', closeDialog), actions = el('div', 'se-actions');
    actions.append(no, button('Удалить', 'trash', 'sw-button-quiet', () => {
      if (!foreground() || busy) return; closeDialog(); void removeConfirmed(message);
    })); surface.body.append(actions); no.focus();
  }
  async function removeConfirmed(message: DiscussionMessage): Promise<void> {
    const token = selectionVersion; busy = true; error = ''; render();
    try {
      const response = await options.api.request<{ id: string; conversationId: string; removed: true }>('apps.discussion.remove', {
        expectedAccountId: options.accountId, appId: options.appId, conversationId: message.conversationId, messageId: message.id });
      if (!valid(token)) return;
      if (!response || response.id !== message.id || response.conversationId !== message.conversationId || response.removed !== true) throw controlledError('app_discussion_invalid_receipt');
      feed?.acceptRemoval(response); notice = 'Сообщение удалено.';
    } catch (reason) { if (valid(token)) error = engagementError(reason); }
    finally { busy = false; if (current()) render(); }
  }
  function newMessage(value: DiscussionMessage): MessageRow {
    const item = el('li', 'se-message'); item.dataset.messageId = value.id;
    const top = el('div', 'se-message-header'), author = el('span', 'se-message-author'), time = el('time'); top.append(author, time);
    const body = el('p', 'se-message-body'), replyLabel = el('span', 'se-message-reply');
    const tools = el('div', 'se-message-tools'), respond = button('Ответить', undefined, 'sw-button-quiet'), remove = button('Удалить', 'trash', 'sw-button-quiet');
    respond.dataset.engagementKey = 'discussion-reply'; remove.dataset.engagementKey = 'discussion-remove';
    const row = { element: item, value, author, time, body, replyLabel, reply: respond, remove };
    respond.addEventListener('click', () => { if (canWrite() && activeDraft && row.value.body) { edit(activeDraft.read().draft.text, row.value.id); input.focus(); } });
    remove.addEventListener('click', () => removeMessage(row.value)); tools.append(respond, remove); item.append(top, replyLabel, body, tools); return row;
  }
  function newRetained(value: RetainedDiscussionDraft): RetainedRow {
    const element = el('details', 'se-retained-draft'), summary = el('summary', '', 'Сохранённый черновик'); element.append(summary);
    element.dataset.conversationId = value.scope.conversationId;
    const area = el('textarea'); area.readOnly = true; area.rows = 3; area.setAttribute('aria-label', 'Текст отдельного черновика'); area.dataset.engagementKey = 'discussion-retained-text';
    const info = el('p', 'se-muted'), actions = el('div', 'se-actions');
    const copy = button('Копировать', undefined, 'sw-button-quiet'), remove = button('Удалить черновик', 'trash', 'sw-button-quiet');
    const retry = button('Проверить отправку', 'refresh', 'sw-button-quiet'), abandon = button('Снять ожидание', undefined, 'sw-button-quiet');
    retry.dataset.engagementKey = 'discussion-retained-retry';
    const row = { element, value, text: area, status: info, retry, abandon, remove };
    copy.addEventListener('click', () => {
      if (!foreground()) return; const content = row.value.draft.text;
      if (!view?.navigator.clipboard) { area.focus(); area.select(); notice = 'Текст выделен — скопируйте его.'; render(); return; }
      void view.navigator.clipboard.writeText(content).then(() => { if (foreground()) { notice = 'Черновик скопирован.'; render(); } }).catch(() => { if (foreground()) { area.focus(); area.select(); notice = 'Текст выделен — скопируйте его.'; render(); } });
    });
    retry.addEventListener('click', () => { const expected = row.value.pending; if (foreground() && !busy && expected) void send(draftFor(row.value.scope), { expectedPending: expected }); });
    abandon.addEventListener('click', () => { const expected = row.value.pending; if (foreground() && !busy && expected) showAbandon(draftFor(row.value.scope), expected); });
    remove.addEventListener('click', () => {
      if (!foreground() || busy || row.value.pending) return;
      const captured = row.value, surface = openDialog('Удалить этот черновик?'); surface.body.append(el('p', '', 'Удалится только этот текст на устройстве. Сообщения обсуждения останутся.'));
      const no = button('Оставить', undefined, 'sw-button-quiet', closeDialog), actions = el('div', 'se-actions');
      actions.append(no, button('Удалить черновик', 'trash', 'sw-button-quiet', () => {
        if (!foreground() || busy) return; closeDialog();
        void draftFor(captured.scope).discardDraft(captured.draft).then(() => { if (current()) { refreshRetained(); render(); } }).catch(reason => { if (current()) { error = engagementError(reason); render(); } });
      })); surface.body.append(actions); no.focus();
    });
    actions.append(copy, remove, retry, abandon); element.append(info, area, actions); return row;
  }
  function renderRetained(editorVisible: boolean): void {
    const activeKey = activeDraft ? scopeKey(activeDraft.scope) : null;
    const values = retained.filter(value => (value.draft.text || value.pending) && !(scopeKey(value.scope) === activeKey && editorVisible));
    const ids = new Set(values.map(value => scopeKey(value.scope)));
    for (const [key, row] of retainedRows) if (!ids.has(key)) { row.element.remove(); retainedRows.delete(key); }
    let previous: ChildNode | null = null;
    for (const value of values) {
      const key = scopeKey(value.scope); let row = retainedRows.get(key); if (!row) { row = newRetained(value); retainedRows.set(key, row); }
      row.value = value; if (row.text.value !== value.draft.text) row.text.value = value.draft.text;
      text(row.status, value.pending ? 'Есть неподтверждённая отправка в прежнее обсуждение.' : 'Отдельный текст на этом устройстве. Он не отправится в новый разговор.');
      row.retry.hidden = !value.pending || scopeKey(value.scope) === activeKey; row.abandon.hidden = row.retry.hidden;
      row.remove.hidden = !!value.pending; disabled(row.retry, busy); disabled(row.abandon, busy);
      const reference: ChildNode | null = previous ? previous.nextSibling : retainedBox.firstChild; if (reference !== row.element) retainedBox.insertBefore(row.element, reference); previous = row.element;
    }
    retainedBox.hidden = !values.length;
  }
  function render(): void {
    if (rendering) { if (!renderQueued) { renderQueued = true; queueMicrotask(() => { renderQueued = false; render(); }); } return; }
    if (!current()) { cancelComposerFit(); root.replaceChildren(); closeDialog(); stopPoll(); return; }
    if (!visible) return;
    rendering = true;
    try {
      const state = feed?.read(), context = state?.context ?? null;
      if (context) resolvedConversationId = context.conversationId;
      if (context?.entry) activeDraft = draftFor({ accountId: options.accountId, appId: options.appId, conversationId: context.conversationId, entry: context.entry });
      else if (!context && resolvedConversationId) activeDraft = [...drafts.values()].find(item => item.model.scope.conversationId === resolvedConversationId)?.model ?? null;
      else activeDraft = null;
      const draft = activeDraft?.read(); lastPending = draft?.pending ?? null;
      text(audience, context ? `${context.ownerAdministrative ? 'Управление · ' : ''}${audienceLabel(context.audience)}${context.mode === 'archive' ? ' · Архив' : ''}` : administrative ? 'Управление обсуждением' : '');
      const readonly = !!context && !context.canPost;
      banner.hidden = !readonly && !state?.resetRequired && !((requestedConversationId || resolvedConversationId) && !context && !state?.loading);
      text(bannerText, state?.resetRequired ? 'Обновите историю, чтобы продолжить. Черновик сохранён отдельно.'
        : context?.ownerAdministrative ? 'Просмотр и удаление сообщений владельцем.'
          : context?.mode === 'archive' ? 'Этот разговор завершён. Его история доступна только для чтения.' : context ? 'Этот разговор доступен только для чтения.' : 'Этот разговор сейчас недоступен.');
      currentButton.hidden = !!context?.isCurrent || !entry && !administrative;
      disabled(currentButton, busy || switching);
      archiveFold.hidden = !feed; disabled(refreshButton, switching || !!state?.loading);
      olderBox.hidden = !state?.historyCursor; disabled(olderButton, busy || switching || !!state?.loading);
      latest.hidden = !state?.hasNewer && !state?.resetRequired; disabled(latest, switching || !!state?.loading);
      if (messageConversation !== context?.conversationId) { messages.clear(); list.replaceChildren(); messageConversation = context?.conversationId ?? null; }
      const writable = canWrite();
      const ids = new Set(state?.messages.map(item => item.id) ?? []);
      for (const [id, row] of messages) if (!ids.has(id)) { row.element.remove(); messages.delete(id); }
      let previous: ChildNode | null = null;
      for (const value of state?.messages ?? []) {
        let row = messages.get(value.id); if (!row) { row = newMessage(value); messages.set(value.id, row); }
        row.value = value; row.element.dataset.own = String(value.author.accountId === options.accountId); row.element.dataset.removed = String(value.body === null);
        text(row.author, value.author.label); const date = new Date(value.createdAt), validDate = Number.isFinite(date.getTime()); row.time.dateTime = validDate ? date.toISOString() : '';
        text(row.time, validDate ? date.toLocaleTimeString('ru', { hour: '2-digit', minute: '2-digit' }) : ''); row.time.title = validDate ? date.toLocaleString('ru') : '';
        text(row.body, value.body ?? 'Сообщение удалено'); row.replyLabel.hidden = !value.replyTo;
        text(row.replyLabel, 'В ответ на сообщение'); row.reply.hidden = !writable || value.body === null;
        row.remove.hidden = !value.canRemove || value.body === null; disabled(row.remove, busy || switching);
        const reference: ChildNode | null = previous ? previous.nextSibling : list.firstChild; if (reference !== row.element) list.insertBefore(row.element, reference); previous = row.element;
      }
      empty.hidden = !!state?.messages.length || !!state?.loading || !!error || !!state?.error;
      text(empty, !feed ? 'Этот вход недоступен. Сохранённые на устройстве черновики остаются ниже.' : context?.mode === 'archive' ? 'В этой истории нет доступных сообщений.' : context ? context.canPost ? 'Начните обсуждение приложения.' : 'Пока нет сообщений.' : 'Открываем обсуждение…');
      // A context refresh may clear all server projections. Keep only the
      // account's previous scoped local text visible and read-only until the
      // fresh context arrives; hiding the focused textarea loses selection.
      const checkingOwnDraft = !context && !!state?.loading && !!activeDraft && activeDraft.scope.conversationId === resolvedConversationId;
      const showComposer = foreground() && (!!context?.canPost && !state?.stale && !state?.resetRequired && !!activeDraft || checkingOwnDraft);
      composer.hidden = !showComposer; input.readOnly = switching || checkingOwnDraft; reply.hidden = !showComposer || !draft?.draft.replyTo;
      if (draft && input.value !== draft.draft.text) input.value = draft.draft.text;
      const replyTarget = draft?.draft.replyTo ? messages.get(draft.draft.replyTo)?.value : null;
      text(replyText, replyTarget?.body ? `Ответ: ${replyTarget.author.label}` : 'Ответ на сообщение');
      disabled(sendButton, busy || !writable || !draft?.draft.text.trim() || !!draft?.pending || !!draft?.conflict);
      pendingBox.hidden = !lastPending; disabled(pendingRetry, busy || switching); disabled(pendingAbandon, busy || switching);
      conflict.hidden = !draft?.conflict; disabled(compare, busy || switching);
      const message = error || draft?.error && engagementError(draft.error) || localError || state?.error && engagementError(state.error) || notice
        || (state?.loading && !context ? 'Обновляем…' : draft && !draft.durable ? 'Черновик ещё не сохранён на устройстве.' : '');
      text(status, message); status.dataset.tone = error || draft?.error || localError || state?.error ? 'error' : '';
      renderRetained(showComposer); renderArchives(); scheduleComposerFit();
    } finally { rendering = false; }
  }
  function renderArchives(): void {
    const state = feed?.read(), values = state?.archives ?? [], ids = new Set(values.map(value => value.conversationId));
    for (const [id, node] of archiveRows) if (!ids.has(id)) { node.remove(); archiveRows.delete(id); }
    let previous: ChildNode | null = null;
    for (let index = 0; index < values.length; index++) {
      const value = values[index]!; let node = archiveRows.get(value.conversationId);
      if (!node) { const id = value.conversationId; node = button('', undefined, 'sw-button-quiet', () => { void choose({ conversationId: id, administrative }); }); node.dataset.engagementKey = 'discussion-archive'; archiveRows.set(id, node); }
      text(node.querySelector('span')!, `Разговор ${index + 1} · ${audienceLabel(value.audience)}`); node.setAttribute('aria-pressed', String(state?.context?.conversationId === value.conversationId)); disabled(node, busy || switching);
      const reference: ChildNode | null = previous ? previous.nextSibling : archiveList.firstChild; if (reference !== node) archiveList.insertBefore(node, reference); previous = node;
    }
    text(archiveStatus, archiveError || (!values.length && archiveLoaded && !state?.loading && !switching ? 'Доступных архивов нет.' : ''));
    archiveOlder.hidden = !state?.nextArchiveCursor; disabled(archiveOlder, !!state?.loading || switching); disabled(archiveLatest, !!state?.loading || switching);
  }
  async function flush(): Promise<void> {
    const outcomes = await Promise.all([...drafts.values()].map(item => item.model.flush()));
    if (outcomes.some(value => !value) || [...drafts.values()].some(item => item.model.hasUnsavedChanges())) {
      if (current()) { error = 'Черновик ещё не сохранён. Скопируйте текст или разрешите конфликт перед переходом.'; render(); }
    }
  }
  async function updateSelection(next: DiscussionSelection): Promise<void> {
    if (!current()) return;
    if (switching) throw controlledError('app_discussion_busy');
    switching = true; input.readOnly = true;
    try {
      await flush(); if (!current()) return;
      if ([...drafts.values()].some(item => item.model.hasUnsavedChanges())) throw controlledError('app_discussion_storage_unavailable');
    } finally { switching = false; input.readOnly = false; if (current()) render(); }
    if (!current()) return;
    const admin = next.administrative === true;
    switching = true; selectionVersion++; stopPoll(); closeDialog(); error = ''; notice = ''; archiveError = '';
    requestedConversationId = next.conversationId; resolvedConversationId = undefined; activeDraft = null;
    if (administrative !== admin) { administrative = admin; createFeed(); }
    else feed?.invalidate();
    switching = false; refreshRetained(); render(); await refresh();
  }
  async function choose(next: DiscussionSelection): Promise<void> {
    if (!foreground() || busy || switching) return;
    const context = feed?.read().context;
    const sameSelection = next.conversationId
      ? next.conversationId === requestedConversationId && next.conversationId === context?.conversationId
      : !requestedConversationId && context?.isCurrent === true;
    if ((next.administrative === true) === administrative && sameSelection) return;
    // Selection clears the old feed and its archive buttons. Move only the
    // active opener to a stable landmark now; a later response never takes
    // focus back from the user's next Tab or click.
    const opener = document.activeElement;
    if (opener === currentButton || [...archiveRows.values()].some(node => node === opener)) title.focus({ preventScroll: true });
    try { await updateSelection(next); if (foreground()) options.onConversationChange?.(next); }
    catch (reason) { if (foreground()) { error = engagementError(reason); render(); } }
  }
  async function refresh(): Promise<void> {
    if (!foreground() || !feed || switching || feed.read().loading) return;
    const token = selectionVersion, model = feed; error = ''; archiveError = ''; stopPoll();
    try {
      const result = await model.load({ api: options.api, isCurrent: () => valid(token), ...(requestedConversationId ? { conversationId: requestedConversationId } : {}) });
      if (!valid(token) || result === 'stale') return;
      archiveLoaded = false; refreshRetained(); render();
      if (archiveFold.open) await loadArchives(false);
    } catch (reason) { if (valid(token)) { error = engagementError(reason); refreshRetained(); render(); } }
    finally { if (valid(token)) schedulePoll(); }
  }
  async function poll(afterSend = false): Promise<void> {
    if (!foreground() || !feed || switching || !afterSend && (busy || feed.read().loading)) return;
    const token = selectionVersion, model = feed, nearEnd = scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight < 64;
    const previousIds = new Set(model.read().messages.map(message => message.id));
    try { await model.poll({ api: options.api, isCurrent: () => valid(token) }); if (valid(token)) {
      const count = model.read().messages.filter(message => !previousIds.has(message.id) && message.body !== null).length;
      if (count && !afterSend) notice = count === 1 ? 'Новое сообщение.' : `Новых сообщений: ${count}.`;
      refreshRetained(); render(); if (nearEnd) scroll.scrollTop = scroll.scrollHeight;
    } }
    catch (reason) { if (valid(token)) { error = engagementError(reason); render(); } }
    finally { if (valid(token)) schedulePoll(); }
  }
  async function older(): Promise<void> {
    if (!foreground() || !feed || busy || switching || feed.read().loading) return;
    const token = selectionVersion, before = scroll.scrollHeight, position = scroll.scrollTop, model = feed; error = '';
    try { await model.older({ api: options.api, isCurrent: () => valid(token) }); if (valid(token)) { render(); scroll.scrollTop = position + scroll.scrollHeight - before; } }
    catch (reason) { if (valid(token)) { error = engagementError(reason); render(); } }
  }
  async function loadArchives(older: boolean): Promise<void> {
    if (!foreground() || !feed || switching || feed.read().loading) return;
    const token = selectionVersion, model = feed; archiveError = '';
    try { await model.archives({ api: options.api, isCurrent: () => valid(token), older }); if (valid(token)) archiveLoaded = true; }
    catch (reason) { if (valid(token)) archiveError = engagementError(reason); }
    finally { if (valid(token)) render(); }
  }
  archiveFold.addEventListener('toggle', () => { if (archiveFold.open && !archiveLoaded) void loadArchives(false); });
  function onStorage(): void { if (!current()) return; for (const { model } of drafts.values()) model.refreshLocal(); refreshRetained(); render(); }
  function onVisibility(): void {
    if (!foreground()) { cancelComposerFit(); stopPoll(); feed?.invalidate(); closeDialog(); }
    else { for (const { model } of drafts.values()) model.refreshLocal(); refreshRetained(); void refresh(); }
  }
  const onPageHide = (): void => { for (const { model } of drafts.values()) void model.flush(); };
  document.addEventListener('visibilitychange', onVisibility); view?.addEventListener('storage', onStorage); view?.addEventListener('pagehide', onPageHide);
  createFeed(); refreshRetained(); render(); void refresh();
  return { refresh, updateSelection, flush, hasUnsavedChanges: () => [...drafts.values()].some(item => item.model.hasUnsavedChanges()),
    async updateEntry(value) {
      if (!current()) return;
      const next = normalizeAppEntry(value);
      if (next.appId !== options.appId || entry && !sameAppEntry(entry, next)) throw controlledError('app_discussion_context_changed');
      if (entry) return;
      entry = next; refreshRetained();
      if (!administrative) { selectionVersion++; stopPoll(); createFeed(); render(); await refresh(); }
    },
    focus() { if (!foreground()) return; (canWrite() ? input : refreshButton).focus(); },
    setVisible(value) {
      if (!current() || visible === value) return; visible = value; root.hidden = !value; selectionVersion++; closeDialog(); stopPoll();
      if (!value) { cancelComposerFit(); feed?.invalidate(); onPageHide(); }
      else { onStorage(); void refresh(); }
    },
    dispose() {
      if (disposed) return; disposed = true; selectionVersion++; stopPoll(); unsubscribeFeed?.(); feed?.dispose(); closeDialog();
      cancelComposerFit(); composerResize?.disconnect();
      view?.removeEventListener('resize', onComposerResize); view?.visualViewport?.removeEventListener('resize', onComposerResize);
      document.fonts?.removeEventListener('loadingdone', onComposerFontLoad);
      document.removeEventListener('visibilitychange', onVisibility); view?.removeEventListener('storage', onStorage); view?.removeEventListener('pagehide', onPageHide);
      for (const { model, unsubscribe } of drafts.values()) { unsubscribe(); model.dispose(); } drafts.clear(); root.remove();
    } };
}
