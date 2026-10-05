import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import { button, el, emptyState, initials, labeledField, textInput } from '../world/dom';
import { icon } from '../world/icons';
import type { WorldDialog } from '../world/dialogs';
import { createAppCreateState, type AppCreateDraft, type AppCreateReceipt } from './app-create-state.mjs';
import { createAssistantFocusHandoff } from './app-builder-focus.mjs';
import { appBuilderProposal, appBuilderReceipt, appBuilderLaunchUrl, matchingAppBuilderRegistration, type AppBuilderRegisteredApp } from './app-builder-result.mjs';

interface HostDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean }
interface Proposal { schema: 'soty.local-app.v1'; name: string; port: number; entryPath: string; sourceJobId: string }
export interface AppBuilderTarget { hostDeviceId: string; connectorId: string; jobId: string }
type Pending = AppBuilderTarget;
interface JobResult { job: { schema?: string; kind?: string; attempts?: number; id: string; deviceId: string; connectorId: string; status: string; cancelRequested?: boolean; executionUncertain?: boolean; result: { text: string; textTruncated?: boolean; appProposal?: Proposal } | null }; task?: { text: string }; done?: boolean; cursor?: number; events?: { seq: number; type: string; text: string }[] }
export interface AppActionDependencies { readAgentCapabilities?: () => Promise<{ agentConfigured: boolean }> }
export interface AppBuilderHandle { dispose(): void; flush(): Promise<void>; hasUnsavedChanges(): boolean }
interface InlineSession { close(): void; creation?: ReturnType<typeof createAppCreateState>; handle: AppBuilderHandle }
interface BuilderOptions extends AppActionDependencies {
  client: ConnectClient;
  refresh(): Promise<void>;
  connectDevice(): Promise<void>;
  dialog(title: string, close?: () => void): WorldDialog;
  friendly(error: unknown): string;
  openAccount?: (() => Promise<void>) | undefined;
  createStates: Map<string, ReturnType<typeof createAppCreateState>>;
}
interface BuilderPanel { element: HTMLElement; body: HTMLElement; close(): void }
function remember(key: string, value: Pending | null): void {
  try { if (value) localStorage.setItem(key, JSON.stringify(value)); else localStorage.removeItem(key); } catch { /* The job remains on the server. */ }
}
function recalled(key: string): Pending | null {
  try {
    const value = JSON.parse(localStorage.getItem(key) || 'null') as Pending | null;
    return value && [value.hostDeviceId, value.connectorId, value.jobId].every(item => typeof item === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(item)) ? value : null;
  } catch { return null; }
}
function forgetPending(key: string, expected: Pending): void {
  if (recalled(key)?.jobId === expected.jobId) remember(key, null);
}

/** App creation is separate from the general assistant. Both legacy job resumes
 * and inline views use the production durable intent and account guards. */
export function createAppBuilderAction(options: BuilderOptions) {
  const { client, refresh, connectDevice, dialog, friendly, createStates } = options;
  const sessions = new Set<() => void>(), inlineSessions = new Map<HTMLElement, InlineSession>();

  async function agentCreate(communityId?: string, hostOrRestore?: HTMLElement | Pending): Promise<void> {
    const host = hostOrRestore instanceof HTMLElement ? hostOrRestore : undefined;
    const restore = host ? undefined : hostOrRestore as Pending | undefined;
    if (host && !host.isConnected) return;
    if (host) inlineSessions.get(host)?.close();
    let closed = false, accountInvalid = false, timer: ReturnType<typeof setTimeout> | undefined, following = '', runVersion = 0;
    let formAbort: AbortController | undefined;
    let devices: HostDevice[] = [];
    const eventsAbort = new AbortController();
    const body = el('div', `sx-agent${host ? ' sx-assistant-inline' : ' is-modal'}`);
    let session: InlineSession | undefined;
    let observer: MutationObserver | undefined;
    const focus = createAssistantFocusHandoff({ root: body, getActiveElement: () => document.activeElement,
      isNeutral: element => !element || element === document.body || element === document.documentElement });
    document.addEventListener('focusin', event => focus.observe(event.target instanceof Element ? event.target : null), { signal: eventsAbort.signal });
    document.addEventListener('pointerdown', event => focus.releaseOutside(event.target instanceof Node ? event.target : null), { capture: true, signal: eventsAbort.signal });
    function cleanup(): void {
      if (closed) return;
      closed = true; eventsAbort.abort(); formAbort?.abort(); observer?.disconnect(); if (timer) clearTimeout(timer);
      sessions.delete(panel.close);
      if (host) { if (inlineSessions.get(host) === session) inlineSessions.delete(host); body.remove(); }
    }
    const panel: BuilderPanel = host ? { element: body, body, close: cleanup } : (() => {
      const modal = dialog('Создать с ИИ', cleanup); modal.body.replaceChildren(body);
      return { element: modal.element, body, close: modal.close };
    })();
    if (host) {
      session = { close: panel.close, handle: { dispose: panel.close,
        flush: async () => { if (closed || !session?.creation) return; if (await current()) await session.creation.flush(); },
        hasUnsavedChanges: () => !closed && Boolean(session?.creation?.hasUnsavedChanges()) } };
      host.replaceChildren(body); inlineSessions.set(host, session);
    }
    sessions.add(panel.close);
    const connected = () => !closed && panel.element.isConnected && (!host || host.isConnected && host.contains(body));
    observer = new MutationObserver(() => { if (!connected()) panel.close(); });
    observer.observe(document.documentElement, { subtree: true, childList: true });
    let identity: Awaited<ReturnType<ConnectClient['getLocalState']>>;
    try { identity = await client.getLocalState(); }
    catch (cause) { if (connected()) body.replaceChildren(emptyState('Не удалось открыть задачу', friendly(cause), button('Повторить', 'refresh', 'sw-button-primary', () => { panel.close(); void agentCreate(communityId, host); }))); return; }
    if (closed) return;
    if (!identity.accountId) {
      body.replaceChildren(emptyState('Нужен ваш профиль', 'Откройте профиль, чтобы сохранить личную задачу и выбрать компьютер.',
        button('Открыть профиль', 'user', 'sw-button-primary', () => { panel.close(); void options.openAccount?.(); })));
      return;
    }
    const accountId = identity.accountId, pendingKey = `soty.world.agent:${accountId}`;
    let storage: Pick<Storage, 'getItem' | 'setItem'>;
    try { storage = localStorage; } catch { storage = { getItem() { throw new Error('storage_unavailable'); }, setItem() { throw new Error('storage_unavailable'); } }; }
    let drafts = createStates.get(accountId);
    if (!drafts) { drafts = createAppCreateState({ accountId, storage, locks: navigator.locks }); createStates.set(accountId, drafts); }
    const creation = drafts;
    if (session) session.creation = creation;
    const note = el('p', 'sx-agent-loading sw-muted', 'Загружаем ваши устройства…'); note.setAttribute('role', 'status'); panel.body.append(note);
    async function sameAccount(): Promise<boolean> {
      if (accountInvalid) return false;
      let state: Awaited<ReturnType<ConnectClient['getLocalState']>>;
      try { state = await client.getLocalState(); } catch { accountInvalid = true; panel.close(); return false; }
      if (state.accountId !== accountId) {
        accountInvalid = true; createStates.delete(accountId); panel.body.replaceChildren(); if (!closed) panel.close(); return false;
      }
      return !accountInvalid;
    }
    async function current(): Promise<boolean> {
      if (!connected()) { panel.close(); return false; }
      return await sameAccount() && connected();
    }
    const request = async <T>(op: string, args: Record<string, unknown>): Promise<T> => {
      if (!await current()) throw Object.assign(new Error('authentication_required'), { code: 'authentication_required' });
      let result: T;
      try {
        const payload = op.startsWith('apps.agent.') || op.startsWith('apps.assistant.') ? { ...args, expectedAccountId: accountId } : args;
        result = await client.extension<T>(op, payload, { expectedAccountId: accountId });
      }
      catch (cause) {
        if (cause && typeof cause === 'object' && 'code' in cause && ['authentication_required', 'device_revoked', 'device_not_found'].includes(String(cause.code))) {
          accountInvalid = true; createStates.delete(accountId); panel.body.replaceChildren(); if (!closed) panel.close();
        }
        throw cause;
      }
      if (!await current()) throw Object.assign(new Error('authentication_required'), { code: 'authentication_required' });
      return result;
    };
    async function forget(pending: Pending): Promise<boolean> {
      if (!await current()) return false;
      const cleared = await creation.clearAccepted(pending);
      if (cleared) forgetPending(pendingKey, pending);
      return cleared;
    }
    function newTask(pending: Pending): void {
      void forget(pending).then(async () => {
        if (!await current()) return;
        if (host) { following = ''; runVersion++; if (timer) clearTimeout(timer); await showForm(false); }
        else { panel.close(); void agentCreate(communityId); }
      })
        .catch(cause => { if (!closed) { note.textContent = friendly(cause); panel.body.append(note); } });
    }
    function workspace(): { conversation: HTMLElement; output: HTMLElement; claim: ReturnType<typeof focus.capture> } {
      const claim = focus.capture(), layout = el('div', 'sx-agent-workspace');
      const conversation = el('section', 'sx-agent-conversation'), output = el('section', 'sx-agent-output');
      conversation.setAttribute('aria-label', 'Задача помощника'); output.setAttribute('aria-label', 'Результат помощника');
      layout.append(conversation, output); body.replaceChildren(layout); return { conversation, output, claim };
    }
    function placeholder(output: HTMLElement, title = 'Здесь появится приложение', detail = 'Опишите идею. Помощник подготовит личный черновик.'): void {
      const content = el('div', 'sx-agent-placeholder'), mark = el('span', 'sx-agent-symbol'); mark.append(icon('sparkle'));
      content.append(mark, el('h2', '', title), el('p', 'sw-muted', detail)); output.replaceChildren(content);
    }
    function bubble(text: string, author: 'user' | 'assistant'): HTMLElement {
      const row = el('div', `sx-agent-message is-${author}`), mark = el('span', `sx-agent-avatar${author === 'assistant' ? ' sx-agent-symbol' : ''}`);
      if (author === 'assistant') mark.append(icon('sparkle')); else mark.textContent = initials('Я');
      const copy = el('div', 'sx-agent-message-copy'); copy.append(el('p', '', text)); row.append(mark, copy); return row;
    }
    function deviceContext(pending: Pick<Pending, 'hostDeviceId' | 'connectorId'>): HTMLElement {
      const device = devices.find(value => value.hostDeviceId === pending.hostDeviceId && value.connectorId === pending.connectorId);
      const context = el('div', 'sx-agent-current-device'), copy = el('span'); context.append(icon('laptop'));
      copy.append(el('strong', '', device?.name || 'Выбранный компьютер'), el('small', '', device ? device.online ? 'На связи' : 'Не в сети' : 'Статус устройства уточняется')); context.append(copy); return context;
    }
    function label(result: JobResult): string {
      const job = result.job;
      if (job.executionUncertain) return 'Результат не подтверждён';
      if (job.cancelRequested && !['succeeded', 'failed', 'cancelled'].includes(job.status)) return 'Остановка запрошена · ждём устройство';
      return ({ queued: 'Ждём ваш компьютер', leased: 'Агент подключился', running: 'Создаём приложение',
        cancelled: 'Задача отменена · изменения не откатываются', failed: 'Нужно проверить результат', succeeded: 'Выполнение завершено' } as Record<string, string>)[job.status] || 'Уточняем состояние';
    }

    async function showResult(result: JobResult, pending: Pending): Promise<void> {
      if (!await current()) return;
      const proposal = appBuilderProposal(result, pending), { conversation: content, output, claim } = workspace();
      const transcript = el('div', 'sx-agent-transcript');
      if (typeof result.task?.text === 'string' && result.task.text.trim()) transcript.append(bubble(result.task.text.slice(0, 16_000), 'user'));
      const assistantReply = bubble(proposal ? 'Черновик готов. Добавьте его в свои соты, чтобы открыть.' : label(result), 'assistant');
      transcript.append(assistantReply); content.append(transcript);
      const actions = el('div', 'sx-agent-result-actions'), actionState = el('div', 'sx-agent-action-state'); content.append(actions, actionState);
      const reset = button('Новая задача', 'plus', 'sw-button-quiet', () => newTask(pending));
      const report = (cause: unknown) => { const error = el('p', 'sw-error sx-agent-error', friendly(cause)); error.setAttribute('role', 'alert'); actionState.replaceChildren(error); };
      if (proposal) {
        let registered: AppBuilderRegisteredApp | null = null, registering = false;
        const card = el('article', 'sx-agent-draft'), heading = el('header', 'sx-agent-draft-heading'), mark = el('span', 'sx-agent-symbol'); mark.append(icon('grid'));
        const state = el('span', 'sx-agent-draft-state', 'Черновик'); state.setAttribute('role', 'status'); heading.append(mark, el('h2', '', proposal.name), state); card.append(heading);
        const visual = el('div', 'sx-agent-draft-visual'), summary = el('div', 'sx-agent-draft-summary'), appMark = el('span', 'sx-agent-draft-mark sx-agent-symbol'); appMark.append(icon('grid'));
        summary.append(appMark, el('h3', '', proposal.name), el('p', '', 'Готово к добавлению в ваши соты'));
        const access = el('span', 'sx-agent-draft-access'); access.append(icon('lock'), el('span', '', 'Личный черновик')); visual.append(summary, access); card.append(visual); output.replaceChildren(card);
        const add = button('Добавить в мои соты', 'plus', 'sw-button-primary', () => {
          if (registering) return;
          registering = true; add.disabled = true; actionState.replaceChildren();
          void (async () => {
            try {
              const response = await request<{ app: AppBuilderRegisteredApp }>('apps.register', { hostDeviceId: pending.hostDeviceId, connectorId: pending.connectorId,
                name: proposal.name, port: proposal.port, entryPath: proposal.entryPath, grants: { accountIds: [], communityIds: [] } });
              if (!await current()) return;
              registered = response.app; renderActions();
              await forget(pending);
              if (!host) await refresh();
            } catch (cause) {
              if (!await current()) return;
              if (!registered) {
                try {
                  const existing = await request<{ apps: AppBuilderRegisteredApp[] }>('apps.list', {});
                  if (!await current()) return;
                  registered = matchingAppBuilderRegistration(existing.apps, pending, proposal, accountId);
                  if (registered) { renderActions(); await forget(pending); if (!host) await refresh(); return; }
                } catch { /* Keep the original failure and the exact durable result. */ }
              }
              if (await current()) report(cause);
            }
            finally { registering = false; if (await current()) add.disabled = false; }
          })();
        });
        function renderActions(): void {
          const handoff = focus.capture(actions);
          if (!registered) { actions.replaceChildren(add, reset); return; }
          assistantReply.querySelector('p')!.textContent = 'Добавлено в ваши соты. Можно открыть приложение.';
          state.textContent = 'Добавлено'; state.classList.add('is-added');
          access.replaceChildren(icon('check'), el('span', '', registered.grants && (registered.grants.accountIds.length || registered.grants.communityIds.length) ? 'Доступ настроен' : 'Только вам'));
          summary.querySelector('p')!.textContent = registered.state === 'offline' ? 'Устройство не в сети' : registered.state === 'stopped' ? 'Приложение остановлено' : 'Ваше приложение в Сотах';
          const launch = button('Открыть приложение', 'external', 'sw-button-primary', () => {
            launch.disabled = true; actionState.replaceChildren();
            void (async () => {
              try {
                if (!registered || !await current()) return;
                const response = await request<{ launchUrl: string }>('apps.launch', { appId: registered.id });
                if (!await current()) return;
                const url = appBuilderLaunchUrl(response.launchUrl, location.href); if (!url) throw new Error('invalid_application_origin');
                const frame = el('iframe', 'sx-agent-preview-frame'); frame.src = url; frame.title = proposal!.name;
                frame.setAttribute('sandbox', 'allow-scripts allow-forms allow-same-origin'); frame.referrerPolicy = 'no-referrer';
                visual.replaceChildren(frame); visual.classList.add('has-preview'); state.textContent = 'Предпросмотр';
              } catch (cause) { if (await current()) report(cause); }
              finally { if (await current()) launch.disabled = false; }
            })();
          });
          const external = button('В новой вкладке', 'arrow', 'sw-button-quiet', () => {
            const target = window.open('about:blank', '_blank'); if (!target) { report(new Error('popup_blocked')); return; } target.opener = null;
            void (async () => {
              try {
                if (!registered || !await current()) { target.close(); return; }
                const response = await request<{ launchUrl: string }>('apps.launch', { appId: registered.id });
                if (!await current()) { target.close(); return; }
                const url = appBuilderLaunchUrl(response.launchUrl, location.href); if (!url) throw new Error('invalid_application_origin'); target.location.replace(url);
              } catch (cause) { target.close(); if (await current()) report(cause); }
            })();
          });
          actions.replaceChildren(launch, external, reset); focus.restore(handoff, launch);
        }
        renderActions(); focus.restore(claim, add);
        void (async () => {
          try {
            const response = await request<{ apps: AppBuilderRegisteredApp[] }>('apps.list', {}); if (!await current() || registering) return;
            const recovered = matchingAppBuilderRegistration(response.apps, pending, proposal, accountId);
            if (recovered) { registered = recovered; await forget(pending); if (await current()) renderActions(); }
          } catch { /* A read failure does not authorize automatic registration or grant changes. */ }
        })();
        if (communityId) content.append(el('small', 'sw-muted', 'Доступ сообществу можно открыть в настройках приложения.'));
      } else {
        placeholder(output, label(result), result.job.executionUncertain ? 'Устройство не подтвердило завершение. Проверьте ответ перед повтором.' : 'Ответ и состояние задачи сохранены в деталях.');
        actions.append(reset); focus.restore(claim, reset);
      }
      const detail = el('details', 'sw-agent-details'); detail.append(el('summary', '', 'Ответ агента'), el('pre', '', result.job.result?.text || 'Результат недоступен.'));
      if (result.job.result?.textTruncated) {
        const download = button('Скачать полный ответ', 'download', 'sw-button-quiet', () => {
          download.disabled = true;
          void (async () => {
            try {
              const parts: string[] = []; let offset: number | null = 0, length = 0;
              while (offset !== null && await current()) {
                const page: { text: string; nextOffset: number | null } = await request('apps.agent.result', { ...pending, offset });
                if (typeof page.text !== 'string' || (length += page.text.length) > 4_000_000 || parts.length >= 500
                  || (page.nextOffset !== null && (!Number.isSafeInteger(page.nextOffset) || page.nextOffset <= offset))) throw new Error('invalid_result_page');
                parts.push(page.text); offset = page.nextOffset;
              }
              if (!await current()) return;
              const url = URL.createObjectURL(new Blob(parts, { type: 'text/plain;charset=utf-8' }));
              const link = el('a'); link.href = url; link.download = 'soty-agent-result.txt'; link.click();
              setTimeout(() => URL.revokeObjectURL(url), 1000);
            } catch (cause) { if (await current()) { note.textContent = friendly(cause); content.append(note); } }
            finally { if (!closed) download.disabled = false; }
          })();
        });
        detail.append(el('small', 'sw-muted', 'Показан фрагмент длинного ответа.'), download);
      }
      content.append(detail, deviceContext(pending));
    }

    function follow(pending: Pending): void {
      if (closed || following === pending.jobId) return;
      const version = ++runVersion;
      formAbort?.abort();
      following = pending.jobId; if (timer) clearTimeout(timer);
      remember(pendingKey, pending);
      const { conversation: activity, output, claim } = workspace();
      const progress = bubble('Задача принята', 'assistant'); progress.setAttribute('role', 'status'); progress.tabIndex = -1;
      const progressCopy = progress.querySelector('p')!;
      const transcript = el('div', 'sx-agent-transcript'); transcript.append(progress); let taskShown = false;
      const events = el('ol', 'sw-agent-events'); events.setAttribute('aria-label', 'Ход создания');
      const history = el('details', 'sw-agent-details sx-agent-details'); history.append(el('summary', '', 'Ход задачи'), events);
      const cancel = button('Остановить', 'close', 'sw-button-quiet', () => {
        cancel.disabled = true;
        void request<JobResult>('apps.agent.cancel', { ...pending }).then(result => { if (!closed) progressCopy.textContent = label(result); })
          .catch(async error => { if (await current()) { progressCopy.textContent = friendly(error); cancel.disabled = false; } });
      });
      const context = deviceContext(pending);
      activity.append(transcript, history, el('small', 'sw-muted', 'Можно вернуться позже — задача сохранена.'), cancel, context);
      placeholder(output, 'Создаём приложение', 'Состояние и ответ будут доступны здесь.'); focus.restore(claim, progress);
      void request<{ devices: HostDevice[] }>('apps.devices', {}).then(async result => {
        if (!await current()) return; devices = result.devices; context.replaceChildren(...Array.from(deviceContext(pending).childNodes));
      }).catch(() => { /* An unknown device status is never shown as online. */ });
      let cursor = 0;
      const poll = async () => {
        if (!await current() || version !== runVersion) return;
        try {
          const result = await request<JobResult>('apps.agent.read', { ...pending, after: cursor });
          if (!await current() || version !== runVersion) return;
          if (!taskShown && typeof result.task?.text === 'string' && result.task.text.trim()) { transcript.prepend(bubble(result.task.text.slice(0, 16_000), 'user')); taskShown = true; }
          for (const event of result.events || []) {
            if (!event.text || event.seq <= cursor) continue;
            const text = event.text.length > 280 ? event.text.slice(0, 280) + '…' : event.text;
            events.append(el('li', '', text)); while (events.children.length > 5) events.firstElementChild?.remove();
          }
          cursor = Math.max(cursor, result.cursor ?? 0, ...(result.events || []).map(event => event.seq));
          progressCopy.textContent = label(result);
          output.querySelector('h2')!.textContent = label(result);
          cancel.disabled = Boolean(result.job.cancelRequested);
          if (result.done) { await showResult(result, pending); return; }
        } catch (error) {
          if (!await current() || version !== runVersion) return;
          progressCopy.textContent = friendly(error);
          const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
          if (code === 'job_not_found') {
            const handoff = focus.capture(), reset = button('Новая задача', 'plus', 'sw-button-primary', () => newTask(pending));
            panel.body.replaceChildren(emptyState('Эта задача больше недоступна', 'Уже добавленные приложения остаются в ваших сотах.', reset, 'sparkle')); focus.restore(handoff, reset);
            return;
          }
        }
        if (!closed && version === runVersion) timer = setTimeout(() => { void poll(); }, document.hidden ? 8000 : 2000);
      };
      void poll();
    }

    async function showForm(useRestore = true): Promise<void> {
    formAbort?.abort(); formAbort = new AbortController(); const formSignal = formAbort.signal;
    const saved = creation.read();
    const known = (useRestore ? restore : undefined) ?? saved.accepted ?? recalled(pendingKey);
    if (!saved.pending && known) {
      try { await creation.adoptAccepted(known); } catch { /* A known job remains readable even when browser storage is unavailable. */ }
      if (await current()) follow(known);
      return;
    }
    const form = el('form', 'sw-stack sx-agent-form'), hosts = el('select', 'sw-input');
    const heading = el('div', 'sx-agent-form-heading'); heading.append(el('h2', '', 'Что создаём?'), el('p', 'sw-muted', 'Опишите идею. Помощник подготовит личный черновик.'));
    const prompt = el('textarea', 'sw-input sx-agent-prompt'); prompt.rows = 4; prompt.maxLength = 16_000; prompt.required = true; prompt.placeholder = 'Например, общий список покупок';
    const cwd = textInput('', 'Выбрать существующую папку', 2000);
    const advanced = el('details', 'sw-agent-details');
    advanced.append(el('summary', '', 'Папка проекта'), labeledField('Полный путь', cwd, 'Оставьте пустым — создадим отдельную папку.'));
    const privilege = el('details', 'sw-agent-details'); privilege.append(el('summary', '', 'Работает с правами вашего пользователя'),
      el('p', 'sw-muted', 'На выбранном устройстве агент может читать файлы, изменять проект и запускать команды. Это ваш доверенный исполнитель; отдельная изоляция внешних агентов пока не включена.'));
    const error = el('p', 'sw-error'); error.setAttribute('role', 'alert');
    const status = el('p', 'sw-muted'); status.setAttribute('role', 'status');
    const create = button('Создать', 'sparkle', 'sw-button-primary sx-agent-submit'); create.type = 'submit';
    const availability = el('div', 'sw-stack'), recovery = el('div', 'sw-stack'); recovery.hidden = true;
    const footer = el('div', 'sx-agent-form-footer'), personal = el('span', 'sx-agent-personal'); personal.append(icon('lock'), el('span', '', 'Личный черновик')); footer.append(personal, create);
    form.append(heading, labeledField('Идея приложения', prompt), labeledField('На устройстве', hosts), advanced, availability, status, error, recovery, footer, privilege);
    const initial = workspace(); initial.conversation.append(form); placeholder(initial.output);
    let modelReady = false, busy = false, loading = true, editGeneration = 0, loadGeneration = 0, closeGeneration = 0;
    const keyOf = (value: Pick<AppCreateDraft, 'hostDeviceId' | 'connectorId'>) => JSON.stringify([value.hostDeviceId, value.connectorId]);
    function requestClose(action: () => void = panel.close, event?: Event): void {
      event?.preventDefault(); event?.stopImmediatePropagation();
      if (closed) return;
      if (accountInvalid || !creation.hasVolatileDraft()) { action(); return; }
      const requested = ++closeGeneration, generation = editGeneration;
      recovery.hidden = false;
      const saving = el('p', 'sw-muted', 'Сохраняем черновик…'); saving.setAttribute('role', 'status'); recovery.replaceChildren(saving);
      void creation.flush().then(() => {
        if (closed || requested !== closeGeneration || generation !== editGeneration) return;
        recovery.hidden = true; error.textContent = ''; updateControls();
        if (!creation.hasVolatileDraft()) action();
      }).catch(cause => {
        if (closed || requested !== closeGeneration || generation !== editGeneration) return;
        error.textContent = friendly(cause);
        recovery.replaceChildren(el('p', 'sw-error', 'Черновик не сохранён. Сохраните его или скачайте текст перед закрытием.'),
          button('Сохранить снова', 'refresh', 'sw-button-quiet', () => { requestClose(action); }),
          button('Скачать черновик', 'download', 'sw-button-quiet', () => {
            const value = creation.read().draft, url = URL.createObjectURL(new Blob([value.text, '\n\nПапка: ', value.cwd], { type: 'text/plain;charset=utf-8' }));
            const link = el('a'); link.href = url; link.download = 'soty-app-draft.txt'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
          }),
          button('Закрыть без несохранённых изменений', 'close', 'sw-button-quiet', () => { creation.discardLocalDraft(); action(); }));
      });
    }
    panel.element.addEventListener('cancel', event => { requestClose(panel.close, event); }, { capture: true, signal: formSignal });
    panel.element.querySelector('.sw-dialog-header button')?.addEventListener('click', event => { requestClose(panel.close, event); }, { capture: true, signal: formSignal });
    function fill(preserveInput = false): void {
      const snapshot = creation.read(), bound = snapshot.pending?.payload ?? snapshot.draft;
      const keep = preserveInput && !snapshot.pending;
      const selectedKey = keep ? hosts.value : bound.hostDeviceId && bound.connectorId ? keyOf(bound) : '';
      if (!keep) { prompt.value = bound.text; cwd.value = bound.cwd; }
      hosts.replaceChildren(el('option', '', 'Выберите устройство'));
      hosts.options[0]!.value = '';
      for (const device of devices) { const option = el('option', '', `${device.name}${device.online ? '' : ' · не в сети'}`); option.value = keyOf(device); hosts.append(option); }
      if (selectedKey && !devices.some(device => keyOf(device) === selectedKey)) {
        const missing = el('option', '', 'Выбранное устройство недоступно'); missing.value = selectedKey; hosts.append(missing);
      }
      hosts.value = selectedKey;
      updateControls();
    }
    function updateControls(): void {
      if (closed || following) return;
      const snapshot = creation.read(), pending = snapshot.pending;
      prompt.readOnly = Boolean(pending) || busy; hosts.disabled = Boolean(pending) || busy; cwd.readOnly = Boolean(pending) || busy;
      create.replaceChildren(icon(pending ? 'refresh' : 'sparkle'), el('span', '', pending ? 'Проверить отправку' : 'Создать'));
      create.disabled = busy || !creation.canDispatch() || (!pending && (loading || !modelReady || !prompt.value.trim() || !devices.some(device => keyOf(device) === hosts.value)));
      status.textContent = creation.hasUnsavedChanges() ? 'Черновик только в этом окне. Перед отправкой нужно сохранить его в браузере.'
        : pending ? 'Подтверждение не получено. Проверим ту же отправку, не создавая другую задачу.' : prompt.value ? 'Черновик сохранён на этом устройстве.' : '';
      if (!creation.canDispatch()) error.textContent = friendly(new Error('app_create_lock_unavailable'));
    }
    async function saveDraft(): Promise<void> {
      const revision = ++editGeneration, device = devices.find(value => keyOf(value) === hosts.value);
      closeGeneration++; recovery.hidden = true;
      const previous = creation.read().draft;
      const patch = { text: prompt.value, cwd: cwd.value, hostDeviceId: device?.hostDeviceId ?? previous.hostDeviceId,
        connectorId: device?.connectorId ?? previous.connectorId };
      try {
        const staged = creation.stageDraft(patch);
        updateControls();
        if (!await sameAccount()) return;
        if (revision !== editGeneration) return;
        await creation.persistDraft(staged);
        if (!closed && revision === editGeneration) { recovery.hidden = true; error.textContent = ''; }
      } catch (cause) { if (!closed && revision === editGeneration) error.textContent = friendly(cause); }
      if (!closed && revision === editGeneration) updateControls();
    }
    prompt.addEventListener('input', () => { void saveDraft(); }, { signal: formSignal }); cwd.addEventListener('input', () => { void saveDraft(); }, { signal: formSignal });
    hosts.addEventListener('change', () => { if (cwd.value) { cwd.value = ''; error.textContent = 'Выбрано другое устройство. Проверьте папку проекта.'; } void saveDraft(); }, { signal: formSignal });
    async function loadDevices(): Promise<void> {
      const version = ++loadGeneration; loading = true; updateControls();
      const values = await Promise.allSettled([request<{ devices: HostDevice[] }>('apps.devices', {}),
        options.readAgentCapabilities ? options.readAgentCapabilities() : fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.any([AbortSignal.timeout(8000), formSignal]) }).then(async response => {
          if (!response.ok) throw new TypeError('Network unavailable'); return await response.json() as { agentConfigured: boolean };
        })]);
      if (!await current() || formSignal.aborted || version !== loadGeneration) return;
      loading = false;
      devices = values[0].status === 'fulfilled' ? values[0].value.devices : [];
      modelReady = values[1].status === 'fulfilled' && values[1].value.agentConfigured === true;
      availability.replaceChildren();
      if (values[0].status === 'rejected') {
        const failure = el('p', 'sw-error', 'Не удалось загрузить ваши устройства. Проверьте соединение и повторите.'); failure.setAttribute('role', 'alert'); availability.append(failure);
      } else if (!devices.length) availability.append(el('p', 'sw-muted', 'Нет доступного компьютера для новой задачи.'),
        button('Подключить компьютер', 'laptop', 'sw-button-quiet', () => { requestClose(() => { if (!host) panel.close(); void connectDevice(); }); }));
      if (!modelReady) availability.append(el('p', 'sw-muted', values[1].status === 'rejected' ? 'Не удалось проверить доступность ИИ. Сохранённую отправку можно проверить.' : 'Подключение к ИИ пока недоступно. Сохранённую отправку можно проверить.'));
      if (!devices.length || !modelReady) availability.append(button('Проверить доступность', 'refresh', 'sw-button-quiet', () => { void loadDevices(); }));
      fill(true);
    }
    form.addEventListener('submit', event => {
      event.preventDefault(); if (busy) return;
      void (async () => {
        if (!await current()) return;
        busy = true; error.textContent = ''; updateControls();
        try {
          const snapshot = creation.read(), device = devices.find(value => keyOf(value) === hosts.value);
          if (snapshot.accepted) { follow(snapshot.accepted); return; }
          if (!snapshot.pending && (!device || !modelReady || !prompt.value.trim())) return;
          // Re-read under the lock on every retry; even an already visible
          // pending cannot dispatch through an unreadable storage record.
          const pending = await creation.prepare(snapshot.pending?.payload ?? { expectedAccountId: accountId, hostDeviceId: device!.hostDeviceId,
            connectorId: device!.connectorId, text: prompt.value, cwd: cwd.value });
          if (!await current()) return;
          fill();
          // A late response may settle the durable intent after the modal closes,
          // but only in the same account. It must never repaint a closed dialog.
          const result = await client.extension<JobResult | { admission: AppCreateReceipt }>('apps.agent.create', { ...pending.payload }, { expectedAccountId: accountId });
          if (!await sameAccount()) return;
          if ('admission' in result) {
            await creation.reject(pending.payload, result.admission);
            if (!closed) { if (result.admission.reason === 'app_model_unavailable') modelReady = false; fill(); error.textContent = friendly({ code: result.admission.reason }); }
            return;
          }
          if (!appBuilderReceipt(result.job, pending.payload)) throw new Error('invalid_app_create_receipt');
          const accepted = { hostDeviceId: pending.payload.hostDeviceId, connectorId: pending.payload.connectorId, jobId: result.job.id };
          const acknowledged = await creation.acknowledge(pending.payload, accepted);
          if (await current() && (acknowledged || creation.read().accepted?.jobId === accepted.jobId)) follow(accepted);
        } catch (cause) {
          if (cause && typeof cause === 'object' && 'code' in cause && ['authentication_required', 'device_revoked', 'device_not_found'].includes(String(cause.code))) {
            accountInvalid = true; createStates.delete(accountId); panel.body.replaceChildren(); if (!closed) panel.close();
          } else if (await current()) { const accepted = creation.read().accepted; if (accepted) follow(accepted); else { fill(); error.textContent = friendly(cause); } }
        }
        finally { busy = false; if (await current()) updateControls(); }
      })();
    }, { signal: formSignal });
    window.addEventListener('storage', event => {
      if (event.key !== creation.key) return;
      void current().then(valid => { if (!valid || busy || following) return; const value = creation.read(); if (value.accepted) follow(value.accepted); else fill(); });
    }, { signal: formSignal });
    fill(); focus.restore(initial.claim, prompt);
    if (!host && document.activeElement && panel.element.contains(document.activeElement)) prompt.focus();
    try {
      await loadDevices();
    } catch (cause) { if (await current()) { error.textContent = friendly(cause); loading = false; updateControls(); } }
    }
    await showForm();
  }

  async function mountInline(host: HTMLElement): Promise<AppBuilderHandle> {
    const pending = agentCreate(undefined, host);
    // Capture before the first identity await. A stale returned handle must
    // never close or flush the next session mounted in the same host.
    const mounted = inlineSessions.get(host);
    // Install the navigation guard before devices/capabilities finish loading.
    // The form can already accept input while those reads are in flight.
    void pending.catch(cause => {
      if (!host.isConnected || inlineSessions.get(host) !== mounted) return;
      const failure = el('p', 'sw-error sx-agent-error', friendly(cause)); failure.setAttribute('role', 'alert');
      host.querySelector('.sx-agent')?.append(failure);
    });
    return mounted?.handle ?? { dispose() {}, async flush() {}, hasUnsavedChanges: () => false };
  }
  return { open: agentCreate, mountInline, destroy: () => { for (const close of [...sessions]) close(); } };
}
