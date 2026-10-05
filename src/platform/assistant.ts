import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import type { WorldAssistantHandle } from '../world/types';
import { button, el, iconButton, labeledField, textInput } from '../world/dom';
import { icon } from '../world/icons';
import { createAssistantState, type AssistantTarget } from './assistant-state.mjs';
import { deviceKey as keyOf, resolveDeviceKey } from './device-key.mjs';
import './assistant.css';

interface Host { hostDeviceId: string; connectorId: string; name: string; online: boolean }
interface Task extends Host { id: string; threadId: string; text: string; cwd: string; appCreation: boolean; status: string; createdAt: string }
interface Job { id: string; threadId: string; status: string; cancelRequested: boolean; executionUncertain: boolean;
  result?: { text: string; textTruncated?: boolean } | null }
interface ReadResult { job: Job; task?: { text: string; cwd: string; appCreation: boolean; canContinue: boolean }; events?: { seq: number; text: string; type: string }[]; cursor?: number; done?: boolean; more?: boolean }
interface Options { client: ConnectClient; connectDevice(): void | Promise<void>; createApp(): void | Promise<void>; resumeApp(target: AssistantTarget): void | Promise<void> }
const finished = (status: string) => ['succeeded', 'failed', 'cancelled'].includes(status);
function friendly(cause: unknown): string {
  const code = cause && typeof cause === 'object' && 'code' in cause ? String(cause.code) : cause instanceof Error ? cause.message : '';
  return ({ app_model_unavailable: 'Подключение к модели пока недоступно. Черновик сохранён.', apps_device_not_owned: 'Доступ к этому устройству изменился. Выберите другое устройство.',
    assistant_continuation_unavailable: 'Этот разговор нельзя продолжить. Ответ сохранён в истории; начните новый разговор.',
    assistant_conversation_busy: 'В этом разговоре ещё выполняется задача. Дождитесь её завершения и проверьте отправку.',
    assistant_conversation_stale: 'Разговор уже продолжили в другом окне. Откройте последний ответ из истории.',
    assistant_pending_unconfirmed: 'Проверьте предыдущую отправку — её подтверждение ещё не получено.',
    assistant_storage_unavailable: 'Браузер не смог сохранить отправку. Освободите место и повторите. Текст остаётся здесь.',
    authentication_required: 'Аккаунт изменился. Откройте помощника заново.', job_not_found: 'Эта задача больше недоступна.',
    invalid_workspace: 'Укажите полный путь к папке на выбранном компьютере.', job_request_conflict: 'Параметры этой отправки не совпадают с принятой задачей. Проверьте историю.' } as Record<string, string>)[code]
    || 'Не удалось выполнить действие. Проверьте соединение и повторите.';
}
function jobLabel(job: Pick<Job, 'status' | 'cancelRequested' | 'executionUncertain'>): string {
  if (job.executionUncertain) return 'Связь с исполнителем потеряна · результат уточняется';
  if (job.cancelRequested && !finished(job.status)) return 'Остановка запрошена · ждём устройство';
  return ({ queued: 'Ждём устройство', leased: 'Помощник подключается', running: 'Выполняется', succeeded: 'Выполнение завершено', failed: 'Нужно проверить результат', cancelled: 'Задача отменена · изменения не откатываются' } as Record<string, string>)[job.status] || 'Уточняем состояние';
}

export async function mountAssistant(host: HTMLElement, options: Options): Promise<WorldAssistantHandle> {
  const identity = await options.client.getLocalState();
  if (!identity.accountId) throw new Error('authentication_required');
  const accountId = identity.accountId;
  let storage: Pick<Storage, 'getItem' | 'setItem'>, tabStorage: Pick<Storage, 'getItem' | 'setItem'>;
  try { storage = localStorage; tabStorage = sessionStorage; }
  catch { storage = tabStorage = { getItem: () => { throw new Error('storage_unavailable'); }, setItem: () => { throw new Error('storage_unavailable'); } }; }
  const drafts = createAssistantState({ accountId, storage, tabStorage });
  let draft = drafts.read(), disposed = false, sending = false, devices: Host[] = [], modelReady = false;
  let selected: AssistantTarget | null = null, activeJob: Job | null = null, continuable = false, generation = 0, refreshGeneration = 0, deviceRefreshGeneration = 0;
  let timer: ReturnType<typeof setTimeout> | undefined, cursor: string | null = null;
  let historyState: 'loading' | 'ready' | 'error' = 'loading';
  const abort = new AbortController(), tasks = new Map<string, Task>();
  const lock = () => {
    if (disposed) return; disposed = true; generation++; abort.abort(); if (timer) clearTimeout(timer);
    const message = el('div', 'sw-assistant'); message.append(el('h2', '', 'Аккаунт изменился'), el('p', 'sw-muted', 'Содержимое закрыто. Откройте помощника в текущем профиле.'), button('Открыть заново', 'refresh', '', () => location.reload())); host.replaceChildren(message);
  };
  const current = async () => {
    if (disposed) return false;
    const identity = await options.client.getLocalState();
    if (disposed) return false;
    if (identity.accountId !== accountId) { lock(); return false; } return true;
  };
  const request = async <T>(op: string, args: Record<string, unknown> = {}): Promise<T> => {
    if (!await current()) throw Object.assign(new Error('authentication_required'), { code: 'authentication_required' });
    let result: T;
    try {
      const payload = op.startsWith('apps.agent.') || op.startsWith('apps.assistant.') ? { ...args, expectedAccountId: accountId } : args;
      result = await options.client.extension<T>(op, payload, { expectedAccountId: accountId });
    }
    catch (cause) { if (cause && typeof cause === 'object' && 'code' in cause && ['authentication_required', 'device_revoked', 'device_not_found'].includes(String(cause.code))) lock(); throw cause; }
    if (!await current()) throw Object.assign(new Error('authentication_required'), { code: 'authentication_required' });
    return result;
  };

  const root = el('section', 'sw-assistant'); root.setAttribute('aria-label', 'Помощник');
  const header = el('header', 'sw-assistant-heading'), title = el('div');
  title.append(el('h1', '', 'Помощник'), el('p', 'sw-muted', 'Ваши задачи, проекты и устройства'));
  const headActions = el('div', 'sw-row');
  const newChat = button('Новый разговор', 'plus', 'sw-button-quiet sw-assistant-new', () => { void reset(); });
  newChat.setAttribute('aria-label', 'Новый разговор');
  newChat.title = 'Новый разговор';
  headActions.append(newChat, button('Создать приложение', 'app', '', () => { void options.createApp(); })); header.append(title, headActions);
  const layout = el('div', 'sw-assistant-layout'), history = el('aside', 'sw-assistant-history'); history.setAttribute('aria-label', 'История задач');
  const historyHeading = el('div', 'sw-assistant-history-heading'); historyHeading.append(el('h2', '', 'Недавние'), iconButton('Обновить историю', 'refresh', () => { void loadHistory(); }));
  const taskList = el('div', 'sw-assistant-task-list'), historyStatus = el('p', 'sw-muted'); historyStatus.setAttribute('role', 'status');
  const more = button('Ещё', 'down', 'sw-button-quiet', () => { void loadHistory(true); }); more.hidden = true;
  history.append(historyHeading, taskList, historyStatus, more);
  const pane = el('div', 'sw-assistant-pane'), notice = el('div', 'sw-assistant-notice'); notice.setAttribute('role', 'status');
  const target = el('div', 'sw-assistant-target'), hosts = el('select', 'sw-input'); hosts.setAttribute('aria-label', 'Устройство помощника');
  const folder = el('details', 'sw-assistant-folder'), cwd = textInput(draft.cwd, 'Полный путь на устройстве', 2000);
  const summary = el('summary'); summary.append(icon('folder'), el('span', '', 'Папка проекта'));
  folder.append(summary, labeledField('Папка', cwd, 'Пусто — отдельная рабочая папка.'));
  target.append(icon('laptop'), hosts, folder);
  const thread = el('section', 'sw-assistant-thread'); thread.setAttribute('aria-label', 'Разговор');
  const status = el('p', 'sw-assistant-status'); status.setAttribute('role', 'status');
  const resultArea = el('div', 'sw-assistant-result'), events = el('ol', 'sw-assistant-events'); events.setAttribute('aria-label', 'Последние действия');
  const stop = button('Остановить', 'close', 'sw-button-quiet', () => { void cancel(); }); stop.hidden = true;
  const form = el('form', 'sw-assistant-composer'), prompt = el('textarea', 'sw-input');
  prompt.rows = 3; prompt.maxLength = 16_000; prompt.value = draft.text; prompt.placeholder = 'Что сделать в вашем проекте?'; prompt.setAttribute('aria-label', 'Задача помощнику');
  const controls = el('div', 'sw-assistant-composer-controls'), saved = el('span', 'sw-muted'), send = button('Отправить', 'arrow', 'sw-button-primary'); send.type = 'submit';
  controls.append(saved, send); form.append(prompt, controls);
  const error = el('p', 'sw-error'); error.setAttribute('role', 'alert');
  const privilege = el('small', 'sw-muted sw-assistant-privilege', 'Работает с правами вашего пользователя на выбранном устройстве.');
  pane.append(notice, target, thread, status, events, resultArea, stop, error, form, privilege); layout.append(history, pane); root.append(header, layout); host.replaceChildren(root);
  const narrowLayout = matchMedia('(max-width: 600px)');
  function positionThread(): void { pane.insertBefore(thread, narrowLayout.matches && pane.dataset.mode === 'welcome' ? privilege : status); }
  narrowLayout.addEventListener('change', positionThread, { signal: abort.signal });

  function updateControls(): void {
    draft = drafts.read();
    const bound = draft.pending?.payload ?? selected;
    if (bound) {
      const key = keyOf(bound);
      if (!Array.from(hosts.options).some(option => option.value === key)) {
        const missing = el('option', '', 'Выбранное устройство недоступно'); missing.value = key; hosts.append(missing);
      }
      hosts.value = key;
      if (draft.pending) cwd.value = draft.pending.payload.cwd;
    }
    const waiting = Boolean(draft.pending), busy = Boolean(selected && (!activeJob || !finished(activeJob.status) || !continuable));
    hosts.disabled = sending || waiting || Boolean(selected) || !devices.length; cwd.disabled = sending || waiting || Boolean(selected); newChat.disabled = sending || waiting;
    send.textContent = ''; send.append(icon(waiting ? 'refresh' : 'arrow'), el('span', '', waiting ? 'Проверить отправку' : 'Отправить'));
    send.disabled = sending || (!waiting && (busy || !modelReady || !devices.some(device => keyOf(device) === hosts.value) || !prompt.value.trim()));
    saved.textContent = drafts.hasUnsavedChanges() ? 'Черновик только в этом окне' : waiting ? 'Подтверждение не получено' : prompt.value ? 'Черновик сохранён' : matchMedia('(pointer: coarse)').matches ? '' : 'Enter — отправить · Shift + Enter — строка';
    saved.dataset.hint = String(!drafts.hasUnsavedChanges() && !waiting && !prompt.value);
    if (drafts.hasUnsavedChanges()) saved.setAttribute('role', 'status'); else saved.removeAttribute('role');
    if (waiting) status.textContent = 'Отправка сохранена. Проверка безопасно повторит тот же запрос.';
    stop.hidden = !activeJob || finished(activeJob.status);
    stop.disabled = Boolean(activeJob?.cancelRequested) || sending;
    prompt.readOnly = waiting;
    thread.querySelectorAll<HTMLButtonElement>('.sw-assistant-starters button').forEach(control => { control.disabled = waiting || sending; });
    const suggestions = thread.querySelector<HTMLElement>('.sw-assistant-starters');
    if (suggestions) suggestions.hidden = Boolean(prompt.value.trim()) || waiting || sending;
  }
  function saveDraft(): void {
    draft = drafts.saveDraft({ text: prompt.value, cwd: cwd.value, deviceKey: hosts.value }); updateControls();
  }
  prompt.addEventListener('input', saveDraft); cwd.addEventListener('input', saveDraft);
  hosts.addEventListener('change', () => {
    if (drafts.read().deviceKey !== hosts.value && cwd.value) { cwd.value = ''; error.textContent = 'Выбрано другое устройство. Укажите папку проекта на нём.'; }
    saveDraft();
  });
  prompt.addEventListener('keydown', event => {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing && !matchMedia('(pointer: coarse)').matches) {
      event.preventDefault(); if (!send.disabled) form.requestSubmit();
    }
  });
  form.addEventListener('submit', event => { event.preventDefault(); void submit(); });

  function welcome(): void {
    pane.dataset.mode = 'welcome';
    positionThread();
    thread.replaceChildren(); resultArea.replaceChildren(); events.replaceChildren(); status.textContent = '';
    const intro = el('div', 'sw-assistant-welcome'); intro.append(icon('sparkle'), el('h2', '', 'От идеи — к работающему проекту'), el('p', 'sw-muted', 'Выберите устройство и поставьте задачу. Ход работы и результат останутся здесь.'));
    const starters = el('div', 'sw-assistant-starters');
    for (const [label, value, symbol] of [['Разобраться в проекте', 'Изучи проект и кратко объясни, как он устроен. Пока ничего не меняй.', 'folder'], ['Найти ошибку', 'Помоги найти и исправить ошибку в проекте: ', 'search'], ['Улучшить интерфейс', 'Изучи интерфейс проекта и предложи три конкретных улучшения удобства. Пока ничего не меняй.', 'app']]) {
      starters.append(button(label!, symbol, 'sw-button-quiet', () => { prompt.value = value!; saveDraft(); prompt.focus(); }));
    }
    intro.append(starters); thread.append(intro);
  }
  async function reset(): Promise<void> {
    if (!await current() || sending || drafts.read().pending) return;
    try { draft = drafts.select({ conversationId: crypto.randomUUID(), cwd: cwd.value, deviceKey: hosts.value }); }
    catch (cause) { error.textContent = friendly(cause); return; }
    generation++; if (timer) clearTimeout(timer); selected = null; activeJob = null;
    prompt.value = draft.text;
    error.textContent = ''; welcome(); updateControls(); renderHistory(); prompt.focus();
  }
  function renderHistory(): void {
    const focused = document.activeElement instanceof HTMLElement ? document.activeElement.dataset.assistantTask : undefined;
    taskList.replaceChildren();
    for (const savedDraft of drafts.listDrafts()) {
      if (savedDraft.conversationId === draft.conversationId) continue;
      const item = button(`Черновик: ${savedDraft.text.slice(0, 80)}`, 'note', 'sw-assistant-task', () => {
        if (sending || drafts.read().pending) { error.textContent = friendly(new Error('assistant_pending_unconfirmed')); return; }
        try {
          draft = drafts.restore(savedDraft.conversationId); prompt.value = draft.text; cwd.value = draft.cwd;
          hosts.value = draft.deviceKey;
          selected = null; activeJob = null; generation++; if (timer) clearTimeout(timer);
          welcome(); updateControls(); void refresh();
          if (draft.lastJob && !draft.pending) follow(draft.lastJob);
        } catch (cause) { error.textContent = friendly(cause); }
      });
      taskList.append(item);
    }
    for (const task of tasks.values()) {
      const item = el('button', 'sw-assistant-task'); item.type = 'button'; item.dataset.assistantTask = task.id;
      item.setAttribute('aria-pressed', String(selected?.jobId === task.id));
      const copy = el('span'); copy.append(el('strong', '', task.text || 'Задача'), el('small', 'sw-muted', `${task.appCreation ? 'Приложение' : 'Разговор'} · ${new Date(task.createdAt).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })}`));
      item.append(icon(task.appCreation ? 'app' : 'chat'), copy);
      item.addEventListener('click', () => {
        if (drafts.read().pending || sending) { error.textContent = friendly(new Error('assistant_pending_unconfirmed')); return; }
        const target = { hostDeviceId: task.hostDeviceId, connectorId: task.connectorId, jobId: task.id };
        if (task.appCreation) { void options.resumeApp(target); return; }
        const conversationId = task.threadId.startsWith('assistant_') ? task.threadId.slice(10) : crypto.randomUUID();
        try {
          draft = drafts.select({ conversationId, lastJob: target, cwd: task.cwd, deviceKey: keyOf(task) });
          prompt.value = draft.text; cwd.value = draft.cwd; hosts.value = draft.deviceKey;
          if (draft.pending) { selected = null; activeJob = null; generation++; if (timer) clearTimeout(timer); welcome(); updateControls(); }
          else follow(target);
        } catch (cause) { error.textContent = friendly(cause); }
      });
      taskList.append(item);
    }
    history.dataset.empty = String(taskList.childElementCount === 0);
    if (historyState === 'ready') historyStatus.textContent = taskList.childElementCount ? '' : 'Здесь появятся ваши задачи.';
    if (focused) Array.from(taskList.querySelectorAll<HTMLButtonElement>('button')).find(node => node.dataset.assistantTask === focused)?.focus();
  }
  async function loadHistory(append = false): Promise<void> {
    const revision = ++refreshGeneration; historyState = 'loading'; history.dataset.state = historyState; more.disabled = true; historyStatus.textContent = 'Загружаем…';
    try {
      const page = await request<{ jobs: Task[]; nextCursor: string | null }>('apps.assistant.history', { limit: 20, ...(append && cursor ? { cursor } : {}) });
      if (disposed || revision !== refreshGeneration) return;
      if (!append) tasks.clear(); for (const task of page.jobs) tasks.set(task.id, task);
      cursor = page.nextCursor; more.hidden = !cursor; historyState = 'ready'; history.dataset.state = historyState; renderHistory();
    } catch (cause) { if (!disposed && revision === refreshGeneration) { historyState = 'error'; history.dataset.state = historyState; historyStatus.textContent = friendly(cause); } }
    finally { if (!disposed && revision === refreshGeneration) more.disabled = false; }
  }
  async function refresh(): Promise<void> {
    const revision = ++deviceRefreshGeneration;
    await Promise.allSettled([loadHistory(), (async () => {
      try {
        const result = await request<{ devices: Host[] }>('apps.devices');
        const response = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.any([abort.signal, AbortSignal.timeout(8000)]) });
        if (!response.ok) throw new Error('unavailable');
        const capabilities = await response.json() as { agentConfigured: boolean };
        if (!await current() || revision !== deviceRefreshGeneration) return;
        devices = result.devices; modelReady = capabilities.agentConfigured === true;
        const savedState = drafts.read();
        const storedKey = hosts.value || savedState.deviceKey;
        const value = savedState.pending ? keyOf(savedState.pending.payload) : selected ? keyOf(selected) : resolveDeviceKey(storedKey, devices);
        if (!savedState.pending && !selected && value !== storedKey) draft = drafts.saveDraft({ deviceKey: value });
        hosts.replaceChildren();
        if (!devices.length) { const option = el('option', '', 'Компьютер не подключён'); option.value = ''; hosts.append(option); }
        for (const device of devices) { const option = el('option', '', `${device.name}${device.online ? '' : ' · не в сети'}`); option.value = keyOf(device); hosts.append(option); }
        if (devices.some(device => keyOf(device) === value)) hosts.value = value;
        else if (value) { const missing = el('option', '', 'Выбранное устройство недоступно'); missing.value = value; hosts.append(missing); hosts.value = value; }
        notice.replaceChildren();
        if (!devices.length) notice.append(icon('laptop'), el('span', '', 'Подключите компьютер для работы с проектами.'), button('Подключить', 'plus', '', () => { void Promise.resolve(options.connectDevice()).then(() => refresh()); }));
        else if (!modelReady) notice.append(icon('sparkle'), el('span', '', 'Подключение к модели пока недоступно.'), button('Проверить', 'refresh', 'sw-button-quiet', () => { void refresh(); }));
        notice.hidden = Boolean(devices.length && modelReady); updateControls();
      } catch { if (!disposed && revision === deviceRefreshGeneration) { notice.hidden = false; notice.replaceChildren(el('span', '', 'Нет подтверждения связи. Черновик остаётся на устройстве.'), button('Повторить', 'refresh', '', () => { void refresh(); })); modelReady = false; updateControls(); } }
    })()]);
  }
  async function submit(): Promise<void> {
    if (sending || !await current()) return;
    sending = true; error.textContent = ''; updateControls();
    const execute = async () => {
      const savedState = drafts.read(), device = devices.find(value => keyOf(value) === hosts.value);
      if (!savedState.pending && (!device || !modelReady || !prompt.value.trim() || (selected && (!activeJob || !finished(activeJob.status) || !continuable)))) return;
      const pending = savedState.pending ?? drafts.prepare({ expectedAccountId: accountId, hostDeviceId: device!.hostDeviceId, connectorId: device!.connectorId,
        conversationId: savedState.conversationId, text: prompt.value, cwd: cwd.value, ...(savedState.previousJobId ? { previousJobId: savedState.previousJobId } : {}) });
      const result = await request<ReadResult | { admission: { status: string; requestId: string; reason: string } }>('apps.assistant.send', { ...pending.payload, requestId: pending.requestId });
      if ('admission' in result) {
        if (result.admission.status === 'rejected' && result.admission.requestId === pending.requestId) drafts.forgetRejected(pending.requestId);
        if (result.admission.reason === 'app_model_unavailable') { modelReady = false; void refresh(); }
        throw Object.assign(new Error(result.admission.reason), { code: result.admission.reason });
      }
      const target = { hostDeviceId: pending.payload.hostDeviceId, connectorId: pending.payload.connectorId, jobId: result.job.id };
      drafts.acknowledge(pending.requestId, target); draft = drafts.read(); prompt.value = draft.text; follow(target); void loadHistory();
    };
    try { if (navigator.locks) await navigator.locks.request(drafts.key, execute); else await execute(); }
    catch (cause) {
      if (!disposed) {
        error.textContent = drafts.read().pending ? `${friendly(cause)} Проверка сохранённой отправки использует тот же запрос.` : friendly(cause);
        // A changed readiness/predecessor is not proof that an earlier request
        // was never accepted. Only the durable request lookup resolves it.
      }
    } finally { sending = false; if (!disposed) updateControls(); }
  }
  async function cancel(): Promise<void> {
    if (!selected || stop.disabled) return;
    const expected = selected.jobId; stop.disabled = true;
    try { const result = await request<ReadResult>('apps.agent.cancel', { ...selected }); if (selected?.jobId !== expected) return; activeJob = result.job; status.textContent = jobLabel(result.job); updateControls(); }
    catch (cause) { if (!disposed && selected?.jobId === expected) { error.textContent = friendly(cause); stop.disabled = false; } }
  }
  function follow(target: AssistantTarget): void {
    pane.dataset.mode = 'conversation';
    positionThread();
    selected = target; activeJob = null; continuable = false; const version = ++generation; if (timer) clearTimeout(timer);
    thread.replaceChildren(); events.replaceChildren(); resultArea.replaceChildren(); error.textContent = ''; status.textContent = 'Открываем задачу…';
    let after = 0; updateControls(); renderHistory();
    const poll = async () => {
      try {
        const result = await request<ReadResult>('apps.agent.read', { ...target, after });
        if (disposed || version !== generation) return;
        activeJob = result.job; status.textContent = jobLabel(result.job);
        if (result.task && !thread.childElementCount) {
          const question = el('article', 'sw-assistant-question'); question.append(el('small', 'sw-muted', 'Вы'), el('p', '', result.task.text)); thread.append(question);
        }
        for (const event of result.events ?? []) {
          if (event.seq <= after || !event.text) continue;
          events.append(el('li', '', event.text.slice(0, 360))); while (events.children.length > 6) events.firstElementChild?.remove();
        }
        after = result.cursor ?? after; updateControls();
        if (result.done) {
          continuable = result.task?.canContinue === true;
          const answer = el('article', 'sw-assistant-answer'); answer.append(el('small', 'sw-muted', 'Помощник'), el('pre', '', result.job.result?.text || 'Исполнитель не вернул текстовый ответ.'));
          if (result.job.result?.textTruncated) answer.append(el('small', 'sw-muted', 'Показан фрагмент ответа.'), button('Скачать полный ответ', 'download', 'sw-button-quiet', () => { void download(target); }));
          resultArea.replaceChildren(answer);
          if (!drafts.read().pending) drafts.select({ conversationId: result.job.threadId.startsWith('assistant_') ? result.job.threadId.slice(10) : draft.conversationId,
            previousJobId: result.task?.canContinue ? result.job.id : '', lastJob: target, cwd: result.task?.cwd ?? cwd.value, deviceKey: keyOf(target) });
          updateControls();
          if (!continuable) status.textContent += ' · для следующей задачи начните новый разговор';
          void loadHistory(); return;
        }
        timer = setTimeout(() => { void poll(); }, result.more ? 50 : document.hidden ? 8000 : 2000);
      } catch (cause) {
        if (disposed || version !== generation) return;
        status.textContent = friendly(cause);
        const code = cause && typeof cause === 'object' && 'code' in cause ? String(cause.code) : '';
        if (['apps_device_not_owned', 'job_not_found', 'job_access_denied'].includes(code)) {
          thread.replaceChildren(); resultArea.replaceChildren(); events.replaceChildren(); tasks.delete(target.jobId);
          // Keep this conversation and its unsent draft attached to their
          // original target. Only an explicit New Conversation can retarget.
          activeJob = null; continuable = false;
          error.textContent = 'Разговор больше недоступен. Черновик сохранён; для другой задачи начните новый разговор.';
          renderHistory(); updateControls(); void refresh(); return;
        }
        timer = setTimeout(() => { void poll(); }, document.hidden ? 12_000 : 5000);
      }
    };
    void poll();
  }
  async function download(target: AssistantTarget): Promise<void> {
    try {
      const parts: string[] = []; let offset: number | null = 0, size = 0;
      while (offset !== null && await current()) {
        const page: { text: string; nextOffset: number | null } = await request('apps.agent.result', { ...target, offset });
        size += page.text.length; if (size > 4_000_000 || (page.nextOffset !== null && page.nextOffset <= offset)) throw new Error('invalid_result_page');
        parts.push(page.text); offset = page.nextOffset;
      }
      if (!await current()) return;
      const url = URL.createObjectURL(new Blob(parts, { type: 'text/plain;charset=utf-8' }));
      const link = el('a'); link.href = url; link.download = 'soty-assistant.txt'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (cause) { if (!disposed) error.textContent = friendly(cause); }
  }
  window.addEventListener('storage', event => {
    if (event.key !== drafts.key || disposed) return;
    const wasPending = draft.pending?.requestId; draft = drafts.read();
    if (document.activeElement !== prompt) prompt.value = draft.text;
    updateControls();
    if (wasPending && !draft.pending && draft.lastJob && draft.lastJob.jobId !== selected?.jobId) follow(draft.lastJob);
  }, { signal: abort.signal });
  welcome(); updateControls(); void refresh();
  if (draft.lastJob && !draft.pending) follow(draft.lastJob);
  return { dispose() { disposed = true; generation++; abort.abort(); if (timer) clearTimeout(timer); host.replaceChildren(); }, refresh,
    async flush() { drafts.flush(); }, hasUnsavedChanges: () => sending || drafts.hasUnsavedChanges() };
}
