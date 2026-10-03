import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import { button, el, emptyState, labeledField, textInput } from '../world/dom';
import { icon } from '../world/icons';
import { createDialog, errorText, type WorldDialog } from '../world/dialogs';
import { createAppCreateState, type AppCreateDraft, type AppCreateReceipt } from './app-create-state.mjs';
import { registerUpdateGuard } from './pwa';
import './local-apps.css';

interface HostDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean }
interface Proposal { schema: 'soty.local-app.v1'; name: string; port: number; entryPath: string; sourceJobId: string }
interface Pending { hostDeviceId: string; connectorId: string; jobId: string }
interface JobResult { job: { id: string; deviceId: string; connectorId: string; status: string; cancelRequested?: boolean; executionUncertain?: boolean; result: { text: string; textTruncated?: boolean; appProposal?: Proposal } | null }; done?: boolean; cursor?: number; events?: { seq: number; type: string; text: string }[] }

function friendly(error: unknown): string {
  const code = typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : error instanceof Error ? error.message : '';
  const messages: Record<string, string> = {
    apps_device_already_owned: 'Этот компьютер уже связан с другим аккаунтом.',
    apps_connector_offline: 'Коннектор пока не подключён к Сотам.',
    apps_device_not_owned: 'Сначала подключите выбранный компьютер к своему аккаунту.',
    apps_origin_not_configured: 'Адрес приложений ещё не настроен на сервере.',
    app_job_failed: 'Не удалось запустить задачу. Проверьте подключение компьютера.',
    app_model_unavailable: 'Подключение к ИИ ещё не настроено. Попробуйте позже.',
    invalid_workspace: 'Укажите папку проекта на выбранном компьютере.',
    job_request_conflict: 'Параметры задачи изменились. Откройте новую задачу.',
    authentication_required: 'Аккаунт изменился. Откройте задачу заново.',
    app_create_storage_unavailable: 'Браузер не смог сохранить отправку. Освободите место и повторите. Текст остаётся в этом окне.',
    app_create_lock_unavailable: 'Этот браузер не поддерживает безопасную отправку между вкладками. Откройте Соты в обновлённом браузере.',
    app_create_pending_unconfirmed: 'Сначала проверьте предыдущую отправку. Её устройство и текст сохранены.',
    app_create_result_pending: 'Предыдущая задача уже принята. Откройте её результат.',
    invalid_app_create_receipt: 'Ответ не подтверждает эту отправку. Повторная проверка использует тот же запрос.',
    invalid_app_create_target: 'Выберите доступное устройство.',
    invalid_app_create_prompt: 'Проверьте текст задачи и полный путь к папке.',
  };
  return messages[code] ?? errorText(error);
}
function remember(key: string, value: Pending | null): void {
  try { if (value) localStorage.setItem(key, JSON.stringify(value)); else localStorage.removeItem(key); } catch { /* The running job still lives on the server. */ }
}
function recalled(key: string): Pending | null {
  try {
    const value = JSON.parse(localStorage.getItem(key) || 'null') as Pending | null;
    return value && [value.hostDeviceId, value.connectorId, value.jobId].every(item => typeof item === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(item)) ? value : null;
  } catch { return null; }
}
function forgetPending(key: string, expected: Pending): void {
  // A late registration ACK must not erase a newer task started in the meantime.
  if (recalled(key)?.jobId === expected.jobId) remember(key, null);
}

export function createAppActions(client: ConnectClient, refresh: () => Promise<void>, openAccount?: () => Promise<void>) {
  const dialogs = new Set<WorldDialog>();
  const createStates = new Map<string, ReturnType<typeof createAppCreateState>>();
  const beforeUnload = (event: BeforeUnloadEvent) => {
    if ([...createStates.values()].some(state => state.hasVolatileDraft())) { event.preventDefault(); event.returnValue = ''; }
  };
  window.addEventListener('beforeunload', beforeUnload);
  const unregisterUpdateGuard = registerUpdateGuard(async () => {
    try { await Promise.all([...createStates.values()].map(state => state.flush())); return [...createStates.values()].every(state => !state.hasUnsavedChanges()); }
    catch { return false; }
  });
  const dialog = (title: string, close?: () => void) => {
    const value = createDialog(title, () => { dialogs.delete(value); close?.(); }); dialogs.add(value); return value;
  };
  async function connectDevice(): Promise<void> {
    let closed = false;
    const panel = dialog('Подключить устройство', () => { closed = true; });
    const status = el('p', 'sw-muted', 'Соединяемся с вашим коннектором…'); status.setAttribute('role', 'status');
    const body = el('div', 'sw-stack'); body.append(status); panel.body.append(body);
    let busy = false, forwardedPort: number | null = null;
    const connect = async () => {
      if (busy || closed) return; busy = true;
      let stage: 'service' | 'connector' | 'claim' = 'service';
      body.replaceChildren(status); status.textContent = 'Соединяемся с вашим коннектором…';
      try {
        const response = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new TypeError('Network unavailable');
        const capabilities = await response.json() as { localConnectorOrigin: string };
        const local = new URL(forwardedPort ? `http://127.0.0.1:${forwardedPort}` : capabilities.localConnectorOrigin);
        if (local.protocol !== 'http:' || local.hostname !== '127.0.0.1' || local.pathname !== '/' || local.search || local.hash || local.username || local.password) throw new Error('invalid_local_endpoint');
        stage = 'connector';
        const claimed = await fetch(new URL('/apps/claim', local), {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}', signal: AbortSignal.timeout(8000),
          targetAddressSpace: 'loopback',
        } as RequestInit & { targetAddressSpace: 'loopback' });
        const claim = await claimed.json() as { ok: boolean; hostDeviceId: string; connectorId: string; claimCode: string };
        if (!claimed.ok || !claim.ok) throw Object.assign(new Error('Connector unavailable'), { code: 'apps_connector_offline' });
        if (closed) return;
        stage = 'claim';
        const result = await client.extension<{ device: HostDevice }>('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
        claim.claimCode = '';
        if (closed) return;
        status.textContent = 'Теперь его приложения могут появляться в ваших сотах.';
        body.replaceChildren(el('h3', '', result.device.name), status, button('Готово', 'check', 'sw-button-primary', () => panel.close()));
        await refresh();
      } catch (error) {
        if (closed) return;
        status.textContent = stage === 'connector' ? 'Не найдено подключение к этому компьютеру.' : friendly(error);
        const windows = /Windows/i.test(navigator.userAgent);
        const mobile = /Android|iPhone|iPad/i.test(navigator.userAgent);
        const installer = (forWindows: boolean, primary = false): HTMLAnchorElement => {
          const link = el('a', `sw-button ${primary ? 'sw-button-primary' : 'sw-button-quiet'}`, forWindows ? 'Для Windows' : 'Для macOS / Linux');
          link.href = forWindows ? '/agent/install-windows-machine.cmd' : '/agent/install-macos-linux.sh';
          link.download = forWindows ? 'soty-connector.cmd' : 'soty-connector.sh'; return link;
        };
        const downloads = el('div', 'sw-row');
        downloads.append(installer(windows, !mobile), installer(!windows));
        body.replaceChildren(status, el('p', 'sw-muted', 'Установите коннектор на этом компьютере, откройте его и нажмите «Повторить».'),
          button('Повторить', 'refresh', '', () => { void connect(); }),
          button('Другое подключение', 'settings', 'sw-button-quiet', forwardedConnection), downloads);
        if (openAccount) body.append(button('Открыть мой профиль на другом устройстве', 'phone', 'sw-button-quiet', () => { panel.close(); void openAccount(); }));
      } finally { busy = false; }
    };
    const choice = (title: string, detail: string, symbol: string, action: () => void) => {
      const target = el('button', 'sw-connect-choice'); target.type = 'button'; const copy = el('span'); copy.append(el('strong', '', title), el('small', '', detail)); target.append(icon(symbol), copy, icon('next')); target.addEventListener('click', action); return target;
    };
    function forwardedConnection(): void {
      const form = el('form', 'sw-stack'), port = textInput('', 'Например, 49427', 5);
      port.type = 'number'; port.min = '1024'; port.max = '65535'; port.required = true;
      const submit = button('Подключить сервер', 'laptop', 'sw-button-primary'); submit.type = 'submit';
      form.append(el('p', 'sw-muted', 'Для сервера с SSH подключите его коннектор через локальный туннель. Соты обращаются только к вашему компьютеру, а туннель передаёт запрос серверу.'),
        labeledField('Локальный порт туннеля', port, 'Сам коннектор на сервере должен быть подключён к этому адресу Сот.'), submit);
      form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity()) return; const value = Number(port.value);
        if (!Number.isSafeInteger(value) || value < 1024 || value > 65535) return; forwardedPort = value; void connect(); });
      body.replaceChildren(form); port.focus();
    }
    body.replaceChildren(choice('Подключить этот компьютер', 'Открывать его приложения и управлять им из Сот', 'laptop', () => { void connect(); }));
    body.append(choice('Подключить сервер по SSH', 'Для проекта, который уже работает на сервере', 'server', forwardedConnection));
    if (openAccount) body.append(choice('Мой профиль на другом устройстве', 'Те же записки, контакты и сообщества — по QR', 'phone', () => { panel.close(); void openAccount(); }));
    if (/Android|iPhone|iPad/i.test(navigator.userAgent)) body.append(el('p', 'sw-muted', 'Чтобы подключить компьютер, откройте Соты на нём. На телефоне можно открыть свой профиль по QR.'));
  }

  async function agentCreate(communityId?: string, restore?: Pending): Promise<void> {
    let closed = false, accountInvalid = false, timer: ReturnType<typeof setTimeout> | undefined, following = '';
    const eventsAbort = new AbortController();
    const panel = dialog('Создать с ИИ', () => { closed = true; eventsAbort.abort(); if (timer) clearTimeout(timer); });
    const identity = await client.getLocalState();
    if (!identity.accountId || closed) { if (!closed) panel.close(); return; }
    const accountId = identity.accountId, pendingKey = `soty.world.agent:${accountId}`;
    let storage: Pick<Storage, 'getItem' | 'setItem'>;
    try { storage = localStorage; } catch { storage = { getItem() { throw new Error('storage_unavailable'); }, setItem() { throw new Error('storage_unavailable'); } }; }
    let drafts = createStates.get(accountId);
    if (!drafts) { drafts = createAppCreateState({ accountId, storage, locks: navigator.locks }); createStates.set(accountId, drafts); }
    const creation = drafts;
    const note = el('p', 'sw-muted', 'Загружаем ваши устройства…'); note.setAttribute('role', 'status'); panel.body.append(note);
    async function sameAccount(): Promise<boolean> {
      if (accountInvalid) return false;
      const state = await client.getLocalState();
      if (state.accountId !== accountId) {
        accountInvalid = true; createStates.delete(accountId); panel.body.replaceChildren(); if (!closed) panel.close(); return false;
      }
      return !accountInvalid;
    }
    async function current(): Promise<boolean> { return !closed && await sameAccount() && !closed; }
    const request = async <T>(op: string, args: Record<string, unknown>): Promise<T> => {
      if (!await current()) throw Object.assign(new Error('authentication_required'), { code: 'authentication_required' });
      let result: T;
      try { result = await client.extension<T>(op, { ...args, expectedAccountId: accountId }); }
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
      void forget(pending).then(async () => { if (await current()) { panel.close(); void agentCreate(communityId); } })
        .catch(cause => { if (!closed) { note.textContent = friendly(cause); panel.body.append(note); } });
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
      const proposal = result.job.result?.appProposal;
      const success = result.job.status === 'succeeded' && !result.job.executionUncertain && proposal?.schema === 'soty.local-app.v1';
      const content = el('div', 'sw-stack');
      content.append(el('h3', '', success ? proposal.name : label(result)));
      if (success) {
        content.append(el('p', 'sw-muted', 'Добавьте приложение, чтобы открыть его в Сотах. Доступ останется личным.'));
        const add = button('Добавить в мои соты', 'plus', 'sw-button-primary', () => {
          add.disabled = true;
          void (async () => {
            try {
              await request('apps.register', { hostDeviceId: pending.hostDeviceId, connectorId: pending.connectorId,
                name: proposal.name, port: proposal.port, entryPath: proposal.entryPath, grants: { accountIds: [], communityIds: [] } });
              await forget(pending);
              if (!await current()) return;
              note.textContent = 'Приложение добавлено'; content.replaceChildren(el('h3', '', proposal.name), note, button('Готово', 'check', 'sw-button-primary', () => panel.close()));
              await refresh();
            } catch (error) { if (await current()) { note.textContent = friendly(error); content.append(note); add.disabled = false; } }
          })();
        });
        content.append(add);
        if (communityId) content.append(el('small', 'sw-muted', 'Доступ сообществу можно открыть в настройках приложения.'));
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
      content.append(detail, button('Новая задача', 'plus', 'sw-button-quiet', () => newTask(pending))); panel.body.replaceChildren(content);
    }

    function follow(pending: Pending): void {
      if (closed || following === pending.jobId) return;
      following = pending.jobId; if (timer) clearTimeout(timer);
      const activity = el('div', 'sw-stack');
      const progress = el('p', 'sw-agent-progress', 'Задача принята'); progress.setAttribute('role', 'status');
      const events = el('ol', 'sw-agent-events'); events.setAttribute('aria-label', 'Ход создания');
      const cancel = button('Остановить', 'close', 'sw-button-quiet', () => {
        cancel.disabled = true;
        void request<JobResult>('apps.agent.cancel', { ...pending }).then(result => { if (!closed) progress.textContent = label(result); })
          .catch(async error => { if (await current()) { progress.textContent = friendly(error); cancel.disabled = false; } });
      });
      activity.append(progress, events, el('small', 'sw-muted', 'Можно закрыть окно — задача продолжится.'), cancel); panel.body.replaceChildren(activity);
      let cursor = 0;
      const poll = async () => {
        if (!await current()) return;
        try {
          const result = await request<JobResult>('apps.agent.read', { ...pending, after: cursor });
          if (!await current()) return;
          for (const event of result.events || []) {
            if (!event.text || event.seq <= cursor) continue;
            const text = event.text.length > 280 ? event.text.slice(0, 280) + '…' : event.text;
            events.append(el('li', '', text)); while (events.children.length > 5) events.firstElementChild?.remove();
          }
          cursor = result.cursor ?? cursor;
          progress.textContent = label(result);
          cancel.disabled = Boolean(result.job.cancelRequested);
          if (result.done) { await showResult(result, pending); return; }
        } catch (error) {
          if (!await current()) return;
          progress.textContent = friendly(error);
          const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
          if (code === 'job_not_found') {
            panel.body.replaceChildren(emptyState('Эта задача больше недоступна', 'Уже добавленные приложения остаются в ваших сотах.', button('Новая задача', 'plus', 'sw-button-primary', () => newTask(pending)), 'sparkle'));
            return;
          }
        }
        if (!closed) timer = setTimeout(() => { void poll(); }, document.hidden ? 8000 : 2000);
      };
      void poll();
    }

    const saved = creation.read();
    const known = restore ?? saved.accepted ?? recalled(pendingKey);
    if (!saved.pending && known) {
      try { await creation.adoptAccepted(known); } catch { /* A known job remains readable even when browser storage is unavailable. */ }
      if (await current()) follow(known);
      return;
    }
    const form = el('form', 'sw-stack'), hosts = el('select', 'sw-input');
    const prompt = el('textarea', 'sw-input'); prompt.rows = 4; prompt.maxLength = 16_000; prompt.required = true; prompt.placeholder = 'Например, общий список покупок';
    const cwd = textInput('', 'Выбрать существующую папку', 2000);
    const advanced = el('details', 'sw-agent-details');
    advanced.append(el('summary', '', 'Папка проекта'), labeledField('Полный путь', cwd, 'Оставьте пустым — создадим отдельную папку.'));
    const privilege = el('details', 'sw-agent-details'); privilege.append(el('summary', '', 'Работает с правами вашего пользователя'),
      el('p', 'sw-muted', 'На выбранном устройстве агент может читать файлы, изменять проект и запускать команды. Это ваш доверенный исполнитель; отдельная изоляция внешних агентов пока не включена.'));
    const error = el('p', 'sw-error'); error.setAttribute('role', 'alert');
    const status = el('p', 'sw-muted'); status.setAttribute('role', 'status');
    const create = button('Создать', 'sparkle', 'sw-button-primary'); create.type = 'submit';
    const availability = el('div', 'sw-stack'), recovery = el('div', 'sw-stack'); recovery.hidden = true;
    form.append(labeledField('Что создаём?', prompt), labeledField('На устройстве', hosts), advanced, availability, status, error, recovery, create, privilege);
    panel.body.replaceChildren(form);
    let devices: HostDevice[] = [], modelReady = false, busy = false, loading = true, editGeneration = 0, loadGeneration = 0, closeGeneration = 0;
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
    panel.element.addEventListener('cancel', event => { requestClose(panel.close, event); }, { capture: true, signal: eventsAbort.signal });
    panel.element.querySelector('.sw-dialog-header button')?.addEventListener('click', event => { requestClose(panel.close, event); }, { capture: true, signal: eventsAbort.signal });
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
    prompt.addEventListener('input', () => { void saveDraft(); }); cwd.addEventListener('input', () => { void saveDraft(); });
    hosts.addEventListener('change', () => { if (cwd.value) { cwd.value = ''; error.textContent = 'Выбрано другое устройство. Проверьте папку проекта.'; } void saveDraft(); });
    async function loadDevices(): Promise<void> {
      const version = ++loadGeneration; loading = true; updateControls();
      const values = await Promise.allSettled([request<{ devices: HostDevice[] }>('apps.devices', {}),
        fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) }).then(async response => {
          if (!response.ok) throw new TypeError('Network unavailable'); return await response.json() as { agentConfigured: boolean };
        })]);
      if (!await current() || version !== loadGeneration) return;
      loading = false;
      devices = values[0].status === 'fulfilled' ? values[0].value.devices : [];
      modelReady = values[1].status === 'fulfilled' && values[1].value.agentConfigured === true;
      availability.replaceChildren();
      if (!devices.length) availability.append(el('p', 'sw-muted', 'Нет доступного компьютера для новой задачи.'),
        button('Подключить компьютер', 'laptop', 'sw-button-quiet', () => { requestClose(() => { panel.close(); void connectDevice(); }); }));
      if (!modelReady) availability.append(el('p', 'sw-muted', 'Подключение к ИИ пока недоступно. Сохранённую отправку можно проверить.'));
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
          const result = await client.extension<JobResult | { admission: AppCreateReceipt }>('apps.agent.create', { ...pending.payload });
          if (!await sameAccount()) return;
          if ('admission' in result) {
            await creation.reject(pending.payload, result.admission);
            if (!closed) { if (result.admission.reason === 'app_model_unavailable') modelReady = false; fill(); error.textContent = friendly({ code: result.admission.reason }); }
            return;
          }
          if (result.job.deviceId !== pending.payload.hostDeviceId || result.job.connectorId !== pending.payload.connectorId) throw new Error('invalid_app_create_receipt');
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
    });
    window.addEventListener('storage', event => {
      if (event.key !== creation.key) return;
      void current().then(valid => { if (!valid || busy || following) return; const value = creation.read(); if (value.accepted) follow(value.accepted); else fill(); });
    }, { signal: eventsAbort.signal });
    fill(); prompt.focus();
    try {
      await loadDevices();
    } catch (cause) { if (await current()) { error.textContent = friendly(cause); loading = false; updateControls(); } }
  }
  const resetAccount = () => { for (const panel of [...dialogs]) panel.close(); dialogs.clear(); createStates.clear(); };
  return { connectDevice, agentCreate, resetAccount, destroy: () => { resetAccount(); unregisterUpdateGuard(); window.removeEventListener('beforeunload', beforeUnload); } };
}
