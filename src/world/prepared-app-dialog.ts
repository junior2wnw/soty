import { button, el, labeledField } from './dom';
import { errorText, type WorldDialog } from './dialogs';
import { createPreparedAppState, type PreparedLocation, type PreparedPayload } from './prepared-app-state.mjs';
import type { WorldApi } from './types';

interface Device { hostDeviceId: string; connectorId: string; name: string; online: boolean; claimed: boolean }
interface Preview { name: string; digest: string; responding: boolean }
interface Roots { roots: { id: string; label: string }[] }
interface Listing { entries: string[]; next: string | null; project: Preview | null; projectError: 'unreadable' | null }
interface Result { state: 'not-found' | 'pending' | 'ready' | 'revoked'; app?: { id: string; name: string; state: string }; feedback?: string }
interface Options { dialog: WorldDialog; api: WorldApi; accountId: string; selectedDevice?: string; current(): boolean;
  accountCurrent(): boolean;
  identity(): Promise<{ accountId: string | null; deviceId?: string | null }>;
  open(app: { id: string; name: string }): void; background(): void; manual(device?: string): void; connectDevice(): void }

export async function mountPreparedAppDialog(options: Options): Promise<void> {
  const { dialog, api, accountId, current } = options;
  const content = el('div', 'sw-stack'), status = el('p', 'sw-muted'), errors = el('p', 'sw-error');
  status.setAttribute('role', 'status'); errors.setAttribute('role', 'alert');
  dialog.body.replaceChildren(content, status, errors);
  const fallback = (): void => content.replaceChildren(button('Указать адрес вручную', undefined, 'sw-button-quiet', () => options.manual(options.selectedDevice)));
  const local = await options.identity().catch(() => null);
  if (!current()) return;
  if (!local?.deviceId || local.accountId !== accountId) { errors.textContent = 'Не удалось подтвердить аккаунт на этом устройстве.'; fallback(); return; }
  const deviceId = local.deviceId;
  let state: ReturnType<typeof createPreparedAppState>;
  try { state = createPreparedAppState({ accountId, deviceId, storage: localStorage, locks: navigator.locks }); state.read(); }
  catch { errors.textContent = 'Не удалось сохранить состояние подключения на этом устройстве.'; fallback(); return; }
  const accountValid = async (): Promise<boolean> => {
    if (!options.accountCurrent()) return false;
    const identity = await options.identity().catch(() => null);
    return options.accountCurrent() && identity?.accountId === accountId && identity.deviceId === deviceId;
  };
  const valid = async (): Promise<boolean> => current() && await accountValid() && current();
  const request = async <T>(op: string, args: Record<string, unknown>, allowClosed = false): Promise<T> => {
    if (!await valid()) throw Object.assign(new Error('account_changed'), { code: 'authentication_required' });
    const result = await api.request<T>(op, args);
    if (!await accountValid() || !allowClosed && !current()) throw Object.assign(new Error('account_changed'), { code: 'authentication_required' });
    return result;
  };
  const message = (cause: unknown): string => {
    const code = typeof cause === 'object' && cause && 'code' in cause ? String(cause.code) : '';
    if (code === 'prepared_connector_unsupported') return 'Обновите подключение Сот на выбранном устройстве.';
    if (code === 'prepared_busy') return 'Устройство ещё проверяет другие папки. Подождите и повторите.';
    if (code === 'prepared_browse_failed') return 'Папка или приложение изменились. Проверьте, что проект запущен, и выберите его снова.';
    if (code === 'prepared_local_unavailable') return 'Не удалось сохранить состояние подключения; действие не начато.';
    if (code === 'prepared_local_capacity') return 'Сначала проверьте предыдущие подключения.';
    return errorText(cause);
  };
  let busy = false, browseGeneration = 0, selected: Device | null = null;
  const chooser = el('div', 'sw-stack'), pendingArea = el('div', 'sw-stack'), retryArea = el('div', 'sw-stack'), directory = el('div', 'sw-stack');
  const manual = button('Указать адрес вручную', undefined, 'sw-button-quiet', () => options.manual(selected?.hostDeviceId));
  content.append(el('p', 'sw-muted', 'Выберите папку уже работающего проекта на подключённом устройстве.'), pendingArea, retryArea, chooser, directory, manual);
  const showError = (cause: unknown): void => { if (current()) { status.textContent = ''; errors.textContent = message(cause); } };
  const finish = async (payload: PreparedPayload, result: Result, open: boolean): Promise<boolean> => {
    if (!await accountValid()) return false;
    if (result.state === 'revoked') {
      await state.acknowledge(payload); if (current()) { status.textContent = 'Это подключение удалено. Чтобы добавить его снова, выберите проект.'; refreshPending(); }
      return true;
    }
    if (result.state !== 'ready' || !result.app || !/^app-[a-f0-9]{32}$/u.test(result.app.id)) return false;
    await state.acknowledge(payload);
    if (!await accountValid()) return true;
    if (!current()) { options.background(); return true; }
    status.textContent = 'Проект подключён'; refreshPending();
    if (open) options.open({ id: result.app.id, name: result.app.name });
    return true;
  };
  async function execute(payload: PreparedPayload): Promise<void> {
    if (busy || !current()) return;
    busy = true; errors.textContent = ''; retryArea.replaceChildren(); status.textContent = 'Подключаем проект…';
    try {
      const result = await request<Result>('apps.prepared.create', { ...payload }, true);
      if (!await finish(payload, result, true)) throw new Error('prepared_unconfirmed');
    } catch (cause) {
      if (!current()) return;
      // Read the durable result before allowing another submission.
      try {
        const result = await request<Result>('apps.prepared.read', { ...payload });
        if (await finish(payload, result, true)) return;
        const code = typeof cause === 'object' && cause && 'code' in cause ? String(cause.code) : '';
        if (code === 'app_port_already_registered' && result.state === 'pending') {
          status.textContent = 'Этот порт уже занят. Проект может быть подключён: проверьте «Моё». Ожидание можно убрать в этом браузере; ранее начатая попытка ещё может завершиться.';
          offerRetry(payload); offerDismiss(payload);
          refreshPending();
          return;
        }
        status.textContent = result.state === 'not-found' ? 'Подключение не подтверждено. Можно повторить.' : 'Подключение ещё не завершено. Проверьте устройство и повторите.';
        offerRetry(payload); offerDismiss(payload);
      } catch {
        status.textContent = 'Результат не подтверждён. Уже отправленная попытка может завершиться; проверьте связь или уберите ожидание в этом браузере.';
        offerRetry(payload); offerDismiss(payload);
      }
      if (current()) { errors.textContent = message(cause); refreshPending(); }
    } finally { busy = false; }
  }
  async function recover(payload: PreparedPayload, allowMissing: boolean): Promise<void> {
    if (busy || !current()) return;
    busy = true; errors.textContent = ''; retryArea.replaceChildren(); status.textContent = 'Проверяем подключение…';
    try {
      const result = await request<Result>('apps.prepared.read', { ...payload });
      if (await finish(payload, result, true)) return;
      if (result.state === 'pending' || result.state === 'not-found' && allowMissing) { busy = false; await execute(payload); return; }
      status.textContent = 'Подключение не подтверждено. Нажмите «Повторить», чтобы отправить его снова.';
      refreshPending(); offerRetry(payload); offerDismiss(payload);
    } catch (cause) {
      if (current()) { status.textContent = 'Результат не подтверждён. Уже отправленная попытка может завершиться; проверьте связь или уберите ожидание в этом браузере.';
        errors.textContent = message(cause); offerRetry(payload); offerDismiss(payload); }
    }
    finally { busy = false; }
  }
  function offerRetry(payload: PreparedPayload): void {
    if (current()) retryArea.replaceChildren(button('Повторить подключение', 'refresh', 'sw-button-quiet', () => { void recover(payload, true); }));
  }
  function offerDismiss(payload: PreparedPayload): void {
    if (!current()) return;
    retryArea.append(el('small', 'sw-muted', `Номер для технической проверки: ${payload.requestId}. Удаление ожидания не отменяет отправленную попытку.`),
      button('Убрать ожидание', 'close', 'sw-button-quiet', () => {
        void (async () => { if (!await valid()) return; await state.forget(payload);
          if (!await valid()) return; retryArea.replaceChildren(); errors.textContent = ''; status.textContent = 'Ожидание убрано из этого браузера.'; refreshPending();
        })().catch(showError);
      }));
  }
  function refreshPending(): void {
    if (!current()) return;
    let pending: PreparedPayload[];
    try { pending = state.read(); } catch (cause) { showError(cause); return; }
    pendingArea.replaceChildren();
    for (const payload of pending) {
      const label = payload.location.segments.at(-1) || 'Проект';
      const row = el('div', 'sw-stack'); row.append(el('small', 'sw-muted', `Не подтверждено: ${label}`),
        button('Проверить и открыть', 'refresh', 'sw-button-quiet', () => { void recover(payload, false); }));
      pendingArea.append(row);
    }
  }
  async function settleKnown(): Promise<void> {
    for (const payload of state.read()) {
      if (!current()) return;
      try {
        const result = await request<Result>('apps.prepared.read', { ...payload });
        if ((result.state === 'ready' && result.app && /^app-[a-f0-9]{32}$/u.test(result.app.id)) || result.state === 'revoked') {
          await state.acknowledge(payload); if (current()) refreshPending();
        }
      } catch { /* Keep an unknown result visible until it can be checked. */ }
    }
  }
  async function begin(project: Preview, location: PreparedLocation): Promise<void> {
    if (!selected || busy || !current()) return;
    try {
      const prepared = await state.prepare({ expectedAccountId: accountId, hostDeviceId: selected.hostDeviceId, connectorId: selected.connectorId,
        location, manifestDigest: project.digest });
      if (!await valid()) return;
      if (prepared.created) await execute(prepared.payload); else await recover(prepared.payload, false);
    } catch (cause) { showError(cause); }
  }
  function target(): Record<string, string> {
    return { expectedAccountId: accountId, hostDeviceId: selected!.hostDeviceId, connectorId: selected!.connectorId };
  }
  async function roots(): Promise<void> {
    if (!selected || !current()) return;
    const generation = ++browseGeneration; directory.replaceChildren(); errors.textContent = '';
    if (!selected.online) { status.textContent = 'Устройство не в сети. Включите его и повторите.'; return; }
    status.textContent = 'Открываем папки устройства…';
    try {
      const result = await request<Roots>('apps.prepared.browse', target());
      if (!current() || generation !== browseGeneration) return;
      status.textContent = '';
      if (result.roots.length === 1) { await listing({ rootId: result.roots[0]!.id, segments: [] }, result.roots[0]!.label); return; }
      directory.append(el('strong', '', 'Выберите рабочую папку'));
      for (const root of result.roots) directory.append(button(root.label, 'folder', 'sw-button-quiet sw-button-wide', () => { void listing({ rootId: root.id, segments: [] }, root.label, '', [], true); }));
    } catch (cause) { if (generation === browseGeneration) showError(cause); }
  }
  async function listing(location: PreparedLocation, label: string, after = '', accumulated: string[] = [], focus = false): Promise<void> {
    if (!selected || !current()) return;
    const generation = ++browseGeneration; errors.textContent = ''; status.textContent = 'Проверяем папку…';
    try {
      const result = await request<Listing>('apps.prepared.browse', { ...target(), location, after });
      if (!current() || generation !== browseGeneration) return;
      status.textContent = ''; const entries = [...accumulated, ...result.entries];
      const title = el('strong', '', [label, ...location.segments].join(' / ')); title.tabIndex = -1;
      directory.replaceChildren(title);
      directory.append(button('Назад', 'back', 'sw-button-quiet', () => {
        if (location.segments.length) void listing({ ...location, segments: location.segments.slice(0, -1) }, label, '', [], true); else void roots();
      }));
      if (result.project) {
        const project = result.project;
        directory.append(el('strong', '', project.name), el('p', 'sw-muted', project.responding
          ? 'Адрес приложения отвечает. Папка и работающий процесс могут не совпадать.' : 'Адрес приложения не отвечает. Запустите проект на этом устройстве.'));
        const add = button('Добавить и открыть', 'plus', 'sw-button-primary sw-button-wide', () => { void begin(project, location); });
        add.disabled = !project.responding; directory.append(add);
      }
      if (result.projectError) directory.append(el('p', 'sw-muted', 'Не удалось прочитать описание проекта в этой папке. Можно выбрать другую папку.'));
      for (const name of entries) directory.append(button(name, 'folder', 'sw-button-quiet sw-button-wide', () => {
        void listing({ ...location, segments: [...location.segments, name] }, label, '', [], true);
      }));
      if (result.next) directory.append(button('Показать ещё', undefined, 'sw-button-quiet', () => { void listing(location, label, result.next!, entries, true); }));
      if (!entries.length && !result.project && !result.projectError) directory.append(el('p', 'sw-muted', 'В этой папке нет подходящего проекта.'));
      if (focus) title.focus({ preventScroll: true });
    } catch (cause) { if (generation === browseGeneration) showError(cause); }
  }
  refreshPending(); void settleKnown().catch(() => {}); status.textContent = 'Ищем ваши устройства…';
  try {
    const devices = (await request<{ devices: Device[] }>('apps.devices', { expectedAccountId: accountId })).devices.filter(value => value.claimed);
    if (!current()) return; status.textContent = '';
    if (!devices.length) {
      chooser.replaceChildren(el('p', 'sw-muted', 'Сначала подключите компьютер.'), button('Подключить устройство', 'laptop', 'sw-button-primary', options.connectDevice)); return;
    }
    const select = el('select', 'sw-select'), blank = el('option', '', 'Выберите устройство'); blank.value = ''; select.append(blank);
    devices.forEach((device, index) => { const option = el('option', '', `${device.name}${device.online ? '' : ' · не в сети'}`);
      option.value = String(index + 1); select.append(option); });
    const preferred = devices.findIndex(device => device.hostDeviceId === options.selectedDevice);
    const online = devices.map((value, index) => value.online ? index : -1).filter(index => index >= 0);
    const choice = preferred >= 0 ? preferred : online.length === 1 ? online[0]! : -1;
    select.value = choice >= 0 ? String(choice + 1) : '';
    chooser.replaceChildren(labeledField('Устройство', select));
    select.addEventListener('change', () => { selected = devices[Number(select.value) - 1] ?? null; browseGeneration++; directory.replaceChildren(); if (selected) void roots(); });
    selected = choice >= 0 ? devices[choice]! : null; if (selected) await roots();
  } catch (cause) { showError(cause); }
}
