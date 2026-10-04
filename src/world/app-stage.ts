import './app-stage.css';
import { button, el, emptyState, iconButton } from './dom';
import { createAppLauncher, formatAppLaunchRoute, parseAppLaunchRoute, sameAppLaunchLocation,
  type AppLaunchIntent, type AppLaunchPresentation, type AppLaunchRequest, type AppResolvedEntry } from './app-launch.mjs';
import { mountAppSaved, type AppSavedHandle } from './app-saved';
import { mountAppDiscussion, type AppDiscussionHandle } from './app-discussion';
import type { WorldApi, WorldAppRecord } from './types';
import { isAppExternalRequest } from './app-actions.mjs';
import { mountHiveDeviceBridge } from './hive-device-bridge.mjs';

export interface AppStageOptions {
  api: WorldApi; accountId: string; app: WorldAppRecord; intent: AppLaunchIntent;
  isCurrent(): boolean;
  request(parameters: AppLaunchRequest): Promise<{ url: string; entry: AppResolvedEntry }>;
  onNavigate(intent: AppLaunchIntent, options?: { replace?: boolean }): void;
  onBack(): void; onAccount(): Promise<void>;
  onSettings(app: WorldAppRecord, onUpdated: (app: WorldAppRecord) => void): void;
  onCommunity?(communityId: string): void;
  onRemember?(intent: AppLaunchIntent, app: WorldAppRecord): void;
}
export interface AppStageHandle {
  ready: Promise<void>; matches(intent: AppLaunchIntent): boolean; updateRoute(intent: AppLaunchIntent): void;
  updateApp(app: WorldAppRecord): void; updateCommunity(group: { communityId: string; name: string } | null): void;
  entry(): AppResolvedEntry | null; flush(): Promise<void>; hasUnsavedChanges(): boolean; dispose(): void;
}

export function appLaunchFailure(error: unknown): { title: string; detail: string } {
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : error instanceof Error ? error.message : '';
  if (code === 'ACTIVE_PROFILE_CHANGED') return { title: 'Аккаунт изменился', detail: 'Выберите нужный аккаунт и откройте приложение ещё раз.' };
  if (code === 'app_offline') return { title: 'Устройство не в сети', detail: 'Попробуйте снова, когда устройство подключится.' };
  if (['apps_access_denied', 'app_access_revoked', 'app_unavailable', 'authentication_required'].includes(code)) return { title: 'Этот вход недоступен', detail: 'Выберите другой аккаунт или попросите владельца проверить доступ.' };
  if (['apps_launch_busy', 'apps_entry_busy', 'apps_sessions_busy', 'app_public_capacity', 'app_capacity'].includes(code)) return { title: 'Приложение сейчас занято', detail: 'Попробуйте ещё раз немного позже.' };
  if (['invalid_app_launch_url', 'invalid_app_entry', 'invalid_app_path'].includes(code)) return { title: 'Не удалось безопасно открыть приложение', detail: 'Попробуйте получить новую ссылку.' };
  return { title: 'Не удалось открыть приложение', detail: 'Проверьте подключение и попробуйте ещё раз.' };
}

/** The runtime and the discussion have separate lifetimes. Presentation changes
 * never move, reparent or replace a running iframe. Only an explicit runtime
 * refresh obtains another ticket for the captured entry. */
export function mountAppStage(host: HTMLElement, options: AppStageOptions): AppStageHandle {
  const initial = options.intent, accountId = options.accountId;
  let intent = initial, app = options.app, selectedEntry: AppResolvedEntry | null = null;
  let disposed = false, runtimeStarted = false, runtimePending = false, accountPending = false;
  let routeGeneration = 0, saved: AppSavedHandle | null = null, discussion: AppDiscussionHandle | null = null;
  let presentationVersion = 0, applyingPresentation = false;
  let lastVisiblePresentation = initial.presentation;
  let group: { communityId: string; name: string } | null = null;
  const controller = new AbortController(), view = host.ownerDocument.defaultView!;
  const matches = (next: AppLaunchIntent): boolean => sameAppLaunchLocation(initial, next, selectedEntry);
  const current = (): boolean => {
    if (disposed || !options.isCurrent()) return false;
    try { const next = parseAppLaunchRoute(view.location.hash); return !!next && matches(next); } catch { return false; }
  };
  const screen = el('section', 'sa-stage'); screen.setAttribute('aria-label', 'Приложение'); screen.dataset.pwaIgnore = '';
  const header = el('header', 'sa-toolbar'), title = el('div', 'sa-title'), name = el('h2', '', app.name);
  name.tabIndex = -1; const subtitle = el('p'); title.append(name, subtitle);
  const workspace = el('div', 'sa-workspace'), runtime = el('div', 'sa-runtime'), panel = el('aside', 'sa-discussion');
  const detachHiveBridge = mountHiveDeviceBridge({ view, getFrame: () => runtime.querySelector('iframe'), isCurrent: current });
  panel.id = `discussion-${app.appId}`; panel.setAttribute('aria-label', 'Обсуждение приложения'); panel.hidden = true;
  const message = el('p', 'sa-message'); message.hidden = true; message.setAttribute('role', 'status');
  const savedHost = el('div', 'sa-saved'); savedHost.hidden = true;
  const moreHost = el('div', 'sa-more'), moreBody = el('div', 'sa-more-body'); moreBody.hidden = true;
  moreBody.id = `actions-${app.appId}`; moreBody.setAttribute('role', 'group'); moreBody.setAttribute('aria-label', 'Действия с приложением');
  const narrow = view.matchMedia('(max-width: 900px)');
  function showMessage(text: string): void { if (current()) { message.textContent = text; message.hidden = !text; } }
  function closeMore(focus = false): void {
    moreBody.hidden = true; more.setAttribute('aria-expanded', 'false'); if (focus && current()) more.focus();
  }
  const back = iconButton('Назад', 'back', () => { if (current()) options.onBack(); });
  const discuss = iconButton('Обсуждение приложения', 'chat', () => {
    if (current()) void navigatePresentation(intent.presentation ? undefined : { panel: 'discussion' }, true);
  });
  discuss.setAttribute('aria-controls', panel.id); discuss.dataset.stageControl = 'discussion';
  const more = iconButton('Действия с приложением', 'more', () => {
    if (!current()) return;
    const open = moreBody.hidden; moreBody.hidden = !open; more.setAttribute('aria-expanded', String(open));
    if (open) moreBody.querySelector<HTMLButtonElement>('button:not([hidden]):not(:disabled)')?.focus();
  });
  more.setAttribute('aria-controls', moreBody.id); more.setAttribute('aria-expanded', 'false'); more.dataset.stageControl = 'more';
  const refresh = button('Обновить приложение', 'refresh', 'sw-button-quiet', () => { closeMore(true); void launchRuntime(); });
  refresh.dataset.stageControl = 'refresh';
  const external = button('Открыть отдельно', 'external', 'sw-button-quiet', () => {
    if (!current() || external.disabled) return;
    closeMore(true); external.disabled = true; external.setAttribute('aria-busy', 'true'); showMessage('');
    // Do not await a route, flush or API read before this user-gesture popup.
    void launcher.openExternal(() => view.open('about:blank', '_blank')).then(result => {
      if (!current()) return;
      captureEntry();
      if (result === 'blocked') showMessage('Разрешите новую вкладку для Сот и нажмите «Открыть отдельно» ещё раз.');
    }).catch(reason => { const failure = appLaunchFailure(reason); showMessage(`${failure.title}. ${failure.detail}`); })
      .finally(() => { if (current()) { external.disabled = false; external.removeAttribute('aria-busy'); } });
  });
  external.dataset.stageControl = 'external';
  const settings = button('Настройки приложения', 'settings', 'sw-button-quiet', () => {
    closeMore(true); if (current()) options.onSettings(app, updateApp);
  });
  const archives = button('Архив владельца', 'history', 'sw-button-quiet', () => {
    closeMore(true); if (current()) void navigatePresentation({ panel: 'discussion', administrative: true }, true);
  });
  const community = button('Сообщество', 'people', 'sw-button-quiet', () => {
    closeMore(true); if (current() && group) options.onCommunity?.(group.communityId);
  }); community.hidden = true;
  const otherAccount = button('Другой аккаунт', 'person', 'sw-button-quiet', () => {
    closeMore(true); if (!current() || accountPending) return;
    accountPending = true; otherAccount.disabled = true;
    void options.onAccount().catch(() => showMessage('Не удалось открыть аккаунт. Попробуйте ещё раз.'))
      .finally(() => { if (current()) { accountPending = false; otherAccount.disabled = false; } });
  });
  moreBody.append(external, refresh, community, settings, archives, otherAccount); moreHost.append(more, moreBody);
  header.append(back, title, savedHost, discuss, moreHost); workspace.append(runtime, panel); screen.append(header, message, workspace);
  host.replaceChildren(screen);
  const launcher = createAppLauncher({ target: initial.target, accountId, shellUrl: view.location.href,
    isCurrent: expected => expected === accountId && current(), request: options.request,
    resolveEntry: parameters => options.api.request<{ entry: AppResolvedEntry }>('apps.entry.get', { ...parameters }),
  });
  function routeWith(presentation?: AppLaunchPresentation): AppLaunchIntent {
    // Preserve canonical/community route identity, while pinning the path that
    // was actually admitted. A named route always keeps its exact domain ID.
    return parseAppLaunchRoute(formatAppLaunchRoute({ ...initial.target, ...(selectedEntry ? { path: selectedEntry.path } : {}) }, initial.communityId, presentation))!;
  }
  async function navigatePresentation(presentation?: AppLaunchPresentation, focus = false): Promise<void> {
    const generation = ++routeGeneration;
    await discussion?.flush();
    if (!current() || generation !== routeGeneration) return;
    if (discussion?.hasUnsavedChanges()) { showMessage('Скопируйте черновик: браузер пока не смог его сохранить.'); return; }
    const next = routeWith(presentation); options.onNavigate(next); updateRoute(next);
    if (focus) { if (presentation) discussion?.focus(); else discuss.focus(); }
  }
  function showPanel(): void {
    const visible = !!intent.presentation;
    panel.hidden = !visible; screen.classList.toggle('has-discussion', visible);
    discuss.setAttribute('aria-expanded', String(visible));
    runtime.inert = visible && narrow.matches;
    discussion?.setVisible(visible && host.ownerDocument.visibilityState === 'visible');
  }
  function ensureDiscussion(): void {
    const presentation = intent.presentation;
    if (!presentation || discussion || !current()) return;
    // A validated route can locate the account's own local draft even when the
    // server can no longer resolve that entry. It authorizes no network read.
    const retainedEntry = initial.target.domainId && initial.target.path !== undefined
      ? { domainId: initial.target.domainId, path: initial.target.path } : undefined;
    discussion = mountAppDiscussion(panel, { api: options.api, accountId, appId: initial.target.appId,
      entry: selectedEntry, ...(retainedEntry ? { retainedEntry } : {}),
      ...(presentation.administrative ? { administrative: true } : {}),
      ...(presentation.conversationId ? { initialConversationId: presentation.conversationId } : {}), isCurrent: current,
      onConversationChange: selection => { void navigatePresentation({ panel: 'discussion',
        ...(selection.conversationId ? { conversationId: selection.conversationId } : {}),
        ...(selection.administrative ? { administrative: true } : {}) }); },
      onClose: () => { void navigatePresentation(undefined, true); },
    });
    showPanel();
  }
  function captureEntry(): void {
    const entry = launcher.entry();
    if (!entry || selectedEntry || !current()) return;
    selectedEntry = entry;
    const pinned = routeWith(intent.presentation); options.onNavigate(pinned, { replace: true }); intent = pinned;
    savedHost.hidden = false; saved = mountAppSaved(savedHost, { api: options.api, accountId, entry, isCurrent: current });
    if (discussion) void discussion.updateEntry(entry).catch(() => showMessage('Не удалось обновить обсуждение. Откройте его ещё раз.'));
    // A local-only panel is mounted only once resolution has settled. It is
    // never upgraded using a later guess from optional catalogue metadata.
    ensureDiscussion(); showPanel();
    options.onRemember?.(pinned, app);
  }
  async function launchRuntime(): Promise<void> {
    if (!current() || runtimePending) return;
    runtimeStarted = true; runtimePending = true; refresh.disabled = true; showMessage('');
    const existingFrame = runtime.querySelector('iframe');
    if (!existingFrame) { const loading = el('p', 'sa-loading', 'Соединяем с приложением'); loading.setAttribute('role', 'status'); runtime.replaceChildren(loading); }
    try {
      const url = await launcher.launch();
      if (!url || !current()) return;
      captureEntry();
      const frame = el('iframe', 'sa-frame'); frame.title = app.name; frame.setAttribute('sandbox', 'allow-scripts allow-forms allow-same-origin allow-downloads'); frame.referrerPolicy = 'no-referrer'; frame.src = url;
      runtime.replaceChildren(frame);
    } catch (reason) {
      if (!current()) return;
      captureEntry(); const failure = appLaunchFailure(reason);
      if (existingFrame) showMessage(`${failure.title}. ${failure.detail}`);
      else runtime.replaceChildren(emptyState(failure.title, failure.detail, button('Попробовать снова', 'refresh', 'sw-button-primary', () => { void launchRuntime(); }), 'app'));
    } finally {
      if (current()) { runtimePending = false; refresh.disabled = false; ensureDiscussion(); showPanel(); }
    }
  }
  function updateRoute(next: AppLaunchIntent): void {
    if (disposed || !matches(next)) return;
    const wasVisible = !panel.hidden, focusedPanel = panel.contains(host.ownerDocument.activeElement);
    intent = next; presentationVersion++;
    // Closing is immediate and keeps the component/drafts mounted. Reopening
    // must select the route again: a previously shown archive is not the
    // implicit current conversation of a later Back/forward entry.
    if (!next.presentation) showPanel();
    else ensureDiscussion();
    void applyPresentation();
    if (wasVisible && !next.presentation && focusedPanel) discuss.focus();
  }
  async function applyPresentation(): Promise<void> {
    if (applyingPresentation) return;
    applyingPresentation = true;
    try {
      let applied;
      do {
        applied = presentationVersion;
        const selection = intent.presentation;
        try {
          if (selection && discussion) await discussion.updateSelection({
            ...(selection.conversationId ? { conversationId: selection.conversationId } : {}), administrative: !!selection.administrative });
        } catch {
          if (current() && applied === presentationVersion) {
            // A refused selection must not leave an archive URL describing a
            // different, still-mounted conversation. Keep the former panel
            // accessible so the person can copy or resolve its own draft.
            const restored = routeWith(lastVisiblePresentation);
            intent = restored; options.onNavigate(restored, { replace: true });
            showMessage('Скопируйте черновик или разрешите его конфликт перед переходом.');
          }
        }
        if (!current()) return;
        if (applied === presentationVersion) {
          showPanel();
          if (intent.presentation) lastVisiblePresentation = intent.presentation;
          if (!intent.presentation?.administrative && !runtimeStarted) void launchRuntime();
        }
      } while (applied !== presentationVersion);
    } finally { applyingPresentation = false; }
  }
  function updateApp(value: WorldAppRecord): void {
    if (!current() || value.appId !== initial.target.appId) return;
    app = value; name.textContent = value.name; name.title = value.name;
    subtitle.textContent = value.deviceLabel ? `На устройстве «${value.deviceLabel}»` : 'Приложение в Сотах';
    const frame = runtime.querySelector('iframe'); if (frame) frame.title = value.name;
    settings.hidden = archives.hidden = value.ownerAccountId !== accountId;
    // A policy change is a reason to re-read the current discussion; its own
    // state machine preserves a former audience's draft independently.
    if (discussion) void discussion.refresh();
  }
  function updateCommunity(value: { communityId: string; name: string } | null): void {
    if (!current()) return;
    group = value && value.communityId === initial.communityId ? value : null; community.hidden = !group;
    if (group) { community.querySelector('span')!.textContent = group.name; community.setAttribute('aria-label', `Сообщество ${group.name}`); }
  }
  host.ownerDocument.addEventListener('pointerdown', event => {
    if (!moreBody.hidden && event.target instanceof view.Node && !moreHost.contains(event.target)) closeMore();
  }, { signal: controller.signal });
  host.ownerDocument.addEventListener('keydown', event => { if (event.key === 'Escape' && !moreBody.hidden) { event.preventDefault(); closeMore(true); } }, { signal: controller.signal });
  host.ownerDocument.addEventListener('visibilitychange', showPanel, { signal: controller.signal });
  narrow.addEventListener('change', showPanel, { signal: controller.signal });
  view.addEventListener('message', event => {
    if (isAppExternalRequest(event, { frameWindow: runtime.querySelector('iframe')?.contentWindow ?? null,
      origin: selectedEntry?.origin, current: current(), activated: view.navigator.userActivation?.isActive === true })) external.click();
  }, { signal: controller.signal });
  updateApp(app);
  // Start admission synchronously before the owner asks for optional metadata.
  const ready = initial.presentation?.administrative
    ? (ensureDiscussion(), showPanel(), Promise.resolve()) : launchRuntime();
  return { ready, matches, updateRoute, updateApp, updateCommunity, entry: () => current() ? selectedEntry : null,
    flush: async () => { await discussion?.flush(); }, hasUnsavedChanges: () => !!discussion?.hasUnsavedChanges(),
    dispose() { if (disposed) return; disposed = true; detachHiveBridge(); routeGeneration++; presentationVersion++; controller.abort(); launcher.dispose(); saved?.dispose(); discussion?.dispose(); screen.remove(); },
  };
}
