import { createDialog, isDialogFocusTarget, type DialogReturnTarget } from './dialogs';
import { createOtsovEditorHost } from './otsov-editor-handoff.mjs';
import type { WorldProjectEditorPort } from './types';
import { createBrowserAssistHost } from './browser-assist-pilot.mjs';
import { mountAppInvitationCarrier } from './app-invitation-carrier.mjs';
import './app-stage.css';
import { button, el, emptyState, iconButton } from './dom';
import { createAppLauncher, formatAppLaunchRoute, parseAppLaunchRoute, sameAppLaunchLocation,isScopedRuntimeProfile,
  validateAppEntry, type AppLaunchIntent, type AppLaunchPresentation, type AppLaunchRequest, type AppResolvedEntry } from './app-launch.mjs';
import { createAppBootRecovery } from './app-boot-recovery.mjs';
import { createAppRuntimeDisplay, type RuntimeDisplayHandle } from './app-runtime-display.mjs';
import { createWidgetRuntimeControls } from './app-widget-runtime-controls.mjs';
import { createAppRuntimePhaseTrace, type RuntimePhase } from './app-runtime-phase-trace.mjs';
import type { AppLaunchBinding } from './app-launch.mjs';
import { mountAppSaved, type AppSavedHandle } from './app-saved';
import { mountAppDiscussion, type AppDiscussionHandle } from './app-discussion';
import type { WorldApi, WorldAppRecord, WorldAssistantContext } from './types';
import { isAppExternalRequest } from './app-actions.mjs';
import { mountHiveDeviceBridge } from './hive-device-bridge.mjs';
import { mountRootVoiceCarrier } from './native-voice-carrier.mjs';
import { mountAppFullscreen } from './app-fullscreen.mjs';
import { icon } from './icons';
import { mountAppFeedback, type AppFeedbackHandle } from './app-feedback';
import { mountAppReviews, type AppReviewsHandle } from './app-reviews';
import './project-feedback-picker.css';
import {mountProjectCaptureBridge,type ProjectCapturePeer} from './project-feedback-capture.mjs';
import {pickProjectFeedbackMedia} from './project-feedback-picker.mjs';
import {captureSourcePin,matchesScopedCaptureContext} from './app-project-capture.mjs';
import {createScopedSlotRenewal,attachScopedRenewalLoads,attachRootSsoWakeRecovery,attachSourceFrameRefresh,isRootSsoWorkspacePin,type ScopedRenewReply} from './app-scoped-renewal.mjs';

export interface AppStageOptions {
  minimalChrome?: boolean;
  presentation?: 'widget';
  /** Stable UI locator only; Root binds it to the authenticated app/person/device. */
  instanceId?: string;
  nativeVoice?: { client:any; mount:any };
  api: WorldApi; accountId: string; app: WorldAppRecord; intent: AppLaunchIntent;
  isCurrent(): boolean;
  request(parameters: AppLaunchRequest): Promise<{ url: string; entry: AppResolvedEntry;launchBinding?:AppLaunchBinding;runtimeProfile?:string;scopedCloseHandle?:string;scopedCleanup?:()=>Promise<void>;scopedSource?:{id:string;version:number;digest:string} }>;
  renew?:((parameters:{appId:string;handle:string;requestId:string;expectedAccountId:string;shortSessionHandoff?:true})=>Promise<{url:string;entry:AppResolvedEntry;runtimeProfile:string;scopedCloseHandle:string;scopedSlotExpiresAt:number;scopedRenewalRequestId:string;scopedSource:unknown;scopedRenewalBindingDigest?:string;scopedCleanup?:(()=>Promise<void>)|undefined}>)|undefined;
  onNavigate(intent: AppLaunchIntent, options?: { replace?: boolean }): void;
  /** Explicit navigation to a fresh server-resolved Source entry, never a silent rebind. */
  onOpenCurrentEntry?(entry: AppResolvedEntry): void | Promise<void>;
  onBack(): void; onAccount(): Promise<void>;
  onFullscreenChange?(stage: HTMLElement, expanded: boolean): void;
  onModalChange?(modal: HTMLDialogElement, open: boolean): void;
  onSettings(app: WorldAppRecord, onUpdated: (app: WorldAppRecord) => void): void;
  onPlaceInField?(app: WorldAppRecord): void;
  onWidget?(app: WorldAppRecord, intent: AppLaunchIntent): void;
  onCommunity?(communityId: string): void;
  onRemember?(intent: AppLaunchIntent, app: WorldAppRecord): void;
}
export interface AppStageHandle {
  /** Local presentation port. It transfers one UI button, never the iframe. */
  mountRuntimeControls?(host:HTMLElement,owner:DialogReturnTarget):boolean;
  ready: Promise<void>; matches(intent: AppLaunchIntent): boolean; updateRoute(intent: AppLaunchIntent): void;
  updateApp(app: WorldAppRecord): void; updateCommunity(group: { communityId: string; name: string } | null): void;
  entry(): AppResolvedEntry | null; flush(): Promise<void>; hasUnsavedChanges(): boolean; dispose(): void;
  assistantContext(): WorldAssistantContext | null;
  browserAssist(): import('./types').WorldBrowserAssist | null;
  openProjectEditor:WorldProjectEditorPort;
}

export function appLaunchFailure(error: unknown): { title: string; detail: string } {
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : error instanceof Error ? error.message : '';
  if (code === 'ACTIVE_PROFILE_CHANGED') return { title: 'Аккаунт изменился', detail: 'Выберите нужный аккаунт и откройте приложение ещё раз.' };
  if (code === 'app_offline') return { title: 'Устройство не в сети', detail: 'Попробуйте снова, когда устройство подключится.' };
  if (['app_scoped_admission_required','app_scoped_human_required','app_scoped_migration_required','app_scoped_key_required'].includes(code)) return {title:'Подключение пока не готово',detail:'Владелец приложения должен завершить подключение выбранного проекта.'};
  if (['apps_access_denied', 'app_access_revoked', 'app_unavailable', 'authentication_required'].includes(code)) return { title: 'Этот вход недоступен', detail: 'Выберите другой аккаунт или попросите владельца проверить доступ.' };
  if (['apps_launch_busy', 'apps_entry_busy', 'apps_sessions_busy', 'app_public_capacity', 'app_capacity'].includes(code)) return { title: 'Приложение сейчас занято', detail: 'Попробуйте ещё раз немного позже.' };
  if (['invalid_app_launch_url', 'invalid_app_entry', 'invalid_app_path', 'invalid_app_launch_binding'].includes(code)) return { title: 'Не удалось безопасно открыть приложение', detail: 'Попробуйте получить новую ссылку.' };
  if (['app_launch_source_changed', 'app_launch_binding_required'].includes(code)) return { title: 'Вход изменился', detail: 'Откройте приложение заново.' };
  if (['scoped_embed_source_changed', 'scoped_embed_authority_changed'].includes(code)) return { title: 'Адрес приложения изменился', detail: 'Откройте актуальный вход. Сохранённая ссылка останется на прежнем адресе.' };
  return { title: 'Не удалось открыть приложение', detail: 'Проверьте подключение и попробуйте ещё раз.' };
}

/** The runtime and the discussion have separate lifetimes. Presentation changes
 * never move, reparent or replace a running iframe. An explicit refresh or one
 * bounded boot recovery obtains another ticket for the captured entry. */
export function mountAppStage(host: HTMLElement, options: AppStageOptions): AppStageHandle {
  const initial = options.intent, accountId = options.accountId;
  let intent = initial, app = options.app, selectedEntry: AppResolvedEntry | null = null;
  let disposed = false, runtimeStarted = false, runtimePending = false, accountPending = false;
  let routeGeneration = 0, frameGeneration=0, saved: AppSavedHandle | null = null, discussion: AppDiscussionHandle | null = null;
  let presentationVersion = 0, applyingPresentation = false;
  let displayEpoch = 0, entryRecoveryEpoch = 0, entryRecoveryPending = false, releaseRuntimeDisplay: (() => void) | null = null;
  let runtimeDisplay: RuntimeDisplayHandle | null = null;
  let feedback: AppFeedbackHandle | null = null;
  let reviews: AppReviewsHandle | null = null;
  let lastVisiblePresentation = initial.presentation;
  let group: { communityId: string; name: string } | null = null;
  const controller = new AbortController(), view = host.ownerDocument.defaultView!;
  const matches = (next: AppLaunchIntent): boolean => sameAppLaunchLocation(initial, next, selectedEntry);
  const current = (): boolean => {
    if (disposed || !options.isCurrent()) return false;
    if (options.presentation === 'widget') return true;
    try { const next = parseAppLaunchRoute(view.location.hash); return !!next && matches(next); } catch { return false; }
  };
  const screen = el('section', 'sa-stage'); screen.setAttribute('aria-label', 'Приложение'); screen.dataset.pwaIgnore = '';
  if (options.presentation === 'widget') screen.classList.add('sa-widget-stage');
  const header = el('header', 'sa-toolbar'), title = el('div', 'sa-title'), name = el('h2', '', app.name);
  name.tabIndex = -1; const subtitle = el('p'); title.append(name, subtitle);
  const workspace = el('div', 'sa-workspace'), runtime = el('div', 'sa-runtime'), panel = el('aside', 'sa-discussion');
  let diagnosticOpen = false, closeDiagnostic: (() => void) | null = null, renderDiagnostic: (() => void) | null = null;
  const runtimeTrace = createAppRuntimePhaseTrace({view,isCurrent:current,getFrame:()=>runtime.querySelector('iframe'),getEpoch:()=>displayEpoch,
    canSample:()=>!diagnosticOpen,onChange:()=>renderDiagnostic?.()});
  const detachHiveBridge = mountHiveDeviceBridge({ view, getFrame: () => runtime.querySelector('iframe'), isCurrent: current });
  const detachVoiceBridge=options.nativeVoice?mountRootVoiceCarrier({view,getFrame:()=>runtime.querySelector('iframe'),isCurrent:current,client:options.nativeVoice.client,mount:options.nativeVoice.mount,appId:app.appId,accountId}):()=>{};
  panel.id = `discussion-${app.appId}`; panel.setAttribute('aria-label', 'Обсуждение приложения'); panel.hidden = true;
  const message = el('p', 'sa-message'); message.hidden = true; message.setAttribute('role', 'status'); message.setAttribute('aria-live', 'polite'); message.setAttribute('aria-atomic', 'true');
  let messageOwner: 'manual' | 'runtime' | 'renewal' = 'manual';
  const savedHost = el('div', 'sa-saved'); savedHost.hidden = true;
  const feedbackHost = el('div');
  const reviewsHost = el('div');
  const report = button('Сообщить проблему', 'flag', 'sw-button-quiet', () => {
    if (!current()) return;
    if (!moreBody.hidden) closeMore(true);
    feedback ??= mountAppFeedback(feedbackHost, { api: options.api, accountId, appId: app.appId, title: app.name, isCurrent: current,
      onModalChange: (modal, open) => { if (!open || current()) options.onModalChange?.(modal, open); } });
    feedback.open();
  });
  report.setAttribute('aria-label', 'Сообщить проблему'); report.title = 'Сообщить проблему';
  report.dataset.stageControl = 'feedback';
  const field = iconButton('Добавить на моё поле', 'plus', () => {
    const returnToMore = field.parentElement === moreBody;
    closeMore(returnToMore);
    void fullscreen.leave().then(() => {
      if (!current()) return;
      if (returnToMore) more.focus();
      options.onPlaceInField?.(app);
    })
      .catch(() => showMessage('Не удалось открыть поле. Попробуйте ещё раз.'));
  });
  field.dataset.stageControl = 'field'; field.hidden = !options.onPlaceInField; field.disabled = true;
  const widget = button('Закрепить виджет', 'pin', 'sw-button-quiet', () => {
    if (!current() || !selectedEntry) return;
    closeMore(); options.onWidget?.(app, parseAppLaunchRoute(formatAppLaunchRoute({ appId: app.appId,
      domainId: selectedEntry.domainId, path: selectedEntry.path }))!);
  });
  widget.dataset.stageControl = 'widget'; widget.hidden = !options.onWidget; widget.disabled = true;
  const moreHost = el('div', 'sa-more'), moreBody = el('div', 'sa-more-body'); moreBody.hidden = true;
  moreBody.id = `actions-${app.appId}`; moreBody.setAttribute('role', 'group'); moreBody.setAttribute('aria-label', 'Действия с приложением');
  const narrow = view.matchMedia('(max-width: 900px)');
  const shortToolbar = view.matchMedia('(max-height: 480px)');
  const compactToolbar = (): boolean => options.minimalChrome === true || shortToolbar.matches;
  function showMessage(text: string, owner: 'manual' | 'runtime' | 'renewal' = 'manual'): void { if (current()) { message.textContent = text; message.hidden = !text; messageOwner = owner; } }
  function closeMore(focus = false): void {
    moreBody.hidden = true; more.setAttribute('aria-expanded', 'false'); if (focus && current()) more.focus();
  }
  function fitMore(): void {
    if (options.presentation === 'widget') return;
    if (!current()) { closeMore(); return; }
    const active = host.ownerDocument.activeElement;
    if (compactToolbar() && active?.closest('.sw-topbar,.sw-mobile-nav') && !active.getClientRects().length) more.focus();
    if (!compactToolbar() || moreBody.hidden) {
      for (const property of ['max-height', 'max-width', 'translate']) moreBody.style.removeProperty(property);
      return;
    }
    const viewport = view.visualViewport;
    const stage = screen.getBoundingClientRect(), padding = view.getComputedStyle(header);
    const pixels = (value: string): number => Math.max(0, Number.parseFloat(value) || 0);
    const left = Math.max(stage.left, viewport?.offsetLeft ?? 0) + pixels(padding.paddingLeft);
    const right = Math.min(stage.right, view.innerWidth,
      (viewport?.offsetLeft ?? 0) + (viewport?.width ?? view.innerWidth)) - pixels(padding.paddingRight);
    const bottom = Math.min(stage.bottom, view.innerHeight,
      (viewport?.offsetTop ?? 0) + (viewport?.height ?? view.innerHeight)) - 8;
    moreBody.style.removeProperty('translate'); moreBody.style.removeProperty('max-height');
    moreBody.style.maxWidth = `${Math.max(0, Math.floor(right - left))}px`;
    const rect = moreBody.getBoundingClientRect();
    const x = Math.max(left - rect.left, Math.min(0, right - rect.right));
    moreBody.style.translate = `${x}px 0px`;
    moreBody.style.maxHeight = `${Math.max(0, Math.floor(bottom - rect.top))}px`;
  }
  function syncShortToolbar(): void {
    if (options.presentation === 'widget' || !current()) return;
    const controls = [field, report, discuss], target = compactToolbar() ? moreBody : header;
    if (controls.some(control => control.parentElement !== target)) {
      const restoreFocus = controls.some(control => control.contains(host.ownerDocument.activeElement));
      if (compactToolbar()) moreBody.prepend(...controls);
      else for (const control of controls) header.insertBefore(control, expand);
      if (restoreFocus) closeMore(true);
    }
    fitMore();
  }
  function focusDiscussionControl(): void {
    (discuss.parentElement === moreBody && moreBody.hidden ? more : discuss).focus();
  }
  const back = iconButton(options.minimalChrome ? 'Вернуться на поле' : 'Назад', options.minimalChrome ? 'close' : 'back', () => { if (current()) options.onBack(); });
  if (options.minimalChrome) { screen.classList.add('of-app-surface'); back.classList.add('of-app-close'); }
  const discuss = iconButton('Обсуждение приложения', 'chat', () => {
    if (current()) { closeMore(); void navigatePresentation(intent.presentation ? undefined : { panel: 'discussion' }, true); }
  });
  discuss.setAttribute('aria-controls', panel.id); discuss.dataset.stageControl = 'discussion';
  const more = iconButton('Действия с приложением', 'more', () => {
    if (!current()) return;
    const open = moreBody.hidden; moreBody.hidden = !open; more.setAttribute('aria-expanded', String(open));
    fitMore();
    if (open) moreBody.querySelector<HTMLButtonElement>('button:not([hidden]):not(:disabled)')?.focus();
  });
  more.setAttribute('aria-controls', moreBody.id); more.setAttribute('aria-expanded', 'false'); more.dataset.stageControl = 'more';
  const expand = iconButton('На весь экран', 'expand', () => { closeMore(); void fullscreen.toggle(); });
  expand.dataset.stageControl = 'fullscreen';
  const fullscreen = mountAppFullscreen({ screen, isCurrent: current, onChange: state => {
    const label = state.active ? 'Вернуть обычный вид' : 'На весь экран';
    expand.title = label; expand.setAttribute('aria-label', label); expand.setAttribute('aria-pressed', String(state.active));
    expand.disabled = state.pending;
    expand.replaceChildren(icon(state.active ? 'collapse' : 'expand'), el('span', '', label));
    options.onFullscreenChange?.(screen, state.active);
  } });
  const refresh = button('Обновить приложение', 'refresh', 'sw-button-quiet', () => { closeMore(true); void launchRuntime(false, () => true, true); });
  refresh.dataset.stageControl = 'refresh';
  const external = button('Открыть отдельно', 'external', 'sw-button-quiet', () => {
    if (!current() || external.disabled) return;
    closeMore(true); external.disabled = externalRecovery.disabled = true; external.setAttribute('aria-busy', 'true'); showMessage('');
    // Do not await a route, flush or API read before this user-gesture popup.
    void launcher.openExternal(() => view.open('about:blank', '_blank')).then(result => {
      if (!current()) return;
      captureEntry();
      if (result === 'blocked') showMessage('Разрешите новую вкладку для Сот и нажмите «Открыть отдельно» ещё раз.');
    }).catch(reason => { const failure = appLaunchFailure(reason); showMessage(`${failure.title}. ${failure.detail}`); })
      .finally(() => { if (current()) { external.disabled = externalRecovery.disabled = false; external.removeAttribute('aria-busy'); } });
  });
  external.dataset.stageControl = 'external';
  const externalRecovery = button('Открыть в новой вкладке', 'external', 'sw-button-quiet sa-recovery-retry sa-external-recovery', () => { external.click(); });
  externalRecovery.dataset.stageControl = 'external-recovery'; externalRecovery.hidden = true;
  const dismissRuntimeNotice = button('Скрыть сообщение', 'close', 'sw-button-quiet sa-recovery-retry', () => {
    if (current() && runtimeDisplay?.dismiss()) name.focus();
  });
  dismissRuntimeNotice.dataset.stageControl = 'runtime-dismiss'; dismissRuntimeNotice.hidden = true;
  const settings = button('Настройки приложения', 'settings', 'sw-button-quiet', () => {
    closeMore(true); if (current()) void fullscreen.leave().then(() => { if (current()) options.onSettings(app, updateApp); });
  });
  const archives = button('Архив владельца', 'history', 'sw-button-quiet', () => {
    closeMore(true); if (current()) void navigatePresentation({ panel: 'discussion', administrative: true }, true);
  });
  const community = button('Сообщество', 'people', 'sw-button-quiet', () => {
    closeMore(true); if (current() && group) options.onCommunity?.(group.communityId);
  }); community.hidden = true;
  const publicReviews = button('Отзывы', 'list', 'sw-button-quiet', () => {
    closeMore(); if (!current()) return;
    reviews ??= mountAppReviews(reviewsHost, { api: options.api, appId: app.appId, title: app.name, isCurrent: current,
      entry: () => selectedEntry ? { domainId: selectedEntry.domainId, path: selectedEntry.path } : null, onClose: () => { if (current()) more.focus(); } });
    reviews.open();
  }); publicReviews.dataset.stageControl = 'reviews';
  const otherAccount = button('Другой аккаунт', 'person', 'sw-button-quiet', () => {
    closeMore(true); if (!current() || accountPending) return;
    accountPending = true; otherAccount.disabled = true;
    void fullscreen.leave().then(() => current() ? options.onAccount() : undefined).catch(() => showMessage('Не удалось открыть аккаунт. Попробуйте ещё раз.'))
      .finally(() => { if (current()) { accountPending = false; otherAccount.disabled = false; } });
  });
  const bookmark = el('div', 'sa-bookmark'); bookmark.hidden = true;
  bookmark.append(el('p', 'sa-bookmark-label', 'Закладка этого входа'), savedHost);
  const diagnostics = button('Диагностика загрузки', 'info', 'sw-button-quiet', openRuntimeDiagnostics);
  diagnostics.hidden = options.presentation === 'widget';
  moreBody.append(widget, publicReviews, external, refresh, diagnostics, community, settings, archives, otherAccount, bookmark); moreHost.append(more, moreBody);
  function openRuntimeDiagnostics(): void {
    if (!current() || options.presentation === 'widget' || diagnosticOpen) return;
    closeMore(); runtimeTrace.sample(); diagnosticOpen = true;
    const dialog = createDialog('Диагностика загрузки', () => {
      diagnosticOpen = false; closeDiagnostic = renderDiagnostic = null; options.onModalChange?.(dialog.element, false);
    }, {isCurrent:current,resolve:()=>more});
    dialog.body.classList.add('sa-runtime-diagnostics');
    closeDiagnostic = () => dialog.close({restoreFocus:false}); options.onModalChange?.(dialog.element, true);
    const explanation = el('p', '', 'Включите запись, затем нажмите обычную кнопку «Обновить приложение» или «Открыть приложение заново». События не подтверждают готовность или доступ.');
    const privacy = el('p', '', 'Отчёт хранится в памяти до двух минут. В нём только этапы загрузки и признаки видимости окна.');
    const status = el('p'); status.setAttribute('role','status');
    const events = el('ol'), geometry = el('p'), technical = el('details'), summary = el('summary','','Технические сведения');
    const report = el('textarea','sw-textarea'); report.readOnly = true; report.rows = 8; technical.append(summary,report);
    report.setAttribute('aria-label','Безопасный отчёт диагностики'); report.spellcheck = false;
    const arm = button('Записать следующее открытие', undefined, 'sw-button-primary', () => { if (runtimeTrace.arm()) dialog.close(); });
    const copy = button('Копировать безопасный отчёт', undefined, 'sw-button-quiet', () => {
      copy.disabled = true;
      void runtimeTrace.copy().then(ok => { if (current() && diagnosticOpen) { status.textContent = ok ? 'Безопасный отчёт скопирован.' : 'Копирование недоступно. Можно выделить отчёт вручную.'; copy.disabled = !runtimeTrace.snapshot(); } });
    });
    const clear = button('Очистить', undefined, 'sw-button-quiet', () => runtimeTrace.clear());
    const actions = el('div','sw-dialog-actions'); actions.append(arm,copy,clear);
    const labels:Record<RuntimePhase,string> = {
      root_launch:'Обычное открытие начато',root_launch_failed:'Открытие завершилось отказом',root_frame:'Встроенное окно создано',root_attached:'Встроенное окно добавлено на страницу',
      root_load:'Окно сообщило о загрузке документа; содержимое ещё не проверено',root_error:'Окно сообщило об ошибке загрузки',root_timeout:'За 12 секунд окно не сообщило о загрузке документа',
      root_boot_unobserved:'Ответ страницы запуска не получен; она могла не загрузиться или уже перейти к приложению',boot_document_ack:'Страница запуска ответила на проверку этого открытия; готовность приложения не подтверждена',
      boot_admission_pending:'Страница запуска ожидает ответ проверки входа',boot_cookie_pending:'Страница запуска ожидает подтверждение сессии окна',boot_continuation_pending:'Страница запуска продолжает открытие приложения',
      boot_navigation:'Запрошен переход к приложению; показ содержимого не подтверждён',boot_failed:'Страница запуска сообщила об отказе',boot_inactive:'Страница запуска покидается или отменена',
      page_csp_frame_enforce:'Браузер применил ограничение для окон на странице',page_csp_frame_report:'Браузер сообщил о проверке ограничений для окон на странице',
      page_csp_script_enforce:'Браузер применил ограничение для скриптов на странице',page_csp_script_report:'Браузер сообщил о проверке ограничений для скриптов на странице',
      page_csp_other_enforce:'Браузер применил другое ограничение на странице',page_csp_other_report:'Браузер сообщил о проверке других ограничений на странице',
    };
    renderDiagnostic = () => {
      const value = runtimeTrace.snapshot(), state = runtimeTrace.state();
      status.textContent = state === 'armed' ? 'Запись подготовлена для следующего ручного открытия.' : value
        ? value.truncated ? 'Достигнут предел записи. Сохранены первые 24 события.' : 'События текущего открытия; готовность приложения неизвестна.' : 'Запись выключена.';
      events.replaceChildren(...(value?.events ?? []).map(event=>el('li','',`${event.t100ms/10} с · ${labels[event.phase]}`)));
      geometry.hidden = !value?.view;
      geometry.textContent = value?.view?.connected && value.view.hasArea && value.view.ancestorsVisible && value.view.hit === 'all'
        ? 'Последняя проверка со стороны Сот не обнаружила перекрытия окна. Содержимое приложения этим не проверяется.'
        : 'Видимость окна полностью не подтверждена. Этот отчёт не определяет причину.';
      report.value = value ? JSON.stringify(value,null,2) : ''; technical.hidden = !value;
      copy.disabled = !value; clear.disabled = !value && state !== 'armed';
    };
    dialog.body.append(explanation,privacy,status,events,geometry,technical,actions); renderDiagnostic();
  }
  let assistChannel = crypto.randomUUID().replaceAll('-', ''), assistEpoch = 0, assistOn = false;
  let closeAssistDialog: (() => void) | null = null;
  const askAssist = (text: string, signal?: AbortSignal): Promise<boolean> => {
    closeAssistDialog?.();
    if (!current() || signal?.aborted) return Promise.resolve(false);
    return new Promise(resolve => {
      let done = false;
      const finish = (accepted: boolean) => {
        if (done) return; done = true; view.clearTimeout(timer); signal?.removeEventListener('abort', cancel);
        closeAssistDialog = null; resolve(accepted && current()); dialog.close();
      };
      const cancel = () => finish(false);
      const dialog = createDialog('Помощник в приложении', () => { options.onModalChange?.(dialog.element, false); finish(false); },
        { isCurrent: current, resolve: () => assistToggle }, screen);
      const timer = view.setTimeout(cancel, 60000);
      closeAssistDialog = cancel; signal?.addEventListener('abort', cancel, { once: true });
      dialog.body.append(el('p', '', text), el('p', 'sw-muted', 'Подпись действия предоставлена приложением; проверьте его смысл.'),
        el('p', 'sw-muted', 'Пилот для доступного приложения: одно действие за запрос. Без подтверждения действие не выполняется. Ожидание — до минуты.'));
      const no = button('Отмена', undefined, 'sw-button-quiet', cancel);
      dialog.body.append(no, button('Разрешить', undefined, 'sw-button-primary', () => finish(true)));
      options.onModalChange?.(dialog.element, true); no.focus();
    });
  };

  const assist = createBrowserAssistHost({ view, readPeer: () => {
    const frame = runtime.querySelector('iframe'), pin = launcher.binding(), entry = launcher.entry();
    return current() && options.presentation !== 'widget' && frame?.contentWindow && pin && entry ? {
      window: frame.contentWindow, binding: { appId: app.appId, origin: entry.origin, domainId: entry.domainId,
        targetRevision: pin.targetRevision, targetDigest: pin.targetDigest, generation: frameGeneration, channel: assistChannel },
    } : null;
  }, approve: async (value, { signal }) => {
    const command = value as { kind: string; label: string; text?: string };
    return askAssist(`Помощник: ${app.name}\n${command.kind === 'fill' ? 'Заполнить поле' : 'Нажать кнопку'}: ${command.label}${command.kind === 'fill' ? `\nТекст: ${command.text}` : ''}\nРазрешить только это действие?`, signal);
  } });
  const stopAssist = () => { closeAssistDialog?.(); assistEpoch++; assistOn = false; assist.revoke(); assistChannel = crypto.randomUUID().replaceAll('-', ''); assistToggle.textContent = 'Помощник в приложении: выключен'; assistToggle.setAttribute('aria-pressed', 'false'); };
  const assistToggle = button('Помощник в приложении: выключен', 'sparkle', 'sw-button-quiet', () => {
    if (assistOn) { stopAssist(); return; }
    const epoch = ++assistEpoch; assistToggle.disabled = true;
    void assist.probe().then(async available => {
      if (epoch !== assistEpoch || !current()) return;
      if (!available) { showMessage('Управление недоступно: автор приложения ещё не включил SDK помощника.'); return; }
      if (!await askAssist(`Разрешить помощнику видеть отмеченные автором элементы приложения «${app.name}» в этом окне? Каждое нажатие и ввод требуют отдельного подтверждения. Доступ завершится при смене приложения, аккаунта или окна.`)) return;
      if (epoch !== assistEpoch || !current()) return;
      assist.enable(); assistOn = true; assistToggle.textContent = 'Помощник в приложении: включён'; assistToggle.setAttribute('aria-pressed', 'true');
    }).catch(() => { stopAssist(); showMessage('Не удалось включить помощника. Откройте приложение заново.'); })
      .finally(() => { if (!disposed) assistToggle.disabled = false; });
  });
  assistToggle.setAttribute('aria-pressed', 'false'); assistToggle.hidden = options.presentation === 'widget'; moreBody.append(assistToggle);
  const assistWatch = view.setInterval(() => { if (assistOn && !assist.binding() || closeAssistDialog && !current()) stopAssist(); }, 250);
  header.append(back, title, field, report, discuss, expand, moreHost); workspace.append(runtime, panel); screen.append(header, message, workspace, feedbackHost, reviewsHost);
  host.replaceChildren(screen);
  syncShortToolbar();
  const launcher = createAppLauncher({ target: initial.target, accountId, instanceId: options.instanceId ?? 'main', shellUrl: view.location.href,
    isCurrent: expected => expected === accountId && current(), request: options.request,
    resolveEntry: parameters => options.api.request<{ entry: AppResolvedEntry }>('apps.entry.get', { ...parameters }),
    abandonScoped:parameters=>options.api.request('apps.scoped.close',{...parameters}),
  });
  const readCapturePeer=():ProjectCapturePeer|null=>{
    const binding=launcher.scopedCapture(),source=captureSourcePin(binding?.source),frame=runtime.querySelector('iframe');
    return current()&&!runtimePending&&source&&binding&&frame?.contentWindow&&selectedEntry&&frameGeneration>0
      ?{approved:true,window:frame.contentWindow,origin:selectedEntry.origin,sourceId:source.id,
        appId:app.appId,accountId,generation:frameGeneration,slot:binding.slot,title:app.name}:null;
  };
  const editorHost=createOtsovEditorHost({view,readPeer:()=>options.presentation==='widget'||!isRootSsoWorkspacePin(launcher.scopedCapture()?.source)?null:readCapturePeer(),
    async assertPeer(peer,signal){const binding=launcher.scopedCapture(),source=captureSourcePin(binding?.source);
      if(signal?.aborted||!current()||!binding||!source||peer.slot!==binding.slot)return false;
      const result=await options.api.request<unknown>('apps.scoped.context',{appId:app.appId,handle:binding.handle});
      const fresh=readCapturePeer();return !signal?.aborted&&!!fresh&&fresh.slot===peer.slot&&fresh.window===peer.window
        &&fresh.generation===peer.generation&&matchesScopedCaptureContext(result,{appId:app.appId,source});}
  });
  const detachInvitation = mountAppInvitationCarrier({ view, appId: app.appId, take: initial.takeInvitation, readPeer: readCapturePeer });
  let renewalFrame:HTMLIFrameElement|null=null;
  let lastRuntimeLaunchAt=0;
  const readRenewBinding=()=>{
    const binding=launcher.scopedCapture(),source=captureSourcePin(binding?.source);return binding&&source?{...binding,source}:null;
  };
  const renewal=createScopedSlotRenewal({isCurrent:current,readBinding:readRenewBinding,
    readContext:handle=>options.api.request('apps.scoped.context',{appId:app.appId,handle}),
    async request(args){if(!options.renew)throw new Error('scoped_renewal_unavailable');
      const reply=await options.renew({appId:app.appId,...args,expectedAccountId:accountId});return launcher.prepareRenewal(reply);},
    async bootstrap(reply:ScopedRenewReply){return new Promise<'ready'|'login_required'|'unknown'>(resolve=>{
      const hidden=el('iframe');hidden.hidden=true;hidden.setAttribute('aria-hidden','true');hidden.tabIndex=-1;
      hidden.sandbox.add('allow-scripts','allow-same-origin');renewalFrame=hidden;let finished=false;
      const finish=(value:'ready'|'login_required'|'unknown')=>{if(finished)return;finished=true;clearTimeout(timeout);view.removeEventListener('message',receive);
        controller.signal.removeEventListener('abort',cancel);hidden.remove();if(renewalFrame===hidden)renewalFrame=null;resolve(value);};
      const cancel=()=>finish('unknown');
      const receive=(event:MessageEvent)=>{if(event.source!==hidden.contentWindow||event.origin!==selectedEntry?.origin)return;
        const data=event.data;if(!data||typeof data!=='object'||data.schema!=='soty.app-slot-ready.v1'||typeof data.ready!=='boolean'
          ||Object.keys(data).some(key=>!['schema','requestId','ready','unknown'].includes(key))||data.ready&&data.requestId!==reply.requestId)return;
        finish(data.ready?'ready':data.unknown===true?'unknown':'login_required');};
      view.addEventListener('message',receive);controller.signal.addEventListener('abort',cancel,{once:true});
      const timeout=setTimeout(()=>finish('unknown'),12000);hidden.src=reply.url;screen.append(hidden);
    });},commit:(old,next)=>{if(!current()||!launcher.commitRenewal(old,next))return false;frameGeneration++;lastRuntimeLaunchAt=Date.now();return true;},
    onState:state=>{if(state==='unknown')showMessage('Продление не подтверждено. Повторите то же действие.','renewal');
      else if(state==='login_required')showMessage(isRootSsoWorkspacePin(readRenewBinding()?.source)
        ?'Обновление доступа пока не подтверждено. Данные сохранены.'
        :'Войдите в приложение заново кнопкой «Обновить приложение». Данные сохранены.','renewal');
      else if(state==='ready'&&messageOwner==='renewal')showMessage(runtimeDisplay?.snapshot().message??'','runtime');},
  });
  const detachRenewalLoads=attachScopedRenewalLoads({runtime,view,signal:controller.signal,
    current:()=>current()&&!!options.renew,profile:()=>launcher.runtimeProfile(),renewal,readBinding:readRenewBinding,
    onTickFailure:(error,binding)=>{frameGeneration++;detachRootWake.recoverExpired(error,binding);}});
  attachSourceFrameRefresh({view,signal:controller.signal,current:()=>current()&&!runtimePending&&!!options.renew,
    readPeer:()=>{const frame=runtime.querySelector('iframe');return launcher.runtimeProfile()==='soty.selected-human-embed.v2'
      &&readRenewBinding()&&frame?.contentWindow&&selectedEntry?{window:frame.contentWindow,origin:selectedEntry.origin}:null;},
    refresh:()=>renewal.refresh()});
  let releaseCaptureLease:(()=>void)|null=null;
  const projectCapture=mountProjectCaptureBridge({view,readPeer:readCapturePeer,
    onCaptureActive(active){if(active){releaseCaptureLease??=renewal.beginCaptureLease();}else{releaseCaptureLease?.();releaseCaptureLease=null;}},
    async preparePeer(signal){if(signal.aborted||!current()||runtimePending)return false;
      const ready=options.renew?await renewal.ensure(190000):true;return ready&&!signal.aborted&&current()&&!runtimePending;},
    async assertPeer(peer,signal){
      const binding=launcher.scopedCapture(),source=captureSourcePin(binding?.source);
      if(signal.aborted||!current()||!binding||!source||peer.slot!==binding.slot)return false;
      const result=await options.api.request<unknown>('apps.scoped.context',{appId:app.appId,handle:binding.handle});
      const fresh=readCapturePeer();return !signal.aborted&&!!fresh&&fresh.slot===peer.slot&&fresh.window===peer.window
        &&fresh.generation===peer.generation&&matchesScopedCaptureContext(result,{appId:app.appId,source});
    },capture:parameters=>pickProjectFeedbackMedia({...parameters,document:host.ownerDocument}),
  });
  const recoveryRetry = button(options.presentation === 'widget' ? 'Обновить окно' : 'Открыть приложение заново', 'refresh', 'sw-button-quiet sa-recovery-retry', () => { void launchRuntime(false, () => true, true); });
  recoveryRetry.hidden = options.presentation !== 'widget';
  const currentEntryRetry = button('Открыть актуальный вход', 'arrow', 'sw-button-primary sa-current-entry-retry', () => { void openCurrentEntry(); });
  currentEntryRetry.hidden = true;
  const runtimeActions = el('div', 'sa-runtime-actions'); runtimeActions.setAttribute('role', 'group');
  runtimeActions.setAttribute('aria-label', 'Восстановление окна приложения');
  runtimeActions.append(recoveryRetry, currentEntryRetry, externalRecovery, dismissRuntimeNotice); message.after(runtimeActions);
  const widgetRuntimeControls = options.presentation === 'widget' ? createWidgetRuntimeControls({
    view,screen,message,runtime,runtimeActions,current,
    capture:()=>{
      const binding=launcher.scopedCapture();return {epoch:displayEpoch,frame:runtime.querySelector('iframe'),
        scoped:!!binding,slot:binding?.slot,handle:binding?.handle,source:captureSourcePin(binding?.source)};
    },
    matchesCapture:value=>{
      if(!current()||value.epoch!==displayEpoch||value.frame!==runtime.querySelector('iframe'))return false;
      const fresh=launcher.scopedCapture();if(!value.scoped||!fresh)return value.scoped===!!fresh;
      const before=value.source,after=captureSourcePin(fresh.source);
      return !!before&&!!after&&value.slot===fresh.slot&&value.handle===fresh.handle
        &&before.id===after.id&&before.version===after.version&&before.digest===after.digest;
    },
    canAct:()=>current()&&!runtimePending,
    createDialog,iconButton,button,el,isFocusTarget:isDialogFocusTarget,
    retry:recoveryRetry,currentEntry:currentEntryRetry,external,dismiss:dismissRuntimeNotice,
    onModalChange:(modal,open)=>{if(!open||current())options.onModalChange?.(modal,open);},
  }):null;
  const mainRuntimeControls = options.presentation === 'widget' ? null : createWidgetRuntimeControls({
    presentation:'main',returnTarget:{isCurrent:current,resolve:()=>name},
    ...(options.minimalChrome ? {mainControlHost:moreBody} : {}),
    view,screen,message,runtime,runtimeActions,current,externalRecovery,
    capture:()=>{
      const binding=launcher.scopedCapture();return {epoch:displayEpoch,frame:runtime.querySelector('iframe'),
        scoped:!!binding,slot:binding?.slot,handle:binding?.handle,source:captureSourcePin(binding?.source)};
    },
    matchesCapture:value=>{
      if(!current()||value.epoch!==displayEpoch||value.frame!==runtime.querySelector('iframe'))return false;
      const fresh=launcher.scopedCapture();if(!value.scoped||!fresh)return value.scoped===!!fresh;
      const before=value.source,after=captureSourcePin(fresh.source);
      return !!before&&!!after&&value.slot===fresh.slot&&value.handle===fresh.handle
        &&before.id===after.id&&before.version===after.version&&before.digest===after.digest;
    },
    canAct:()=>current()&&!runtimePending,
    createDialog,iconButton,button,el,isFocusTarget:isDialogFocusTarget,
    retry:recoveryRetry,currentEntry:currentEntryRetry,external,dismiss:dismissRuntimeNotice,
    onModalChange:(modal,open)=>{if(!open||current())options.onModalChange?.(modal,open);},
  });
  async function openCurrentEntry(): Promise<void> {
    if (!current() || runtimePending || entryRecoveryPending || currentEntryRetry.hidden || !options.onOpenCurrentEntry) return;
    const epoch = entryRecoveryEpoch, live = () => current() && epoch === entryRecoveryEpoch;
    entryRecoveryPending = true; currentEntryRetry.disabled = true;
    try {
      await discussion?.flush(); await feedback?.flush();
      if (!live()) return;
      if (discussion?.hasUnsavedChanges() || feedback?.hasUnsavedChanges() || reviews?.hasUnsavedChanges()) {
        showMessage('Сохраните незавершённое действие перед переходом к актуальному входу.'); return;
      }
      const result = await options.api.request<{ entry: AppResolvedEntry }>('apps.entry.get', {
        appId: initial.target.appId, current: true, expectedAccountId: accountId,
      });
      if (!live()) return;
      const entry = validateAppEntry(result.entry, { appId: initial.target.appId }, view.location.href);
      // The owner of this stage creates a new exact route/stage. Its original
      // launcher, saved address and any existing iframe are never rebound here.
      await options.onOpenCurrentEntry(entry);
    } catch (reason) {
      if (live()) { const failure = appLaunchFailure(reason); showMessage(`${failure.title}. ${failure.detail}`); }
    } finally {
      if (live()) { entryRecoveryPending = false; currentEntryRetry.disabled = false; }
    }
  }
  const recovery = createAppBootRecovery({ view, appId: initial.target.appId, getFrame: () => runtime.querySelector('iframe'),
    isCurrent: current, canRecover: () => !runtimePending,
    recover: stillCurrent => launchRuntime(true, stillCurrent),
    onFailure: () => { if (current()) { showMessage('Не удалось восстановить вход. Откройте приложение заново.'); recoveryRetry.hidden = false; } },
  });
  const detachRootWake=attachRootSsoWakeRecovery({view,signal:controller.signal,
    current:()=>current()&&!runtimePending&&!projectCapture.isCapturing(),readBinding:readRenewBinding,
    lastLaunchAt:()=>lastRuntimeLaunchAt,recover:async()=>{await renewal.refresh();}});
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
    if (focus) { if (presentation) discussion?.focus(); else focusDiscussionControl(); }
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
    field.disabled = false;
    widget.disabled = false;
    const pinned = routeWith(intent.presentation); options.onNavigate(pinned, { replace: true }); intent = pinned;
    bookmark.hidden = false; savedHost.hidden = false;
    saved = mountAppSaved(savedHost, { api: options.api, accountId, entry, isCurrent: current, asBookmark: true,
      dialogReturnTarget: { isCurrent: current, resolve: () => moreBody.hidden ? more : savedHost.querySelector<HTMLButtonElement>('button') ?? more } });
    if (discussion) void discussion.updateEntry(entry).catch(() => showMessage('Не удалось обновить обсуждение. Откройте его ещё раз.'));
    // A local-only panel is mounted only once resolution has settled. It is
    // never upgraded using a later guess from optional catalogue metadata.
    ensureDiscussion(); showPanel();
    options.onRemember?.(pinned, app);
  }
  function resetRuntimeDisplay(): void {
    runtimeTrace.reset();
    displayEpoch++; releaseRuntimeDisplay?.(); releaseRuntimeDisplay = null;
    runtime.setAttribute('aria-busy', 'false');
  }
  function watchRuntimeDisplay(frame: HTMLIFrameElement): void {
    const epoch = displayEpoch, binding = launcher.scopedCapture(), source = captureSourcePin(binding?.source), entry = launcher.entry();
    const live = () => current() && epoch === displayEpoch && runtime.querySelector('iframe') === frame;
    const sameSlot = () => {
      const fresh = launcher.scopedCapture(), pin = captureSourcePin(fresh?.source);
      return !!binding && !!fresh && !!source && !!pin && fresh.slot === binding.slot && fresh.handle === binding.handle
        && pin.id === source.id && pin.version === source.version && pin.digest === source.digest;
    };
    const denied = new Set(['ACTIVE_PROFILE_CHANGED','ACCOUNT_MISMATCH','authentication_required','apps_access_denied','app_access_revoked',
      'app_access_changed','app_source_changed','app_policy_changed','scoped_embed_authority_changed','scoped_embed_source_changed']);
    const readiness = async (): Promise<'ready'|'pending'|'denied'|'unknown'|'unsupported'> => {
      if (!live()) return 'unknown';
      if (!binding || !source || !isScopedRuntimeProfile(launcher.runtimeProfile())) return 'unsupported';
      if (!sameSlot()) return 'unknown';
      try {
        // Existing authenticated Root read. It never renews, grants, replays a
        // ticket or accepts a frame's self-declared ready assertion.
        const value = await options.api.request<unknown>('apps.scoped.context', { appId: app.appId, handle: binding.handle });
        if (!live() || !sameSlot()) return 'unknown';
        if (!matchesScopedCaptureContext(value, { appId: app.appId, source })) return 'pending';
        const session = (value as { sourceSession?: { ready?: boolean; reason?: string; accessExpiresAt?: number; sessionExpiresAt?: number } }).sourceSession;
        if (session?.ready === false && session.reason === 'login_required') return 'denied';
        const now = Date.now();
        return session?.ready === true && Number.isSafeInteger(session.accessExpiresAt) && session.accessExpiresAt! > now
          && Number.isSafeInteger(session.sessionExpiresAt) && session.sessionExpiresAt! > now ? 'ready' : 'pending';
      } catch (error) {
        if (!live() || !sameSlot()) return 'unknown';
        const code = error && typeof error === 'object' && 'code' in error ? error.code : undefined;
        return typeof code === 'string' && denied.has(code) ? 'denied' : 'unknown';
      }
    };
    const loaded = () => { if (live()) { frameGeneration++; stopAssist(); } };
    frame.addEventListener('load', loaded, { signal: controller.signal });
    const display = createAppRuntimeDisplay({ view, frame, origin: entry!.origin, isCurrent: live, readiness, onChange: state => {
      if (!live()) return;
      runtime.setAttribute('aria-busy', String(state.busy));
      // Other account/renewal errors own their message and are never erased by
      // a late transport event. All copy here is finite and contains no URL.
      if (!message.textContent || messageOwner === 'runtime') {
        showMessage(state.message, 'runtime');
        recoveryRetry.hidden = options.presentation === 'widget' ? false : !state.canRetry;
        externalRecovery.hidden = !state.canExternal; dismissRuntimeNotice.hidden = !state.canDismiss;
      }
    } });
    runtimeDisplay = display;
    if (!message.textContent || messageOwner === 'runtime') showMessage(display.snapshot().message, 'runtime');
    runtime.setAttribute('aria-busy', 'true');
    releaseRuntimeDisplay = () => {
      display.dispose(); frame.removeEventListener('load', loaded);
      if (runtimeDisplay === display) runtimeDisplay = null;
      dismissRuntimeNotice.hidden = true;
    };
  }
  async function launchRuntime(automatic = false, recoveryCurrent: () => boolean = () => true, diagnosticManual = false): Promise<void> {
    const alive = () => current() && (!automatic || recoveryCurrent());
    if (!alive() || runtimePending) return;
    if(options.nativeVoice?.mount.appId===app.appId&&options.nativeVoice.mount.parentOrigin!==view.location.origin){
      runtimeStarted=true;runtime.replaceChildren(emptyState('Голосовое приложение недоступно на этом адресе',
        'Откройте его в основном адресе Сот. Профиль и сохранённые данные остаются на прежних адресах.',undefined,'app'));return;
    }
    resetRuntimeDisplay();
    if (diagnosticManual && !automatic && options.presentation !== 'widget') runtimeTrace.beginManual();
    entryRecoveryEpoch++; entryRecoveryPending = false; currentEntryRetry.disabled = false; currentEntryRetry.hidden = true;
    runtimeStarted = true; runtimePending = true; refresh.disabled = true; recoveryRetry.disabled = true;
    recoveryRetry.hidden = options.presentation !== 'widget'; externalRecovery.hidden = true; showMessage('');
    stopAssist();
    const existingFrame = runtime.querySelector('iframe');
    if (existingFrame) showMessage('Соединяем с приложением…', 'runtime');
    if (!existingFrame) { const loading = el('p', 'sa-loading', 'Соединяем с приложением'); loading.setAttribute('role', 'status'); runtime.replaceChildren(loading); }
    try {
      const url = await launcher.launch({ requireSameBinding: automatic });
      if (!url || !alive()) return;
      captureEntry();
      const frame = el('iframe', 'sa-frame'); frame.title = app.name;
      const voiceFrame=options.nativeVoice?.mount.appId===app.appId&&options.nativeVoice.mount.parentOrigin===view.location.origin
        &&new URL(url).origin===options.nativeVoice.mount.frameOrigin&&new URL(url).pathname==='/soty-voice';
      frame.setAttribute('sandbox', 'allow-scripts allow-forms allow-same-origin allow-downloads'+(isScopedRuntimeProfile(launcher.runtimeProfile())||voiceFrame?' allow-popups allow-popups-to-escape-sandbox':''));frame.referrerPolicy = 'no-referrer';
      runtimeTrace.attach(frame,launcher.entry()!.origin);
      frame.src = url;
      if(options.nativeVoice?.mount.appId===app.appId&&options.nativeVoice.mount.parentOrigin===view.location.origin&&new URL(url).origin===options.nativeVoice.mount.frameOrigin&&!isScopedRuntimeProfile(launcher.runtimeProfile()))frame.allow='microphone '+options.nativeVoice.mount.frameOrigin+'; autoplay '+options.nativeVoice.mount.frameOrigin;
      frameGeneration++;
      lastRuntimeLaunchAt=Date.now();
      if(!isScopedRuntimeProfile(launcher.runtimeProfile()))recovery.bind(frame,{origin:launcher.entry()!.origin,binding:launcher.binding()});
      watchRuntimeDisplay(frame);
      runtime.replaceChildren(frame);
      runtimeTrace.inserted();
    } catch (reason) {
      if (!alive()) return;
      runtimeTrace.launchFailed();
      runtime.setAttribute('aria-busy', 'false');
      captureEntry(); const failure = appLaunchFailure(reason);
      const sourceChanged = reason && typeof reason === 'object' && 'code' in reason
        && ['scoped_embed_source_changed', 'scoped_embed_authority_changed'].includes(String(reason.code));
      currentEntryRetry.hidden = !sourceChanged || !options.onOpenCurrentEntry;
      if (existingFrame) showMessage(`${failure.title}. ${failure.detail}`);
      else runtime.replaceChildren(emptyState(failure.title, failure.detail, options.presentation === 'widget' || !currentEntryRetry.hidden ? undefined : button('Попробовать снова', 'refresh', 'sw-button-primary', () => { void launchRuntime(false, () => true, true); }), 'app'));
      if (!currentEntryRetry.hidden) recoveryRetry.hidden = true;
      if (automatic) recoveryRetry.hidden = false;
    } finally {
      if (current()) { runtimePending = false; refresh.disabled = false; recoveryRetry.disabled = false; ensureDiscussion(); showPanel(); }
    }
  }
  function updateRoute(next: AppLaunchIntent): void {
    if (disposed || !matches(next)) return;
    closeDiagnostic?.(); runtimeTrace.clear();
    const wasVisible = !panel.hidden, focusedPanel = panel.contains(host.ownerDocument.activeElement);
    stopAssist();
    intent = next; presentationVersion++;
    // Closing is immediate and keeps the component/drafts mounted. Reopening
    // must select the route again: a previously shown archive is not the
    // implicit current conversation of a later Back/forward entry.
    if (!next.presentation) showPanel();
    else ensureDiscussion();
    void applyPresentation();
    if (wasVisible && !next.presentation && focusedPanel) focusDiscussionControl();
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
    const access = (record: WorldAppRecord): string => JSON.stringify({
      grants: { accountIds: [...(record.grants?.accountIds ?? [])].sort(), communityIds: [...(record.grants?.communityIds ?? [])].sort() },
      publication: record.publication ?? null,
    });
    const accessChanged = Boolean(app.grants || app.publication) && access(app) !== access(value);
    app = value; name.textContent = value.name; name.title = value.name;
    subtitle.textContent = value.deviceLabel ? `На устройстве «${value.deviceLabel}»` : 'Приложение в Сотах';
    const frame = runtime.querySelector('iframe'); if (frame) frame.title = value.name;
    settings.hidden = archives.hidden = value.ownerAccountId !== accountId;
    // The discussion already resolves and polls its own exact-entry authority.
    // Late initial metadata and title/device changes must not clear a loaded
    // conversation. Observed grant/policy changes still refresh immediately.
    if (accessChanged) { recovery.cancel(); entryRecoveryEpoch++; entryRecoveryPending = false; currentEntryRetry.disabled = false; }
    if (discussion && accessChanged) void discussion.refresh();
  }
  function updateCommunity(value: { communityId: string; name: string } | null): void {
    if (!current()) return;
    group = value && value.communityId === initial.communityId ? value : null; community.hidden = !group;
    if (group) { community.querySelector('span')!.textContent = group.name; community.setAttribute('aria-label', `Сообщество ${group.name}`); }
  }
  host.ownerDocument.addEventListener('pointerdown', event => {
    if (!moreBody.hidden && event.target instanceof view.Node && !moreHost.contains(event.target)) closeMore();
  }, { signal: controller.signal });
  host.ownerDocument.addEventListener('keydown', event => {
    if (event.key !== 'Escape' || event.defaultPrevented || event.isComposing || !current()) return;
    if (!moreBody.hidden) { event.preventDefault(); closeMore(true); return; }
    if (runtimeActions.contains(host.ownerDocument.activeElement) && runtimeDisplay?.dismiss()) { event.preventDefault(); name.focus(); return; }
    if (options.minimalChrome && !host.ownerDocument.querySelector('dialog[open]') && !screen.matches(':fullscreen') && !screen.classList.contains('is-window-expanded')
      && !(event.target instanceof view.Element && event.target.closest('input,textarea,select,[contenteditable]:not([contenteditable=false])'))) {
      event.preventDefault(); options.onBack();
    }
  }, { signal: controller.signal });
  host.ownerDocument.addEventListener('visibilitychange', showPanel, { signal: controller.signal });
  narrow.addEventListener('change', showPanel, { signal: controller.signal });
  shortToolbar.addEventListener('change', syncShortToolbar, { signal: controller.signal });
  view.addEventListener('resize', fitMore, { signal: controller.signal });
  view.visualViewport?.addEventListener('resize', fitMore, { signal: controller.signal });
  view.visualViewport?.addEventListener('scroll', fitMore, { signal: controller.signal });
  view.addEventListener('message', event => {
    if (isAppExternalRequest(event, { frameWindow: runtime.querySelector('iframe')?.contentWindow ?? null,
      origin: selectedEntry?.origin, current: current(), activated: view.navigator.userActivation?.isActive === true })) external.click();
  }, { signal: controller.signal });
  updateApp(app);
  // Start admission synchronously before the owner asks for optional metadata.
  const ready = initial.presentation?.administrative
    ? (ensureDiscussion(), showPanel(), Promise.resolve()) : launchRuntime();
  return { ready, matches, updateRoute, updateApp, updateCommunity, entry: () => current() ? selectedEntry : null,
    mountRuntimeControls:(header,owner)=>widgetRuntimeControls?.attach(header,owner)??false,
    browserAssist: () => current() && assistOn ? assist : null,
    openProjectEditor:(artifact,request)=>editorHost.open(artifact,request),
    assistantContext: () => {
      if (!current() || !selectedEntry) return null;
      const binding = launcher.scopedCapture();
      return { appId: app.appId, name: app.name, path: selectedEntry.path.split(/[?#]/u)[0]!, status: app.status,
        ...(binding ? { selection: { appId: app.appId, handle: binding.handle } } : {}) };
    },
    flush: async () => { await discussion?.flush(); await feedback?.flush(); }, hasUnsavedChanges: () => !!discussion?.hasUnsavedChanges() || !!feedback?.hasUnsavedChanges() || !!reviews?.hasUnsavedChanges(),
    dispose() { if (disposed) return; editorHost.close(); mainRuntimeControls?.dispose(); widgetRuntimeControls?.dispose(); closeDiagnostic?.(); runtimeTrace.dispose(); resetRuntimeDisplay(); stopAssist(); assist.close(); view.clearInterval(assistWatch); projectCapture.dispose();detachInvitation();detachRootWake();detachRenewalLoads();renewal.dispose();renewalFrame?.remove();feedback?.dispose(); reviews?.dispose(); disposed = true; recovery.dispose(); fullscreen.dispose(); detachVoiceBridge(); detachHiveBridge(); routeGeneration++; presentationVersion++; controller.abort(); launcher.dispose(); saved?.dispose(); discussion?.dispose(); screen.remove(); },
  };
}
