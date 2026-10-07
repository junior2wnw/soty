import './world.css';
import './product.css';
import './shell.css';
import './chat.css';
import './community.css';
import { mountAppStage, type AppStageHandle } from './app-stage';
import { createAppFieldPlacementController, type AppFieldPlacementController, type AppFieldPlacementSnapshot } from './app-field-placement-browser';
import { createFieldPersistence } from './field-persistence';
import { preferredInspectionEntry } from './app-deployment.mjs';
import { mountAppLibrary } from './app-library';
import './experience.css';
import { createHome, createAppCard, type AppCardData, type HomeFilter } from './home';
import { resolveAppArt } from './app-art.mjs';
import { createUnifiedFieldScreen, type FieldFilter, type UnifiedFieldScreen } from './unified-field-screen';
import type { UnifiedFieldViewState } from './unified-field';
import type { DirectoryApp, DirectoryContact, DirectoryPerson } from './field-directory';
import { createBrandMark } from './brand';
import { createOriginContinuityLinks } from '../platform/origin-continuity';
import { observeVisibleViewport } from './visible-viewport';
import { createChatDraftStore, readChatForward } from './chat-state.mjs';
import { canGroupMessages, chatDayKey, chatDayLabel, chatListTime, chatPreview, shouldSendOnEnter } from './messenger.mjs';
import { capabilities, createLibrary, loadDeskPreferences, openCommandPalette, saveDeskPreferences, type CapabilityId, type DeskPreferences } from './product';
import { createAppsHome, appStatusLabel, type AppHomeState } from './apps-home';
import { createApplicationCard } from './application-card';
import { describeAppAudience, publicationFromInspection } from './app-audience.mjs';
import { formatAppLaunchRoute, parseAppLaunchRoute, type AppLaunchIntent, type AppResolvedEntry, type AppLaunchBinding } from './app-launch.mjs';
import { mountAppSettings } from './app-settings';
import { createThemeController, createThemeControls, type ThemeController } from './theme/theme';
import { getPwaController, registerUpdateGuard, watchFormEdits, type PwaState } from '../platform/pwa';
import { AvatarHydrator, prepareAvatar } from './avatars';
import { avatar, badge, button, el, emptyState, heading, iconButton, labeledField, nounCount, textInput, timeLabel } from './dom';
import { createDialog, errorText, isDialogFocusTarget, pinDialogSubmit, switchControl, type DialogCloseContext, type DialogReturnTarget, type WorldDialog } from './dialogs';
import { communityEmblem, createHexField, createHexFieldState, type HexField } from './hex-field';
import { communityIcons, icon } from './icons';
import { loadPreferences, savePreferences, type WorldPreferences, type WorldView } from './preferences';
import { entityId, entityName, worldColor, worldColors, type WorldApi, type WorldAppOptions, type WorldAppRecord, type WorldAssistantHandle, type WorldCommunity, type WorldDevice, type WorldEntity, type WorldMember, type WorldMessage, type WorldProfile, type WorldSearch } from './types';

type GroupTab = 'about' | 'chat' | 'apps';
type CatalogKind = 'all' | 'people' | 'communities';
interface AppProjection { id: string; name: string; ownerAccountId: string; hostDeviceId?: string; deviceName?: string; state: string; grants?: { accountIds: string[]; communityIds: string[] }; publication?: WorldAppRecord['publication']; entry?: WorldAppRecord['entry']; port?: number; }
interface DeviceProjection { hostDeviceId: string; connectorId: string; name: string; online: boolean; claimed: boolean; }
interface HomeNote { noteId: string; title: string; preview: string; pinned: boolean; updatedAt: number; }
type HomeSection = 'devices' | 'apps' | 'communities' | 'notes';
interface NotesHandle { dispose(): void; focus(): void; flush(): Promise<void>; reconnect(): void; hasUnsavedChanges(): boolean; }

export interface WorldAppHandle { destroy(): void; refresh(): Promise<void>; }

export function mountWorldApp(root: HTMLElement, options: WorldAppOptions): WorldAppHandle {
  const app = new WorldApplication(root, options);
  void app.refresh();
  return { destroy: () => app.destroy(), refresh: () => app.refresh() };
}

class WorldApplication {
  private readonly root: HTMLElement;
  private readonly options: WorldAppOptions;
  private readonly api: WorldApi;
  private readonly preferences: WorldPreferences;
  private readonly main = el('main', 'sw-main');
  private readonly header = el('header', 'sw-topbar');
  private readonly sidebar = el('aside', 'sx-sidebar');
  private readonly mobileNav = el('nav', 'sw-mobile-nav');
  private readonly live = el('div', 'sw-sr-only');
  private readonly controller = new AbortController();
  private readonly avatars: AvatarHydrator;
  private readonly dialogs = new Set<WorldDialog>();
  private appSettingsDialog: WorldDialog | null = null;
  private appSettingsRouteClose: ((resume: () => void) => void) | null = null;
  private appPlacementDialog: WorldDialog | null = null;
  private appPlacementController: AppFieldPlacementController | null = null;
  private appPlacementRouteClose: ((resume?: () => void) => void) | null = null;
  private readonly theme: ThemeController;
  private readonly pwa = getPwaController();
  private readonly formEdits = watchFormEdits();
  private readonly pwaBanner = el('aside', 'sw-pwa-banner');
  private pwaBannerKey = '';
  private connection: PwaState['connection'] = 'checking';
  private readonly unsubscribePwa: () => void;
  private readonly unregisterUpdateGuard: () => void;
  private desk: DeskPreferences = { favorites: [], recent: [] };
  private deskAccount = '';
  private accountGeneration = 0;
  private homeHandle: ReturnType<typeof createAppsHome> | null = null;
  private readonly homeState: AppHomeState = { lens: 'all', communityId: null, presentation: 'cards', scroll: 0, fieldX: 0, fieldY: 0, slots: new Map(), focusId: null, pinned: new Set() };
  private assistantHandle: WorldAssistantHandle | null = null;
  private assistantMode: 'create' | 'chat' = 'create';
  private accessHandle: WorldAssistantHandle | null = null;
  private appStage: AppStageHandle | null = null;
  private appReturnView: 'mine' | 'world' = 'mine';
  private activeRoute = location.hash || '#mine';
  private homeNotes: HomeNote[] | null = null;
  private homeRequest = 0;
  private homeStatus: Record<HomeSection, 'loading' | 'ready' | 'error'> = { devices: 'loading', apps: 'loading', communities: 'loading', notes: 'loading' };
  private notesHandle: NotesHandle | null = null;
  private noteActionPending = false;
  private profile: WorldProfile | null = null;
  private communities: WorldCommunity[] = [];
  private results: WorldSearch = { people: [], communities: [], nextCursor: null, totals: { people: 0, communities: 0 } };
  private apps: WorldAppRecord[] = [];
  private devices: WorldDevice[] = [];
  private view: WorldView;
  private homeQuery = '';
  private homeFilter: HomeFilter = 'all';
  private query = '';
  private kind: CatalogKind = 'all';
  private discoveryPages: (string | null)[] = [null];
  private selected: WorldEntity | null = null;
  private group: WorldCommunity | null = null;
  private groupTab: GroupTab = 'about';
  private groupReturn: 'mine' | 'world' | 'messages' = 'mine';
  private field: HexField | null = null;
  private unifiedField: UnifiedFieldScreen | null = null;
  private fieldOutbox: ReturnType<typeof createFieldPersistence> | null = null;
  private unifiedFieldView: UnifiedFieldViewState = {};
  private unifiedFieldFilter: FieldFilter = 'all';
  private unifiedFieldFilters: Record<'mine' | 'world', FieldFilter> = { mine: 'all', world: 'all' };
  private discoveryApps = new Map<string, WorldAppRecord[]>();
  private fieldState = createHexFieldState();
  private homeFieldState = createHexFieldState();
  private discoveryScope = '';
  private discoveryStatus: 'loading' | 'ready' | 'error' = 'loading';
  private searchTimer: ReturnType<typeof setTimeout> | null = null;
  private chatTimer: ReturnType<typeof setInterval> | null = null;
  private chatCleanup: (() => void) | null = null;
  private selectedChat: string | undefined;
  private readonly chatDrafts = createChatDraftStore({ getItem: key => localStorage.getItem(key), setItem: (key, value) => localStorage.setItem(key, value), removeItem: key => localStorage.removeItem(key) });
  private toastTimer: ReturnType<typeof setTimeout> | null = null;
  private requestSequence = 0;
  private screenSequence = 0;
  private destroyed = false;
  private visibilityOpen = false;
  private routeLoaded = false;
  private readonly releaseViewport: () => void;

  constructor(root: HTMLElement, options: WorldAppOptions) {
    this.root = root; this.options = options; this.api = options.api;
    this.releaseViewport = observeVisibleViewport(root.ownerDocument);
    this.preferences = loadPreferences(); this.view = this.preferences.view;
    this.theme = createThemeController({ initial: this.preferences, onChange: next => { Object.assign(this.preferences, next); this.persist(); } });
    if (!location.hash) history.replaceState({ soty: true }, '', `#${this.view}`);
    root.classList.add('sw-app'); root.dataset.motion = this.preferences.motion ? 'on' : 'off'; root.dataset.compact = String(this.preferences.compact);
    this.live.setAttribute('role', 'status'); this.live.setAttribute('aria-live', 'polite'); this.mobileNav.setAttribute('aria-label', 'Главная навигация');
    this.main.id = 'soty-main'; this.main.tabIndex = -1;
    const skip = el('a', 'sx-skip', 'К содержимому'); skip.href = '#soty-main';
    skip.addEventListener('click', event => { event.preventDefault(); this.main.focus(); });
    root.replaceChildren(skip, this.sidebar, this.header, this.pwaBanner, this.main, this.mobileNav, this.live);
    this.pwaBanner.setAttribute('aria-live', 'polite'); this.pwaBanner.setAttribute('aria-label', 'Состояние приложения');
    this.unsubscribePwa = this.pwa.subscribe(state => {
      const recovered = state.connection === 'online' && this.connection !== 'online';
      this.connection = state.connection;
      this.renderPwaBanner(state);
      if (recovered) {
        this.notesHandle?.reconnect();
        this.unifiedField?.reconnect();
        this.syncFieldOutbox();
        if (!this.profile && this.deskAccount) void this.refresh(true);
      }
    });
    this.unregisterUpdateGuard = registerUpdateGuard(async () => { await this.flushScreen(); return !this.screenHasUnsavedChanges() && !this.chatDrafts.hasVolatile() && !this.formEdits.hasUnsavedChanges() && !document.querySelector('dialog[open] form'); });
    root.addEventListener('click', event => {
      const target = event.target instanceof Element ? event.target.closest<HTMLElement>('button, a[href]') : null;
      if (!target || target.closest('.sn-workspace,.sw-assistant-host,.sw-access-host,.sa-stage,.uf-screen,.uf-global-search') || !this.screenHasUnsavedChanges()) return;
      event.preventDefault(); event.stopImmediatePropagation();
      this.afterNoteSaved(() => { if (target.isConnected) target.click(); });
    }, { capture: true, signal: this.controller.signal });
    this.avatars = new AvatarHydrator(this.api, root);
    this.renderNavigation(); this.main.append(this.loading('Открываем Соты'));
    window.addEventListener('popstate', () => { void this.openRoute(); }, { signal: this.controller.signal });
    window.addEventListener('beforeunload', event => { if (this.chatDrafts.hasVolatile() || this.screenHasUnsavedChanges()) { event.preventDefault(); event.returnValue = ''; } }, { signal: this.controller.signal });
    window.addEventListener('storage', event => this.chatDrafts.storageChanged(event.key), { signal: this.controller.signal });
    root.addEventListener('soty:chat-updated', event => {
      const group=(event as CustomEvent<{community:WorldCommunity}>).detail?.community;
      if(!group)return;const known=this.communities.find(value=>value.communityId===group.communityId);if(known)known.unreadCount=group.unreadCount;this.updateNavUnread();
    },{signal:this.controller.signal});
    root.addEventListener('soty:inbox-updated',event=>{const groups=(event as CustomEvent<{communities:WorldCommunity[]}>).detail?.communities;if(Array.isArray(groups)){this.communities=groups;this.updateNavUnread();}},{signal:this.controller.signal});
    document.addEventListener('keydown', event => {
      if ((event.ctrlKey || event.metaKey) && (event.code === 'KeyK' || event.key.toLowerCase() === 'k') && !event.isComposing && !document.querySelector('dialog[open]')) { event.preventDefault(); if (this.unifiedField) this.unifiedField.focusSearch(); else this.openQuickActions(); return; }
      if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes((event.target as HTMLElement).tagName) && !document.querySelector('dialog[open]')) {
        const search = this.root.querySelector<HTMLInputElement>('.uf-global-search input,.sw-search input');
        if (search) { event.preventDefault(); search.focus(); }
      }
      if (event.key === 'Escape' && this.selected && !document.querySelector('dialog[open]')) this.closePreview();
    }, { signal: this.controller.signal });
  }

  destroy(): void {
    this.destroyed = true; this.requestSequence++; this.screenSequence++;
    this.controller.abort(); this.fieldOutbox?.dispose(); this.fieldOutbox = null; this.cleanScreen(); this.avatars.destroy(); this.theme.destroy(); this.unsubscribePwa(); this.unregisterUpdateGuard(); this.formEdits.destroy(); this.releaseViewport();
    if (this.searchTimer) clearTimeout(this.searchTimer);
    if (this.toastTimer) clearTimeout(this.toastTimer);
    for (const dialog of this.dialogs) dialog.close({ restoreFocus: false });
    this.dialogs.clear(); this.root.replaceChildren(); this.root.classList.remove('sw-app');
  }

  async refresh(preserveNote = false): Promise<void> {
    this.avatars.setContext(this.group?.membership?.state === 'active' ? this.group.communityId : undefined);
    const sequence = ++this.requestSequence, accountAtStart = this.deskAccount, screenAtStart = this.screenSequence;
    try {
      if (this.options.localAccount) {
        const local = await this.options.localAccount();
        if (this.destroyed || sequence !== this.requestSequence) return;
        if (!local.accountId) throw Object.assign(new Error('No local identity'), { code: 'authentication_required' });
        this.transitionAccount(local.accountId);
      }
      const [profile, mine] = await Promise.all([
        this.api.request<{ profile: WorldProfile }>('world.profile.get', {}),
        this.api.request<{ communities: WorldCommunity[] }>('world.community.list', {}),
      ]);
      if (this.destroyed || sequence !== this.requestSequence) return;
      if (this.options.localAccount) {
        const local = await this.options.localAccount();
        if (this.destroyed || sequence !== this.requestSequence) return;
        if (!local.accountId || local.accountId !== profile.profile.profileId) throw Object.assign(new Error('Identity changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
      }
      const sameAccount = !this.transitionAccount(profile.profile.profileId);
      this.profile = profile.profile; this.communities = mine.communities; this.homeStatus.communities = 'ready';
      this.syncFieldOutbox();
      this.renderNavigation();
      // A same-account shell refresh is not navigation. The owner window and
      // its running app keep their draft, selection and existing connection.
      if (sameAccount && (this.appSettingsDialog?.element.open || this.appPlacementDialog?.element.open)) { this.routeLoaded = true; return; }
      // Recover the authenticated shell after an offline launch without replacing the live editor.
      if (preserveNote && sameAccount && this.notesHandle) { this.routeLoaded = true; return; }
      if (!this.routeLoaded || /^#(?:app|launch)(?:\/|\?|$)/u.test(location.hash)) { this.routeLoaded = true; if (await this.openRoute()) return; }
      if (this.group) { await this.openGroup(this.group.communityId, this.groupTab); return; }
      this.renderCurrent();
      if (this.unifiedField) await this.unifiedField.refresh();
      else if (this.view === 'world') await this.search();
      else if (this.view === 'mine') await this.loadPersonal();
    } catch (error) {
      if (this.destroyed || sequence !== this.requestSequence) return;
      const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
      if (['NETWORK_ERROR', 'NETWORK_TIMEOUT'].includes(code) && this.options.localAccount) {
        const local = await this.options.localAccount().catch(() => null);
        if (this.destroyed || sequence !== this.requestSequence) return;
        if (local?.accountId) {
          const changed = this.transitionAccount(local.accountId);
          const sameScreen = !changed && this.deskAccount === accountAtStart && this.screenSequence === screenAtStart;
          if (sameScreen && (this.appSettingsDialog?.element.open || this.appPlacementDialog?.element.open)) return;
          if (sameScreen && this.appStage) { this.toast(errorText(error), true); return; }
          if (sameScreen && this.unifiedField) { this.unifiedField.reconnect(); return; }
          if (sameScreen && preserveNote && this.notesHandle) { this.toast(errorText(error), true); return; }
          if (!changed) this.cleanScreen();
          this.renderNavigation();
          if (location.hash.startsWith('#notes')) { const id = location.hash.split('/')[1]; this.openNotes(id && /^[A-Za-z0-9_-]{3,160}$/.test(id) ? id : undefined, false); return; }
          if (/^#(?:app|launch)(?:\/|\?|$)/u.test(location.hash)) {
            const accountId = this.deskAccount, screen = this.screenSequence, route = location.hash;
            const current = (): boolean => !this.destroyed && this.deskAccount === accountId && this.screenSequence === screen && location.hash === route;
            const detail = el('p', 'sw-muted'); detail.setAttribute('role', 'status');
            const otherAccount = button('Другой аккаунт', 'person', 'sw-button-quiet', () => {
              if (!current()) return; otherAccount.disabled = true;
              void (async () => {
                try { await this.options.openAccount('recovery'); if (current()) await this.refresh(); }
                catch { if (current()) detail.textContent = 'Не удалось открыть аккаунт. Попробуйте ещё раз.'; }
                finally { if (current()) otherAccount.disabled = false; }
              })();
            });
            const box = emptyState('Приложение ждёт подключения', 'Подключитесь к сети и попробуйте снова.', button('Повторить', 'refresh', 'sw-button-primary', () => { if (current()) void this.refresh(); }), 'app');
            box.append(otherAccount, detail); this.main.replaceChildren(box); return;
          }
          if (this.view === 'mine' || this.view === 'world') { this.renderCurrent(); return; }
          this.main.replaceChildren(emptyState('Можно продолжать записывать', 'Сервер пока недоступен. Черновики этого аккаунта сохранены на устройстве.', button('Открыть записки', 'list', 'sw-button-primary', () => this.openNotes())), button('Повторить подключение', 'refresh', 'sw-button-quiet', () => { void this.refresh(); })); return;
        }
      }
      // Missing, revoked or unverifiable identity cannot retain a private UI.
      // Local durable notes/commands are not deleted; a later valid login may reopen them.
      if (!this.transitionAccount(null)) this.cleanScreen();
      this.renderNavigation();
      const box = emptyState('Соты ждут вас', errorText(error), button('Повторить', 'refresh', 'sw-button-primary', () => { this.main.replaceChildren(this.loading('Подключаемся')); void this.refresh(); }));
      box.append(button('Мой аккаунт', 'person', 'sw-button-quiet', () => this.runHook(this.options.openAccount)));
      this.main.replaceChildren(box);
    }
  }

  /** One identity boundary for authenticated, offline and invalid-account paths. */
  private transitionAccount(accountId: string | null): boolean {
    const next = typeof accountId === 'string' && accountId.trim() ? accountId : '';
    if (next === this.deskAccount) return false;
    this.deskAccount = next; this.accountGeneration++; this.homeRequest++;
    this.fieldOutbox?.dispose(); this.fieldOutbox = null;
    this.cleanScreen(); for (const dialog of [...this.dialogs]) dialog.close({ restoreFocus: false });
    this.profile = null; this.communities = []; this.apps = []; this.devices = []; this.homeNotes = null;
    this.group = null; this.selected = null; this.selectedChat = undefined; this.groupReturn = 'mine'; this.groupTab = 'about';
    this.homeState.slots.clear(); this.homeState.scroll = 0; this.homeState.fieldX = 0; this.homeState.fieldY = 0;
    this.homeState.communityId = null; this.homeState.focusId = null; this.homeState.focusControl = null; this.homeState.lens = 'all';
    this.desk = next ? loadDeskPreferences(next) : { favorites: [], recent: [] };
    this.homeState.pinned = new Set(this.desk.pinnedApps ?? []);
    this.homeStatus = { devices: 'loading', apps: 'loading', communities: 'loading', notes: 'loading' };
    this.homeQuery = ''; this.homeFilter = 'all'; this.discoveryApps.clear();
    this.fieldState = createHexFieldState(); this.homeFieldState = createHexFieldState(); this.discoveryScope = ''; this.discoveryPages = [null]; this.discoveryStatus = 'loading'; this.query = ''; this.kind = 'all';
    this.unifiedFieldView = this.desk.lastSpace ? { mineContext: this.desk.lastSpace, mineFit: 'context' } : {};
    this.unifiedFieldFilter = 'all';
    this.unifiedFieldFilters = { mine: 'all', world: 'all' };
    this.results = { people: [], communities: [], nextCursor: null, totals: { people: 0, communities: 0 } };
    this.routeLoaded = false; this.noteActionPending = false; this.visibilityOpen = false;
    if (this.searchTimer) clearTimeout(this.searchTimer); this.searchTimer = null;
    if (this.toastTimer) clearTimeout(this.toastTimer); this.toastTimer = null; this.root.querySelector('.sw-toast')?.remove();
    this.main.replaceChildren(); this.avatars.setContext(); this.renderNavigation(); return true;
  }

  private accountTask(): () => boolean {
    const accountId = this.deskAccount, generation = this.accountGeneration;
    return () => !this.destroyed && Boolean(accountId) && accountId === this.deskAccount && generation === this.accountGeneration;
  }

  /** Drain only the existing durable journal. No placement or conflict choice
   * is generated here; navigation never strands an app-stage offline intent. */
  private syncFieldOutbox(): void {
    if (this.destroyed || !this.deskAccount || this.fieldOutbox || this.unifiedField || this.connection !== 'online') return;
    const current = this.accountTask();
    const outbox = createFieldPersistence({ api: this.api, accountId: this.deskAccount, isCurrent: current });
    this.fieldOutbox = outbox;
    void outbox.load().catch(() => { /* Durable pending intents remain for reconnect or the field. */ })
      .finally(() => { outbox.dispose(); if (this.fieldOutbox === outbox) this.fieldOutbox = null; });
  }

  private loading(label: string): HTMLElement { const node = el('div', 'sw-loading'); node.append(el('span', '', label)); return node; }
  private cleanScreen(): void { this.screenSequence++; this.appSettingsDialog?.close({ restoreFocus: false }); this.appSettingsDialog = null; this.appSettingsRouteClose = null; this.appPlacementDialog?.close({ restoreFocus: false }); this.appPlacementController?.dispose(); this.appPlacementDialog = null; this.appPlacementController = null; this.appPlacementRouteClose = null; this.appStage?.dispose(); this.appStage = null; this.live.textContent = ''; this.field?.destroy(); this.field = null; const hadField = !!this.unifiedField; this.unifiedField?.dispose(); this.unifiedField = null; delete this.root.dataset.field; this.homeHandle?.destroy(); this.homeHandle = null; this.notesHandle?.dispose(); this.notesHandle = null; this.assistantHandle?.dispose(); this.assistantHandle = null; this.accessHandle?.dispose(); this.accessHandle = null; this.chatCleanup?.(); this.chatCleanup = null; if (this.chatTimer) clearInterval(this.chatTimer); this.chatTimer = null; if (hadField && !this.destroyed) this.renderNavigation(); }
  private screenHasUnsavedChanges(): boolean { return !!(this.unifiedField?.hasUnsavedChanges() || this.appPlacementController?.hasUnsavedChanges() || this.notesHandle?.hasUnsavedChanges() || this.assistantHandle?.hasUnsavedChanges?.() || this.accessHandle?.hasUnsavedChanges?.() || this.appStage?.hasUnsavedChanges()); }
  private async flushScreen(): Promise<void> { await Promise.all([this.unifiedField?.flush(), this.appPlacementController?.retry(), this.notesHandle?.flush(), this.assistantHandle?.flush?.(), this.accessHandle?.flush?.(), this.appStage?.flush()]); }
  private persist(): void { savePreferences(this.preferences); }
  private announce(message: string): void { this.live.textContent = message; }
  private toast(message: string, isError = false): void {
    this.root.querySelector('.sw-toast')?.remove(); if (this.toastTimer) clearTimeout(this.toastTimer);
    const toast = el('div', `sw-toast${isError ? ' is-error' : ''}`, message); toast.setAttribute('role', isError ? 'alert' : 'status'); this.root.append(toast);
    this.toastTimer = setTimeout(() => toast.remove(), isError ? 6500 : 3800);
  }
  private runHook(hook: () => void | Promise<void>): void { void Promise.resolve().then(hook).catch(error => this.toast(errorText(error), true)); }
  private afterNoteSaved(action: () => void): void {
    if (this.destroyed || this.noteActionPending) return;
    if (this.appPlacementDialog?.element.open && this.appPlacementRouteClose) {
      this.appPlacementRouteClose(() => this.afterNoteSaved(action)); return;
    }
    const sequence = this.screenSequence;
    if (!this.screenHasUnsavedChanges()) { action(); return; }
    this.noteActionPending = true;
    void this.flushScreen().then(() => {
      this.noteActionPending = false;
      if (this.destroyed || sequence !== this.screenSequence) return;
      if (this.screenHasUnsavedChanges()) this.toast(this.notesHandle ? 'Сначала сохраните или скачайте черновик записки' : 'Сохраните или закройте незавершённое действие', true);
      else action();
    }, () => {
      this.noteActionPending = false;
      if (!this.destroyed && sequence === this.screenSequence) this.toast('Изменения ещё не сохранены. Повторите сохранение перед выходом.', true);
    });
  }
  private dialogReturnTarget(resolve?: () => HTMLElement | null): DialogReturnTarget {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const accountId = this.deskAccount, generation = this.accountGeneration, sequence = this.screenSequence;
    const route = location.hash, activeRoute = this.activeRoute;
    return { isCurrent: () => !this.destroyed && accountId === this.deskAccount && generation === this.accountGeneration &&
      sequence === this.screenSequence && route === location.hash && activeRoute === this.activeRoute,
      resolve: resolve ?? (() => isDialogFocusTarget(opener) ? opener : isDialogFocusTarget(this.main) ? this.main : null) };
  }

  private appDialogReturnTarget(appId: string): DialogReturnTarget {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    return this.dialogReturnTarget(() => {
      if (isDialogFocusTarget(opener)) return opener;
      const inspect = Array.from(this.main.querySelectorAll<HTMLElement>('[data-app-action="inspect"]')).find(node => node.dataset.appId === appId && isDialogFocusTarget(node));
      if (inspect) return inspect;
      const launch = Array.from(this.main.querySelectorAll<HTMLElement>('[data-entity-id]')).find(node => node.dataset.entityId === `app:${appId}` && isDialogFocusTarget(node));
      return launch ?? (isDialogFocusTarget(this.main) ? this.main : null);
    });
  }

  private dialog(title: string, onClose?: (context: DialogCloseContext) => void, returnTarget = this.dialogReturnTarget()): WorldDialog {
    const dialog = createDialog(title, context => { this.dialogs.delete(dialog); onClose?.(context); }, returnTarget);
    this.dialogs.add(dialog); this.avatars.observe(dialog.element); return dialog;
  }

  private renderNavigation(): void {
    const brandButton = el('button', 'sx-brand-button'); brandButton.type = 'button'; brandButton.setAttribute('aria-label', 'Соты — главная');
    brandButton.append(createBrandMark()); brandButton.addEventListener('click', () => this.navigate('mine'));
    const nav = el('nav', 'sx-rail-nav'); nav.setAttribute('aria-label', 'Главная навигация');
    const active = this.view === 'messages' ? 'messages' : this.view === 'assistant' ? 'assistant' : 'mine';
    const items = [['mine', 'Поле', 'grid'], ['messages', 'Чаты', 'chat'], ['assistant', 'Помощник', 'sparkle']] as const;
    this.mobileNav.replaceChildren();
    for (const [id, label, symbol] of items) {
      const make = (className: string) => { const node = button(label, symbol, className, () => this.navigate(id));node.dataset.navView=id; if (active === id) node.setAttribute('aria-current', 'page'); return node; };
      nav.append(make('sx-rail-button')); this.mobileNav.append(make('sw-nav-button'));
    }
    const footer = el('div', 'sx-rail-footer');
    const theme = iconButton(this.theme.get().resolvedTheme === 'dark' ? 'Включить светлую тему' : 'Включить тёмную тему', this.theme.get().resolvedTheme === 'dark' ? 'sun' : 'moon', () => { this.theme.set({ themeMode: this.theme.get().resolvedTheme === 'dark' ? 'light' : 'dark' }); this.renderNavigation(); });
    const profile = el('button', 'sx-profile-button'); profile.type = 'button'; profile.setAttribute('aria-label', 'Мой профиль и настройки');
    profile.append(avatar(this.profile?.displayName ?? 'Я', this.profile?.avatarUrl, worldColor(this.profile?.avatarColor), this.profile?.profileId, this.profile?.avatarRevision)); profile.addEventListener('click', () => this.openProfileMenu());
    footer.append(theme, profile, iconButton('Оформление', 'settings', () => this.openAppearance()));
    this.sidebar.replaceChildren(brandButton, nav, footer);
    const brand = el('button', 'sx-header-brand', 'СОТЫ'); brand.type = 'button'; brand.addEventListener('click', () => this.navigate('mine'));
    const context = el('span', 'sx-header-context', this.group?.name || ({ mine:'Моё поле', world:'Поиск', messages:'Чаты', assistant:'Помощник', notes:'Записки', library:'Возможности', access:'Доступы и действия' } as Record<WorldView,string>)[this.view]);
    const search = button('Приложения, люди, сообщества', 'search', 'sx-global-search', () => this.openQuickActions()); search.setAttribute('aria-keyshortcuts', 'Control+k Meta+k'); search.setAttribute('aria-label', 'Поиск приложений, людей и сообществ'); search.append(el('kbd', '', 'Ctrl K'));
    const add = button('Добавить', 'plus', 'sw-button-primary sx-global-add', () => this.openAddMenu());add.setAttribute('aria-label','Добавить');
    const mobileProfile=el('button','sx-profile-button sx-mobile-profile');mobileProfile.type='button';mobileProfile.setAttribute('aria-label','Мой профиль и настройки');mobileProfile.append(avatar(this.profile?.displayName??'Я',this.profile?.avatarUrl,worldColor(this.profile?.avatarColor),this.profile?.profileId,this.profile?.avatarRevision));mobileProfile.addEventListener('click',()=>this.openProfileMenu());
    const fieldSearch = this.unifiedField ? this.header.querySelector<HTMLElement>('.uf-global-search') : null;
    if (fieldSearch) {
      // Keep the live search input connected: shell metadata/theme refresh must
      // not interrupt focus, selection or an in-progress IME composition.
      for (const child of [...this.header.children]) if (child !== fieldSearch) child.remove();
      this.header.prepend(brand, context); this.header.append(mobileProfile, add);
    } else this.header.replaceChildren(brand, context, search, mobileProfile, add);
    this.updateNavUnread();
    this.unifiedField?.attachHeaderSearch(this.header);
  }

  private updateNavUnread(): void {
    const unread=this.communities.filter(group=>group.membership?.state==='active'&&!group.membership.muted).reduce((sum,group)=>sum+Math.max(0,group.unreadCount),0);
    for(const nav of this.root.querySelectorAll<HTMLElement>('[data-nav-view="messages"]')){let badge=nav.querySelector<HTMLElement>('.sx-nav-unread');if(!unread){badge?.remove();continue;}if(!badge){badge=el('span','sx-nav-unread');badge.setAttribute('aria-hidden','true');nav.append(badge);}badge.textContent=unread>99?'99+':String(unread);}
  }

  private openProfileMenu(): void {
    const menu=this.dialog('Мои настройки');const options=el('div','sx-profile-menu');
    const entry=(label:string,symbol:string,action:()=>void)=>button(label,symbol,'sw-button-wide',()=>{menu.close();action();});
    options.append(entry('Профиль','person',()=>this.openProfileEditor()),entry('Контакты','people',()=>this.runHook(()=>this.options.openAccount('people'))),entry('Устройства','laptop',()=>this.openResources('devices')),entry('Сохранённые приложения','folder',()=>this.openSavedLibrary()),entry('Доступы и действия','shield',()=>this.navigate('access')),entry('Видимость','eye',()=>this.openVisibility()),entry('Аккаунт и восстановление','lock',()=>this.runHook(()=>this.options.openAccount('recovery'))),entry('Оформление','sun',()=>this.openAppearance()),entry('Все возможности','grid',()=>this.navigate('library')));
    const continuity = createOriginContinuityLinks(); if (continuity) options.append(continuity);
    const docs = el('a', 'sw-button sw-button-quiet'); docs.href = '/agents'; docs.target = '_blank'; docs.rel = 'noopener'; docs.setAttribute('aria-label', 'Для разработчиков и ИИ (в новой вкладке)'); docs.append(icon('connections'), el('span', '', 'Для разработчиков и ИИ'), icon('external')); options.append(docs); menu.body.append(options);
  }

  private openSavedLibrary(): void {
    const accountId = this.deskAccount, accountCurrent = this.accountTask();
    if (!accountId || !accountCurrent()) return;
    let library: ReturnType<typeof mountAppLibrary> | null = null;
    const dialog = this.dialog('Сохранённые приложения', () => library?.dispose());
    const host = el('div', 'sx-saved-dialog'); dialog.body.append(host);
    const open = (entry: AppResolvedEntry, discussion = false): void => {
      if (!accountCurrent() || !dialog.element.open) return;
      dialog.close({ restoreFocus: false });
      this.afterNoteSaved(() => {
        if (!accountCurrent()) return;
        this.writeRoute(formatAppLaunchRoute({ appId: entry.appId, domainId: entry.domainId, path: entry.path }, undefined, discussion ? { panel: 'discussion' } : undefined));
        void this.openRoute();
      });
    };
    library = mountAppLibrary(host, { api: this.api, accountId, isCurrent: () => accountCurrent() && dialog.element.open, openEntry: entry => open(entry), discussEntry: entry => open(entry, true) });
  }

  private openAppCardActions(app: WorldAppRecord): void {
    const accountCurrent = this.accountTask();
    const dialog = this.dialog(app.name, undefined, this.appDialogReturnTarget(app.appId));
    const actions = el('div', 'sx-profile-menu');
    const action = (label: string, symbol: string, run: () => void): HTMLElement => button(label, symbol, 'sw-button-wide', () => {
      if (!accountCurrent()) return; dialog.close({ restoreFocus: false }); run();
    });
    actions.append(action('О приложении', 'info', () => this.inspectApplication(app)), action(this.homeState.pinned.has(app.appId) ? 'Открепить' : 'Закрепить', 'pin', () => {
      this.homeState.pinned.has(app.appId) ? this.homeState.pinned.delete(app.appId) : this.homeState.pinned.add(app.appId);
      this.desk.pinnedApps = [...this.homeState.pinned].slice(0, 200); this.saveDesk();
      if (this.view === 'mine' && !this.group && !this.appStage) this.renderPersonal();
      const target = this.appDialogReturnTarget(app.appId).resolve(); if (isDialogFocusTarget(target)) target.focus({ preventScroll: true });
    }));
    if (app.ownerAccountId === this.profile?.profileId) actions.append(action('Название и доступ', 'settings', () => this.openAppSettings(app)));
    dialog.body.append(actions);
  }

  private navigate(view: WorldView, chatId?: string): void {
    this.afterNoteSaved(() => this.navigateReady(view, chatId));
  }

  private findCommunities(): void {
    this.afterNoteSaved(() => {
      this.query = ''; this.kind = 'communities'; this.unifiedFieldFilters.world = 'community';
      this.navigateReady('world');
    });
  }

  private searchEverything(query: string): void {
    this.afterNoteSaved(() => {
      this.query = query; this.kind = 'all'; this.unifiedFieldFilters.world = 'all'; this.navigateReady('world');
    });
  }

  private navigateReady(view: WorldView, chatId?: string): void {
    if (view === 'mine' || view === 'world') this.unifiedFieldFilter = this.unifiedFieldFilters[view];
    this.selectedChat = view === 'messages' ? chatId : undefined;
    if (view === 'world') this.discoveryStatus = 'loading';
    this.writeRoute(view === 'messages' && chatId ? `messages/${chatId}` : view === 'mine' || view === 'world' ? this.unifiedFieldRoute(view) : view === 'assistant' ? `assistant/${this.assistantMode}` : view);
    this.view = view; this.preferences.view = view; this.persist(); this.group = null; this.selected = null;
    this.renderNavigation(); this.renderCurrent();
    if (!this.unifiedField && view === 'world') void this.search();
    if (!this.unifiedField && view === 'mine') void this.loadPersonal();
    if (view === 'messages') {
      const sequence = this.screenSequence;
      void this.api.request<{ communities: WorldCommunity[] }>('world.community.list', {}).then(result => { if (!this.destroyed && this.screenSequence === sequence && this.view === 'messages') { this.communities = result.communities; if (!this.main.querySelector('.sw-chat')) this.renderMessages(this.selectedChat); } }).catch(error => this.toast(errorText(error), true));
    }
  }

  private writeRoute(route: string): void {
    const next = `#${route}`; if (location.hash !== next) history.pushState({ soty: true }, '', next); this.activeRoute = next;
  }

  private discoveryRoute(): string {
    const parameters = new URLSearchParams();
    if (this.query.trim()) parameters.set('q', this.query.trim());
    if (this.kind !== 'all') parameters.set('kind', this.kind);
    const query = parameters.toString(); return `world${query ? `?${query}` : ''}`;
  }

  private unifiedFieldRoute(view: 'mine' | 'world'): string {
    const params = new URLSearchParams(), query = view === 'world' ? this.query : this.homeQuery;
    if (query.trim()) params.set('q', query.trim());
    if (this.unifiedFieldFilter !== 'all') params.set('kind', this.unifiedFieldFilter === 'person' ? 'people' : this.unifiedFieldFilter === 'community' ? 'communities' : this.unifiedFieldFilter);
    return `${view}${params.size ? '?' + params.toString() : ''}`;
  }

  private async openRoute(): Promise<boolean> {
    if (this.destroyed || (!this.profile && !this.deskAccount)) return false;
    if (this.appPlacementDialog?.element.open && this.appPlacementRouteClose) {
      if (location.hash !== this.activeRoute) {
        const accountCurrent = this.accountTask(), sequence = this.screenSequence;
        history.pushState({ soty: true }, '', this.activeRoute);
        this.appPlacementRouteClose(() => { if (accountCurrent() && this.screenSequence === sequence) history.back(); });
      }
      return true;
    }
    if (this.appSettingsDialog?.element.open && this.appSettingsRouteClose) {
      if (location.hash !== this.activeRoute) {
        const accountCurrent = this.accountTask(), sequence = this.screenSequence;
        // Keep both the requested history entry and the current form. A
        // confirmed close goes back to that entry; cancelling keeps the form.
        history.pushState({ soty: true }, '', this.activeRoute);
        this.appSettingsRouteClose(() => { if (accountCurrent() && this.screenSequence === sequence) history.back(); });
      }
      return true;
    }
    if (this.screenHasUnsavedChanges()) {
      const accountId = this.deskAccount, sequence = this.screenSequence;
      await this.flushScreen().catch(() => {});
      if (this.destroyed || sequence !== this.screenSequence || accountId !== this.deskAccount) return true;
      if (this.screenHasUnsavedChanges()) { history.replaceState({ soty: true }, '', this.activeRoute); this.toast('Сохраните или закройте незавершённое действие', true); return true; }
    }
    const fragment = location.hash.slice(1), queryAt = fragment.indexOf('?');
    const [route, id, tab] = (queryAt < 0 ? fragment : fragment.slice(0, queryAt)).split('/');
    const parameters = new URLSearchParams(queryAt < 0 ? '' : fragment.slice(queryAt + 1));
    if (route === 'community' && id && /^[A-Za-z0-9_-]{3,160}$/.test(id)) { await this.openGroup(id, ['about', 'chat', 'apps'].includes(tab ?? '') ? tab as GroupTab : 'about'); return true; }
    if (route === 'app' || route === 'launch') {
      let intent: AppLaunchIntent;
      try { const parsed = parseAppLaunchRoute(location.hash); if (!parsed) throw new Error('Invalid application link'); intent = parsed; }
      catch {
        this.cleanScreen(); this.activeRoute = location.hash;
        this.main.replaceChildren(emptyState('Ссылка на приложение повреждена', 'Попросите отправить ссылку ещё раз.', button('На главную', 'back', 'sw-button-quiet', () => this.navigate('mine')), 'link'));
        return true;
      }
      // Canonicalize only the shell route, never a server-issued ticket URL.
      history.replaceState({ soty: true }, '', `#${intent.route}`);
      if (this.appStage?.matches(intent)) { this.activeRoute = '#' + intent.route; this.appStage.updateRoute(intent); return true; }
      const known = this.apps.find(value => value.appId === intent.target.appId);
      const app = known ?? { appId: intent.target.appId, name: 'Приложение', status: 'unknown' };
      await this.openApplication(app, intent, !known); return true;
    }
    if (route === 'notes') { this.openNotes(id && /^[A-Za-z0-9_-]{3,160}$/.test(id) ? id : undefined, false); return true; }
    if (route === 'messages') { this.navigate('messages', id && /^[A-Za-z0-9_-]{3,160}$/.test(id) ? id : undefined); return true; }
    if (route === 'apps') { this.navigate('mine'); return true; }
    if (route === 'world') {
      this.query = (parameters.get('q') || '').slice(0, 100); const kind = parameters.get('kind');
      this.unifiedFieldFilter = kind === 'people' ? 'person' : kind === 'communities' ? 'community' : kind === 'app' ? 'app' : 'all';
      this.unifiedFieldFilters.world = this.unifiedFieldFilter;
      this.kind = kind === 'people' || kind === 'communities' ? kind : 'all'; this.navigate('world'); return true;
    }
    if (route === 'assistant') {
      // Existing assistant links continue to open the general task workspace.
      this.assistantMode = id === 'create' || !this.options.openAssistant ? 'create' : 'chat';
      this.navigate('assistant'); return true;
    }
    if (route === 'mine') { this.homeQuery = (parameters.get('q') || '').slice(0, 100); const kind = parameters.get('kind'); this.unifiedFieldFilter = kind === 'people' ? 'person' : kind === 'communities' ? 'community' : kind === 'app' || kind === 'device' ? kind : 'all'; this.unifiedFieldFilters.mine = this.unifiedFieldFilter; this.navigate('mine'); return true; }
    if (route === 'library' || route === 'access') { this.navigate(route); return true; }
    if (route) return false;
    return false;
  }

  private renderCurrent(): void {
    if ((this.view === 'mine' || this.view === 'world') && !this.group) {
      if (!this.unifiedField) { this.cleanScreen(); this.renderUnifiedField(); }
      else this.unifiedField.setMode(this.view === 'world' ? 'search' : 'mine', this.view === 'world' ? this.query : this.homeQuery, this.unifiedFieldFilter);
      this.avatars.setContext(); return;
    }
    this.cleanScreen();
    this.avatars.setContext();
    if (this.view === 'world') this.renderDiscovery();
    else if (this.view === 'mine') this.renderPersonal();
    else if (this.view === 'messages') this.renderMessages(this.selectedChat);
    else if (this.view === 'notes') this.renderNotes();
    else if (this.view === 'assistant') this.renderAssistant();
    else if (this.view === 'access') this.renderAccess();
    else this.renderLibrary();
  }

  private renderUnifiedField(): void {
    if (!this.deskAccount || this.destroyed) return;
    const isCurrent = this.accountTask();
    this.root.dataset.field = 'unified';
    const field = createUnifiedFieldScreen({ api: this.api, accountId: this.deskAccount, isCurrent,
      mode: this.view === 'world' ? 'search' : 'mine', query: this.view === 'world' ? this.query : this.homeQuery,
      filter: this.unifiedFieldFilter,
      filtersByMode: { mine: this.unifiedFieldFilters.mine, search: this.unifiedFieldFilters.world },
      viewState: this.unifiedFieldView, ...(this.desk.pinnedApps ? { pinnedApps: this.desk.pinnedApps } : {}),
      ...(this.options.fieldArt ? { resolveArt: this.options.fieldArt } : {}),
      onMessage: (message, error) => { if (isCurrent()) this.toast(message, error); },
      onContextChange: contextId => { if (isCurrent()) { this.desk.lastSpace = contextId; this.saveDesk(true); } },
      onAppSettings: app => { if (isCurrent()) this.openAppSettings({ appId: app.id, name: app.name, status: app.state, ownerAccountId: app.ownerAccountId, entry: app.entry }); },
      onRoute: (mode, query, filter) => {
        if (!isCurrent()) return; this.view = mode === 'search' ? 'world' : 'mine'; this.preferences.view = this.view;
        if (mode === 'search') this.query = query; else this.homeQuery = query; this.unifiedFieldFilter = filter;
        this.unifiedFieldFilters[this.view as 'mine' | 'world'] = filter;
        this.kind = filter === 'person' ? 'people' : filter === 'community' ? 'communities' : 'all'; this.persist();
        const params = new URLSearchParams(); if (query.trim()) params.set('q', query.trim()); if (filter !== 'all') params.set('kind', filter === 'person' ? 'people' : filter === 'community' ? 'communities' : filter);
        const next = `#${this.view}${params.size ? '?' + params.toString() : ''}`;
        // Typing replaces its own route; choosing the other side creates one history step.
        if (location.hash.split('?')[0] !== next.split('?')[0]) history.pushState({ soty: true }, '', next);
        else history.replaceState({ soty: true }, '', next);
        this.activeRoute = next;
        const context = this.header.querySelector('.sx-header-context'); if (context) context.textContent = mode === 'mine' ? 'Моё поле' : 'Поиск';
      },
      onOpen: (item, record) => this.afterNoteSaved(() => {
        if (!isCurrent()) return;
        if (item.entity.kind === 'builtin') { if (item.entity.id === 'notes') this.openNotes(); else if (item.entity.id === 'chess') this.runHook(() => this.options.openLegacy('chess')); return; }
        if (item.entity.kind === 'app') {
          const app = record && 'entry' in record && 'id' in record ? record as DirectoryApp : null;
          void this.openApplication({ appId: item.entity.id, name: item.title, status: app?.state ?? 'unknown', ...(app ? { entry: app.entry, ownerAccountId: app.ownerAccountId } : {}) }, undefined, !app); return;
        }
        if (item.entity.kind === 'community') { const group = record && 'communityId' in record ? record as WorldCommunity : null; void this.openGroup(item.entity.id, group?.membership?.state === 'active' ? 'chat' : 'about'); return; }
        if (item.entity.kind === 'person' && record && 'kind' in record && record.kind === 'contact') { this.runHook(() => this.options.openAccount('people')); return; }
        if (item.entity.kind === 'person' && record && 'profileId' in record) { const person = record as DirectoryPerson; this.openPersonDialog({ ...person, bio: person.bio ?? '', interests: person.interests ?? [] }); return; }
        if (item.entity.kind === 'device') this.runHook(() => this.options.openAccount('devices'));
      }),
      onCreate: kind => { if (!isCurrent()) return; if (kind === 'app') this.openAddApp(); else if (kind === 'community') this.openCommunityForm(); else if (kind === 'device') this.runHook(this.options.connectDevice); else if (kind === 'person') this.runHook(() => this.options.openAccount('people')); else this.navigate('assistant'); },
    });
    this.unifiedField = field; this.main.replaceChildren(field.element); field.attachHeaderSearch(this.header);
  }

  private renderAssistant(): void {
    const section = el('section', 'sx-assistant');
    const header = el('header', 'sx-assistant-heading');
    const copy = el('div'); copy.append(el('h1', '', 'Помощник'), el('p', 'sx-subtitle', this.assistantMode === 'create' ? 'Из идеи — в полезное приложение' : 'Задачи на ваших устройствах'));
    const tabs = el('div', 'sx-assistant-modes'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', 'Режим помощника');
    const choose = (mode: 'create' | 'chat', keyboard = false): void => this.afterNoteSaved(() => {
      this.assistantMode = mode; this.navigateReady('assistant');
      if (keyboard) this.main.querySelector<HTMLButtonElement>('[role=tab][aria-selected=true]')?.focus({ preventScroll: true });
    });
    for (const [mode, label] of [['create', 'Приложение'], ['chat', 'Задачи']] as const) {
      const tab = button(label, mode === 'create' ? 'grid' : 'sparkle', '', () => choose(mode, document.activeElement === tab));
      tab.setAttribute('role', 'tab'); tab.setAttribute('aria-selected', String(mode === this.assistantMode));
      tab.tabIndex = mode === this.assistantMode ? 0 : -1;
      tab.addEventListener('keydown', event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault(); choose(event.key === 'Home' ? 'create' : event.key === 'End' ? 'chat' : mode === 'create' ? 'chat' : 'create', true);
      });
      tabs.append(tab);
    }
    const host = el('div', 'sx-assistant-content sw-assistant-host'); host.dataset.pwaIgnore = '';
    header.append(copy, tabs); section.append(header, host); this.main.replaceChildren(section);
    const sequence = this.screenSequence;
    const mount = this.assistantMode === 'create' ? this.options.openAppBuilder : this.options.openAssistant;
    if (!mount) {
      const panel = el('div', 'sx-assistant-fallback'); panel.append(heading('Помощник', 'Ваши задачи и устройства'),
        button('Создать приложение', 'sparkle', 'sw-button-primary', () => this.runHook(() => this.options.agentCreate())),
        button('Мои устройства', 'laptop', 'sw-button-quiet', () => this.openResources('devices'))); host.append(panel); return;
    }
    host.append(this.loading('Открываем помощника'));
    void Promise.resolve().then(() => mount(host)).then(handle => {
      if (this.destroyed || sequence !== this.screenSequence || !host.isConnected) { handle.dispose(); return; }
      this.assistantHandle = handle;
    }).catch(error => { if (host.isConnected && sequence === this.screenSequence) host.replaceChildren(emptyState('Помощник пока недоступен', errorText(error), button('Повторить', 'refresh', 'sw-button-primary', () => this.navigate('assistant')))); });
  }

  private renderAccess(): void {
    const host = el('section', 'sw-access-host sx-page-host'); host.dataset.pwaIgnore = ''; this.main.replaceChildren(host);
    const accountId = this.profile?.profileId || this.deskAccount, sequence = this.screenSequence;
    if (!accountId) { host.append(emptyState('Нужен ваш аккаунт', 'Подключитесь, чтобы управлять доступом.')); return; }
    host.append(this.loading('Открываем доступы и действия'));
    void import('./access-panel').then(({ mountAccessPanel }) => {
      if (this.destroyed || sequence !== this.screenSequence || !host.isConnected) return;
      this.accessHandle = mountAccessPanel(host, { api: this.api, accountId, ...(this.profile?.displayName ? { accountLabel: this.profile.displayName } : {}), ...(this.options.accessAvailability ? { availability: this.options.accessAvailability } : {}) });
    }).catch(error => { if (host.isConnected) host.replaceChildren(emptyState('Не удалось открыть доступы', errorText(error), button('Повторить', 'refresh', 'sw-button-primary', () => this.navigate('access')))); });
  }

  private renderDiscovery(): void {
    if (this.deskAccount && !this.group) { this.renderCurrent(); return; }
    const workspace = el('div', 'sw-workspace'); const discovery = el('section', 'sw-discovery');
    const mobileHeading = el('div', 'sw-mobile-heading sx-world-heading'); mobileHeading.append(heading('Общий мир', 'Приложения, люди, сообщества'));
    const presentation=el('div','sx-presentation sx-world-presentation');presentation.setAttribute('role','group');presentation.setAttribute('aria-label','Вид общего мира');
    for(const [value,label,symbol] of [['list','Карточки','grid'],['field','Поле','cells']] as const){const choice=button(label,symbol,'',()=>{this.preferences.presentation=value;this.persist();presentation.querySelectorAll('button').forEach(node=>node.setAttribute('aria-pressed',String(node===choice)));this.renderSearchResults();});choice.setAttribute('aria-label',label);choice.setAttribute('aria-pressed',String(value===this.preferences.presentation));presentation.append(choice);}mobileHeading.append(presentation);
    const searchbar = el('div', 'sw-searchbar'); const search = el('label', 'sw-search');
    const input = textInput(this.query, 'Люди и сообщества', 100); input.type = 'search'; input.setAttribute('aria-label', 'Поиск людей и сообществ');
    const clear = iconButton('Очистить поиск', 'close', () => { input.value = ''; this.query = ''; input.focus(); void this.search(); }); clear.hidden = !this.query;
    input.addEventListener('input', () => {
      this.query = input.value; clear.hidden = !this.query;
      if (this.searchTimer) clearTimeout(this.searchTimer);
      this.searchTimer = setTimeout(() => { void this.search(); }, 230);
    });
    search.append(icon('search'), input, clear);
    const filters = el('div', 'sw-filters'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Искать среди');
    for (const [kind, label] of [['all', 'Все'], ['people', 'Люди'], ['communities', 'Сообщества']] as const) {
      const filter = button(label, undefined, this.kind === kind ? 'is-selected' : '', () => {
        this.kind = kind; filters.querySelectorAll('button').forEach(node => { node.classList.toggle('is-selected', node === filter); node.setAttribute('aria-pressed', String(node === filter)); }); void this.search();
      }); filter.setAttribute('aria-pressed', String(this.kind === kind)); filters.append(filter);
    }
    const create = iconButton('Создать сообщество', 'plus', () => this.openCommunityForm()); filters.append(create);
    searchbar.append(search, filters);
    const note = el('div', 'sw-search-result-note'); note.dataset.searchNote = '';
    const stage = el('div', 'sw-world-stage'); stage.dataset.worldStage = '';
    discovery.append(mobileHeading, searchbar, note, stage); workspace.append(discovery); this.main.replaceChildren(workspace);
    this.renderSearchResults();
  }

  private async search(next = false, previous = false): Promise<void> {
    const sequence = ++this.requestSequence;
    this.discoveryStatus = 'loading';
    const cursor = previous ? this.discoveryPages.at(-2) : next ? this.results.nextCursor : null;
    const scope = `${this.profile?.profileId || this.deskAccount}:${this.query.trim()}:${cursor ?? ''}`;
    if (this.view === 'world' && !this.group) { this.activeRoute = `#${this.discoveryRoute()}`; history.replaceState({ soty: true }, '', this.activeRoute); }
    const note = this.main.querySelector<HTMLElement>('[data-search-note]'); if (note) note.textContent = 'Ищем…';
    const stage = this.main.querySelector<HTMLElement>('[data-world-stage]'); stage?.setAttribute('aria-busy', 'true');
    if (!this.results.people.length && !this.results.communities.length) this.renderSearchResults();
    try {
      const response = await this.api.request<WorldSearch>('world.discovery.search', { query: this.query.trim(), kind: this.kind, limit: 60, ...(cursor ? { cursor } : {}) });
      if (this.destroyed || sequence !== this.requestSequence) return;
      this.discoveryStatus = 'ready';
      this.results = response;
      if (previous) this.discoveryPages.pop(); else if (next) this.discoveryPages = [...this.discoveryPages, cursor ?? null].slice(-50); else this.discoveryPages = [null];
      if (scope !== this.discoveryScope) { this.field?.destroy(); this.field = null; this.fieldState = createHexFieldState(); this.discoveryScope = scope; this.selected = null; this.main.querySelector('.sw-side')?.remove(); }
      if (this.view !== 'world' || this.group) return;
      this.renderSearchResults();
      void this.loadDiscoveryApps(sequence);
      stage?.setAttribute('aria-busy', 'false');
      this.announce(this.searchCount(response.totals));
    } catch (error) {
      if (this.destroyed || sequence !== this.requestSequence) return;
      this.discoveryStatus = 'error';
      stage?.setAttribute('aria-busy', 'false');
      if (!this.results.people.length && !this.results.communities.length) stage?.replaceChildren();
      if (note) { note.replaceChildren(el('span', '', errorText(error)), button('Повторить', 'refresh', 'sw-button-small sw-button-quiet', () => { void this.search(); })); }
    }
  }

  private renderSearchResults(): void {
    const stage = this.main.querySelector<HTMLElement>('[data-world-stage]'); if (!stage) return;
    const note = this.main.querySelector<HTMLElement>('[data-search-note]');
    const entities: WorldEntity[] = [...this.results.communities.map(value => ({ type: 'community' as const, value })), ...this.results.people.map(value => ({ type: 'person' as const, value }))];
    if (note) note.textContent = entities.length ? this.searchCount(this.results.totals) : '';
    if (!entities.length) {
      this.field?.destroy(); this.field = null;
      if (this.discoveryStatus !== 'ready') {
        stage.replaceChildren();
        if (this.discoveryStatus === 'loading') stage.append(this.loading('Ищем людей и сообщества…'));
        return;
      }
      const empty = emptyState(this.query ? 'Пока никого не нашли' : 'Здесь начинается ваш круг', this.query ? 'Попробуйте другое имя или тему.' : 'Создайте сообщество и пригласите первых участников.', button('Создать сообщество', 'plus', 'sw-button-primary', () => this.openCommunityForm()));
      if (!this.profile?.discoverable && !this.query) empty.append(button('Показать меня в мире', 'eye', 'sw-button-quiet', () => this.openVisibility()));
      if (this.discoveryPages.length > 1) empty.append(button('Предыдущая страница', 'back', 'sw-button-quiet', () => { void this.search(false, true); }));
      stage.replaceChildren(empty); return;
    }
    const selected = this.selected ? entityId(this.selected) : '';
    if (this.preferences.presentation === 'field') {
      if (!this.field || !stage.contains(this.field.element)) { this.field?.destroy(); this.field = createHexField(entity => { void this.preview(entity); }, this.preferences.scale,{state:this.fieldState,onSelectApp:app=>{void this.openApplication(app);},resolveAppArt:app=>resolveAppArt(app).srcset.split(',')[0]?.trim().split(' ')[0]||resolveAppArt(app).src,onScaleChange:scale=>{this.preferences.scale=scale;this.persist();}}); stage.replaceChildren(this.field.element); }
      this.field.update(entities, selected);
      this.field.setApps(this.visibleDiscoveryApps());
    } else {
      this.field?.destroy(); this.field = null;
      const list = el('div', 'sw-results'); list.setAttribute('role', 'list');
      for (const entity of entities) { const row = el('div'); row.setAttribute('role', 'listitem'); row.append(this.resultCard(entity)); list.append(row); }
      stage.replaceChildren(list);
    }
    stage.querySelector('.sw-field-controls')?.remove();
    const controls = el('div', 'sw-field-controls');
    if (this.preferences.presentation === 'field') {
      controls.append(iconButton('Уменьшить поле', 'minus', () => this.changeScale(-.12)), iconButton('Увеличить поле', 'plus', () => this.changeScale(.12)));
      const reset = button('Сброс', 'refresh', 'sw-button-small sw-field-reset', () => { this.field?.resetView(); }); reset.setAttribute('aria-label', 'Вернуть обзор поля'); reset.title = 'Вернуть обзор поля'; controls.append(reset);
    }
    if (this.discoveryPages.length > 1) controls.append(iconButton('Предыдущая страница', 'back', () => { void this.search(false, true); }));
    if (this.results.nextCursor) controls.append(iconButton('Следующая страница', 'next', () => { void this.search(true); }));
    if(this.preferences.presentation==='field'){const legend=el('div','sx-field-legend');for(const [symbol,label] of [['cells','Приложение'],['person','Человек'],['people','Сообщество']]){const item=el('span');item.append(icon(symbol!),el('span','',label));legend.append(item);}controls.append(legend);}stage.append(controls);
  }

  private async loadDiscoveryApps(sequence: number): Promise<void> {
    const allowed = new Set(this.results.communities.filter(group => group.membership?.state === 'active').map(group => group.communityId));
    for (const id of this.discoveryApps.keys()) if (!allowed.has(id)) this.discoveryApps.delete(id);
    const groups=this.results.communities.filter(group=>allowed.has(group.communityId)).slice(0,12);let index=0;
    const worker=async()=>{while(index<groups.length){const group=groups[index++]!;try{const apps=await this.loadApps(group.communityId);if(this.destroyed||sequence!==this.requestSequence||this.view!=='world'||this.group)return;this.discoveryApps.set(group.communityId,apps);}catch{if(this.destroyed||sequence!==this.requestSequence||this.view!=='world'||this.group)return;this.discoveryApps.delete(group.communityId);}this.field?.setApps(this.visibleDiscoveryApps());}};await Promise.all(Array.from({length:Math.min(3,groups.length)},worker));
  }

  private visibleDiscoveryApps(): WorldAppRecord[] {
    return this.results.communities.filter(group => group.membership?.state === 'active').flatMap(group => this.discoveryApps.get(group.communityId) || []);
  }

  private changeScale(delta: number): void { this.preferences.scale = Math.max(.65, Math.min(1.4, this.preferences.scale + delta)); this.persist(); this.field?.setScale(this.preferences.scale); }

  private resultCard(entity: WorldEntity): HTMLButtonElement {
    const card = el('button', `sw-result${this.selected && entityId(this.selected) === entityId(entity) ? ' is-selected' : ''}`); card.type = 'button';
    const copy = el('span', 'sw-grow'); copy.append(el('strong', 'sw-result-title', entityName(entity)));
    if (entity.type === 'community') {
      const group = entity.value; card.append(communityEmblem(group)); copy.append(el('span', 'sw-result-description', group.description), el('span', 'sw-result-meta', `${nounCount(group.memberCount, 'участник', 'участника', 'участников')} · ${this.joinLabel(group)}`));
    } else { const profile = entity.value; card.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision)); copy.append(el('span', 'sw-result-description', profile.bio), el('span', 'sw-result-meta', profile.interests.join(' · '))); }
    card.append(copy, icon('next')); card.addEventListener('click', () => { void this.preview(entity); }); return card;
  }

  private joinLabel(group: WorldCommunity): string { return group.joinPolicy === 'open' ? 'Открытое сообщество' : group.joinPolicy === 'request' ? 'По заявке' : 'По приглашению'; }
  private tags(topics: string[]): HTMLElement { const tags = el('div', 'sw-tags'); topics.slice(0, 5).forEach((topic, index) => tags.append(badge(topic, undefined, ['sage', 'lilac', 'honey'][index % 3]))); return tags; }
  private faces(profiles: WorldProfile[], count: number): HTMLElement { const row = el('div', 'sw-face-stack'); profiles.slice(0, 4).forEach(profile => row.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision))); if (count > Math.min(4, profiles.length)) row.append(el('span', 'sw-face-more', `+${count - Math.min(4, profiles.length)}`)); return row; }

  private async preview(entity: WorldEntity): Promise<void> {
    this.selected = entity; this.renderSearchResults();
    const workspace = this.main.querySelector('.sw-workspace'); if (!workspace) return;
    workspace.querySelector('.sw-side')?.remove();
    const side = el('aside', 'sw-side'); side.setAttribute('aria-label', entityName(entity));
    const inner = el('div', 'sw-side-inner'); const close = iconButton('Закрыть карточку', 'close', () => this.closePreview()); close.classList.add('sw-side-close');
    const expand = iconButton('Развернуть карточку', 'down', () => { const expanded = side.classList.toggle('is-expanded'); expand.setAttribute('aria-expanded', String(expanded)); expand.setAttribute('aria-label', expanded ? 'Свернуть карточку' : 'Развернуть карточку'); }); expand.classList.add('sw-side-expand'); expand.setAttribute('aria-expanded', 'false');
    side.append(close, expand, inner); workspace.append(side);
    if (entity.type === 'community') {
      const group = entity.value;
      const summary = el('div', 'sw-side-summary'); summary.append(communityEmblem(group), el('h2', '', group.name), el('p', 'sw-side-description', group.description), this.tags(group.topics));
      const members = el('div', 'sw-preview-members'); const memberInfo = el('div', 'sw-preview-member-info'); memberInfo.append(el('p', 'sw-small-note', nounCount(group.memberCount, 'участник', 'участника', 'участников')), badge(this.joinLabel(group), group.joinPolicy === 'open' ? 'world' : 'lock', 'sage')); members.append(this.faces(group.previewMembers, group.memberCount), memberInfo);
      inner.append(summary, members);
      if (group.showcase) { const about = el('details', 'sx-preview-about'); about.append(el('summary', '', 'О сообществе'), el('p', '', group.showcase)); inner.append(about); }
      if(group.membership?.state==='active'){const appsSection=el('section','sx-preview-apps');appsSection.append(el('h3','','Приложения'));const list=el('div','sx-preview-app-list');list.append(this.loading('Открываем приложения'));appsSection.append(list);inner.append(appsSection);void this.loadApps(group.communityId).then(apps=>{if(!side.isConnected||this.destroyed||this.selected!==entity)return;list.replaceChildren();for(const app of apps){const row=button(app.name,app.symbol||'grid','sx-preview-app',()=>{this.group=group;void this.openApplication(app);});row.append(icon('next'));list.append(row);}if(!apps.length)list.append(el('p','sw-muted','Пока нет приложений'));}).catch(()=>{if(list.isConnected)list.replaceChildren(el('p','sw-muted','Приложения не обновлены'));});}
      const footer = el('div', 'sw-preview-footer'); footer.append(button(group.membership?.state === 'active' ? 'Открыть сообщество' : 'Посмотреть группу', 'arrow', 'sw-button-primary sw-button-large sw-button-wide', () => { void this.openGroup(group.communityId); })); inner.append(footer);
    } else {
      const profile = entity.value;
      const summary = el('div', 'sw-side-summary'); summary.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), el('h2', '', profile.displayName), el('p', 'sw-side-description', profile.bio), this.tags(profile.interests)); inner.append(summary);
      const pending = this.loading('Открываем профиль'); inner.append(pending);
      try {
        const response = await this.api.request<{ profile: WorldProfile; communities: WorldCommunity[]; canRequestContact?: boolean }>('world.profile.view', { profileId: profile.profileId });
        if (!side.isConnected || this.destroyed) return; pending.remove();
        if (profile.profileId === this.profile?.profileId) inner.append(button('Редактировать профиль', 'person', 'sw-button-primary', () => this.openProfileEditor()));
        else if (response.canRequestContact && this.options.requestContact) {
          const contact = button('Добавить в контакты', 'person', 'sw-button-primary sw-button-wide', () => {
            contact.disabled = true;
            void Promise.resolve(this.options.requestContact!(profile)).then(() => { contact.replaceChildren(icon('check'), el('span', '', 'Запрос отправлен')); this.toast('Запрос в контакты отправлен'); }).catch(error => { contact.disabled = false; this.toast(errorText(error), true); });
          }); inner.append(contact, el('p', 'sw-small-note', 'После принятия человек появится в контактах.'));
        }
        if (response.communities.length) {
          inner.append(el('hr', 'sw-rule'), el('h3', '', 'Открыто в профиле'));
          response.communities.forEach(group => inner.append(this.resultCard({ type: 'community', value: group })));
        }
      } catch (error) { if (side.isConnected) pending.replaceWith(el('div', 'sw-error', errorText(error))); }
    }
    if (matchMedia('(max-width:760px)').matches) close.focus({ preventScroll: true });
  }

  private closePreview(): void { const selected = this.selected ? entityId(this.selected) : ''; this.main.querySelector('.sw-side')?.remove(); this.selected = null; this.renderSearchResults(); if (selected) this.main.querySelector<HTMLButtonElement>(`[data-entity-id="${CSS.escape(selected)}"]`)?.focus({ preventScroll: true }); }

  private async openGroup(id: string, tab: GroupTab = 'about'): Promise<void> {
    if (!this.group) this.groupReturn = this.view === 'world' || this.view === 'messages' ? this.view : 'mine';
    this.writeRoute(`community/${id}/${tab}`);
    this.cleanScreen(); const sequence = this.screenSequence;
    this.main.replaceChildren(this.loading('Открываем сообщество'));
    try {
      const { community } = await this.api.request<{ community: WorldCommunity }>('world.community.get', { communityId: id });
      if (this.destroyed || sequence !== this.screenSequence) return;
      this.group = community; this.groupTab = tab; this.remember(`community/${id}/${tab}`, community.name, 'people'); this.renderGroup();
    } catch (error) { if (this.destroyed || sequence !== this.screenSequence) return; this.main.replaceChildren(emptyState('Сообщество недоступно', errorText(error), button('В общий мир', 'back', 'sw-button-primary', () => this.navigate('world')))); }
  }

  private renderGroup(): void {
    const group = this.group; if (!group) return;
    this.cleanScreen();
    this.writeRoute(`community/${group.communityId}/${this.groupTab}`);
    this.view = this.groupTab === 'chat' ? 'messages' : 'mine'; this.renderNavigation();
    this.avatars.setContext(group.membership?.state === 'active' ? group.communityId : undefined);
    const screen = el('section', `sw-detail-view${this.groupTab === 'chat' ? ' has-chat' : ''}`); const summary = el('header', 'sw-community-summary');
    const crumb = el('div', 'sw-breadcrumb'); crumb.append(button(this.groupReturn === 'world' ? 'К поиску' : this.groupReturn === 'messages' ? 'Все чаты' : 'Моё поле', 'back', 'sw-button-quiet', () => this.navigate(this.groupReturn)));
    const title = el('div', 'sw-community-title'); const copy = el('div'); copy.append(el('h1', '', group.name), el('p', '', group.description)); title.append(communityEmblem(group), copy);
    const meta = el('div', 'sw-community-meta'); meta.append(badge(nounCount(group.memberCount, 'участник', 'участника', 'участников'), 'people'), badge(this.joinLabel(group), group.joinPolicy === 'open' ? 'world' : 'lock', 'sage'));
    summary.append(crumb, title, meta);
    const people = el('div', 'sw-community-people');
    group.previewMembers.slice(0, 5).forEach(profile => { const target = el('button', 'sw-member-card'); target.type = 'button'; target.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), el('span', '', profile.displayName)); target.addEventListener('click', () => this.openPersonDialog(profile)); people.append(target); });
    const actions = el('div', 'sw-community-actions');
    if (group.membership?.state === 'active') {
      actions.append(badge('Вы участник', 'check', 'sage'));
      const pin = iconButton(group.membership.pinned ? 'Открепить сообщество' : 'Закрепить в моих сотах', 'pin', () => { void this.mutateMembership('world.membership.preferences', { pinned: !group.membership?.pinned }); }); pin.classList.toggle('is-selected', group.membership.pinned); actions.append(pin);
      actions.append(iconButton(group.pendingCount ? `Участники и управление, ${nounCount(group.pendingCount, 'новая заявка', 'новые заявки', 'новых заявок')}` : 'Участники и управление', 'people', () => this.openGroupManagement(group)));
      if (group.pendingCount) actions.append(button(`${group.pendingCount} ${group.pendingCount === 1 ? 'заявка' : 'заявки'}`, 'people', 'sw-button-small', () => this.openGroupManagement(group, 'requested')));
    } else actions.append(this.joinButton(group));
    actions.append(iconButton('Поделиться группой', 'external', () => this.shareCommunity(group)));
    if (group.permissions.canManage) actions.append(iconButton('Настроить сообщество', 'settings', () => this.openCommunityForm(group)));
    summary.append(actions);
    const content = el('div', 'sw-community-content'); const tabs = el('div', 'sw-community-tabs'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', 'Сообщество');
    for (const [id, label] of [['about', 'О группе'], ['chat', 'Чат'], ['apps', 'Приложения']] as const) {
      const tab = button(label, undefined, this.groupTab === id ? 'is-selected' : '', () => { this.groupTab = id; this.renderGroup(); });
      tab.id = `sw-community-tab-${id}`; tab.setAttribute('role', 'tab'); tab.setAttribute('aria-controls', 'sw-community-panel'); tab.setAttribute('aria-selected', String(this.groupTab === id)); tab.tabIndex = this.groupTab === id ? 0 : -1;
      tab.addEventListener('keydown', event => { if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return; event.preventDefault(); const order: GroupTab[] = ['about', 'chat', 'apps']; const index = order.indexOf(this.groupTab); this.groupTab = event.key === 'Home' ? 'about' : event.key === 'End' ? 'apps' : order[(index + (event.key === 'ArrowRight' ? 1 : 2)) % 3]!; this.renderGroup(); this.main.querySelector<HTMLButtonElement>('[role=tab][aria-selected=true]')?.focus(); }); tabs.append(tab);
    }
    content.append(tabs); screen.append(summary, content); this.main.replaceChildren(screen);
    if (group.membership?.state === 'requested' || group.membership?.state === 'invited') {
      const sequence = this.screenSequence; let checking = false;
      this.chatTimer = setInterval(() => {
        if (checking || this.destroyed || document.visibilityState === 'hidden' || this.screenSequence !== sequence) return;
        checking = true;
        void this.api.request<{ community: WorldCommunity }>('world.community.get', { communityId: group.communityId }).then(result => {
          if (this.destroyed || this.screenSequence !== sequence) return;
          if (result.community.membership?.state !== group.membership?.state) { this.group = result.community; this.updateCommunity(result.community); if (result.community.membership?.state === 'active') { this.groupTab = 'chat'; this.toast('Вас приняли в сообщество'); } this.renderGroup(); }
        }).catch(() => { /* A visible retry remains available in the group navigation. */ }).finally(() => { checking = false; });
      }, 4500);
    }
    const body = el('div', `sw-community-body${this.groupTab === 'chat' ? ' is-chat-panel' : ''}`); body.id = 'sw-community-panel'; body.setAttribute('role', 'tabpanel'); body.setAttribute('aria-labelledby', `sw-community-tab-${this.groupTab}`); content.append(body);
    if (this.groupTab === 'chat' && group.membership?.state === 'active') { this.mountChat(body, group, true); return; }
    if (this.groupTab === 'chat') { body.append(emptyState('Разговор начинается здесь', group.joinPolicy === 'invite' ? 'Организатор может пригласить вас в сообщество.' : 'Вступите, чтобы читать и писать в общий чат.', this.joinButton(group), 'chat')); return; }
    if (this.groupTab === 'apps') { void this.renderApps(body, group); return; }
    const stack = el('div', 'sw-stack');
    const showcase = el('div', `sw-showcase sw-community-intro sw-color-${worldColor(group.color)}`); showcase.append(el('p', '', group.showcase || group.description || 'Здесь можно общаться и открывать общие приложения.'));
    stack.append(showcase, this.tags(group.topics));
    if (group.membership?.state !== 'active') stack.append(el('p', 'sw-small-note', group.joinPolicy === 'open' ? 'После вступления откроется общий чат' : 'Организатор подтвердит участие'));
    else stack.append(button('Открыть общий чат', 'chat', 'sw-button-primary sw-button-large', () => { this.groupTab = 'chat'; this.renderGroup(); }));
    if (group.previewMembers.length) { const memberSection = el('section', 'sw-community-member-section'); const memberHead = el('div', 'sw-panel-head'); memberHead.append(el('h2', '', 'Участники')); if (group.membership?.state === 'active') memberHead.append(button('Все', 'people', 'sw-button-small sw-button-quiet', () => this.openGroupManagement(group))); memberSection.append(memberHead, people); stack.append(memberSection); }
    body.append(stack);
  }

  private joinButton(group: WorldCommunity): HTMLButtonElement {
    const state = group.membership?.state;
    const label = state === 'requested' ? 'Заявка отправлена' : state === 'banned' ? 'Участие ограничено' : state === 'invited' ? 'Принять приглашение' : group.joinPolicy === 'open' ? 'Вступить в группу' : group.joinPolicy === 'request' ? 'Подать заявку' : 'По приглашению';
    const target = button(label, state === 'requested' ? 'check' : 'people', 'sw-button-primary sw-button-large', () => {
      target.disabled = true; const sequence = this.screenSequence;
      void this.api.request<{ community: WorldCommunity }>('world.membership.join', { communityId: group.communityId }).then(response => {
        if (this.destroyed) return;
        this.updateCommunity(response.community);
        if (sequence === this.screenSequence && target.isConnected) { this.group = response.community; this.groupTab = response.community.membership?.state === 'active' ? 'chat' : 'about'; this.renderGroup(); }
        this.toast(response.community.membership?.state === 'active' ? 'Вы в сообществе' : 'Заявка отправлена организатору');
      }).catch(error => { target.disabled = false; this.toast(errorText(error), true); });
    }); target.disabled = state === 'requested' || state === 'banned' || (group.joinPolicy === 'invite' && state !== 'invited'); return target;
  }

  private updateCommunity(group: WorldCommunity): void {
    const replace = (items: WorldCommunity[]): WorldCommunity[] => items.map(item => item.communityId === group.communityId ? group : item);
    this.communities = replace(this.communities); this.results.communities = replace(this.results.communities);
    if (group.membership?.state === 'active' && !this.communities.some(item => item.communityId === group.communityId)) this.communities.push(group);
  }

  private async mutateMembership(method: string, parameters: Record<string, unknown>): Promise<void> {
    if (!this.group) return;
    const sequence = this.screenSequence, communityId = this.group.communityId;
    try { const response = await this.api.request<{ community: WorldCommunity | null }>(method, { communityId, ...parameters }); if (this.destroyed) return; if (response.community) this.updateCommunity(response.community); if (sequence !== this.screenSequence || this.group?.communityId !== communityId) return; if (response.community) { this.group = response.community; this.renderGroup(); } else this.navigate('world'); }
    catch (error) { this.toast(errorText(error), true); }
  }

  private mountChat(parent: HTMLElement, group: WorldCommunity, showHeader = false): void {
    this.avatars.setContext(group.communityId);
    const sequence = this.screenSequence;
    const accountId = this.profile?.profileId ?? this.deskAccount;
    const initialDraft = this.chatDrafts.read(accountId, group.communityId);
    const chat = el('section', 'sw-chat'); chat.setAttribute('aria-label', `Чат: ${group.name}`);
    let replyTo = initialDraft.replyTo, sending = false, accessClosed = false, searchIndex = -1;
    let syncSeq = 0, firstSeq = Number.MAX_SAFE_INTEGER, lastRead = 0, reading = false, initial = true, fetching = false, historyLoading = false, hasHistory = false;
    let catchupTimer: ReturnType<typeof setTimeout> | undefined;
    const highlightTimers = new Set<ReturnType<typeof setTimeout>>();
    const known = new Map<string, WorldMessage>();
    const rows = new Map<string, HTMLElement>();
    const active = (): boolean => !this.destroyed && !accessClosed && sequence === this.screenSequence && chat.isConnected;
    const nearEnd = (): boolean => messages.clientHeight > 0 && messages.getClientRects().length > 0 && messages.scrollHeight - messages.scrollTop - messages.clientHeight < 100;
    let timelineDirty = false, searchMatches: string[] = [], unseen = 0;

    const menu = el('div', 'sw-chat-menu'); menu.hidden = true; menu.setAttribute('role', 'menu'); menu.setAttribute('aria-label', 'Действия');
    let menuAnchor: HTMLButtonElement | null = null, menuScrollTop = 0;
    const closeMenu = (restoreFocus = false): void => {
      const anchor = menuAnchor; menu.hidden = true; menuAnchor = null; anchor?.setAttribute('aria-expanded', 'false'); anchor?.closest('.sw-chat-message')?.classList.remove('has-actions');
      if (restoreFocus && anchor?.isConnected) anchor.focus({ preventScroll: true });
    };
    const openMenu = (anchor: HTMLButtonElement, actions: { label: string; symbol: string; run(): void; danger?: boolean; disabled?: boolean }[]): void => {
      if (menuAnchor === anchor && !menu.hidden) { closeMenu(true); return; }
      closeMenu(); menuAnchor = anchor; menuScrollTop = messages.scrollTop; anchor.setAttribute('aria-expanded', 'true'); anchor.closest('.sw-chat-message')?.classList.add('has-actions');
      menu.replaceChildren(...actions.map(action => {
        const item = button(action.label, action.symbol, `sw-chat-menu-item${action.danger ? ' is-danger' : ''}`, () => { closeMenu(true); action.run(); });
        item.setAttribute('role', 'menuitem'); item.disabled = !!action.disabled; return item;
      }));
      menu.hidden = false;
      const bounds = chat.getBoundingClientRect(), target = anchor.getBoundingClientRect();
      menu.style.left = `${Math.max(8, Math.min(bounds.width - menu.offsetWidth - 8, target.right - bounds.left - menu.offsetWidth))}px`;
      const below = target.bottom - bounds.top + 6;
      menu.style.top = `${Math.max(8, below + menu.offsetHeight < bounds.height - 8 ? below : target.top - bounds.top - menu.offsetHeight - 6)}px`;
      menu.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus({ preventScroll: true });
    };
    menu.addEventListener('keydown', event => {
      if (event.key === 'Escape') { event.preventDefault(); closeMenu(true); return; }
      if (event.key === 'Tab') { closeMenu(); return; }
      if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault(); const items = Array.from(menu.querySelectorAll<HTMLButtonElement>('button:not(:disabled)'));
      const index = items.indexOf(document.activeElement as HTMLButtonElement);
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : (index + (event.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length;
      items[next]?.focus({ preventScroll: true });
    });
    const outsideMenu = (event: PointerEvent): void => { if (event.target instanceof Node && !menu.contains(event.target) && !menuAnchor?.contains(event.target)) closeMenu(); };
    document.addEventListener('pointerdown', outsideMenu);

    const searchBar = el('div', 'sw-chat-search'); searchBar.hidden = true;
    const searchInput = textInput('', 'Поиск в загруженных сообщениях', 120); searchInput.type = 'search'; searchInput.setAttribute('aria-label', 'Поиск в загруженной истории чата');
    const searchCount = el('span', 'sw-chat-search-count'); searchCount.setAttribute('role', 'status');
    const searchPrevious = iconButton('Предыдущее совпадение', 'back', () => moveSearch(-1));
    const searchNext = iconButton('Следующее совпадение', 'next', () => moveSearch(1));
    const searchClose = iconButton('Закрыть поиск', 'close', () => { searchBar.hidden = true; searchInput.value = ''; refreshSearch(); input.focus({ preventScroll: true }); });
    searchBar.append(icon('search'), searchInput, searchCount, searchPrevious, searchNext, searchClose);
    let refreshHeader = (): void => {};
    if (showHeader) {
      const header = el('div', 'sw-chat-header'); const back = iconButton('Все чаты', 'back', () => { this.navigate('messages'); this.main.querySelector<HTMLHeadingElement>('.sw-inbox-head h1')?.focus({ preventScroll: true }); }); back.classList.add('sw-mobile-back');
      const identity = el('button', 'sw-chat-identity'); identity.type = 'button'; identity.setAttribute('aria-label', `О сообществе ${group.name}`); identity.addEventListener('click', () => { void this.openGroup(group.communityId); });
      const copy = el('span', 'sw-grow'), name = el('strong', '', group.name), members = el('small', 'sw-muted', nounCount(group.memberCount, 'участник', 'участника', 'участников')); copy.append(name, members); identity.append(communityEmblem(group), copy);
      refreshHeader = () => { name.textContent = group.name; members.textContent = nounCount(group.memberCount, 'участник', 'участника', 'участников'); identity.setAttribute('aria-label', `О сообществе ${group.name}`); identity.replaceChildren(communityEmblem(group), copy); chat.setAttribute('aria-label', `Чат: ${group.name}`); };
      const searchOpen = iconButton('Поиск в чате', 'search', () => { searchBar.hidden = !searchBar.hidden; if (!searchBar.hidden) searchInput.focus(); else { searchInput.value = ''; refreshSearch(); } });
      const more = iconButton('Действия с чатом', 'more'); more.setAttribute('aria-haspopup', 'menu'); more.setAttribute('aria-expanded', 'false');
      const changePreference = (key: 'muted' | 'pinned'): void => {
        void this.api.request<{ community: WorldCommunity }>('world.membership.preferences', { communityId: group.communityId, [key]: !group.membership?.[key] }).then(result => {
          if (!active()) return; group = result.community; this.updateCommunity(group); if (this.group?.communityId === group.communityId) this.group = group; notifyConversation();
        }).catch(reason => { if (active()) this.toast(errorText(reason), true); });
      };
      more.addEventListener('click', () => openMenu(more, [
        { label: 'О сообществе', symbol: 'people', run: () => { void this.openGroup(group.communityId); } },
        { label: 'Участники', symbol: 'person', run: () => this.openGroupManagement(group) },
        { label: group.membership?.muted ? 'Включить уведомления' : 'Отключить уведомления', symbol: 'bell', run: () => changePreference('muted') },
        { label: group.membership?.pinned ? 'Открепить чат' : 'Закрепить чат', symbol: 'pin', run: () => changePreference('pinned') },
      ]));
      header.append(back, identity, searchOpen, more); chat.append(header);
    }
    const messages = el('div', 'sw-chat-messages'); messages.setAttribute('role', 'log'); messages.setAttribute('aria-label', 'Сообщения'); messages.setAttribute('aria-live', 'polite'); messages.setAttribute('aria-relevant', 'additions'); messages.setAttribute('tabindex', '0'); messages.append(this.loading('Загружаем разговор'));
    const previous = button('Ранее', 'back', 'sw-chat-history', () => { void loadHistory(); }); previous.hidden = true;
    const jump = iconButton('К последним сообщениям', 'down', () => { messages.scrollTo({ top: messages.scrollHeight, behavior: this.preferences.motion ? 'smooth' : 'instant' }); unseen = 0; updateJump(); markRead(); }); jump.classList.add('sw-chat-jump'); jump.hidden = true;
    const jumpCount = el('span', 'sw-chat-jump-count'); jumpCount.hidden = true; jump.append(jumpCount);
    const dock = el('div', 'sw-composer-dock');
    const reply = el('div', 'sw-composer-reply'); reply.hidden = true;
    const replyCopy = el('button', 'sw-composer-reply-copy'); replyCopy.type = 'button'; replyCopy.addEventListener('click', () => { if (replyTo) void jumpToMessage(replyTo); });
    const replyAuthor = el('strong'), replyText = el('span'); replyCopy.append(icon('back'), replyAuthor, replyText);
    const cancelReply = iconButton('Отменить ответ', 'close', () => { replyTo = null; this.chatDrafts.edit(accountId, group.communityId, input.value, null); refreshReply(); input.focus({ preventScroll: true }); }); reply.append(replyCopy, cancelReply);
    const composer = el('form', 'sw-composer'); composer.dataset.pwaIgnore = ''; const input = el('textarea'); input.rows = 1; input.maxLength = 6000; input.placeholder = group.permissions.canWrite ? 'Сообщение' : 'В этом чате доступно только чтение'; input.enterKeyHint = 'send'; input.setAttribute('aria-label', `Сообщение в ${group.name}`); input.setAttribute('aria-keyshortcuts', 'Enter'); input.value = initialDraft.text;
    const send = button('Отправить', 'send', 'sw-button-primary'); send.type = 'submit'; send.setAttribute('aria-label', 'Отправить сообщение');
    const error = el('div', 'sw-chat-error'); const errorMessage = el('div', 'sw-error'); errorMessage.setAttribute('role', 'alert'); error.append(errorMessage);
    const retrySend = button('Повторить отправку', 'refresh', 'sw-button-small sw-button-quiet', () => composer.requestSubmit()); retrySend.hidden = true; error.append(retrySend);
    const draftStatus = el('div', 'sw-chat-draft-status'); draftStatus.setAttribute('role', 'status');
    const downloadDraft = button('Скачать черновик', 'download', 'sw-button-small sw-button-quiet', () => {
      const url = URL.createObjectURL(new Blob([input.value], { type: 'text/plain;charset=utf-8' })); const link = el('a'); link.href = url; link.download = 'soty-chat-draft.txt'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    const limitStatus = el('span', 'sw-composer-limit'); limitStatus.hidden = true;
    const updateComposer = (): void => { input.disabled = accessClosed || !group.permissions.canWrite; input.placeholder = group.permissions.canWrite ? 'Сообщение' : 'В этом чате доступно только чтение'; input.setAttribute('aria-label', `Сообщение в ${group.name}`); send.disabled = sending || input.disabled || !input.value.trim(); cancelReply.disabled = input.disabled; composer.setAttribute('aria-busy', String(sending)); send.dataset.state = sending ? 'sending' : 'ready'; send.setAttribute('aria-label', sending ? 'Отправляем сообщение' : 'Отправить сообщение'); limitStatus.hidden = input.value.length < 5800; limitStatus.textContent = `${input.value.length} / 6000`; };
    const describeDraft = (): void => {
      const volatile = this.chatDrafts.isVolatile(accountId, group.communityId);
      draftStatus.textContent = volatile ? 'Черновик пока только в открытом приложении' : '';
      draftStatus.classList.toggle('is-error', volatile); draftStatus.hidden = !volatile; downloadDraft.hidden = !volatile;
    };
    error.append(draftStatus, downloadDraft, limitStatus); describeDraft();
    const fitComposer = (): void => { const stick = nearEnd(); input.style.height = 'auto'; input.style.height = `${Math.min(144, Math.max(24, input.scrollHeight))}px`; if (stick) messages.scrollTop = messages.scrollHeight; };
    const refreshReply = (): void => {
      reply.hidden = !replyTo; if (!replyTo) return;
      const target = known.get(replyTo); replyAuthor.textContent = target ? `Ответ · ${target.author.displayName}` : 'Ответ на сообщение'; replyText.textContent = target ? chatPreview(target) : 'Сообщение из предыдущей истории';
    };
    input.addEventListener('input', () => { this.chatDrafts.edit(accountId, group.communityId, input.value, replyTo); updateComposer(); fitComposer(); describeDraft(); });
    input.addEventListener('keydown', event => {
      if (event.isComposing || event.keyCode === 229) return;
      if (shouldSendOnEnter(event)) { event.preventDefault(); if (!send.disabled) composer.requestSubmit(); }
      else if (event.key === 'Escape' && replyTo && !sending) { event.preventDefault(); cancelReply.click(); }
    });
    composer.append(input, send); dock.append(reply, error, composer); chat.append(searchBar, messages, jump, dock, menu); parent.append(chat);
    fitComposer(); updateComposer(); refreshReply();
    let composerWidth = 0;
    const composerResize = new ResizeObserver(entries => { const width = entries.find(entry => entry.target === composer)?.contentRect.width ?? 0; if (width > 0 && width !== composerWidth) { composerWidth = width; fitComposer(); } chat.style.setProperty('--sw-chat-dock-height', `${dock.getBoundingClientRect().height}px`); });
    composerResize.observe(composer); composerResize.observe(dock);
    const unsubscribeDraft = this.chatDrafts.subscribe(accountId, group.communityId, draft => { if (input.value !== draft.text) { input.value = draft.text; fitComposer(); } replyTo = draft.replyTo; updateComposer(); refreshReply(); describeDraft(); });

    const ordered = (): WorldMessage[] => [...known.values()].sort((a, b) => a.seq - b.seq);
    const notifyConversation = (readAcknowledged = false): void => { chat.dispatchEvent(new CustomEvent('soty:chat-updated', { bubbles: true, detail: { community: group, message: ordered().at(-1), readAcknowledged } })); };
    const capturePosition = (): { node?: HTMLElement; top: number; height: number; scroll: number } => {
      const bounds = messages.getBoundingClientRect(), top = bounds.top;
      const node = Array.from(messages.querySelectorAll<HTMLElement>('[data-message-seq]')).find(row => { const rect = row.getBoundingClientRect(); return rect.bottom > top && rect.top < bounds.bottom; });
      return { ...(node ? { node } : {}), top: node?.getBoundingClientRect().top ?? top, height: messages.scrollHeight, scroll: messages.scrollTop };
    };
    const restorePosition = (position: ReturnType<typeof capturePosition>): void => { messages.scrollTop = position.node?.isConnected ? messages.scrollTop + position.node.getBoundingClientRect().top - position.top : position.scroll + messages.scrollHeight - position.height; };
    const updateJump = (): void => {
      jump.hidden = initial || nearEnd() || !known.size; if (nearEnd()) unseen = 0;
      jumpCount.hidden = !unseen; jumpCount.textContent = unseen > 99 ? '99+' : String(unseen); jump.setAttribute('aria-label', unseen ? `К последним сообщениям, ${unseen} новых` : 'К последним сообщениям');
    };
    const focusMessage = (messageId: string): void => {
      const row = rows.get(messageId); if (!row) return;
      row.scrollIntoView({ block: 'center', behavior: this.preferences.motion ? 'smooth' : 'instant' }); row.classList.add('is-highlighted');
      const timer = setTimeout(() => { row.classList.remove('is-highlighted'); highlightTimers.delete(timer); }, 1800); highlightTimers.add(timer);
    };
    const jumpToMessage = async (messageId: string): Promise<void> => {
      for (let page = 0; !known.has(messageId) && hasHistory && page < 8 && active(); page++) { if (!await loadHistory()) break; }
      if (!active()) return;
      if (known.has(messageId)) focusMessage(messageId); else this.toast('Сообщение ещё не загружено. Откройте более раннюю историю.', true);
    };
    const renderText = (node: HTMLElement, text: string, query: string): void => {
      node.replaceChildren(); const needle = query.trim().toLocaleLowerCase('ru-RU'), haystack = text.toLocaleLowerCase('ru-RU');
      if (!needle) { node.textContent = text; return; }
      let cursor = 0, found = haystack.indexOf(needle);
      while (found >= 0) { node.append(document.createTextNode(text.slice(cursor, found)), el('mark', '', text.slice(found, found + needle.length))); cursor = found + needle.length; found = haystack.indexOf(needle, cursor); }
      node.append(document.createTextNode(text.slice(cursor)));
    };
    const moveSearch = (direction: number): void => {
      if (!searchMatches.length) return; searchIndex = (searchIndex + direction + searchMatches.length) % searchMatches.length;
      const target = searchMatches[searchIndex]; if (target) focusMessage(target); searchCount.textContent = `${searchIndex + 1} / ${searchMatches.length}`;
    };
    const refreshSearch = (): void => {
      const query = searchInput.value.trim().toLocaleLowerCase('ru-RU'); const previousMatch = searchMatches[searchIndex];
      searchMatches = ordered().filter(message => !message.removed && query && message.text.toLocaleLowerCase('ru-RU').includes(query)).map(message => message.messageId);
      searchIndex = previousMatch ? searchMatches.indexOf(previousMatch) : -1;
      if (searchIndex < 0 && searchMatches.length) searchIndex = searchMatches.length - 1;
      searchPrevious.disabled = searchNext.disabled = !searchMatches.length;
      searchCount.textContent = query ? searchMatches.length ? `${searchIndex + 1} / ${searchMatches.length}` : 'Нет совпадений' : '';
      for (const message of known.values()) { const text = rows.get(message.messageId)?.querySelector<HTMLElement>('.sw-message-text'); if (text) renderText(text, message.removed ? 'Сообщение удалено' : message.text, query); }
    };
    searchInput.addEventListener('input', () => { searchIndex = -1; refreshSearch(); const target = searchMatches[searchIndex]; if (target) focusMessage(target); });
    searchInput.addEventListener('keydown', event => { if (event.isComposing || event.keyCode === 229) return; if (event.key === 'Escape') { event.preventDefault(); searchClose.click(); } else if (event.key === 'Enter') { event.preventDefault(); moveSearch(event.shiftKey ? -1 : 1); } });
    const messageActions = (messageId: string, anchor: HTMLButtonElement): void => {
      const message = known.get(messageId); if (!message || message.removed) return;
      const actions: Parameters<typeof openMenu>[1] = [
        { label: 'Ответить', symbol: 'back', disabled: sending || accessClosed || !group.permissions.canWrite, run: () => { replyTo = messageId; this.chatDrafts.edit(accountId, group.communityId, input.value, replyTo); refreshReply(); input.focus({ preventScroll: true }); } },
        { label: 'Копировать текст', symbol: 'list', run: () => { if (!navigator.clipboard) { this.toast('Выделите текст сообщения, чтобы скопировать его.', true); return; } void navigator.clipboard.writeText(message.text).then(() => { if (active()) this.toast('Текст скопирован'); }).catch(() => { if (active()) this.toast('Не удалось скопировать. Выделите текст сообщения.', true); }); } },
      ];
      if (message.author.profileId === accountId || group.permissions.canModerate) actions.push({ label: 'Удалить у всех', symbol: 'trash', danger: true, run: () => this.confirmAction('Удалить сообщение?', 'Оно исчезнет из разговора у всех участников.', 'Удалить', async () => {
        await this.api.request('world.chat.remove', { communityId: group.communityId, messageId });
        if (!active()) return; const position = capturePosition(); append({ ...message, text: '', removed: true }); refreshTimeline(); restorePosition(position); notifyConversation();
      }) });
      openMenu(anchor, actions);
    };
    const createMessage = (message: WorldMessage): HTMLElement => {
      const own = message.author.profileId === accountId;
      const row = el('article', `sw-chat-message${own ? ' is-own' : ''}${message.removed ? ' is-removed' : ''}`); row.dataset.messageId = message.messageId; row.dataset.messageSeq = String(message.seq);
      if (!own) row.append(avatar(message.author.displayName, message.author.avatarUrl, worldColor(message.author.avatarColor), message.author.profileId, message.author.avatarRevision));
      const body = el('div', 'sw-message-body');
      if (!own) { const author = el('button', 'sw-message-author', message.author.displayName); author.type = 'button'; author.addEventListener('click', () => this.openPersonDialog(message.author)); body.append(author); }
      if (message.replyTo) { const quote = el('button', 'sw-message-reply'); quote.type = 'button'; quote.dataset.replyTo = message.replyTo; quote.append(el('strong'), el('span')); quote.addEventListener('click', () => { if (message.replyTo) void jumpToMessage(message.replyTo); }); body.append(quote); }
      body.append(el('div', 'sw-message-text', message.removed ? 'Сообщение удалено' : message.text));
      const meta = el('div', 'sw-message-meta'); const time = el('time', '', timeLabel(message.createdAt)); time.dateTime = new Date(message.createdAt).toISOString(); time.title = new Date(message.createdAt).toLocaleString('ru-RU'); meta.append(time);
      if (own && !message.removed) { const delivered = el('span', 'sw-message-sent'); delivered.append(icon('check')); delivered.setAttribute('aria-label', 'Отправлено'); delivered.title = 'Отправлено'; meta.append(delivered); }
      body.append(meta); row.append(body);
      if (!message.removed) { const more = iconButton('Действия с сообщением', 'more', () => messageActions(message.messageId, more)); more.classList.add('sw-message-actions'); more.setAttribute('aria-haspopup', 'menu'); more.setAttribute('aria-expanded', 'false'); row.append(more); row.addEventListener('contextmenu', event => { if (window.getSelection()?.toString()) return; event.preventDefault(); messageActions(message.messageId, more); }); }
      return row;
    };
    const append = (message: WorldMessage): void => {
      const old = known.get(message.messageId);
      if (old && old.removed === message.removed && old.text === message.text && old.author.revision === message.author.revision && old.author.avatarRevision === message.author.avatarRevision) return;
      known.set(message.messageId, message); firstSeq = Math.min(firstSeq, message.seq); timelineDirty = true;
      messages.querySelector('.sw-empty')?.remove();
      const row = createMessage(message), existing = rows.get(message.messageId); rows.set(message.messageId, row);
      if (existing?.isConnected) { if (menuAnchor && existing.contains(menuAnchor)) closeMenu(); existing.replaceWith(row); }
      else { const next = Array.from(messages.querySelectorAll<HTMLElement>('[data-message-seq]')).find(item => Number(item.dataset.messageSeq) > message.seq); messages.insertBefore(row, next ?? null); }
    };
    const refreshTimeline = (): void => {
      if (!timelineDirty) return; timelineDirty = false;
      messages.querySelectorAll('.sw-chat-day').forEach(day => day.remove());
      const orderedMessages = ordered(); let day = '';
      orderedMessages.forEach((message, index) => {
        const row = rows.get(message.messageId); if (!row) return;
        const currentDay = chatDayKey(message.createdAt);
        if (currentDay !== day) { const separator = el('div', 'sw-chat-day', chatDayLabel(message.createdAt)); separator.setAttribute('role', 'separator'); separator.setAttribute('aria-label', separator.textContent ?? ''); messages.insertBefore(separator, row); day = currentDay; }
        row.classList.toggle('is-grouped', canGroupMessages(orderedMessages[index - 1], message));
        const next = orderedMessages[index + 1]; row.classList.toggle('is-group-end', !next || !canGroupMessages(message, next));
      });
      for (const quote of messages.querySelectorAll<HTMLButtonElement>('.sw-message-reply')) { const target = known.get(quote.dataset.replyTo ?? ''); quote.querySelector('strong')!.textContent = target?.author.displayName ?? 'Ответ на сообщение'; quote.querySelector('span')!.textContent = target ? chatPreview(target) : 'Сообщение из предыдущей истории'; quote.setAttribute('aria-label', `Открыть сообщение: ${quote.querySelector('strong')!.textContent}`); }
      refreshReply(); refreshSearch();
    };

    const markRead = (): void => {
      if (!reading && !accessClosed && syncSeq > lastRead && document.visibilityState === 'visible' && nearEnd()) {
        reading = true; const throughSeq = syncSeq;
        void this.api.request<{ unreadCount: number }>('world.chat.read', { communityId: group.communityId, throughSeq }).then(result => { if (!active()) return; lastRead = throughSeq; group.unreadCount = result.unreadCount; this.updateCommunity(group); notifyConversation(true); }).catch(() => { /* Reading acknowledgement retries with the next refresh. */ }).finally(() => { reading = false; });
      }
    };
    const closeAccess = (reason: unknown): boolean => {
      if (!(typeof reason === 'object' && reason && 'code' in reason && ['community_membership_required', 'community_not_found', 'community_banned'].includes(String(reason.code)))) return false;
      accessClosed = true; closeMenu(); updateComposer(); this.avatars.setContext(); known.clear(); rows.clear(); reply.hidden = true; messages.replaceChildren(emptyState('Доступ к чату закрыт', 'Вернитесь в общий мир, чтобы продолжить.', button('В общий мир', 'world', 'sw-button-primary', () => this.navigate('world')), 'lock'));
      previous.hidden = true; jump.hidden = true; searchBar.hidden = true; if (this.chatTimer) clearInterval(this.chatTimer); if (catchupTimer) clearTimeout(catchupTimer); return true;
    };
    chat.addEventListener('soty:chat-community', event => {
      const detail = (event as CustomEvent<{ communityId: string; community: WorldCommunity | null }>).detail;
      if (!active() || detail.communityId !== group.communityId) return;
      if (!detail.community || detail.community.membership?.state !== 'active') { closeAccess({ code: 'community_membership_required' }); return; }
      group = detail.community; refreshHeader(); updateComposer();
    });
    const loadHistory = async (): Promise<boolean> => {
      if (historyLoading || !hasHistory || initial || accessClosed || !active()) return false;
      historyLoading = true; previous.disabled = true; previous.querySelector('span')!.textContent = 'Загружаем историю'; messages.setAttribute('aria-busy', 'true');
      try {
        const history = await this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, before: firstSeq, limit: 60 });
        if (!active()) return false; const position = capturePosition(); history.messages.forEach(append); hasHistory = history.hasMore && history.messages.length > 0; previous.hidden = !hasHistory; refreshTimeline(); restorePosition(position); updateJump(); if (retrySend.hidden) errorMessage.textContent = ''; return history.messages.length > 0;
      } catch (reason) { if (active() && !closeAccess(reason)) errorMessage.textContent = errorText(reason); return false; }
      finally { historyLoading = false; previous.disabled = false; previous.querySelector('span')!.textContent = 'Ранее'; messages.setAttribute('aria-busy', 'false'); }
    };
    const load = async (): Promise<void> => {
      if (fetching || historyLoading || accessClosed || !active() || document.visibilityState === 'hidden') return;
      fetching = true;
      try {
        // Standalone group chats have no inbox poll to refresh their permissions.
        if (!chat.closest('.sw-inbox')) {
          const current = await this.api.request<{ community: WorldCommunity }>('world.community.get', { communityId: group.communityId });
          if (!active()) return;
          if (current.community.membership?.state !== 'active') { closeAccess({ code: 'community_membership_required' }); return; }
          group = current.community; this.updateCommunity(group); refreshHeader(); updateComposer();
        }
        const response = await this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, limit: 60 });
        if (!active()) return;
        const updates: WorldMessage[] = [], incoming: WorldMessage[] = []; let nextSync = Math.max(0, ...response.messages.map(message => message.seq)), hasMoreForward = false;
        if (!initial) {
          // Refresh existing tombstones without jumping past an unseen interval.
          const visiblePosition = capturePosition();
          if (visiblePosition.node && Number(visiblePosition.node.dataset.messageSeq) < (response.messages[0]?.seq ?? 0)) {
            const visible = await this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, after: Math.max(0, Number(visiblePosition.node.dataset.messageSeq) - 1), limit: 60 });
            if (!active()) return; updates.push(...visible.messages);
          }
          const forward = await readChatForward({ after: syncSeq, active, fetchPage: after => this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, after, limit: 60 }), append: message => incoming.push(message) });
          if (!active()) return;
          nextSync = forward.cursor; hasMoreForward = forward.hasMore;
        }
        // Capture the user's current position after network waits, then apply a batch.
        const stickToEnd = initial || nearEnd(), position = capturePosition(), countBefore = known.size;
        if (initial) { messages.replaceChildren(previous); response.messages.forEach(append); hasHistory = response.hasMore; previous.hidden = !hasHistory; }
        else { [...response.messages, ...updates].filter(message => known.has(message.messageId)).forEach(append); incoming.forEach(append); }
        syncSeq = nextSync; if (hasMoreForward) catchupTimer = setTimeout(() => { void load(); }, 100);
        if (!known.size) messages.replaceChildren(emptyState('Первое сообщение за вами', group.memberCount < 2 ? 'Пригласите человека и начните разговор.' : 'Поздоровайтесь со своим кругом.', group.permissions.canModerate ? button('Пригласить человека', 'people', 'sw-button-primary', () => this.openInvite(group)) : undefined, 'chat'));
        refreshTimeline();
        if (!stickToEnd) unseen += Math.max(0, known.size - countBefore);
        initial = false; if (retrySend.hidden) errorMessage.textContent = '';
        if (stickToEnd) messages.scrollTop = messages.scrollHeight; else restorePosition(position);
        updateJump(); markRead(); notifyConversation();
      } catch (error) {
        if (!active()) return;
        if (closeAccess(error)) return;
        if (initial) messages.replaceChildren(emptyState('Разговор пока недоступен', errorText(error), button('Повторить', 'refresh', 'sw-button-quiet', () => { void load(); }), 'chat'));
        else errorMessage.textContent = errorText(error);
      } finally { fetching = false; }
    };
    messages.addEventListener('scroll', () => { if (Math.abs(messages.scrollTop - menuScrollTop) > 1) closeMenu(); updateJump(); if (!fetching && !historyLoading) { markRead(); if (messages.scrollTop < 64) void loadHistory(); } }, { passive: true });
    composer.addEventListener('submit', event => {
      event.preventDefault(); const text = input.value.trim(); if (!text || send.disabled) return;
      sending = true; const pending = this.chatDrafts.beginSend(accountId, group.communityId, text, replyTo); describeDraft(); updateComposer();
      errorMessage.textContent = ''; retrySend.hidden = true;
      void this.api.request<{ message: WorldMessage }>('world.chat.send', { communityId: group.communityId, clientId: pending.clientId, text, replyTo: pending.replyTo }).then(response => {
        const draft = this.chatDrafts.acknowledge(accountId, group.communityId, pending.clientId);
        if (!active()) return;
        const stickToEnd = nearEnd(), position = capturePosition();
        append(response.message); refreshTimeline(); input.value = draft.text; replyTo = draft.replyTo; refreshReply(); fitComposer(); describeDraft();
        if (stickToEnd) messages.scrollTop = messages.scrollHeight; else { restorePosition(position); unseen++; }
        updateJump(); notifyConversation(); void load();
      }).catch(reason => { if (active() && !closeAccess(reason)) { errorMessage.textContent = errorText(reason); retrySend.hidden = false; } }).finally(() => { sending = false; if (active()) { const returnToComposer = document.activeElement === input || document.activeElement === send || document.activeElement === retrySend; updateComposer(); if (returnToComposer && !input.disabled && !matchMedia('(pointer:coarse)').matches) input.focus({ preventScroll: true }); } });
    });
    const returned = (): void => { if (document.visibilityState === 'visible') { this.chatDrafts.retrySave(accountId, group.communityId); describeDraft(); void load(); } };
    document.addEventListener('visibilitychange', returned);
    const previousCleanup = this.chatCleanup;
    this.chatCleanup = () => { previousCleanup?.(); closeMenu(); unsubscribeDraft(); composerResize.disconnect(); document.removeEventListener('visibilitychange', returned); document.removeEventListener('pointerdown', outsideMenu); if (catchupTimer) clearTimeout(catchupTimer); for (const timer of highlightTimers) clearTimeout(timer); };
    void load(); this.chatTimer = setInterval(() => { void load(); }, 4500);
  }

  private renderMessages(selectedId?: string): void {
    const restoreHeadingFocus = document.activeElement instanceof HTMLElement && document.activeElement.matches('.sw-inbox-head h1');
    const groups = this.communities.filter(group => group.membership?.state === 'active'); const accountId = this.profile?.profileId ?? this.deskAccount;
    const selected = groups.find(group => group.communityId === selectedId);
    this.selectedChat = selected?.communityId;
    if (selectedId && !selected) { this.activeRoute = '#messages'; history.replaceState({ soty: true }, '', this.activeRoute); this.toast('Этот чат сейчас недоступен. Выберите другой разговор.'); }
    const sequence = this.screenSequence, previews = new Map<string, WorldMessage>(), conversationRows = new Map<string, HTMLButtonElement>(), unsubscribers = new Map<string, () => void>();
    const active = (): boolean => !this.destroyed && sequence === this.screenSequence && screen.isConnected;
    let selectedUpdate = 0, refreshing = false;
    const screen = el('section', `sw-messages-view sw-inbox${selected ? ' has-room' : ''}`); screen.setAttribute('aria-label', 'Чаты');
    const list = el('aside', 'sw-conversations'); list.setAttribute('aria-label', 'Список чатов');
    const top = el('div', 'sw-inbox-head'); const title = el('h1', '', 'Чаты'); title.tabIndex = -1; const contacts = iconButton('Контакты и приглашения', 'people', () => this.runHook(() => this.options.openAccount('people'))); top.append(title, contacts);
    const search = el('label', 'sw-inbox-search'); const query = textInput('', 'Поиск', 160); query.type = 'search'; query.setAttribute('aria-label', 'Поиск чатов'); const clearSearch = iconButton('Очистить поиск', 'close', () => { query.value = ''; renderList(); query.focus(); }); clearSearch.hidden = true; search.append(icon('search'), query, clearSearch);
    const filters = el('div', 'sw-inbox-filters'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Фильтр чатов'); let unreadOnly = false;
    const allFilter = button('Все', undefined, 'is-selected', () => { unreadOnly = false; renderList(); }); const unreadFilter = button('Непрочитанные', undefined, '', () => { unreadOnly = true; renderList(); }); filters.append(allFilter, unreadFilter);
    const conversations = el('div', 'sw-conversation-list'); const listStatus = el('div', 'sw-inbox-status'); listStatus.setAttribute('role', 'status');
    const footer = button('Найти сообщество', 'world', 'sw-inbox-discover', () => this.findCommunities());
    list.append(top, search, filters, conversations, listStatus, footer);
    const room = el('div', 'sw-message-room');
    const paintConversation = (group: WorldCommunity): HTMLButtonElement => {
      let target = conversationRows.get(group.communityId);
      if (!target) { target = el('button', 'sw-conversation'); target.type = 'button'; target.dataset.communityId = group.communityId; target.addEventListener('click', event => { if (this.selectedChat === group.communityId) return; this.writeRoute(`messages/${group.communityId}`); this.cleanScreen(); this.renderMessages(group.communityId); if (event.detail === 0) this.main.querySelector<HTMLButtonElement>('.sw-chat-identity')?.focus({ preventScroll: true }); }); conversationRows.set(group.communityId, target); }
      target.classList.toggle('is-selected', selectedId === group.communityId); target.classList.toggle('is-muted', !!group.membership?.muted); target.setAttribute('aria-current', selectedId === group.communityId ? 'true' : 'false');
      const draft = this.chatDrafts.read(accountId, group.communityId), message = previews.get(group.communityId);
      const copy = el('span', 'sw-conversation-copy'); const head = el('span', 'sw-conversation-head'); const name = el('strong', '', group.name); head.append(name);
      if (message) { const time = el('time', '', chatListTime(message.createdAt)); time.dateTime = new Date(message.createdAt).toISOString(); head.append(time); }
      const line = el('span', `sw-conversation-preview${draft.text ? ' is-draft' : ''}`);
      if (draft.text) line.append(el('span', 'sw-conversation-draft-label', 'Черновик: '), document.createTextNode(draft.text.replace(/\s+/gu, ' ')));
      else if (message) line.textContent = `${message.author.profileId === accountId ? 'Вы' : message.author.displayName.split(' ')[0]}: ${chatPreview(message)}`;
      else line.textContent = nounCount(group.memberCount, 'участник', 'участника', 'участников');
      const sub = el('span', 'sw-conversation-sub'); sub.append(line); const indicators = el('span', 'sw-conversation-indicators');
      if (group.membership?.pinned) { const pin = icon('pin'); pin.setAttribute('aria-hidden', 'false'); pin.setAttribute('aria-label', 'Закреплён'); indicators.append(pin); }
      if (group.membership?.muted) { const muted = icon('bell'); muted.setAttribute('aria-hidden', 'false'); muted.setAttribute('aria-label', 'Уведомления выключены'); indicators.append(muted); }
      if (group.unreadCount > 0) { const count = el('span', 'sw-message-count', group.unreadCount > 99 ? '99+' : String(group.unreadCount)); count.setAttribute('aria-label', nounCount(group.unreadCount, 'непрочитанное сообщение', 'непрочитанных сообщения', 'непрочитанных сообщений')); indicators.append(count); }
      sub.append(indicators); copy.append(head, sub); target.replaceChildren(communityEmblem(group), copy); return target;
    };
    const renderList = (): void => {
      const needle = query.value.trim().toLocaleLowerCase('ru-RU'); clearSearch.hidden = !query.value;
      allFilter.classList.toggle('is-selected', !unreadOnly); unreadFilter.classList.toggle('is-selected', unreadOnly); allFilter.setAttribute('aria-pressed', String(!unreadOnly)); unreadFilter.setAttribute('aria-pressed', String(unreadOnly));
      const visible = groups.filter(group => (!unreadOnly || group.unreadCount > 0) && (!needle || `${group.name} ${group.description} ${group.topics.join(' ')}`.toLocaleLowerCase('ru-RU').includes(needle))).sort((a, b) => Number(!!b.membership?.pinned) - Number(!!a.membership?.pinned) || (previews.get(b.communityId)?.createdAt ?? 0) - (previews.get(a.communityId)?.createdAt ?? 0));
      const focused = document.activeElement instanceof HTMLButtonElement && conversations.contains(document.activeElement) ? document.activeElement.dataset.communityId : undefined;
      conversations.replaceChildren(...visible.map(paintConversation)); listStatus.textContent = '';
      if (!groups.length) conversations.append(emptyState('Пока нет чатов', 'Создайте чат для своих или найдите сообщество.', button('Новый чат', 'plus', 'sw-button-primary', () => this.openCommunityForm(undefined, true)), 'chat'));
      else if (!visible.length) listStatus.textContent = needle ? 'Чат не найден' : 'Все сообщения прочитаны';
      if (focused) conversationRows.get(focused)?.focus({ preventScroll: true });
    };
    query.addEventListener('input', renderList); query.addEventListener('keydown', event => { if (event.isComposing || event.keyCode === 229) return; if (event.key === 'Escape' && query.value) { event.preventDefault(); clearSearch.click(); } else if (event.key === 'ArrowDown') { event.preventDefault(); conversations.querySelector<HTMLButtonElement>('button')?.focus(); } });
    conversations.addEventListener('keydown', event => { if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return; const items = Array.from(conversations.querySelectorAll<HTMLButtonElement>('.sw-conversation')); const index = items.indexOf(document.activeElement as HTMLButtonElement); if (index < 0) return; event.preventDefault(); items[event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : (index + (event.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length]?.focus({ preventScroll: true }); });
    screen.addEventListener('soty:chat-updated', event => { const detail = (event as CustomEvent<{ community: WorldCommunity; message?: WorldMessage; readAcknowledged?: boolean }>).detail; const index = groups.findIndex(group => group.communityId === detail.community.communityId); if (index < 0) return; groups[index] = detail.community; if (detail.community.communityId === selectedId && detail.readAcknowledged) selectedUpdate++; if (detail.message) previews.set(detail.community.communityId, detail.message); renderList(); });
    const subscribeDrafts = (): void => { for (const group of groups) if (!unsubscribers.has(group.communityId)) unsubscribers.set(group.communityId, this.chatDrafts.subscribe(accountId, group.communityId, () => { if (!active()) return; const current = groups.find(item => item.communityId === group.communityId); if (current && conversationRows.get(group.communityId)?.isConnected) paintConversation(current); })); };
    subscribeDrafts();
    const previousCleanup = this.chatCleanup;
    let inboxTimer: ReturnType<typeof setInterval> | undefined;
    const returned = (): void => { if (document.visibilityState === 'visible') void refreshInbox(); };
    this.chatCleanup = () => { previousCleanup?.(); unsubscribers.forEach(unsubscribe => unsubscribe()); if (inboxTimer) clearInterval(inboxTimer); document.removeEventListener('visibilitychange', returned); };
    screen.append(list, room); this.main.replaceChildren(screen);
    renderList();
    if (restoreHeadingFocus) title.focus({ preventScroll: true });
    if (selected) this.mountChat(room, selected, true);
    else room.append(emptyState('Ближе к своим', 'Выберите чат, чтобы продолжить разговор.', undefined, 'chat'));
    const pendingPreviews = new Set(groups.filter(group => group.communityId !== selectedId).map(group => group.communityId)); let previewWorkers = 0;
    const loadPreviews = async (): Promise<void> => {
      previewWorkers++;
      try { while (active() && pendingPreviews.size) {
        const id = pendingPreviews.values().next().value; if (!id) break; pendingPreviews.delete(id);
        if (!groups.some(group => group.communityId === id)) continue;
        try { const response = await this.api.request<{ messages: WorldMessage[] }>('world.chat.list', { communityId: id, limit: 1 }); if (!active()) return; if (!groups.some(group => group.communityId === id)) continue; const message = response.messages[0]; if (message) previews.set(id, message); renderList(); }
        catch { /* A community remains usable when its preview could not refresh. */ }
      } } finally { previewWorkers--; }
    };
    const startPreviews = (): void => { while (active() && previewWorkers < 3 && pendingPreviews.size) void loadPreviews(); };
    const visibleConversation = (id: string): boolean => { const target = conversationRows.get(id); if (!target?.isConnected || !conversations.clientHeight || !conversations.getClientRects().length) return false; const bounds = conversations.getBoundingClientRect(), row = target.getBoundingClientRect(); return row.bottom > bounds.top && row.top < bounds.bottom; };
    const refreshVisiblePreviews = (): void => { if (!active()) return; for (const group of groups) if (group.communityId !== selectedId && visibleConversation(group.communityId)) pendingPreviews.add(group.communityId); startPreviews(); };
    conversations.addEventListener('scroll', refreshVisiblePreviews, { passive: true });
    const refreshInbox = async (): Promise<void> => {
      if (refreshing || !active() || document.visibilityState === 'hidden') return; refreshing = true;
      const updateBefore = selectedUpdate;
      try {
        const response = await this.api.request<{ communities: WorldCommunity[] }>('world.community.list', {}); if (!active()) return;
        const old = new Map(groups.map(group => [group.communityId, group])); const next = response.communities.filter(group => group.membership?.state === 'active');
        for (const group of next) {
          const previous = old.get(group.communityId);
          if (previous?.membership && group.membership && previous.membership.revision > group.membership.revision) group.membership = previous.membership;
          if (group.communityId === selectedId && selectedUpdate !== updateBefore && previous) group.unreadCount = previous.unreadCount;
          // Removal and an author's rename need not change the unread count or
          // community revision. Refresh visible previews without polling every
          // offscreen community; the existing three workers bound concurrency.
          if (group.communityId !== selectedId && (!previous || previous.unreadCount !== group.unreadCount || previous.revision !== group.revision || visibleConversation(group.communityId))) pendingPreviews.add(group.communityId);
        }
        this.communities = response.communities; groups.splice(0, groups.length, ...next);
        for (const [id, unsubscribe] of unsubscribers) if (!next.some(group => group.communityId === id)) { unsubscribe(); unsubscribers.delete(id); previews.delete(id); conversationRows.delete(id); pendingPreviews.delete(id); }
        subscribeDrafts(); renderList(); startPreviews();
        screen.dispatchEvent(new CustomEvent('soty:inbox-updated', { bubbles: true, detail: { communities: response.communities } }));
        if (selectedId) room.querySelector('.sw-chat')?.dispatchEvent(new CustomEvent('soty:chat-community', { detail: { communityId: selectedId, community: next.find(group => group.communityId === selectedId) ?? null } }));
        const notify = next.find(group => group.communityId === selectedId) ?? next[0]; if (notify) screen.dispatchEvent(new CustomEvent('soty:chat-updated', { bubbles: true, detail: { community: notify } }));
      } catch { /* Existing conversations and drafts remain usable while metadata is offline. */ }
      finally { refreshing = false; }
    };
    startPreviews(); inboxTimer = setInterval(() => { void refreshInbox(); }, 6000); document.addEventListener('visibilitychange', returned);
  }

  private async loadPersonal(only?: HomeSection): Promise<void> {
    const sequence = this.screenSequence, request = ++this.homeRequest, accountCurrent = this.accountTask();
    const sections: HomeSection[] = ['devices', 'apps', 'communities', 'notes'];
    const includes = (section: HomeSection): boolean => !only || only === section;
    for (const section of sections) if (includes(section)) this.homeStatus[section] = 'loading';
    this.renderPersonal();
    const results = await Promise.allSettled([
      includes('devices') ? this.options.listDevices ? this.options.listDevices() : this.api.request<{ devices: DeviceProjection[] }>('apps.devices', {}).then(result => result.devices.map(item => ({ deviceId: item.hostDeviceId, label: item.name, state: item.online ? 'online' : 'offline' }))) : Promise.resolve(this.devices),
      includes('apps') ? this.loadApps() : Promise.resolve(this.apps),
      includes('communities') ? this.api.request<{ communities: WorldCommunity[] }>('world.community.list', {}) : Promise.resolve({ communities: this.communities }),
      includes('notes') ? this.api.request<{ notes: HomeNote[] }>('notes.list', { expectedAccountId: this.deskAccount, bucket: 'active', limit: 3 }) : Promise.resolve({ notes: this.homeNotes }),
    ]);
    if (!accountCurrent() || request !== this.homeRequest || this.view !== 'mine' || sequence !== this.screenSequence || this.group) return;
    sections.forEach((section, index) => { if (includes(section)) this.homeStatus[section] = results[index]?.status === 'fulfilled' ? 'ready' : 'error'; });
    const devices = results[0]; const apps = results[1];
    if (devices.status === 'fulfilled') this.devices = devices.value;
    if (apps.status === 'fulfilled') this.apps = apps.value;
    if (results[2].status === 'fulfilled') this.communities = results[2].value.communities;
    if (results[3].status === 'fulfilled') this.homeNotes = results[3].value.notes;
    this.renderPersonal();
  }

  private renderPersonal(): void {
    if (this.deskAccount && !this.group && !this.appStage) { this.renderCurrent(); void this.unifiedField?.refresh().catch(reason => this.toast(errorText(reason), true)); return; }
    this.field?.destroy(); this.field = null;
    const groups = this.communities.filter(group => ['active', 'invited', 'requested'].includes(group.membership?.state ?? ''));
    const makeApp = (app: WorldAppRecord, featured = false): AppCardData => ({
      id: app.appId, title: app.name, description: app.description || app.audience || 'Приложение в Сотах',
      ...(app.coverKey?{coverKey:app.coverKey}:{}), symbol: app.symbol || 'grid', featured,
      own: app.ownerAccountId === this.profile?.profileId, shared: Boolean(app.communityId || app.grants?.communityIds.length || app.grants?.accountIds.length || app.ownerAccountId && app.ownerAccountId !== this.profile?.profileId),
      ...(app.status === 'ready' ? {} : {status:app.status === 'offline' ? 'Не в сети' : this.appStateLabel(app.status)}),
      actionLabel: this.desk.recent.some(recent => recent.route === `app/${app.appId}`) ? 'Продолжить' : 'Открыть',
      open: () => { void this.openApplication(app); }, settings: () => this.openAppCardActions(app), menuLabel: `Действия: ${app.name}`,
    });
    const notes: AppCardData = { id:'builtin-notes', title:'Записки', description:'Мысли, которые останутся', coverKey:'notes', symbol:'list', own:true, open:()=>this.openNotes() };
    const chess: AppCardData = { id:'builtin-chess', title:'Шахматы', description:'Хороший повод встретиться', coverKey:'chess', symbol:'game', open:()=>this.runHook(()=>this.options.openLegacy('chess')) };
    const orderedApps = [...this.apps].sort((a,b) => Number(this.homeState.pinned.has(b.appId)) - Number(this.homeState.pinned.has(a.appId)));
    const featured = orderedApps.slice(0,2).map(app=>makeApp(app,true)), remaining = orderedApps.slice(2).map(app=>makeApp(app));
    if (!featured.length && this.homeStatus.apps!=='loading') featured.push({ id:'add-first-project', title:'Ваше приложение', description:'Добавьте проект с компьютера', coverKey:'hive', symbol:'plus', featured:true, actionLabel:'Добавить', open:()=>this.openAddApp() }, { ...notes, featured:true, actionLabel:'Записать' });
    const cards: AppCardData[] = [...featured, ...(!featured.some(card=>card.id===notes.id)?[notes]:[]), ...remaining.slice(0,1), chess, ...remaining.slice(1)];
    const activeSearch=document.activeElement instanceof HTMLInputElement && document.activeElement.closest('.sx-mobile-search') && this.main.contains(document.activeElement) ? document.activeElement : null;
    if(activeSearch?.dataset.composing==='true'){activeSearch.addEventListener('compositionend',()=>{if(activeSearch.isConnected&&this.view==='mine')this.renderPersonal();},{once:true});return;}
    const selection=activeSearch?{start:activeSearch.selectionStart,end:activeSearch.selectionEnd,direction:activeSearch.selectionDirection}:null;
    this.main.replaceChildren(createHome({
      cards, query:this.homeQuery, filter:this.homeFilter, presentation:this.preferences.homePresentation,
      communities:groups.map(group=>({ id:group.communityId, name:group.name, unread:group.unreadCount, open:()=>{void this.openGroup(group.communityId,'apps');}, people:group.previewMembers.map(person=>({name:person.displayName,...(person.avatarUrl?{avatarUrl:person.avatarUrl}:{}),profileId:person.profileId,...(person.avatarRevision!==undefined?{avatarRevision:person.avatarRevision}:{})})) })),
      loading:this.homeStatus.apps==='loading', errors:Object.entries(this.homeStatus).filter(([,value])=>value==='error').map(([key])=>({apps:'Приложения не обновлены',devices:'Устройства не обновлены',communities:'Сообщества не обновлены',notes:'Записки не обновлены'} as Record<string,string>)[key]!),
      create:()=>this.openAddApp(), discover:()=>this.navigate('world'), saved:()=>this.openSavedLibrary(), searchWorld:query=>this.searchEverything(query), assistant:()=>this.navigate('assistant'), retry:()=>{void this.loadPersonal();},
      changeFilter:value=>{this.homeFilter=value;}, changeQuery:value=>{this.homeQuery=value;},
      changePresentation:value=>{const restoreFocus = document.activeElement instanceof HTMLElement && Boolean(document.activeElement.closest('.sx-presentation'));this.preferences.homePresentation=value;this.persist();this.renderPersonal();if(restoreFocus)this.main.querySelector<HTMLButtonElement>('.sx-presentation button[aria-pressed=true]')?.focus({preventScroll:true});},
      unmountField:()=>{this.field?.destroy();this.field=null;},
      mountField:(host,visibleCards)=>{
        const ids=new Set(visibleCards.map(card=>card.id));const shownApps=this.apps.filter(app=>ids.has(app.appId));
        const builtinApps:WorldAppRecord[]=[];if(ids.has('builtin-notes'))builtinApps.push({appId:'builtin:notes',name:'Записки',symbol:'list',coverKey:'notes',status:'ready'});if(ids.has('builtin-chess'))builtinApps.push({appId:'builtin:chess',name:'Шахматы',symbol:'game',coverKey:'chess',status:'ready'});
        const shownGroups=visibleCards.length===cards.length?groups:groups.filter(group=>shownApps.some(app=>app.communityId===group.communityId||app.grants?.communityIds.includes(group.communityId)));
        const placedApps=shownApps.map(app=>{if(app.communityId)return app;const group=shownGroups.find(value=>app.grants?.communityIds.includes(value.communityId));return group?{...app,communityId:group.communityId}:app;});
        this.field = createHexField(entity=>{void this.previewPersonalEntity(entity);}, this.preferences.scale, { state: this.homeFieldState, onSelectApp:app=>{if(app.appId==='builtin:notes')this.openNotes();else if(app.appId==='builtin:chess')this.runHook(()=>this.options.openLegacy('chess'));else void this.openApplication(app);}, resolveAppArt:app=>resolveAppArt(app).srcset.split(',')[0]?.trim().split(' ')[0]||resolveAppArt(app).src, onScaleChange:scale=>{this.preferences.scale=scale;this.persist();} });
        const visiblePeople = new Map(shownGroups.flatMap(group => group.previewMembers).map(person => [person.profileId, person]));
        const entities: WorldEntity[] = [...shownGroups.map(value => ({type:'community' as const,value})), ...Array.from(visiblePeople.values(), value => ({type:'person' as const,value}))];
        host.append(this.field.element); this.field.update(entities, ''); this.field.setApps([...placedApps,...builtinApps]);
        const controls=el('div','sx-personal-field-controls');controls.append(iconButton('Уменьшить поле','minus',()=>this.changeScale(-.12)),iconButton('Увеличить поле','plus',()=>this.changeScale(.12)),iconButton('Вернуть обзор поля','refresh',()=>this.field?.resetView()));host.append(controls);
      },
    }));
    if(activeSearch){const search=this.main.querySelector<HTMLInputElement>('.sx-mobile-search input');search?.focus({preventScroll:true});if(search&&selection?.start!==null&&selection?.end!==null){try{search.setSelectionRange(selection?.start??0,selection?.end??0,selection?.direction??'none');}catch{/* Unsupported selection APIs do not interrupt navigation. */}}}
  }

  private async previewPersonalEntity(entity: WorldEntity): Promise<void> {
    if(entity.type==='community') await this.openGroup(entity.value.communityId,'apps');
    else this.openPersonDialog(entity.value);
  }

  private openRecent(): void {
    const dialog = this.dialog('Недавнее'); const list = el('div', 'sx-profile-menu');
    for (const item of this.desk.recent) list.append(button(item.title, item.symbol, 'sw-button-quiet', () => { dialog.close(); this.writeRoute(item.route); void this.openRoute(); }));
    if (!this.desk.recent.length) list.append(el('p', 'sw-muted', 'Здесь появятся открытые приложения и записки'));
    else list.append(button('Очистить недавнее', 'close', 'sw-button-quiet', () => { this.desk.recent = []; this.saveDesk(); dialog.close(); if (this.view === 'mine') this.renderPersonal(); }));
    dialog.body.append(list);
  }

  private inspectApplication(app: WorldAppRecord): void {
    const returnTarget = this.appDialogReturnTarget(app.appId);
    const dialog = this.dialog(app.name, undefined, returnTarget); const body = el('div', 'sw-stack');
    const status = el('p', 'sw-muted', this.appStateLabel(app.status));
    const audience = describeAppAudience(app, this.deskAccount);
    body.append(status, ...(audience.details.length ? audience.details : [audience.label]).map(text => el('p', '', text)));
    if (app.deviceLabel) body.append(button(app.deviceLabel, 'laptop', 'sw-button-quiet', () => { dialog.close({ restoreFocus: false }); this.openResources('devices'); }));
    for (const group of this.communities.filter(value => value.membership?.state === 'active' && (value.communityId === app.communityId || app.grants?.communityIds.includes(value.communityId)))) {
      body.append(button(group.name, 'people', 'sw-button-quiet', () => { dialog.close({ restoreFocus: false }); void this.openGroup(group.communityId); }));
    }
    body.append(button('Открыть приложение', 'arrow', 'sw-button-primary', () => { dialog.close({ restoreFocus: false }); void this.openApplication(app); }));
    if (app.ownerAccountId === this.profile?.profileId) body.append(button('Название и доступ', 'settings', 'sw-button-quiet', () => {
      dialog.close({ restoreFocus: false }); this.openAppSettings(app, undefined, returnTarget);
    }));
    dialog.body.append(body);
  }

  private saveDesk(selectedSpace = false): void { if (this.deskAccount) saveDeskPreferences(this.deskAccount, this.desk, { selectedSpace }); }
  private remember(route: string, title: string, symbol: string): void { this.desk.recent = [{ route, title: title.slice(0, 100), symbol }, ...this.desk.recent.filter(item => item.route !== route)].slice(0, 6); this.saveDesk(); }
  private searchCount(totals: WorldSearch['totals']): string { return `${totals.communitiesExact === false ? `${totals.communities}+ сообществ` : nounCount(totals.communities, 'сообщество', 'сообщества', 'сообществ')} · ${totals.peopleExact === false ? `${totals.people}+ человек` : nounCount(totals.people, 'человек', 'человека', 'человек')}`; }

  private openResources(kind: 'all' | 'apps' | 'devices' = 'all'): void {
    const dialog = this.dialog(kind === 'apps' ? 'Приложения' : kind === 'devices' ? 'Устройства' : 'Мои подключения'); const list = el('div', 'sw-stack'); dialog.body.append(list, this.loading('Проверяем доступ'));
    const accountCurrent = this.accountTask(), sequence = this.screenSequence;
    const current = (): boolean => accountCurrent() && sequence === this.screenSequence && dialog.element.open && dialog.element.isConnected;
    void Promise.all([
      kind === 'devices' ? Promise.resolve(this.apps) : this.loadApps(),
      kind === 'apps' ? Promise.resolve(this.devices) : this.options.listDevices ? this.options.listDevices() : this.api.request<{ devices: DeviceProjection[] }>('apps.devices').then(result => result.devices.map(item => ({ deviceId: item.hostDeviceId, label: item.name, state: item.online ? 'online' : 'offline' }))),
    ]).then(([apps, devices]) => {
      if (!current()) return;
      if (kind !== 'devices') this.apps = apps;
      if (kind !== 'apps') this.devices = devices;
      dialog.body.replaceChildren(list);
      if (kind !== 'devices') { list.append(el('h3', '', 'Приложения')); for (const app of apps) list.append(button(app.name, 'grid', 'sw-button-large', () => { dialog.close(); void this.openApplication(app); })); list.append(button('Добавить приложение', 'plus', 'sw-button-quiet', () => { dialog.close(); this.openAddApp(); })); }
      if (kind !== 'apps') { list.append(el('h3', '', 'Устройства')); for (const device of devices) list.append(button(`${device.label} · ${device.state === 'online' ? 'В сети' : 'Не в сети'}`, 'laptop', 'sw-button-large', () => { dialog.close(); this.openDevice(device); })); list.append(button('Подключить устройство', 'plus', 'sw-button-quiet', () => { dialog.close(); this.runHook(this.options.connectDevice); })); }
    }).catch(error => { if (current()) dialog.body.replaceChildren(el('p', 'sw-muted', errorText(error))); });
  }

  private openCapability(id: CapabilityId): void {
    if (id === 'notes') this.openNotes();
    else if (id === 'apps') this.navigate('mine');
    else if (id === 'devices') this.openResources('devices');
    else if (id === 'agent') this.navigate('assistant');
    else if (id === 'communities') this.findCommunities();
    else if (id === 'contacts') this.runHook(() => this.options.openAccount('people'));
    else if (id === 'appearance') this.openAppearance();
    else this.runHook(() => this.options.openLegacy(id === 'legacy-notes' ? 'notes' : id));
  }

  private renderLibrary(): void {
    this.main.replaceChildren(createLibrary({ favorites: this.desk.favorites, open: id => this.openCapability(id), toggle: id => {
      const wasPinned = this.desk.favorites.includes(id);
      if (!wasPinned && this.desk.favorites.length >= 8) { this.toast('Можно закрепить до 8 возможностей'); return false; }
      this.desk.favorites = wasPinned ? this.desk.favorites.filter(value => value !== id) : [...this.desk.favorites, id]; this.saveDesk(); return !wasPinned;
    } }));
  }

  private openQuickActions(): void {
    this.afterNoteSaved(() => {
      if (document.querySelector('dialog[open]')) return;
      openCommandPalette([
      { title: 'Новая записка', detail: 'Сохранить мысль или список', symbol: 'plus', action: () => this.openNotes('new') },
      { title: 'Доступы и действия', detail: 'Разрешения и история', symbol: 'shield', action: () => this.navigate('access') },
      ...capabilities.map(item => ({ title: item.title, detail: item.detail, symbol: item.symbol, action: () => this.openCapability(item.id) })),
      ...this.apps.map(app => ({ title: app.name, detail: 'Приложение', symbol: 'grid', action: () => { this.writeRoute(`app/${app.appId}`); void this.openRoute(); } })),
      ...this.communities.map(group => ({ title: group.name, detail: 'Сообщество', symbol: 'people', action: () => { void this.openGroup(group.communityId, 'chat'); } })),
      ], this.dialog('Быстрый переход'), query => this.searchEverything(query));
    });
  }

  private openNotes(noteId?: string, push = true): void {
    if (push) this.writeRoute(`notes${noteId ? `/${noteId}` : ''}`);
    this.view = 'notes'; this.preferences.view = 'notes'; this.persist(); this.group = null; this.selected = null; this.cleanScreen(); this.renderNavigation(); this.renderNotes(noteId);
  }
  private renderNotes(noteId?: string): void {
    const host = el('section', 'sw-notes-host'); host.dataset.pwaIgnore = ''; this.main.replaceChildren(host); host.append(this.loading('Открываем записки'));
    const sequence = this.screenSequence, accountId = this.profile?.profileId || this.deskAccount; if (!accountId) return;
    void import('./notes').then(({ mountNotes }) => {
      if (this.destroyed || sequence !== this.screenSequence || !host.isConnected) return;
      this.notesHandle = mountNotes(host, { api: this.api, accountId, projectId: 'soty', initialNoteId: noteId, openLegacy: () => this.runHook(() => this.options.openLegacy('notes')), onOpenNote: (id: string, title?: string) => { this.activeRoute = `#notes/${id}`; history.replaceState({ soty: true }, '', this.activeRoute); if (id !== 'new') this.remember(`notes/${id}`, title || 'Записка', 'list'); } });
    }).catch(error => { if (host.isConnected) host.replaceChildren(emptyState('Не удалось открыть записки', errorText(error), button('Повторить', 'refresh', '', () => this.renderNotes(noteId)))); });
  }

  private openAppearance(): void {
    const controls = createThemeControls(this.theme); let unsubscribe = (): void => {};
    const dialog = this.dialog('Оформление', () => { controls.destroy(); unsubscribe(); }); dialog.body.append(controls.element);
    for (const [key, label] of [['compact', 'Компактный интерфейс'], ['motion', 'Плавные переходы']] as const) {
      const row = el('div', 'sw-setting-row'); row.append(el('span', '', label), switchControl(label, this.preferences[key], async next => { this.preferences[key] = next; this.root.dataset[key] = key === 'motion' ? next ? 'on' : 'off' : String(next); this.persist(); })); dialog.body.append(row);
    }
    const pwa = el('div', 'sw-pwa-status'); dialog.body.append(pwa);
    unsubscribe = this.pwa.subscribe(state => this.renderPwaSettings(pwa, state));
  }

  private renderPwaBanner(state: PwaState): void {
    const key = `${state.connection}:${state.update}`; if (this.pwaBannerKey === key) return; this.pwaBannerKey = key;
    const offline = state.connection === 'offline' || state.connection === 'unreachable';
    const update = state.update !== 'idle';
    this.pwaBanner.hidden = !offline && !update;
    if (this.pwaBanner.hidden) { this.pwaBanner.replaceChildren(); return; }
    const text = offline ? 'Нет связи с сервером · черновики доступны на устройстве' : state.update === 'blocked' ? 'Обновление ждёт сохранения открытой работы' : state.update === 'failed' ? 'Обновление не удалось. Можно повторить.' : state.update === 'saving' ? 'Сохраняем работу перед обновлением…' : state.update === 'activating' ? 'Обновляем Соты…' : 'Готова новая версия Сот';
    this.pwaBanner.replaceChildren(icon(offline ? 'activity' : 'refresh'), el('span', '', text));
    if (offline) this.pwaBanner.append(button('Проверить', 'refresh', 'sw-button-small', () => { void this.pwa.checkConnection(); }));
    else if (['available', 'blocked', 'failed'].includes(state.update)) this.pwaBanner.append(button('Обновить', 'refresh', 'sw-button-small', () => { void this.pwa.applyUpdate(); }));
  }
  private renderPwaSettings(host: HTMLElement, state: PwaState): void {
    const key = `${state.install}:${state.offlineReady}:${state.worker}`; if (host.dataset.state === key) return; host.dataset.state = key;
    const text = state.install === 'installed' ? 'Соты установлены' : state.install === 'accepted' ? 'Установка принята браузером' : 'Соты — приложение для ваших устройств';
    host.replaceChildren(icon('phone'), el('span', '', text));
    if (state.install === 'available') host.append(button('Установить', 'plus', 'sw-button-primary sw-button-small', () => { void this.pwa.requestInstall(); }));
    const details = el('small', '', state.offlineReady ? 'Оболочка сохранена для открытия без сети. Личные черновики остаются на этом устройстве.' : state.worker === 'failed' ? 'Браузер не сохранил оболочку для работы без сети.' : 'Офлайн-доступ появится после сохранения оболочки браузером.'); host.append(details);
    if (state.install === 'unavailable') host.append(el('small', '', 'Установка доступна через меню поддерживаемого браузера. На iPhone: «Поделиться» → «На экран Домой».'));
  }

  private syntheticEmblem(name: string, color: string, symbol: string): HTMLElement {
    return communityEmblem({ communityId: '', name, description: '', topics: [], joinPolicy: 'open', showcase: '', symbol, color, revision: 0, memberCount: 0, previewMembers: [], membership: null, permissions: { canManage: false, canModerate: false, canWrite: false }, unreadCount: 0 });
  }

  private openAddMenu(): void {
    if (this.unifiedField) { this.unifiedField.openAdd(); return; }
    if (this.view === 'messages' && !this.group) { this.openCommunityForm(undefined, true); return; }
    const dialog = this.dialog('Добавить'); const list = el('div', 'sx-profile-menu');
    const option = (title: string, symbol: string, action: () => void): void => list.append(button(title, symbol, 'sw-button-large', () => { dialog.close(); action(); }));
    option('Приложение', 'grid', () => this.openAddApp());
    option('Записку', 'note', () => this.openNotes('new'));
    option('Устройство', 'laptop', () => this.runHook(this.options.connectDevice));
    option('Сообщество', 'people', () => this.openCommunityForm());
    option('Создать с ИИ', 'sparkle', () => this.navigate('assistant'));
    option('Контакт', 'person', () => this.runHook(() => this.options.openAccount('people')));
    dialog.body.append(list);
  }

  private openDevice(device: WorldDevice): void {
    const dialog = this.dialog(device.label); const content = el('div', 'sw-stack'); const summary = el('div', 'sw-profile-large');
    summary.append(this.syntheticEmblem(device.label, device.state === 'online' ? 'sage' : 'blue', 'laptop'), heading(device.label));
    content.append(summary, badge(device.state === 'online' ? 'В сети' : 'Не в сети', 'activity', device.state === 'online' ? 'sage' : 'muted'));
    const apps = this.apps.filter(app => app.deviceId === device.deviceId);
    if (apps.length) apps.forEach(app => content.append(button(app.name, 'grid', '', () => { dialog.close(); void this.openApplication(app); })));
    else content.append(el('p', 'sw-muted', 'На устройстве пока нет добавленных приложений.'));
    content.append(button('Добавить приложение', 'plus', 'sw-button-primary', () => { dialog.close(); this.openAddApp(undefined, device.deviceId); }), button('Управление устройствами', 'settings', 'sw-button-quiet', () => { dialog.close(); this.runHook(() => this.options.openAccount('devices')); })); dialog.body.append(content);
  }

  private appStateLabel(status: string): string { return appStatusLabel(status); }

  private async loadApps(communityId?: string, includeCatalog = true): Promise<WorldAppRecord[]> {
    const current = this.accountTask(), accountId = this.deskAccount;
    if (!current()) throw Object.assign(new Error('No current account'), { code: 'authentication_required' });
    const personal = this.options.listApps ? null : (await this.api.request<{ apps: AppProjection[] }>('apps.list',
      { ...(communityId ? { communityId } : {}), expectedAccountId: accountId })).apps;
    let publicApps: AppProjection[] = [];
    if (includeCatalog && !this.options.listApps && !communityId) {
      try { publicApps = (await this.api.request<{ apps: AppProjection[] }>('apps.catalog', { expectedAccountId: accountId })).apps; }
      catch { if (current()) this.toast('Общий каталог пока недоступен. Ваши приложения доступны.'); }
    }
    const projections = personal ? [...personal.map(app => ({ ...app, ...(publicApps.find(value => value.id === app.id)?.entry
      ? { entry: publicApps.find(value => value.id === app.id)!.entry } : {}) })), ...publicApps.filter(app => !personal.some(value => value.id === app.id))] : [];
    const apps = this.options.listApps ? await this.options.listApps(communityId)
      : projections
        .map(app => {
          const record: WorldAppRecord = { appId: app.id, name: app.name, ...(app.hostDeviceId ? { deviceId: app.hostDeviceId } : {}),
            ...(app.deviceName ? { deviceLabel: app.deviceName } : {}), status: app.state, ownerAccountId: app.ownerAccountId,
            ...(communityId ? { communityId } : {}), ...(app.grants ? { grants: app.grants } : {}),
            ...(app.publication ? { publication: app.publication } : {}), ...(app.entry ? { entry: app.entry } : {}) };
          return { ...record, audience: describeAppAudience(record, accountId).label };
        });
    if (!current()) throw Object.assign(new Error('Identity changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
    return apps;
  }

  private async renderApps(parent: HTMLElement, group: WorldCommunity): Promise<void> {
    parent.replaceChildren(this.loading('Загружаем приложения'));
    const accountCurrent = this.accountTask(), sequence = this.screenSequence;
    const current = (): boolean => accountCurrent() && sequence === this.screenSequence && parent.isConnected;
    try {
      const apps = await this.loadApps(group.communityId); if (!current()) return;
      const stack = el('div', 'sw-stack');
      if (!apps.length) stack.append(emptyState('Что будем делать вместе?', group.permissions.canModerate ? 'Добавьте приложение со своего компьютера или создайте новое с ИИ.' : 'Здесь появятся приложения, которыми поделятся организаторы.', undefined, 'grid'));
      else {
        const cards = el('div', 'sw-app-cards');
        for (const app of apps) cards.append(createAppCard({
          id: app.appId, title: app.name, description: app.description || app.audience || group.name,
          symbol: app.symbol || 'grid', ...(app.coverKey ? { coverKey: app.coverKey } : {}),
          ...(app.status !== 'ready' ? { status: this.appStateLabel(app.status) } : {}),
          open: () => { this.group = group; void this.openApplication(app); },
          ...(app.ownerAccountId === this.profile?.profileId ? { settings: () => this.openAppSettings(app) } : {}),
        }));
        stack.append(cards);
      }
      if (group.membership?.state === 'active' && group.permissions.canModerate) { const actions = el('div', 'sw-row'); actions.append(button('Добавить приложение', 'plus', 'sw-button-primary', () => this.openAddApp(group.communityId)), button('Создать с ИИ', 'sparkle', '', () => this.runHook(() => this.options.agentCreate(group.communityId)))); stack.append(actions); }
      parent.replaceChildren(stack);
    } catch (error) { if (current()) parent.replaceChildren(emptyState('Приложения пока недоступны', errorText(error), button('Повторить', 'refresh', 'sw-button-quiet', () => { void this.renderApps(parent, group); }), 'grid')); }
  }

  private async openApplication(app: WorldAppRecord, intent?: AppLaunchIntent, resolveMetadata = false): Promise<void> {
    if (this.destroyed || !this.deskAccount) return;
    const returnView = this.appStage ? this.appReturnView : /^#world(?:\?|$)/u.test(this.activeRoute) ? 'world' : 'mine';
    this.appReturnView = returnView;
    const previousGroup = this.group?.membership?.state === 'active' && (this.group.communityId === app.communityId || app.grants?.communityIds.includes(this.group.communityId)) ? this.group : null;
    const launchIntent = intent ?? parseAppLaunchRoute(formatAppLaunchRoute({ appId: app.appId,
      ...(app.entry ? { domainId: app.entry.domainId, path: app.entry.path } : {}) }, app.entry ? undefined : previousGroup?.communityId))!;
    const communityId = launchIntent.communityId;
    const knownGroup = communityId ? [previousGroup, ...this.communities].find(value => value?.communityId === communityId && value.membership?.state === 'active') ?? null : null;
    this.group = knownGroup; this.view = 'mine'; this.renderNavigation(); this.writeRoute(launchIntent.route);
    this.avatars.setContext(knownGroup?.communityId);
    this.cleanScreen();
    const sequence = this.screenSequence, accountId = this.deskAccount, accountGeneration = this.accountGeneration;
    const current = (): boolean => !this.destroyed && this.screenSequence === sequence && this.deskAccount === accountId && this.accountGeneration === accountGeneration;
    const stage = mountAppStage(this.main, { api: this.api, app, accountId, intent: launchIntent, isCurrent: current,
      request: parameters => this.options.openApp ? this.options.openApp(app, parameters)
        : this.api.request<{ launchUrl: string; entry: AppResolvedEntry; launchBinding?: AppLaunchBinding }>('apps.launch', { ...parameters })
          .then(result => ({ url: result.launchUrl, entry: result.entry, ...(result.launchBinding === undefined ? {} : { launchBinding: result.launchBinding }) })),
      onNavigate: (next, navigation) => {
        if (!current()) return;
        if (navigation?.replace) { history.replaceState({ soty: true }, '', '#' + next.route); this.activeRoute = '#' + next.route; }
        else this.writeRoute(next.route);
      },
      onBack: () => this.afterNoteSaved(() => {
        if (!current()) return;
        if (this.group) { this.groupTab = 'apps'; this.renderGroup(); } else this.navigate(returnView);
      }),
      onAccount: async () => {
        await stage.flush();
        if (!current()) return;
        if (stage.hasUnsavedChanges()) { this.toast('Скопируйте черновик перед сменой аккаунта', true); return; }
        await this.options.openAccount('recovery'); if (current()) await this.refresh();
      },
      onSettings: (value, onUpdated) => { if (current()) this.openAppSettings(value, onUpdated); },
      onPlaceInField: value => { if (current()) this.openAppFieldPlacement(value, contextId => {
        if (!current()) return;
        this.unifiedFieldView.mineContext = contextId; this.unifiedFieldView.mineFit = 'context'; this.unifiedFieldView.mineSelection = '';
        this.desk.lastSpace = contextId; this.saveDesk(true);
      }); },
      onCommunity: id => this.afterNoteSaved(() => { if (current()) void this.openGroup(id, 'chat'); }),
      onRemember: (next, value) => { if (current()) this.remember(next.route, value.name, 'grid'); },
    });
    this.appStage = stage;
    if (knownGroup) stage.updateCommunity(knownGroup);
    // The exact entry, including the offline fallback, settles before optional
    // catalogue requests join the serialized Connect queue.
    await stage.ready;
    if (!current()) return;
    if (communityId && !knownGroup) void this.api.request<{ community: WorldCommunity }>('world.community.get', { communityId })
      .then(result => {
        if (!current() || result.community.membership?.state !== 'active') return;
        this.group = result.community; this.renderNavigation(); this.avatars.setContext(communityId); stage.updateCommunity(result.community);
      }).catch(() => { /* App admission does not grant or require community chat access. */ });
    if (resolveMetadata) void this.loadApps(communityId, !!launchIntent.target.domainId).then(apps => {
      if (!current()) return;
      const found = apps.find(value => value.appId === launchIntent.target.appId);
      if (found) { app = found; stage.updateApp(found); }
    }).catch(() => { /* Optional names never change the exact admitted entry. */ });
  }
  private openAddApp(communityId?: string, selectedDevice?: string): void {
    const accountCurrent = this.accountTask(), sequence = this.screenSequence;
    const dialog = this.dialog('Добавить приложение'); dialog.body.append(this.loading('Ищем ваши устройства'));
    const current = (): boolean => accountCurrent() && sequence === this.screenSequence && dialog.element.open;
    void this.api.request<{ devices: DeviceProjection[] }>('apps.devices', {}).then(result => {
      if (!current()) return;
      const devices = result.devices.filter(device => device.claimed);
      if (!devices.length) { dialog.body.replaceChildren(emptyState('Подключите компьютер', 'Проект будет работать на вашем устройстве и открываться здесь.', button('Подключить устройство', 'laptop', 'sw-button-primary', () => { dialog.close(); this.runHook(this.options.connectDevice); }), 'laptop')); return; }
      const form = el('form'); const name = textInput('', 'Например, Галерея выходных', 64); name.required = true;
      const device = el('select', 'sw-select'); devices.forEach(item => { const option = el('option', '', `${item.name}${item.online ? '' : ' · не в сети'}`); option.value = item.hostDeviceId; option.selected = item.hostDeviceId === selectedDevice; device.append(option); });
      const port = el('input', 'sw-input'); port.type = 'number'; port.min = '1024'; port.max = '65535'; port.placeholder = '3000'; port.required = true;
      const path = textInput('/', '/'); path.pattern = '/.*';
      const audience = el('select', 'sw-select'); const privateOption = el('option', '', 'Только мне'); privateOption.value = ''; audience.append(privateOption);
      this.communities.filter(group => group.membership?.state === 'active' && group.permissions.canModerate).forEach(group => { const option = el('option', '', group.name); option.value = group.communityId; option.selected = group.communityId === communityId; audience.append(option); });
      const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const save = button('Подключить проект', 'plus', 'sw-button-primary sw-button-wide'); save.type = 'submit';
      let registering = false;
      form.append(labeledField('Название', name), labeledField('Устройство', device), labeledField('Порт проекта', port, 'Проект уже должен работать на этом компьютере.'), labeledField('Начальная страница', path), labeledField('Кому открыть', audience), error, save);
      form.addEventListener('submit', event => {
        event.preventDefault(); const chosen = devices.find(item => item.hostDeviceId === device.value);
        if (!current() || registering || !chosen || !form.reportValidity()) return;
        registering = true; save.disabled = true; error.textContent = '';
        for (const control of [name, device, port, path, audience]) control.disabled = true;
        // The registry returns the canonical identity. A field placement is a
        // separate private choice, never proof of publication or a new grant.
        void this.api.request<{ app: { id: string; name: string } }>('apps.register', { hostDeviceId: chosen.hostDeviceId, connectorId: chosen.connectorId, name: name.value.trim(), port: Number(port.value), entryPath: path.value.trim() || '/', grants: { accountIds: [], communityIds: audience.value ? [audience.value] : [] } }).then(result => {
          if (!current()) return;
          if (!/^app-[a-f0-9]{32}$/.test(result.app.id)) throw new Error('invalid_registered_app');
          dialog.close({ restoreFocus: false }); this.toast('Проект подключён');
          // Registration changes the owner's available directory even when
          // the independent placement chooser is closed without adding a cell.
          const field = this.unifiedField;
          const fieldCurrent = (): boolean => accountCurrent() && sequence === this.screenSequence && field === this.unifiedField;
          if (field) void field.ready.then(async () => {
            if (fieldCurrent()) await field.refresh({ preserveView: true });
          }).catch(reason => { if (fieldCurrent()) this.toast(errorText(reason), true); });
          this.openAppFieldPlacement({ appId: result.app.id, name: result.app.name }, contextId => {
            if (!accountCurrent()) return;
            this.homeQuery = ''; this.unifiedFieldFilters.mine = 'all';
            this.navigate('mine'); const field = this.unifiedField;
            const showPlacement = (): boolean => accountCurrent() && field === this.unifiedField && this.view === 'mine' && !this.group && !this.appStage;
            void field?.ready.then(async () => {
              if (!showPlacement()) return;
              await field.refresh();
              if (showPlacement()) field.focusContext(contextId);
            }).catch(reason => { if (showPlacement()) this.toast(errorText(reason), true); });
          });
        }).catch(reason => { if (current()) error.textContent = errorText(reason); })
          .finally(() => { registering = false; if (current()) { save.disabled = false; for (const control of [name, device, port, path, audience]) control.disabled = false; } });
      }); dialog.body.replaceChildren(form); name.focus();
    }).catch(error => { if (current()) dialog.body.replaceChildren(el('div', 'sw-error', errorText(error))); });
  }

  private openAppFieldPlacement(app: Pick<WorldAppRecord, 'appId' | 'name'>, onPlaced?: (contextId: string) => void): void {
    const accountId = this.deskAccount, sequence = this.screenSequence, accountCurrent = this.accountTask();
    if (this.destroyed || !accountId || this.appPlacementDialog?.element.open || !/^app-[a-f0-9]{32}$/.test(app.appId)) return;
    let placement: AppFieldPlacementController | null = null, busy = false;
    let pendingClose: (() => void) | null = null;
    const dialog = this.dialog('На моё поле', () => {
      placement?.dispose();
      if (this.appPlacementDialog === dialog) { this.appPlacementDialog = null; this.appPlacementController = null; this.appPlacementRouteClose = null; }
    }); this.appPlacementDialog = dialog;
    const current = (): boolean => accountCurrent() && sequence === this.screenSequence && dialog.element.open;
    const error = el('p', 'sw-error'); error.setAttribute('role', 'status');
    const stack = el('div', 'sw-stack'), retry = button('Проверить ещё раз', 'refresh', 'sw-button-quiet');
    const finishClose = (resume?: () => void): void => {
      const allowed = accountCurrent() && sequence === this.screenSequence;
      dialog.close({ restoreFocus: !resume }); if (allowed) resume?.();
    };
    const leave = button('Закрыть всё равно', 'close', 'sw-button-quiet', () => { if (pendingClose) pendingClose(); else finishClose(); }); leave.hidden = true;
    const copy = el('p', 'sw-muted', app.name), hint = el('p', 'sw-muted', 'Выберите пространство на вашем поле.');
    dialog.body.append(copy, hint, stack, error, retry, leave); retry.hidden = true;
    const report = (code?: string): void => {
      if (!current()) return;
      error.textContent = ['field_revision_conflict', 'field_workspace_changed', 'app_field_selection_changed', 'field_context_missing'].includes(code ?? '')
        ? 'Поле изменилось. Проверьте пространства перед добавлением.'
        : code === 'field_local_storage_unavailable' || code === 'field_local_capacity'
          ? 'Выбор ещё не сохранён. Освободите место на устройстве и повторите.'
          : 'Не удалось подтвердить сохранение. Проверьте поле ещё раз.';
      retry.hidden = false;
      leave.hidden = !placement?.hasUnsavedChanges();
    };
    const render = (snapshot: AppFieldPlacementSnapshot): void => {
      if (!current()) return;
      stack.replaceChildren();
      leave.hidden = !placement?.hasUnsavedChanges();
      const blocked = snapshot.pendingCount > 0 || snapshot.persistence === 'conflict' || !snapshot.localDurable;
      for (const context of snapshot.contexts) {
        const choose = button(context.title, context.placed ? 'check' : 'plus', 'sw-button-quiet sw-button-wide', () => {
          if (!current() || busy || !placement) return;
          busy = true; error.textContent = ''; retry.hidden = true;
          stack.querySelectorAll('button').forEach(control => { control.disabled = true; });
          void placement.place({ contextId: context.contextId, generation: snapshot.generation }).then(result => {
            if (!current()) return;
            if (result.status === 'saved' && result.shortcutId) {
              dialog.close(); this.toast(result.changed ? `На поле «${context.title}»` : `Уже на поле «${context.title}»`);
              if (accountCurrent() && sequence === this.screenSequence) onPlaced?.(context.contextId);
            } else if (result.status === 'pending' && result.shortcutId && result.snapshot.localDurable) {
              dialog.close(); this.toast('Сота сохранена на устройстве. Ожидает синхронизации.'); this.syncFieldOutbox();
              if (accountCurrent() && sequence === this.screenSequence) onPlaced?.(context.contextId);
            } else { render(result.snapshot); report(result.errorCode); }
          }).catch(reason => { if (current()) report(String(reason?.code ?? '')); })
            .finally(() => { busy = false; if (current()) { const latest = placement?.getSnapshot(); if (latest) render(latest); } });
        });
        choose.disabled = busy || blocked;
        choose.setAttribute('aria-label', context.placed ? `Уже в пространстве «${context.title}»` : `Добавить в пространство «${context.title}»`);
        if (context.placed) choose.append(el('small', 'sw-muted', 'Уже здесь'));
        else if (context.willCreate) choose.append(el('small', 'sw-muted', 'Создать пространство'));
        stack.append(choose);
      }
      if (blocked || snapshot.errorCode) report(snapshot.errorCode);
    };
    const requestClose = (resume?: () => void): void => {
      if (!current()) return;
      if (placement?.hasUnsavedChanges()) { pendingClose = () => finishClose(resume); report('field_local_storage_unavailable'); return; }
      if (pendingClose && !resume) pendingClose(); else finishClose(resume);
    };
    this.appPlacementRouteClose = requestClose;
    const protectLocal = (event: Event): void => {
      event.preventDefault(); event.stopImmediatePropagation(); requestClose();
    };
    dialog.element.addEventListener('cancel', protectLocal, { capture: true });
    dialog.element.querySelector('.sw-dialog-header button')?.addEventListener('click', protectLocal, { capture: true });
    dialog.element.addEventListener('keydown', event => { if (event.key === 'Escape') protectLocal(event); }, { capture: true });
    try {
      placement = createAppFieldPlacementController({ api: this.api, accountId, appId: app.appId, isCurrent: current,
        initialAppIds: [...new Set(this.desk.pinnedApps ?? [])].filter(id => /^app-[a-f0-9]{32}$/.test(id)).slice(0, 100) });
      this.appPlacementController = placement;
      retry.addEventListener('click', () => {
        if (!current() || busy || !placement) return;
        busy = true; retry.disabled = true; error.textContent = ''; retry.hidden = true;
        void placement.retry().then(render).catch(reason => report(String(reason?.code ?? '')))
          .finally(() => { busy = false; if (current()) { retry.disabled = false; const latest = placement?.getSnapshot(); if (latest) render(latest); } });
      });
      stack.append(this.loading('Проверяем поле'));
      void placement.load().then(render).catch(reason => report(String(reason?.code ?? '')));
    } catch (reason) { report(String((reason as {code?: string})?.code ?? '')); }
  }

  private openAppSettings(app: WorldAppRecord, onUpdated?: (app: WorldAppRecord) => void, returnTarget = this.appDialogReturnTarget(app.appId)): void {
    const accountId = this.deskAccount, sequence = this.screenSequence, accountCurrent = this.accountTask();
    if (this.destroyed || !accountId) return;
    this.appSettingsDialog?.close({ restoreFocus: false });
    let changed = false, handingOff = false;
    let finalReturnTarget = returnTarget;
    let handle: ReturnType<typeof mountAppSettings> | null = null;
    const dialog = this.dialog('Настройки приложения', ({ interrupted }) => {
      handle?.dispose(); if (this.appSettingsDialog === dialog) { this.appSettingsDialog = null; this.appSettingsRouteClose = null; }
      if (!changed || handingOff || interrupted || !accountCurrent() || this.screenSequence !== sequence || !returnTarget.isCurrent() || onUpdated) return;
      if (this.group && this.groupTab === 'apps') {
        const groupId = this.group.communityId, route = location.hash;
        this.renderGroup();
        // This specific synchronous repaint advances screenSequence itself.
        // Its cards arrive later: return once to the new main, never wait for
        // them or make the old screen ticket current again.
        if (accountCurrent() && this.group?.communityId === groupId && location.hash === route && this.activeRoute === route)
          finalReturnTarget = this.dialogReturnTarget(() => this.main);
      } else if (this.view === 'mine' && !this.group && !this.appStage) this.renderPersonal();
    }, { isCurrent: () => finalReturnTarget.isCurrent(), resolve: () => finalReturnTarget.resolve() }); this.appSettingsDialog = dialog;
    dialog.element.classList.add('sw-app-settings-dialog');
    const isCurrent = (): boolean => accountCurrent() && this.screenSequence === sequence && dialog.element.open;
    handle = mountAppSettings({ host: dialog.body, accountId, appId: app.appId, api: this.api, communities: [...this.communities], isCurrent,
      onChanged: snapshot => {
        if (!isCurrent()) return;
        const before = JSON.stringify([app.name, app.grants, app.status, app.publication, app.audience, app.deviceId, app.deviceLabel, app.entry]);
        const { entry: _previousEntry, ...metadata } = app;
        const entry = preferredInspectionEntry(snapshot);
        app = { ...metadata, name: snapshot.app.name, grants: snapshot.app.grants,
          deviceId: snapshot.source.hostDeviceId, deviceLabel: snapshot.source.deviceName,
          status: snapshot.app.state === 'revoked' ? 'revoked' : ({ offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' } as const)[snapshot.source.observation.state],
          publication: publicationFromInspection(snapshot), ...(entry ? { entry } : {}) };
        app.audience = describeAppAudience(app, accountId).label;
        changed ||= before !== JSON.stringify([app.name, app.grants, app.status, app.publication, app.audience, app.deviceId, app.deviceLabel, app.entry]);
        this.apps = this.apps.map(value => {
          if (value.appId !== app.appId) return value;
          const { entry: _oldEntry, ...previous } = value; return { ...previous, ...app };
        });
        // Updating metadata must not recreate the running iframe or its chat.
        onUpdated?.(app);
      },
      onPreview: target => {
        if (!isCurrent()) return;
        const route = formatAppLaunchRoute({ appId: app.appId, ...target });
        const parsed = parseAppLaunchRoute(route); if (!parsed) return;
        // The permanent exact-domain route carries no chat authority. Keep the
        // already-open community as local context for this deliberate preview.
        const intent = this.group?.membership?.state === 'active' ? { ...parsed, communityId: this.group.communityId } : parsed;
        handingOff = true; dialog.close({ restoreFocus: false }); void this.openApplication(app, intent);
      },
      onClose: afterClose => {
        const continueCurrent = isCurrent(); handingOff = Boolean(afterClose);
        dialog.close({ restoreFocus: !handingOff });
        if (continueCurrent && accountCurrent() && this.screenSequence === sequence) afterClose?.();
      },
    });
    this.appSettingsRouteClose = resume => handle?.requestClose(document.activeElement instanceof HTMLElement ? document.activeElement : undefined, resume);
    const close = dialog.element.querySelector<HTMLButtonElement>('.sw-dialog-header button');
    close?.addEventListener('click', event => { event.preventDefault(); event.stopImmediatePropagation(); handle?.requestClose(close); }, { capture: true });
    dialog.element.addEventListener('keydown', event => {
      if (event.key !== 'Escape') return;
      event.preventDefault(); event.stopPropagation();
      handle?.requestClose(document.activeElement instanceof HTMLElement ? document.activeElement : close ?? undefined);
    });
    dialog.element.addEventListener('cancel', event => { event.preventDefault(); handle?.requestClose(document.activeElement instanceof HTMLElement ? document.activeElement : close ?? undefined); });
  }

  private openVisibility(): void {
    if (!this.profile || this.visibilityOpen) return;
    this.visibilityOpen = true;
    const dialog = this.dialog('Видимость в общем мире', () => { this.visibilityOpen = false; });
    const intro = el('p', 'sw-muted', 'Вы решаете, что о вас видно другим.'); dialog.body.append(intro);
    const error = el('div', 'sw-error'); error.setAttribute('role', 'alert');
    const setting = (title: string, description: string, symbol: string, checked: boolean, update: (checked: boolean) => Promise<void>): void => {
      const row = el('div', 'sw-settings-row'); const copy = el('div', 'sw-grow'); copy.append(el('strong', '', title), el('p', '', description)); row.append(icon(symbol), copy, switchControl(title, checked, update)); dialog.body.append(row);
    };
    const serverSetting = async (field: string, value: unknown): Promise<void> => {
      error.textContent = '';
      try { await this.updateProfile({ [field]: value }); }
      catch (reason) { error.textContent = errorText(reason); throw reason; }
    };
    setting('Показывать меня в общем мире', 'Профиль появится на поле и в поиске.', 'eye', this.profile.discoverable === true, value => serverSetting('discoverable', value));
    setting('Показывать мои сообщества', 'В профиле будут видны открытые группы, которые вы разрешили показывать.', 'people', this.profile.showMemberships === true, value => serverSetting('showMemberships', value));
    const contact = el('select', 'sw-select');
    for (const [value, label] of [['everyone', 'Все видимые пользователи'], ['members', 'Участники общих групп'], ['nobody', 'Не принимать запросы']] as const) { const option = el('option', '', label); option.value = value; option.selected = this.profile.contactPolicy === value; contact.append(option); }
    contact.addEventListener('change', () => { contact.disabled = true; void serverSetting('contactPolicy', contact.value).catch(() => { contact.value = this.profile?.contactPolicy ?? 'everyone'; }).finally(() => { contact.disabled = false; }); });
    const row = el('div', 'sw-settings-row'); const copy = el('div', 'sw-grow'); copy.append(labeledField('Кто может предложить общение', contact)); row.append(icon('person'), copy); dialog.body.append(row);
    const preserved = el('div', 'sw-settings-row'); const preservedCopy = el('div', 'sw-grow'); preservedCopy.append(el('strong', '', 'Ваши группы и доступ сохраняются'), el('p', '', 'Скрытие профиля не закрывает уже выданный доступ к приложениям и перепискам.')); preserved.append(icon('lock'), preservedCopy); dialog.body.append(preserved);
    dialog.body.append(error, button('Готово', 'check', 'sw-button-primary sw-button-wide', () => dialog.close()));
  }

  private async updateProfile(parameters: Record<string, unknown>): Promise<void> {
    if (!this.profile) return;
    const response = await this.api.request<{ profile: WorldProfile }>('world.profile.update', { expectedRevision: this.profile.revision, ...parameters });
    if (this.destroyed) return;
    this.profile = response.profile; this.renderNavigation(); this.announce('Настройки сохранены');
    this.avatars.setContext(this.group?.membership?.state === 'active' ? this.group.communityId : undefined);
    if (this.view === 'mine' && !this.group) this.renderPersonal();
    if (this.view === 'world' && !this.group) void this.search();
  }

  private openProfileEditor(): void {
    const profile = this.profile; if (!profile) { this.runHook(this.options.openAccount); return; }
    const dialog = this.dialog('Моя страница'); const form = el('form');
    const name = textInput(profile.displayName, 'Ваше имя', 80); name.required = true;
    const bio = el('textarea', 'sw-textarea'); bio.value = profile.bio; bio.maxLength = 400; bio.placeholder = 'Чем занимаетесь, что любите';
    const interests = textInput(profile.interests.join(', '), 'Фото, музыка, технологии', 240);
    let color = worldColor(profile.avatarColor); const preview = avatar(name.value, profile.avatarUrl, color, profile.profileId, profile.avatarRevision); preview.style.setProperty('--hex-width', '94px');
    const colors = this.colorPicker(color, next => { color = worldColor(next); preview.className = `sw-avatar sw-color-${color}`; });
    const identity = el('div', 'sw-row'); const identityControls = el('div', 'sw-stack'); identityControls.append(labeledField('Ваш цвет', colors)); identity.append(preview, identityControls);
    const upload = el('input', 'sw-sr-only'); upload.type = 'file'; upload.accept = 'image/png,image/jpeg,image/webp'; upload.setAttribute('aria-label', 'Выбрать фотографию профиля');
    let pendingAvatar: { avatarUrl: string | null; thumbnailUrl: string | null } | undefined;
    const photoActions = el('div', 'sw-row'); const choose = button('Фото', 'camera', 'sw-button-small', () => upload.click());
    const removePhoto = button('Убрать', 'close', 'sw-button-small sw-button-quiet', () => { pendingAvatar = { avatarUrl: null, thumbnailUrl: null }; delete preview.dataset.profileId; preview.replaceChildren(el('span', '', name.value.slice(0, 1).toUpperCase())); }); removePhoto.hidden = !profile.avatarRevision;
    photoActions.append(choose, removePhoto, upload); identityControls.append(photoActions);
    const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const save = button('Сохранить', 'check', 'sw-button-primary'); save.type = 'submit';
    upload.addEventListener('change', () => { const file = upload.files?.[0]; upload.value = ''; if (!file) return; choose.disabled = true; save.disabled = true; error.textContent = ''; void prepareAvatar(file).then(value => { pendingAvatar = value; delete preview.dataset.profileId; const image = el('img'); image.src = value.avatarUrl; image.alt = 'Предпросмотр фотографии'; preview.replaceChildren(image); removePhoto.hidden = false; }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { choose.disabled = false; save.disabled = false; }); });
    form.classList.add('sw-profile-form');
    form.append(identity, labeledField('Имя', name), labeledField('О себе', bio), labeledField('Интересы', interests, 'Через запятую. По ним вас смогут найти.'), error);
    pinDialogSubmit(dialog, form, save);
    form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity()) return; save.disabled = true; error.textContent = ''; void this.updateProfile({ displayName: name.value.trim(), bio: bio.value.trim(), interests: interests.value.split(',').map(item => item.trim().slice(0, 32)).filter(Boolean).slice(0, 8), avatarColor: worldColors[color] }).then(async () => { if (pendingAvatar && this.profile) { const result = await this.api.request<{ profile: WorldProfile }>('world.profile.avatar.set', { expectedRevision: this.profile.revision, ...pendingAvatar }); this.profile = result.profile; this.avatars.setContext(this.group?.membership?.state === 'active' ? this.group.communityId : undefined); this.renderNavigation(); if (this.view === 'mine' && !this.group) this.renderPersonal(); } dialog.close(); this.toast('Профиль сохранён'); }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { save.disabled = false; }); });
    const extra = el('div', 'sw-stack'); extra.append(button('Видимость', 'eye', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.openVisibility(); }), button('Оформление', 'settings', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.openAppearance(); }), button('Аккаунт, устройства и контакты', 'person', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.runHook(this.options.openAccount); }), button('Прежние комнаты и инструменты', 'chat', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.runHook(this.options.openLegacy); })); dialog.body.append(form, el('hr', 'sw-rule'), extra);
  }

  private colorPicker(current: string, change: (color: string) => void): HTMLElement {
    const colors = el('div', 'sw-setting-options'); colors.setAttribute('role', 'group'); colors.setAttribute('aria-label', 'Цвет');
    const names = { honey: 'Медовый', sage: 'Шалфей', lilac: 'Сиреневый', coral: 'Коралловый', blue: 'Голубой' };
    for (const [value, label] of Object.entries(names)) { const target = el('button', `sw-choice-color sw-color-${value}`); target.type = 'button'; target.setAttribute('aria-label', label); target.title = label; target.setAttribute('aria-pressed', String(value === current)); target.addEventListener('click', () => { colors.querySelectorAll('button').forEach(node => node.setAttribute('aria-pressed', String(node === target))); change(value); }); colors.append(target); } return colors;
  }

  private openCommunityForm(existing?: WorldCommunity, chat = false): void {
    const accountCurrent = this.accountTask(), returnTab = this.groupTab;
    const dialog = this.dialog(existing ? 'Настроить сообщество' : chat ? 'Новый чат' : 'Создать сообщество'); const form = el('form');
    const name = textInput(existing?.name ?? '', chat ? 'Например, Близкие' : 'Например, Фотоклуб', 80); name.required = true;
    const description = textInput(existing?.description ?? '', 'Что вас объединяет', 240);
    const topics = textInput(existing?.topics.join(', ') ?? '', 'Фото, прогулки, творчество', 240);
    const showcase = el('textarea', 'sw-textarea'); showcase.value = existing?.showcase ?? ''; showcase.maxLength = 2500; showcase.placeholder = 'Пригласите людей: расскажите, чем здесь можно заняться.';
    const access = el('select', 'sw-select'); for (const [value, label] of [['open', 'Любой может вступить'], ['request', 'По заявке'], ['invite', 'Только по приглашению']] as const) { const option = el('option', '', label); option.value = value; option.selected = (existing?.joinPolicy ?? (chat ? 'invite' : 'open')) === value; access.append(option); }
    const canChangeAccess = !existing || existing.membership?.role === 'owner'; access.disabled = !canChangeAccess;
    let color = worldColor(existing?.color), symbol = existing?.symbol ?? 'cells';
    const styles = el('div', 'sw-stack'); styles.append(this.colorPicker(color, value => { color = worldColor(value); })); const icons = el('div', 'sw-setting-options'); icons.setAttribute('role', 'group'); icons.setAttribute('aria-label', 'Значок сообщества');
    const iconNames: Record<string, string> = { cells: 'Соты', tools: 'Мастерская', camera: 'Фотография', game: 'Игры', music: 'Музыка', bulb: 'Идеи', image: 'Галерея', people: 'Люди' };
    communityIcons.forEach(value => { const target = el('button', 'sw-choice-icon'); target.type = 'button'; target.setAttribute('aria-label', iconNames[value] ?? value); target.title = iconNames[value] ?? value; target.setAttribute('aria-pressed', String(value === symbol)); target.append(icon(value)); target.addEventListener('click', () => { symbol = value; icons.querySelectorAll('button').forEach(node => node.setAttribute('aria-pressed', String(node === target))); }); icons.append(target); }); styles.append(icons);
    const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const save = button(existing ? 'Сохранить' : chat ? 'Создать чат' : 'Создать сообщество', existing ? 'check' : 'plus', 'sw-button-primary sw-button-wide'); save.type = 'submit'; const requestId = crypto.randomUUID();
    const optional = el('div', 'sw-stack'); optional.append(labeledField('Описание (необязательно)', description), labeledField('Темы (необязательно)', topics), labeledField('Облик', styles), labeledField('Витрина (необязательно)', showcase));
    form.append(labeledField(chat ? 'Название чата' : 'Название', name));
    if (chat) form.append(el('p', 'sw-small-note', 'Чат доступен только приглашённым участникам. Их можно добавить после создания.'));
    else form.append(labeledField('Вступление', access));
    if (existing) form.append(optional);
    else { const details = el('details', 'sw-form-extra'); details.append(el('summary', '', 'Описание и оформление'), optional); form.append(details); }
    form.append(error); pinDialogSubmit(dialog, form, save);
    form.addEventListener('submit', event => {
      event.preventDefault(); if (!accountCurrent() || !dialog.element.open || !form.reportValidity()) return; save.disabled = true; error.textContent = '';
      const parameters: Record<string, unknown> = { name: name.value.trim(), description: description.value.trim(), topics: topics.value.split(',').map(value => value.trim().slice(0, 32)).filter(Boolean).slice(0, 8), ...(canChangeAccess ? { joinPolicy: access.value } : {}), showcase: showcase.value.trim(), color: worldColors[color], symbol, ...(existing ? { communityId: existing.communityId, expectedRevision: existing.revision } : { requestId }) };
      void this.api.request<{ community: WorldCommunity }>(existing ? 'world.community.update' : 'world.community.create', parameters).then(response => {
        if (!accountCurrent() || !dialog.element.open) return;
        this.updateCommunity(response.community); dialog.close();
        if (chat) { this.navigateReady('messages', response.community.communityId); this.toast('Чат создан'); return; }
        this.group = response.community;
        this.groupTab = existing ? returnTab : 'about';
        this.writeRoute(`community/${response.community.communityId}/${this.groupTab}`); this.renderGroup();
        this.toast(existing ? 'Сообщество обновлено' : 'Ваше сообщество готово');
      }).catch(reason => { if (accountCurrent() && dialog.element.open) error.textContent = errorText(reason); }).finally(() => { save.disabled = false; });
    }); dialog.body.append(form); name.focus();
  }

  private openPersonDialog(profile: WorldProfile): void {
    const dialog = this.dialog(profile.displayName); const summary = el('div', 'sw-profile-large'); const copy = el('div'); copy.append(el('h2', '', profile.displayName), el('p', 'sw-muted', profile.bio)); summary.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), copy); dialog.body.append(summary, this.tags(profile.interests));
    void this.api.request<{ profile: WorldProfile; communities: WorldCommunity[]; canRequestContact?: boolean }>('world.profile.view', { profileId: profile.profileId }).then(response => {
      if (!dialog.element.open) return;
      if (response.canRequestContact && profile.profileId !== this.profile?.profileId && this.options.requestContact) {
        const target = button('Добавить в контакты', 'person', 'sw-button-primary sw-button-wide', () => { target.disabled = true; void Promise.resolve(this.options.requestContact!(profile)).then(() => { this.toast('Запрос в контакты отправлен'); dialog.close(); }).catch(error => { target.disabled = false; this.toast(errorText(error), true); }); }); dialog.body.append(el('hr', 'sw-rule'), target, el('p', 'sw-small-note', 'После принятия человек появится в контактах.'));
      }
      if (response.communities.length) { dialog.body.append(el('hr', 'sw-rule'), el('h3', 'sw-section-title', 'Открыто в профиле')); response.communities.forEach(group => dialog.body.append(button(group.name, group.symbol, 'sw-button-wide sw-button-quiet', () => { dialog.close(); void this.openGroup(group.communityId); }))); }
    }).catch(error => { if (dialog.element.open) dialog.body.append(el('div', 'sw-error', errorText(error))); });
  }

  private openGroupManagement(group: WorldCommunity, initialState = 'active'): void {
    const accountCurrent = this.accountTask(), sequence = this.screenSequence;
    const returnTarget = this.dialogReturnTarget(); let finalReturnTarget = returnTarget;
    const dialog = this.dialog('Участники и доступ', ({ interrupted }) => {
      if (interrupted || !accountCurrent() || sequence !== this.screenSequence || this.group?.communityId !== group.communityId || !returnTarget.isCurrent()) return;
      const route = location.hash; this.renderGroup();
      if (accountCurrent() && this.group?.communityId === group.communityId && location.hash === route && this.activeRoute === route)
        finalReturnTarget = this.dialogReturnTarget(() => this.main);
    }, { isCurrent: () => finalReturnTarget.isCurrent(), resolve: () => finalReturnTarget.resolve() });
    const body = el('div', 'sw-stack'); dialog.body.append(body);
    const error = el('div', 'sw-error'); error.setAttribute('role', 'alert');
    const load = async (state: string): Promise<void> => {
      body.replaceChildren(this.loading('Загружаем участников'));
      try {
        const response = await this.api.request<{ members: WorldMember[]; nextCursor: string | null }>('world.membership.list', { communityId: group.communityId, state, limit: 60 });
        if (!dialog.element.open) return;
        body.replaceChildren(); const tabs = el('div', 'sw-filters');
        for (const [id, label] of [['active', 'Участники'], ['requested', 'Заявки'], ['banned', 'Исключённые']] as const) { if (id !== 'active' && !group.permissions.canModerate) continue; tabs.append(button(label, undefined, state === id ? 'is-selected' : '', () => { void load(id); })); }
        body.append(tabs);
        if (group.permissions.canModerate) body.append(button('Пригласить человека', 'plus', 'sw-button-primary', () => this.openInvite(group)));
        const list = el('div', 'sw-members');
        if (!response.members.length) list.append(el('p', 'sw-muted', state === 'requested' ? 'Новых заявок нет' : state === 'banned' ? 'Исключённых участников нет' : 'Пока никого нет'));
        const appendMember = (member: WorldMember): void => {
          const row = el('div', 'sw-member-row'); const copy = el('div', 'sw-grow'); copy.append(el('strong', '', member.profile.displayName), el('small', '', member.role === 'owner' ? 'Владелец' : member.role === 'moderator' ? 'Модератор' : 'Участник')); row.append(avatar(member.profile.displayName, member.profile.avatarUrl, worldColor(member.profile.avatarColor), member.profile.profileId, member.profile.avatarRevision), copy);
          const mutate = (method: string, args: Record<string, unknown>): void => {
            row.classList.add('sw-busy'); error.textContent = '';
            void this.api.request<{ community?: WorldCommunity }>(method, { communityId: group.communityId, profileId: member.profile.profileId, ...args }).then(result => { if (result.community) { this.updateCommunity(result.community); this.group = result.community; group = result.community; } void load(state); }).catch(reason => { error.textContent = errorText(reason); }).finally(() => row.classList.remove('sw-busy'));
          };
          if (group.permissions.canModerate && state === 'requested') row.append(iconButton('Принять заявку', 'check', () => mutate('world.membership.decide', { accept: true })), iconButton('Отклонить заявку', 'close', () => mutate('world.membership.decide', { accept: false })));
          else if (group.permissions.canModerate && state === 'banned') row.append(button('Вернуть доступ', undefined, 'sw-button-small', () => mutate('world.membership.unban', {})));
          else if (group.permissions.canModerate && member.role !== 'owner' && member.profile.profileId !== this.profile?.profileId) {
            row.append(iconButton('Действия с участником', 'more', () => this.openMemberActions(group, member, () => { void load(state); })));
          }
          list.append(row);
        }; response.members.forEach(appendMember);
        body.append(list, error);
        if (response.nextCursor) {
          const more = button('Показать ещё', 'plus', 'sw-button-quiet', () => {
            more.disabled = true;
            void this.api.request<{ members: WorldMember[]; nextCursor: string | null }>('world.membership.list', { communityId: group.communityId, state, limit: 60, cursor: response.nextCursor }).then(next => { if (!dialog.element.open || !list.isConnected) return; next.members.forEach(appendMember); response.nextCursor = next.nextCursor; if (!next.nextCursor) more.remove(); }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { more.disabled = false; });
          }); body.append(more);
        }
        if (group.membership?.state === 'active') {
          const preferences = el('div', 'sw-stack');
          const visibleRow = el('div', 'sw-setting-row'); const visibleCopy = el('span'); visibleCopy.append(el('strong', '', 'Показывать в моём профиле'));
          if (!this.profile?.discoverable || this.profile.showMemberships === false) visibleCopy.append(el('small', 'sw-muted', 'Разрешение сохранится. Сейчас ваши сообщества скрыты настройками профиля.'));
          const visible = switchControl('Показывать в моём профиле', group.membership.showInProfile, async next => {
            error.textContent = '';
            try { const result = await this.api.request<{ community: WorldCommunity }>('world.membership.preferences', { communityId: group.communityId, showInProfile: next }); this.updateCommunity(result.community); if (this.group?.communityId === result.community.communityId) this.group = result.community; group = result.community; }
            catch (reason) { error.textContent = errorText(reason); throw reason; }
          }); visibleRow.append(visibleCopy, visible); preferences.append(visibleRow);
          const muted = button(group.membership.muted ? 'Включить уведомления' : 'Отключить уведомления', 'bell', 'sw-button-quiet', () => { void this.api.request<{ community: WorldCommunity }>('world.membership.preferences', { communityId: group.communityId, muted: !group.membership?.muted }).then(result => { this.updateCommunity(result.community); this.group = result.community; dialog.close(); this.renderGroup(); }).catch(reason => { error.textContent = errorText(reason); }); });
          preferences.append(muted);
          if (group.membership.role !== 'owner') preferences.append(button('Выйти из сообщества', 'exit', 'sw-button-quiet sw-button-danger', () => this.confirmAction('Выйти из сообщества?', 'Чат и приложения группы станут недоступны.', 'Выйти', async () => { await this.api.request('world.membership.leave', { communityId: group.communityId }); dialog.close(); this.communities = this.communities.filter(item => item.communityId !== group.communityId); this.group = null; this.navigate('world'); })));
          else preferences.append(el('p', 'sw-small-note', 'Чтобы выйти, передайте сообщество другому участнику через его меню.'));
          body.append(el('hr', 'sw-rule'), preferences);
        }
      } catch (reason) { if (dialog.element.open) body.replaceChildren(el('div', 'sw-error', errorText(reason)), button('Повторить', 'refresh', '', () => { void load(state); })); }
    }; void load(initialState);
  }

  private openMemberActions(group: WorldCommunity, member: WorldMember, reload: () => void): void {
    const dialog = this.dialog(member.profile.displayName); const stack = el('div', 'sw-stack');
    const action = (label: string, symbol: string, method: string, args: Record<string, unknown>, confirmText?: string): void => {
      const run = async (): Promise<void> => { const result = await this.api.request<{ community?: WorldCommunity }>(method, { communityId: group.communityId, profileId: member.profile.profileId, ...args }); if (result.community) { this.group = result.community; this.updateCommunity(result.community); } dialog.close(); reload(); };
      stack.append(button(label, symbol, 'sw-button-wide', () => { if (confirmText) this.confirmAction(label + '?', confirmText, label, run); else void run().catch(error => this.toast(errorText(error), true)); }));
    };
    if (group.membership?.role === 'owner') {
      action(member.role === 'moderator' ? 'Снять модератора' : 'Сделать модератором', 'people', 'world.membership.role', { role: member.role === 'moderator' ? 'member' : 'moderator' });
      action('Передать сообщество', 'arrow', 'world.membership.transfer', { requestId: crypto.randomUUID() }, 'Этот участник станет владельцем и сможет управлять всем сообществом.');
    }
    action('Удалить из группы', 'exit', 'world.membership.remove', {}, 'Человек потеряет доступ к чату и приложениям. Сможет вступить заново по правилам группы.');
    action('Заблокировать участие', 'lock', 'world.membership.ban', {}, 'Человек потеряет доступ и не сможет вступить снова, пока вы не снимете ограничение.');
    dialog.body.append(stack);
  }

  private confirmAction(title: string, description: string, label: string, action: () => Promise<void>): void {
    const dialog = this.dialog(title); const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const actions = el('div', 'sw-dialog-actions');
    const run = button(label, undefined, 'sw-button-primary', () => { run.disabled = true; void action().then(() => dialog.close()).catch(reason => { error.textContent = errorText(reason); }).finally(() => { run.disabled = false; }); });
    actions.append(button('Отмена', undefined, '', () => dialog.close()), run); dialog.body.append(el('p', 'sw-muted', description), error, actions);
  }

  private shareCommunity(group: WorldCommunity): void {
    const url = new URL(location.href); url.hash = `community/${group.communityId}/about`;
    void navigator.clipboard.writeText(url.href).then(() => this.toast('Ссылка на сообщество скопирована')).catch(() => {
      const dialog = this.dialog('Ссылка на сообщество'); const input = textInput(url.href); input.readOnly = true; dialog.body.append(labeledField('Ссылка', input), el('p', 'sw-muted', group.joinPolicy === 'invite' ? 'Увидят только участники и приглашённые.' : 'Откроется страница сообщества.')); input.select();
    });
  }

  private openInvite(group: WorldCommunity): void {
    const accountCurrent = this.accountTask();
    let timer: ReturnType<typeof setTimeout> | null = null, sequence = 0;
    let contactsRequest: Promise<{ contacts: Omit<DirectoryContact, 'kind'>[] }> | null = null;
    const dialog = this.dialog('Пригласить человека', () => { if (timer) clearTimeout(timer); sequence++; }); const input = textInput('', 'Имя или интерес', 100); input.setAttribute('aria-label', 'Найти человека');
    const results = el('div', 'sw-stack'); const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); dialog.body.append(input, el('hr', 'sw-rule'), results, error);
    const search = async (): Promise<void> => {
      const request = ++sequence, query = input.value.trim(); results.replaceChildren(this.loading('Ищем людей')); error.textContent = '';
      contactsRequest ??= this.api.request('contacts.list', {});
      const [publicPeople, contacts] = await Promise.allSettled([
        this.api.request<WorldSearch>('world.discovery.search', { query, kind: 'people', limit: 24 }), contactsRequest,
      ]);
      if (!accountCurrent() || !dialog.element.open || request !== sequence) return;
      if (contacts.status === 'rejected') contactsRequest = null;
      results.replaceChildren();
      type Target = Pick<WorldProfile, 'profileId' | 'displayName'> & Partial<Pick<WorldProfile, 'avatarUrl' | 'avatarColor' | 'avatarRevision'>> & { contact?: boolean };
      const targets = new Map<string, Target>();
      const visible = publicPeople.status === 'fulfilled' ? publicPeople.value.people : [];
      const needle = query.toLocaleLowerCase('ru');
      if (contacts.status === 'fulfilled') for (const contact of contacts.value.contacts) {
        if (!contact.peerAccountId || !contact.label.toLocaleLowerCase('ru').includes(needle)) continue;
        const profile = visible.find(value => value.profileId === contact.peerAccountId);
        targets.set(contact.peerAccountId, { ...profile, profileId: contact.peerAccountId, displayName: contact.label, contact: true });
        if (targets.size >= 24) break;
      }
      for (const profile of visible) if (!targets.has(profile.profileId) && targets.size < 24) targets.set(profile.profileId, profile);
      const people = [...targets.values()].filter(profile => profile.profileId !== this.profile?.profileId);
      if (publicPeople.status === 'rejected' || contacts.status === 'rejected') {
        error.textContent = publicPeople.status === 'rejected' && contacts.status === 'rejected' ? 'Не удалось загрузить людей. Повторите поиск.' : contacts.status === 'rejected' ? 'Контакты временно недоступны. Показаны открытые профили.' : 'Поиск открытых профилей недоступен. Показаны ваши контакты.';
        results.append(button('Повторить поиск', 'refresh', 'sw-button-quiet', () => { void search(); }));
      }
      if (!people.length) results.append(el('p', 'sw-muted', 'Никого не нашли. Попробуйте другое имя.'));
      people.forEach(profile => {
        const row = el('div', 'sw-member-row'), copy = el('div', 'sw-grow'); copy.append(el('strong', '', profile.displayName)); if (profile.contact) copy.append(el('p', 'sw-small-note', 'Ваш контакт'));
        row.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), copy);
        const invite = button('Пригласить', 'plus', '', () => {
          if (!accountCurrent() || !dialog.element.open) return; invite.disabled = true;
          void this.api.request('world.membership.invite', { communityId: group.communityId, profileId: profile.profileId }).then(() => {
            if (accountCurrent() && dialog.element.open && invite.isConnected) invite.replaceChildren(icon('check'), el('span', '', 'Приглашён'));
          }).catch(reason => { if (accountCurrent() && dialog.element.open && invite.isConnected) { error.textContent = errorText(reason); invite.disabled = false; } });
        }); row.append(invite); results.append(row);
      });
    };
    input.addEventListener('input', () => { sequence++; if (timer) clearTimeout(timer); timer = setTimeout(() => { void search(); }, 230); }); void search(); input.focus();
  }
}
