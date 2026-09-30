import './world.css';
import './product.css';
import './shell.css';
import './chat.css';
import './community.css';
import { mountAppStage, type AppStageHandle } from './app-stage';
import { mountAppLibrary } from './app-library';
import { createChatDraftStore, readChatForward } from './chat-state.mjs';
import { capabilities, createLibrary, loadDeskPreferences, openCommandPalette, saveDeskPreferences, type CapabilityId, type DeskPreferences } from './product';
import { createAppsHome, appStatusLabel, type AppHomeState } from './apps-home';
import { createApplicationCard } from './application-card';
import { describeAppAudience, publicationFromInspection } from './app-audience.mjs';
import { formatAppLaunchRoute, parseAppLaunchRoute, type AppLaunchIntent, type AppResolvedEntry } from './app-launch.mjs';
import { mountAppSettings } from './app-settings';
import { createThemeController, createThemeControls, type ThemeController } from './theme/theme';
import { getPwaController, registerUpdateGuard, watchFormEdits, type PwaState } from '../platform/pwa';
import { AvatarHydrator, prepareAvatar } from './avatars';
import { avatar, badge, button, el, emptyState, heading, iconButton, labeledField, nounCount, textInput, timeLabel } from './dom';
import { createDialog, errorText, isDialogFocusTarget, switchControl, type DialogCloseContext, type DialogReturnTarget, type WorldDialog } from './dialogs';
import { communityEmblem, createHexField, createHexFieldState, type HexField } from './hex-field';
import { communityIcons, icon } from './icons';
import { loadPreferences, savePreferences, type WorldPreferences, type WorldView } from './preferences';
import { entityId, entityName, worldColor, worldColors, type WorldApi, type WorldAppOptions, type WorldAppRecord, type WorldAssistantHandle, type WorldCommunity, type WorldDevice, type WorldEntity, type WorldMember, type WorldMessage, type WorldProfile, type WorldSearch } from './types';

type GroupTab = 'about' | 'chat' | 'apps';
type CatalogKind = 'all' | 'people' | 'communities';
interface AppProjection { id: string; name: string; ownerAccountId: string; hostDeviceId: string; deviceName?: string; state: string; grants?: { accountIds: string[]; communityIds: string[] }; publication?: WorldAppRecord['publication']; port?: number; }
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
  private readonly header = el('header', 'sx-header');
  private readonly rail = el('aside', 'sx-rail');
  private readonly mobileNav = el('nav', 'sx-mobile-nav');
  private readonly live = el('div', 'sw-sr-only');
  private readonly controller = new AbortController();
  private readonly avatars: AvatarHydrator;
  private readonly dialogs = new Set<WorldDialog>();
  private appSettingsDialog: WorldDialog | null = null;
  private appSettingsRouteClose: ((resume: () => void) => void) | null = null;
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
  private accessHandle: WorldAssistantHandle | null = null;
  private appStage: AppStageHandle | null = null;
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
  private query = '';
  private kind: CatalogKind = 'all';
  private discoveryPages: (string | null)[] = [null];
  private selected: WorldEntity | null = null;
  private group: WorldCommunity | null = null;
  private groupTab: GroupTab = 'about';
  private groupReturn: 'mine' | 'world' | 'messages' = 'mine';
  private field: HexField | null = null;
  private fieldState = createHexFieldState();
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

  constructor(root: HTMLElement, options: WorldAppOptions) {
    this.root = root; this.options = options; this.api = options.api;
    this.preferences = loadPreferences(); this.view = this.preferences.view;
    this.theme = createThemeController({ initial: this.preferences, onChange: next => { Object.assign(this.preferences, next); this.persist(); } });
    if (!location.hash) history.replaceState({ soty: true }, '', `#${this.view}`);
    root.classList.add('sw-app'); root.dataset.motion = this.preferences.motion ? 'on' : 'off'; root.dataset.compact = String(this.preferences.compact);
    this.live.setAttribute('role', 'status'); this.live.setAttribute('aria-live', 'polite'); this.mobileNav.setAttribute('aria-label', 'Главная навигация');
    this.main.id = 'soty-main'; this.main.tabIndex = -1;
    const skip = el('a', 'sx-skip', 'К содержимому'); skip.href = '#soty-main';
    skip.addEventListener('click', event => { event.preventDefault(); this.main.focus(); });
    root.replaceChildren(skip, this.rail, this.header, this.pwaBanner, this.main, this.mobileNav, this.live);
    this.pwaBanner.setAttribute('aria-live', 'polite'); this.pwaBanner.setAttribute('aria-label', 'Состояние приложения');
    this.unsubscribePwa = this.pwa.subscribe(state => {
      const recovered = state.connection === 'online' && this.connection !== 'online';
      this.connection = state.connection;
      this.renderPwaBanner(state);
      if (recovered) {
        this.notesHandle?.reconnect();
        if (!this.profile && this.deskAccount) void this.refresh(true);
      }
    });
    this.unregisterUpdateGuard = registerUpdateGuard(async () => { await this.flushScreen(); return !this.screenHasUnsavedChanges() && !this.chatDrafts.hasVolatile() && !this.formEdits.hasUnsavedChanges() && !document.querySelector('dialog[open] form'); });
    root.addEventListener('click', event => {
      const target = event.target instanceof Element ? event.target.closest<HTMLElement>('button, a[href]') : null;
      if (!target || target.closest('.sn-workspace,.sw-assistant-host,.sw-access-host,.sa-stage') || !this.screenHasUnsavedChanges()) return;
      event.preventDefault(); event.stopImmediatePropagation();
      this.afterNoteSaved(() => { if (target.isConnected) target.click(); });
    }, { capture: true, signal: this.controller.signal });
    this.avatars = new AvatarHydrator(this.api, root);
    this.renderNavigation(); this.main.append(this.loading('Открываем Соты'));
    window.addEventListener('popstate', () => { void this.openRoute(); }, { signal: this.controller.signal });
    window.addEventListener('beforeunload', event => { if (this.chatDrafts.hasVolatile() || this.screenHasUnsavedChanges()) { event.preventDefault(); event.returnValue = ''; } }, { signal: this.controller.signal });
    window.addEventListener('storage', event => this.chatDrafts.storageChanged(event.key), { signal: this.controller.signal });
    document.addEventListener('keydown', event => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k' && !event.isComposing && !document.querySelector('dialog[open]')) { event.preventDefault(); this.openQuickActions(); return; }
      if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes((event.target as HTMLElement).tagName) && !document.querySelector('dialog[open]')) {
        const search = this.root.querySelector<HTMLInputElement>('.sw-search input');
        if (search) { event.preventDefault(); search.focus(); }
      }
      if (event.key === 'Escape' && this.selected && !document.querySelector('dialog[open]')) this.closePreview();
    }, { signal: this.controller.signal });
  }

  destroy(): void {
    this.destroyed = true; this.requestSequence++; this.screenSequence++;
    this.controller.abort(); this.cleanScreen(); this.avatars.destroy(); this.theme.destroy(); this.unsubscribePwa(); this.unregisterUpdateGuard(); this.formEdits.destroy();
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
      this.renderNavigation();
      // A same-account shell refresh is not navigation. The owner window and
      // its running app keep their draft, selection and existing connection.
      if (sameAccount && this.appSettingsDialog?.element.open) { this.routeLoaded = true; return; }
      // Recover the authenticated shell after an offline launch without replacing the live editor.
      if (preserveNote && sameAccount && this.notesHandle) { this.routeLoaded = true; return; }
      if (!this.routeLoaded || /^#(?:app|launch)(?:\/|\?|$)/u.test(location.hash)) { this.routeLoaded = true; if (await this.openRoute()) return; }
      if (this.group) { await this.openGroup(this.group.communityId, this.groupTab); return; }
      this.renderCurrent();
      if (this.view === 'world') await this.search();
      if (this.view === 'mine') await this.loadPersonal();
    } catch (error) {
      if (this.destroyed || sequence !== this.requestSequence) return;
      const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
      if (['NETWORK_ERROR', 'NETWORK_TIMEOUT'].includes(code) && this.options.localAccount) {
        const local = await this.options.localAccount().catch(() => null);
        if (this.destroyed || sequence !== this.requestSequence) return;
        if (local?.accountId) {
          const changed = this.transitionAccount(local.accountId);
          const sameScreen = !changed && this.deskAccount === accountAtStart && this.screenSequence === screenAtStart;
          if (sameScreen && this.appSettingsDialog?.element.open) return;
          if (sameScreen && this.appStage) { this.toast(errorText(error), true); return; }
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
    this.cleanScreen(); for (const dialog of [...this.dialogs]) dialog.close({ restoreFocus: false });
    this.profile = null; this.communities = []; this.apps = []; this.devices = []; this.homeNotes = null;
    this.group = null; this.selected = null; this.selectedChat = undefined; this.groupReturn = 'mine'; this.groupTab = 'about';
    this.homeState.slots.clear(); this.homeState.scroll = 0; this.homeState.fieldX = 0; this.homeState.fieldY = 0;
    this.homeState.communityId = null; this.homeState.focusId = null; this.homeState.focusControl = null; this.homeState.lens = 'all';
    this.desk = next ? loadDeskPreferences(next) : { favorites: [], recent: [] };
    this.homeState.pinned = new Set(this.desk.pinnedApps ?? []);
    this.homeStatus = { devices: 'loading', apps: 'loading', communities: 'loading', notes: 'loading' };
    this.fieldState = createHexFieldState(); this.discoveryScope = ''; this.discoveryPages = [null]; this.discoveryStatus = 'loading'; this.query = ''; this.kind = 'all';
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

  private loading(label: string): HTMLElement { const node = el('div', 'sw-loading'); node.append(el('span', '', label)); return node; }
  private cleanScreen(): void { this.screenSequence++; this.appSettingsDialog?.close({ restoreFocus: false }); this.appSettingsDialog = null; this.appSettingsRouteClose = null; this.appStage?.dispose(); this.appStage = null; this.live.textContent = ''; this.field?.destroy(); this.field = null; this.homeHandle?.destroy(); this.homeHandle = null; this.notesHandle?.dispose(); this.notesHandle = null; this.assistantHandle?.dispose(); this.assistantHandle = null; this.accessHandle?.dispose(); this.accessHandle = null; this.chatCleanup?.(); this.chatCleanup = null; if (this.chatTimer) clearInterval(this.chatTimer); this.chatTimer = null; }
  private screenHasUnsavedChanges(): boolean { return !!(this.notesHandle?.hasUnsavedChanges() || this.assistantHandle?.hasUnsavedChanges?.() || this.accessHandle?.hasUnsavedChanges?.() || this.appStage?.hasUnsavedChanges()); }
  private async flushScreen(): Promise<void> { await Promise.all([this.notesHandle?.flush(), this.assistantHandle?.flush?.(), this.accessHandle?.flush?.(), this.appStage?.flush()]); }
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
    const brand = button('Соты', undefined, 'sx-wordmark', () => this.navigate('mine'));
    const context = el('span', 'sx-header-context', this.group?.name || 'Личное');
    const nav = el('nav', 'sx-rail-nav'); nav.setAttribute('aria-label', 'Главная навигация');
    const items: [WorldView, string, string][] = [['mine', 'Аппки', 'app'], ['messages', 'Чаты', 'chat'], ['assistant', 'Помощник', 'sparkle']];
    this.mobileNav.replaceChildren();
    for (const [id, label, symbol] of items) {
      const make = (): HTMLButtonElement => {
        const item = button(label, symbol, 'sx-nav-button', () => this.navigate(id));
        if (this.view === id || id === 'mine' && !['messages', 'assistant', 'access'].includes(this.view)) item.setAttribute('aria-current', 'page'); return item;
      };
      nav.append(make()); this.mobileNav.append(make());
    }
    const makeProfile = (): HTMLButtonElement => {
      const profile = el('button', 'sx-profile'); profile.type = 'button'; profile.setAttribute('aria-label', 'Профиль и настройки');
      profile.append(avatar(this.profile?.displayName || 'Я', this.profile?.avatarUrl, worldColor(this.profile?.avatarColor), this.profile?.profileId, this.profile?.avatarRevision));
      profile.addEventListener('click', () => this.openProfileMenu()); return profile;
    };
    const emblem = button('', undefined, 'sx-brand-mark', () => this.navigate('mine')); emblem.setAttribute('aria-label', 'Соты — на главную');
    const hex = el('span', 'soty-hex', 'S'); emblem.append(hex);
    const bottom = el('div', 'sx-rail-bottom'); bottom.append(iconButton('Оформление', 'sun', () => this.openAppearance()), makeProfile());
    this.rail.replaceChildren(emblem, nav, bottom);
    const search = button('Аппки, люди, возможности', 'search', 'sx-global-search', () => this.openQuickActions());
    search.setAttribute('aria-label', 'Аппки, люди, возможности');
    search.setAttribute('aria-keyshortcuts', 'Control+k Meta+k'); search.append(el('kbd', '', 'Ctrl K'));
    const access = button('Доступы и действия', 'shield', 'sx-access-button', () => this.navigate('access'));
    access.setAttribute('aria-label', 'Доступы и действия');
    if (this.view === 'access') access.setAttribute('aria-current', 'page');
    const add = button('Добавить', 'plus', 'sw-button-primary sx-add-button', () => this.openAddMenu()); add.setAttribute('aria-label', 'Добавить');
    const actions = el('div', 'sx-header-actions'); actions.append(search, access, add);
    const profile = makeProfile(); profile.classList.add('sx-mobile-profile'); actions.append(profile);
    this.header.replaceChildren(brand, context, actions);
  }

  private openProfileMenu(): void {
    const dialog = this.dialog(this.profile?.displayName || 'Мой аккаунт'); const list = el('div', 'sx-profile-menu');
    const choice = (label: string, symbol: string, action: () => void): void => { list.append(button(label, symbol, 'sw-button-quiet', () => { dialog.close(); action(); })); };
    choice('Профиль', 'person', () => this.openProfileEditor());
    choice(this.profile?.discoverable ? 'Вы видны в общем мире' : 'Вы скрыты в общем мире', this.profile?.discoverable ? 'eye' : 'hidden', () => this.openVisibility());
    choice('Устройства', 'laptop', () => this.openResources('devices'));
    choice('Доступы и действия', 'shield', () => this.navigate('access'));
    choice('Оформление', 'sun', () => this.openAppearance());
    choice('Все возможности', 'grid', () => this.navigate('library'));
    const developerDocs = el('a', 'sw-button sw-button-quiet');
    developerDocs.href = '/agents'; developerDocs.target = '_blank'; developerDocs.rel = 'noopener';
    developerDocs.setAttribute('aria-label', 'Для разработчиков и ИИ (в новой вкладке)');
    developerDocs.append(icon('connections'), el('span', '', 'Для разработчиков и ИИ'), icon('external'));
    list.append(developerDocs);
    choice('Аккаунт и восстановление', 'lock', () => this.runHook(() => this.options.openAccount('recovery')));
    dialog.body.append(list);
  }

  private navigate(view: WorldView, chatId?: string): void {
    this.selectedChat = view === 'messages' ? chatId : undefined;
    if (view === 'world') this.discoveryStatus = 'loading';
    this.writeRoute(view === 'messages' && chatId ? `messages/${chatId}` : view === 'world' ? this.discoveryRoute() : view);
    this.view = view; this.preferences.view = view; this.persist(); this.group = null; this.selected = null;
    this.renderNavigation(); this.renderCurrent();
    if (view === 'world') void this.search();
    if (view === 'mine') void this.loadPersonal();
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

  private async openRoute(): Promise<boolean> {
    if (this.destroyed || (!this.profile && !this.deskAccount)) return false;
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
      this.kind = kind === 'people' || kind === 'communities' ? kind : 'all'; this.navigate('world'); return true;
    }
    if (route === 'mine' || route === 'library' || route === 'assistant' || route === 'access') { this.navigate(route); return true; }
    if (route) return false;
    return false;
  }

  private renderCurrent(): void {
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

  private renderAssistant(): void {
    const host = el('section', 'sw-assistant-host sx-page-host'); host.dataset.pwaIgnore = ''; this.main.replaceChildren(host);
    const sequence = this.screenSequence;
    if (!this.options.openAssistant) {
      const panel = el('div', 'sx-assistant-fallback'); panel.append(heading('Помощник', 'Ваши задачи и устройства'),
        button('Создать приложение', 'sparkle', 'sw-button-primary', () => this.runHook(() => this.options.agentCreate())),
        button('Мои устройства', 'laptop', 'sw-button-quiet', () => this.openResources('devices'))); host.append(panel); return;
    }
    host.append(this.loading('Открываем помощника'));
    void Promise.resolve().then(() => this.options.openAssistant!(host)).then(handle => {
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
    const workspace = el('div', 'sw-workspace'); const discovery = el('section', 'sw-discovery');
    const mobileHeading = el('div', 'sw-mobile-heading'); mobileHeading.append(heading('Открытия', 'Люди и сообщества'));
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
      if (!this.field || !stage.contains(this.field.element)) { this.field?.destroy(); this.field = createHexField(entity => { void this.preview(entity); }, this.preferences.scale, this.fieldState); stage.replaceChildren(this.field.element); }
      this.field.update(entities, selected);
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
      const reset = button('Обзор', 'cells', 'sw-button-small sw-field-reset', () => { this.preferences.scale = 1; this.persist(); this.field?.setScale(1); this.field?.element.scrollTo({ left: 0, top: 0, behavior: 'instant' }); }); reset.setAttribute('aria-label', 'Вернуть обзор поля'); reset.title = 'Вернуть обзор поля'; controls.append(reset);
    }
    if (this.discoveryPages.length > 1) controls.append(iconButton('Предыдущая страница', 'back', () => { void this.search(false, true); }));
    if (this.results.nextCursor) controls.append(iconButton('Следующая страница', 'next', () => { void this.search(true); }));
    const segment = el('div', 'sw-segment'); segment.setAttribute('role', 'group'); segment.setAttribute('aria-label', 'Вид результатов');
    for (const [view, label] of [['field', 'Поле'], ['list', 'Список']] as const) {
      const item = button(label, undefined, this.preferences.presentation === view ? 'is-selected' : '', () => { this.preferences.presentation = view; this.persist(); this.renderSearchResults(); });
      item.setAttribute('aria-pressed', String(this.preferences.presentation === view)); segment.append(item);
    }
    controls.append(segment); stage.append(controls);
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
      const showcase = el('div', 'sw-stack sw-preview-showcase'); showcase.append(el('h3', '', 'О группе'), el('div', 'sw-showcase', group.showcase || 'Общее место для новых идей и совместных дел.'));
      inner.append(showcase);
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
    const crumb = el('div', 'sw-breadcrumb'); crumb.append(button(this.groupReturn === 'world' ? 'Открытия' : this.groupReturn === 'messages' ? 'Все чаты' : 'Моё пространство', 'back', 'sw-button-quiet', () => this.navigate(this.groupReturn)));
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
    if (showHeader) {
      const header = el('div', 'sw-chat-header'); const back = iconButton('Все чаты', 'back', () => this.navigate('messages')); back.classList.add('sw-mobile-back');
      const copy = el('div', 'sw-grow'); copy.append(el('h3', '', group.name), el('small', 'sw-muted', `Сообщество · ${nounCount(group.memberCount, 'участник', 'участника', 'участников')}`));
      header.append(back, copy, iconButton('О сообществе', 'people', () => { void this.openGroup(group.communityId); })); chat.append(header);
    }
    const messages = el('div', 'sw-chat-messages'); messages.setAttribute('role', 'log'); messages.setAttribute('aria-label', 'Сообщения'); messages.setAttribute('aria-live', 'polite'); messages.setAttribute('aria-relevant', 'additions'); messages.append(this.loading('Загружаем разговор'));
    const composer = el('form', 'sw-composer'); composer.dataset.pwaIgnore = ''; const input = el('textarea'); input.rows = 1; input.maxLength = 6000; input.placeholder = 'Написать в общий чат'; input.setAttribute('aria-label', `Сообщение в ${group.name}`); input.value = initialDraft.text;
    const send = button('Отправить', 'send', 'sw-button-primary'); send.type = 'submit'; send.disabled = !input.value.trim(); send.setAttribute('aria-label', 'Отправить сообщение');
    const error = el('div', 'sw-chat-error'); const errorMessage = el('div', 'sw-error'); errorMessage.setAttribute('role', 'alert'); error.append(errorMessage);
    const draftStatus = el('div', 'sw-chat-draft-status'); draftStatus.setAttribute('role', 'status');
    const downloadDraft = button('Скачать черновик', 'download', 'sw-button-small sw-button-quiet', () => {
      const url = URL.createObjectURL(new Blob([input.value], { type: 'text/plain;charset=utf-8' })); const link = el('a'); link.href = url; link.download = 'soty-chat-draft.txt'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    const describeDraft = (): void => {
      const volatile = this.chatDrafts.isVolatile(accountId, group.communityId);
      draftStatus.textContent = input.value ? volatile ? 'Черновик пока только в открытом приложении' : 'Черновик на этом устройстве' : '';
      draftStatus.classList.toggle('is-error', volatile); draftStatus.hidden = !input.value; downloadDraft.hidden = !volatile;
    };
    error.append(draftStatus, downloadDraft); describeDraft();
    const fitComposer = (): void => { input.style.height = 'auto'; input.style.height = `${Math.min(144, input.scrollHeight)}px`; };
    input.addEventListener('input', () => { this.chatDrafts.edit(accountId, group.communityId, input.value); send.disabled = !input.value.trim(); fitComposer(); describeDraft(); });
    input.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.shiftKey && !event.isComposing && !matchMedia('(pointer:coarse)').matches) { event.preventDefault(); if (input.value.trim() && !send.disabled) composer.requestSubmit(); } });
    composer.append(input, send); chat.append(messages, error, composer); parent.append(chat);
    fitComposer();
    let composerWidth = 0;
    const composerResize = new ResizeObserver(entries => { const width = entries[0]?.contentRect.width ?? 0; if (width > 0 && width !== composerWidth) { composerWidth = width; fitComposer(); } });
    composerResize.observe(composer);
    const unsubscribeDraft = this.chatDrafts.subscribe(accountId, group.communityId, draft => { if (input.value !== draft.text) { input.value = draft.text; fitComposer(); } if (!input.disabled) send.disabled = !input.value.trim(); describeDraft(); });
    let syncSeq = 0, firstSeq = Number.MAX_SAFE_INTEGER, lastRead = 0, reading = false, initial = true, fetching = false, accessClosed = false, catchupTimer: ReturnType<typeof setTimeout> | undefined;
    const known = new Set<string>();
    const active = (): boolean => !this.destroyed && sequence === this.screenSequence && chat.isConnected;

    const append = (message: WorldMessage, prepend = false): void => {
      if (known.has(message.messageId)) {
        if (message.removed) { const row = messages.querySelector<HTMLElement>(`[data-message-id="${CSS.escape(message.messageId)}"]`); if (row) { row.classList.add('is-removed'); row.querySelector('.sw-message-text')!.textContent = 'Сообщение удалено'; row.querySelector('.sw-message-delete')?.remove(); } }
        return;
      }
      known.add(message.messageId); firstSeq = Math.min(firstSeq, message.seq);
      messages.querySelector('.sw-empty')?.remove();
      const row = el('article', `sw-chat-message${message.author.profileId === this.profile?.profileId ? ' is-own' : ''}${message.removed ? ' is-removed' : ''}`);
      row.dataset.messageId = message.messageId; row.dataset.messageSeq = String(message.seq); row.append(avatar(message.author.displayName, message.author.avatarUrl, worldColor(message.author.avatarColor), message.author.profileId, message.author.avatarRevision));
      const body = el('div', 'sw-message-body'); const meta = el('div', 'sw-message-meta');
      const time = el('time', '', timeLabel(message.createdAt)); time.dateTime = new Date(message.createdAt).toISOString(); meta.append(el('strong', '', message.author.displayName), time);
      if (!message.removed && (message.author.profileId === this.profile?.profileId || group.permissions.canModerate)) {
        const remove = iconButton('Удалить сообщение', 'trash', () => this.confirmAction('Удалить сообщение?', 'Оно исчезнет из разговора у всех участников.', 'Удалить', async () => { await this.api.request('world.chat.remove', { communityId: group.communityId, messageId: message.messageId }); if (row.isConnected) { row.classList.add('is-removed'); row.querySelector('.sw-message-text')!.textContent = 'Сообщение удалено'; remove.remove(); } })); remove.classList.add('sw-message-delete'); meta.append(remove);
      }
      body.append(meta, el('div', 'sw-message-text', message.removed ? 'Сообщение удалено' : message.text)); row.append(body);
      const next = Array.from(messages.querySelectorAll<HTMLElement>('[data-message-seq]')).find(item => Number(item.dataset.messageSeq) > message.seq);
      messages.insertBefore(row, next ?? null);
    };

    const markRead = (): void => {
      if (!reading && syncSeq > lastRead && document.visibilityState === 'visible' && messages.getClientRects().length > 0 && messages.scrollHeight - messages.scrollTop - messages.clientHeight < 140) {
        reading = true; const throughSeq = syncSeq;
        void this.api.request<{ unreadCount: number }>('world.chat.read', { communityId: group.communityId, throughSeq }).then(result => { lastRead = throughSeq; group.unreadCount = result.unreadCount; this.updateCommunity(group); }).catch(() => { /* Reading acknowledgement retries with the next refresh. */ }).finally(() => { reading = false; });
      }
    };

    const load = async (): Promise<void> => {
      if (fetching || !active() || document.visibilityState === 'hidden') return;
      fetching = true;
      try {
        const stickToEnd = initial || messages.scrollHeight - messages.scrollTop - messages.clientHeight < 130;
        const response = await this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, limit: 60 });
        if (!active()) return;
        if (initial) { messages.querySelector('.sw-loading')?.remove(); response.messages.forEach(message => append(message)); syncSeq = Math.max(0, ...response.messages.map(message => message.seq)); }
        else {
          // Refresh existing tombstones without jumping past an unseen interval.
          response.messages.filter(message => known.has(message.messageId)).forEach(message => append(message));
          const forward = await readChatForward({ after: syncSeq, active, fetchPage: after => this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, after, limit: 60 }), append: message => append(message) });
          if (!active()) return;
          syncSeq = forward.cursor;
          if (forward.hasMore) catchupTimer = setTimeout(() => { void load(); }, 100);
        }
        if (initial && response.hasMore) {
          const previous = button('Раньше', 'back', 'sw-button-small sw-button-quiet', () => {
            previous.disabled = true; const beforeHeight = messages.scrollHeight;
            void this.api.request<{ messages: WorldMessage[]; hasMore: boolean }>('world.chat.list', { communityId: group.communityId, before: firstSeq, limit: 60 }).then(history => {
              if (!active()) return; [...history.messages].reverse().forEach(message => append(message, true));
              if (history.hasMore) messages.prepend(previous); else previous.remove(); messages.scrollTop += messages.scrollHeight - beforeHeight;
            }).catch(error => { if (active()) errorMessage.textContent = errorText(error); }).finally(() => { previous.disabled = false; });
          }); messages.prepend(previous);
        }
        if (!known.size) messages.replaceChildren(emptyState('Поздоровайтесь первыми', 'Начните с вопроса или поделитесь идеей.', undefined, 'chat'));
        initial = false; errorMessage.textContent = '';
        if (stickToEnd) messages.scrollTop = messages.scrollHeight;
        markRead();
      } catch (error) {
        if (!active()) return;
        if (initial) messages.replaceChildren(emptyState('Разговор пока недоступен', errorText(error), button('Повторить', 'refresh', 'sw-button-quiet', () => { void load(); }), 'chat'));
        else errorMessage.textContent = errorText(error);
        if (typeof error === 'object' && error && 'code' in error && ['community_membership_required', 'community_not_found', 'community_banned'].includes(String(error.code))) { accessClosed = true; input.disabled = true; send.disabled = true; this.avatars.setContext(); messages.replaceChildren(emptyState('Доступ к сообществу закрыт', 'Вернитесь в общий мир, чтобы продолжить.', button('В общий мир', 'world', 'sw-button-primary', () => this.navigate('world')), 'lock')); if (this.chatTimer) clearInterval(this.chatTimer); }
      } finally { fetching = false; }
    };
    messages.addEventListener('scroll', () => { if (!fetching) markRead(); }, { passive: true });
    composer.addEventListener('submit', event => {
      event.preventDefault(); const text = input.value.trim(); if (!text || send.disabled) return;
      const pending = this.chatDrafts.beginSend(accountId, group.communityId, text); describeDraft();
      send.disabled = true; input.disabled = true; errorMessage.textContent = '';
      void this.api.request<{ message: WorldMessage }>('world.chat.send', { communityId: group.communityId, clientId: pending.clientId, text }).then(response => {
        const draft = this.chatDrafts.acknowledge(accountId, group.communityId, pending.clientId);
        if (!active()) return;
        append(response.message); input.value = draft.text; fitComposer(); describeDraft(); messages.scrollTop = messages.scrollHeight; void load();
      }).catch(error => { if (active()) errorMessage.textContent = errorText(error); }).finally(() => { if (active()) { input.disabled = accessClosed; send.disabled = accessClosed || !input.value.trim(); if (!accessClosed) input.focus({ preventScroll: true }); } });
    });
    const returned = (): void => { if (document.visibilityState === 'visible') { this.chatDrafts.retrySave(accountId, group.communityId); describeDraft(); void load(); } };
    document.addEventListener('visibilitychange', returned);
    this.chatCleanup = () => { unsubscribeDraft(); composerResize.disconnect(); document.removeEventListener('visibilitychange', returned); if (catchupTimer) clearTimeout(catchupTimer); };
    void load(); this.chatTimer = setInterval(() => { void load(); }, 4500);
  }

  private renderMessages(selectedId?: string): void {
    const groups = this.communities.filter(group => group.membership?.state === 'active');
    const selected = groups.find(group => group.communityId === selectedId);
    this.selectedChat = selected?.communityId;
    if (selectedId && !selected) { this.activeRoute = '#messages'; history.replaceState({ soty: true }, '', this.activeRoute); this.toast('Этот чат сейчас недоступен. Выберите другой разговор.'); }
    const screen = el('section', `sw-messages-view${selected ? ' has-room' : ''}`); const list = el('aside', 'sw-conversations'); list.append(heading('Чаты'));
    if (!groups.length) list.append(emptyState('Найдите свой круг', 'Вступите в сообщество, чтобы начать разговор.', button('Найти сообщество', 'world', 'sw-button-primary', () => this.navigate('world')), 'chat'));
    const room = el('div', 'sw-message-room');
    for (const group of groups) {
      const target = el('button', `sw-conversation${selectedId === group.communityId ? ' is-selected' : ''}`); target.type = 'button';
      const copy = el('span', 'sw-grow'); copy.append(el('strong', '', group.name), el('small', '', group.description || 'Общий чат')); target.append(communityEmblem(group), copy);
      if (group.unreadCount > 0) target.append(el('span', 'sw-message-count', group.unreadCount > 99 ? '99+' : String(group.unreadCount)));
      target.addEventListener('click', () => { this.writeRoute(`messages/${group.communityId}`); this.cleanScreen(); this.renderMessages(group.communityId); }); list.append(target);
    }
    list.append(button('Контакты и приглашения', 'people', 'sw-button-quiet sw-button-wide', () => this.runHook(() => this.options.openAccount('people'))));
    screen.append(list, room); this.main.replaceChildren(screen);
    if (selected) this.mountChat(room, selected, true);
    else room.append(emptyState('Выберите разговор', 'Здесь находятся чаты ваших сообществ.', undefined, 'chat'));
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
    const focused = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    if (focused && this.homeHandle?.element.contains(focused)) {
      if (focused.dataset.homeControl) this.homeState.focusControl = focused.dataset.homeControl;
      else if (focused.dataset.entityId) this.homeState.focusId = focused.dataset.entityId;
    }
    this.homeHandle?.destroy();
    const screen = this.screenSequence, accountCurrent = this.accountTask(), accountId = this.deskAccount;
    this.homeHandle = createAppsHome({ apps: this.apps, communities: this.communities, accountId: this.profile?.profileId || this.deskAccount,
      notes: this.homeNotes, state: this.homeState, appStatus: this.homeStatus.apps, notesStatus: this.homeStatus.notes,
      onChange: () => { this.desk.pinnedApps = [...this.homeState.pinned].slice(0, 200); this.saveDesk(); this.renderPersonal(); }, openApp: app => { void this.openApplication(app); }, openNotes: id => this.openNotes(id),
      openCommunity: (group, chat) => { void this.openGroup(group.communityId, chat ? 'chat' : 'about'); },
      inspectApp: app => this.inspectApplication(app), add: () => this.openAddMenu(), explore: () => this.navigate('world'),
      library: () => this.navigate('library'), retry: () => { void this.loadPersonal(); },
      mountSaved: host => {
        const current = (): boolean => accountCurrent() && this.screenSequence === screen;
        const open = (entry: AppResolvedEntry, discussion = false): void => {
          if (!current()) return;
          this.writeRoute(formatAppLaunchRoute({ appId: entry.appId, domainId: entry.domainId, path: entry.path }, undefined,
            discussion ? { panel: 'discussion' } : undefined));
          void this.openRoute();
        };
        const library = mountAppLibrary(host, { api: this.api, accountId, isCurrent: current,
          openEntry: entry => open(entry), discussEntry: entry => open(entry, true) });
        return () => library.dispose();
      },
      recent: () => this.openRecent(), hasRecent: this.desk.recent.length > 0,
      shortcuts: this.desk.favorites.filter(id => id !== 'apps' && id !== 'notes').flatMap(id => {
        const entry = capabilities.find(value => value.id === id); return entry ? [{ title: entry.title, symbol: entry.symbol, action: () => this.openCapability(id) }] : [];
      }),
    });
    this.main.replaceChildren(this.homeHandle.element);
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

  private saveDesk(): void { if (this.deskAccount) saveDeskPreferences(this.deskAccount, this.desk); }
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
    else if (id === 'communities') this.navigate('world');
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
      ], this.dialog('Быстрый переход'), query => { this.query = query; this.kind = 'all'; this.navigate('world'); });
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
    const dialog = this.dialog('Добавить'); const list = el('div', 'sx-profile-menu');
    const option = (title: string, symbol: string, action: () => void): void => list.append(button(title, symbol, 'sw-button-large', () => { dialog.close(); action(); }));
    option('Приложение', 'grid', () => this.openAddApp());
    option('Записку', 'note', () => this.openNotes('new'));
    option('Устройство', 'laptop', () => this.runHook(this.options.connectDevice));
    option('Сообщество', 'people', () => this.openCommunityForm());
    option('Создать с ИИ', 'sparkle', () => this.runHook(() => this.options.agentCreate()));
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

  private async loadApps(communityId?: string): Promise<WorldAppRecord[]> {
    const current = this.accountTask(), accountId = this.deskAccount;
    if (!current()) throw Object.assign(new Error('No current account'), { code: 'authentication_required' });
    const apps = this.options.listApps ? await this.options.listApps(communityId)
      : (await this.api.request<{ apps: AppProjection[] }>('apps.list', { ...(communityId ? { communityId } : {}), expectedAccountId: accountId })).apps
        .map(app => {
          const record: WorldAppRecord = { appId: app.id, name: app.name, deviceId: app.hostDeviceId,
            ...(app.deviceName ? { deviceLabel: app.deviceName } : {}), status: app.state, ownerAccountId: app.ownerAccountId,
            ...(communityId ? { communityId } : {}), ...(app.grants ? { grants: app.grants } : {}),
            ...(app.publication ? { publication: app.publication } : {}) };
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
        const cards = el('div', 'sx-app-grid sx-community-apps');
        for (const app of apps) cards.append(createApplicationCard({ app, accountId: this.profile?.profileId || this.deskAccount,
          communities: [group, ...this.communities.filter(value => value.communityId !== group.communityId)], contextCommunityId: group.communityId,
          pinned: this.homeState.pinned.has(app.appId), open: () => { void this.openApplication(app); }, inspect: () => this.inspectApplication(app),
          openCommunity: (target, chat) => { void this.openGroup(target.communityId, chat ? 'chat' : 'about'); }, togglePin: () => {
            this.homeState.pinned.has(app.appId) ? this.homeState.pinned.delete(app.appId) : this.homeState.pinned.add(app.appId);
            this.desk.pinnedApps = [...this.homeState.pinned].slice(0, 200); this.saveDesk(); return this.homeState.pinned.has(app.appId);
          } })); stack.append(cards);
      }
      if (group.membership?.state === 'active' && group.permissions.canModerate) { const actions = el('div', 'sw-row'); actions.append(button('Добавить приложение', 'plus', 'sw-button-primary', () => this.openAddApp(group.communityId)), button('Создать с ИИ', 'sparkle', '', () => this.runHook(() => this.options.agentCreate(group.communityId)))); stack.append(actions); }
      parent.replaceChildren(stack);
    } catch (error) { if (current()) parent.replaceChildren(emptyState('Приложения пока недоступны', errorText(error), button('Повторить', 'refresh', 'sw-button-quiet', () => { void this.renderApps(parent, group); }), 'grid')); }
  }

  private async openApplication(app: WorldAppRecord, intent?: AppLaunchIntent, resolveMetadata = false): Promise<void> {
    if (this.destroyed || !this.deskAccount) return;
    const previousGroup = this.group?.membership?.state === 'active' && (this.group.communityId === app.communityId || app.grants?.communityIds.includes(this.group.communityId)) ? this.group : null;
    const launchIntent = intent ?? parseAppLaunchRoute(formatAppLaunchRoute({ appId: app.appId }, previousGroup?.communityId))!;
    const communityId = launchIntent.communityId;
    const knownGroup = communityId ? [previousGroup, ...this.communities].find(value => value?.communityId === communityId && value.membership?.state === 'active') ?? null : null;
    this.group = knownGroup; this.view = 'mine'; this.renderNavigation(); this.writeRoute(launchIntent.route);
    this.avatars.setContext(knownGroup?.communityId);
    this.cleanScreen();
    const sequence = this.screenSequence, accountId = this.deskAccount, accountGeneration = this.accountGeneration;
    const current = (): boolean => !this.destroyed && this.screenSequence === sequence && this.deskAccount === accountId && this.accountGeneration === accountGeneration;
    const stage = mountAppStage(this.main, { api: this.api, app, accountId, intent: launchIntent, isCurrent: current,
      request: parameters => this.options.openApp ? this.options.openApp(app, parameters)
        : this.api.request<{ launchUrl: string; entry: AppResolvedEntry }>('apps.launch', { ...parameters }).then(result => ({ url: result.launchUrl, entry: result.entry })),
      onNavigate: (next, navigation) => {
        if (!current()) return;
        if (navigation?.replace) { history.replaceState({ soty: true }, '', '#' + next.route); this.activeRoute = '#' + next.route; }
        else this.writeRoute(next.route);
      },
      onBack: () => this.afterNoteSaved(() => {
        if (!current()) return;
        if (this.group) { this.groupTab = 'apps'; this.renderGroup(); } else this.navigate('mine');
      }),
      onAccount: async () => {
        await stage.flush();
        if (!current()) return;
        if (stage.hasUnsavedChanges()) { this.toast('Скопируйте черновик перед сменой аккаунта', true); return; }
        await this.options.openAccount('recovery'); if (current()) await this.refresh();
      },
      onSettings: (value, onUpdated) => { if (current()) this.openAppSettings(value, onUpdated); },
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
    if (resolveMetadata) void this.loadApps(communityId).then(apps => {
      if (!current()) return;
      const found = apps.find(value => value.appId === launchIntent.target.appId);
      if (found) { app = found; stage.updateApp(found); }
    }).catch(() => { /* Optional names never change the exact admitted entry. */ });
  }
  private openAddApp(communityId?: string, selectedDevice?: string): void {
    const dialog = this.dialog('Добавить приложение'); dialog.body.append(this.loading('Ищем ваши устройства'));
    void this.api.request<{ devices: DeviceProjection[] }>('apps.devices', {}).then(result => {
      if (!dialog.element.open) return;
      const devices = result.devices.filter(device => device.claimed);
      if (!devices.length) { dialog.body.replaceChildren(emptyState('Подключите компьютер', 'Проект будет работать на вашем устройстве и открываться здесь.', button('Подключить устройство', 'laptop', 'sw-button-primary', () => { dialog.close(); this.runHook(this.options.connectDevice); }), 'laptop')); return; }
      const form = el('form'); const name = textInput('', 'Например, Галерея выходных', 64); name.required = true;
      const device = el('select', 'sw-select'); devices.forEach(item => { const option = el('option', '', `${item.name}${item.online ? '' : ' · не в сети'}`); option.value = item.hostDeviceId; option.selected = item.hostDeviceId === selectedDevice; device.append(option); });
      const port = el('input', 'sw-input'); port.type = 'number'; port.min = '1024'; port.max = '65535'; port.placeholder = '3000'; port.required = true;
      const path = textInput('/', '/'); path.pattern = '/.*';
      const audience = el('select', 'sw-select'); const privateOption = el('option', '', 'Только мне'); privateOption.value = ''; audience.append(privateOption);
      this.communities.filter(group => group.membership?.state === 'active' && group.permissions.canModerate).forEach(group => { const option = el('option', '', group.name); option.value = group.communityId; option.selected = group.communityId === communityId; audience.append(option); });
      const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const save = button('Добавить соту', 'plus', 'sw-button-primary sw-button-wide'); save.type = 'submit';
      form.append(labeledField('Название', name), labeledField('Устройство', device), labeledField('Порт проекта', port, 'Проект уже должен работать на этом компьютере.'), labeledField('Начальная страница', path), labeledField('Кому открыть', audience), error, save);
      form.addEventListener('submit', event => {
        event.preventDefault(); const chosen = devices.find(item => item.hostDeviceId === device.value); if (!chosen || !form.reportValidity()) return;
        save.disabled = true; error.textContent = '';
        void this.api.request('apps.register', { hostDeviceId: chosen.hostDeviceId, connectorId: chosen.connectorId, name: name.value.trim(), port: Number(port.value), entryPath: path.value.trim() || '/', grants: { accountIds: [], communityIds: audience.value ? [audience.value] : [] } }).then(() => { dialog.close(); this.toast('Приложение добавлено в ваши соты'); if (this.group) { this.groupTab = 'apps'; this.renderGroup(); } else { this.navigate('mine'); } }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { save.disabled = false; });
      }); dialog.body.replaceChildren(form); name.focus();
    }).catch(error => { if (dialog.element.open) dialog.body.replaceChildren(el('div', 'sw-error', errorText(error))); });
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
        const before = JSON.stringify([app.name, app.grants, app.status, app.publication, app.audience, app.deviceId, app.deviceLabel]);
        app = { ...app, name: snapshot.app.name, grants: snapshot.app.grants,
          deviceId: snapshot.source.hostDeviceId, deviceLabel: snapshot.source.deviceName,
          status: snapshot.app.state === 'revoked' ? 'revoked' : ({ offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' } as const)[snapshot.source.observation.state],
          publication: publicationFromInspection(snapshot) };
        app.audience = describeAppAudience(app, accountId).label;
        changed ||= before !== JSON.stringify([app.name, app.grants, app.status, app.publication, app.audience, app.deviceId, app.deviceLabel]);
        this.apps = this.apps.map(value => value.appId === app.appId ? { ...value, ...app } : value);
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
    form.append(identity, labeledField('Имя', name), labeledField('О себе', bio), labeledField('Интересы', interests, 'Через запятую. По ним вас смогут найти.'), error, save);
    form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity()) return; save.disabled = true; error.textContent = ''; void this.updateProfile({ displayName: name.value.trim(), bio: bio.value.trim(), interests: interests.value.split(',').map(item => item.trim().slice(0, 32)).filter(Boolean).slice(0, 8), avatarColor: worldColors[color] }).then(async () => { if (pendingAvatar && this.profile) { const result = await this.api.request<{ profile: WorldProfile }>('world.profile.avatar.set', { expectedRevision: this.profile.revision, ...pendingAvatar }); this.profile = result.profile; this.avatars.setContext(this.group?.membership?.state === 'active' ? this.group.communityId : undefined); this.renderNavigation(); if (this.view === 'mine' && !this.group) this.renderPersonal(); } dialog.close(); this.toast('Профиль сохранён'); }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { save.disabled = false; }); });
    const extra = el('div', 'sw-stack'); extra.append(button('Видимость', 'eye', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.openVisibility(); }), button('Оформление', 'settings', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.openAppearance(); }), button('Аккаунт, устройства и контакты', 'person', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.runHook(this.options.openAccount); }), button('Прежние комнаты и инструменты', 'chat', 'sw-button-quiet sw-button-wide', () => { dialog.close(); this.runHook(this.options.openLegacy); })); dialog.body.append(form, el('hr', 'sw-rule'), extra);
  }

  private colorPicker(current: string, change: (color: string) => void): HTMLElement {
    const colors = el('div', 'sw-setting-options'); colors.setAttribute('role', 'group'); colors.setAttribute('aria-label', 'Цвет');
    const names = { honey: 'Медовый', sage: 'Шалфей', lilac: 'Сиреневый', coral: 'Коралловый', blue: 'Голубой' };
    for (const [value, label] of Object.entries(names)) { const target = el('button', `sw-choice-color sw-color-${value}`); target.type = 'button'; target.setAttribute('aria-label', label); target.title = label; target.setAttribute('aria-pressed', String(value === current)); target.addEventListener('click', () => { colors.querySelectorAll('button').forEach(node => node.setAttribute('aria-pressed', String(node === target))); change(value); }); colors.append(target); } return colors;
  }

  private openCommunityForm(existing?: WorldCommunity): void {
    const dialog = this.dialog(existing ? 'Настроить сообщество' : 'Создать сообщество'); const form = el('form');
    const name = textInput(existing?.name ?? '', 'Например, Фотоклуб', 80); name.required = true;
    const description = textInput(existing?.description ?? '', 'Что вас объединяет', 240);
    const topics = textInput(existing?.topics.join(', ') ?? '', 'Фото, прогулки, творчество', 240);
    const showcase = el('textarea', 'sw-textarea'); showcase.value = existing?.showcase ?? ''; showcase.maxLength = 2500; showcase.placeholder = 'Пригласите людей: расскажите, чем здесь можно заняться.';
    const access = el('select', 'sw-select'); for (const [value, label] of [['open', 'Любой может вступить'], ['request', 'По заявке'], ['invite', 'Только по приглашению']] as const) { const option = el('option', '', label); option.value = value; option.selected = (existing?.joinPolicy ?? 'open') === value; access.append(option); }
    const canChangeAccess = !existing || existing.membership?.role === 'owner'; access.disabled = !canChangeAccess;
    let color = worldColor(existing?.color), symbol = existing?.symbol ?? 'cells';
    const styles = el('div', 'sw-stack'); styles.append(this.colorPicker(color, value => { color = worldColor(value); })); const icons = el('div', 'sw-setting-options'); icons.setAttribute('role', 'group'); icons.setAttribute('aria-label', 'Значок сообщества');
    const iconNames: Record<string, string> = { cells: 'Соты', tools: 'Мастерская', camera: 'Фотография', game: 'Игры', music: 'Музыка', bulb: 'Идеи', image: 'Галерея', people: 'Люди' };
    communityIcons.forEach(value => { const target = el('button', 'sw-choice-icon'); target.type = 'button'; target.setAttribute('aria-label', iconNames[value] ?? value); target.title = iconNames[value] ?? value; target.setAttribute('aria-pressed', String(value === symbol)); target.append(icon(value)); target.addEventListener('click', () => { symbol = value; icons.querySelectorAll('button').forEach(node => node.setAttribute('aria-pressed', String(node === target))); }); icons.append(target); }); styles.append(icons);
    const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); const save = button(existing ? 'Сохранить' : 'Создать сообщество', existing ? 'check' : 'plus', 'sw-button-primary sw-button-wide'); save.type = 'submit'; const requestId = crypto.randomUUID();
    form.append(labeledField('Название', name), labeledField('Коротко о сообществе', description), labeledField('Темы', topics), labeledField('Облик', styles), labeledField('Вступление', access), labeledField('Витрина', showcase), error, save);
    form.addEventListener('submit', event => {
      event.preventDefault(); if (!form.reportValidity()) return; save.disabled = true; error.textContent = '';
      const parameters: Record<string, unknown> = { name: name.value.trim(), description: description.value.trim(), topics: topics.value.split(',').map(value => value.trim().slice(0, 32)).filter(Boolean).slice(0, 8), ...(canChangeAccess ? { joinPolicy: access.value } : {}), showcase: showcase.value.trim(), color: worldColors[color], symbol, ...(existing ? { communityId: existing.communityId, expectedRevision: existing.revision } : { requestId }) };
      void this.api.request<{ community: WorldCommunity }>(existing ? 'world.community.update' : 'world.community.create', parameters).then(response => { if (this.destroyed) return; this.updateCommunity(response.community); dialog.close(); this.group = response.community; this.groupTab = 'about'; this.renderGroup(); this.toast(existing ? 'Сообщество обновлено' : 'Ваше сообщество готово'); }).catch(reason => { error.textContent = errorText(reason); }).finally(() => { save.disabled = false; });
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
    let timer: ReturnType<typeof setTimeout> | null = null, sequence = 0;
    const dialog = this.dialog('Пригласить в сообщество', () => { if (timer) clearTimeout(timer); sequence++; }); const input = textInput('', 'Имя или интерес', 100); input.setAttribute('aria-label', 'Найти человека');
    const results = el('div', 'sw-stack'); const error = el('div', 'sw-error'); error.setAttribute('role', 'alert'); dialog.body.append(input, el('hr', 'sw-rule'), results, error);
    const search = async (): Promise<void> => {
      const request = ++sequence; results.replaceChildren(this.loading('Ищем людей'));
      try {
        const response = await this.api.request<WorldSearch>('world.discovery.search', { query: input.value.trim(), kind: 'people', limit: 24 });
        if (!dialog.element.open || request !== sequence) return; results.replaceChildren();
        const people = response.people.filter(profile => profile.profileId !== this.profile?.profileId);
        if (!people.length) results.append(el('p', 'sw-muted', 'Никого не нашли. Попробуйте другое имя.'));
        people.forEach(profile => { const row = el('div', 'sw-member-row'); row.append(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), el('strong', 'sw-grow', profile.displayName)); const invite = button('Пригласить', 'plus', 'sw-button-small', () => { invite.disabled = true; void this.api.request('world.membership.invite', { communityId: group.communityId, profileId: profile.profileId }).then(() => { invite.replaceChildren(icon('check'), el('span', '', 'Приглашён')); }).catch(reason => { error.textContent = errorText(reason); invite.disabled = false; }); }); row.append(invite); results.append(row); });
      } catch (reason) { if (dialog.element.open) results.replaceChildren(el('div', 'sw-error', errorText(reason))); }
    };
    input.addEventListener('input', () => { if (timer) clearTimeout(timer); timer = setTimeout(() => { void search(); }, 230); }); void search(); input.focus();
  }
}
