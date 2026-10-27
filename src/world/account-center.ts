import './account-center.css';
import { avatar, button, el } from './dom';

export type AccountSection = 'profile' | 'notifications' | 'appearance' | 'ai' | 'security' | 'data';
export type AccountAction = 'edit-profile' | 'profile-visibility' | 'switch-profile' | 'appearance' | 'notifications'
  | 'auth-devices' | 'recovery' | 'data-export' | 'ai-api' | 'human-signins' | 'agent-access' | 'device-status' | 'help' | 'developers';
export interface AccountCenterOptions {
  host: HTMLElement;
  identity: { displayName: string; bio?: string; avatarUrl?: string | null; profileId?: string; avatarRevision?: number | null; color?: string };
  isCurrent(): boolean;
  onAction(action: AccountAction): void;
  availableActions?: AccountAction[];
  initialSection?: AccountSection;
}

/** One navigation surface; actions open the existing, scope-owned editors. */
export function mountAccountCenter(options: AccountCenterOptions): { dispose(): void; select(section: AccountSection): void } {
  const root = el('div', 'sw-account-center'), navigation = el('nav', 'sw-account-navigation');
  navigation.setAttribute('aria-label', 'Настройки профиля');
  const body = el('section', 'sw-account-content'); body.setAttribute('aria-label', 'Настройки выбранного раздела');
  const identity = el('div', 'sw-account-identity'), copy = el('div');
  copy.append(el('strong', '', options.identity.displayName), el('p', 'sw-muted', 'Ваш профиль в Сотах'));
  identity.append(avatar(options.identity.displayName, options.identity.avatarUrl, options.identity.color, options.identity.profileId, options.identity.avatarRevision), copy);
  const available = (action: AccountAction) => !options.availableActions || options.availableActions.includes(action);
  const sectionActions: Record<Exclude<AccountSection, 'profile'>, AccountAction[]> = {
    notifications: ['notifications'], appearance: ['appearance'], ai: ['ai-api'],
    security: ['auth-devices', 'recovery', 'human-signins', 'agent-access'],
    data: ['device-status', 'data-export', 'help', 'developers'],
  };
  const allSections: [AccountSection, string, string, string][] = [
    ['profile', 'Профиль', 'person', 'Имя, фото и видимость для других людей'],
    ['notifications', 'Уведомления', 'bell', 'Получение уведомлений на этом устройстве'],
    ['appearance', 'Вид и управление', 'sun', 'Тема и управление в этом браузере'],
    ['ai', 'ИИ и API', 'assistant', 'Использование ИИ и ключи API этого профиля'],
    ['security', 'Безопасность', 'shield', 'Входы в профиль и разрешённые действия'],
    ['data', 'Данные и помощь', 'folder', 'Копии, состояние Сот и поддержка'],
  ];
  const sections = allSections.filter(([id]) => id === 'profile' || sectionActions[id].some(available));
  let disposed = false, section: AccountSection = sections.some(([id]) => id === options.initialSection) ? options.initialSection! : 'profile';
  const current = () => !disposed && options.host.isConnected && options.isCurrent();
  const entries = new Map<AccountSection, HTMLButtonElement>();
  const invoke = (action: AccountAction) => { if (current() && available(action)) options.onAction(action); };
  const action = (value: AccountAction, label: string, icon: string, scope: string): void => {
    if (!available(value)) return;
    const control = button(label, icon, 'sw-button-quiet sw-account-action', () => invoke(value));
    control.dataset.accountAction = value;
    control.append(el('small', 'sw-muted', scope)); body.append(control);
  };
  const render = (): void => {
    if (!current()) return;
    for (const [id, control] of entries) control.setAttribute('aria-current', id === section ? 'page' : 'false');
    body.replaceChildren(el('h3', '', sections.find(value => value[0] === section)![1]));
    if (section === 'profile') {
      if (options.identity.bio) body.append(el('p', 'sw-muted', options.identity.bio));
      action('edit-profile', 'Имя, фото и описание', 'edit', 'Данные этого профиля');
      action('profile-visibility', 'Кто видит мой профиль', 'eye', 'Публичность профиля и его разделов');
      action('switch-profile', 'Выбрать другой профиль', 'person', 'Известные профили на этом устройстве');
    } else if (section === 'appearance') {
      action('appearance', 'Тема и управление', 'sun', 'Этот браузер · изменения можно отменить');
    } else if (section === 'notifications') {
      action('notifications', 'Настроить уведомления', 'bell', 'Этот профиль и этот браузер');
      body.append(el('p', 'sw-muted', 'Тихий режим отдельного разговора меняется в самом разговоре.'));
    } else if (section === 'ai') {
      action('ai-api', 'ИИ и API', 'assistant', 'Использование ИИ и ключи API этого профиля');
    } else if (section === 'security') {
      action('auth-devices', 'Устройства входа', 'phone', 'Устройства с доступом к этому профилю');
      action('recovery', 'Восстановление профиля', 'lock', 'Запасной способ входа · это не копия данных');
      action('human-signins', 'Входы в приложения', 'lock', 'Входы через Соты и запомненные согласия');
      action('agent-access', 'Действия ИИ и приложений', 'shield', 'Права на действия, сроки и история выполнения');
      body.append(el('p', 'sw-muted', 'Компьютеры, где работают приложения, находятся в управлении размещением. Отзыв входа и выключение компьютера — разные действия.'));
    } else {
      action('device-status', 'Соты на этом устройстве', 'phone', 'Установка, обновление и доступность без сети');
      action('data-export', 'Копии и экспорт данных', 'download', 'Показываем, какие данные входят в копию');
      action('help', 'Помощь и поддержка', 'info', 'Вопросы о Сотах; помощь приложению — в приложении');
      action('developers', 'Для разработчиков и ИИ', 'connections', 'Инструкции интеграции и доступные контракты');
    }
  };
  const select = (next: AccountSection): void => {
    if (!current() || !entries.has(next)) return;
    // Frequent categories lead straight to their canonical editor, without an extra landing page.
    if (next === 'appearance' && available('appearance')) { invoke('appearance'); return; }
    if (next === 'notifications' && available('notifications')) { invoke('notifications'); return; }
    if (next === 'ai' && available('ai-api')) { invoke('ai-api'); return; }
    section = next; render();
  };
  for (const [id, label, icon, hint] of sections) {
    const control = button(label, icon, 'sw-button-quiet sw-account-section', () => select(id));
    control.dataset.accountSection = id; control.title = hint;
    entries.set(id, control); navigation.append(control);
  }
  root.append(identity, navigation, body); options.host.append(root); render();
  return { select, dispose() { disposed = true; root.remove(); } };
}
