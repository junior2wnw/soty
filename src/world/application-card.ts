import { button, el, iconButton } from './dom';
import { icon } from './icons';
import { worldColor, type WorldAppRecord, type WorldCommunity } from './types';

export function appStatusLabel(status: string): string {
  return ({ ready: 'Работает', starting: 'Запускается', offline: 'Устройство не в сети', stopped: 'Остановлено', revoked: 'Доступ закрыт' } as Record<string, string>)[status] ?? 'Проверяем состояние';
}

export function appTone(app: WorldAppRecord): string {
  if (app.color) return worldColor(app.color);
  const hash = [...app.appId].reduce((value, char) => (value * 31 + char.charCodeAt(0)) >>> 0, 0);
  return ['honey', 'sage', 'lilac', 'blue', 'coral'][hash % 5]!;
}

export interface ApplicationCardOptions {
  app: WorldAppRecord; accountId: string; communities: WorldCommunity[]; pinned: boolean;
  contextCommunityId?: string;
  open(): void;
  inspect(): void;
  openCommunity(group: WorldCommunity, chat?: boolean): void;
  togglePin(): boolean;
}

/** One app identity in every placement. A conversation always names its actual community. */
export function createApplicationCard(options: ApplicationCardOptions): HTMLElement {
  const { app } = options, symbol = app.symbol || 'app';
  const card = el('article', `sx-app-card sw-color-${appTone(app)}`);
  const launch = el('button', 'sx-app-launch'); launch.type = 'button'; launch.dataset.entityId = `app:${app.appId}`;
  launch.setAttribute('aria-label', `Открыть ${app.name}`);
  launch.setAttribute('aria-description', appStatusLabel(app.status)); launch.addEventListener('click', options.open);
  const cover = el('div', 'sx-app-cover'); cover.setAttribute('aria-hidden', 'true');
  const backdrop = el('span', 'sx-cover-pattern'); backdrop.append(icon('cells'));
  const glyph = el('span', 'sx-cover-glyph'); glyph.append(icon(symbol));
  const status = el('span', 'sx-cover-status', appStatusLabel(app.status)); status.dataset.status = app.status;
  cover.append(backdrop, glyph, el('strong', 'sx-cover-name', app.name), status);
  const identity = el('div', 'sx-card-identity'); const mark = el('span', 'sx-card-mark soty-hex'); mark.append(icon(symbol));
  const text = el('span', 'sx-card-copy'); text.append(el('strong', '', app.name), el('span', '', app.description || app.deviceLabel || 'Приложение в Сотах'));
  identity.append(mark, text, icon('diagonal')); launch.append(cover, identity);
  const context = el('div', 'sx-card-context');
  const groups = options.communities.filter(group => group.membership?.state === 'active' &&
    (group.communityId === app.communityId || app.grants?.communityIds.includes(group.communityId)));
  const group = groups.find(value => value.communityId === options.contextCommunityId) ?? groups[0];
  if (group) {
    context.append(button(group.name, 'people', 'sx-card-scope', () => options.openCommunity(group)));
    context.append(iconButton(`Чат сообщества ${group.name}`, 'chat', () => options.openCommunity(group, true)));
  } else {
    const shared = !!app.grants?.accountIds.length || !!app.grants?.communityIds.length;
    const scope = el('span', 'sx-card-scope'); scope.append(icon(shared ? 'people' : 'lock'),
      el('span', '', app.audience || (shared ? 'Выбранным участникам' : app.ownerAccountId === options.accountId ? 'Личное' : 'Вам доступно'))); context.append(scope);
  }
  context.append(iconButton(`Связи и доступ: ${app.name}`, 'connections', options.inspect));
  const pin = iconButton(`${options.pinned ? 'Открепить здесь' : 'Закрепить здесь'}: ${app.name}`, 'pin', () => {
    const pinned = options.togglePin(); pin.setAttribute('aria-pressed', String(pinned));
    pin.setAttribute('aria-label', `${pinned ? 'Открепить здесь' : 'Закрепить здесь'}: ${app.name}`); pin.title = pin.getAttribute('aria-label')!;
  });
  pin.dataset.homeControl = `pin:${app.appId}`;
  pin.setAttribute('aria-pressed', String(options.pinned)); context.append(pin);
  card.append(launch, context); return card;
}
