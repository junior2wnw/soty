import './world.css';
import { mountAccountCenter } from './account-center';
import { mountAppSettings } from './app-settings';
import { createDialog } from './dialogs';
import { createThemeController, createThemeControls } from './theme/theme';
import { button, el } from './dom';
import type { AppInspection } from './app-settings.types';
import type { WorldApi } from './types';

if (!import.meta.env.DEV) throw Error('Settings fixture is development-only');
const params = new URLSearchParams(location.search), app = document.querySelector<HTMLElement>('#app')!;
app.className = 'sw-app'; app.dataset.experience = 'one-field';
const controller = createThemeController({ initial: { themeMode: params.get('theme') === 'dark' ? 'dark' : 'light', themeBrightness: 50 }, onChange() {} });
const appId = `app-${'1'.repeat(32)}`, aliasId = `dom_${'2'.repeat(32)}`;
const snapshot: AppInspection = {
  schema: 'soty.app-inspection.v1', checkedAt: Date.now(),
  app: { id: appId, name: 'Длинное название приложения для работы и обсуждения', state: 'enabled', revision: 1, grants: { accountIds: [], communityIds: [] } },
  addresses: { revision: 1, claimOrigin: 'https://apps.example.test', canonical: null,
    aliases: [{ id: aliasId, slug: 'sample', origin: 'https://sample.apps.example.test', state: 'bound', active: false, shareUrl: null, createdAt: 1, retiredAt: null }], limits: { perApp: 10, perAccount: 100, usedByApp: 1, usedByAccount: 1 } },
  publication: { policyEpoch: 1, launchPolicy: 'restricted', audience: 'private', listed: false, activeDomainIds: [], activeTargetRevision: 1 },
  source: { hostDeviceId: 'fixture-host', connectorId: 'fixture-connector', deviceName: 'Компьютер с размещённым приложением', port: 8080, entryPath: '/', revision: 1, digest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', requiredBindingVersion: 2,
    binding: { state: 'bound' }, observation: { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' } },
  actions: { canReserveName: true, canEdit: true, canPublish: true, canPreview: true }
};
const status = el('p', 'sw-muted'); status.id = 'fixture-result';
const api = { async request(op: string, args: Record<string, unknown>) {
  if (params.has('participant')) throw Object.assign(Error('Only owner'), { code: 'apps_owner_required' });
  if (op === 'apps.inspect') return structuredClone(snapshot);
  if (op === 'apps.update') {
    if (args.expectedAccountId !== 'fixture-owner' || args.expectedRevision !== snapshot.app.revision) throw Error('fixture_owner_or_revision_changed');
    snapshot.app.name = String(args.name); snapshot.app.revision++; status.textContent = `Сохранено на fixture API: ${snapshot.app.name}`; return { app: snapshot.app };
  }
  throw Error('This fixture does not execute publication, grants or source changes');
} } as unknown as WorldApi;
const open = (mode: string): void => {
  const dialog = createDialog(mode === 'owner' ? 'Управление приложением' : mode === 'theme' ? 'Вид и управление' : 'Профиль и настройки', undefined, undefined, app);
  if (mode === 'owner') {
    dialog.element.classList.add('sw-app-settings-dialog');
    const handle = mountAppSettings({ host: dialog.body, accountId: 'fixture-owner', appId, api, communities: [], isCurrent: () => dialog.element.open,
      aboutActions: [button('Обложка приложения', 'image', '', () => { status.textContent = 'Канонический редактор обложки'; })],
      supportActions: [button('Подключить агента к обращениям', 'shield', '', () => { status.textContent = 'Узкое разрешение очереди обращений'; })],
      onChanged() {}, onPreview() {}, onClose(after) { handle.dispose(); dialog.close(); after?.(); } });
    dialog.element.addEventListener('close', () => handle.dispose(), { once: true });
  } else if (mode === 'theme') {
    const controls = createThemeControls(controller); dialog.body.append(controls.element);
    dialog.element.addEventListener('close', () => controls.destroy(), { once: true });
  } else {
    const panel = mountAccountCenter({ host: dialog.body, identity: { displayName: 'Александра · личный рабочий профиль', bio: 'Работа, приложения и разговоры' },
      isCurrent: () => dialog.element.open, onAction(action) { status.textContent = action; if (action === 'appearance') { dialog.close(); open('theme'); } } });
    dialog.element.addEventListener('close', () => panel.dispose(), { once: true });
  }
};
app.append(button('Профиль и настройки', 'person', '', () => open('account')), button('Управление приложением', 'app', '', () => open('owner')), status);
open(params.get('mode') ?? 'account');
