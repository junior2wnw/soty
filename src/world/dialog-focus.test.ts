import { createDialog } from './dialogs';
import { button, el } from './dom';
import type { WorldAppHandle } from './app';
import type { AppInspection } from './app-settings.types';
import type { WorldApi, WorldAppRecord, WorldCommunity, WorldProfile } from './types';

if (!import.meta.env.DEV) throw new Error('Development fixture only');

// This realm never reads or writes the user's persistent UI/draft records.
// It still exercises the production storage consumers, with an in-memory port.
const memory = new Map<string, string>();
const memoryStorage: Storage = {
  get length() { return memory.size; }, clear: () => memory.clear(),
  key: index => [...memory.keys()][index] ?? null,
  getItem: key => memory.get(key) ?? null, setItem: (key, value) => { memory.set(key, String(value)); },
  removeItem: key => { memory.delete(key); },
};
Object.defineProperty(window, 'localStorage', { configurable: true, value: memoryStorage });
const { mountWorldApp } = await import('./app');
const host = document.querySelector<HTMLElement>('#qa-host')!, output = document.querySelector<HTMLElement>('#qa-results')!;
const outside = document.querySelector<HTMLButtonElement>('#qa-outside')!;
const APP = `app-${'12'.repeat(16)}`, DOMAIN = `dom_${'34'.repeat(16)}`;
const GROUP = 'group-dialog-focus-fixture';
const ORIGIN = 'https://focus-fixture.invalid', ACCOUNT = `dialog-focus-${crypto.randomUUID()}`;
const clone = <T>(value: T): T => structuredClone(value);
const delay = (ms = 20): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const paint = (): Promise<void> => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
async function until(check: () => unknown, label: string): Promise<void> {
  output.dataset.step = label; const deadline = performance.now() + 5000;
  while (!check()) { if (performance.now() > deadline) throw new Error(`Timeout: ${label}`); await delay(); }
}
function control<T extends HTMLElement = HTMLElement>(selector: string, parent: ParentNode = document): T {
  const value = parent.querySelector<T>(selector); assert(value, `Missing: ${selector}`); return value;
}
function named(label: string, parent: ParentNode = document): HTMLButtonElement {
  const value = [...parent.querySelectorAll<HTMLButtonElement>('button')].find(node => node.textContent?.trim() === label || node.getAttribute('aria-label') === label);
  assert(value, `Missing button: ${label}`); return value;
}
function activate(node: HTMLElement): void { node.focus(); node.click(); }
function escape(dialog: HTMLDialogElement): void {
  const event = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
  dialog.dispatchEvent(event); assert(event.defaultPrevented, 'Managed Escape was not prevented');
}
const earlyErrors: string[] = [];
window.addEventListener('error', event => earlyErrors.push(event.message));
window.addEventListener('unhandledrejection', event => earlyErrors.push(String(event.reason?.message ?? event.reason)));

// Preserve actual iframe identity/focus APIs; only the runtime document is a
// synthetic srcdoc and never sends an HTTP request to the declared origin.
const originalCreate = document.createElement;
document.createElement = function (tag: string, options?: ElementCreationOptions): HTMLElement {
  const node = originalCreate.call(document, tag, options);
  if (tag.toLowerCase() === 'iframe') node.setAttribute('srcdoc', '<!doctype html><html lang="ru"><meta charset="utf-8"><label>Текст внутри приложения <input id="value" value="before"></label><p>Синтетический runtime.</p></html>');
  return node;
} as typeof document.createElement;

let world: WorldAppHandle | null = null, running = false;
function stop(): void { world?.destroy(); world = null; host.replaceChildren(); }
function backend() {
  let account = ACCOUNT, name = 'Пример фокуса', revision = 1, updates = 0;
  let groupWait: Promise<void> | null = null;
  const record = (): WorldAppRecord => ({ appId: APP, name, ownerAccountId: account, deviceId: 'fixture-device', deviceLabel: 'Устройство стенда',
    status: 'offline', audience: 'Личный доступ', grants: { accountIds: [], communityIds: [] }, publication: { launchPolicy: 'restricted', activeNamedAddressCount: 0 } });
  const inspection = (): AppInspection => ({ schema: 'soty.app-inspection.v1', checkedAt: Date.now(),
    app: { id: APP, name, state: 'enabled', revision, grants: { accountIds: [], communityIds: [] } },
    addresses: { revision: 1, claimOrigin: null, canonical: { id: DOMAIN, origin: ORIGIN, shareUrl: `${location.origin}/#launch/${APP}/${DOMAIN}?path=%2Fboard` }, aliases: [],
      limits: { perApp: 3, perAccount: 10, usedByApp: 0, usedByAccount: 0 } },
    publication: { policyEpoch: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [], activeTargetRevision: 1 },
    source: { hostDeviceId: 'fixture-device', connectorId: 'fixture-connector', deviceName: 'Устройство стенда', port: 54321, entryPath: '/board', revision: 1,
      digest: 'ab'.repeat(32), profile: 'soty.relay-restricted.v1', requiredBindingVersion: 1, binding: { state: 'offline' },
      observation: { state: 'offline', observedAt: null, freshUntil: null, evidence: 'connector-offline' } },
    actions: { canEdit: true, canPublish: true, canPreview: true, canReserveName: false } });
  const profile = (): WorldProfile => ({ profileId: account, displayName: 'Участник стенда', bio: '', interests: [], avatarColor: '#DDA149', revision: 1 });
  const community = (): WorldCommunity => ({ communityId: GROUP, name: 'Сообщество стенда', description: '', topics: [], joinPolicy: 'invite', showcase: '', symbol: 'people', color: 'honey', revision: 1,
    memberCount: 1, previewMembers: [], membership: { state: 'active', role: 'owner', pinned: false, muted: false, showInProfile: false, revision: 1 },
    permissions: { canManage: true, canModerate: true, canWrite: true }, unreadCount: 0 });
  const api: WorldApi = { async request<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
    let value: unknown;
    if (op === 'world.profile.get') value = { profile: profile() };
    else if (op === 'world.community.list') value = { communities: [] };
    else if (op === 'world.community.get') { assert(args.communityId === GROUP, 'Unexpected group'); value = { community: community() }; }
    else if (op === 'apps.devices') value = { devices: [] };
    else if (op === 'notes.list') value = { notes: [] };
    else if (op === 'apps.saved.list') value = { revision: 0, entries: [], nextCursor: null };
    else if (op === 'apps.saved.get') value = { revision: 0, entry: null };
    else if (op === 'apps.inspect') value = inspection();
    else if (op === 'apps.update') {
      assert(args.expectedAccountId === account && args.expectedRevision === revision, 'Wrong name CAS/account');
      assert(typeof args.name === 'string', 'Unexpected update'); name = args.name; revision++; updates++; value = { app: record() };
    } else if (op === 'apps.entry.get') value = { entry: { appId: APP, domainId: DOMAIN, origin: ORIGIN, path: '/board' } };
    else throw new Error(`Unexpected synthetic API: ${op}`);
    return clone(value) as T;
  } };
  return { api, record, profile, inspection, updates: () => updates, account: () => account, setAccount(value: string) { account = value; },
    async listApps(groupId?: string) { if (groupId && groupWait) await groupWait; return [record()]; },
    holdGroupApps() { let release!: () => void; groupWait = new Promise<void>(resolve => { release = resolve; }); return () => { groupWait = null; release(); }; },
  };
}
async function start() {
  stop(); memory.clear(); history.replaceState({ soty: true }, '', '#mine');
  const f = backend();
  world = mountWorldApp(host, { api: f.api, localAccount: async () => ({ accountId: f.account(), label: 'Fixture' }),
    listApps: f.listApps, listDevices: async () => [], openLegacy() {}, openAccount() {}, connectDevice() {}, agentCreate() {},
    openApp: async (_app, target) => ({ url: `${ORIGIN}/_soty/boot?path=%2Fboard#${'t'.repeat(43)}`,
      entry: { appId: APP, domainId: DOMAIN, origin: ORIGIN, path: target?.path ?? '/board' } }),
  });
  await until(() => host.querySelector('[data-app-action="inspect"]'), 'Application card'); await paint(); return f;
}
async function settingsFromCard(): Promise<{ opener: HTMLElement; dialog: HTMLDialogElement; input: HTMLInputElement }> {
  const opener = control<HTMLElement>('[data-app-action="inspect"]', host); activate(opener);
  activate(named('Название и доступ', control('dialog[open]')));
  const dialog = control<HTMLDialogElement>('.sw-app-settings-dialog[open]');
  await until(() => dialog.querySelector<HTMLInputElement>('[data-settings-key="name"]')?.value, 'Settings inspection');
  return { opener, dialog, input: control<HTMLInputElement>('[data-settings-key="name"]', dialog) };
}
async function rename(f: ReturnType<typeof backend>, dialog: HTMLDialogElement, text = 'Переименованное приложение'): Promise<void> {
  const input = control<HTMLInputElement>('[data-settings-key="name"]', dialog); input.focus(); input.value = text; input.dispatchEvent(new Event('input', { bubbles: true }));
  activate(control('[data-settings-key="save-name"]', dialog));
  await until(() => f.updates() === 1 && dialog.textContent?.includes('Сохранение названия подтверждено.') &&
    control('[data-settings-key="refresh"]', dialog).getAttribute('aria-disabled') === 'false', 'Confirmed rename');
}

const cases: Array<[string, () => Promise<void>]> = [
  ['Rename: same app action after main rerender; no delayed frame steals focus', async () => {
    const f = await start(), { opener, dialog } = await settingsFromCard(); await rename(f, dialog);
    activate(control('.sw-dialog-header button', dialog));
    const replacement = control('[data-app-action="inspect"]', host);
    assert(replacement !== opener && document.activeElement === replacement, 'Focus not on recreated same-app inspect');
    assert(replacement.getAttribute('aria-label')?.includes('Переименованное'), 'Stale action label');
    outside.focus(); await paint(); await delay(); assert(document.activeElement === outside, 'Queued close/home frame stole focus');
  }],
  ['Unchanged settings return to surviving action', async () => {
    await start(); const { opener, dialog } = await settingsFromCard(); activate(control('.sw-dialog-header button', dialog)); await paint();
    assert(document.activeElement === opener && opener.isConnected, 'Surviving opener lost');
  }],
  ['Two managed Escapes and native cancel preserve unsaved field; explicit discard closes', async () => {
    await start(); const { dialog, input, opener } = await settingsFromCard(); input.focus(); input.value = 'Не терять ввод'; input.dispatchEvent(new Event('input', { bubbles: true })); input.setSelectionRange(2, 6);
    escape(dialog); assert(dialog.open, 'First Escape closed dirty settings');
    escape(dialog); assert(dialog.open && document.activeElement === input && input.value === 'Не терять ввод', 'Second Escape lost field/focus');
    assert(input.selectionStart === 2 && input.selectionEnd === 6, 'Selection lost');
    const cancel = new Event('cancel', { cancelable: true }); dialog.dispatchEvent(cancel);
    assert(cancel.defaultPrevented && dialog.open, 'Native cancel bypassed unsaved guard');
    activate(control('[data-settings-key="confirm-no"]', dialog)); assert(document.activeElement === input, 'Cancel did not return to field');
    activate(control('.sw-dialog-header button', dialog)); activate(control('[data-settings-key="confirm-yes"]', dialog)); await paint();
    assert(!dialog.open && document.activeElement === opener, 'Discard did not return to opener');
  }],
  ['Shared lifecycle runs cleanup once before return; immediate follow-up modal keeps focus', async () => {
    stop(); outside.focus(); let cleaned = 0;
    const first = createDialog('Первое окно', () => { cleaned++; }); first.close();
    assert(cleaned === 1 && !first.element.isConnected, 'Managed cleanup was deferred');
    const second = createDialog('Следующее окно'); const focused = document.activeElement; await delay();
    assert(cleaned === 1 && document.activeElement === focused && second.element.contains(focused), 'Old close stole newer dialog focus');
    second.close(); first.close(); assert(cleaned === 1, 'Cleanup duplicated');
  }],
  ['Native cancel veto and delayed close fallback do not override later focus', async () => {
    stop(); outside.focus(); let count = 0, interrupted = false;
    const dialog = createDialog('Нативное окно', result => { count++; interrupted = result.interrupted; });
    const veto = (event: Event): void => event.preventDefault(); dialog.element.addEventListener('cancel', veto);
    dialog.element.requestClose(); assert(dialog.element.open && count === 0, 'Native veto ignored'); dialog.element.removeEventListener('cancel', veto);
    dialog.element.close(); control<HTMLElement>('#qa-main').focus(); await delay();
    assert(Number(count) === 1 && interrupted && document.activeElement === document.querySelector('#qa-main'), 'Native fallback stole focus');
  }],
  ['Preview owns the next screen; runtime settings keep iframe, input and toolbar identity', async () => {
    const f = await start(); const { dialog } = await settingsFromCard(); activate(control('[data-settings-key="preview"]', dialog));
    await until(() => host.querySelector('iframe'), 'Preview iframe'); const frame = control<HTMLIFrameElement>('iframe', host);
    await until(() => frame.contentDocument?.querySelector('#value'), 'Synthetic runtime loaded');
    const value = frame.contentDocument!.querySelector<HTMLInputElement>('#value')!; value.value = 'Незавершённый ввод';
    const more = control<HTMLElement>('[data-stage-control="more"]', host); activate(more);
    const settings = named('Настройки приложения', host); activate(settings); const next = control<HTMLDialogElement>('.sw-app-settings-dialog[open]');
    await until(() => next.querySelector<HTMLInputElement>('[data-settings-key="name"]')?.value, 'Runtime settings'); await rename(f, next);
    activate(control('.sw-dialog-header button', next)); await paint();
    assert(document.activeElement === more, 'Toolbar opener lost'); assert(host.querySelector('iframe') === frame && value.value === 'Незавершённый ввод', 'Runtime remounted');
  }],
  ['Account A → B → A closes old window and cannot reuse its old return ticket', async () => {
    const f = await start(), { dialog, opener, input } = await settingsFromCard(); input.value = 'Старый личный ввод'; input.dispatchEvent(new Event('input', { bubbles: true }));
    f.setAccount(`${ACCOUNT}-B`); await world!.refresh(); f.setAccount(ACCOUNT); await world!.refresh();
    await until(() => host.querySelector('[data-app-action="inspect"]'), 'New account card'); outside.focus(); await paint();
    assert(!dialog.open && !dialog.isConnected && !opener.isConnected, 'Old account DOM survived');
    assert(document.activeElement === outside && !document.querySelector('.sw-app-settings-dialog'), 'Old close returned into new account');
  }],
  ['Route leave confirmation hands off once; no late return to the departed app card', async () => {
    await start(); const { dialog, input } = await settingsFromCard(); input.value = 'Несохранённое'; input.dispatchEvent(new Event('input', { bubbles: true }));
    history.pushState({ soty: true }, '', '#library'); window.dispatchEvent(new PopStateEvent('popstate'));
    assert(dialog.open && location.hash === '#mine', 'Pending route removed dirty settings');
    activate(control('[data-settings-key="confirm-no"]', dialog)); assert(dialog.open, 'Route cancel closed settings');
    history.pushState({ soty: true }, '', '#library'); window.dispatchEvent(new PopStateEvent('popstate'));
    activate(control('[data-settings-key="confirm-yes"]', dialog));
    await until(() => location.hash === '#library' && !dialog.isConnected, 'Confirmed route handoff'); await paint();
    assert(!host.querySelector('[data-app-action="inspect"]'), 'Old main repainted after handoff');
  }],
  ['Same-group repaint returns to the new main once; late cards cannot move a later user focus', async () => {
    const f = await start(); history.pushState({ soty: true }, '', `#community/${GROUP}/apps`); window.dispatchEvent(new PopStateEvent('popstate'));
    await until(() => host.querySelector('.sx-community-apps [data-app-action="inspect"]'), 'Group apps');
    const { dialog } = await settingsFromCard(); await rename(f, dialog); const release = f.holdGroupApps();
    activate(control('.sw-dialog-header button', dialog));
    const main = control<HTMLElement>('#soty-main', host);
    assert(document.activeElement === main && !main.querySelector('[data-app-action="inspect"]'), 'Did not return to synchronous group main');
    outside.focus(); release(); await until(() => main.querySelector('.sx-community-apps [data-app-action="inspect"]'), 'Later group cards'); await paint(); await delay();
    assert(document.activeElement === outside, 'Late cards stole user focus'); assert(location.hash === `#community/${GROUP}/apps`, 'Group route changed');
  }],
];

async function run(): Promise<void> {
  if (running) return; running = true; output.dataset.testStatus = 'running'; const lines: string[] = []; const initialErrors = earlyErrors.length;
  try {
    for (const [label, action] of cases) { output.textContent = `Проверка: ${label}\n${lines.join('\n')}`; await action(); lines.push(`PASS ${label}`); }
    assert(earlyErrors.length === initialErrors, `Page errors: ${earlyErrors.slice(initialErrors).join('; ')}`);
    output.dataset.testStatus = 'passed'; output.dataset.passed = String(cases.length); output.textContent = `${cases.length}/${cases.length} PASS\n${lines.join('\n')}`;
  } catch (reason) { output.dataset.testStatus = 'failed'; output.textContent = `${lines.join('\n')}\nFAIL ${reason instanceof Error ? reason.stack : String(reason)}`; }
  finally { stop(); running = false; }
}
document.querySelector('#qa-run')!.addEventListener('click', () => { void run(); });
document.querySelector('#qa-main')!.addEventListener('click', () => { if (!running) void start(); });
if (new URLSearchParams(location.search).get('run') === '1') void run(); else void start();
