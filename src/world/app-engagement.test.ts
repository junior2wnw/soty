import './world.css';
import { createPalette } from './theme/palette.mjs';
import { mountAppStage, type AppStageHandle } from './app-stage';
import { mountAppDiscussion, type AppDiscussionHandle } from './app-discussion';
import { mountAppSaved } from './app-saved';
import { mountAppLibrary } from './app-library';
import { createAppSavedState, type AppEntry, type SavedSnapshot } from './app-saved-state.mjs';
import { type DiscussionContext, type DiscussionMessage, type DiscussionSendResponse } from './app-discussion-state.mjs';
import { formatAppLaunchRoute, parseAppLaunchRoute, type AppLaunchIntent, type AppLaunchPresentation, type AppLaunchRequest } from './app-launch.mjs';
import type { WorldApi } from './types';

if (!import.meta.env.DEV) throw new Error('Development fixture only');

// This page mounts production components. Only transport, the runtime document
// and explicitly injected persistence adapters are synthetic. It never calls a
// working account's API. Root runs the page in the actual browser separately.
const host = document.querySelector<HTMLElement>('#qa-host')!;
const output = document.querySelector<HTMLElement>('#qa-results')!;
const APP = `app-${'d3'.repeat(16)}`, DOMAIN = `dom_${'d4'.repeat(16)}`;
const CURRENT = `conv_${'a1'.repeat(16)}`, ARCHIVE = `conv_${'b2'.repeat(16)}`, NEXT = `conv_${'c3'.repeat(16)}`;
const ENTRY: AppEntry = { appId: APP, domainId: DOMAIN, origin: 'https://runtime.engagement.invalid', path: '/доска?tag=a%2Bb#section' };
const clone = <T>(value: T): T => structuredClone(value);
const delay = (ms = 10): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const error = (code: string): Error & { code: string } => Object.assign(new Error(code), { code });
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
function deferred<T>() { let resolve!: (value: T) => void; let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
async function until(check: () => unknown, label: string): Promise<void> {
  output.dataset.currentStep = label;
  if (running) output.textContent = `Сценарий: ${output.dataset.currentTest ?? ''}\nШаг: ${label}\n${completedLines.join('\n')}`;
  const deadline = performance.now() + 6000;
  while (!check()) { if (performance.now() > deadline) throw new Error(`fixture timeout: ${label}`); await delay(); }
}
function key<T extends HTMLElement = HTMLElement>(name: string, parent: ParentNode = host): T {
  const node = parent.querySelector<T>(`[data-engagement-key="${name}"]`); assert(node, `missing control: ${name}`); return node;
}
function click(name: string, parent: ParentNode = host): void { key<HTMLButtonElement>(name, parent).click(); }
function visible(node: HTMLElement): boolean { return !node.closest('[hidden]') && getComputedStyle(node).display !== 'none' && node.getClientRects().length > 0; }
function type(value: string): HTMLTextAreaElement { const input = key<HTMLTextAreaElement>('discussion-input'); input.focus(); input.value = value;
  input.dispatchEvent(new Event('input', { bubbles: true })); return input; }
const paint = (): Promise<void> => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
const AUTOSIZE_CASE = 'Многострочный черновик читается после восстановления и изменения размера без потери позиции';
const ARCHIVE_FOCUS_CASE = 'Переходы в архив и обратно сохраняют фокус, поздний ответ не забирает его';
const scopedBrowserKeys = new Set<string>(), errors: string[] = [];
let mounted: { dispose(): void } | null = null, restoreFrames: (() => void) | null = null;
let generation = 0, running = false, theme: 'light' | 'dark' = 'dark';
let completedLines: string[] = [];
const callLog: Array<{ op: string; args: Record<string, unknown> }> = [];
window.addEventListener('error', event => { errors.push(event.message); });
window.addEventListener('unhandledrejection', event => { errors.push(String(event.reason?.message ?? event.reason)); });
function palette(): void { for (const [name, value] of Object.entries(createPalette(theme, 50))) document.documentElement.style.setProperty(name, value); }
palette();

function persistence() {
  const values = new Map<string, string>(), queues = new Map<string, Promise<unknown>>();
  let failed = false;
  const storage = { getItem: (name: string) => values.get(name) ?? null, setItem: (name: string, value: string) => { if (failed) throw new Error('synthetic storage quota failure'); values.set(name, value); } };
  const locks = { request(name: string, callback: () => unknown) {
    const work = (queues.get(name) ?? Promise.resolve()).catch(() => {}).then(callback); queues.set(name, work.catch(() => {})); return work;
  } } as unknown as Pick<LockManager, 'request'>;
  return { values, storage, locks, fail(value: boolean) { failed = value; } };
}
function stop(): void {
  generation++; mounted?.dispose(); mounted = null; host.replaceChildren(); restoreFrames?.(); restoreFrames = null;
  // Exact keys created by this page's random account names only. No shared or
  // pre-existing user records are searched or removed.
  for (const name of scopedBrowserKeys) localStorage.removeItem(name);
  scopedBrowserKeys.clear();
}
function runtimeFrames(): () => void {
  const original = document.createElement;
  document.createElement = function (name: string, options?: ElementCreationOptions): HTMLElement {
    const node = original.call(document, name, options);
    if (name.toLowerCase() === 'iframe') node.setAttribute('srcdoc', '<!doctype html><html lang="ru"><meta charset="utf-8"><style>body{margin:16px;font:16px system-ui;color:#222;background:#faf9f6}input{font:inherit;max-width:90%;padding:8px}</style><label>Состояние тестового приложения <input id="runtime-value" value="initial"></label><p>Синтетический runtime без сетевых запросов.</p></html>');
    return node;
  } as typeof document.createElement;
  return () => { document.createElement = original; };
}

interface Backend {
  accountId: string; api: WorldApi; entry: AppEntry; current: string; denied: boolean; retired: boolean;
  failLaunch: boolean; failEntry: boolean; malformedLaunch: boolean; loseSend: boolean; loseSave: boolean; resetHistory: boolean;
  launchCount: number; sendEffects: number; saved: SavedSnapshot; messages: Map<string, DiscussionMessage[]>;
  hold: null | ((op: string, args: Record<string, unknown>, value: unknown) => Promise<unknown>);
  launch(args: AppLaunchRequest): Promise<{ url: string; entry: AppEntry }>;
  message(body: string, conversationId?: string): DiscussionMessage;
  context(conversationId?: string, administrative?: boolean): DiscussionContext;
}
function backend(forcedAccountId?: string): Backend {
  const accountId = forcedAccountId ?? `engagement-fixture-${crypto.randomUUID()}`, receipts = new Map<string, DiscussionSendResponse>();
  const savedReceipts = new Map<string, { requestId: string; replayed: boolean; receipt: { appId: string; saved: boolean; revision: number; committedAt: number }; current: SavedSnapshot }>();
  let messageNumber = 0, timestamp = 1_800_000_000_000;
  const f: Backend = {
    accountId, entry: clone(ENTRY), current: CURRENT, denied: false, retired: false, failLaunch: false, failEntry: false, malformedLaunch: false,
    loseSend: false, loseSave: false, resetHistory: false, launchCount: 0, sendEffects: 0, saved: { revision: 0, entry: null },
    messages: new Map([[CURRENT, []], [ARCHIVE, []], [NEXT, []]]), hold: null,
    message(body, conversationId = f.current) { const row: DiscussionMessage = { id: `msg_${(++messageNumber).toString(16).padStart(32, '0')}`, conversationId,
      author: { accountId, label: 'Участник стенда' }, body, replyTo: null, createdAt: ++timestamp, removedAt: null, canRemove: true };
      f.messages.get(conversationId)!.push(row); return clone(row); },
    context(conversationId = f.current, administrative = false) { const isCurrent = conversationId === f.current;
      return { appId: APP, entry: administrative ? null : clone(f.entry), conversationId, mode: administrative || !isCurrent ? 'archive' : 'current', isCurrent,
        ownerAdministrative: administrative, audience: conversationId === NEXT ? 'public' : 'shared', canPost: isCurrent && !administrative, canModerate: true }; },
    async launch(args) { callLog.push({ op: 'fixture.launch', args: { ...clone(args) } }); f.launchCount++;
      if (f.failLaunch) throw error('app_offline');
      const selected = { ...f.entry, ...(args.path === undefined ? {} : { path: args.path }) };
      const value = { url: `${selected.origin}/_soty/boot?${new URLSearchParams({ path: selected.path })}#${'t'.repeat(43)}`, entry: selected };
      return f.malformedLaunch ? { ...value, entry: { ...selected, domainId: `dom_${'ff'.repeat(16)}` } } : value;
    },
    api: { async request<T>(op: string, given: Record<string, unknown> = {}): Promise<T> {
      const args = clone(given); callLog.push({ op, args });
      if (args.expectedAccountId !== accountId) throw error('authentication_required');
      const admin = args.administrative === true, conversationId = String(args.conversationId ?? f.current);
      const read = op !== 'apps.discussion.send' && op !== 'apps.discussion.remove' && op !== 'apps.saved.set';
      if (f.denied && read) throw error('authentication_required');
      if (f.retired && (op === 'apps.entry.get' || op.startsWith('apps.discussion.') && !admin && read)) throw error('app_unavailable');
      let value: unknown;
      if (op === 'apps.entry.get') { if (f.failEntry) throw error('app_unavailable'); value = { entry: clone(f.entry) }; }
      else if (op === 'apps.saved.get') value = clone(f.saved);
      else if (op === 'apps.saved.list') value = { revision: f.saved.revision, entries: f.saved.entry ? [clone(f.saved.entry)] : [], nextCursor: null };
      else if (op === 'apps.saved.set') {
        const requestId = String(args.requestId), prior = savedReceipts.get(requestId);
        if (prior) value = { ...clone(prior), replayed: true, current: clone(f.saved) };
        else {
          if (args.expectedRevision !== f.saved.revision) throw error('apps_saved_revision_conflict');
          f.saved.revision++;
          f.saved.entry = args.saved ? { ...f.entry, domainId: String(args.domainId), path: String(args.path), label: 'Приложение стенда',
            savedRevision: f.saved.revision, updatedAt: ++timestamp, current: { name: 'Приложение стенда', status: 'offline', canManage: true } } : null;
          const result = { requestId, replayed: false, receipt: { appId: APP, saved: args.saved === true, revision: f.saved.revision, committedAt: ++timestamp }, current: clone(f.saved) };
          savedReceipts.set(requestId, clone(result)); value = result;
          if (f.loseSave) { f.loseSave = false; throw new TypeError('synthetic lost save response'); }
        }
      } else if (op === 'apps.discussion.context') value = { context: f.context(conversationId, admin), messages: clone(f.messages.get(conversationId) ?? []), historyCursor: 'history_fixture', changeCursor: 'changes_fixture' };
      else if (op === 'apps.discussion.history') value = { context: f.context(conversationId, admin), messages: [], nextCursor: null, resetRequired: f.resetHistory };
      else if (op === 'apps.discussion.changes') value = { context: f.context(conversationId, admin), changes: (f.messages.get(conversationId) ?? []).map(message => ({ type: message.body === null ? 'removed' : 'message', message: clone(message) })), nextCursor: 'changes_fixture_after', hasMore: false, resetRequired: false };
      else if (op === 'apps.discussion.archives') value = { entries: [...f.messages.keys()].filter(id => id !== f.current && f.messages.get(id)!.length).map(id => f.context(id, admin)), nextCursor: null, resetRequired: false };
      else if (op === 'apps.discussion.send') {
        const requestId = String(args.requestId), prior = receipts.get(requestId);
        if (prior) value = { ...clone(prior), replayed: true, message: f.denied ? null : clone(prior.message) };
        else {
          if (f.denied || f.retired) throw error('app_unavailable');
          if (conversationId !== f.current) throw error('apps_discussion_changed');
          const message = f.message(String(args.body), conversationId); message.replyTo = args.replyTo ? String(args.replyTo) : null; f.sendEffects++;
          const response = { requestId, replayed: false, receipt: { id: message.id, conversationId, createdAt: message.createdAt }, ownCurrent: { removed: false }, message };
          receipts.set(requestId, clone(response)); value = response;
          if (f.loseSend) { f.loseSend = false; throw new TypeError('synthetic lost send response'); }
        }
      } else if (op === 'apps.discussion.remove') {
        const row = f.messages.get(conversationId)?.find(message => message.id === args.messageId); assert(row, 'synthetic remove target');
        row.body = null; row.removedAt = ++timestamp; row.canRemove = false; value = { id: row.id, conversationId, removed: true };
      } else throw error('fixture_operation_unsupported');
      if (f.hold) value = await f.hold(op, args, value);
      return clone(value) as T;
    } },
  };
  f.message('Текущее сообщение'); f.message('Доступная старая история', ARCHIVE);
  return f;
}

function route(presentation?: AppLaunchPresentation): AppLaunchIntent {
  const value = parseAppLaunchRoute(formatAppLaunchRoute({ appId: APP, domainId: DOMAIN, path: ENTRY.path }, undefined, presentation));
  assert(value, 'fixture route'); return value;
}
function applyRoute(value: AppLaunchIntent): void { history.replaceState(null, '', `#${value.route.replace(/^#/, '')}`); }
function stage(f: Backend, presentation?: AppLaunchPresentation): AppStageHandle {
  stop(); const token = generation, initial = route(presentation); applyRoute(initial);
  restoreFrames = runtimeFrames();
  scopedBrowserKeys.add(`soty.app-discussion.drafts.v1:${f.accountId}`);
  scopedBrowserKeys.add(`soty.app-saved.pending.v1:${f.accountId}`);
  const handle = mountAppStage(host, { api: f.api, accountId: f.accountId,
    app: { appId: APP, name: 'Приложение стенда', status: 'offline', ownerAccountId: f.accountId }, intent: initial,
    isCurrent: () => generation === token, request: parameters => f.launch(parameters),
    onNavigate: applyRoute, onBack: () => { applyRoute(route()); handle.updateRoute(route()); }, onAccount: async () => {}, onSettings: () => {} });
  mounted = handle; return handle;
}
function discussion(f: Backend, adapters = persistence(), initialConversationId?: string): AppDiscussionHandle {
  stop(); const token = generation;
  const handle = mountAppDiscussion(host, { api: f.api, accountId: f.accountId, appId: APP, entry: f.entry,
    ...adapters, ...(initialConversationId ? { initialConversationId } : {}), isCurrent: () => generation === token });
  mounted = handle; return handle;
}
async function readyComposer(): Promise<HTMLTextAreaElement> {
  await until(() => { const input = host.querySelector<HTMLTextAreaElement>('[data-engagement-key="discussion-input"]'); return input && visible(input) && !input.readOnly; }, 'writable composer');
  return key<HTMLTextAreaElement>('discussion-input');
}
async function frameOf(): Promise<HTMLIFrameElement> {
  await until(() => host.querySelector<HTMLIFrameElement>('iframe')?.contentDocument?.querySelector('#runtime-value'), 'srcdoc iframe ready');
  return host.querySelector<HTMLIFrameElement>('iframe')!;
}
async function openPanel(): Promise<void> { host.querySelector<HTMLButtonElement>('[data-stage-control="discussion"]')!.click(); await readyComposer(); }
async function archives(): Promise<void> {
  key<HTMLDetailsElement>('discussion-archives').open = true;
  await until(() => host.querySelector('[data-engagement-key="discussion-archive"]'), 'visible archive row');
}

const cases: Array<[string, () => Promise<void>]> = [
  ['Панель, архив и Back сохраняют iframe и введённое состояние', async () => {
    const f = backend(), h = stage(f); await h.ready; const frame = await frameOf(), win = frame.contentWindow;
    const field = frame.contentDocument!.querySelector<HTMLInputElement>('#runtime-value')!; field.value = 'runtime state must survive';
    await openPanel(); type('Черновик в текущем обсуждении'); await h.flush(); await archives(); click('discussion-archive');
    await until(() => key('discussion-audience').textContent?.includes('Архив'), 'archive shown');
    assert(!visible(key('discussion-input')), 'archive composer must not be writable');
    host.querySelector<HTMLButtonElement>('[data-stage-control="discussion"]')!.click();
    await until(() => !visible(key('discussion')), 'panel closed');
    const back = route({ panel: 'discussion' }); applyRoute(back); h.updateRoute(back); await readyComposer();
    assert(host.querySelector('iframe') === frame && frame.contentWindow === win && field.value === 'runtime state must survive', 'presentation recreated browsing context');
    assert(key<HTMLTextAreaElement>('discussion-input').value === 'Черновик в текущем обсуждении', 'Back lost current draft');
    h.updateApp({ appId: APP, name: 'Новое название', status: 'ready', ownerAccountId: f.accountId });
    assert(host.querySelector('iframe') === frame && f.launchCount === 1, 'metadata or panel requested another ticket');
  }],
  ['Архив владельца не запускает приложение; обычный вход гидратирует ту же панель', async () => {
    const f = backend(), h = stage(f, { panel: 'discussion', administrative: true }); await h.ready;
    await until(() => key('discussion-audience').textContent?.includes('Управление'), 'administrative read');
    const panel = key('discussion'); assert(f.launchCount === 0 && !host.querySelector('iframe'), 'administrative read minted ticket');
    assert(!visible(key('discussion-input')), 'administrative current view exposed a composer');
    assert(!panel.querySelector('.se-discussion-banner')?.textContent?.includes('Этот разговор завершён'), 'administrative read falsely declared the current conversation closed');
    const ordinary = route({ panel: 'discussion' }); applyRoute(ordinary); h.updateRoute(ordinary); await readyComposer(); await frameOf();
    assert(key('discussion') === panel && Number(f.launchCount) === 1 && h.entry()?.path === ENTRY.path, 'null entry hydration replaced panel or failed');
  }],
  [ARCHIVE_FOCUS_CASE, async () => {
    const f = backend(), h = discussion(f), draft = 'Черновик остаётся в текущем разговоре';
    await readyComposer(); type(draft); await h.flush(); await archives();
    const samples: Array<Record<string, unknown>> = [];
    function focusInPanel(step: string): HTMLElement {
      const node = document.activeElement;
      assert(node instanceof HTMLElement && host.contains(node) && node.isConnected && visible(node), `${step}: focus fell outside the visible panel`);
      assert(!!(node.getAttribute('aria-label') || node.textContent?.trim()), `${step}: focus target has no readable name`);
      samples.push({ step, tag: node.tagName, key: node.dataset.engagementKey ?? null, name: node.getAttribute('aria-label') || node.textContent?.trim(), connected: node.isConnected });
      output.dataset.archiveFocusMetrics = JSON.stringify(samples);
      return node;
    }
    async function transition(control: string, target: string, moveDuringWait = false): Promise<void> {
      const gate = deferred<unknown>(); let captured: unknown;
      f.hold = async (op, args, value) => {
        if (op === 'apps.discussion.context' && String(args.conversationId ?? f.current) === target) { captured = value; return gate.promise; }
        return value;
      };
      const trigger = key<HTMLButtonElement>(control); trigger.focus();
      assert(document.activeElement === trigger, `${control}: actual control did not take focus`);
      trigger.click();
      try {
        await until(() => captured !== undefined, `${control}: context held after activation`);
        // Give the browser a layout turn: an immediately settled Promise can
        // hide the fact that removal of a focused button sends focus to BODY.
        await delay(40); await paint();
        let expectedFocus = focusInPanel(`${control}: pending`);
        assert(!key('discussion-archives').textContent?.includes('Доступных архивов нет.'), `${control}: pending read falsely reported an empty archive`);
        if (moveDuringWait) {
          expectedFocus = key<HTMLButtonElement>('discussion-refresh'); expectedFocus.focus();
          assert(document.activeElement === expectedFocus, 'user could not move focus during the read');
        }
        f.hold = null; gate.resolve(captured);
        if (target === CURRENT) await readyComposer();
        else await until(() => key('discussion-audience').textContent?.includes('Архив')
          && host.querySelector('[data-engagement-key="discussion-archive"][aria-pressed="true"]'), 'archive selected after held response');
        await delay(40); await paint();
        assert(document.activeElement === expectedFocus, `${control}: completion moved the user's focus`);
        focusInPanel(`${control}: complete${moveDuringWait ? ' after user moved focus' : ''}`);
        if (target === CURRENT) assert(key<HTMLTextAreaElement>('discussion-input').value === draft, 'return from archive lost the scoped current draft');
        else assert(!visible(key('discussion-input')), 'archive exposed a writable current draft');
      } finally { f.hold = null; gate.resolve(captured); }
    }
    await transition('discussion-archive', ARCHIVE);
    await transition('discussion-current', CURRENT);
    await transition('discussion-archive', ARCHIVE, true);
    const externalSelectionFocus = document.activeElement;
    await h.updateSelection({}); await readyComposer(); await paint();
    assert(document.activeElement === externalSelectionFocus, 'programmatic route selection stole focus');
    assert(key<HTMLTextAreaElement>('discussion-input').value === draft, 'programmatic current selection lost the scoped draft');
    assert(f.sendEffects === 0, 'focus/navigation produced a send effect');
  }],
  ['Неудачный первый launch допускает позднее точное разрешение входа', async () => {
    const f = backend(); f.failLaunch = f.failEntry = true; const h = stage(f, { panel: 'discussion' }); await h.ready;
    const panel = key('discussion'); assert(h.entry() === null && !visible(key('discussion-input')), 'failed entry became writable');
    f.failLaunch = f.failEntry = false;
    host.querySelector<HTMLButtonElement>('[data-stage-control="refresh"]')!.click(); await readyComposer(); await frameOf();
    assert(key('discussion') === panel && h.entry()?.domainId === DOMAIN, 'later launch did not hydrate exact retained panel');
  }],
  ['Некорректный entry после success не заменяется поздней догадкой', async () => {
    const f = backend(); f.malformedLaunch = true; const at = callLog.length, h = stage(f, { panel: 'discussion' }); await h.ready;
    assert(h.entry() === null && !host.querySelector('iframe') && !host.querySelector('[data-engagement-key="save"]'), 'invalid successful launch admitted UI');
    assert(!callLog.slice(at).some(call => call.op === 'apps.entry.get'), 'success with bad DTO triggered fallback');
  }],
  ['Обновление обсуждения сохраняет поле, фокус и выделение', async () => {
    const f = backend(), h = discussion(f); await readyComposer(); const input = type('Текст пользователя перед обновлением'); await h.flush();
    input.setSelectionRange(5, 14); const gate = deferred<unknown>(); let pending: unknown;
    f.hold = async (op, _args, value) => { if (op === 'apps.discussion.context') { pending = value; return gate.promise; } return value; };
    const refresh = h.refresh(); await until(() => pending !== undefined, 'network-held refresh');
    await delay(40);
    try {
      assert(visible(input) && input.readOnly, 'held refresh must retain only readonly own draft');
      assert(key('discussion-send').getAttribute('aria-disabled') === 'true', 'held refresh retained send authority');
      assert(key('discussion-messages').textContent?.trim() === '', 'held refresh retained old server messages');
      assert(input.value === 'Текст пользователя перед обновлением', 'held refresh changed own local text');
    } finally { gate.resolve(pending); }
    await refresh; await readyComposer();
    assert(key('discussion-input') === input && input.value === 'Текст пользователя перед обновлением', 'refresh replaced or reset composer');
    assert(document.activeElement === input && input.selectionStart === 5 && input.selectionEnd === 14, 'refresh stole input focus or selection');
  }],
  [AUTOSIZE_CASE, async () => {
    const originalWidth = host.style.width, originalDisplay = host.style.display;
    const f = backend(), adapters = persistence(), samples: Array<Record<string, unknown>> = [];
    for (let index = 0; index < 24; index++) f.message(`Сообщение ${index + 1} для проверки положения истории, без сетевой отправки.`);
    let h = discussion(f, adapters); await readyComposer();
    const restored = 'Первая строка\nВторая строка';
    type(restored); await h.flush();
    h = discussion(f, adapters);
    let input = await readyComposer(); const identity = input;
    const history = host.querySelector<HTMLElement>('.se-discussion-scroll')!;
    const height = (): number => input.getBoundingClientRect().height;
    const unclipped = (): boolean => input.scrollHeight <= input.clientHeight + 1;
    const sample = (step: string): void => {
      const css = getComputedStyle(input), box = input.getBoundingClientRect();
      samples.push({ step, viewport: `${innerWidth}x${innerHeight}`, width: box.width, height: box.height,
        clientHeight: input.clientHeight, scrollHeight: input.scrollHeight, min: css.minHeight, max: css.maxHeight,
        selection: [input.selectionStart, input.selectionEnd], focused: document.activeElement === input,
        historyTop: history.scrollTop, historyGap: history.scrollHeight - history.clientHeight - history.scrollTop });
      output.dataset.autosizeMetrics = JSON.stringify(samples);
    };
    const intact = (value: string, start: number, end: number, step: string): void => {
      assert(key('discussion-input') === identity && input.value === value, `${step}: textarea identity or text changed`);
      assert(document.activeElement === input && input.selectionStart === start && input.selectionEnd === end, `${step}: focus or selection changed`);
      assert(f.sendEffects === 0, `${step}: sizing sent a message`);
    };
    const replace = async (value: string): Promise<void> => {
      input.setSelectionRange(0, input.value.length); input.setRangeText(value, 0, input.value.length, 'end');
      input.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: value }));
      await h.flush(); await paint();
    };
    try {
      input.focus(); input.setSelectionRange(2, 8);
      await until(unclipped, 'restored two-line value fits without an input event');
      intact(restored, 2, 8, 'restored'); sample('restored');
      const restoredHeight = height();

      input.setSelectionRange(input.value.length, input.value.length);
      input.setRangeText('\nЕщё строка', input.value.length, input.value.length, 'end');
      input.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertLineBreak', data: '\nЕщё строка' }));
      const inserted = input.value, insertionEnd = input.selectionEnd;
      await h.flush(); await until(() => unclipped() && height() > restoredHeight + 1, 'insertion grows the actual textarea');
      intact(inserted, insertionEnd, insertionEnd, 'insertion'); sample('insertion');

      await replace('Коротко');
      await until(() => unclipped() && height() < restoredHeight - 1, 'deletion returns unused height');
      intact('Коротко', 7, 7, 'deletion'); sample('deletion');
      const shortHeight = height();
      assert(history.scrollHeight > history.clientHeight + 200, 'fixture needs a real scrollable history');
      history.scrollTop = history.scrollHeight;
      await replace(restored);
      await until(() => height() > shortHeight + 1, 'growth while history is at its end');
      assert(Math.abs(history.scrollHeight - history.clientHeight - history.scrollTop) <= 2, 'growing draft moved history away from its end');
      sample('history-bottom');
      await replace('Коротко'); await until(() => height() <= shortHeight + 1, 'short draft before history midpoint');
      history.scrollTop = Math.floor((history.scrollHeight - history.clientHeight) / 2);
      const middle = history.scrollTop;
      await replace(restored); await until(() => height() > shortHeight + 1, 'growth while history is in the middle');
      assert(Math.abs(history.scrollTop - middle) <= 2, 'growing draft moved the reader from the middle of history');
      sample('history-middle');

      const wrapping = 'Список покупок для дома на неделю: хлеб, молоко, яблоки.';
      await replace(wrapping); input.setSelectionRange(4, 11); await paint();
      const wideWidth = input.getBoundingClientRect().width, wideHeight = height();
      const durableBeforeResize = JSON.stringify([...adapters.values]); sample('wide');
      host.style.width = `${Math.max(216, Math.min(320, Math.floor(host.clientWidth * 0.7)))}px`;
      await until(() => input.getBoundingClientRect().width < wideWidth - 1 && height() > wideHeight + 1, 'width-only narrowing changes wrapped height');
      intact(wrapping, 4, 11, 'narrow'); sample('narrow');
      host.style.width = originalWidth;
      await until(() => input.getBoundingClientRect().width >= wideWidth - 1 && height() <= wideHeight + 1, 'width-only widening releases height');
      intact(wrapping, 4, 11, 'wide-again'); sample('wide-again');
      assert(JSON.stringify([...adapters.values]) === durableBeforeResize, 'width changes wrote a new draft revision');

      const long = 'Длинная строка черновика для прокрутки.\n'.repeat(130).slice(0, 4000);
      await replace(long); input.setSelectionRange(long.length - 10, long.length - 2); await paint();
      const css = getComputedStyle(input), maximum = Number.parseFloat(css.maxHeight);
      assert(Number.isFinite(maximum) && height() <= maximum + 1, 'long draft exceeded the CSS height budget');
      assert(input.scrollHeight > input.clientHeight + 40 && /auto|scroll/u.test(css.overflowY), 'capped long draft has no internal scrolling');
      input.scrollTop = input.scrollHeight;
      assert(input.scrollTop > 0, 'capped text cannot be reached by scrolling');
      const sendBox = key('discussion-send').getBoundingClientRect(), hostBox = host.getBoundingClientRect();
      assert(sendBox.top >= hostBox.top - 1 && sendBox.bottom <= hostBox.bottom + 1, 'long draft displaced send outside its panel');
      assert(host.scrollWidth <= host.clientWidth + 1, 'draft introduced horizontal overflow');
      intact(long, long.length - 10, long.length - 2, 'bounded-long'); sample('bounded-long');

      await replace(restored); await until(unclipped, 'short draft before hidden layout');
      const hiddenHeight = input.style.height;
      host.style.display = 'none'; assert(input.getBoundingClientRect().width === 0, 'fixture parent was not actually hidden');
      await h.refresh(); await paint();
      assert(input.style.height === hiddenHeight, 'zero-width hidden layout replaced the last usable size');
      host.style.display = originalDisplay;
      await until(() => visible(input) && unclipped(), 'parent becomes measurable again');
      h.setVisible(false); await paint(); h.setVisible(true); await readyComposer(); await until(unclipped, 'same composer returns after hidden panel');
      input.focus(); input.setSelectionRange(2, 8); intact(restored, 2, 8, 'shown-again'); sample('shown-again');

      // Inline height is the browser's native resize signal. This is not a
      // pointer-drag claim; actual pointer/keyboard geometry belongs to root QA.
      const manualCss = getComputedStyle(input), minimum = Number.parseFloat(manualCss.minHeight), limit = Number.parseFloat(manualCss.maxHeight);
      const manual = Math.round(minimum + (limit - minimum) / 2);
      input.style.height = `${manual}px`; await paint();
      await replace('Сохраняем ручной размер');
      assert(Math.abs(height() - manual) <= 1, 'automatic fit overwrote the explicit manual height');
      const manualStable = height(); await h.refresh(); await paint();
      assert(Math.abs(height() - manualStable) <= 1, 'unchanged render oscillated manual height');
      sample('manual-size');

      // A fresh mount returns to automatic sizing. Dispose immediately after
      // input, before the queued frame can resize its detached textarea.
      h = discussion(f, adapters); input = await readyComposer(); await paint();
      input.value = restored; input.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: restored }));
      h.dispose(); mounted = null; const afterDispose = input.style.height;
      await paint(); assert(input.style.height === afterDispose, 'queued sizing wrote to a disposed component');
      assert(f.sendEffects === 0, 'sizing lifecycle sent a message');
    } finally { host.style.width = originalWidth; host.style.display = originalDisplay; }
  }],
  ['Смена аудитории оставляет старый текст отдельно и новый composer пустым', async () => {
    const f = backend(), h = discussion(f); await readyComposer(); type('Текст прежней закрытой аудитории'); await h.flush();
    f.current = NEXT; await h.updateSelection({}); await readyComposer();
    assert(key<HTMLTextAreaElement>('discussion-input').value === '', 'old draft moved to new audience');
    assert([...host.querySelectorAll<HTMLTextAreaElement>('[data-engagement-key="discussion-retained-text"]')].some(area => area.value === 'Текст прежней закрытой аудитории'), 'old draft disappeared');
    assert(key('discussion-audience').textContent === 'Публичное' && f.sendEffects === 0, 'audience mislabeled or automatic send');
  }],
  ['Ошибка записи оставляет собственный текст и блокирует уход к архиву', async () => {
    const f = backend(), adapters = persistence(), h = discussion(f, adapters); await readyComposer(); adapters.fail(true);
    type('Этот текст пока только в памяти'); await h.flush();
    assert(h.hasUnsavedChanges(), 'storage failure was treated as durable');
    const at = callLog.length; let refused = false;
    try { await h.updateSelection({ conversationId: ARCHIVE }); } catch { refused = true; }
    assert(refused && key<HTMLTextAreaElement>('discussion-input').value === 'Этот текст пока только в памяти', 'failed save discarded local draft during navigation');
    assert(!callLog.slice(at).some(call => call.op === 'apps.discussion.context'), 'navigation occurred despite volatile draft');
    adapters.fail(false); await h.flush(); assert(!h.hasUnsavedChanges(), 'recovered local storage did not retain text');
  }],
  ['Отказ перехода из-за quota возвращает stage URL к показанному разговору', async () => {
    const f = backend(), h = stage(f, { panel: 'discussion' }); await h.ready; await readyComposer();
    const draftKey = `soty.app-discussion.drafts.v1:${f.accountId}`, original = Storage.prototype.setItem;
    Storage.prototype.setItem = function (name: string, value: string): void {
      if (name === draftKey) throw new DOMException('Synthetic account-only quota failure', 'QuotaExceededError');
      original.call(this, name, value);
    };
    try {
      type('Несохранённый текст перед Back');
      await until(() => h.hasUnsavedChanges() && !!key('discussion-status').textContent, 'volatile stage draft');
      const at = callLog.length, requested = route({ panel: 'discussion', conversationId: ARCHIVE });
      applyRoute(requested); h.updateRoute(requested);
      await until(() => !parseAppLaunchRoute(location.hash)?.presentation?.conversationId && !!host.querySelector('.sa-message')?.textContent, 'refused route restored');
      assert(visible(key('discussion')) && key<HTMLTextAreaElement>('discussion-input').value === 'Несохранённый текст перед Back', 'stage refusal removed recovery panel or own text');
      assert(h.hasUnsavedChanges() && !callLog.slice(at).some(call => call.op === 'apps.discussion.context' && call.args.conversationId === ARCHIVE), 'stage navigated to rejected archive');
    } finally { Storage.prototype.setItem = original; await h.flush(); }
  }],
  ['Потерянный ACK после remount повторяет прежний requestId без дубликата', async () => {
    const f = backend(), adapters = persistence(); f.loseSend = true; let h = discussion(f, adapters); await readyComposer();
    type('Ровно одно сообщение'); await h.flush(); click('discussion-send');
    await until(() => f.sendEffects === 1 && visible(key('discussion-retry')) && key('discussion-retry').getAttribute('aria-disabled') === 'false', 'lost ACK pending');
    const before = callLog.filter(call => call.op === 'apps.discussion.send').at(-1)!.args.requestId;
    h = discussion(f, adapters); await readyComposer(); click('discussion-retry');
    await until(() => !visible(key('discussion-retry')), 'same intent acknowledged');
    const after = callLog.filter(call => call.op === 'apps.discussion.send').at(-1)!.args.requestId;
    assert(before === after && f.sendEffects === 1 && key<HTMLTextAreaElement>('discussion-input').value === '', 'remount duplicated or changed send');
    await h.flush();
  }],
  ['Retry сохранения не отправляет заменивший показанный запрос другого окна', async () => {
    stop(); const f = backend(), adapters = persistence(), other = persistence();
    const state = createAppSavedState({ accountId: f.accountId, ...adapters, randomId: () => 'shown-A' });
    const pending = await state.prepare({ entry: f.entry, saved: true, expectedRevision: 0, currentEntry: null }); state.dispose();
    const token = generation; mounted = mountAppSaved(host, { api: f.api, accountId: f.accountId, entry: f.entry, ...adapters, isCurrent: () => generation === token });
    await until(() => key('save').dataset.state === 'pending' && key('save').getAttribute('aria-disabled') === 'false', 'shown saved pending');
    click('save'); await until(() => document.querySelector('[data-engagement-key="save-retry"]'), 'retry dialog A');
    const replacement = createAppSavedState({ accountId: f.accountId, ...other, randomId: () => 'hidden-B' });
    await replacement.prepare({ entry: { ...f.entry, path: '/other' }, saved: true, expectedRevision: 0, currentEntry: null }); replacement.dispose();
    adapters.values.set(state.key, other.values.get(state.key)!); // other tab's durable change before its storage event
    const at = callLog.length; click('save-retry', document);
    await until(() => key('save').dataset.state === 'pending' && key('save').getAttribute('aria-disabled') === 'false', 'replacement detected');
    assert(!callLog.slice(at).some(call => call.op === 'apps.saved.set'), 'retry A dispatched hidden B');
    assert(pending.args.requestId === 'shown-A' && JSON.parse(adapters.values.get(state.key)!).pending.args.requestId === 'hidden-B', 'changed pending was cleared');
  }],
  ['Отказ чтения очищает сообщения, но сохраняет собственный локальный текст', async () => {
    const f = backend(), h = discussion(f); await readyComposer(); type('Мой текст после потери доступа'); await h.flush();
    f.denied = true; await h.refresh();
    assert(!key('discussion-messages').textContent?.includes('Текущее сообщение') && !key('discussion-audience').textContent, 'denied private projection remained');
    assert([...host.querySelectorAll<HTMLTextAreaElement>('[data-engagement-key="discussion-retained-text"]')].some(area => area.value === 'Мой текст после потери доступа'), 'denial destroyed own draft');
  }],
  ['Reset cursor сохраняет черновик и требует явного нового snapshot', async () => {
    const f = backend(), h = discussion(f); await readyComposer(); type('Неотправленный текст при reset'); await h.flush();
    f.resetHistory = true; click('discussion-older'); await until(() => visible(key('discussion-latest')), 'cursor reset action');
    assert(f.sendEffects === 0 && host.textContent?.includes('Черновик сохранён отдельно'), 'reset silently sent or omitted recovery');
    click('discussion-latest'); await readyComposer(); assert(key<HTMLTextAreaElement>('discussion-input').value === 'Неотправленный текст при reset', 'snapshot reset lost draft');
  }],
  ['Поздний ответ после account A→B→A не возвращает прежний экран', async () => {
    const a = backend(), gate = deferred<unknown>(); let entered = false;
    a.hold = async (op, _args, value) => { if (op === 'apps.discussion.context') { entered = true; return gate.promise; } return value; };
    discussion(a); await until(() => entered, 'old request captured');
    const b = backend(); discussion(b); await readyComposer();
    const fresh = backend(a.accountId); fresh.messages.set(CURRENT, []); fresh.message('Новый экран аккаунта A'); discussion(fresh);
    await readyComposer(); const currentRoot = key('discussion');
    gate.resolve({ context: { ...a.context(), audience: 'public' }, messages: [], historyCursor: null, changeCursor: 'late_cursor' });
    await delay(25); assert(key('discussion') === currentRoot && key('discussion-messages').textContent?.includes('Новый экран аккаунта A'), 'old account response mutated new root');
  }],
  ['Недоступный сохранённый вход не заменяется другим origin; удаление остаётся явным', async () => {
    stop(); const f = backend(), adapters = persistence(); f.saved = { revision: 1, entry: { ...f.entry, label: 'Моя прежняя подпись', savedRevision: 1,
      updatedAt: Date.now(), current: null } }; const opened: AppEntry[] = [], token = generation;
    mounted = mountAppLibrary(host, { api: f.api, accountId: f.accountId, ...adapters, isCurrent: () => generation === token, openEntry: value => opened.push(value) });
    await until(() => host.querySelector('[data-engagement-key="library-open"]'), 'unavailable saved row'); click('library-open');
    assert(opened.length === 0 && host.textContent?.includes('Моя прежняя подпись') && host.textContent?.includes('Недоступно'), 'inaccessible bookmark launched or leaked fresh name');
    click('library-remove'); await until(() => f.saved.entry === null, 'explicit remove');
  }],
  ['Длинный текст остаётся plaintext; видимые элементы имеют имена и помещаются по ширине', async () => {
    const f = backend(); const literal = '<img src=x onerror=alert(1)> ' + 'ДлинноеСлово'.repeat(190); f.message(literal);
    discussion(f); await readyComposer();
    assert(host.textContent?.includes(literal) && !host.querySelector('.se-message-body img'), 'message markup was interpreted');
    const unnamed = [...host.querySelectorAll<HTMLElement>('button,textarea')].filter(node => visible(node)
      && !(node.getAttribute('aria-label') || node.textContent?.trim() || node.getAttribute('aria-labelledby')));
    assert(unnamed.length === 0, 'visible control without accessible name');
    assert(host.scrollWidth <= host.clientWidth + 1, `horizontal overflow at actual width ${innerWidth}`);
  }],
];

async function run(): Promise<void> {
  if (running) return; running = true; output.dataset.testStatus = 'running'; errors.length = 0;
  const requestedCase = new URL(location.href).searchParams.get('case');
  const selectedName = requestedCase === 'autosize' ? AUTOSIZE_CASE : requestedCase === 'archive-focus' ? ARCHIVE_FOCUS_CASE : null;
  const selected = selectedName ? cases.filter(([name]) => name === selectedName) : cases;
  delete output.dataset.autosizeMetrics;
  delete output.dataset.archiveFocusMetrics;
  let passed = 0, failed = 0; completedLines = []; output.textContent = 'Проверяем настоящий DOM с вымышленными ответами…';
  try {
    for (const [name, check] of selected) {
      const startErrors = errors.length; output.dataset.currentTest = name; output.dataset.currentStep = 'начало';
      try { await check(); assert(errors.length === startErrors, `unhandled browser error: ${errors.slice(startErrors).join('; ')}`);
        passed++; completedLines.push(`PASS ${name}`);
      } catch (reason) { failed++; completedLines.push(`FAIL ${name} — ${reason instanceof Error ? reason.message : String(reason)}`); }
      finally { stop(); output.textContent = completedLines.join('\n'); }
    }
    output.dataset.testStatus = failed ? 'fail' : 'pass'; output.textContent = `${failed ? 'FAIL' : 'PASS'} ${passed}/${selected.length} · ошибок ${failed} · фактический viewport ${innerWidth}×${innerHeight}\n${completedLines.join('\n')}`;
  }
  finally { running = false; stop(); }
}
document.querySelector('#qa-run')!.addEventListener('click', () => { void run(); });
document.querySelector('#qa-stage')!.addEventListener('click', () => { if (!running) stage(backend()); });
document.querySelector('#qa-discussion')!.addEventListener('click', () => { if (!running) discussion(backend()); });
document.querySelector('#qa-library')!.addEventListener('click', () => {
  if (running) return; stop(); const f = backend(), token = generation;
  f.saved = { revision: 1, entry: { ...f.entry, label: 'Сохранённое приложение', savedRevision: 1, updatedAt: Date.now(), current: { name: 'Приложение стенда', status: 'offline', canManage: true } } };
  mounted = mountAppLibrary(host, { api: f.api, accountId: f.accountId, ...persistence(), isCurrent: () => generation === token, openEntry: () => { stage(f); } });
});
document.querySelector('#qa-theme')!.addEventListener('click', () => { theme = theme === 'dark' ? 'light' : 'dark'; palette(); });
window.addEventListener('pagehide', stop);
if (new URL(location.href).searchParams.get('run') === '1') void run();
