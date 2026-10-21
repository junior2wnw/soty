import { createUnifiedField, type FieldCommitResult, type FieldDirectoryEntity, type UnifiedFieldSummary } from './unified-field';
import { applyFieldCommand } from './unified-field-state.mjs';
import type { FieldDocument } from '../../modules/field/contract.mjs';
import { resolveFieldAppArt } from './field-art.mjs';
import { applyThemePreferences } from './theme/theme';
import './world.css';
import './theme/theme.css';
import './one-field.css';

const parameters = new URL(location.href).searchParams, count = Math.min(256, Math.max(3, Number(parameters.get('count') || 6)));
const paintMode = parameters.get('paint') === '1', paintKinds = ['app', 'builtin', 'community', 'device'] as const;
if (paintMode && (!import.meta.env.DEV || !['127.0.0.1', 'localhost', '[::1]'].includes(location.hostname))) throw Error('Paint fixture is local development-only');
const initialPaintKind = paintKinds.find(kind => kind === parameters.get('kind')) ?? 'device';
let paintReadout: (() => void) | undefined;

applyThemePreferences({ themeMode: parameters.get('theme') === 'light' ? 'light' : 'dark', themeBrightness: 50 });
document.body.classList.add('sw-app'); // Use the production World box-sizing/font scope.
if (parameters.get('layout') === 'one-field') document.body.dataset.experience = 'one-field';
document.body.style.display = 'block'; // This engine harness has no product rail/grid shell.
document.body.style.margin = '0'; document.body.style.height = '100dvh';
const header = document.createElement('header'); header.style.cssText = 'min-height:76px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:10px 16px;background:#242625;color:#fff;box-sizing:border-box;max-width:100%;font-size:12px';
header.append(Object.assign(document.createElement('span'), { textContent: 'Тест движка · вымышленные записи' }));
const host = document.createElement('main'); host.className = 'uf-screen'; host.style.height = 'calc(100dvh - 150px)';
const outside = document.createElement('input'); outside.placeholder = 'Фокус вне поля'; outside.hidden = paintMode; header.append(outside); document.body.append(header, host);
let initial: FieldDocument = { schema: 'soty.field.v1', contexts: [], shortcuts: [] };
const contextCount = count > 20 ? Math.min(24, Math.ceil(count / 10)) : 1;
for (let i = 0; i < contextCount; i++) initial.contexts.push({ contextId: `context-${i}`, title: i ? `Пространство ${i + 1}` : 'Студия · пример', x: i % 3 * 900, y: Math.floor(i / 3) * 1000 });
const items: FieldDirectoryEntity[] = [];
for (let i = 0; i < count; i++) {
  const kind = i % 10 === 3 || i % 10 === 4 ? 'person' : i % 10 === 5 ? 'device' : 'app';
  const entity = { kind, id: `test-${i}` } as const;
  const contextId = initial.contexts[Math.min(Math.floor(i / (contextCount > 1 ? 10 : count)), contextCount - 1)]!.contextId;
  initial = applyFieldCommand(initial, { type: 'add-shortcut', shortcutId: `shortcut-${i}`, entity, contextId }).document;
  items.push({ entity, title: kind === 'person' ? `Человек ${i}` : kind === 'device' ? 'Ноутбук · тест' : i % 3 === 0 ? `HIVE ${i}` : i % 3 === 1 ? `Canvas ${i}` : `Pulse ${i}`,
    coverKey: ['hive', 'canvas', 'pulse'][i % 3]!, symbol: kind === 'person' ? 'user' : kind === 'device' ? 'laptop' : 'cells', source: 'owner' });
}
let revision = 0, behavior: 'saved' | 'volatile' | 'quota' | 'conflict' | 'replay-newer' | 'delayed' = 'saved';
const commits: { document: FieldDocument; expectedRevision: number; requestId: string }[] = [], activations: string[] = [], inspections: string[] = [];
let state: UnifiedFieldSummary | null = null, resolveDelayed: (() => void) | null = null;
const stateEvents: { time: number; document: FieldDocument }[] = [];
const engine = createUnifiedField({ accountId: 'synthetic-engine-account', document: initial, entities: items,
  readableMobileFocus: parameters.get('layout') === 'one-field', dimNonMatches: parameters.get('layout') === 'one-field',
  resolveArt: entity => {
    const art = resolveFieldAppArt(entity.coverKey ?? null);
    if (parameters.get('art') === 'fallback') return { ...art, src: null, srcset: '', status: 'fallback' };
    if (parameters.get('art') === 'broken') return { ...art, src: '/__fixture_missing-art.webp', srcset: '' };
    return art;
  }, onActivate: (_, id) => { activations.push(id!); paintReadout?.(); }, onInspect: (_, id) => { inspections.push(id); paintReadout?.(); },
  onStateChange: value => { state = value; stateEvents.push({ time: performance.now(), document: value.document }); if (stateEvents.length > 1024) stateEvents.shift(); paintReadout?.(); },
  commit: async (document, context) => {
    commits.push({ document: structuredClone(document), ...context }); paintReadout?.();
    if (behavior === 'delayed') await new Promise<void>(resolve => { resolveDelayed = resolve; });
    if (behavior === 'quota') throw Object.assign(new Error('Blocked storage'), { code: 'FIELD_QUOTA' });
    if (behavior === 'conflict') return { status: 'conflict', revision, document: structuredClone(initial) };
    if (behavior === 'volatile') return { status: 'volatile', localDurable: true, revision, document };
    revision = Math.max(revision, context.expectedRevision) + 1;
    return { status: 'saved', revision, document: behavior === 'replay-newer' ? applyFieldCommand(document, { type: 'rename-context', contextId: 'context-0', title: 'Изменено в другом окне' }).document : document } satisfies FieldCommitResult;
  } });
host.append(engine.element);
if (!paintMode) for (const [label, action] of [['Расставить', () => engine.setArrange(true)], ['Обзор', () => engine.fitOverview()], ['Фокус', () => engine.focusContext('context-0')], ['Отменить', () => engine.cancel()]] as const) {
  const button = document.createElement('button'); button.textContent = label; button.addEventListener('click', action); header.append(button);
}
if (!paintMode) Object.assign(window, { __fieldTest: { sessionId: crypto.randomUUID(), engine, commits, activations, inspections, items,
  state: () => state, stateEvents, behavior: (next: typeof behavior) => { behavior = next; }, release: () => { resolveDelayed?.(); resolveDelayed = null; }, initial } });


if (paintMode) {
  document.body.dataset.paintFixture = 'local-synthetic';
  document.body.style.display = 'flex'; document.body.style.flexDirection = 'column';
  header.style.flex = 'none'; host.style.flex = '1 1 0'; host.style.height = '0'; host.style.minHeight = '0';
  engine.element.style.minHeight = '0'; // Container-only specimen sizing; no paint/hit CSS override.
  const panel = document.createElement('div'); panel.style.cssText = 'display:flex;flex-wrap:wrap;gap:8px;align-items:center;flex-basis:100%;min-width:0';
  const kind = document.createElement('select'); kind.id = 'paint-kind'; kind.setAttribute('aria-label', 'Тип проверяемой соты');
  for (const [value, title] of [['app', 'Приложение'], ['builtin', 'Встроенная'], ['community', 'Сообщество'], ['device', 'Устройство']]) {
    const option = document.createElement('option'); option.value = value!; option.textContent = title!; kind.append(option);
  }
  kind.value = initialPaintKind;
  const disabled = document.createElement('input'); disabled.type = 'checkbox'; disabled.id = 'paint-disabled';
  const disabledLabel = document.createElement('label'); disabledLabel.append(disabled, document.createTextNode('Disabled filter'));
  const center = document.createElement('button'); center.type = 'button'; center.textContent = 'Центр и фокус'; center.id = 'paint-center';
  const observation = document.createElement('output'); observation.id = 'paint-observation'; observation.setAttribute('aria-label', 'Активации, осмотры и камера');
  observation.style.cssText = 'display:block;flex-basis:100%;max-width:100%;overflow-x:auto;white-space:pre;font:11px/18px monospace';
  panel.append(kind, disabledLabel, center, observation); header.append(panel);
  const session = crypto.randomUUID(); let ready = false, readiness = 'loading';
  paintReadout = () => { observation.textContent = JSON.stringify({ session, ready, readiness, kind: kind.value, disabled: disabled.checked,
    activations: activations.length, inspections: inspections.length, commits: commits.length, camera: engine.getCamera(),
    nodeCount: host.querySelectorAll('.uf-node').length, nodeId: host.querySelector<HTMLElement>('.uf-node')?.dataset.shortcutId ?? null,
    nodeDisabled: host.querySelector<HTMLButtonElement>('.uf-node')?.disabled ?? null }); };
  const centerAndFocus = (): void => {
    ready = false; readiness = 'centering'; kind.disabled = disabled.disabled = center.disabled = true; paintReadout?.();
    requestAnimationFrame(() => {
    const node = host.querySelector<HTMLButtonElement>('[data-shortcut-id="paint-node"]'); if (!node) { readiness = 'paint-node-missing'; paintReadout?.(); return; }
    engine.setCamera({ x: parseFloat(node.style.left) + parseFloat(node.style.width) / 2,
      y: parseFloat(node.style.top) + parseFloat(node.style.height) / 2, scale: 128 / parseFloat(node.style.width) });
    requestAnimationFrame(() => {
      if (node.isConnected && !node.disabled) node.focus({ preventScroll: true });
      ready = true; readiness = 'ready'; kind.disabled = disabled.disabled = center.disabled = false; paintReadout?.();
    });
  }); };
  const applyKind = (): void => {
    const selected = paintKinds.find(value => value === kind.value); if (!selected) throw Error('Invalid paint fixture kind');
    ready = false; readiness = 'rendering'; kind.disabled = disabled.disabled = center.disabled = true;
    const entity = { kind: selected, id: selected === 'builtin' ? 'notes' : `synthetic-paint-${selected}` };
    let specimen: FieldDocument = { schema: 'soty.field.v1', contexts: [{ contextId: 'context-0', title: 'Paint · вымышленный пример', x: 0, y: 0 }], shortcuts: [] };
    specimen = applyFieldCommand(specimen, { type: 'add-shortcut', shortcutId: 'paint-node', entity, contextId: 'context-0' }).document;
    const title = parameters.get('caption')?.slice(0, 160) || (selected === 'device' ? 'Мой ноутбук' : `Проверка подписи · ${selected}`);
    activations.length = 0; inspections.length = 0; commits.length = 0;
    engine.setArrange(false); engine.update({ document: specimen, acceptRemote: true,
      entities: [{ entity, title, symbol: selected === 'device' ? 'laptop' : 'cells', source: 'owner', coverKey: 'hive' }],
      filter: disabled.checked ? '__synthetic_absent_query__' : '' });
    paintReadout?.(); centerAndFocus();
  };
  kind.disabled = disabled.disabled = center.disabled = true;
  kind.addEventListener('change', applyKind);
  disabled.addEventListener('change', () => {
    ready = false; readiness = 'rendering'; kind.disabled = disabled.disabled = center.disabled = true;
    engine.update({ filter: disabled.checked ? '__synthetic_absent_query__' : '' }); paintReadout?.();
    requestAnimationFrame(() => { ready = true; readiness = 'ready'; kind.disabled = disabled.disabled = center.disabled = false; paintReadout?.(); });
  });
  center.addEventListener('click', centerAndFocus); paintReadout();
  void engine.ready.then(applyKind)
    .catch(() => { readiness = 'engine-ready-failed'; paintReadout?.(); });
}
