import { createUnifiedField, type FieldCommitResult, type FieldDirectoryEntity, type UnifiedFieldSummary } from './unified-field';
import { applyFieldCommand } from './unified-field-state.mjs';
import type { FieldDocument } from '../../modules/field/contract.mjs';
import { resolveFieldAppArt } from './field-art.mjs';
import { applyThemePreferences } from './theme/theme';
import './world.css';
import './theme/theme.css';

const parameters = new URL(location.href).searchParams, count = Math.min(256, Math.max(3, Number(parameters.get('count') || 6)));
applyThemePreferences({ themeMode: parameters.get('theme') === 'light' ? 'light' : 'dark', themeBrightness: 50 });
document.body.classList.add('sw-app'); // Use the production World box-sizing/font scope.
document.body.style.display = 'block'; // This engine harness has no product rail/grid shell.
document.body.style.margin = '0'; document.body.style.height = '100dvh';
const header = document.createElement('header'); header.style.cssText = 'min-height:76px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:10px 16px;background:#242625;color:#fff;box-sizing:border-box;max-width:100%;font-size:12px';
header.append(Object.assign(document.createElement('span'), { textContent: 'Тест движка · вымышленные записи' }));
const host = document.createElement('main'); host.className = 'uf-screen'; host.style.height = 'calc(100dvh - 150px)';
const outside = document.createElement('input'); outside.placeholder = 'Фокус вне поля'; header.append(outside); document.body.append(header, host);
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
  resolveArt: entity => resolveFieldAppArt(entity.coverKey ?? null), onActivate: (_, id) => activations.push(id!), onInspect: (_, id) => inspections.push(id),
  onStateChange: value => { state = value; stateEvents.push({ time: performance.now(), document: value.document }); if (stateEvents.length > 1024) stateEvents.shift(); },
  commit: async (document, context) => {
    commits.push({ document: structuredClone(document), ...context });
    if (behavior === 'delayed') await new Promise<void>(resolve => { resolveDelayed = resolve; });
    if (behavior === 'quota') throw Object.assign(new Error('Blocked storage'), { code: 'FIELD_QUOTA' });
    if (behavior === 'conflict') return { status: 'conflict', revision, document: structuredClone(initial) };
    if (behavior === 'volatile') return { status: 'volatile', localDurable: true, revision, document };
    revision = Math.max(revision, context.expectedRevision) + 1;
    return { status: 'saved', revision, document: behavior === 'replay-newer' ? applyFieldCommand(document, { type: 'rename-context', contextId: 'context-0', title: 'Изменено в другом окне' }).document : document } satisfies FieldCommitResult;
  } });
host.append(engine.element);
for (const [label, action] of [['Расставить', () => engine.setArrange(true)], ['Обзор', () => engine.fitOverview()], ['Фокус', () => engine.focusContext('context-0')], ['Отменить', () => engine.cancel()]] as const) {
  const button = document.createElement('button'); button.textContent = label; button.addEventListener('click', action); header.append(button);
}
Object.assign(window, { __fieldTest: { sessionId: crypto.randomUUID(), engine, commits, activations, inspections, items,
  state: () => state, stateEvents, behavior: (next: typeof behavior) => { behavior = next; }, release: () => { resolveDelayed?.(); resolveDelayed = null; }, initial } });
