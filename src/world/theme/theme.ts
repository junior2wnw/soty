import { loadPreferences, savePreferences } from '../preferences';
import { createPalette, normalizeTheme, type ThemePreferences, type ThemeScheme } from './palette.mjs';
import './theme.css';

export interface ThemeState extends ThemePreferences { resolvedTheme: ThemeScheme; }
export interface ThemeController {
  get(): ThemeState;
  set(patch: Partial<ThemePreferences>, options?: { persist?: boolean }): void;
  subscribe(listener: (state: ThemeState) => void): () => void;
  destroy(): void;
}

export function applyThemePreferences(value: Partial<ThemePreferences>): ThemeState {
  const preferences = normalizeTheme(value);
  const resolvedTheme: ThemeScheme = preferences.themeMode === 'system'
    ? matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light' : preferences.themeMode;
  const palette = createPalette(resolvedTheme, preferences.themeBrightness), root = document.documentElement;
  for (const [name, color] of Object.entries(palette)) root.style.setProperty(name, color);
  root.dataset.sotyTheme = resolvedTheme;
  root.dataset.sotyThemeMode = preferences.themeMode;
  root.style.colorScheme = resolvedTheme;
  let meta = document.querySelector<HTMLMetaElement>('meta[name="theme-color"]');
  if (!meta) { meta = document.createElement('meta'); meta.name = 'theme-color'; document.head.append(meta); }
  meta.content = palette['--sw-header']!;
  return { ...preferences, resolvedTheme };
}

export function createThemeController({ initial = loadPreferences(), onChange = next => savePreferences({ ...loadPreferences(), ...next }) }: {
  initial?: Partial<ThemePreferences>; onChange?: (next: ThemePreferences) => void;
} = {}): ThemeController {
  let state = applyThemePreferences(initial), committed = normalizeTheme(initial), destroyed = false;
  const listeners = new Set<(state: ThemeState) => void>();
  const media = matchMedia('(prefers-color-scheme: dark)');
  const publish = () => { for (const listener of listeners) listener({ ...state }); };
  const set = (patch: Partial<ThemePreferences>, { persist = true } = {}) => {
    if (destroyed) return;
    state = applyThemePreferences({ ...state, ...patch }); publish();
    if (persist && (state.themeMode !== committed.themeMode || state.themeBrightness !== committed.themeBrightness)) {
      committed = normalizeTheme(state); onChange({ ...committed });
    }
  };
  const systemChanged = () => { if (state.themeMode === 'system') set({}, { persist: false }); };
  const storageChanged = (event: StorageEvent) => { if (event.key === 'soty.world.ui.v1') set(loadPreferences()); };
  media.addEventListener('change', systemChanged); window.addEventListener('storage', storageChanged);
  return { get: () => ({ ...state }), set,
    subscribe(listener) { listeners.add(listener); listener({ ...state }); return () => listeners.delete(listener); },
    destroy() { if (destroyed) return; set({}); destroyed = true; media.removeEventListener('change', systemChanged); window.removeEventListener('storage', storageChanged); listeners.clear(); },
  };
}

export function createThemeControls(controller: ThemeController): { element: HTMLElement; destroy(): void } {
  const element = document.createElement('section'); element.className = 'sw-theme-controls';
  const modes = document.createElement('div'); modes.className = 'sw-theme-modes'; modes.setAttribute('role', 'group'); modes.setAttribute('aria-label', 'Тема');
  const choices = [['dark', 'Тёмная', 'moon'], ['system', 'Как в системе', 'system'], ['light', 'Светлая', 'sun']] as const;
  const buttons = choices.map(([mode, label, symbol]) => {
    const button = document.createElement('button'); button.type = 'button'; button.className = 'sw-theme-mode';
    button.setAttribute('aria-label', label); button.title = label; button.append(themeIcon(symbol));
    button.addEventListener('click', () => controller.set({ themeMode: mode })); modes.append(button); return button;
  });
  const label = document.createElement('label'); label.className = 'sw-theme-brightness';
  const row = document.createElement('span'); row.className = 'sw-theme-label';
  const title = document.createElement('span'); title.textContent = 'Яркость';
  const output = document.createElement('output'); output.setAttribute('aria-hidden', 'true'); row.append(title, output);
  const range = document.createElement('input'); range.type = 'range'; range.min = '0'; range.max = '100'; range.step = '1'; range.setAttribute('aria-label', 'Яркость темы');
  range.addEventListener('input', () => controller.set({ themeBrightness: Number(range.value) }, { persist: false }));
  range.addEventListener('change', () => controller.set({ themeBrightness: Number(range.value) }));
  label.append(row, range); element.append(modes, label);
  const unsubscribe = controller.subscribe(state => {
    buttons.forEach((button, index) => button.setAttribute('aria-pressed', String(choices[index]![0] === state.themeMode)));
    range.value = String(state.themeBrightness); range.setAttribute('aria-valuetext', `${state.themeBrightness} из 100`); output.value = `${state.themeBrightness}%`;
  });
  return { element, destroy() { controller.set({}); unsubscribe(); element.remove(); } };
}

function themeIcon(name: 'moon' | 'system' | 'sun'): SVGSVGElement {
  const ns = 'http://www.w3.org/2000/svg', svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24'); svg.setAttribute('aria-hidden', 'true'); svg.setAttribute('fill', 'none'); svg.setAttribute('stroke', 'currentColor'); svg.setAttribute('stroke-width', '1.7'); svg.setAttribute('stroke-linecap', 'round'); svg.setAttribute('stroke-linejoin', 'round');
  const paths = { moon: 'M20.3 14A8.4 8.4 0 0 1 10 3.7 8.5 8.5 0 1 0 20.3 14Z', system: 'M4 4h16v12H4zM8 20h8M12 16v4', sun: 'M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0' };
  const path = document.createElementNS(ns, 'path'); path.setAttribute('d', paths[name]); svg.append(path); return svg;
}
