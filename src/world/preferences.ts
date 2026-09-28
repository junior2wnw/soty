import { normalizeTheme, type ThemePreferences } from './theme/palette.mjs';
export type WorldView = 'world' | 'mine' | 'messages' | 'notes' | 'library';
export interface WorldPreferences extends ThemePreferences { view: WorldView; presentation: 'field' | 'list'; scale: number; compact: boolean; motion: boolean; pinned: string[]; }
const key = 'soty.world.ui.v1';
const defaults: WorldPreferences = { view: 'mine', presentation: 'field', scale: 1, compact: false, motion: true, pinned: [], themeMode: 'system', themeBrightness: 50 };

export function loadPreferences(): WorldPreferences {
  try {
    const value = JSON.parse(localStorage.getItem(key) ?? '{}') as Partial<WorldPreferences>;
    return {
      view: ['world', 'mine', 'messages', 'notes', 'library'].includes(value.view ?? '') ? value.view! : defaults.view,
      presentation: value.presentation === 'list' ? 'list' : 'field',
      scale: typeof value.scale === 'number' && Number.isFinite(value.scale) ? Math.max(0.65, Math.min(1.4, value.scale)) : 1,
      compact: value.compact === true,
      motion: value.motion !== false,
      pinned: Array.isArray(value.pinned) ? value.pinned.filter((id): id is string => typeof id === 'string').slice(0, 100) : [],
      ...normalizeTheme(value),
    };
  } catch { return { ...defaults, pinned: [] }; }
}

export function savePreferences(preferences: WorldPreferences): void {
  try { localStorage.setItem(key, JSON.stringify(preferences)); } catch { /* Browsing remains usable when local storage is unavailable. */ }
}
