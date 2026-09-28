export type ThemeMode = 'system' | 'light' | 'dark';
export type ThemeScheme = 'light' | 'dark';
export interface ThemePreferences { themeMode: ThemeMode; themeBrightness: number; }
export const DEFAULT_THEME: Readonly<ThemePreferences>;
export function normalizeTheme(value?: Partial<ThemePreferences>): ThemePreferences;
export function mix(a: string, b: string, amount: number): string;
export function luminance(hex: string): number;
export function contrast(a: string, b: string): number;
export function createPalette(scheme: ThemeScheme, brightness?: number): Record<string, string>;
export function contrastPairs(palette: Record<string, string>): {foreground:string; background:string; minimum:number; ratio:number}[];
