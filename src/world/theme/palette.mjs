export const DEFAULT_THEME = Object.freeze({ themeMode: 'system', themeBrightness: 50 });
export function normalizeTheme(value = {}) {
  return { themeMode: ['system', 'light', 'dark'].includes(value.themeMode) ? value.themeMode : 'system',
    themeBrightness: Number.isFinite(value.themeBrightness) ? Math.round(Math.max(0, Math.min(100, value.themeBrightness))) : 50 };
}
const rgb = hex => hex.slice(1).match(/../g).map(value => parseInt(value, 16));
export function mix(a, b, amount) { return '#' + rgb(a).map((value, index) => Math.round(value + (rgb(b)[index] - value) * amount).toString(16).padStart(2, '0')).join(''); }
export function luminance(hex) { return rgb(hex).map(value => { const n = value / 255; return n <= 0.04045 ? n / 12.92 : ((n + 0.055) / 1.055) ** 2.4; }).reduce((sum, value, index) => sum + value * [0.2126, 0.7152, 0.0722][index], 0); }
export function contrast(a, b) { const values = [luminance(a), luminance(b)].sort((x, y) => y - x); return (values[0] + 0.05) / (values[1] + 0.05); }

// Brightness moves surfaces within a scheme. Text never fades through grey and
// the two schemes never interpolate through a low-contrast middle frame.
export function createPalette(scheme, brightness = 50) {
  const dark = scheme === 'dark', t = normalizeTheme({ themeBrightness: brightness }).themeBrightness / 100;
  const bg = dark ? mix('#0e1511', '#29352c', t) : mix('#e7e5dc', '#fffdf7', t);
  const surface = dark ? mix('#18231b', '#354238', t) : mix('#efede4', '#fffefa', t);
  const raised = dark ? mix('#202e24', '#405046', t) : mix('#f6f4ec', '#ffffff', t);
  const ink = dark ? '#f1f5eb' : '#202b23', muted = dark ? '#c5d1c1' : '#4f5c50';
  const honey = '#e6ad4a', sage = '#9bb890', lilac = '#c4a7e4', coral = '#e3a28f', blue = '#a3c2d2';
  const colors = { honey, sage, lilac, coral, blue };
  const tokens = {
    '--sw-bg': bg, '--sw-surface': surface, '--sw-surface-raised': raised,
    '--sw-surface-hover': dark ? mix(raised, '#536d59', .15) : '#ffffff',
    '--sw-surface-subtle': mix(bg, surface, .45), '--sw-ink': ink, '--sw-muted': muted,
    '--sw-line': mix(bg, ink, dark ? .14 : .12),
    '--sw-control-border': dark ? '#a1b298' : '#727e6c',
    '--sw-focus': dark ? '#ffd37b' : '#80520c', '--sw-color-scheme': dark ? 'dark' : 'light',
    '--sw-on-accent': '#19231b', '--sw-primary-hover': mix(honey, '#fff6d4', .15),
    '--sw-header': dark ? mix('#0a100c', '#17251b', t) : '#303a31',
    '--sw-header-ink': '#f0f4e9', '--sw-header-muted': '#c7d1bf', '--sw-header-line': '#687761',
    '--sw-header-hover': '#43523e', '--sw-header-selected': '#514b32', '--sw-header-accent': '#ffda8e',
    '--sw-danger': dark ? '#ffb8aa' : '#8f3025', '--sw-danger-bg': mix(surface, dark ? '#904833' : '#eab2a1', .20),
    '--sw-unread': '#a83627', '--sw-on-unread': '#ffffff',
    '--sw-toast': dark ? '#e5edde' : '#28392c', '--sw-on-toast': dark ? '#233429' : '#f6f7eb',
    '--sw-switch-track': dark ? '#879480' : '#727e6c', '--sw-switch-on': dark ? '#bbd79d' : '#44643d',
    '--sw-switch-thumb': dark ? '#15231a' : '#ffffff',
    '--sw-shadow': dark ? '0 12px 36px #00000038' : '0 10px 30px #27341b12',
    '--sw-shadow-small': dark ? '0 2px 7px #00000028' : '0 2px 7px #27341b0c',
    '--sw-shadow-dialog': dark ? '0 24px 80px #00000066' : '0 24px 80px #17251035',
    '--sw-backdrop': dark ? '#030b08b8' : '#14231c66',
  };
  const lightInks = { honey: '#6b4812', sage: '#365632', lilac: '#674584', coral: '#803e2e', blue: '#365569' };
  for (const [name, value] of Object.entries(colors)) {
    tokens[`--sw-${name}`] = value;
    tokens[`--sw-${name}-light`] = mix(surface, value, dark ? .15 : .20);
    tokens[`--sw-${name}-ink`] = dark ? mix(value, '#ffffff', .38) : lightInks[name];
    tokens[`--sw-${name}-border`] = dark ? mix(value, surface, .20) : mix(value, '#63572c', .28);
  }
  tokens['--sw-selected'] = tokens['--sw-honey-light'];
  tokens['--sw-message'] = tokens['--sw-sage-light'];
  tokens['--sw-message-own'] = tokens['--sw-honey-light'];
  return tokens;
}

export function contrastPairs(palette) {
  const pairs = [];
  const add = (foreground, background, minimum) => pairs.push({ foreground, background, minimum, ratio: contrast(palette[foreground], palette[background]) });
  for (const surface of ['--sw-bg', '--sw-surface', '--sw-surface-raised', '--sw-surface-hover', '--sw-surface-subtle', '--sw-selected', '--sw-message', '--sw-message-own']) {
    add('--sw-ink', surface, 4.5); add('--sw-muted', surface, 4.5);
    add('--sw-focus', surface, 3); add('--sw-control-border', surface, 3);
  }
  for (const color of ['honey', 'sage', 'lilac', 'coral', 'blue']) {
    add(`--sw-${color}-ink`, `--sw-${color}-light`, 4.5); add('--sw-on-accent', `--sw-${color}`, 4.5);
  }
  for (const foreground of ['--sw-header-ink', '--sw-header-muted', '--sw-header-accent']) {
    for (const background of ['--sw-header', '--sw-header-hover', '--sw-header-selected']) add(foreground, background, 4.5);
  }
  add('--sw-danger', '--sw-danger-bg', 4.5); add('--sw-on-unread', '--sw-unread', 4.5);
  add('--sw-on-toast', '--sw-toast', 4.5); add('--sw-on-accent', '--sw-primary-hover', 4.5);
  add('--sw-switch-thumb', '--sw-switch-track', 3); add('--sw-switch-thumb', '--sw-switch-on', 3);
  return pairs;
}
