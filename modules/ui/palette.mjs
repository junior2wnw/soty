// One palette for the PWA shell and isolated application entry/status pages.
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
  const tone = (low, middle, high) => t <= .5 ? mix(low, middle, t * 2) : mix(middle, high, (t - .5) * 2);
  const bg = dark ? tone('#08090a', '#101112', '#242527') : tone('#e9e8e2', '#f5f4f0', '#ffffff');
  const surface = dark ? tone('#111214', '#1a1b1d', '#2b2d30') : tone('#f4f2ec', '#fffefa', '#ffffff');
  const raised = dark ? tone('#191a1c', '#222426', '#343639') : tone('#dedbd3', '#eae8e1', '#f1efea');
  const ink = dark ? '#f3f2ee' : '#222320', muted = dark ? '#c1c1bd' : '#565951';
  const honey = '#d5c6a7', sage = '#a5c9b6', lilac = '#c8b6d6', coral = '#ddb2a6', blue = '#b0c3cd';
  const colors = { honey, sage, lilac, coral, blue };
  const tokens = {
    '--sw-bg': bg, '--sw-surface': surface, '--sw-surface-raised': raised,
    '--sw-surface-hover': dark ? mix(raised, '#505154', .12) : '#ffffff',
    '--sw-surface-subtle': mix(bg, surface, .45), '--sw-ink': ink, '--sw-muted': muted,
    '--sw-line': mix(bg, ink, dark ? .14 : .12),
    '--sw-control-border': dark ? '#a2a39d' : '#74766d',
    '--sw-focus': dark ? '#d5c6a7' : '#716044', '--sw-color-scheme': dark ? 'dark' : 'light',
    '--sw-on-accent': '#201d17', '--sw-primary-hover': mix(honey, '#fffef7', .15),
    '--sw-action': dark ? honey : '#716044', '--sw-action-ink': dark ? '#201d17' : '#fffef7',
    '--sw-action-hover': dark ? '#e4d7be' : '#5f5038',
    '--sw-chrome': dark ? tone('#0e0f10', '#141516', '#27282a') : tone('#f0eee8', '#fbfaf7', '#ffffff'),
    '--sw-header': bg,
    '--sw-header-ink': ink, '--sw-header-muted': muted, '--sw-header-line': mix(bg, ink, .15),
    '--sw-header-hover': raised, '--sw-header-selected': raised, '--sw-header-accent': dark ? honey : '#69583d',
    '--sw-danger': dark ? '#efa89f' : '#97392f', '--sw-danger-bg': mix(surface, dark ? '#784138' : '#eab2a1', .16),
    '--sw-unread': '#a83627', '--sw-on-unread': '#ffffff',
    '--sw-toast': dark ? '#e8e5dc' : '#292a27', '--sw-on-toast': dark ? '#272822' : '#fffef7',
    '--sw-switch-track': dark ? '#989992' : '#74766d', '--sw-switch-on': dark ? honey : '#716044',
    '--sw-switch-thumb': dark ? '#201d17' : '#ffffff',
    '--sw-shadow': dark ? '0 12px 36px #00000038' : '0 10px 30px #27341b12',
    '--sw-shadow-small': dark ? '0 2px 7px #00000028' : '0 2px 7px #27341b0c',
    '--sw-shadow-dialog': dark ? '0 24px 80px #00000066' : '0 24px 80px #17251035',
    '--sw-backdrop': dark ? '#060708b8' : '#20211e66',
  };
  const lightInks = { honey: '#695333', sage: '#345a47', lilac: '#66467c', coral: '#7d4134', blue: '#36586b' };
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
