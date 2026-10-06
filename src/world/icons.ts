import { hexCluster, hexPolygonPoints } from '../geometry/hex.mjs';

const cellIcon = hexCluster([[0, 0], [1, 0], [1, -1]], 4.75, 1.6);
const cellIconOffset = { x: (24 - cellIcon.width) / 2, y: (24 - cellIcon.height) / 2 };
const cellsPath = cellIcon.cells.map(cell => `<polygon points="${hexPolygonPoints(cellIcon.radius, { x: cell.x + cellIconOffset.x, y: cell.y + cellIconOffset.y })}"/>`).join('');

const paths: Record<string, string> = {
  app: '<path d="m12 2 9 5v10l-9 5-9-5V7ZM3 7l9 5 9-5M12 12v10M7.5 4.5l9 5"/>',
  note: '<path d="M5 3h14v18H5ZM8 7h8M8 11h8M8 15h5"/>',
  brush: '<path d="m14 3 7 7-9 9-6-6ZM14 3l-3 3M21 10l-3 3M6 13c-4 1-1 5-4 8 5 1 9-1 10-2"/>',
  chess: '<path d="M5 21h14M6 18h12l-1-6 2-3-3-6-5-2 1 4-6 5 1 3 4-1-2 6M15 7h.01"/>',
  bars: '<path d="M5 10v10M12 4v16M19 7v13"/>',
  sliders: '<path d="M3 6h7m4 0h7M3 18h3m4 0h11"/><circle cx="12" cy="6" r="2"/><circle cx="8" cy="18" r="2"/>',
  edit: '<path d="m4 16 12-12 4 4-12 12-5 1ZM14 6l4 4"/>',
  up: '<path d="m6 15 6-6 6 6"/>',
  forward: '<path d="m10 4 8 8-8 8"/>',
  layers: '<path d="m12 3 10 5-10 5L2 8Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
  moon: '<path d="M21 13a9 9 0 1 1-10-10 7 7 0 0 0 10 10Z"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/>',
  connections: '<circle cx="5" cy="5" r="2"/><circle cx="19" cy="5" r="2"/><circle cx="12" cy="19" r="2"/><path d="M7 5h10M6 7l5 10M18 7l-5 10"/>',
  diagonal: '<path d="M6 18 18 6M6 6h12v12"/>',
  shield: '<path d="m12 2 9 4v6c0 5-9 10-9 10S3 17 3 12V6ZM8 12l3 3 5-6"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1 1m12 12 1 1M5 19l1-1M18 6l1-1"/>',
  history: '<path d="M3 11a9 9 0 1 1 2 7M3 4v7h7M12 7v6l4 2"/>',
  world: '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3 4 6 4 9s-1 6-4 9c-3-3-4-6-4-9s1-6 4-9Z"/>',
  cells: cellsPath,
  chat: '<path d="M20 11.5a8.5 8.5 0 0 1-8.5 8.5H4l-2 2V11.5A8.5 8.5 0 0 1 10.5 3H12a8 8 0 0 1 8 8Z"/>',
  people: '<circle cx="9" cy="7" r="3"/><path d="M3 21v-4a6 6 0 0 1 12 0v4M16 4a3 3 0 0 1 0 6m2 3a5 5 0 0 1 3 5v3"/>',
  person: '<circle cx="12" cy="7" r="4"/><path d="M4 22v-3a8 8 0 0 1 16 0v3Z"/>',
  eye: '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/>',
  hidden: '<path d="m3 3 18 18M10.5 5.1 12 5c6.5 0 10 7 10 7a18 18 0 0 1-3.5 4.5M6.2 6.2A22 22 0 0 0 2 12s3.5 7 10 7a12 12 0 0 0 5.8-1.5M10 10a3 3 0 0 0 4 4"/>',
  search: '<circle cx="10.5" cy="10.5" r="7"/><path d="m16 16 5 5"/>',
  plus: '<path d="M12 4v16M4 12h16"/>', minus: '<path d="M4 12h16"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  trash: '<path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7M14 10v7"/>',
  download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
  back: '<path d="m14 4-8 8 8 8"/>', next: '<path d="m9 5 7 7-7 7"/>', down: '<path d="m6 9 6 6 6-6"/>',
  arrow: '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  more: '<circle cx="4" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="20" cy="12" r="1"/>',
  check: '<path d="m5 12 4 4L20 5"/>',
  lock: '<rect x="5" y="10" width="14" height="11" rx="3"/><path d="M8 10V7a4 4 0 0 1 8 0v3M12 14v3"/>',
  laptop: '<rect x="4" y="3" width="16" height="13" rx="2"/><path d="m4 16-3 5h22l-3-5M9 19h6"/>',
  monitor: '<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M12 17v4M7 21h10"/>',
  phone: '<rect x="6" y="2" width="12" height="20" rx="3"/><path d="M10 5h4M11 19h2"/>',
  sparkle: '<path d="m12 2 2.5 7.5L22 12l-7.5 2.5L12 22l-2.5-7.5L2 12l7.5-2.5ZM20 2v4M18 4h4"/>',
  camera: '<path d="m8 5 2-3h4l2 3h4a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2Z"/><circle cx="12" cy="12" r="4"/>',
  image: '<rect x="2" y="2" width="20" height="20" rx="3"/><circle cx="8" cy="8" r="2"/><path d="m2 18 6-6 5 5 4-4 5 5"/>',
  tools: '<path d="m3 3 5 2 1 4-4-1ZM8 8l13 13M21 3a6 6 0 0 1-7 8L4 21l-2-2L12 9a6 6 0 0 1 8-7l-4 4 2 2Z"/>',
  music: '<path d="M9 18V5l12-3v13M9 9l12-3"/><ellipse cx="5.5" cy="18.5" rx="3.5" ry="2.5"/><ellipse cx="17.5" cy="15.5" rx="3.5" ry="2.5"/>',
  game: '<path d="M7 7h10a4 4 0 0 1 4 3l2 7a3 3 0 0 1-5 3l-3-3H9l-3 3a3 3 0 0 1-5-3l2-7a4 4 0 0 1 4-3Z"/><path d="M7 10v6M4 13h6M17 11h.01M20 14h.01"/>',
  bulb: '<path d="M9 18v-2a7 7 0 1 1 6 0v2M9 18h6M10 22h4M12 11v7"/>',
  grid: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  list: '<path d="M8 5h13M8 12h13M8 19h13M3 5h.01M3 12h.01M3 19h.01"/>',
  send: '<path d="m22 2-7 20-4-9L2 9Z M22 2 11 13"/>',
  exit: '<path d="M9 4H3v16h6M10 12h12m-5-5 5 5-5 5"/>',
  settings: '<path d="m9 3 1-2h4l1 2 3 2h3l2 4-2 2v2l2 2-2 4h-3l-3 2-1 2h-4l-1-2-3-2H3l-2-4 2-2v-2L1 9l2-4h3Z"/><circle cx="12" cy="12" r="3"/>',
  pin: '<path d="m8 3 8 0-1 6 4 4v2H5v-2l4-4ZM12 15v7"/>',
  refresh: '<path d="M20 8a9 9 0 1 0 0 8M20 2v6h-6"/>',
  external: '<path d="M14 3h7v7M21 3 10 14M10 3H3v18h18v-7"/>',
  expand: '<path d="M8 3H3v5M16 3h5v5M21 16v5h-5M8 21H3v-5"/>',
  collapse: '<path d="M3 8h5V3M16 3v5h5M21 16h-5v5M8 21v-5H3"/>',
  flag: '<path d="M4 22V3m0 0c6-5 10 5 16 0v10c-6 5-10-5-16 0"/>',
  heart: '<path d="M20 4a5 5 0 0 0-8 2 5 5 0 0 0-8-2c-6 5 3 12 8 16 5-4 14-11 8-16Z"/>',
  activity: '<path d="M2 12h4l3-9 6 18 3-9h4"/>',
  folder: '<path d="M2 6a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2v11H2Z"/>',
  bell: '<path d="M6 8a6 6 0 0 1 12 0c0 8 3 8 3 10H3c0-2 3-2 3-10ZM9 22h6"/>',
};

export function icon(name: string, className = ''): SVGSVGElement {
  const element = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  element.setAttribute('viewBox', '0 0 24 24');
  element.setAttribute('fill', 'none');
  element.setAttribute('stroke', 'currentColor');
  element.setAttribute('stroke-width', '1.7');
  element.setAttribute('stroke-linecap', 'round');
  element.setAttribute('stroke-linejoin', 'round');
  element.setAttribute('aria-hidden', 'true');
  element.setAttribute('focusable', 'false');
  element.setAttribute('class', `sw-icon ${className}`.trim());
  element.innerHTML = paths[name] ?? paths.cells!;
  return element;
}

export const communityIcons = ['cells', 'tools', 'camera', 'game', 'music', 'bulb', 'image', 'people'] as const;
