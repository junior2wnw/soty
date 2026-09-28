import { SQRT3, HEX_ASPECT_RATIO, HEX_HEIGHT_FACTOR, HEX_POLYGON, HEX_SAFE_HEIGHT, HEX_SAFE_WIDTH, roundedHexPath, type HexCluster } from './hex.mjs';
import './hex.css';

/** CSS receives the same constants as placement and SVG; no independent shape formula lives in a theme. */
export function installHexGeometry(root: HTMLElement = document.documentElement): void {
  const doc = root.ownerDocument;
  if (!doc.getElementById('soty-geometry-defs')) {
    const svg = doc.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.id = 'soty-geometry-defs';
    svg.classList.add('soty-geometry-defs');
    svg.setAttribute('aria-hidden', 'true'); svg.setAttribute('focusable', 'false');
    svg.setAttribute('width', '0'); svg.setAttribute('height', '0');
    const defs = doc.createElementNS(svg.namespaceURI, 'defs');
    const clip = doc.createElementNS(svg.namespaceURI, 'clipPath');
    clip.id = 'soty-hex-rounded'; clip.setAttribute('clipPathUnits', 'objectBoundingBox');
    const path = doc.createElementNS(svg.namespaceURI, 'path');
    path.setAttribute('d', roundedHexPath(1));
    path.setAttribute('transform', `matrix(.5 0 0 ${1 / SQRT3} .5 .5)`);
    clip.append(path); defs.append(clip); svg.append(defs);
    (doc.body ?? doc.documentElement).append(svg);
  }
  root.style.setProperty('--hex-aspect-ratio', String(HEX_ASPECT_RATIO));
  root.style.setProperty('--hex-height-factor', String(HEX_HEIGHT_FACTOR));
  root.style.setProperty('--hex-inset-factor', String(HEX_ASPECT_RATIO));
  root.style.setProperty('--hex-polygon', HEX_POLYGON);
  root.style.setProperty('--hex-clip', 'url("#soty-hex-rounded")');
  root.style.setProperty('--hex-safe-width', String(HEX_SAFE_WIDTH));
  root.style.setProperty('--hex-safe-height', String(HEX_SAFE_HEIGHT));
}

export function placeHex(node: HTMLElement, cluster: HexCluster, index: number, top = 0): void {
  const cell = cluster.cells[index];
  if (!cell) throw new RangeError('Hex cell does not exist');
  node.classList.add('soty-hex', 'soty-hex-placed');
  node.style.setProperty('--hex-width', `${cluster.radius * 2}px`);
  node.style.left = `${cell.left}px`;
  node.style.top = `${cell.top + top}px`;
}

installHexGeometry();
