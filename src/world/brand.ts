import { roundedHexPath, SQRT3 } from '../geometry/hex.mjs';
import { el } from './dom';
const ns = 'http://www.w3.org/2000/svg';
/** All modern marks use the same regular hexagon, rotated without changing its geometry. */
export function appendHexSurface(host: HTMLElement): void {
    const radius = 31, center = 32;
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('viewBox', `${center - radius * SQRT3 / 2} ${center - radius} ${radius * SQRT3} ${radius * 2}`);
    svg.setAttribute('aria-hidden', 'true');
    svg.setAttribute('focusable', 'false');
    svg.classList.add('sx-mark-shape');
    const path = document.createElementNS(ns, 'path');
    path.setAttribute('d', roundedHexPath(radius, radius * .14, { x: center, y: center }));
    path.setAttribute('transform', `rotate(30 ${center} ${center})`);
    path.setAttribute('fill', 'currentColor');
    svg.append(path);
    host.prepend(svg);
}
export function createBrandMark(): HTMLElement { const mark = el('span', 'sx-brand-mark'); appendHexSurface(mark); mark.append(el('span', '', 'S')); return mark; }
