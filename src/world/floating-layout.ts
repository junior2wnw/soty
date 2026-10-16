export interface FloatingPoint { x: number; y: number }
export interface FloatingRect { left: number; top: number; width: number; height: number }
export interface FloatingViewport extends FloatingRect { bottomInset?: number }

const size = 44, gap = 6;
const clamp = (value: number, low: number, high: number): number => Math.max(low, Math.min(Math.max(low, high), Number.isFinite(value) ? value : low));

/** Presentation only: callers keep the user's saved position unchanged. */
export function placeFloatingControl(preferred: FloatingPoint, view: FloatingViewport, obstacles: readonly FloatingRect[]): FloatingPoint {
  const minX = view.left + gap, maxX = Math.max(minX, view.left + view.width - size - gap);
  const minY = view.top + gap, maxY = Math.max(minY, view.top + view.height - size - (view.bottomInset ?? 88));
  const point = { x: clamp(preferred.x, minX, maxX), y: clamp(preferred.y, minY, maxY) };
  const blocks = obstacles.filter(rect => Number.isFinite(rect.left + rect.top + rect.width + rect.height) && rect.width > 0 && rect.height > 0
    && rect.left < maxX + size + gap && rect.left + rect.width + gap > minX
    && rect.top < maxY + size + gap && rect.top + rect.height + gap > minY);
  const collision = (candidate: FloatingPoint, rect: FloatingRect): boolean => candidate.x < rect.left + rect.width + gap
    && candidate.x + size + gap > rect.left && candidate.y < rect.top + rect.height + gap && candidate.y + size + gap > rect.top;
  if (!blocks.some(rect => collision(point, rect))) return point;

  // A nearest free rectangle has an edge at the preference, a viewport edge,
  // or an obstacle edge. Search these coordinates, nearest first, and stop
  // considering distances once a closer free placement has been found.
  const xs = [...new Set([point.x, minX, maxX, ...blocks.flatMap(rect => [clamp(rect.left - size - gap, minX, maxX), clamp(rect.left + rect.width + gap, minX, maxX)])])]
    .sort((a, b) => Math.abs(a - point.x) - Math.abs(b - point.x));
  const ys = [...new Set([point.y, minY, maxY, ...blocks.flatMap(rect => [clamp(rect.top - size - gap, minY, maxY), clamp(rect.top + rect.height + gap, minY, maxY)])])]
    .sort((a, b) => Math.abs(a - point.y) - Math.abs(b - point.y));
  let best: FloatingPoint | null = null, distance = Infinity;
  for (const x of xs) {
    if ((x - point.x) ** 2 >= distance) break;
    for (const y of ys) {
      const nextDistance = (x - point.x) ** 2 + (y - point.y) ** 2;
      if (nextDistance >= distance) break;
      const candidate = { x, y };
      if (!blocks.some(rect => collision(candidate, rect))) { best = candidate; distance = nextDistance; }
    }
  }
  // A viewport smaller than its controls may have no free rectangle. Keep the
  // control reachable; ordinary shell layouts reserve a free header dock.
  return best ?? point;
}

/** Read visible shell controls, including iframe bounds we cannot inspect. */
export function floatingObstacles(doc: Document, ignore: (element: Element) => boolean): FloatingRect[] {
  const result: FloatingRect[] = [];
  for (const element of doc.querySelectorAll?.('button,a[href],input,textarea,select,summary,[role="button"],iframe') ?? []) {
    if (ignore(element) || element.closest('[hidden],[inert]') || !element.getClientRects().length) continue;
    let collapsed = false;
    for (let parent = element.parentElement; parent; parent = parent.parentElement) {
      if (parent.tagName === 'DETAILS' && !parent.hasAttribute('open') && !parent.querySelector(':scope > summary')?.contains(element)) { collapsed = true; break; }
    }
    if (collapsed) continue;
    if (doc.defaultView?.getComputedStyle?.(element).visibility === 'hidden') continue;
    const rect = element.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) result.push({ left: rect.left, top: rect.top, width: rect.width, height: rect.height });
  }
  return result;
}
