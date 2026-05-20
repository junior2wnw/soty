export interface HexItem {
  readonly id: string;
  readonly label: string;
  readonly color: string;
  readonly active: boolean;
  readonly unread: boolean;
}

export interface HexFieldActions {
  readonly select: (id: string) => void;
  readonly menu: (id: string, x: number, y: number) => void;
}

let panX = 0;
let panY = 0;
let zoom = 1;
let movedDuringPointer = false;
let cancelActiveHexPress: (() => void) | null = null;
const panCleanups = new WeakMap<HTMLElement, () => void>();
const hexStepX = 62;
const hexStepY = 72;
const minZoom = 0.62;
const maxZoom = 1.7;

type HexMetrics = {
  readonly stepX: number;
  readonly stepY: number;
};

type Point = {
  readonly x: number;
  readonly y: number;
};

export function renderHexField(
  root: HTMLElement,
  items: readonly HexItem[],
  actions: HexFieldActions
): void {
  root.innerHTML = `<div class="hex-map"></div>`;
  const map = root.querySelector<HTMLDivElement>(".hex-map");
  if (!map) {
    return;
  }
  applyHexTransform(map);
  const rect = root.getBoundingClientRect();
  const fieldRadius = Math.max(
    5,
    Math.ceil(Math.max(rect.width / 110, rect.height / 92)) + 2,
    Math.ceil(Math.sqrt(Math.max(items.length, 1))) + 4
  );
  const metrics = hexMetrics(root);
  const positions = gridPositions(fieldRadius);
  const itemByIndex = new Map(items.map((item, index) => [index, item]));
  map.innerHTML = positions.map((position, index) => {
    const { left, top } = positionPoint(position, metrics);
    const item = itemByIndex.get(index);
    if (!item) {
      return `<div class="retro-hex hex hex-cell" style="--x:${left}px;--y:${top}px"></div>`;
    }
    return `
      <button class="retro-hex hex filled${item.active ? " active" : ""}" data-id="${item.id}" type="button"
        style="--x:${left}px;--y:${top}px;--color:${item.color}" aria-label="${escapeHtml(item.label)}" data-tooltip="Открыть ${escapeHtml(item.label)}">
        <span class="hex-core"><span>${escapeHtml(initials(item.label))}</span></span>
        <b>${escapeHtml(item.label)}</b>
        <em></em>
        ${item.unread ? "<i></i>" : ""}
      </button>
    `;
  }).join("");

  installPan(root, map);
  map.querySelectorAll<HTMLButtonElement>(".hex.filled").forEach((button) => {
    let timer = 0;
    let held = false;
    let pressX = 0;
    let pressY = 0;
    const id = button.dataset.id || "";
    const open = (x: number, y: number) => actions.menu(id, x, y);
    button.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      open(event.clientX, event.clientY);
    });
    button.addEventListener("pointerdown", (event) => {
      held = false;
      movedDuringPointer = false;
      pressX = event.clientX;
      pressY = event.clientY;
      window.addEventListener("pointerup", finishPress, { once: true });
      window.addEventListener("pointercancel", finishPress, { once: true });
      const cancelPress = () => {
        window.clearTimeout(timer);
        held = false;
      };
      cancelActiveHexPress = cancelPress;
      timer = window.setTimeout(() => {
        held = true;
        open(event.clientX, event.clientY);
      }, 560);
    });
    button.addEventListener("pointermove", (event) => {
      if (Math.abs(event.clientX - pressX) + Math.abs(event.clientY - pressY) >= 5) {
        movedDuringPointer = true;
        window.clearTimeout(timer);
        if (cancelActiveHexPress) {
          cancelActiveHexPress();
          cancelActiveHexPress = null;
        }
      }
    });
    const finishPress = () => {
      window.clearTimeout(timer);
      if (cancelActiveHexPress) {
        cancelActiveHexPress = null;
      }
    };
    button.addEventListener("pointerup", finishPress);
    button.addEventListener("pointercancel", finishPress);
    button.addEventListener("pointerleave", finishPress);
    button.addEventListener("click", (event) => {
      if (held || movedDuringPointer) {
        event.preventDefault();
        return;
      }
      actions.select(id);
    });
  });
}

function installPan(root: HTMLElement, map: HTMLElement): void {
  panCleanups.get(root)?.();
  let dragging = false;
  let startX = 0;
  let startY = 0;
  let baseX = 0;
  let baseY = 0;
  let pinchDistance = 0;
  let pinchZoom = zoom;
  const pointers = new Map<number, Point>();
  const down = (event: PointerEvent) => {
    pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    movedDuringPointer = false;
    if (pointers.size >= 2) {
      const pair = firstTwoPoints(pointers);
      pinchDistance = distance(pair[0], pair[1]);
      pinchZoom = zoom;
      dragging = false;
      if (cancelActiveHexPress) {
        cancelActiveHexPress();
        cancelActiveHexPress = null;
      }
    } else {
      dragging = true;
      startX = event.clientX;
      startY = event.clientY;
      baseX = panX;
      baseY = panY;
    }
    root.setPointerCapture(event.pointerId);
  };
  const move = (event: PointerEvent) => {
    if (!pointers.has(event.pointerId)) {
      return;
    }
    pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    if (pointers.size >= 2) {
      const pair = firstTwoPoints(pointers);
      const nextDistance = distance(pair[0], pair[1]);
      if (pinchDistance > 0 && nextDistance > 0) {
        movedDuringPointer = true;
        if (cancelActiveHexPress) {
          cancelActiveHexPress();
          cancelActiveHexPress = null;
        }
        zoomAt(root, map, midpoint(pair[0], pair[1]), pinchZoom * (nextDistance / pinchDistance));
      }
      event.preventDefault();
      return;
    }
    if (!dragging) {
      return;
    }
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    if (Math.abs(dx) + Math.abs(dy) < 5) {
      return;
    }
    movedDuringPointer = true;
    if (cancelActiveHexPress) {
      cancelActiveHexPress();
      cancelActiveHexPress = null;
    }
    panX = baseX + dx;
    panY = baseY + dy;
    applyHexTransform(map);
  };
  const up = (event: PointerEvent) => {
    pointers.delete(event.pointerId);
    if (pointers.size === 1) {
      const remaining = pointers.values().next().value;
      if (remaining) {
        dragging = true;
        startX = remaining.x;
        startY = remaining.y;
        baseX = panX;
        baseY = panY;
      }
    } else {
      dragging = false;
    }
    try {
      root.releasePointerCapture(event.pointerId);
    } catch {
      // The capture may already be released by the browser.
    }
  };
  const wheel = (event: WheelEvent) => {
    event.preventDefault();
    if (event.ctrlKey || event.shiftKey) {
      const factor = Math.exp(-event.deltaY * 0.0018);
      zoomAt(root, map, { x: event.clientX, y: event.clientY }, zoom * factor);
      return;
    }
    panX -= event.deltaX;
    panY -= event.deltaY;
    movedDuringPointer = true;
    applyHexTransform(map);
  };
  root.addEventListener("pointerdown", down);
  root.addEventListener("pointermove", move);
  root.addEventListener("pointerup", up);
  root.addEventListener("pointercancel", up);
  root.addEventListener("wheel", wheel, { passive: false });
  panCleanups.set(root, () => {
    root.removeEventListener("pointerdown", down);
    root.removeEventListener("pointermove", move);
    root.removeEventListener("pointerup", up);
    root.removeEventListener("pointercancel", up);
    root.removeEventListener("wheel", wheel);
  });
}

function applyHexTransform(map: HTMLElement): void {
  map.style.transform = `translate(${panX}px, ${panY}px) scale(${zoom})`;
}

function zoomAt(root: HTMLElement, map: HTMLElement, clientPoint: Point, nextZoom: number): void {
  const rect = root.getBoundingClientRect();
  const localX = clientPoint.x - rect.left - rect.width / 2;
  const localY = clientPoint.y - rect.top - rect.height / 2;
  const clamped = clamp(nextZoom, minZoom, maxZoom);
  const worldX = (localX - panX) / zoom;
  const worldY = (localY - panY) / zoom;
  zoom = clamped;
  panX = localX - worldX * zoom;
  panY = localY - worldY * zoom;
  applyHexTransform(map);
}

function firstTwoPoints(points: Map<number, Point>): [Point, Point] {
  const values = [...points.values()];
  return [values[0] || { x: 0, y: 0 }, values[1] || { x: 0, y: 0 }];
}

function midpoint(left: Point, right: Point): Point {
  return {
    x: (left.x + right.x) / 2,
    y: (left.y + right.y) / 2
  };
}

function distance(left: Point, right: Point): number {
  return Math.hypot(left.x - right.x, left.y - right.y);
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function gridPositions(radius: number): [number, number][] {
  const result: [number, number][] = [[0, 0]];
  for (let ring = 1; ring <= radius; ring += 1) {
    result.push(...ringPositions(ring));
  }
  return result;
}

function ringPositions(radius: number): [number, number][] {
  const result: [number, number][] = [];
  const dirs: [number, number][] = [[1, 0], [1, -1], [0, -1], [-1, 0], [-1, 1], [0, 1]];
  let q = -radius;
  let r = radius;
  for (const [dq, dr] of dirs) {
    for (let step = 0; step < radius; step += 1) {
      result.push([q, r]);
      q += dq;
      r += dr;
    }
  }
  return result;
}

function hexMetrics(root: HTMLElement): HexMetrics {
  const style = getComputedStyle(root);
  return {
    stepX: readCssPixel(style, "--hex-step-x", hexStepX),
    stepY: readCssPixel(style, "--hex-step-y", hexStepY)
  };
}

function readCssPixel(style: CSSStyleDeclaration, property: string, fallback: number): number {
  const parsed = Number.parseFloat(style.getPropertyValue(property));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function positionPoint([q, r]: [number, number], metrics: HexMetrics): { readonly left: number; readonly top: number } {
  return {
    left: q * metrics.stepX,
    top: (r + q / 2) * metrics.stepY
  };
}

function initials(value: string): string {
  const parts = value.trim().split(/\s+/u).filter(Boolean);
  const letters = parts.length > 1
    ? `${parts[0]?.[0] ?? ""}${parts[1]?.[0] ?? ""}`
    : value.trim().slice(0, 2);
  return letters || ".";
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/gu, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  })[char] || char);
}
