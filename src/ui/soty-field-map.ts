import { icon } from "../icons";
import type { IconName } from "../icons";
import type { SotyFieldActions, SotyFieldItem, SotyFieldKind } from "./soty-field";

type Point = {
  readonly x: number;
  readonly y: number;
};

type MapState = {
  panX: number;
  panY: number;
  zoom: number;
};

type HexMetrics = {
  readonly stepX: number;
  readonly stepY: number;
};

const stateByRoot = new WeakMap<HTMLElement, MapState>();
const cleanupByRoot = new WeakMap<HTMLElement, () => void>();
const suppressUntilById = new Map<string, number>();
const minZoom = 0.68;
const maxZoom = 1.7;
const tapSlopPx = 10;
const longPressMs = 540;
const defaultStepX = 112;
const defaultStepY = 86;

export function renderSotyFieldMap(root: HTMLElement, items: readonly SotyFieldItem[], actions: SotyFieldActions): void {
  cleanupByRoot.get(root)?.();
  const metrics = hexMetrics(root);
  const state = mapState(root, items.length, metrics);
  root.dataset.empty = items.length > 0 ? "0" : "1";
  root.innerHTML = `<div class="soty-field-map-canvas">${mapHtml(root, items, metrics)}</div>`;
  const canvas = root.querySelector<HTMLElement>(".soty-field-map-canvas");
  if (!canvas) {
    return;
  }
  applyTransform(canvas, state);
  installPan(root, canvas, state, actions);
  root.querySelectorAll<HTMLButtonElement>("[data-soty-map-id]").forEach((button) => {
    const id = button.dataset.sotyMapId || "";
    button.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      actions.menu(id, event.clientX, event.clientY);
    });
    button.addEventListener("click", (event) => {
      if (shouldSuppress(id)) {
        event.preventDefault();
        return;
      }
      actions.select(id);
    });
  });
}

export function resetSotyFieldMap(root: HTMLElement): void {
  const itemCount = root.querySelectorAll("[data-soty-map-id]").length;
  const state = mapState(root, itemCount, hexMetrics(root));
  const view = initialView(root, itemCount, hexMetrics(root));
  state.panX = view.panX;
  state.panY = view.panY;
  state.zoom = view.zoom;
  const canvas = root.querySelector<HTMLElement>(".soty-field-map-canvas");
  if (canvas) {
    applyTransform(canvas, state);
  }
}

function mapHtml(root: HTMLElement, items: readonly SotyFieldItem[], metrics: HexMetrics): string {
  const radius = fieldRadius(root, items.length);
  const positions = gridPositions(radius);
  return positions.map((position, index) => {
    const point = positionPoint(position, metrics);
    const item = items[index];
    if (!item) {
      return `<span class="soty-field-map-slot" style="--x:${point.left}px;--y:${point.top}px"></span>`;
    }
    return mapCellHtml(item, point);
  }).join("");
}

function mapCellHtml(item: SotyFieldItem, point: { readonly left: number; readonly top: number }): string {
  const badges = [
    item.agentMode ? mapBadge("agent", "Агент") : "",
    item.remote ? mapBadge("remote", "Доступ") : "",
    !item.remote && item.access ? mapBadge("shield", "Можно подключиться") : "",
    item.apps > 0 ? `<span class="soty-field-map-badge text" aria-label="Mini-apps">${escapeHtml(String(item.apps))}</span>` : "",
    item.peers > 1 ? `<span class="soty-field-map-badge text" aria-label="Устройства">${escapeHtml(String(item.peers))}</span>` : ""
  ].filter(Boolean).join("");
  return `
    <button class="soty-field-map-cell${item.active ? " is-active" : ""}${item.unread ? " has-unread" : ""}" type="button" role="listitem"
      data-soty-map-id="${escapeHtml(item.id)}" style="--x:${point.left}px;--y:${point.top}px;--soty-field-color:${escapeHtml(item.color)}" aria-label="${escapeHtml(item.label)}" data-tooltip="${escapeHtml(item.label)}">
      <span class="soty-field-map-kind">${icon(kindIcon(item.kind))}</span>
      <span class="soty-field-map-core">${escapeHtml(initials(item.label))}</span>
      <b>${escapeHtml(item.label)}</b>
      ${badges ? `<span class="soty-field-map-badges">${badges}</span>` : ""}
      ${item.unread ? "<i></i>" : ""}
    </button>
  `;
}

function installPan(root: HTMLElement, canvas: HTMLElement, state: MapState, actions: SotyFieldActions): void {
  let dragging = false;
  let moved = false;
  let startX = 0;
  let startY = 0;
  let baseX = 0;
  let baseY = 0;
  let pinchDistance = 0;
  let pinchZoom = state.zoom;
  let pressPointerId = 0;
  let pressTargetId = "";
  let pressX = 0;
  let pressY = 0;
  let pressHeld = false;
  let pressTimer = 0;
  const pointers = new Map<number, Point>();
  const cancelPress = () => {
    window.clearTimeout(pressTimer);
    pressTimer = 0;
  };
  const clearPress = () => {
    cancelPress();
    pressPointerId = 0;
    pressTargetId = "";
    pressHeld = false;
  };
  const down = (event: PointerEvent) => {
    if (event.button !== 0 && event.pointerType !== "touch") {
      return;
    }
    pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    moved = false;
    if (pointers.size >= 2) {
      const pair = firstTwoPoints(pointers);
      pinchDistance = distance(pair[0], pair[1]);
      pinchZoom = state.zoom;
      dragging = false;
      if (pressTargetId) {
        suppressClick(pressTargetId);
      }
      clearPress();
    } else {
      const cell = closestMapCell(event.target, root);
      dragging = true;
      startX = event.clientX;
      startY = event.clientY;
      baseX = state.panX;
      baseY = state.panY;
      pressPointerId = event.pointerId;
      pressTargetId = cell?.dataset.sotyMapId || "";
      pressX = event.clientX;
      pressY = event.clientY;
      pressHeld = false;
      cancelPress();
      if (pressTargetId) {
        pressTimer = window.setTimeout(() => {
          pressHeld = true;
          moved = true;
          suppressClick(pressTargetId);
          actions.menu(pressTargetId, pressX, pressY);
        }, longPressMs);
      }
    }
    try {
      root.setPointerCapture(event.pointerId);
    } catch {
      // Pointer capture can fail for cancelled synthetic events.
    }
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
        moved = true;
        if (pressTargetId) {
          suppressClick(pressTargetId);
        }
        cancelPress();
        zoomAt(root, canvas, state, midpoint(pair[0], pair[1]), pinchZoom * (nextDistance / pinchDistance));
      }
      event.preventDefault();
      return;
    }
    if (!dragging) {
      return;
    }
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    if (Math.hypot(dx, dy) < tapSlopPx) {
      return;
    }
    moved = true;
    if (pressTargetId) {
      suppressClick(pressTargetId);
    }
    cancelPress();
    state.panX = baseX + dx;
    state.panY = baseY + dy;
    applyTransform(canvas, state);
    event.preventDefault();
  };
  const up = (event: PointerEvent) => {
    const shouldSelect = event.pointerId === pressPointerId
      && Boolean(pressTargetId)
      && !pressHeld
      && !moved;
    pointers.delete(event.pointerId);
    dragging = pointers.size === 1;
    try {
      root.releasePointerCapture(event.pointerId);
    } catch {
      // Capture may already be released.
    }
    if (shouldSelect) {
      actions.select(pressTargetId);
      suppressClick(pressTargetId);
      event.preventDefault();
    }
    if (event.pointerId === pressPointerId) {
      clearPress();
    }
  };
  const wheel = (event: WheelEvent) => {
    event.preventDefault();
    if (!event.ctrlKey && (event.shiftKey || event.altKey)) {
      state.panX -= event.shiftKey && event.deltaX === 0 ? event.deltaY : event.deltaX;
      state.panY -= event.altKey ? event.deltaY : 0;
      applyTransform(canvas, state);
      return;
    }
    const primaryDelta = event.deltaY || event.deltaX;
    const modeFactor = event.deltaMode === WheelEvent.DOM_DELTA_LINE ? 0.045 : 0.0018;
    const factor = Math.exp(-primaryDelta * modeFactor);
    zoomAt(root, canvas, state, { x: event.clientX, y: event.clientY }, state.zoom * factor);
  };
  root.addEventListener("pointerdown", down);
  root.addEventListener("pointermove", move);
  root.addEventListener("pointerup", up);
  root.addEventListener("pointercancel", up);
  root.addEventListener("wheel", wheel, { passive: false });
  cleanupByRoot.set(root, () => {
    clearPress();
    root.removeEventListener("pointerdown", down);
    root.removeEventListener("pointermove", move);
    root.removeEventListener("pointerup", up);
    root.removeEventListener("pointercancel", up);
    root.removeEventListener("wheel", wheel);
  });
}

function closestMapCell(target: EventTarget | null, root: HTMLElement): HTMLButtonElement | null {
  if (!(target instanceof Element)) {
    return null;
  }
  const button = target.closest<HTMLButtonElement>("button.soty-field-map-cell[data-soty-map-id]");
  return button && root.contains(button) ? button : null;
}

function suppressClick(id: string): void {
  if (id) {
    suppressUntilById.set(id, Date.now() + 350);
  }
}

function shouldSuppress(id: string): boolean {
  const until = suppressUntilById.get(id) || 0;
  if (!until) {
    return false;
  }
  if (Date.now() > until) {
    suppressUntilById.delete(id);
    return false;
  }
  return true;
}

function zoomAt(root: HTMLElement, canvas: HTMLElement, state: MapState, clientPoint: Point, nextZoom: number): void {
  const rect = root.getBoundingClientRect();
  const localX = clientPoint.x - rect.left - rect.width / 2;
  const localY = clientPoint.y - rect.top - rect.height / 2;
  const clamped = clamp(nextZoom, minZoom, maxZoom);
  const worldX = (localX - state.panX) / state.zoom;
  const worldY = (localY - state.panY) / state.zoom;
  state.zoom = clamped;
  state.panX = localX - worldX * state.zoom;
  state.panY = localY - worldY * state.zoom;
  applyTransform(canvas, state);
}

function applyTransform(canvas: HTMLElement, state: MapState): void {
  canvas.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.zoom})`;
}

function mapState(root: HTMLElement, count: number, metrics: HexMetrics): MapState {
  const existing = stateByRoot.get(root);
  if (existing) {
    return existing;
  }
  const fresh = initialView(root, count, metrics);
  stateByRoot.set(root, fresh);
  return fresh;
}

function initialView(root: HTMLElement, count: number, metrics: HexMetrics): MapState {
  const zoom = initialZoom(root, count, metrics);
  const positions = gridPositions(occupiedRing(count)).slice(0, Math.max(count, 1));
  const points = positions.map((position) => positionPoint(position, metrics));
  const minX = Math.min(...points.map((point) => point.left));
  const maxX = Math.max(...points.map((point) => point.left));
  const minY = Math.min(...points.map((point) => point.top));
  const maxY = Math.max(...points.map((point) => point.top));
  return {
    panX: -((minX + maxX) / 2) * zoom,
    panY: -((minY + maxY) / 2) * zoom,
    zoom
  };
}

function initialZoom(root: HTMLElement, count: number, metrics: HexMetrics): number {
  const rect = root.getBoundingClientRect();
  if (count <= 1 || rect.width <= 0 || rect.height <= 0) {
    return 1;
  }
  const ring = occupiedRing(count);
  const neededWidth = ring * metrics.stepX * 2 + 138;
  const neededHeight = ring * metrics.stepY * 2 + 96;
  const fit = Math.min(1, rect.width / neededWidth, rect.height / neededHeight);
  return clamp(fit, 0.74, 1);
}

function occupiedRing(count: number): number {
  let ring = 0;
  let capacity = 1;
  while (capacity < count) {
    ring += 1;
    capacity += ring * 6;
  }
  return ring;
}

function fieldRadius(root: HTMLElement, count: number): number {
  const rect = root.getBoundingClientRect();
  const bySize = Math.ceil(Math.max(rect.width / 180, rect.height / 145)) + 1;
  const byCount = Math.ceil(Math.sqrt(Math.max(count, 1))) + 2;
  return Math.max(3, bySize, byCount);
}

function hexMetrics(root: HTMLElement): HexMetrics {
  const style = getComputedStyle(root);
  return {
    stepX: readCssPixel(style, "--soty-map-step-x", defaultStepX),
    stepY: readCssPixel(style, "--soty-map-step-y", defaultStepY)
  };
}

function readCssPixel(style: CSSStyleDeclaration, property: string, fallback: number): number {
  const parsed = Number.parseFloat(style.getPropertyValue(property));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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

function positionPoint([q, r]: [number, number], metrics: HexMetrics): { readonly left: number; readonly top: number } {
  return {
    left: q * metrics.stepX,
    top: (r + q / 2) * metrics.stepY
  };
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

function mapBadge(name: IconName, label: string): string {
  return `<span class="soty-field-map-badge" aria-label="${escapeHtml(label)}">${icon(name)}</span>`;
}

function kindIcon(kind: SotyFieldKind): IconName {
  if (kind === "agent") {
    return "agent";
  }
  if (kind === "self") {
    return "person";
  }
  return "mail";
}

function initials(label: string): string {
  const parts = label.trim().split(/\s+/u).filter(Boolean);
  const value = parts.length > 1
    ? `${parts[0]?.[0] || ""}${parts[1]?.[0] || ""}`
    : (parts[0] || "S").slice(0, 2);
  return value.toLocaleUpperCase("ru-RU");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}
