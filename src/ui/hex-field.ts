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
let movedDuringPointer = false;
const panCleanups = new WeakMap<HTMLElement, () => void>();

export function renderHexField(
  root: HTMLElement,
  items: readonly HexItem[],
  actions: HexFieldActions
): void {
  const positions = gridPositions(Math.max(7, Math.ceil(Math.sqrt(Math.max(items.length, 1))) + 4));
  const itemByIndex = new Map(items.map((item, index) => [index, item]));
  root.innerHTML = `<div class="hex-map"></div>`;
  const map = root.querySelector<HTMLDivElement>(".hex-map");
  if (!map) {
    return;
  }
  map.style.transform = `translate(${panX}px, ${panY}px)`;
  map.innerHTML = positions.map(([q, r], index) => {
    const item = itemByIndex.get(index);
    const left = q * 60;
    const top = (r + q / 2) * 70;
    if (item) {
      return `
        <button class="hex filled${item.active ? " active" : ""}" data-id="${item.id}" type="button"
          style="--x:${left}px;--y:${top}px;--color:${item.color}" aria-label="counterparty">
          <span>${escapeHtml(initials(item.label))}</span>
          <b>${escapeHtml(item.label)}</b>
          ${item.unread ? "<i></i>" : ""}
        </button>
      `;
    }
    return `<div class="hex hex-cell" style="--x:${left}px;--y:${top}px"></div>`;
  }).join("");

  installPan(root, map);
  map.querySelectorAll<HTMLButtonElement>(".hex.filled").forEach((button) => {
    let timer = 0;
    let held = false;
    const id = button.dataset.id || "";
    const open = (x: number, y: number) => actions.menu(id, x, y);
    button.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      open(event.clientX, event.clientY);
    });
    button.addEventListener("pointerdown", (event) => {
      held = false;
      movedDuringPointer = false;
      timer = window.setTimeout(() => {
        held = true;
        open(event.clientX, event.clientY);
      }, 560);
    });
    button.addEventListener("pointermove", () => {
      if (movedDuringPointer) {
        window.clearTimeout(timer);
      }
    });
    button.addEventListener("pointerup", () => window.clearTimeout(timer));
    button.addEventListener("pointercancel", () => window.clearTimeout(timer));
    button.addEventListener("pointerleave", () => window.clearTimeout(timer));
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
  const down = (event: PointerEvent) => {
    dragging = true;
    movedDuringPointer = false;
    startX = event.clientX;
    startY = event.clientY;
    baseX = panX;
    baseY = panY;
    root.setPointerCapture(event.pointerId);
  };
  const move = (event: PointerEvent) => {
    if (!dragging) {
      return;
    }
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    if (Math.abs(dx) + Math.abs(dy) < 5) {
      return;
    }
    movedDuringPointer = true;
    panX = baseX + dx;
    panY = baseY + dy;
    map.style.transform = `translate(${panX}px, ${panY}px)`;
  };
  const up = () => {
    dragging = false;
  };
  root.addEventListener("pointerdown", down);
  root.addEventListener("pointermove", move);
  root.addEventListener("pointerup", up);
  root.addEventListener("pointercancel", up);
  panCleanups.set(root, () => {
    root.removeEventListener("pointerdown", down);
    root.removeEventListener("pointermove", move);
    root.removeEventListener("pointerup", up);
    root.removeEventListener("pointercancel", up);
  });
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
