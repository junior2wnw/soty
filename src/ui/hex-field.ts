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

export function renderHexField(root: HTMLElement, items: readonly HexItem[], actions: HexFieldActions): void {
  const positions = spiral(items.length);
  root.innerHTML = `<div class="hex-map"></div>`;
  const map = root.querySelector<HTMLDivElement>(".hex-map");
  if (!map) {
    return;
  }
  map.style.transform = `translate(${panX}px, ${panY}px)`;
  map.innerHTML = items.map((item, index) => {
    const [q, r] = positions[index] ?? [0, 0];
    const left = 50 + (q + r / 2) * 82;
    const top = 50 + r * 72;
    return `
      <button class="hex${item.active ? " active" : ""}" data-id="${item.id}" type="button"
        style="--x:${left}px;--y:${top}px;--color:${item.color}" aria-label="chat">
        <span>${escapeHtml(initials(item.label))}</span>
        <b>${escapeHtml(item.label)}</b>
        ${item.unread ? "<i></i>" : ""}
      </button>
    `;
  }).join("");

  installPan(root, map);
  map.querySelectorAll<HTMLButtonElement>(".hex").forEach((button) => {
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
      timer = window.setTimeout(() => {
        held = true;
        open(event.clientX, event.clientY);
      }, 560);
    });
    button.addEventListener("pointerup", () => window.clearTimeout(timer));
    button.addEventListener("pointerleave", () => window.clearTimeout(timer));
    button.addEventListener("click", (event) => {
      if (held) {
        event.preventDefault();
        return;
      }
      actions.select(id);
    });
  });
}

function installPan(root: HTMLElement, map: HTMLElement): void {
  let dragging = false;
  let startX = 0;
  let startY = 0;
  let baseX = 0;
  let baseY = 0;
  root.addEventListener("pointerdown", (event) => {
    if ((event.target as HTMLElement).closest(".hex")) {
      return;
    }
    dragging = true;
    startX = event.clientX;
    startY = event.clientY;
    baseX = panX;
    baseY = panY;
    root.setPointerCapture(event.pointerId);
  });
  root.addEventListener("pointermove", (event) => {
    if (!dragging) {
      return;
    }
    panX = baseX + event.clientX - startX;
    panY = baseY + event.clientY - startY;
    map.style.transform = `translate(${panX}px, ${panY}px)`;
  });
  root.addEventListener("pointerup", () => {
    dragging = false;
  });
}

function spiral(count: number): [number, number][] {
  const result: [number, number][] = [[0, 0]];
  const dirs: [number, number][] = [[1, 0], [0, 1], [-1, 1], [-1, 0], [0, -1], [1, -1]];
  for (let radius = 1; result.length < count; radius += 1) {
    let q = -radius;
    let r = 0;
    for (const [dq, dr] of dirs) {
      for (let step = 0; step < radius && result.length < count; step += 1) {
        result.push([q, r]);
        q += dq;
        r += dr;
      }
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
