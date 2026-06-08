import { icon } from "../icons";

type IconName = Parameters<typeof icon>[0];

export type SotyFieldKind = "agent" | "self" | "chat";

export interface SotyFieldItem {
  readonly id: string;
  readonly label: string;
  readonly color: string;
  readonly kind: SotyFieldKind;
  readonly active: boolean;
  readonly unread: boolean;
  readonly agentMode: boolean;
  readonly remote: boolean;
  readonly access: boolean;
  readonly apps: number;
  readonly peers: number;
}

export interface SotyFieldActions {
  readonly connect: () => void;
  readonly select: (id: string) => void;
  readonly menu: (id: string, x: number, y: number) => void;
}

const longPressMs = 540;

export function renderSotyField(root: HTMLElement, items: readonly SotyFieldItem[], actions: SotyFieldActions): void {
  root.dataset.empty = items.length > 0 ? "0" : "1";
  root.innerHTML = `
    <button class="soty-field-connect" type="button" aria-label="Подключить" data-tooltip="Подключить QR">
      ${icon("qr")}
      <span>Подключить</span>
    </button>
    <div class="soty-field-map" role="list" aria-label="Соты">
      ${items.length > 0 ? items.map(renderSotyFieldCell).join("") : renderEmptySotyFieldCell()}
    </div>
  `;
  root.querySelector<HTMLButtonElement>(".soty-field-connect")?.addEventListener("click", actions.connect);
  root.querySelector<HTMLButtonElement>(".soty-field-empty")?.addEventListener("click", actions.connect);
  root.querySelectorAll<HTMLButtonElement>("[data-soty-field-id]").forEach((button) => {
    const id = button.dataset.sotyFieldId || "";
    bindCell(button, id, actions);
  });
}

function renderSotyFieldCell(item: SotyFieldItem): string {
  const badges = [
    item.agentMode ? fieldBadge("agent", "Agent") : "",
    item.remote ? fieldBadge("remote", "Доступ") : "",
    !item.remote && item.access ? fieldBadge("shield", "Можно подключиться") : "",
    item.apps > 0 ? `<span class="soty-field-badge text" aria-label="Mini-apps">${escapeHtml(String(item.apps))}</span>` : "",
    item.peers > 1 ? `<span class="soty-field-badge text" aria-label="Устройства">${escapeHtml(String(item.peers))}</span>` : ""
  ].filter(Boolean).join("");
  return `
    <button class="soty-field-cell${item.active ? " is-active" : ""}${item.unread ? " has-unread" : ""}" type="button" role="listitem"
      data-soty-field-id="${escapeHtml(item.id)}" style="--soty-field-color:${escapeHtml(item.color)}" aria-label="${escapeHtml(item.label)}" data-tooltip="${escapeHtml(item.label)}">
      <span class="soty-field-mark">${icon(kindIcon(item.kind))}</span>
      <span class="soty-field-core">${escapeHtml(initials(item.label))}</span>
      <b>${escapeHtml(item.label)}</b>
      ${badges ? `<span class="soty-field-badges">${badges}</span>` : ""}
      ${item.unread ? "<i></i>" : ""}
    </button>
  `;
}

function renderEmptySotyFieldCell(): string {
  return `
    <button class="soty-field-cell soty-field-empty" type="button" role="listitem" aria-label="Подключить первую соту" data-tooltip="Подключить QR">
      <span class="soty-field-mark">${icon("qr")}</span>
      <span class="soty-field-core">+</span>
      <b>Новая сота</b>
    </button>
  `;
}

function bindCell(button: HTMLButtonElement, id: string, actions: SotyFieldActions): void {
  if (!id) {
    return;
  }
  let pressTimer = 0;
  let pressX = 0;
  let pressY = 0;
  let menuOpened = false;
  const clearPress = () => {
    window.clearTimeout(pressTimer);
    pressTimer = 0;
  };
  button.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    clearPress();
    menuOpened = true;
    actions.menu(id, event.clientX, event.clientY);
  });
  button.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 && event.pointerType !== "touch") {
      return;
    }
    menuOpened = false;
    pressX = event.clientX;
    pressY = event.clientY;
    clearPress();
    pressTimer = window.setTimeout(() => {
      menuOpened = true;
      actions.menu(id, pressX, pressY);
    }, longPressMs);
  });
  button.addEventListener("pointermove", (event) => {
    if (Math.hypot(event.clientX - pressX, event.clientY - pressY) > 10) {
      clearPress();
    }
  });
  button.addEventListener("pointerup", clearPress);
  button.addEventListener("pointercancel", clearPress);
  button.addEventListener("click", (event) => {
    if (menuOpened) {
      event.preventDefault();
      menuOpened = false;
      return;
    }
    actions.select(id);
  });
}

function fieldBadge(name: IconName, label: string): string {
  return `<span class="soty-field-badge" aria-label="${escapeHtml(label)}">${icon(name)}</span>`;
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
