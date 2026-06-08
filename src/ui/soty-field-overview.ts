import { icon } from "../icons";
import type { IconName } from "../icons";
import type { SotyFieldActions, SotyFieldItem, SotyFieldKind } from "./soty-field";

let currentOverview: HTMLElement | null = null;
let currentCleanup: (() => void) | null = null;
let previousFocus: HTMLElement | null = null;

const longPressMs = 540;

export function openSotyFieldOverview(items: readonly SotyFieldItem[], actions: SotyFieldActions): void {
  closeSotyFieldOverview();
  previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const overlay = document.createElement("div");
  overlay.className = "action-modal soty-field-overview-modal";
  overlay.innerHTML = `
    <section class="action-sheet soty-field-overview" role="dialog" aria-modal="true" aria-label="Поле сот">
      <header class="action-head">
        <span class="action-mark">${icon("hexagon")}</span>
        <span>
          <b>Поле сот</b>
          <small>${fieldSummary(items)}</small>
        </span>
        <button class="action-close icon-button" type="button" aria-label="Закрыть" data-tooltip="Закрыть">${icon("close")}</button>
      </header>
      <div class="soty-field-overview-grid" role="list" aria-label="Все соты">
        ${items.length > 0 ? items.map(overviewCellHtml).join("") : emptyOverviewHtml()}
      </div>
      <button class="soty-field-overview-connect" type="button">
        ${icon("qr")}
        <span>Подключить</span>
      </button>
    </section>
  `;
  document.body.append(overlay);
  currentOverview = overlay;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      closeSotyFieldOverview();
    }
  });
  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") {
      closeSotyFieldOverview();
    }
  };
  document.addEventListener("keydown", onKeyDown);
  currentCleanup = () => document.removeEventListener("keydown", onKeyDown);
  overlay.querySelector<HTMLButtonElement>(".action-close")?.addEventListener("click", closeSotyFieldOverview);
  overlay.querySelector<HTMLButtonElement>(".soty-field-overview-connect")?.addEventListener("click", () => {
    closeSotyFieldOverview();
    actions.connect();
  });
  overlay.querySelectorAll<HTMLButtonElement>("[data-soty-field-overview-id]").forEach((button) => {
    bindOverviewCell(button, button.dataset.sotyFieldOverviewId || "", actions);
  });
  const focusTarget = overlay.querySelector<HTMLButtonElement>(".soty-field-overview-cell.is-active")
    || overlay.querySelector<HTMLButtonElement>(".soty-field-overview-cell")
    || overlay.querySelector<HTMLButtonElement>(".soty-field-overview-connect")
    || overlay.querySelector<HTMLButtonElement>(".action-close");
  focusTarget?.focus();
}

export function closeSotyFieldOverview(): void {
  currentCleanup?.();
  currentCleanup = null;
  currentOverview?.remove();
  currentOverview = null;
  previousFocus?.focus();
  previousFocus = null;
}

function overviewCellHtml(item: SotyFieldItem): string {
  return `
    <button class="soty-field-overview-cell${item.active ? " is-active" : ""}${item.unread ? " has-unread" : ""}" type="button" role="listitem"
      data-soty-field-overview-id="${escapeHtml(item.id)}" style="--soty-field-color:${escapeHtml(item.color)}" aria-label="${escapeHtml(item.label)}">
      <span class="soty-field-overview-kind">${icon(kindIcon(item.kind))}</span>
      <span class="soty-field-overview-core">${escapeHtml(initials(item.label))}</span>
      <span class="soty-field-overview-copy">
        <b>${escapeHtml(item.label)}</b>
        <small>${itemStatus(item)}</small>
      </span>
      ${badgesHtml(item)}
      ${item.unread ? "<i></i>" : ""}
    </button>
  `;
}

function emptyOverviewHtml(): string {
  return `
    <div class="soty-field-overview-empty" role="listitem">
      ${icon("qr")}
      <span>Подключите первую соту</span>
    </div>
  `;
}

function bindOverviewCell(button: HTMLButtonElement, id: string, actions: SotyFieldActions): void {
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
  const openMenu = (x: number, y: number) => {
    menuOpened = true;
    closeSotyFieldOverview();
    actions.menu(id, x, y);
  };
  button.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    clearPress();
    openMenu(event.clientX, event.clientY);
  });
  button.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 && event.pointerType !== "touch") {
      return;
    }
    menuOpened = false;
    pressX = event.clientX;
    pressY = event.clientY;
    clearPress();
    pressTimer = window.setTimeout(() => openMenu(pressX, pressY), longPressMs);
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
    closeSotyFieldOverview();
    actions.select(id);
  });
}

function fieldSummary(items: readonly SotyFieldItem[]): string {
  if (items.length === 0) {
    return "нет подключений";
  }
  const unread = items.filter((item) => item.unread).length;
  const agents = items.filter((item) => item.agentMode || item.kind === "agent").length;
  return [
    `${items.length} ${pluralRu(items.length, "сота", "соты", "сот")}`,
    unread > 0 ? `${unread} ${pluralRu(unread, "новое", "новых", "новых")}` : "",
    agents > 0 ? `${agents} ${pluralRu(agents, "агент", "агента", "агентов")}` : ""
  ].filter(Boolean).join(" · ");
}

function itemStatus(item: SotyFieldItem): string {
  return [
    item.kind === "agent" ? "агент" : item.kind === "self" ? "вы" : "чат",
    item.remote ? "доступ" : item.access ? "можно подключиться" : "",
    item.apps > 0 ? `${item.apps} app` : "",
    item.peers > 1 ? `${item.peers} ${pluralRu(item.peers, "устройство", "устройства", "устройств")}` : ""
  ].filter(Boolean).join(" · ");
}

function badgesHtml(item: SotyFieldItem): string {
  const badges = [
    item.agentMode ? badge("agent", "Агент") : "",
    item.remote ? badge("remote", "Доступ") : "",
    !item.remote && item.access ? badge("shield", "Можно подключиться") : "",
    item.apps > 0 ? textBadge(String(item.apps), "Mini-apps") : "",
    item.peers > 1 ? textBadge(String(item.peers), "Устройства") : ""
  ].filter(Boolean).join("");
  return badges ? `<span class="soty-field-overview-badges">${badges}</span>` : "";
}

function badge(name: IconName, label: string): string {
  return `<span class="soty-field-overview-badge" aria-label="${escapeHtml(label)}">${icon(name)}</span>`;
}

function textBadge(value: string, label: string): string {
  return `<span class="soty-field-overview-badge text" aria-label="${escapeHtml(label)}">${escapeHtml(value)}</span>`;
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

function pluralRu(count: number, one: string, few: string, many: string): string {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) {
    return one;
  }
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) {
    return few;
  }
  return many;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}
