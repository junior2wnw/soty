import { icon } from "../icons";
import type { IconName } from "../icons";

export interface SotyAppDockItem {
  readonly key: string;
  readonly title: string;
  readonly icon: IconName;
  readonly active: boolean;
}

export interface SotyAppDockActions {
  readonly open: (key: string) => void;
  readonly all: () => void;
}

const maxDockItems = 4;

export function renderSotyAppDock(
  root: HTMLElement,
  items: readonly SotyAppDockItem[],
  totalCount: number,
  actions: SotyAppDockActions
): void {
  const visible = items.slice(0, maxDockItems);
  root.hidden = totalCount < 1;
  if (totalCount < 1) {
    root.innerHTML = "";
    return;
  }
  root.innerHTML = `
    <button class="soty-app-dock-all" type="button" aria-label="Mini-apps" data-tooltip="Mini-apps">
      ${icon("apps")}
      <span>${escapeHtml(String(totalCount))}</span>
    </button>
    ${visible.map(renderDockItem).join("")}
  `;
  root.querySelector<HTMLButtonElement>(".soty-app-dock-all")?.addEventListener("click", actions.all);
  root.querySelectorAll<HTMLButtonElement>("[data-soty-app-key]").forEach((button) => {
    button.addEventListener("click", () => {
      actions.open(button.dataset.sotyAppKey || "");
    });
  });
}

function renderDockItem(item: SotyAppDockItem): string {
  return `
    <button class="soty-app-dock-item${item.active ? " is-active" : ""}" type="button" data-soty-app-key="${escapeHtml(item.key)}" aria-label="${escapeHtml(item.title)}" data-tooltip="${escapeHtml(item.title)}">
      ${icon(item.icon)}
      <span>${escapeHtml(shortTitle(item.title))}</span>
    </button>
  `;
}

function shortTitle(value: string): string {
  return value.trim().slice(0, 18) || "App";
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}
