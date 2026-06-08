import { icon } from "../icons";
import type { IconName } from "../icons";

export interface SotyActionSheetItem {
  readonly id: string;
  readonly label: string;
  readonly detail: string;
  readonly icon: IconName;
  readonly active?: boolean;
  readonly danger?: boolean;
  readonly disabled?: boolean;
}

export interface SotyActionSheetOptions {
  readonly title: string;
  readonly subtitle: string;
}

export interface SotyActionSheetActions {
  readonly run: (id: string) => void;
}

let currentSheet: HTMLElement | null = null;
let currentCleanup: (() => void) | null = null;
let previousFocus: HTMLElement | null = null;

export function openSotyActionSheet(
  items: readonly SotyActionSheetItem[],
  actions: SotyActionSheetActions,
  options: SotyActionSheetOptions
): void {
  closeSotyActionSheet();
  if (items.length === 0) {
    return;
  }
  previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const overlay = document.createElement("div");
  overlay.className = "action-modal soty-action-sheet-modal";
  overlay.innerHTML = `
    <section class="action-sheet soty-action-sheet" role="dialog" aria-modal="true" aria-label="${escapeHtml(options.title)}">
      <header class="action-head">
        <span class="action-mark">${icon("more")}</span>
        <span>
          <b>${escapeHtml(options.title)}</b>
          <small>${escapeHtml(options.subtitle)}</small>
        </span>
        <button class="action-close icon-button" type="button" aria-label="Закрыть" data-tooltip="Закрыть">${icon("close")}</button>
      </header>
      <div class="soty-action-sheet-list">
        ${items.map(actionRowHtml).join("")}
      </div>
    </section>
  `;
  document.body.append(overlay);
  currentSheet = overlay;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      closeSotyActionSheet();
    }
  });
  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") {
      closeSotyActionSheet();
    }
  };
  document.addEventListener("keydown", onKeyDown);
  currentCleanup = () => {
    document.removeEventListener("keydown", onKeyDown);
  };
  overlay.querySelector<HTMLButtonElement>(".action-close")?.addEventListener("click", closeSotyActionSheet);
  overlay.querySelectorAll<HTMLButtonElement>("[data-soty-action-id]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.sotyActionId || "";
      closeSotyActionSheet();
      actions.run(id);
    });
  });
  overlay.querySelector<HTMLButtonElement>("[data-soty-action-id]")?.focus();
}

export function closeSotyActionSheet(): void {
  currentCleanup?.();
  currentCleanup = null;
  currentSheet?.remove();
  currentSheet = null;
  previousFocus?.focus();
  previousFocus = null;
}

function actionRowHtml(item: SotyActionSheetItem): string {
  return `
    <button class="soty-action-sheet-row${item.active ? " is-on" : ""}${item.danger ? " is-danger" : ""}" type="button" data-soty-action-id="${escapeHtml(item.id)}"${item.disabled ? " disabled" : ""}>
      <span>${icon(item.icon)}</span>
      <span>
        <b>${escapeHtml(item.label)}</b>
        <small>${escapeHtml(item.detail)}</small>
      </span>
    </button>
  `;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}
