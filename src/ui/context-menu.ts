import { icon } from "../icons";

export interface CounterpartyMenuActions {
  readonly attach: () => void;
  readonly remote: () => void;
  readonly close: () => void;
}

export interface CounterpartyMenuState {
  readonly remoteEnabled: boolean;
}

let currentCleanup = () => undefined;

export function openCounterpartyMenu(
  x: number,
  y: number,
  actions: CounterpartyMenuActions,
  state: CounterpartyMenuState = { remoteEnabled: false }
): void {
  closeCounterpartyMenu();
  const menu = document.createElement("div");
  menu.className = "counterparty-menu";
  menu.innerHTML = `
    <button type="button" data-action="attach" aria-label="attach">${icon("clip")}</button>
    <button class="${state.remoteEnabled ? "is-on" : ""}" type="button" data-action="remote" aria-label="remote">${icon("remote")}</button>
    <button type="button" data-action="close" aria-label="close">${icon("close")}</button>
  `;
  document.body.append(menu);
  const rect = menu.getBoundingClientRect();
  menu.style.left = `${Math.min(window.innerWidth - rect.width - 8, Math.max(8, x))}px`;
  menu.style.top = `${Math.min(window.innerHeight - rect.height - 8, Math.max(8, y))}px`;

  const off = (event: Event) => {
    if (event.target instanceof Node && menu.contains(event.target)) {
      return;
    }
    closeCounterpartyMenu();
  };
  window.setTimeout(() => {
    if (!document.body.contains(menu)) {
      return;
    }
    window.addEventListener("pointerdown", off);
    window.addEventListener("keydown", off, { once: true });
    currentCleanup = () => {
      window.removeEventListener("pointerdown", off);
      window.removeEventListener("keydown", off);
      currentCleanup = () => undefined;
    };
  });

  menu.querySelectorAll<HTMLButtonElement>("button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const action = button.dataset.action as keyof CounterpartyMenuActions;
      closeCounterpartyMenu();
      actions[action]?.();
    });
  });
}

export function closeCounterpartyMenu(): void {
  currentCleanup();
  document.querySelector(".counterparty-menu")?.remove();
}
