import { icon } from "../icons";

export interface CounterpartyMenuActions {
  readonly attach: () => void;
  readonly remote: () => void;
  readonly close: () => void;
}

export function openCounterpartyMenu(x: number, y: number, actions: CounterpartyMenuActions): void {
  closeCounterpartyMenu();
  const menu = document.createElement("div");
  menu.className = "counterparty-menu";
  menu.innerHTML = `
    <button type="button" data-action="attach" aria-label="attach">${icon("clip")}</button>
    <button type="button" data-action="remote" aria-label="remote">${icon("remote")}</button>
    <button type="button" data-action="close" aria-label="close">${icon("close")}</button>
  `;
  document.body.append(menu);
  const rect = menu.getBoundingClientRect();
  menu.style.left = `${Math.min(window.innerWidth - rect.width - 8, Math.max(8, x))}px`;
  menu.style.top = `${Math.min(window.innerHeight - rect.height - 8, Math.max(8, y))}px`;

  const off = () => closeCounterpartyMenu();
  window.setTimeout(() => {
    window.addEventListener("pointerdown", off, { once: true });
    window.addEventListener("keydown", off, { once: true });
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
  document.querySelector(".counterparty-menu")?.remove();
}
