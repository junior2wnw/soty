type TooltipSide = "top" | "right" | "bottom" | "left";

const tooltipSelector = [
  "[data-tooltip]",
  "[title]",
  "button[aria-label]",
  "a[aria-label]",
  "input[aria-label]",
  "textarea[aria-label]",
  "[role='button'][aria-label]"
].join(",");

const labelText = new Map<string, string>([
  ["attach", "Прикрепить файл"],
  ["close", "Закрыть"],
  ["copy", "Скопировать"],
  ["download", "Скачать обычный установщик"],
  ["install", "Установить приложение"],
  ["knock", "Отправить сигнал"],
  ["machine", "Установить с правами администратора"],
  ["message", "Сообщение"],
  ["ok", "Подтвердить"],
  ["qr", "Показать QR-код"],
  ["refresh", "Обновить"],
  ["remote", "Удаленное подключение"],
  ["run", "Выполнить команду"],
  ["send", "Отправить"]
]);

let cleanup = () => undefined;

export function installTooltips(root: Document | HTMLElement = document): () => void {
  cleanup();

  const ownerDocument = root instanceof Document ? root : root.ownerDocument;
  const tooltip = ownerDocument.createElement("div");
  tooltip.className = "soty-tooltip";
  tooltip.setAttribute("role", "tooltip");
  tooltip.hidden = true;
  ownerDocument.body.append(tooltip);

  let activeElement: HTMLElement | null = null;
  let showTimer = 0;
  let hideTimer = 0;

  const clearTimers = () => {
    window.clearTimeout(showTimer);
    window.clearTimeout(hideTimer);
  };

  const hide = () => {
    clearTimers();
    activeElement = null;
    tooltip.classList.remove("is-visible");
    hideTimer = window.setTimeout(() => {
      tooltip.hidden = true;
      tooltip.textContent = "";
    }, 120);
  };

  const show = (target: HTMLElement, delayMs: number) => {
    const text = tooltipText(target);
    if (!text) {
      hide();
      return;
    }
    clearTimers();
    activeElement = target;
    showTimer = window.setTimeout(() => {
      if (activeElement !== target || !target.isConnected) {
        return;
      }
      tooltip.textContent = text;
      tooltip.hidden = false;
      tooltip.classList.add("is-visible");
      positionTooltip(tooltip, target);
    }, delayMs);
  };

  const onPointerOver = (event: Event) => {
    const target = tooltipTarget(event.target);
    if (!target || containsRelatedTarget(target, event)) {
      return;
    }
    show(target, Number(target.dataset.tooltipDelay || 320));
  };

  const onPointerOut = (event: Event) => {
    if (!activeElement || containsRelatedTarget(activeElement, event)) {
      return;
    }
    hide();
  };

  const onFocusIn = (event: Event) => {
    const target = tooltipTarget(event.target);
    if (target) {
      show(target, 120);
    }
  };

  const onFocusOut = (event: Event) => {
    if (!activeElement || containsRelatedTarget(activeElement, event)) {
      return;
    }
    hide();
  };

  const onReposition = () => {
    if (activeElement && !tooltip.hidden) {
      positionTooltip(tooltip, activeElement);
    }
  };

  const onEscape = (event: KeyboardEvent) => {
    if (event.key === "Escape") {
      hide();
    }
  };

  root.addEventListener("pointerover", onPointerOver);
  root.addEventListener("pointerout", onPointerOut);
  root.addEventListener("focusin", onFocusIn);
  root.addEventListener("focusout", onFocusOut);
  root.addEventListener("pointerdown", hide);
  ownerDocument.defaultView?.addEventListener("scroll", hide, true);
  ownerDocument.defaultView?.addEventListener("resize", onReposition);
  ownerDocument.defaultView?.addEventListener("keydown", onEscape);

  cleanup = () => {
    clearTimers();
    root.removeEventListener("pointerover", onPointerOver);
    root.removeEventListener("pointerout", onPointerOut);
    root.removeEventListener("focusin", onFocusIn);
    root.removeEventListener("focusout", onFocusOut);
    root.removeEventListener("pointerdown", hide);
    ownerDocument.defaultView?.removeEventListener("scroll", hide, true);
    ownerDocument.defaultView?.removeEventListener("resize", onReposition);
    ownerDocument.defaultView?.removeEventListener("keydown", onEscape);
    tooltip.remove();
    activeElement = null;
    cleanup = () => undefined;
  };

  return cleanup;
}

function tooltipTarget(target: EventTarget | null): HTMLElement | null {
  if (!(target instanceof Element)) {
    return null;
  }
  const element = target.closest<HTMLElement>(tooltipSelector);
  if (!element || element.dataset.tooltip === "off") {
    return null;
  }
  if (element instanceof HTMLButtonElement && element.disabled) {
    return null;
  }
  if (element.getAttribute("aria-disabled") === "true") {
    return null;
  }
  return element;
}

function tooltipText(element: HTMLElement): string {
  const explicit = element.dataset.tooltip?.trim();
  if (explicit) {
    return explicit;
  }

  const title = element.getAttribute("title")?.trim();
  if (title) {
    element.dataset.tooltip = title;
    element.removeAttribute("title");
    return title;
  }

  const label = element.getAttribute("aria-label")?.trim();
  if (!label) {
    return "";
  }
  return labelText.get(label.toLowerCase()) || label;
}

function containsRelatedTarget(element: HTMLElement, event: Event): boolean {
  const relatedTarget = (event as PointerEvent | FocusEvent).relatedTarget;
  return relatedTarget instanceof Node && element.contains(relatedTarget);
}

function positionTooltip(tooltip: HTMLElement, target: HTMLElement): void {
  const side = tooltipSide(target);
  const targetRect = target.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  const gap = 10;
  const margin = 8;
  let left = targetRect.left + (targetRect.width - tooltipRect.width) / 2;
  let top = targetRect.top - tooltipRect.height - gap;

  if (side === "bottom") {
    top = targetRect.bottom + gap;
  } else if (side === "left") {
    left = targetRect.left - tooltipRect.width - gap;
    top = targetRect.top + (targetRect.height - tooltipRect.height) / 2;
  } else if (side === "right") {
    left = targetRect.right + gap;
    top = targetRect.top + (targetRect.height - tooltipRect.height) / 2;
  }

  const maxLeft = window.innerWidth - tooltipRect.width - margin;
  const maxTop = window.innerHeight - tooltipRect.height - margin;
  tooltip.style.left = `${clamp(left, margin, Math.max(margin, maxLeft))}px`;
  tooltip.style.top = `${clamp(top, margin, Math.max(margin, maxTop))}px`;
  tooltip.dataset.side = side;
}

function tooltipSide(target: HTMLElement): TooltipSide {
  const side = target.dataset.tooltipSide;
  return side === "right" || side === "bottom" || side === "left" ? side : "top";
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
