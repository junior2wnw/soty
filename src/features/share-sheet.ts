import QRCode from "qrcode";
import { icon } from "../icons";
import type { IconName } from "../icons";

type ShareActionContext = {
  readonly overlay: HTMLDivElement;
  readonly close: () => void;
  readonly setNote: (text: string) => void;
};

export type LinkShareAction = {
  readonly id: string;
  readonly label: string;
  readonly icon: IconName;
  readonly tone?: "primary" | "secondary";
  readonly closeOnClick?: boolean;
  readonly run: (context: ShareActionContext) => void | string | Promise<void | string>;
};

export type LinkShareSheetOptions = {
  readonly title: string;
  readonly url: string;
  readonly actions?: readonly LinkShareAction[];
  readonly mount?: HTMLElement;
  readonly onClose?: (overlay: HTMLDivElement) => void;
};

export async function showLinkShareSheet(options: LinkShareSheetOptions): Promise<HTMLDivElement> {
  closeLinkShareSheets();
  const supportsNativeShare = "share" in navigator;
  const overlay = document.createElement("div");
  const primaryActions: readonly LinkShareAction[] = [
    {
      id: "copy",
      label: "Копировать",
      icon: "copy",
      run: async ({ setNote }) => {
        await copyText(options.url);
        setNote("Скопировано");
      }
    },
    ...(supportsNativeShare
      ? [{
        id: "send",
        label: "Отправить",
        icon: "send",
        run: () => navigator.share({ title: options.title, url: options.url }).catch(() => undefined)
      } satisfies LinkShareAction]
      : [])
  ];
  const extraActions = options.actions ?? [];
  const secondaryActions = extraActions.filter((action) => action.tone === "secondary");
  const allPrimaryActions = [
    ...primaryActions,
    ...extraActions.filter((action) => action.tone !== "secondary")
  ];
  const actionGroups = [
    renderActionGroup(allPrimaryActions, ""),
    secondaryActions.length ? renderActionGroup(secondaryActions, " is-secondary") : ""
  ].join("");

  overlay.className = "qr-modal link-share-modal";
  overlay.innerHTML = `
    <div class="qr-sheet link-share-sheet" role="dialog" aria-modal="true" aria-label="Поделиться">
      <button class="icon-button link-share-close" type="button" aria-label="close" data-tooltip="Закрыть">${icon("close")}</button>
      <canvas aria-label="QR"></canvas>
      <div class="link-share-caption">
        <b>${escapeHtml(options.title)}</b>
        <small>${escapeHtml(shortShareUrl(options.url))}</small>
      </div>
      ${actionGroups}
    </div>
  `;
  (options.mount ?? document.body).append(overlay);
  const close = () => {
    overlay.remove();
    options.onClose?.(overlay);
  };
  const setNote = (text: string) => {
    const note = overlay.querySelector<HTMLElement>(".link-share-caption small");
    if (note) {
      note.textContent = text;
    }
  };
  const canvas = overlay.querySelector<HTMLCanvasElement>("canvas");
  if (canvas) {
    await QRCode.toCanvas(canvas, options.url, {
      margin: 1,
      scale: 8,
      color: {
        dark: "#000000",
        light: "#ffffff"
      }
    });
  }
  overlay.querySelector<HTMLElement>(".link-share-close")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  overlay.querySelectorAll<HTMLElement>("[data-share-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const action = [...allPrimaryActions, ...secondaryActions].find((item) => item.id === button.dataset.shareAction);
      if (!action) {
        return;
      }
      const result = action.run({ overlay, close, setNote });
      void Promise.resolve(result)
        .then((note) => {
          if (typeof note === "string" && note) {
            setNote(note);
          }
          if (action.closeOnClick) {
            close();
          }
        })
        .catch(() => undefined);
    });
  });
  return overlay;
}

export function closeLinkShareSheets(): void {
  document.querySelectorAll<HTMLElement>(".link-share-modal").forEach((overlay) => overlay.remove());
}

export async function copyText(value: string): Promise<void> {
  if (!value) {
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return;
    }
  } catch {
    // Fall back to a temporary textarea below.
  }
  try {
    const input = document.createElement("textarea");
    input.value = value;
    input.style.position = "fixed";
    input.style.opacity = "0";
    document.body.append(input);
    input.focus();
    input.select();
    document.execCommand("copy");
    input.remove();
  } catch {
    // Clipboard is a convenience; the QR remains usable even if copying is blocked.
  }
}

function renderActionGroup(actions: readonly LinkShareAction[], className: string): string {
  if (actions.length === 0) {
    return "";
  }
  return `
    <div class="link-share-actions${actions.length === 1 ? " is-single" : ""}${className}">
      ${actions.map((action) => `
        <button class="icon-button${action.tone === "secondary" ? " is-secondary" : ""}" type="button" data-share-action="${escapeAttr(action.id)}">
          ${icon(action.icon)}
          <span>${escapeHtml(action.label)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function shortShareUrl(value: string): string {
  try {
    const url = new URL(value);
    return `${url.host}${url.pathname}`;
  } catch {
    return value;
  }
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/"/gu, "&quot;");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;")
    .replace(/'/gu, "&#39;");
}
