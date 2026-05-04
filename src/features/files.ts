import { clock } from "../core/time";
import { icon } from "../icons";
import { ReceivedFile } from "../sync";

export const maxFileBytes = 512_000_000;

export function filesFrom(list?: FileList | null): File[] {
  return list ? Array.from(list).filter((file) => file.size <= maxFileBytes) : [];
}

export function renderFileRail(
  root: HTMLElement,
  files: readonly ReceivedFile[],
  color: string,
  onDelete: (fileId: string) => void
): void {
  root.innerHTML = files.map((file) => `
    <div class="file-chip" style="--color:${color}" data-id="${escapeHtml(file.id)}">
      <a href="${file.url}" download="${escapeHtml(file.name)}" draggable="false" data-id="${escapeHtml(file.id)}" data-tooltip="Скачать файл">
        <span>${icon("download")}</span>
        <b>${escapeHtml(file.name)}</b>
        <small>${escapeHtml(clock(new Date(file.createdAt)))}</small>
      </a>
      <button type="button" aria-label="close" data-tooltip="Убрать файл из списка" data-id="${escapeHtml(file.id)}">${icon("close")}</button>
    </div>
  `).join("");
  const byId = new Map(files.map((file) => [file.id, file]));
  root.querySelectorAll<HTMLAnchorElement>("a[data-id]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const file = byId.get(link.dataset.id || "");
      if (file) {
        downloadReceivedFile(file);
      }
    });
  });
  root.querySelectorAll<HTMLButtonElement>("button[data-id]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const id = button.dataset.id;
      if (id) {
        onDelete(id);
      }
    });
  });
  installRailScroll(root);
}

function downloadReceivedFile(file: ReceivedFile): void {
  const body = file.bytes.buffer.slice(file.bytes.byteOffset, file.bytes.byteOffset + file.bytes.byteLength) as ArrayBuffer;
  const blob = new Blob([body], { type: file.type || "application/octet-stream" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = file.name || "file";
  link.rel = "noopener";
  link.style.display = "none";
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

function installRailScroll(root: HTMLElement): void {
  let dragging = false;
  let dragged = false;
  let startX = 0;
  let startScroll = 0;

  root.onwheel = (event) => {
    if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) {
      return;
    }
    root.scrollLeft += event.deltaY;
    event.preventDefault();
  };

  root.onpointerdown = (event) => {
    if (event.button !== 0) {
      return;
    }
    if (event.target instanceof Element && event.target.closest("a,button")) {
      return;
    }
    dragging = true;
    dragged = false;
    startX = event.clientX;
    startScroll = root.scrollLeft;
    root.setPointerCapture(event.pointerId);
  };

  root.onpointermove = (event) => {
    if (!dragging) {
      return;
    }
    const delta = event.clientX - startX;
    if (Math.abs(delta) > 4) {
      dragged = true;
    }
    root.scrollLeft = startScroll - delta;
  };

  root.onpointerup = (event) => {
    dragging = false;
    if (dragged) {
      event.preventDefault();
    }
  };

  root.onpointercancel = () => {
    dragging = false;
  };
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
