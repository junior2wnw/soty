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
      <a href="${file.url}" download="${escapeHtml(file.name)}" draggable="false">
        <span>${icon("download")}</span>
        <b>${escapeHtml(file.name)}</b>
        <small>${escapeHtml(clock(new Date(file.createdAt)))}</small>
      </a>
      <button type="button" aria-label="close" data-id="${escapeHtml(file.id)}">${icon("close")}</button>
    </div>
  `).join("");
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
    if (event.target instanceof Element && event.target.closest("button")) {
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
