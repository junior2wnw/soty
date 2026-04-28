import { clock } from "../core/time";
import { icon } from "../icons";
import { ReceivedFile } from "../sync";

export const maxFileBytes = 20_000_000;

export function filesFrom(list?: FileList | null): File[] {
  return list ? Array.from(list).filter((file) => file.size <= maxFileBytes) : [];
}

export function renderFileRail(root: HTMLElement, files: readonly ReceivedFile[], color: string): void {
  root.innerHTML = files.map((file) => `
    <a class="file-chip" style="--color:${color}" href="${file.url}" download="${escapeHtml(file.name)}">
      <span>${icon("download")}</span>
      <b>${escapeHtml(file.name)}</b>
      <small>${escapeHtml(clock(new Date(file.createdAt)))}</small>
    </a>
  `).join("");
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
