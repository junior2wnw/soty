import { clock } from "../core/time";
import { icon } from "../icons";
import { ReceivedFile } from "../sync";

export function filesFrom(list?: FileList | null): File[] {
  return list ? Array.from(list) : [];
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
