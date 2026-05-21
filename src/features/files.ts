import type { ReceivedFile } from "../sync";

export const maxFileBytes = 2_000_000_000;

export function filesFrom(list?: FileList | null): File[] {
  return list ? Array.from(list).filter((file) => file.size <= maxFileBytes) : [];
}

export function oversizedFilesFrom(list?: FileList | null): File[] {
  return list ? Array.from(list).filter((file) => file.size > maxFileBytes) : [];
}

export function formatFileSize(bytes: number): string {
  if (bytes >= 1_000_000_000) {
    return `${(bytes / 1_000_000_000).toFixed(1)} GB`;
  }
  if (bytes >= 1_000_000) {
    return `${Math.round(bytes / 1_000_000)} MB`;
  }
  if (bytes >= 1000) {
    return `${Math.round(bytes / 1000)} KB`;
  }
  return `${bytes} B`;
}

export function downloadReceivedFile(file: ReceivedFile): void {
  const hasInlineBytes = file.bytes.byteLength > 0;
  const body = hasInlineBytes
    ? file.bytes.buffer.slice(file.bytes.byteOffset, file.bytes.byteOffset + file.bytes.byteLength) as ArrayBuffer
    : null;
  const blob = body ? new Blob([body], { type: file.type || "application/octet-stream" }) : null;
  const url = blob ? URL.createObjectURL(blob) : file.url;
  const link = document.createElement("a");
  link.href = url;
  link.download = file.name || "file";
  link.rel = "noopener";
  link.style.display = "none";
  document.body.append(link);
  link.click();
  link.remove();
  if (blob) {
    window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
  }
}
