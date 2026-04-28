export type IconName = "install" | "qr" | "close" | "check" | "person" | "clip" | "remote" | "download";

const paths: Record<IconName, string> = {
  install: "M17 1H7C5.9 1 5 1.9 5 3v18c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V3c0-1.1-.9-2-2-2Zm0 18H7V5h10v14Zm-5-1 4-4h-3V8h-2v6H8l4 4Z",
  qr: "M3 3h8v8H3V3Zm2 2v4h4V5H5Zm8-2h8v8h-8V3Zm2 2v4h4V5h-4ZM3 13h8v8H3v-8Zm2 2v4h4v-4H5Zm10-2h2v2h-2v-2Zm2 2h2v2h-2v-2Zm-4 2h2v2h-2v-2Zm2 2h2v2h-2v-2Zm4-2h2v4h-4v-2h2v-2Zm-6-4h2v2h-2v-2Zm6 0h2v2h-2v-2Z",
  close: "M6.4 5 5 6.4 10.6 12 5 17.6 6.4 19 12 13.4 17.6 19 19 17.6 13.4 12 19 6.4 17.6 5 12 10.6 6.4 5Z",
  check: "M9 16.2 4.8 12l-1.4 1.4L9 19 21 7 19.6 5.6 9 16.2Z",
  person: "M12 12c2.2 0 4-1.8 4-4s-1.8-4-4-4-4 1.8-4 4 1.8 4 4 4Zm0 2c-2.7 0-8 1.4-8 4v2h16v-2c0-2.6-5.3-4-8-4Z",
  clip: "M16.5 6.5 8.4 14.6c-1.2 1.2-1.2 3.1 0 4.2 1.2 1.2 3.1 1.2 4.2 0l8.8-8.8c1.9-1.9 1.9-5 0-6.9s-5-1.9-6.9 0L5.3 12.4c-2.7 2.7-2.7 7.1 0 9.8s7.1 2.7 9.8 0l8.1-8.1-1.4-1.4-8.1 8.1c-1.9 1.9-5 1.9-6.9 0s-1.9-5 0-6.9l9.2-9.2c1.1-1.1 2.9-1.1 4 0s1.1 2.9 0 4l-8.8 8.8c-.4.4-1 .4-1.4 0s-.4-1 0-1.4l8.1-8.1-1.4-1.5Z",
  remote: "M3 4h18v12H3V4Zm2 2v8h14V6H5Zm4 12h6v2H9v-2Zm-4 2h14v2H5v-2Zm7-12 4 3-4 3V8Z",
  download: "M5 20h14v-2H5v2ZM13 4h-2v8H8l4 4 4-4h-3V4Z"
};

export function icon(name: IconName): string {
  return `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="${paths[name]}"/></svg>`;
}
