export type IconName = "install" | "qr" | "scan" | "close" | "check" | "person" | "clip" | "remote" | "download" | "upload" | "refresh" | "copy" | "bell" | "shield" | "send" | "stop" | "chess" | "collapse" | "expand" | "menu";

const paths: Record<IconName, string> = {
  install: "M17 1H7C5.9 1 5 1.9 5 3v18c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V3c0-1.1-.9-2-2-2Zm0 18H7V5h10v14Zm-5-1 4-4h-3V8h-2v6H8l4 4Z",
  qr: "M3 3h8v8H3V3Zm2 2v4h4V5H5Zm8-2h8v8h-8V3Zm2 2v4h4V5h-4ZM3 13h8v8H3v-8Zm2 2v4h4v-4H5Zm10-2h2v2h-2v-2Zm2 2h2v2h-2v-2Zm-4 2h2v2h-2v-2Zm2 2h2v2h-2v-2Zm4-2h2v4h-4v-2h2v-2Zm-6-4h2v2h-2v-2Zm6 0h2v2h-2v-2Z",
  scan: "M4 4h5v2H6v3H4V4Zm11 0h5v5h-2V6h-3V4ZM4 15h2v3h3v2H4v-5Zm14 0h2v5h-5v-2h3v-3ZM8 8h8v8H8V8Zm2 2v4h4v-4h-4Z",
  close: "M6.4 5 5 6.4 10.6 12 5 17.6 6.4 19 12 13.4 17.6 19 19 17.6 13.4 12 19 6.4 17.6 5 12 10.6 6.4 5Z",
  check: "M9 16.2 4.8 12l-1.4 1.4L9 19 21 7 19.6 5.6 9 16.2Z",
  person: "M12 12c2.2 0 4-1.8 4-4s-1.8-4-4-4-4 1.8-4 4 1.8 4 4 4Zm0 2c-2.7 0-8 1.4-8 4v2h16v-2c0-2.6-5.3-4-8-4Z",
  clip: "M16.5 6.5 8.4 14.6c-1.2 1.2-1.2 3.1 0 4.2 1.2 1.2 3.1 1.2 4.2 0l8.8-8.8c1.9-1.9 1.9-5 0-6.9s-5-1.9-6.9 0L5.3 12.4c-2.7 2.7-2.7 7.1 0 9.8s7.1 2.7 9.8 0l8.1-8.1-1.4-1.4-8.1 8.1c-1.9 1.9-5 1.9-6.9 0s-1.9-5 0-6.9l9.2-9.2c1.1-1.1 2.9-1.1 4 0s1.1 2.9 0 4l-8.8 8.8c-.4.4-1 .4-1.4 0s-.4-1 0-1.4l8.1-8.1-1.4-1.5Z",
  remote: "M3 4h18v12H3V4Zm2 2v8h14V6H5Zm4 12h6v2H9v-2Zm-4 2h14v2H5v-2Zm7-12 4 3-4 3V8Z",
  download: "M5 20h14v-2H5v2ZM13 4h-2v8H8l4 4 4-4h-3V4Z",
  upload: "M5 20h14v-2H5v2ZM11 16h2V8h3l-4-4-4 4h3v8Z",
  refresh: "M17.7 6.3C16.2 4.9 14.2 4 12 4c-3.7 0-6.8 2.6-7.6 6h2.1c.7-2.3 2.9-4 5.5-4 1.7 0 3.3.7 4.4 1.8L13 11h8V3l-3.3 3.3ZM6.3 17.7C7.8 19.1 9.8 20 12 20c3.7 0 6.8-2.6 7.6-6h-2.1c-.7 2.3-2.9 4-5.5 4-1.7 0-3.3-.7-4.4-1.8L11 13H3v8l3.3-3.3Z",
  copy: "M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1Zm3 4H8c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2Zm0 18H8V7h11v16Z",
  bell: "M12 22c1.1 0 2-.9 2-2h-4c0 1.1.9 2 2 2Zm6-6v-5c0-3.1-1.6-5.6-4.5-6.3V4c0-.8-.7-1.5-1.5-1.5S10.5 3.2 10.5 4v.7C7.6 5.4 6 7.9 6 11v5l-2 2v1h16v-1l-2-2Zm-2 1H8v-6c0-2.5 1.5-4.5 4-4.5s4 2 4 4.5v6Z",
  shield: "M12 2 4 5v6c0 5 3.4 9.7 8 11 4.6-1.3 8-6 8-11V5l-8-3Zm0 2.2 6 2.3V11c0 3.8-2.4 7.4-6 8.8-3.6-1.4-6-5-6-8.8V6.5l6-2.3Zm-1 10.6 5.2-5.2-1.4-1.4L11 12l-1.8-1.8-1.4 1.4 3.2 3.2Z",
  send: "M2 21 23 12 2 3v7l15 2-15 2v7Z",
  stop: "M6 6h12v12H6V6Z",
  chess: "M8 3h8v2h-2v3h2l2 4v8H6v-8l2-4h2V5H8V3Zm3 2v3h2V5h-2Zm-2 5-1 2h8l-1-2H9Zm-1 4v4h8v-4H8Z",
  collapse: "M5 11h14v2H5v-2Zm4-6h6v2H9V5Zm0 12h6v2H9v-2Z",
  expand: "M5 11h14v2H5v-2Zm2-8h10v2H7V3Zm0 16h10v2H7v-2Z",
  menu: "M4 6h16v2H4V6Zm0 5h16v2H4v-2Zm0 5h16v2H4v-2Z"
};

export function icon(name: IconName): string {
  return `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="${paths[name]}"/></svg>`;
}
