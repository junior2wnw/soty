import { appModeKey } from "./storage";

export function isStandalone(): boolean {
  return window.matchMedia("(display-mode: standalone)").matches
    || window.matchMedia("(display-mode: fullscreen)").matches
    || window.matchMedia("(display-mode: minimal-ui)").matches
    || window.matchMedia("(display-mode: window-controls-overlay)").matches
    || (navigator as Navigator & { standalone?: boolean }).standalone === true;
}

export function isAppRuntime(): boolean {
  const url = new URL(window.location.href);
  if (url.searchParams.get("pwa") === "1") {
    rememberAppRuntime();
    return true;
  }
  return isStandalone() || localStorage.getItem(appModeKey) === "1";
}

export function rememberAppRuntime(): void {
  localStorage.setItem(appModeKey, "1");
}
