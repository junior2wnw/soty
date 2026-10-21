import { assertApps, runtimePath } from './protocol.mjs';

// Save and launch admit the same path, including the encoded bootstrap limit.
// This helper is server-only: no change to the connector wire protocol/bundle.
export function createLaunchPath(value) {
  const entryPath = runtimePath(value);
  const bootPath = `/_soty/boot?${new URLSearchParams({ path: entryPath })}`;
  assertApps(bootPath.length <= 8192, 'invalid_app_path');
  return { entryPath, bootPath };
}

// Navigation data only: the admitted Source request remains exactly /embed.
// Kept self-contained so the boot page validates the same boundary in-browser.
export function checkedScopedEntryPath(value) {
  if (typeof value !== 'string' || !value.isWellFormed()) return null;
  const at = value.indexOf('#');
  if ((at < 0 ? value : value.slice(0, at)) !== '/embed') return null;
  if (at < 0) return value;
  const fragment = value.slice(at + 1);
  if (fragment.length > 2048 || /[\\#\u0000-\u0020\u007f]/u.test(fragment)) return null;
  let decoded;
  try { decoded = decodeURIComponent(fragment); } catch { return null; }
  if (/[\\\u0000-\u001f\u007f]/u.test(decoded)) return null;
  return value;
}

export function createScopedLaunchPath(value) {
  assertApps(checkedScopedEntryPath(value) !== null, 'app_scoped_admission_required', 503);
  return createLaunchPath(value);
}
