import { assertApps, runtimePath } from './protocol.mjs';

// Save and launch admit the same path, including the encoded bootstrap limit.
// This helper is server-only: no change to the connector wire protocol/bundle.
export function createLaunchPath(value) {
  const entryPath = runtimePath(value);
  const bootPath = `/_soty/boot?${new URLSearchParams({ path: entryPath })}`;
  assertApps(bootPath.length <= 8192, 'invalid_app_path');
  return { entryPath, bootPath };
}
