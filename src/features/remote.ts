export const remoteKey = "soty:remote-enabled:v1";
export const accessKey = "soty:remote-access:v1";

let memoryRemoteEnabled = new Set<string>();
let memoryRemoteAccess = new Map<string, string>();

function clearLegacyLocalState(): void {
  try {
    localStorage.removeItem(remoteKey);
    localStorage.removeItem(accessKey);
  } catch {
    // Ignore blocked storage; remote grants are runtime-only.
  }
}

function readSession(key: string): string | null {
  try {
    return sessionStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeSession(key: string, value: string): boolean {
  try {
    sessionStorage.setItem(key, value);
    return true;
  } catch {
    return false;
  }
}

export function loadRemoteEnabled(): Set<string> {
  clearLegacyLocalState();
  try {
    const parsed = JSON.parse(readSession(remoteKey) || "[]") as string[];
    memoryRemoteEnabled = new Set(parsed.filter((item) => typeof item === "string" && item.length > 0));
    return new Set(memoryRemoteEnabled);
  } catch {
    return new Set(memoryRemoteEnabled);
  }
}

export function setRemoteEnabled(tunnelId: string, enabled: boolean): Set<string> {
  const items = loadRemoteEnabled();
  if (enabled) {
    items.add(tunnelId);
  } else {
    items.delete(tunnelId);
  }
  memoryRemoteEnabled = new Set(items);
  writeSession(remoteKey, JSON.stringify([...items]));
  return items;
}

export function loadRemoteAccess(): Map<string, string> {
  clearLegacyLocalState();
  try {
    const parsed = JSON.parse(readSession(accessKey) || "{}") as Record<string, unknown>;
    memoryRemoteAccess = new Map(Object.entries(parsed)
      .filter((entry): entry is [string, string] => typeof entry[1] === "string" && entry[1].length > 0));
    return new Map(memoryRemoteAccess);
  } catch {
    return new Map(memoryRemoteAccess);
  }
}

export function setRemoteAccess(tunnelId: string, hostDeviceId: string, enabled: boolean): Map<string, string> {
  const items = loadRemoteAccess();
  if (enabled) {
    items.set(tunnelId, hostDeviceId);
  } else {
    items.delete(tunnelId);
  }
  memoryRemoteAccess = new Map(items);
  writeSession(accessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

export function clearRemoteSessionState(): void {
  memoryRemoteEnabled = new Set();
  memoryRemoteAccess = new Map();
  try {
    sessionStorage.removeItem(remoteKey);
    sessionStorage.removeItem(accessKey);
    localStorage.removeItem(remoteKey);
    localStorage.removeItem(accessKey);
  } catch {
    // Best effort only.
  }
}
