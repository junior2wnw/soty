export const remoteKey = "soty:remote-enabled:v1";
export const accessKey = "soty:remote-access:v1";
export const grantTargetsKey = "soty:remote-grant-targets:v1";

let memoryRemoteEnabled = new Set<string>();
let memoryRemoteAccess = new Map<string, string>();
let memoryRemoteGrantTargets = new Map<string, string>();

function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key) ?? sessionStorage.getItem(key);
  } catch {
    return readSession(key);
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

function writeStored(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Keep a runtime fallback for locked-down browsers.
  }
  writeSession(key, value);
}

export function loadRemoteEnabled(): Set<string> {
  try {
    const parsed = JSON.parse(readStored(remoteKey) || "[]") as string[];
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
  writeStored(remoteKey, JSON.stringify([...items]));
  return items;
}

export function loadRemoteAccess(): Map<string, string> {
  try {
    const parsed = JSON.parse(readStored(accessKey) || "{}") as Record<string, unknown>;
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
  writeStored(accessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

export function loadRemoteGrantTargets(): Map<string, string> {
  try {
    const parsed = JSON.parse(readStored(grantTargetsKey) || "{}") as Record<string, unknown>;
    memoryRemoteGrantTargets = new Map(Object.entries(parsed)
      .filter((entry): entry is [string, string] => typeof entry[1] === "string" && entry[1].length > 0));
    return new Map(memoryRemoteGrantTargets);
  } catch {
    return new Map(memoryRemoteGrantTargets);
  }
}

export function setRemoteGrantTarget(tunnelId: string, targetDeviceId: string, enabled: boolean): Map<string, string> {
  const items = loadRemoteGrantTargets();
  if (enabled && targetDeviceId) {
    items.set(tunnelId, targetDeviceId);
  } else {
    items.delete(tunnelId);
  }
  memoryRemoteGrantTargets = new Map(items);
  writeStored(grantTargetsKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

export function clearRemoteSessionState(): void {
  memoryRemoteEnabled = new Set();
  memoryRemoteAccess = new Map();
  memoryRemoteGrantTargets = new Map();
  try {
    sessionStorage.removeItem(remoteKey);
    sessionStorage.removeItem(accessKey);
    sessionStorage.removeItem(grantTargetsKey);
    localStorage.removeItem(remoteKey);
    localStorage.removeItem(accessKey);
    localStorage.removeItem(grantTargetsKey);
  } catch {
    // Best effort only.
  }
}
