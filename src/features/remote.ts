const remoteKey = "soty:remote-enabled:v1";
const accessKey = "soty:remote-access:v1";

export function loadRemoteEnabled(): Set<string> {
  try {
    const parsed = JSON.parse(localStorage.getItem(remoteKey) || "[]") as string[];
    return new Set(parsed.filter((item) => typeof item === "string" && item.length > 0));
  } catch {
    return new Set();
  }
}

export function setRemoteEnabled(tunnelId: string, enabled: boolean): Set<string> {
  const items = loadRemoteEnabled();
  if (enabled) {
    items.add(tunnelId);
  } else {
    items.delete(tunnelId);
  }
  localStorage.setItem(remoteKey, JSON.stringify([...items]));
  return items;
}

export function loadRemoteAccess(): Map<string, string> {
  try {
    const parsed = JSON.parse(localStorage.getItem(accessKey) || "{}") as Record<string, unknown>;
    return new Map(Object.entries(parsed)
      .filter((entry): entry is [string, string] => typeof entry[1] === "string" && entry[1].length > 0));
  } catch {
    return new Map();
  }
}

export function setRemoteAccess(tunnelId: string, hostDeviceId: string, enabled: boolean): Map<string, string> {
  const items = loadRemoteAccess();
  if (enabled) {
    items.set(tunnelId, hostDeviceId);
  } else {
    items.delete(tunnelId);
  }
  localStorage.setItem(accessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}
