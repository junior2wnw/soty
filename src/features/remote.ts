const remoteKey = "soty:remote-enabled:v1";

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
