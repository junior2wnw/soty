export const trafficShareKey = "soty:link-traffic-share:v1";
export const trafficAccessKey = "soty:link-traffic-access:v1";

export const linkControlCapability = "link.control";
export const linkTrafficExitCapability = "traffic.exit";
export const linkTrafficProxyCapability = "traffic.proxy";
export const linkTrafficSystemCapability = "traffic.system";

export type LinkTrafficMode = "proxy" | "system";

export interface LinkTrafficAccess {
  readonly hostDeviceId: string;
  readonly mode: LinkTrafficMode;
  readonly updatedAt: string;
}

let memoryTrafficShare = new Set<string>();
let memoryTrafficAccess = new Map<string, LinkTrafficAccess>();

function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key) ?? sessionStorage.getItem(key);
  } catch {
    try {
      return sessionStorage.getItem(key);
    } catch {
      return null;
    }
  }
}

function writeStored(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Keep a runtime fallback for locked-down browsers.
  }
  try {
    sessionStorage.setItem(key, value);
  } catch {
    // Best effort only.
  }
}

export function loadTrafficShare(): Set<string> {
  try {
    const parsed = JSON.parse(readStored(trafficShareKey) || "[]") as unknown;
    const items = Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === "string" && item.length > 0) : [];
    memoryTrafficShare = new Set(items);
    return new Set(memoryTrafficShare);
  } catch {
    return new Set(memoryTrafficShare);
  }
}

export function setTrafficShare(tunnelId: string, enabled: boolean): Set<string> {
  const items = loadTrafficShare();
  if (enabled) {
    items.add(tunnelId);
  } else {
    items.delete(tunnelId);
  }
  memoryTrafficShare = new Set(items);
  writeStored(trafficShareKey, JSON.stringify([...items]));
  return items;
}

export function loadTrafficAccess(): Map<string, LinkTrafficAccess> {
  try {
    const parsed = JSON.parse(readStored(trafficAccessKey) || "{}") as Record<string, unknown>;
    memoryTrafficAccess = new Map(Object.entries(parsed)
      .map(([tunnelId, value]): [string, LinkTrafficAccess] | null => {
        if (!value || typeof value !== "object") {
          return null;
        }
        const record = value as Record<string, unknown>;
        const hostDeviceId = typeof record.hostDeviceId === "string" ? record.hostDeviceId : "";
        if (!hostDeviceId) {
          return null;
        }
        return [tunnelId, {
          hostDeviceId,
          mode: record.mode === "system" ? "system" : "proxy",
          updatedAt: typeof record.updatedAt === "string" ? record.updatedAt : new Date().toISOString()
        }];
      })
      .filter((entry): entry is [string, LinkTrafficAccess] => entry !== null));
    return new Map(memoryTrafficAccess);
  } catch {
    return new Map(memoryTrafficAccess);
  }
}

export function setTrafficAccess(tunnelId: string, hostDeviceId: string, enabled: boolean, mode: LinkTrafficMode = "proxy"): Map<string, LinkTrafficAccess> {
  const items = loadTrafficAccess();
  if (enabled && hostDeviceId) {
    items.set(tunnelId, {
      hostDeviceId,
      mode,
      updatedAt: new Date().toISOString()
    });
  } else {
    items.delete(tunnelId);
  }
  memoryTrafficAccess = new Map(items);
  writeStored(trafficAccessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

export function trafficGrantCapabilities(enabled: boolean): string[] {
  return enabled
    ? [linkControlCapability, linkTrafficExitCapability, linkTrafficProxyCapability]
    : [linkControlCapability];
}

export function capabilitiesAllowTraffic(capabilities: readonly string[] | undefined): boolean {
  return Boolean(capabilities?.includes(linkTrafficExitCapability));
}

export function trafficModeFromCapabilities(capabilities: readonly string[] | undefined): LinkTrafficMode {
  return capabilities?.includes(linkTrafficSystemCapability) ? "system" : "proxy";
}

export function clearTrafficState(): void {
  memoryTrafficShare = new Set();
  memoryTrafficAccess = new Map();
  try {
    localStorage.removeItem(trafficShareKey);
    localStorage.removeItem(trafficAccessKey);
    sessionStorage.removeItem(trafficShareKey);
    sessionStorage.removeItem(trafficAccessKey);
  } catch {
    // Best effort only.
  }
}
