import type { ConnectorAccessCredential } from "./connector";

export const remoteKey = "soty:remote-enabled:v1";
export const accessKey = "soty:remote-access:v1";
export const connectorAccessKey = "soty:remote-connector-access:v1";
export const issuedConnectorAccessKey = "soty:remote-issued-connector-access:v1";

let memoryRemoteEnabled = new Set<string>();
let memoryRemoteAccess = new Map<string, string>();
let memoryConnectorAccess = new Map<string, ConnectorAccessCredential>();
let memoryIssuedConnectorAccess = new Map<string, ConnectorAccessCredential>();

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

export function loadConnectorAccess(): Map<string, ConnectorAccessCredential> {
  memoryConnectorAccess = readConnectorAccess(connectorAccessKey, memoryConnectorAccess);
  return new Map(memoryConnectorAccess);
}

export function setConnectorAccess(tunnelId: string, access?: ConnectorAccessCredential): Map<string, ConnectorAccessCredential> {
  const items = loadConnectorAccess();
  if (access) items.set(tunnelId, access);
  else items.delete(tunnelId);
  memoryConnectorAccess = new Map(items);
  writeStored(connectorAccessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

export function loadIssuedConnectorAccess(): Map<string, ConnectorAccessCredential> {
  memoryIssuedConnectorAccess = readConnectorAccess(issuedConnectorAccessKey, memoryIssuedConnectorAccess);
  return new Map(memoryIssuedConnectorAccess);
}

export function setIssuedConnectorAccess(tunnelId: string, access?: ConnectorAccessCredential): Map<string, ConnectorAccessCredential> {
  const items = loadIssuedConnectorAccess();
  if (access) items.set(tunnelId, access);
  else items.delete(tunnelId);
  memoryIssuedConnectorAccess = new Map(items);
  writeStored(issuedConnectorAccessKey, JSON.stringify(Object.fromEntries(items)));
  return items;
}

function readConnectorAccess(
  key: string,
  fallback: Map<string, ConnectorAccessCredential>
): Map<string, ConnectorAccessCredential> {
  try {
    const parsed = JSON.parse(readStored(key) || "{}") as Record<string, unknown>;
    const entries = Object.entries(parsed)
      .map(([tunnelId, value]) => [tunnelId, sanitizeConnectorAccess(value)] as const)
      .filter((entry): entry is readonly [string, ConnectorAccessCredential] => Boolean(entry[0] && entry[1]));
    return new Map(entries);
  } catch {
    return new Map(fallback);
  }
}

function sanitizeConnectorAccess(value: unknown): ConnectorAccessCredential | null {
  const item = value && typeof value === "object" ? value as Partial<ConnectorAccessCredential> : {};
  const id = typeof item.id === "string" ? item.id : "";
  const token = typeof item.token === "string" ? item.token : "";
  const deviceId = typeof item.deviceId === "string" ? item.deviceId : "";
  const controllerDeviceId = typeof item.controllerDeviceId === "string" ? item.controllerDeviceId : "";
  const expiresAt = typeof item.expiresAt === "string" ? item.expiresAt : "";
  const capabilities = Array.isArray(item.capabilities)
    ? item.capabilities.filter((part): part is string => typeof part === "string").slice(0, 16)
    : [];
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,179}$/u.test(id)
    || !/^[A-Za-z0-9_-]{40,160}$/u.test(token)
    || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,179}$/u.test(deviceId)
    || !(controllerDeviceId === "*" || /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,179}$/u.test(controllerDeviceId))
    || !Number.isFinite(Date.parse(expiresAt))) {
    return null;
  }
  return { id, token, deviceId, controllerDeviceId, capabilities, expiresAt };
}

export function clearRemoteSessionState(): void {
  memoryRemoteEnabled = new Set();
  memoryRemoteAccess = new Map();
  memoryConnectorAccess = new Map();
  memoryIssuedConnectorAccess = new Map();
  try {
    sessionStorage.removeItem(remoteKey);
    sessionStorage.removeItem(accessKey);
    sessionStorage.removeItem(connectorAccessKey);
    sessionStorage.removeItem(issuedConnectorAccessKey);
    localStorage.removeItem(remoteKey);
    localStorage.removeItem(accessKey);
    localStorage.removeItem(connectorAccessKey);
    localStorage.removeItem(issuedConnectorAccessKey);
  } catch {
    // Best effort only.
  }
}
