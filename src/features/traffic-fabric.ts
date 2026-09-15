export interface TrafficInterface {
  readonly name: string;
  readonly index: number;
  readonly up: boolean;
  readonly metric: number;
  readonly vpn: boolean;
}

export interface TrafficClient {
  readonly id: string;
  readonly exitId: string;
  readonly label: string;
  readonly platform: string;
  readonly createdAt: string;
  readonly revokedAt: string;
}

export interface TrafficExit {
  readonly id: string;
  readonly label: string;
  readonly createdAt: string;
}

export interface TrafficRuntime {
  readonly configured?: boolean;
  readonly enabled?: boolean;
  readonly active?: boolean;
  readonly phase?: string;
  readonly version?: string;
  readonly lastError?: string;
}

const profileStorageKey = "soty:traffic-profile:v1";

export function adoptTrafficProfileFromUrl(): boolean {
  const url = new URL(window.location.href);
  const encoded = url.searchParams.get("trafficProfile") || "";
  const profile = decodeProfile(encoded);
  if (!isTrafficProfile(profile)) return false;
  localStorage.setItem(profileStorageKey, profile);
  url.searchParams.delete("trafficProfile");
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  return true;
}

export function readTrafficProfile(): string {
  const profile = localStorage.getItem(profileStorageKey) || "";
  return isTrafficProfile(profile) ? profile : "";
}

export function saveTrafficProfile(profile: string): void {
  if (isTrafficProfile(profile)) localStorage.setItem(profileStorageKey, profile);
}

export function trafficProfileInviteUrl(profile: string): string {
  if (!isTrafficProfile(profile)) return "";
  const url = new URL(window.location.origin);
  url.searchParams.set("trafficProfile", encodeProfile(profile));
  return url.toString();
}

export async function trafficFabricStatus(): Promise<TrafficRuntime> {
  const payload = await localAgentJson("/operator/traffic/fabric", { method: "GET" });
  return (payload.runtime || {}) as TrafficRuntime;
}

export async function trafficInterfaces(): Promise<readonly TrafficInterface[]> {
  const payload = await localAgentJson("/operator/traffic/fabric/interfaces", { method: "GET" });
  return Array.isArray(payload.interfaces) ? payload.interfaces as TrafficInterface[] : [];
}

export async function trafficServerStatus(): Promise<{ readonly exits: readonly TrafficExit[]; readonly clients: readonly TrafficClient[] }> {
  const payload = await trafficCoreAction({ action: "server-status" });
  return {
    exits: Array.isArray(payload.exits) ? payload.exits as TrafficExit[] : [],
    clients: Array.isArray(payload.clients) ? payload.clients as TrafficClient[] : []
  };
}

export async function provisionTraffic(value: { readonly requireVpn: boolean; readonly vpnInterface: string; readonly clientLabel: string }): Promise<Record<string, unknown>> {
  return await trafficCoreAction({ action: "provision", platform: "android", ...value });
}

export async function provisionTrafficClient(exitId: string, clientLabel: string): Promise<Record<string, unknown>> {
  return await trafficCoreAction({ action: "provision-client", exitId, clientLabel, platform: "android" });
}

export async function revokeTrafficClient(clientId: string): Promise<void> {
  await trafficCoreAction({ action: "revoke-client", clientId });
}

export async function stopTraffic(): Promise<void> {
  await trafficCoreAction({ action: "stop" });
}

async function trafficCoreAction(body: Record<string, unknown>): Promise<Record<string, unknown>> {
  return await localAgentJson("/operator/traffic/fabric/core", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
}

async function localAgentJson(path: string, init: RequestInit): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 150_000);
  try {
    const response = await fetch(`http://127.0.0.1:49424${path}`, {
      ...init,
      cache: "no-store",
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    const payload = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok || payload.ok === false) throw new Error(typeof payload.error === "string" ? payload.error : `traffic-http-${response.status}`);
    return payload;
  } finally {
    window.clearTimeout(timer);
  }
}

function encodeProfile(profile: string): string {
  const bytes = new TextEncoder().encode(profile);
  let binary = "";
  bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/gu, "");
}

function decodeProfile(value: string): string {
  try {
    const base64 = value.replace(/-/gu, "+").replace(/_/gu, "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
    const binary = atob(base64);
    return new TextDecoder().decode(Uint8Array.from(binary, (char) => char.charCodeAt(0)));
  } catch {
    return "";
  }
}

function isTrafficProfile(value: string): boolean {
  return /^vless:\/\/[0-9a-f-]{36}@[^\s]{10,1600}$/iu.test(value);
}
