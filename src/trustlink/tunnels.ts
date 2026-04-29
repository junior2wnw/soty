import { createTrustLinkRoom } from "trustlink-kernel";
import { createWebRoomAuth } from "trustlink-kernel/platform/web";
import {
  loadJson,
  loadStored,
  removeStored,
  saveJson,
  saveStored,
  selectedKey,
  tunnelsKey
} from "./storage";
import { TunnelRecord } from "./types";

export function loadTunnels(): TunnelRecord[] {
  const parsed = loadJson<TunnelRecord[]>(tunnelsKey) ?? [];
  return parsed.filter((item) => item.id && item.key);
}

export function saveTunnels(tunnels: readonly TunnelRecord[]): void {
  saveJson(tunnelsKey, tunnels);
}

export function loadSelectedTunnelId(): string | null {
  return loadStored(selectedKey);
}

export function saveSelectedTunnelId(id: string): void {
  saveStored(selectedKey, id);
}

export function createTunnel(label = "", counterparty = false): TunnelRecord {
  const now = new Date().toISOString();
  const room = createTrustLinkRoom({ label, now });
  return {
    id: room.id,
    key: room.secret,
    label,
    score: 0,
    lastActionAt: now,
    counterparty,
    createdAt: now,
    updatedAt: now,
    unread: false
  };
}

export function upsertTunnel(tunnel: TunnelRecord): TunnelRecord[] {
  const tunnels = loadTunnels();
  const now = new Date().toISOString();
  const next = [{ ...tunnel, updatedAt: now, lastActionAt: now }, ...tunnels.filter((item) => item.id !== tunnel.id)];
  saveTunnels(next);
  saveSelectedTunnelId(tunnel.id);
  return next;
}

export function removeTunnel(id: string): TunnelRecord[] {
  const next = loadTunnels().filter((item) => item.id !== id);
  saveTunnels(next);
  if (loadSelectedTunnelId() === id) {
    removeStored(selectedKey);
  }
  return next;
}

export function markTunnel(id: string, unread: boolean): TunnelRecord[] {
  const now = new Date().toISOString();
  const next = loadTunnels().map((item) => item.id === id ? { ...item, unread, updatedAt: now, lastActionAt: now } : item);
  saveTunnels(next);
  return next;
}

export function touchTunnel(id: string): TunnelRecord[] {
  const now = new Date().toISOString();
  const next = loadTunnels().map((item) => item.id === id
    ? { ...item, score: (item.score ?? 0) + 1, updatedAt: now, lastActionAt: now }
    : item);
  saveTunnels(next);
  return next;
}

export function roomAuth(tunnel: TunnelRecord): Promise<string> {
  return createWebRoomAuth(
    { id: tunnel.id, secret: tunnel.key },
    { namespace: "soty.room-auth.v1" }
  );
}
