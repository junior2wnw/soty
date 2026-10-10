export interface OrbPoint { x: number; y: number }
export interface OrbViewport { width: number; height: number; left: number; top: number }
const size = 44, inset = 6;

export function clampOrb(point: OrbPoint, view: OrbViewport): OrbPoint {
  const limit = (value: number, low: number, high: number): number => Math.min(Math.max(low, high), Math.max(low, Number.isFinite(value) ? value : low));
  return { x: limit(point.x, view.left + inset, view.left + view.width - size - inset),
    y: limit(point.y, view.top + inset, view.top + view.height - size - 88) };
}
export function dockOrb(view: OrbViewport): OrbPoint {
  return clampOrb({ x: view.left + view.width - size - inset, y: view.top + inset }, view);
}
export function readOrbPosition(storage: Pick<Storage, 'getItem'>, account: string, view: OrbViewport): OrbPoint | null {
  if (!account) return null;
  try {
    const value: unknown = JSON.parse(storage.getItem(`soty.agent-orb.v1:${account}`) ?? 'null');
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    const saved = value as Record<string, unknown>;
    if (Object.keys(saved).length !== 2 || typeof saved.x !== 'number' || typeof saved.y !== 'number'
      || !Number.isFinite(saved.x) || !Number.isFinite(saved.y) || saved.x < 0 || saved.x > 1 || saved.y < 0 || saved.y > 1) return null;
    return clampOrb({ x: view.left + saved.x * view.width, y: view.top + saved.y * view.height }, view);
  } catch { return null; }
}
export function writeOrbPosition(storage: Pick<Storage, 'setItem' | 'removeItem'>, account: string, point: OrbPoint | null, view: OrbViewport): void {
  if (!account) return;
  try {
    const key = `soty.agent-orb.v1:${account}`;
    if (!point) { storage.removeItem(key); return; }
    const safe = clampOrb(point, view);
    storage.setItem(key, JSON.stringify({ x: (safe.x - view.left) / Math.max(1, view.width), y: (safe.y - view.top) / Math.max(1, view.height) }));
  } catch { /* A layout preference is optional; the control remains usable. */ }
}
