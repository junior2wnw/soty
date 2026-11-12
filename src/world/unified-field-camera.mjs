export const UNIFIED_SCALE_MIN = .00001;
export const UNIFIED_SCALE_MAX = 2;
const finite = value => typeof value === 'number' && Number.isFinite(value);
export function normalizeFieldCamera(camera = {}) {
  return { x: finite(camera.x) ? Math.max(-3_000_000, Math.min(3_000_000, camera.x)) : 0,
    y: finite(camera.y) ? Math.max(-3_000_000, Math.min(3_000_000, camera.y)) : 0,
    scale: finite(camera.scale) ? Math.max(UNIFIED_SCALE_MIN, Math.min(UNIFIED_SCALE_MAX, camera.scale)) : 1 };
}
export function fieldWorldToScreen(point, camera, viewport) {
  return { x: (point.x - camera.x) * camera.scale + viewport.width / 2, y: (point.y - camera.y) * camera.scale + viewport.height / 2 };
}
export function fieldScreenToWorld(point, camera, viewport) {
  return { x: (point.x - viewport.width / 2) / camera.scale + camera.x, y: (point.y - viewport.height / 2) / camera.scale + camera.y };
}
export function zoomFieldCamera(camera, nextScale, anchor, viewport) {
  const scale = normalizeFieldCamera({ scale: nextScale }).scale, world = fieldScreenToWorld(anchor, camera, viewport);
  return normalizeFieldCamera({ scale, x: world.x - (anchor.x - viewport.width / 2) / scale,
    y: world.y - (anchor.y - viewport.height / 2) / scale });
}
export function fitFieldCamera(bounds, viewport, { padding = 28, maxScale = 1, minScale = UNIFIED_SCALE_MIN } = {}) {
  if (!bounds || viewport.width <= 0 || viewport.height <= 0) return normalizeFieldCamera();
  const width = Math.max(1, bounds.right - bounds.left), height = Math.max(1, bounds.bottom - bounds.top);
  const inset = side => finite(viewport.insets?.[side]) ? Math.max(0, viewport.insets[side]) : 0;
  const left = inset('left'), right = inset('right'), top = inset('top'), bottom = inset('bottom');
  const scale = normalizeFieldCamera({ scale: Math.max(minScale, Math.min(maxScale,
    Math.max(1, viewport.width - left - right - padding * 2) / width,
    Math.max(1, viewport.height - top - bottom - padding * 2) / height)) }).scale;
  // Projection still uses the full viewport; move the fitted centre into its
  // unobscured rectangle without changing manual pan or anchored zoom math.
  return normalizeFieldCamera({ x: (bounds.left + bounds.right) / 2 + (right - left) / (2 * scale),
    y: (bounds.top + bounds.bottom) / 2 + (bottom - top) / (2 * scale), scale });
}
/** Hysteresis prevents semantic labels from flickering on a wheel threshold. */
export function fieldLevelOfDetail(scale, previous = 'detail') {
  if (previous === 'overview' && scale < .48 || scale < .4) return 'overview';
  if (previous === 'detail' && scale >= .78 || scale >= .9) return 'detail';
  return 'context';
}
