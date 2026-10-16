import { formatAppLaunchRoute, parseAppLaunchRoute } from './app-launch.mjs';

export const widgetLimit = 12;
const key = account => `soty.workspace-widgets.v1:${encodeURIComponent(account)}`;
const finite = (value, fallback, low, high) => Number.isFinite(value) ? Math.max(low, Math.min(high, value)) : fallback;
const empty = () => ({ hidden: false, widgets: [] });

/** Only stable shell entries and layout. Boot URLs, Source handles and grants never belong here. */
export function normalizeWidget(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || typeof value.id !== 'string' || !/^widget-[a-f0-9-]{36}$/u.test(value.id)
    || typeof value.route !== 'string' || value.route.length > 16384
    || typeof value.name !== 'string' || !value.name.trim() || value.name.length > 100) return null;
  try {
    const intent = parseAppLaunchRoute(value.route);
    if (!intent || intent.presentation) return null;
    return { id: value.id, route: formatAppLaunchRoute(intent.target, intent.communityId), name: value.name,
      visible: value.visible !== false, x: finite(value.x, 0.65, 0, 1), y: finite(value.y, 0.12, 0, 1),
      width: finite(value.width, 320, 220, 640), height: finite(value.height, 260, 160, 640) };
  } catch { return null; }
}

export function readWidgetWorkspace(storage, account) {
  if (!storage || !account) return empty();
  try {
    const raw = storage.getItem(key(account));
    if (!raw || raw.length > 220000) return empty();
    const value = JSON.parse(raw);
    if (!value || typeof value !== 'object' || value.schema !== 'soty.workspace-widgets.v1') return empty();
    const widgets = [], ids = new Set();
    for (const candidate of (Array.isArray(value.widgets) ? value.widgets.slice(0, widgetLimit) : [])) {
      const widget = normalizeWidget(candidate);
      if (widget && !ids.has(widget.id)) { ids.add(widget.id); widgets.push(widget); }
    }
    return { hidden: value.hidden === true, widgets };
  } catch { return empty(); }
}

/** Read fresh before each item edit, so another tab's unrelated window is preserved. */
export function editWidgetWorkspace(storage, account, change) {
  const state = readWidgetWorkspace(storage, account);
  change(state);
  const safe = { schema: 'soty.workspace-widgets.v1', hidden: state.hidden === true,
    widgets: state.widgets.slice(0, widgetLimit).map(normalizeWidget).filter(Boolean) };
  let saved = false;
  try {
    if (storage && account) { const text = JSON.stringify(safe); storage.setItem(key(account), text); saved = storage.getItem(key(account)) === text; }
  } catch { /* The caller keeps the current layout usable and reports the unsaved preference. */ }
  return { state: { hidden: safe.hidden, widgets: safe.widgets }, saved };
}

export const widgetStorageKey = key;

function widgetArea(viewport) {
  const width = Math.max(1, viewport.width), height = Math.max(1, viewport.height);
  const side = Math.min(6, (width - 1) / 2);
  // If chrome alone fills an unusually small visual viewport, retain a usable
  // widget header rather than projecting it to a clipped one-pixel window.
  const header = Math.min(52, height);
  const bottom = Math.min(finite(viewport.bottomInset, 36, 36, Infinity), height - header);
  const top = Math.min(finite(viewport.topInset, 56, 56, Infinity), height - bottom - header);
  return { left: viewport.left + side, top: viewport.top + top, width: width - 2 * side, height: height - top - bottom };
}

/** Route/viewport chrome clamps presentation only, never the stored preference. */
export function widgetRect(widget, viewport) {
  const area = widgetArea(viewport);
  const width = Math.max(1, Math.min(widget.width, area.width));
  const height = Math.max(1, Math.min(widget.height, area.height));
  const x = area.left + finite(widget.x, 0.65, 0, 1) * Math.max(0, area.width - width);
  const y = area.top + finite(widget.y, 0.12, 0, 1) * Math.max(0, area.height - height);
  return { x, y, width, height };
}

export function widgetPosition(rect, viewport, previous = { x: 0, y: 0 }) {
  const area = widgetArea(viewport), spanX = area.width - rect.width, spanY = area.height - rect.height;
  return { x: spanX > 0 ? finite((rect.x - area.left) / spanX, 0, 0, 1) : previous.x,
    y: spanY > 0 ? finite((rect.y - area.top) / spanY, 0, 0, 1) : previous.y };
}
