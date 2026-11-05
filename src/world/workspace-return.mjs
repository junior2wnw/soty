import {parseCanvasPageRoute} from './canvas-page-client.mjs';
import {parseAppLaunchRoute} from './app-launch.mjs';
import {normalizeFieldCamera} from './unified-field-camera.mjs';
const ROUTE = /^#(?:mine|world|messages|notes|library|assistant|access|community)(?:[/?]|$)/u;

function canvasView(value) {
  if (!value || typeof value !== 'object') return undefined;
  const result = {};
  if (value.mine && ['x', 'y', 'scale'].every(key => Number.isFinite(value.mine[key])) && value.mine.scale > 0)
    result.mine = normalizeFieldCamera(value.mine);
  for (const key of ['mineContext', 'mineSelection']) if (typeof value[key] === 'string' && value[key].length <= 180
    && !/[\u0000-\u001f\u007f]/u.test(value[key])) result[key] = value[key];
  if (['manual', 'overview', 'context'].includes(value.mineFit)) result.mineFit = value.mineFit;
  return Object.keys(result).length ? result : undefined;
}

const actorId = value => typeof value === 'string' && value.length > 0 && value.length <= 160 && !/[\u0000-\u001f\u007f]/u.test(value);
const launchKey = value => { try { const parsed = parseAppLaunchRoute(value); return parsed ? '#' + parsed.route : null; } catch { return null; } };

/** Display state only. A guest return can name only the exact public-page route. */
export function readCanvasPageHandoff(value) {
  if (!value || !parseCanvasPageRoute(value.route) || !launchKey(value.launchRoute)
    || !(value.accountId === null && value.deviceId === null || actorId(value.accountId) && actorId(value.deviceId))) return null;
  const view = canvasView(value.canvasView);
  return { route: value.route, launchRoute: launchKey(value.launchRoute), accountId: value.accountId, deviceId: value.deviceId,
    ...(view ? { canvasView: view } : {}) };
}

export function bindCanvasPageReturn(value, identity) {
  const handoff = readCanvasPageHandoff(value);
  if (!handoff || !actorId(identity?.accountId) || !actorId(identity?.deviceId) || handoff.launchRoute !== launchKey(identity.launchRoute)
    || handoff.accountId !== null && (handoff.accountId !== identity.accountId || handoff.deviceId !== identity.deviceId)) return null;
  return readWorkspaceReturn({ accountId: identity.accountId, route: handoff.route, scroll: 0,
    ...(handoff.canvasView ? { canvasView: handoff.canvasView } : {}) }, identity.accountId);
}

/** A local return location is never permission to read its resource. */
export function readWorkspaceReturn(value, accountId) {
  if (!value || value.accountId !== accountId || typeof value.route !== 'string' || value.route.length > 2048
    || !(ROUTE.test(value.route)||parseCanvasPageRoute(value.route)) || /[\u0000-\u001f\u007f]/u.test(value.route)) return null;
  const view = parseCanvasPageRoute(value.route) ? canvasView(value.canvasView) : undefined;
  return { accountId, route: value.route,
    scroll: Number.isFinite(value.scroll) ? Math.max(0, Math.min(1_000_000, value.scroll)) : 0,
    ...(value.presentation === 'saved' || value.presentation === 'list' ? { presentation: value.presentation } : {}),
    ...(typeof value.listQuery === 'string' ? { listQuery: value.listQuery.slice(0, 100) } : {}),
    ...(Number.isSafeInteger(value.libraryPages) && value.libraryPages >= 1 && value.libraryPages <= 10 ? { libraryPages: value.libraryPages } : {}),
    ...(Number.isFinite(value.libraryScroll) ? { libraryScroll: Math.max(0, Math.min(1_000_000, value.libraryScroll)) } : {}),
    ...(typeof value.libraryFocus === 'string' && /^app-[a-f0-9]{32}$/u.test(value.libraryFocus) ? { libraryFocus: value.libraryFocus } : {}),
    ...(typeof value.focusEntity === 'string' && value.focusEntity.length <= 180 ? { focusEntity: value.focusEntity } : {}),
    ...(view ? { canvasView: view } : {}) };
}
