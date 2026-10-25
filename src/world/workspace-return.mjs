const ROUTE = /^#(?:mine|world|messages|notes|library|assistant|access|community)(?:[/?]|$)/u;

/** A local return location is never permission to read its resource. */
export function readWorkspaceReturn(value, accountId) {
  if (!value || value.accountId !== accountId || typeof value.route !== 'string' || value.route.length > 2048
    || !ROUTE.test(value.route) || /[\u0000-\u001f\u007f]/u.test(value.route)) return null;
  return { accountId, route: value.route,
    scroll: Number.isFinite(value.scroll) ? Math.max(0, Math.min(1_000_000, value.scroll)) : 0,
    ...(value.presentation === 'saved' || value.presentation === 'list' ? { presentation: value.presentation } : {}),
    ...(typeof value.listQuery === 'string' ? { listQuery: value.listQuery.slice(0, 100) } : {}),
    ...(Number.isSafeInteger(value.libraryPages) && value.libraryPages >= 1 && value.libraryPages <= 10 ? { libraryPages: value.libraryPages } : {}),
    ...(Number.isFinite(value.libraryScroll) ? { libraryScroll: Math.max(0, Math.min(1_000_000, value.libraryScroll)) } : {}),
    ...(typeof value.libraryFocus === 'string' && /^app-[a-f0-9]{32}$/u.test(value.libraryFocus) ? { libraryFocus: value.libraryFocus } : {}),
    ...(typeof value.focusEntity === 'string' && value.focusEntity.length <= 180 ? { focusEntity: value.focusEntity } : {}) };
}
