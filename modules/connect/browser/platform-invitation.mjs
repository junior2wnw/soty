/** Capture before routing; credentials never enter profile DTOs or HTTP URLs. */
export function capturePlatformInvitation(scope, view = globalThis) {
  const key = `connect.platform-invitation:${scope}`;
  let token = null;
  try { token = view.sessionStorage?.getItem(key) ?? null; } catch {}
  const fragment = view.location?.hash ?? '';
  if (fragment.startsWith('#soty-invite=')) {
    token = fragment.slice('#soty-invite='.length);
    view.history.replaceState(view.history.state, '', view.location.pathname + view.location.search);
    if (!/^[A-Za-z0-9_-]{43}$/u.test(token)) token = '';
    try { view.sessionStorage?.setItem(key, token); } catch {}
  }
  if (token !== null && !/^[A-Za-z0-9_-]{43}$/u.test(token)) token = '';
  return { get: () => token, clear() { token = null; try { view.sessionStorage?.removeItem(key); } catch {} } };
}
