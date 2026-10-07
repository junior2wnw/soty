/** Browser-only gesture helper. The Source prepares PKCE server-side inside
 * the existing iframe session, before the popup navigates to its fixed issuer. */
export async function startEmbedLogin() {
  const popup = window.open('about:blank', '_blank');
  if (!popup) throw new Error('Разрешите новую вкладку и нажмите «Войти через Соты» ещё раз.');
  let csrf;
  try {
    const prepared = await fetch('/api/embed/login', { headers: { accept: 'application/json' }, credentials: 'same-origin', cache: 'no-store', signal:AbortSignal.timeout(8000) });
    if (!prepared.ok) throw new Error('Не удалось подготовить вход. Попробуйте ещё раз.');
    const intent = await prepared.json();
    if (Object.keys(intent).sort().join(',') !== 'clientId,csrf,issuer,redirectUri,schema' || intent.schema !== 'planner.embed-login-preparation.v1'
      || !/^[A-Za-z0-9_-]{43}$/.test(intent.csrf) || intent.redirectUri !== location.origin + '/api/embed/callback') throw new Error('Вход недоступен. Откройте приложение заново.');
    csrf = intent.csrf;
    const response = await fetch('/api/embed/login', { method: 'POST', credentials: 'same-origin', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ csrf }), cache: 'no-store', signal:AbortSignal.timeout(8000) });
    if (!response.ok) throw new Error('Вход или доступ изменился. Откройте приложение заново.');
    const result = await response.json();
    if (Object.keys(result).sort().join(',') !== 'authorizationUrl,schema' || result.schema !== 'planner.embed-login-authorization.v1') throw new Error('Не удалось безопасно подготовить вход.');
    const destination = new URL(result.authorizationUrl), issuer = new URL(intent.issuer);
    if (issuer.href !== intent.issuer || issuer.pathname !== '/human-identity' || issuer.username || issuer.password || !['https:', 'http:'].includes(issuer.protocol)
      || destination.origin !== issuer.origin || destination.pathname !== issuer.pathname + '/authorize'
      || destination.username || destination.password || destination.hash
      || destination.searchParams.get('client_id') !== intent.clientId || destination.searchParams.get('redirect_uri') !== intent.redirectUri
      || destination.searchParams.get('response_type') !== 'code' || destination.searchParams.get('code_challenge_method') !== 'S256'
      || !/^[A-Za-z0-9_-]{43}$/.test(destination.searchParams.get('state') || '') || popup.closed) throw new Error('Вход отменён. Попробуйте ещё раз.');
    popup.location.replace(destination.href);
  } catch (error) {
    popup.close();
    if (csrf) {
      // Cancel the exact server intent if preparation committed but its reply
      // was lost. Unknown cancellation is never automatic login or replay.
      try { await fetch('/api/embed/login', { method: 'POST', credentials: 'same-origin', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ csrf, cancel: true }), cache: 'no-store', signal:AbortSignal.timeout(8000) }); } catch { /* Bounded Source expiry remains fail-closed. */ }
    }
    throw error;
  }
}
