/** Ask Soty for an identity proof only on this same-origin request. */
export function createRelayIdentityFetch({ fetch: fetcher = globalThis.fetch, origin = globalThis.location?.origin } = {}) {
  let base;
  try { base = new URL(origin); } catch { throw new TypeError('invalid_soty_origin'); }
  if (typeof fetcher !== 'function' || !['http:', 'https:'].includes(base.protocol) || base.origin !== origin) throw new TypeError('invalid_soty_origin');
  return (path, options = {}) => {
    if (typeof path !== 'string' || !options || typeof options !== 'object') throw new TypeError('invalid_soty_request');
    let url;
    try { url = new URL(path, base); } catch { throw new TypeError('invalid_soty_request'); }
    if (url.origin !== origin || url.hash || options.credentials !== undefined && options.credentials !== 'same-origin'
      || options.mode === 'no-cors' || options.redirect !== undefined && options.redirect !== 'error') throw new TypeError('invalid_soty_request');
    const headers = new Headers(options.headers); headers.set('x-soty-identity-request', '1');
    return fetcher(url.pathname + url.search, { ...options, headers, credentials: 'same-origin', redirect: 'error' });
  };
}
