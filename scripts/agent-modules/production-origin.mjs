const productionShells = new Set(['https://4-2.xn--p1ai', 'https://xn--n1afe0b.online', 'https://soty.pochinit.online']);

// An installed connector keeps its relay and credentials. These exact HTTPS
// shells belong to the same production relay; app subdomains are excluded.
export function productionShellOriginAllowed(origin, relayOrigin) {
  return origin !== '' && (origin === relayOrigin || (productionShells.has(origin) && productionShells.has(relayOrigin)));
}
