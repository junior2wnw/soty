const cacheName = "soty-online-v20";
const buildAssets = [];
const classicAssets = [];
const shell = ["/", "/manifest.webmanifest", "/icon.svg", "/icons/soty.svg", "/icons/soty-180.png", "/icons/soty-192.png", "/icons/soty-512.png"];
const legacyHost = new URL(self.location.origin).hostname === 'xn--n1afe0b.online';
const sharedPrimaryHost = new URL(self.location.origin).hostname === '4-2.xn--p1ai';
const offlineShell = legacyHost ? '/__soty' : '/';
const shellUrls = shell.map(url => url === '/' ? offlineShell : url);
function isReservedProtocolDocument(pathname, humanOnly = false) {
  let value = pathname;
  for (let round = 0; round < 3; round++) {
    if (humanOnly ? /^\/human-identity(?:\/|$)/iu.test(value)
      : /^\/(?:agents|api|oauth|mcp|human-identity)(?:\/|$)/iu.test(value)
        || /^\/\.well-known\/(?:oauth-authorization-server|oauth-protected-resource)(?:\/|$)/iu.test(value)) return true;
    try { const decoded = decodeURIComponent(value); if (decoded === value) break; value = decoded; } catch { break; }
  }
  return false;
}
function isSotyDocument(value) {
  const url = new URL(value);
  if (isReservedProtocolDocument(url.pathname, true)) return true;
  if (sharedPrimaryHost) {
    // The retained HIVE and /ecolab documents share this origin, but cannot
    // become an offline Soty shell or participate in Soty's reload handshake.
    return (url.pathname === '/' && !['project', 'hive'].some(name => url.searchParams.has(name)))
      || url.pathname === '/install' || url.pathname.startsWith('/install/')
      || url.pathname === '/agents' || url.pathname.startsWith('/agents/') || url.pathname.startsWith('/oauth/') || url.pathname.startsWith('/human-identity/');
  }
  if (!legacyHost) return true;
  return url.pathname === '/__soty' || url.pathname.startsWith('/install/')
    || url.pathname === '/agents' || url.pathname.startsWith('/agents/') || url.pathname.startsWith('/oauth/') || url.pathname.startsWith('/human-identity/')
    || ['j', 'connector', 'link', 'agent', 'agentRelay', 'agentRelayId', 'reset-local', 'soty-reset', 'repair', 'traffic', 'pwa'].some(name => url.searchParams.has(name))
    || ['classic', 'world'].includes(url.searchParams.get('view'))
    || /^#(?:app|launch|notes|access|mine|library|community)(?:\/|\?|$)/u.test(url.hash);
}

self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const clients = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
    const classic = clients.some(client => {
      const url = new URL(client.url);
      return url.searchParams.get('view') === 'classic'
        || ['j', 'connector', 'link', 'agent', 'agentRelay', 'agentRelayId', 'reset-local', 'soty-reset', 'repair', 'traffic'].some(name => url.searchParams.has(name))
        || url.pathname.startsWith('/install/');
    });
    const cache = await caches.open(cacheName);
    await cache.addAll([...new Set([...shellUrls, ...buildAssets, ...(classic ? classicAssets : [])])]);
  })());
  // A waiting update is activated by an explicit safe reload, never during an edit.
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then(async (keys) => {
      const owned = keys.filter(key => key.startsWith("soty-online-"));
      const keep = new Set([cacheName, ...owned.filter(key => key !== cacheName).slice(-1)]);
      await Promise.all(owned.filter(key => !keep.has(key)).map(key => caches.delete(key)));
      await self.clients.claim();
    })
  );
});

self.addEventListener("message", (event) => {
  if (event.data?.type === 'SOTY_OFFLINE_STATUS') {
    event.waitUntil((async () => {
      const cache = await caches.open(cacheName);
      const ready = await Promise.all([...new Set([...shellUrls, ...buildAssets])].map(url => cache.match(url)));
      event.ports[0]?.postMessage({ version: cacheName, offlineReady: ready.every(Boolean) });
    })());
  } else if (event.data?.type === 'SOTY_ACTIVATE_UPDATE' || event.data?.type === 'skipWaiting') {
    event.waitUntil(prepareUpdate().then(ready => event.ports[0]?.postMessage({ ready })));
  }
});

let preparing;
function prepareUpdate() {
  if (preparing) return preparing;
  preparing = (async () => {
    const clients = (await self.clients.matchAll({ includeUncontrolled: true, type: 'window' })).filter(client => isSotyDocument(client.url));
    const confirmedDocuments = new Map(clients.map(client => [client.id, client.url]));
    const decisions = await Promise.all(clients.map(client => new Promise(resolve => {
      const channel = new MessageChannel();
      const timer = setTimeout(() => { channel.port1.close(); resolve(false); }, 10_000);
      channel.port1.onmessage = event => { clearTimeout(timer); channel.port1.close(); resolve(event.data?.ready === true); };
      client.postMessage({ type: 'SOTY_PREPARE_UPDATE' }, [channel.port2]);
    })));
    if (!decisions.every(Boolean)) return false;
    // A newly opened client has not yet confirmed durable drafts.
    const latest = (await self.clients.matchAll({ includeUncontrolled: true, type: 'window' })).filter(client => isSotyDocument(client.url));
    if (latest.some(client => confirmedDocuments.get(client.id) !== client.url)) return false;
    for (const client of latest) client.postMessage({ type: 'SOTY_UPDATE_COMMIT' });
    await self.skipWaiting();
    return true;
  })().catch(() => false).finally(() => { preparing = undefined; });
  return preparing;
}

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") {
    return;
  }
  const url = new URL(request.url);
  if (url.origin !== self.location.origin || url.pathname.startsWith("/ws")) {
    return;
  }
  // These URLs describe server representations. A cached application shell is
  // never an offline substitute for a contract, authorization or API error.
  if (isReservedProtocolDocument(url.pathname)) {
    event.respondWith(fetch(request));
    return;
  }
  if (request.mode === "navigate") {
    if (!isSotyDocument(url.href)) return;
    event.respondWith(fetch(request).catch(() => caches.open(cacheName).then(async cache => await cache.match(offlineShell) || Response.error())));
    return;
  }
  if (url.search) {
    return;
  }
  const cacheable = url.pathname.startsWith("/assets/")
    || shell.includes(url.pathname);
  if (!cacheable) {
    event.respondWith(fetch(request));
    return;
  }
  const network = fetch(request);
  event.waitUntil(network.then(async response => {
    if (response.ok) {
      const copy = response.clone();
      await (await caches.open(cacheName)).put(request, copy);
    }
  }).catch(() => {}));
  event.respondWith(network.catch(() => caches.match(request).then(cached => cached || Response.error())));
});
