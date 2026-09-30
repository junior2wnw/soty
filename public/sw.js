const cacheName = "soty-online-v20";
const buildAssets = [];
const classicAssets = [];
const shell = ["/", "/manifest.webmanifest", "/icon.svg", "/icons/soty.svg", "/icons/soty-180.png", "/icons/soty-192.png", "/icons/soty-512.png"];

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
    await cache.addAll([...new Set([...shell, ...buildAssets, ...(classic ? classicAssets : [])])]);
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
      const ready = await Promise.all([...new Set([...shell, ...buildAssets])].map(url => cache.match(url)));
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
    const clients = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
    const confirmedDocuments = new Map(clients.map(client => [client.id, client.url]));
    const decisions = await Promise.all(clients.map(client => new Promise(resolve => {
      const channel = new MessageChannel();
      const timer = setTimeout(() => { channel.port1.close(); resolve(false); }, 10_000);
      channel.port1.onmessage = event => { clearTimeout(timer); channel.port1.close(); resolve(event.data?.ready === true); };
      client.postMessage({ type: 'SOTY_PREPARE_UPDATE' }, [channel.port2]);
    })));
    if (!decisions.every(Boolean)) return false;
    // A newly opened client has not yet confirmed durable drafts.
    const latest = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
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
  if (['/agents', '/api/capabilities', '/oauth', '/mcp',
    '/.well-known/oauth-authorization-server', '/.well-known/oauth-protected-resource']
    .some(prefix => url.pathname === prefix || url.pathname.startsWith(`${prefix}/`))) {
    event.respondWith(fetch(request));
    return;
  }
  if (request.mode === "navigate") {
    event.respondWith(fetch(request).catch(() => caches.open(cacheName).then(async cache => await cache.match("/") || Response.error())));
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
