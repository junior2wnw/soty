const cacheName = 'soty-nfc-shell-v2';
self.addEventListener('install', event => {
  event.waitUntil((async () => {
    const cache = await caches.open(cacheName);
    const shell = await fetch('/', { cache: 'reload' });
    if (!shell.ok) throw Error('shell unavailable');
    const html = await shell.clone().text();
    const assets = [...html.matchAll(/(?:src|href)="(\/assets\/[^"?#]+)"/g)].map(match => match[1]);
    if (!assets.some(path => path.endsWith('.js'))) throw Error('bundle unavailable');
    await cache.addAll(['/mark.svg', '/manifest.webmanifest', ...assets]);
    await cache.put('/', shell);
  })());
});
self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    for (const key of await caches.keys()) if (key.startsWith('soty-nfc-shell-') && key !== cacheName) await caches.delete(key);
    await self.clients.claim();
  })());
});
self.addEventListener('fetch', event => {
  const request = event.request, url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin || url.search) return;
  if (!['/', '/mark.svg', '/manifest.webmanifest'].includes(url.pathname) && !url.pathname.startsWith('/assets/')) return;
  event.respondWith((async () => {
    const cache = await caches.open(cacheName);
    try {
      const response = await fetch(request);
      if (response.ok) await cache.put(request, response.clone()).catch(() => {});
      return response;
    } catch { return (await cache.match(request)) || Response.error(); }
  })());
});
