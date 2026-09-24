const cacheName = "soty-online-v20";
const buildAssets = [];
const shell = ["/", "/manifest.webmanifest", "/icon.svg"];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(cacheName).then((cache) => cache.addAll([...shell, ...buildAssets])));
  // A waiting update is activated by an explicit safe reload, never during an edit.
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      const owned = keys.filter(key => key.startsWith("soty-online-"));
      const keep = new Set([cacheName, ...owned.filter(key => key !== cacheName).slice(-1)]);
      return Promise.all(owned.filter(key => !keep.has(key)).map(key => caches.delete(key)));
    })
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "skipWaiting") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") {
    return;
  }
  const url = new URL(request.url);
  if (url.pathname.startsWith("/ws")) {
    return;
  }
  if (request.mode === "navigate") {
    event.respondWith(fetch(request).catch(() => caches.open(cacheName).then(cache => cache.match("/"))));
    return;
  }
  if (url.origin !== self.location.origin || url.search) {
    return;
  }
  const cacheable = url.pathname.startsWith("/assets/")
    || shell.includes(url.pathname);
  if (!cacheable) {
    event.respondWith(fetch(request));
    return;
  }
  event.respondWith(
    fetch(request)
      .then((response) => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(cacheName).then((cache) => cache.put(request, copy));
        }
        return response;
      })
      .catch(() => caches.match(request).then((cached) => cached || Response.error()))
  );
});
