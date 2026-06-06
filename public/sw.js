const cacheName = "soty-online-v29";
const shell = ["/", "/icon.svg", "/boot.js"];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(cacheName).then((cache) => cache.addAll(shell)));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== cacheName).map((key) => caches.delete(key)))
    )
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "skipWaiting") {
    self.skipWaiting();
  }
});

self.addEventListener("push", (event) => {
  event.waitUntil(showPushNotices(event));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = new URL(event.notification.data?.url || "/?pwa=1", self.location.origin).href;
  event.waitUntil((async () => {
    const windows = await clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const client of windows) {
      const url = new URL(client.url);
      if (url.origin !== self.location.origin) {
        continue;
      }
      if ("navigate" in client) {
        await client.navigate(targetUrl);
      }
      await client.focus();
      return;
    }
    await clients.openWindow(targetUrl);
  })());
});

async function showPushNotices(event) {
  const notices = await pushNotices(event);
  const visibleNotices = notices.length
    ? notices
    : [{ title: "Соты", body: "Новое сообщение", url: "/?pwa=1", icon: "/icon.svg" }];
  await Promise.all(visibleNotices.slice(0, 4).map((notice) => {
    const title = cleanNoticeText(notice.title, 80) || "Соты";
    const body = cleanNoticeText(notice.body, 180) || "Новое сообщение";
    const url = cleanNoticeUrl(notice.url);
    const icon = cleanNoticeAssetUrl(notice.icon) || "/icon.svg";
    const badge = cleanNoticeAssetUrl(notice.badge) || "/icon.svg";
    const tag = cleanNoticeTag(notice.tag) || `soty:${url}`;
    const vibrate = cleanNoticeVibrate(notice.vibrate);
    const timestamp = cleanNoticeTimestamp(notice.timestamp);
    const options = {
      body,
      icon,
      badge,
      tag,
      renotify: notice.renotify === true,
      data: { url, tag }
    };
    if (vibrate.length > 0) {
      options.vibrate = vibrate;
    }
    if (timestamp) {
      options.timestamp = timestamp;
    }
    return self.registration.showNotification(title, options);
  }));
}

async function pushNotices(event) {
  const dataNotice = noticeFromPushData(event);
  if (dataNotice) {
    return [dataNotice];
  }
  try {
    const subscription = await self.registration.pushManager.getSubscription();
    const endpoint = subscription?.endpoint || "";
    if (!endpoint) {
      return [];
    }
    const response = await fetch("/api/push/notices", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ endpoint })
    });
    if (!response.ok) {
      return [];
    }
    const payload = await response.json();
    return Array.isArray(payload?.notices) ? payload.notices.map(normalizeNotice).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function noticeFromPushData(event) {
  try {
    return normalizeNotice(event.data?.json());
  } catch {
    return null;
  }
}

function normalizeNotice(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  return {
    title: cleanNoticeText(value.title, 80),
    body: cleanNoticeText(value.body, 180),
    url: cleanNoticeUrl(value.url),
    icon: cleanNoticeAssetUrl(value.icon),
    badge: cleanNoticeAssetUrl(value.badge),
    tag: cleanNoticeTag(value.tag),
    renotify: value.renotify === true,
    vibrate: cleanNoticeVibrate(value.vibrate),
    timestamp: cleanNoticeTimestamp(value.timestamp)
  };
}

function cleanNoticeText(value, max) {
  return String(typeof value === "string" || typeof value === "number" ? value : "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanNoticeUrl(value) {
  try {
    const url = new URL(String(value || "/?pwa=1"), self.location.origin);
    return url.origin === self.location.origin ? `${url.pathname}${url.search}${url.hash}` : "/?pwa=1";
  } catch {
    return "/?pwa=1";
  }
}

function cleanNoticeAssetUrl(value) {
  try {
    const text = cleanNoticeText(value, 240);
    if (!text) {
      return "";
    }
    const url = new URL(text, self.location.origin);
    return url.origin === self.location.origin ? `${url.pathname}${url.search}` : "";
  } catch {
    return "";
  }
}

function cleanNoticeTag(value) {
  return cleanNoticeText(value, 140)
    .replace(/[^\p{L}\p{N}:._/?=@-]+/gu, "-")
    .replace(/-+/gu, "-")
    .slice(0, 140);
}

function cleanNoticeVibrate(value) {
  const raw = Array.isArray(value) ? value : typeof value === "number" ? [value] : [];
  return raw
    .map((item) => Math.round(Number(item)))
    .filter((item) => Number.isFinite(item) && item >= 0)
    .map((item) => Math.min(item, 220))
    .slice(0, 7);
}

function cleanNoticeTimestamp(value) {
  const number = Math.round(Number(value));
  return Number.isFinite(number) && number > 0 && number < 4_102_444_800_000
    ? number
    : 0;
}

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
    event.respondWith(fetch(request).catch(() => caches.match("/")));
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
      .catch(() => caches.match(request).then((cached) => cached || caches.match("/")))
  );
});
