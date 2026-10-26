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
  // A window client can be an inherited-origin about:srcdoc iframe. It is not
  // a Soty document and cannot answer the shell's update protocol. Keep actual
  // nested Soty documents on this origin in the cross-tab safety check.
  if (url.origin !== self.location.origin) return false;
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

const pushBindingCache = 'soty-notifications-v1';
const pushBindingPath = '/__soty_push_binding';
let pushBindingWork = Promise.resolve();
const pushSeenCache = 'soty-notifications-seen-v1';
// Cover provider retries after dismissal/restart without keeping an unbounded history.
const pushSeenTtl = 2 * 3600_000, pushSeenLimit = 4096;
let pushSeenWork = Promise.resolve();
const pushInFlight = new Set();
const pushBindingId = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
function validPush(value) {
  return value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => ['schema', 'notificationId', 'bindingId'].includes(key))
    && value.schema === 'soty.push.v1' && typeof value.notificationId === 'string'
    && /^[A-Za-z0-9_-]{32}$/u.test(value.notificationId) && pushBindingId(value.bindingId);
}
async function currentPushBinding() {
  await pushBindingWork;
  const response = await (await caches.open(pushBindingCache)).match(pushBindingPath);
  try { const value=response ? await response.json() : null;return value?.enabled === true ? value.bindingId : null; } catch { return null; }
}
function updatePushBinding(data) {
  const updating = pushBindingWork.then(async () => {
    const cache = await caches.open(pushBindingCache);
    if (data.type === 'SOTY_PUSH_BIND') {
      await cache.put(pushBindingPath, new Response(JSON.stringify({ bindingId: data.bindingId, enabled: data.enabled }), { headers: { 'Content-Type': 'application/json' } }));
    } else {
      const prior = await cache.match(pushBindingPath);
      if (prior && (await prior.json()).bindingId === data.bindingId) await cache.delete(pushBindingPath);
    }
    const shown = await self.registration.getNotifications?.() ?? [];
    for (const notification of shown) {
      const bound = notification.data?.bindingId;
      if (pushBindingId(bound) && (data.type === 'SOTY_PUSH_BIND' ? !data.enabled || bound !== data.bindingId : bound === data.bindingId)) notification.close();
    }
  });
  pushBindingWork = updating.catch(() => {});
  return updating;
}
function reservePush(data) {
  const reserving=pushSeenWork.then(async()=>{
    if(await currentPushBinding()!==data.bindingId)return false;
    const key='/__soty_push_seen/'+data.bindingId+'/'+data.notificationId;
    if(pushInFlight.has(key)||pushInFlight.size>=pushSeenLimit)return false;
    const previous=await(await caches.open(pushSeenCache)).match(key,{ignoreSearch:true});
    const until=Number(previous?.headers.get('X-Soty-Seen-Until')),now=Date.now();
    if(until>now&&until<=now+pushSeenTtl)return false;
    pushInFlight.add(key);return key;
  });
  pushSeenWork=reserving.catch(()=>{});return reserving;
}
function rememberPush(key) {
  const remembering=pushSeenWork.then(async()=>{
    const cache=await caches.open(pushSeenCache),now=Date.now(),live=[];
    for(const request of await cache.keys()){
      const until=Number(new URL(request.url).searchParams.get('until'));
      if(!Number.isSafeInteger(until)||until<=now||until>now+pushSeenTtl)await cache.delete(request);
      else live.push({request,until});
    }
    live.sort((left,right)=>left.until-right.until);
    while(live.length>=pushSeenLimit)await cache.delete(live.shift().request);
    await cache.delete(key,{ignoreSearch:true});
    const until=now+pushSeenTtl;
    await cache.put(key+'?until='+until,new Response('',{headers:{'X-Soty-Seen-Until':String(until)}}));
  });
  pushSeenWork=remembering.catch(()=>{});return remembering;
}
self.addEventListener('push', event => {
  event.waitUntil((async () => {
    let data;
    try { if (!event.data || event.data.text().length > 1024) return; data = event.data.json(); } catch { return; }
    if (!validPush(data)) return;
    const reservation=await reservePush(data);if(!reservation)return;
    try {
      if(await currentPushBinding()!==data.bindingId)return;
      const tag='soty-'+data.bindingId+'-'+data.notificationId;
      await self.registration.showNotification('Соты', { body: 'Новое сообщение', icon: '/icons/soty-192.png',
        badge: '/icons/soty-192.png', tag, data, renotify: false });
      // Binding changes must not wait for a held OS show. Clean up its late card only.
      try {
        if(await currentPushBinding()!==data.bindingId){
          const shown=await self.registration.getNotifications({tag});
          if(await currentPushBinding()!==data.bindingId)for(const notification of shown)
            if(notification.tag===tag&&notification.data?.bindingId===data.bindingId&&notification.data?.notificationId===data.notificationId)notification.close();
        }
      } finally {await rememberPush(reservation);}
    } finally {pushInFlight.delete(reservation);}
  })());
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil((async () => {
    const data = event.notification.data;
    if (!validPush(data) || await currentPushBinding() !== data.bindingId) return;
    // Only the fixed Soty shell and an opaque lookup ID cross this boundary.
    // The signed app checks the current account/device/ACL before opening a chat.
    const url = new URL(offlineShell, self.location.origin); url.searchParams.set('notification', data.notificationId);
    await self.clients.openWindow(url.href);
  })());
});

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
  if (['SOTY_PUSH_BIND', 'SOTY_PUSH_CLEAR'].includes(event.data?.type)) {
    const source = event.source?.url;
    if (!source || new URL(source).origin !== self.location.origin || !isSotyDocument(source)
      || !pushBindingId(event.data.bindingId) || event.data.type === 'SOTY_PUSH_BIND' && typeof event.data.enabled !== 'boolean'
      || Object.keys(event.data).some(key => !['type', 'bindingId', ...(event.data.type === 'SOTY_PUSH_BIND' ? ['enabled'] : [])].includes(key))) return;
    event.waitUntil(updatePushBinding(event.data).then(() => event.ports[0]?.postMessage({ ok: true }), () => event.ports[0]?.postMessage({ ok: false })));
  } else if (event.data?.type === 'SOTY_OFFLINE_STATUS') {
    event.waitUntil((async () => {
      const cache = await caches.open(cacheName);
      const ready = await Promise.all([...new Set([...shellUrls, ...buildAssets])].map(url => cache.match(url)));
      event.ports[0]?.postMessage({ version: cacheName, offlineReady: ready.every(Boolean) });
    })());
  } else if (event.data?.type === 'SOTY_ACTIVATE_UPDATE' || event.data?.type === 'skipWaiting') {
    event.waitUntil(prepareUpdate().then(result => {
      const location = result.clientId && event.source?.id ? (result.clientId === event.source.id ? 'current' : 'other') : 'unknown';
      event.ports[0]?.postMessage(result.ready ? { ready: true } : { ready: false, reason: result.reason, location });
    }));
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
      const timer = setTimeout(() => { channel.port1.close(); resolve({ ready: false, reason: 'unresponsive', clientId: client.id }); }, 10_000);
      channel.port1.onmessage = event => {
        clearTimeout(timer); channel.port1.close();
        const reason = ['work', 'app', 'form', 'dialog', 'busy', 'signin', 'save'].includes(event.data?.reason) ? event.data.reason : 'unknown';
        resolve(event.data?.ready === true ? { ready: true } : { ready: false, reason, clientId: client.id });
      };
      client.postMessage({ type: 'SOTY_PREPARE_UPDATE' }, [channel.port2]);
    })));
    const blocked = decisions.find(decision => !decision.ready);
    if (blocked) return blocked;
    // A newly opened client has not yet confirmed durable drafts.
    const latest = (await self.clients.matchAll({ includeUncontrolled: true, type: 'window' })).filter(client => isSotyDocument(client.url));
    if (latest.some(client => confirmedDocuments.get(client.id) !== client.url)) return { ready: false, reason: 'tabs-changed' };
    for (const client of latest) client.postMessage({ type: 'SOTY_UPDATE_COMMIT' });
    await self.skipWaiting();
    return { ready: true };
  })().catch(() => ({ ready: false, reason: 'unknown' })).finally(() => { preparing = undefined; });
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
  let cacheHit = false;
  const network = (async () => {
    // This exact build's immutable modules need no network revalidation on reload.
    if (buildAssets.includes(url.pathname) || classicAssets.includes(url.pathname)) {
      const cached = await caches.open(cacheName).then(cache => cache.match(request)).catch(() => undefined);
      if (cached) { cacheHit = true; return cached; }
    }
    return fetch(request);
  })();
  event.waitUntil(network.then(async response => {
    if (response.ok && !cacheHit) {
      const copy = response.clone();
      await (await caches.open(cacheName)).put(request, copy);
    }
  }).catch(() => {}));
  event.respondWith(network.catch(() => caches.match(request).then(cached => cached || Response.error())));
});
