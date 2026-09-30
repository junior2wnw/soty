// Progressive PWA coordination only. Search, links, and all documentation work
// without this script. A missing reply must still block an update safely.
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.addEventListener('message', event => {
    if (!(event.source instanceof ServiceWorker) || event.data?.type !== 'SOTY_PREPARE_UPDATE' || event.ports.length !== 1) return;
    // This script is included only by the read-only documentation renderer.
    try { event.ports[0].postMessage({ ready: true }); }
    finally { event.ports[0].close(); }
  });
}
