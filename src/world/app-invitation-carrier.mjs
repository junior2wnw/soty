/** One-use browser location data, never authority or an Apps HTTP path. */
export function mountAppInvitationCarrier({ view, appId, take, readPeer }) {
  let pendingTake = take, used = false, disposed = false;
  const receive = event => {
    if (disposed || used || !pendingTake) return;
    const data = event.data;
    if (!data || Object.keys(data).sort().join(',') !== 'appId,requestId,schema'
      || data.schema !== 'soty.app-invitation-ready.v1' || data.appId !== appId
      || typeof data.requestId !== 'string' || !/^[a-f0-9]{32}$/u.test(data.requestId)) return;
    const peer = readPeer();
    if (disposed || used || !peer || peer.approved !== true || peer.appId !== appId
      || event.source !== peer.window || event.origin !== peer.origin) return;
    // Retire before the one-use take: reentrant ready events cannot redeem it twice.
    used = true; const takeCurrent = pendingTake; pendingTake = undefined;
    const value = takeCurrent();
    if (disposed || !value) return;
    peer.window.postMessage({ schema: 'soty.app-invitation.v1', appId, requestId: data.requestId, token: value }, peer.origin);
  };
  view.addEventListener('message', receive);
  return () => { disposed = true; pendingTake = undefined; view.removeEventListener('message', receive); };
}
